"""Read complete count-pool publications; missing ranges are errors, never zeros."""
from collections import OrderedDict
import json
from pathlib import Path
import duckdb
import polars as pl
from consumer_keys import PAIR
from .count_pool import SCHEMA, SUMS
from .stages import file_sha
from .verified_files import is_verified, verify_many


def _verify(items):
    try:
        verify_many(items)
    except ValueError as exc:
        raise ValueError("count range hash changed") from exc


class CountPoolReader:
    def __init__(self,path,column):
        if column!="parent_hash":raise ValueError("policy-count pools have parent keys only")
        self.path=Path(path).resolve();self.column=column
        m=json.loads(self.path.read_text())
        parts=m.get("parts",0)
        if (m.get("schema")!=SCHEMA or m.get("complete") is not True or m.get("buckets")!=512
            or m.get("key_bits")!=128 or m.get("min_games")!=1 or parts not in (1,2,4,8,16,32,64)):
            raise ValueError("not a complete uncut count pool")
        expected={(b,r) for b in range(512) for r in range(parts)};self.files={}
        for f in m["files"]:
            key=f["bucket"],f["part"]
            if key not in expected or key in self.files:raise ValueError("duplicate/extra count range")
            if f["file"]!=f"b{key[0]:03d}-r{key[1]:02d}.parquet":raise ValueError("invalid range filename")
            width=(1<<64)//parts;lo=-(1<<63)+key[1]*width
            if (f["low"],f["high"])!=(lo,lo+width-1):raise ValueError("range bounds differ")
            p=self.path.parent/f["file"]
            if not p.is_file() or p.stat().st_size!=f["bytes"]:raise ValueError("missing/changed count range")
            self.files[key]=f
        if set(self.files)!=expected:raise ValueError("incomplete count publication")
        self.parts=parts;self.open=OrderedDict();self.verified={}
        self.stats=dict(queries=0,total_groups=0,selected_groups=0,selected_rows=0,
            selected_compressed_bytes=0,total_compressed_bytes=0,unpruned_queries=0)

    def fetch(self,db,keys):
        from .keyed_parquet import KeyedParquet
        grouped={};width=(1<<64)//self.parts
        for k in keys:
            key=(k["lo"]%512,(k["lo"]+(1<<63))//width)
            grouped.setdefault(key,[]).append(k)
        frames=[]
        # Hash the ranges this fetch needs together, once per process (sweep.verified_files).
        need=[]
        for key in grouped:
            f=self.files[key];path=self.path.parent/f["file"];stat=path.stat()
            if stat.st_size!=f["bytes"]:raise ValueError("count range changed")
            if self.verified.get(key)!=(stat.st_size,stat.st_mtime_ns):need.append((path,f["sha256"]))
        _verify(need)
        for key,ks in grouped.items():
            f=self.files[key];path=self.path.parent/f["file"]
            stat=path.stat();fingerprint=(stat.st_size,stat.st_mtime_ns)
            if stat.st_size!=f["bytes"]:raise ValueError("count range changed")
            if self.verified.get(key)!=fingerprint:
                if not is_verified(path,f["sha256"]):_verify([(path,f["sha256"])])
                self.verified[key]=fingerprint
            if key not in self.open:
                if len(self.open)>=8:self.open.popitem(last=False)[1].close()
                self.open[key]=KeyedParquet(path,self.column)
            reader=self.open[key];self.open.move_to_end(key)
            before=dict(reader.stats);frame=reader.fetch(db,ks)
            for n,v in reader.stats.items():self.stats[n]+=v-before[n]
            # DuckDB applies the same signed bitwise XOR as the explorer reader.
            db.register("compact_counts",frame.to_arrow())
            try:
                frame=pl.from_arrow(db.execute("SELECT * EXCLUDE(move_xor_lo,move_xor_hi), "
                    "struct_pack(lo:=xor(parent_hash.lo,move_xor_lo),hi:=xor(parent_hash.hi,move_xor_hi)) "
                    "AS child_hash FROM compact_counts").to_arrow_table())
            finally:db.unregister("compact_counts")
            frames.append(frame)
        if frames:return pl.concat(frames)
        return pl.DataFrame(schema=dict(parent_hash=PAIR,move_san=pl.String,ply=pl.Int32,
            **{c:pl.Int64 for c in SUMS},child_hash=PAIR))

    def close(self):
        for reader in self.open.values():reader.close()
        self.open.clear()
