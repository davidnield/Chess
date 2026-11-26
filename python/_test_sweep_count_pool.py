"""Full-count exports against an independent Python census and published readers."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import duckdb
import polars as pl
from polars.testing import assert_frame_equal
from consumer_keys import PAIR
from sweep.count_pool import aggregate,check_receipt,inputs,run_bucket,capacity,SCHEMA,SUMS
from sweep.count_pool_reader import CountPoolReader
from sweep.keyed_parquet import KeyedParquet
from sweep.stages import atomic_json,file_sha


def compact_fixture(root,frame,meta):
    root.mkdir();records=[]
    schema=dict(parent_hash=PAIR,move_san=pl.String,move_xor_lo=pl.Int64,move_xor_hi=pl.Int64,
                ply=pl.Int32,**{c:pl.Int64 for c in SUMS})
    grouped={}
    for r in frame.to_dicts():
        k=r["parent_hash"];ch=r["child_hash"]
        grouped.setdefault(k["lo"]%512,[]).append(dict(parent_hash=k,move_san=r["move_san"],
            move_xor_lo=k["lo"]^ch["lo"],move_xor_hi=k["hi"]^ch["hi"],ply=r.get("ply",1),**{c:r[c] for c in SUMS}))
    for b in range(512):
        p=root/f"b{b:03d}-r00.parquet"
        pl.DataFrame(grouped.get(b,[]),schema=schema).sort("parent_hash","move_san").write_parquet(p)
        records.append(dict(bucket=b,part=0,file=p.name,bytes=p.stat().st_size,sha256=file_sha(p),low=-(1<<63),high=(1<<63)-1))
    atomic_json(root/"pool.json",dict(meta,schema=SCHEMA,complete=True,buckets=512,parts=1,files=records))
    atomic_json(root/"pool.json.meta.json",meta)
    return root/"pool.json"


class FullCounts(unittest.TestCase):
    def rows(self):
        return [dict(parent_hash=lo,parent_hash_hi=hi,move_san=san,
            parent_epd="fixture",child_hash=ch,child_hash_hi=chi,ply=ply,
            white_wins=w,draws=d,black_wins=b,total=w+d+b)
            for lo,hi,san,ch,chi,ply,w,d,b in [
                (-512,7,"e4",12,19,1,2,0,1),(-512,7,"e4",12,19,3,1,2,0),
                (-512,9,"d4",12,29,2,0,0,1),(0,3,"Nf3",51,99,30,1,0,0),
                (0,3,"Nf3",51,99,31,100,0,0),(512,3,"d4",-9,20,29,0,3,0)]]

    def source(self,root,rows):
        path=root/"source.parquet"
        pl.DataFrame(rows).write_parquet(path)
        return path

    def test_every_observed_move_128bit_transpositions_and_exact_cap(self):
        with tempfile.TemporaryDirectory() as td,duckdb.connect(config={"threads":1}) as db:
            root=Path(td);rows=self.rows();source=self.source(root,rows);frames=[]
            for part in range(4):
                out=root/f"r{part}.parquet"
                aggregate(db,[str(source)],out,cap=30,bucket=0,part=part,parts=4)
                frames+=pl.read_parquet(out).to_dicts()
            expected={}
            for r in rows:
                if r["ply"]>30:continue
                k=r["parent_hash"],r["parent_hash_hi"],r["move_san"]
                x=expected.setdefault(k,dict(counts=[0]*4,ply=r["ply"],child=(r["child_hash"],r["child_hash_hi"])))
                x["counts"]=[a+r[c] for a,c in zip(x["counts"],SUMS)];x["ply"]=min(x["ply"],r["ply"])
            self.assertEqual(len(frames),len(expected))
            for r in frames:
                lo,hi=r["parent_hash"].values();x=expected[(lo,hi,r["move_san"])]
                self.assertEqual([r[c] for c in SUMS],x["counts"])
                self.assertEqual(r["ply"],x["ply"])
                self.assertEqual((lo^r["move_xor_lo"],hi^r["move_xor_hi"]),x["child"])

    def test_invalid_child_count_and_bucket_fail(self):
        for fault in ("child","count","bucket","half"):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as td,duckdb.connect() as db:
                root=Path(td);rows=self.rows()
                if fault=="child":rows[1]["child_hash_hi"]=999
                if fault=="count":rows[0]["total"]=90
                if fault=="bucket":rows[0]["parent_hash"]=-511
                if fault=="half":rows[0]["parent_hash_hi"]=None
                path=self.source(root,rows)
                with self.assertRaises((ValueError,duckdb.Error)):
                    aggregate(db,[str(path)],root/"out.parquet",cap=30,bucket=0,part=0,parts=1)

    def publication(self,root,db):
        source=self.source(root,self.rows());out=root/"b000-r00.parquet"
        stats=aggregate(db,[str(source)],out,cap=30,bucket=0,part=0,parts=1)
        empty=root/"empty.parquet";pl.read_parquet(out).head(0).write_parquet(empty)
        records=[]
        for b in range(512):
            p=root/f"b{b:03d}-r00.parquet"
            if b:p.write_bytes(empty.read_bytes())
            records.append(dict(bucket=b,part=0,file=p.name,bytes=p.stat().st_size,sha256=file_sha(p),
                low=-(1<<63),high=(1<<63)-1))
        manifest=dict(schema=SCHEMA,complete=True,key_bits=128,min_games=1,buckets=512,parts=1,files=records)
        atomic_json(root/"pool.json",manifest)
        return manifest

    def test_published_lookup_missing_files_and_hash_checks(self):
        with tempfile.TemporaryDirectory() as td,duckdb.connect() as db:
            root=Path(td);m=self.publication(root,db)
            reader=KeyedParquet(root/"pool.json","parent_hash")
            keys=[dict(lo=-512,hi=7),dict(lo=-512,hi=9),dict(lo=0,hi=88)]
            got=reader.fetch(db,keys).sort("move_san")
            self.assertEqual(got["total"].to_list(),[1,6])
            self.assertEqual(got["child_hash"].to_list(),[dict(lo=12,hi=29),dict(lo=12,hi=19)])
            self.assertEqual(reader.fetch(db,[]).height,0)
            self.assertEqual(reader.fetch(db,[dict(lo=1,hi=7)]).height,0)
            reader.close()
            missing=root/"b511-r00.parquet";saved=missing.read_bytes();missing.unlink()
            with self.assertRaisesRegex(ValueError,"missing"):CountPoolReader(root/"pool.json","parent_hash")
            missing.write_bytes(saved)
            m["files"][0]["sha256"]="0"*64;atomic_json(root/"pool.json",m)
            reader=CountPoolReader(root/"pool.json","parent_hash")
            with self.assertRaisesRegex(ValueError,"hash"):reader.fetch(db,keys)
            reader.close()
            m["files"].pop();atomic_json(root/"pool.json",m)
            with self.assertRaisesRegex(ValueError,"incomplete"):CountPoolReader(root/"pool.json","parent_hash")

    def test_input_inventory_and_resume_receipt_fail_closed(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);p=root/"done.parquet";p.write_bytes(b"count data")
            receipt=root/"receipt.json";atomic_json(receipt,dict(identity="a",file=p.name,bytes=p.stat().st_size,sha256=file_sha(p)))
            self.assertEqual(check_receipt(root,receipt,"a")["identity"],"a")
            with self.assertRaises(ValueError):check_receipt(root,receipt,"b")
            p.write_bytes(b"bad counts")
            with self.assertRaises(ValueError):check_receipt(root,receipt,"a")
            source=dict(book=str(root),manifests={},files={})
            recipe=dict(events=["Rapid"],elo_bands=[1800])
            self.assertEqual(inputs(source,recipe,0),[])
            surprise=root/"ps/event=Rapid/elo_band=1800/bkt000.parquet"
            surprise.parent.mkdir(parents=True);surprise.write_bytes(b"unexpected")
            with self.assertRaises(ValueError):inputs(source,recipe,0)

    def test_resume_keeps_checked_ranges_stop_and_space_guards(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);source=self.source(root,self.rows());temp=root/"tmp";temp.mkdir()
            cfg=dict(source={},producer={},parts=2,pilot_buckets=[0],stop=str(root/"STOP"),
                temp=str(temp),range_headroom_bytes=1000,temp_reserve_bytes=0,spill_bytes=10000000,
                memory="256MB",threads=1)
            recipe=dict(id="test",output=str(root/"out"),reserve_bytes=0,max_ply=30)
            with patch("sweep.count_pool.inputs",return_value=[str(source)]):
                self.assertTrue(run_bucket(cfg,recipe,0))
                p=root/"out/b000-r00.parquet";before=p.stat().st_mtime_ns
                with patch("sweep.count_pool.aggregate",side_effect=AssertionError("recomputed completed range")):
                    self.assertTrue(run_bucket(cfg,recipe,0))
                self.assertEqual(p.stat().st_mtime_ns,before)
                self.assertTrue(capacity(cfg,recipe)["fits"])
                Path(cfg["stop"]).write_text("pause")
                self.assertFalse(run_bucket(cfg,recipe,1))
                Path(cfg["stop"]).unlink()
                recipe["reserve_bytes"]=10**20
                with self.assertRaisesRegex(RuntimeError,"reserve"):run_bucket(cfg,recipe,1)
                self.assertFalse(capacity(cfg,recipe)["fits"])


if __name__=="__main__":unittest.main()
