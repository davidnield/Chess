"""Read a complete paired outcome pool, routing requests to their exact buckets."""
from collections import OrderedDict
import json
from pathlib import Path
import polars as pl
from .partitioned_pool import SCHEMA
from .stages import file_sha
from .verified_files import is_verified, verify_many


def _verify(items, message):
    try:
        verify_many(items)
    except ValueError as exc:
        raise ValueError(message) from exc


class PartitionedPoolReader:
    def __init__(self, path, column):
        self.path = Path(path).resolve(); self.column = column
        self.root = self.path.parent
        self.manifest = json.loads(self.path.read_text())
        m = self.manifest
        role = m.get('role')
        if (m.get('schema') != SCHEMA or m.get('complete') is not True or m.get('buckets') != 512
            or role not in ('edges','aux') or column != ('parent_hash' if role=='edges' else 'position_hash')
            or m.get('metadata',{}).get('key_bits') != 128 or m['metadata'].get('min_games') != 1):
            raise ValueError('not a complete full-key outcome publication')
        self.manifest_sha = file_sha(self.path)
        pubpath = self.root/'publication.json'
        pub = json.loads(pubpath.read_text())
        self.publication_sha = file_sha(pubpath)
        if pub.get('schema') != SCHEMA or pub.get('complete') is not True or pub.get('pool_id') != m['pool_id']:
            raise ValueError('paired publication incomplete or changed')
        for kind in ('edges','aux'):
            info = pub[kind]
            if info['file'] != f'{kind}.json':raise ValueError('invalid paired manifest path')
            p = self.root/info['file']
            if file_sha(p) != info['sha256'] or file_sha(str(p)+'.meta.json') != info['metadata_sha256']:
                raise ValueError('paired manifest or metadata changed')
            sibling=json.loads(p.read_text())
            if (sibling.get('schema')!=SCHEMA or sibling.get('complete') is not True
                or sibling.get('role')!=kind or sibling.get('buckets')!=512
                or sibling.get('pool_id')!=m['pool_id'] or sibling.get('metadata')!=m['metadata']):
                raise ValueError('paired manifest semantics differ')
            seen=set()
            for item in sibling['files']:
                bucket=item['bucket'];child=self.root/item['file']
                if (type(bucket) is not int or bucket not in range(512) or bucket in seen
                    or item['file']!=f'buckets/b{bucket:03d}.{kind}.parquet'):
                    raise ValueError('invalid paired bucket inventory')
                if not child.is_file() or child.stat().st_size!=item['bytes']:
                    raise ValueError('missing or changed paired outcome bucket')
                seen.add(bucket)
            if seen!=set(range(512)):raise ValueError('incomplete paired bucket inventory')
        if self.path.name != pub[role]['file'] or self.manifest_sha != pub[role]['sha256']:
            raise ValueError('unpublished outcome manifest')
        sidecar=json.loads(Path(str(self.path)+'.meta.json').read_text())
        if sidecar != dict(m['metadata'],role=role):raise ValueError('pool sidecar semantics differ')
        self.files = {}
        for f in m['files']:
            b=f['bucket']
            if b not in range(512) or b in self.files or f['file'] != f'buckets/b{b:03d}.{role}.parquet':
                raise ValueError('duplicate, extra or unsafe bucket path')
            p=self.root/f['file']
            if not p.is_file() or p.stat().st_size != f['bytes']:
                raise ValueError('missing or changed outcome bucket')
            self.files[b]=f
        if set(self.files)!=set(range(512)):raise ValueError('incomplete outcome inventory')
        self.open=OrderedDict(); self.verified={}
        self.stats=dict(queries=0,total_groups=0,selected_groups=0,selected_rows=0,
                       selected_compressed_bytes=0,total_compressed_bytes=0,unpruned_queries=0)

    def fetch(self, db, keys):
        from .keyed_parquet import KeyedParquet
        if file_sha(self.root/'publication.json') != self.publication_sha or file_sha(self.path) != self.manifest_sha:
            raise ValueError('outcome publication changed while open')
        grouped={}
        for k in keys:
            if set(k)!= {'lo','hi'} or any(type(k[c]) is not int or not -(1<<63)<=k[c]<(1<<63) for c in k):
                raise ValueError('invalid full position key')
            grouped.setdefault(k['lo']%512,[]).append(k)
        frames=[]
        # Hash the buckets this fetch needs together, once per process (sweep.verified_files).
        need=[]
        for b in grouped:
            f=self.files[b]; p=self.root/f['file']; st=p.stat()
            if st.st_size!=f['bytes']:raise ValueError('outcome bucket changed')
            if self.verified.get(b)!=(st.st_size,st.st_mtime_ns):need.append((p,f['sha256']))
        _verify(need,'outcome bucket hash differs')
        for b,ks in grouped.items():
            f=self.files[b]; p=self.root/f['file']; st=p.stat(); stamp=st.st_size,st.st_mtime_ns
            if st.st_size!=f['bytes']:raise ValueError('outcome bucket changed')
            if self.verified.get(b)!=stamp:
                if not is_verified(p,f['sha256']):_verify([(p,f['sha256'])],'outcome bucket hash differs')
                self.verified[b]=stamp
                if b in self.open:self.open.pop(b).close()
            if b not in self.open:
                if len(self.open)>=8:self.open.popitem(last=False)[1].close()
                self.open[b]=KeyedParquet(p,self.column)
            reader=self.open[b]; self.open.move_to_end(b)
            before=dict(reader.stats); frames.append(reader.fetch(db,ks))
            for name,v in reader.stats.items():self.stats[name]+=v-before[name]
            after=p.stat()
            if (after.st_size,after.st_mtime_ns)!=stamp:raise ValueError('outcome bucket changed during read')
        if frames:return pl.concat(frames)
        import pool_from_book as pool
        columns=pool.POOL_SCHEMA if self.column=='parent_hash' else pool.AUX_SCHEMA
        return pl.from_arrow(pool._arrow_schema(pool.output_schema(columns,True)).empty_table())

    def close(self):
        for r in self.open.values():r.close()
        self.open.clear()


def fetch_positions(db,path,keys,column='parent_hash'):
    from .keyed_parquet import KeyedParquet
    reader=KeyedParquet(path,column)
    try:return reader.fetch(db,keys)
    finally:reader.close()
