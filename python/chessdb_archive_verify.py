"""Verify a completed archive without the original database or WSL.
python chessdb_archive_verify.py --out ARCHIVE [--allow-bounded] [--deep] [--report PATH]
Default: exact manifest/coverage checks and unbuffered SHA256 of every Parquet file.
--deep additionally reconstructs native logical digests from all Parquet columns.
"""
import argparse,json,time
from pathlib import Path
from month_archive import read_json,sha,write_json,file_lock
from chessdb_archive import EXPECTED,CHECK_FIELDS,verify_chain,validate_inventory,run_json,HERE
from chessdb_archive_windows import unbuffered_sha

def verify(root,allow_bounded=False,deep=False):
    marker=root/'ARCHIVE.COMPLETE.json'
    if not marker.exists() and allow_bounded:marker=root/'BOUNDED.TEST.COMPLETE.json'
    m=read_json(marker);identity=read_json(root/'identity.json');idh=sha(root/'identity.json')
    if m['identity']!=idh or m['bounded']!=identity['bounded'] or (m['bounded'] and not allow_bounded):raise ValueError('wrong archive identity/scope')
    if not m['bounded'] and identity['expected_records']!=EXPECTED:raise ValueError('wrong full-snapshot size')
    inv=identity['inventory'];validate_inventory(inv)
    if not m['bounded']:
        expected_plans=[dict(id=i,start=s,end=inv['starts'][i+1] if i+1<len(inv['starts']) else None) for i,s in enumerate(inv['starts'])]
        if identity['plans']!=expected_plans:raise ValueError('source range coverage changed')
    elif len(identity['plans'])!=1 or identity['plans'][0]['start']!=inv['first_key'] or identity['plans'][0]['id']!=0:raise ValueError('invalid bounded coverage')
    prov=root/'provenance/manifest.json'
    if sha(prov)!=m['provenance_manifest_sha256']:raise ValueError('provenance manifest changed')
    for name,digest in read_json(prov).items():
        p=(prov.parent/name).resolve()
        if prov.parent.resolve() not in p.parents or sha(p)!=digest:raise ValueError('provenance file changed')
    actual={p.relative_to(root).as_posix():p for p in (root/'receipts').rglob('*.json')}
    if set(actual)!=set(m['receipts']):raise ValueError('archive receipt set changed')
    rows=[]
    for name,path in actual.items():
        if sha(path)!=m['receipts'][name]:raise ValueError('archive receipt changed')
        rows.append(read_json(path))
    files=set();count=0;bytes_=0
    for plan in identity['plans']:
        records=sorted([r for r in rows if r['range']==plan['id']],key=lambda r:r['index'])
        _,done,n=verify_chain(plan,records,idh)
        if not done:raise ValueError('incomplete source range')
        for r in records:
            name=f'parquet/r{plan["id"]:03d}/c{r["index"]:06d}.parquet'
            if r['file']!=name or name in files:raise ValueError('invalid archive file path')
            f=root/name
            if f.stat().st_size!=r['audit']['bytes'] or unbuffered_sha(f)!=r['audit']['file_sha256']:raise ValueError('archive bytes changed')
            if deep:
                audit=run_json([str(HERE/'chessdb-archive.exe'),'audit',str(f)])
                if any(audit[k]!=r['source'][k] for k in CHECK_FIELDS):raise ValueError('deep logical audit mismatch')
            files.add(name);bytes_+=r['audit']['bytes']
        count+=n
    if count!=m['records'] or count!=identity['expected_records'] or len(files)!=m['shards'] or bytes_!=m['parquet_bytes']:raise ValueError('final archive totals disagree')
    if len(rows)!=len(files) or {p.relative_to(root).as_posix() for p in (root/'parquet').rglob('*.parquet')}!=files:raise ValueError('extra archive data')
    return dict(verified=True,bounded=m['bounded'],deep=deep,records=count,shards=len(files),bytes=bytes_,marker_sha256=sha(marker),original_source_required=False)
def main():
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--allow-bounded',action='store_true');p.add_argument('--deep',action='store_true');p.add_argument('--report',type=Path);a=p.parse_args();t=time.monotonic()
    with file_lock(a.out/'coordinator.lock'):r=verify(a.out,a.allow_bounded,a.deep)
    r['seconds']=time.monotonic()-t
    if a.report:write_json(a.report,r)
    print(json.dumps(r))
if __name__=='__main__':main()
