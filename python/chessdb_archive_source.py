"""WSL source census/hash verification. Read-only; actual bytes are rehashed on every invocation.
python3 chessdb_archive_source.py DB MANIFEST OUTPUT_JSON
Manifest SHA-256 entries and Git-blob SHA-1 entries follow the upstream download manifest.
"""
import concurrent.futures,hashlib,json,os,subprocess,sys,time
from pathlib import Path
def verify_file(db,row):
    name,size,expected=row;p=db/name;before=p.stat()
    if not p.is_file() or before.st_size!=size:raise ValueError(f'source size mismatch {name}')
    h=hashlib.sha256() if len(expected)==64 else hashlib.sha1()
    if len(expected)==40:h.update(f'blob {size}\0'.encode())
    elif len(expected)!=64:raise ValueError('unknown source checksum')
    with p.open('rb') as f:
        for b in iter(lambda:f.read(8<<20),b''):h.update(b)
    after=p.stat()
    if (before.st_size,before.st_mtime_ns,before.st_ino)!=(after.st_size,after.st_mtime_ns,after.st_ino):raise ValueError(f'source changed {name}')
    if h.hexdigest()!=expected:raise ValueError(f'source checksum mismatch {name}')
    return dict(name=name,bytes=size,sha=expected,mtime_ns=after.st_mtime_ns,inode=after.st_ino)
def main():
    if len(sys.argv)==3 and sys.argv[1]=='--check-stamps':
        r=json.loads(Path(sys.argv[2]).read_text())
        options=subprocess.check_output(['findmnt','-n','-o','OPTIONS','--target',r['db']],text=True).strip().splitlines()
        if not options or any('ro' not in s.split(',') for s in options):raise ValueError('source no longer read-only')
        for x in r['files']:
            s=(Path(r['db'])/x['name']).stat()
            if (s.st_size,s.st_mtime_ns,s.st_ino)!=(x['bytes'],x['mtime_ns'],x['inode']):raise ValueError('source changed: '+x['name'])
        return
    db,manifest,out=map(Path,sys.argv[1:]);start=time.monotonic()
    options=subprocess.check_output(['findmnt','-n','-o','OPTIONS','--target',str(db)],text=True).strip().splitlines()
    if not options or any('ro' not in s.split(',') for s in options):raise ValueError('source mount is not read-only')
    text=manifest.read_bytes();rows=[]
    for line in text.decode().splitlines():
        path,size,sha,*_=line.split('\t');name=path.replace('\\','/').split('/')[-1]
        if not name or name in ('.','..'):raise ValueError('bad source filename')
        rows.append((name,int(size),sha))
    if len({r[0] for r in rows})!=len(rows):raise ValueError('duplicate source file')
    expected={r[0] for r in rows};actual={p.name for p in db.iterdir()};extras=actual-expected
    # The original downloader copied one local .ok receipt beside each payload.
    # These are inventoried separately and never substituted for hashing source bytes.
    if expected-actual or not extras<={n+'.ok' for n in expected}:raise ValueError('source file set differs from manifest')
    sidecars={}
    for name in sorted(extras):
        p=db/name
        if not p.is_file() or p.is_symlink() or p.stat().st_size>65536:raise ValueError('invalid downloader sidecar')
        sidecars[name]=hashlib.sha256(p.read_bytes()).hexdigest()
    done=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        jobs=[ex.submit(verify_file,db,r) for r in rows]
        for f in concurrent.futures.as_completed(jobs):
            done.append(f.result());print(f'verified {len(done)}/{len(rows)}',flush=True)
    result=dict(schema='chessdb-source-hashes-v1',manifest_sha256=hashlib.sha256(text).hexdigest(),files=sorted(done,key=lambda r:r['name']),downloader_sidecars=sidecars,seconds=time.monotonic()-start,checked_unix=time.time(),db=str(db))
    tmp=out.with_suffix('.tmp');out.parent.mkdir(parents=True,exist_ok=True)
    with tmp.open('w') as f:json.dump(result,f,indent=2);f.flush();os.fsync(f.fileno())
    tmp.replace(out)
if __name__=='__main__':main()
