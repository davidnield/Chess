"""Lossless ChessDB -> F: archive coordinator; run only from a frozen release.

python chessdb_archive.py --out F:/chess/chessdb-archive-20260702-v1
  --spool D:/chess/chessdb-archive-spool-v1 [--workers 4 --chunk-rows 20000000]
  [--max-new-chunks N] [--stop-file PATH]

Source hashes and completed archive bytes are rechecked on restart. A stop file or Ctrl+C
stops after active shards finish. Original sources are never deleted. Completion requires
all ranges, the exact source count, and a last-written ARCHIVE.COMPLETE.json receipt.
Test mode: --test-prefix-rows N --source-verified PATH (fresh preflight receipt required).
Test archives are permanently labeled bounded and cannot be published as full snapshots.
"""
import argparse,concurrent.futures as cf,datetime as dt,hashlib,json,math,os,re,shutil,stat,subprocess,sys,threading,time,uuid
from pathlib import Path
from month_archive import file_lock,read_json,write_json,sha,disjoint
from chessdb_archive_windows import unbuffered_sha,kill_children_on_exit

HERE=Path(__file__).resolve().parent
DB='/mnt/chessdb-ro'
EXPECTED=63996865341
CHECK_FIELDS=('records','first_key_hex','last_key_hex','source_logical_sha256','stream_sha256')
SOURCE_MANIFEST=Path('H:/chess/chessdb/manifest.tsv')

def wslpath(p):
    s=Path(p).resolve().as_posix();return '/mnt/'+s[0].lower()+s[2:]
def run_json(cmd):
    p=subprocess.run(cmd,capture_output=True,text=True)
    if p.returncode:raise RuntimeError(f'command failed ({p.returncode}): {p.stderr[-3000:]}')
    return json.loads(p.stdout.strip())
def validate_inventory(inv):
    starts=inv['starts']
    if not starts or starts!=sorted(set(starts)) or starts[0]!=inv['first_key']:raise ValueError('invalid source partition boundaries')
    if inv['estimated_records']!=EXPECTED or inv['comparator']!='leveldb.BytewiseComparator':raise ValueError('unexpected source snapshot')
    if starts[-1]>inv['last_key']:raise ValueError('range beyond source')
    for k in starts+[inv['last_key']]:bytes.fromhex(k)
def verify_chain(plan,recs,identity):
    """Validate exact monotone coverage of a single source range, returning its next cursor."""
    cursor=plan['start'];done=False;rows=0
    for i,r in enumerate(recs):
        if done or r['identity']!=identity or r['index']!=i or r['range']!=plan['id'] or r['start']!=cursor or r['end']!=plan['end']:raise ValueError('broken checkpoint chain')
        s=r['source'];first=s['first_key_hex'];last=s['last_key_hex'];n=s['next_key_hex']
        if s['records']<=0 or first!=cursor or last<first or (plan['end'] is not None and last>=plan['end']):raise ValueError('invalid shard coverage')
        if s['range_done']!=(n is None) or (n is not None and (n<=last or (plan['end'] is not None and n>=plan['end']))):raise ValueError('invalid continuation')
        if any(s[k]!=r['audit'][k] for k in CHECK_FIELDS):raise ValueError('audit/source mismatch in receipt')
        cursor=n;done=s['range_done'];rows+=s['records']
    return cursor,done,rows
def producer():
    return {n:sha(HERE/n) for n in ('chessdb_archive.py','chessdb_archive_windows.py','chessdb_archive_source.py','month_archive.py','mount_archive.sh','cdb_archive','chessdb-archive.exe','reader-provenance.txt','release-source.zip','decoder-source.tar.gz','release.json')}
def check_source_stamps(path):
    p=subprocess.run(['wsl','-d','Ubuntu','--','python3',wslpath(HERE/'chessdb_archive_source.py'),'--check-stamps',wslpath(path)],capture_output=True,text=True)
    if p.returncode:raise ValueError('source changed since byte verification: '+p.stderr)
def source_now(manifest,out,reuse=None):
    if reuse:
        r=read_json(reuse)
        if time.time()-r['checked_unix']>4*3600:raise ValueError('test source receipt older than four hours')
    else:
        log=out/'source-verification.log'
        with log.open('w') as f:
            p=subprocess.run(['wsl','-d','Ubuntu','--','python3',wslpath(HERE/'chessdb_archive_source.py'),DB,wslpath(manifest),wslpath(out/'source-verified.json')],stdout=f,stderr=subprocess.STDOUT)
        if p.returncode:raise RuntimeError(f'source hashes failed; see {log}')
        r=read_json(out/'source-verified.json')
    if r['schema']!='chessdb-source-hashes-v1' or r['manifest_sha256']!=sha(manifest) or r['db']!=DB:raise ValueError('wrong source receipt')
    # A reusable test receipt must still refer to the very same immutable source files.
    path=reuse if reuse else out/'source-verified.json'
    check_source_stamps(path)
    write_json(out/'source-verified.json',r);return r

def preserve_provenance(out,source):
    root=out/'provenance';root.mkdir(exist_ok=True)
    metadata=root/'source-metadata';metadata.mkdir(exist_ok=True)
    for row in source['files']:
        if row['name'].endswith('.sst'):continue
        data=(SOURCE_MANIFEST.parent/'chess-20260702/data'/row['name']).read_bytes()
        h=hashlib.sha256(data).hexdigest() if len(row['sha'])==64 else hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()
        if len(data)!=row['bytes'] or h!=row['sha']:raise ValueError('original metadata differs from verified snapshot')
        dest=metadata/row['name']
        if dest.exists():
            if dest.read_bytes()!=data:raise ValueError('archived source metadata changed')
        else:
            with dest.open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
    shutil.copyfile(SOURCE_MANIFEST,root/'source-manifest.tsv')
    for name in ('release-source.zip','decoder-source.tar.gz','release.json','chessdb-archive.exe'):
        src=HERE/name
        if not src.exists():raise ValueError('release provenance package missing: '+name)
        dest=root/name
        if dest.exists():
            if sha(dest)!=sha(src):raise ValueError('archived producer package changed')
        else:shutil.copyfile(src,dest)
    write_json(root/'manifest.json',{p.relative_to(root).as_posix():sha(p) for p in root.rglob('*') if p.is_file() and p.name!='manifest.json'})

def staged_bytes(root):
    total=0
    for p in root.glob('r*-c*-*/*'):
        # Another worker can finish and remove its owned attempt after globbing.
        # Count each remaining file from a single stat; only disappearance is benign.
        try:s=p.stat()
        except FileNotFoundError:continue
        if stat.S_ISREG(s.st_mode):total+=s.st_size
    return total

class Coordinator:
    def __init__(self,a,inv,identity):
        self.a=a;self.out=a.out;self.spool=a.spool;self.inv=inv;self.identity=identity;self.stop=threading.Event();self.mutex=threading.Lock();self.copy_lock=threading.Lock();self.issued=0;self.finished=0
    def status(self,state,**kw):
        with self.mutex:write_json(self.out/'status.json',dict(state=state,pid=os.getpid(),new_completed=self.finished,**kw))
    def stopping(self):return self.stop.is_set() or (self.a.stop_file and self.a.stop_file.exists())
    def capacity(self):
        if shutil.disk_usage(self.out).free<self.a.archive_reserve_gb*1e9:raise RuntimeError('archive free-space reserve reached')
        if shutil.disk_usage(self.spool).free<self.a.spool_reserve_gb*1e9:raise RuntimeError('spool drive free-space reserve reached')
        used=staged_bytes(self.spool)
        if used>self.a.spool_limit_gb*1e9:raise RuntimeError('bounded spool limit reached')
    def take(self):
        with self.mutex:
            if self.stopping() or (self.a.max_new_chunks and self.issued>=self.a.max_new_chunks):return False
            self.issued+=1;return True
    def checkpoint_paths(self,rid):return sorted((self.out/'receipts'/f'r{rid:03d}').glob('c*.json'))
    def existing(self,plan):
        recs=[]
        for i,p in enumerate(self.checkpoint_paths(plan['id'])):
            if p.name!=f'c{i:06d}.json':raise ValueError('missing/noncontiguous checkpoint')
            r=read_json(p);expected=f'parquet/r{plan["id"]:03d}/c{i:06d}.parquet'
            if r['file']!=expected:raise ValueError('unexpected checkpoint file path')
            target=self.out/expected
            if target.stat().st_size!=r['audit']['bytes'] or unbuffered_sha(target)!=r['audit']['file_sha256']:raise ValueError('completed archive bytes changed')
            recs.append(r)
        verify_chain(plan,recs,self.identity);return recs
    def chunk(self,plan,index,start):
        check_source_stamps(self.out/'source-verified.json')
        self.capacity();attempt=self.spool/f'r{plan["id"]:03d}-c{index:06d}-{uuid.uuid4().hex}';attempt.mkdir()
        local=attempt/'data.parquet';log=attempt/'source.log';active=self.out/'active'/f'r{plan["id"]:03d}.json'
        write_json(active,dict(range=plan['id'],index=index,start=start,attempt=str(attempt),state='exporting'))
        t=time.monotonic();reader=None;writer=None
        try:
            with log.open('wb') as err:
                reader=subprocess.Popen(['wsl','-d','Ubuntu','--',wslpath(HERE/'cdb_archive'),'chunk',DB,start,plan['end'] or '-',str(self.a.chunk_rows)],stdout=subprocess.PIPE,stderr=err)
                writer=subprocess.Popen([str(HERE/'chessdb-archive.exe'),'write','-',str(local),str(self.a.zstd),str(self.a.chunk_rows)],stdin=reader.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                reader.stdout.close()
                while writer.poll() is None:
                    self.capacity();time.sleep(.5)
                stdout,stderr=writer.communicate();rc=reader.wait(timeout=30)
                if writer.returncode or rc:raise RuntimeError(f'export failed: reader={rc}, writer={writer.returncode}: {stderr.decode(errors="replace")} {log.read_text()[-2000:]}')
            source=json.loads(log.read_text().strip().splitlines()[-1]);written=json.loads(stdout)
            if any(source[k]!=written[k] for k in CHECK_FIELDS):raise ValueError('source/transport mismatch')
            if not 0<source['records']<=self.a.chunk_rows:raise ValueError('invalid source shard count')
            audit=run_json([str(HERE/'chessdb-archive.exe'),'audit',str(local)])
            if any(source[k]!=audit[k] for k in CHECK_FIELDS):raise ValueError('source/Parquet mismatch')
            rec=dict(identity=self.identity,range=plan['id'],index=index,start=start,end=plan['end'],source=source,audit=audit,file=f'parquet/r{plan["id"]:03d}/c{index:06d}.parquet')
            # Validate this continuation before publishing even one receipt.
            verify_chain(dict(plan,start=start),[dict(rec,index=0)],self.identity)
            target=self.out/rec['file'];target.parent.mkdir(parents=True,exist_ok=True)
            with self.copy_lock:
                self.capacity();part=self.out/'partial'/f'{attempt.name}.part';part.parent.mkdir(exist_ok=True)
                with local.open('rb') as src,part.open('xb') as dst:
                    for b in iter(lambda:src.read(16<<20),b''):
                        if shutil.disk_usage(self.out).free<self.a.archive_reserve_gb*1e9+len(b):raise RuntimeError('archive reserve would be crossed')
                        dst.write(b)
                    dst.flush();os.fsync(dst.fileno())
                if unbuffered_sha(part)!=audit['file_sha256']:raise ValueError('archive read-back differs from source output')
                if target.exists():
                    if unbuffered_sha(target)!=audit['file_sha256']:raise ValueError('unreceipted archive file disagrees; investigate')
                    part.unlink()
                else:part.replace(target)
                rec['seconds']=time.monotonic()-t;rec['verified_at']=dt.datetime.now(dt.timezone.utc).isoformat()
                write_json(self.out/'receipts'/f'r{plan["id"]:03d}'/f'c{index:06d}.json',rec)
            with self.mutex:self.finished+=1
            self.status('exporting')
            print(f'range {plan["id"]} chunk {index}: {source["records"]:,} verified rows, {rec["seconds"]:.1f}s',flush=True)
            active.unlink(missing_ok=True);clean_attempt(attempt,self.spool);return rec
        finally:
            for p in (writer,reader):
                if p is not None and p.poll() is None:p.kill();p.wait()
    def range(self,plan):
        try:
            if self.stopping():return []
            recs=self.existing(plan);cursor,done,_=verify_chain(plan,recs,self.identity)
            while not done and self.take():
                rec=self.chunk(plan,len(recs),cursor);recs.append(rec);cursor,done,_=verify_chain(plan,recs,self.identity)
            return recs
        except BaseException:self.stop.set();raise

def clean_attempt(path,root):
    path=path.resolve();root=root.resolve()
    if path.parent!=root or not re.fullmatch(r'r\d{3}-c\d{6}-[0-9a-f]{32}',path.name) or path.is_symlink():raise ValueError('unsafe attempt cleanup')
    allowed={'data.parquet','data.parquet.tmp','data.parquet.receipt.json','source.log'}
    children=list(path.iterdir())
    if any(p.name not in allowed or not p.is_file() or p.is_symlink() for p in children):raise ValueError('unexpected staging content')
    for p in children:p.unlink()
    path.rmdir()
def clean_orphans(out,spool):
    for p in spool.glob('r*-c*-*'):clean_attempt(p,spool)
    for p in (out/'partial').glob('*.part'):
        if p.resolve().parent!=(out/'partial').resolve() or p.is_symlink() or not re.fullmatch(r'r\d{3}-c\d{6}-[0-9a-f]{32}\.part',p.name):raise ValueError('unsafe partial cleanup')
        p.unlink()

def main():
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--spool',type=Path,required=True);p.add_argument('--workers',type=int,default=4);p.add_argument('--chunk-rows',type=int,default=20000000);p.add_argument('--zstd',type=int,default=9)
    p.add_argument('--archive-reserve-gb',type=float,default=500);p.add_argument('--spool-reserve-gb',type=float,default=250);p.add_argument('--spool-limit-gb',type=float,default=24)
    p.add_argument('--max-new-chunks',type=int);p.add_argument('--stop-file',type=Path);p.add_argument('--test-prefix-rows',type=int);p.add_argument('--source-verified',type=Path);a=p.parse_args()
    if a.source_verified and not a.test_prefix_rows:raise ValueError('production cannot bypass fresh source hashing')
    if not 1<=a.workers<=8 or not 0<a.chunk_rows<=50000000 or not 1<=a.zstd<=22 or any(not math.isfinite(x) or x<=0 for x in (a.archive_reserve_gb,a.spool_reserve_gb,a.spool_limit_gb)):raise ValueError('invalid limits')
    if (a.test_prefix_rows is not None and a.test_prefix_rows<=0) or (a.max_new_chunks is not None and a.max_new_chunks<=0):raise ValueError('invalid bounded run length')
    a.out=a.out.resolve();a.spool=a.spool.resolve();disjoint(a.out,a.spool,SOURCE_MANIFEST.parent,HERE)
    a.out.mkdir(parents=True,exist_ok=True);a.spool.mkdir(parents=True,exist_ok=True)
    global JOB_HANDLE;JOB_HANDLE=kill_children_on_exit()
    with file_lock(a.out/'coordinator.lock'),file_lock(a.spool/'coordinator.lock'):
        prior=a.out/'identity.json'
        if prior.exists():
            old=read_json(prior)
            proposed=dict(producer=producer(),source_manifest_sha256=sha(SOURCE_MANIFEST),chunk_rows=a.chunk_rows,zstd=a.zstd,bounded=bool(a.test_prefix_rows),expected_records=a.test_prefix_rows or EXPECTED)
            if any(old[k]!=v for k,v in proposed.items()):raise ValueError('source/code/schema/settings changed; use a new archive')
        global CURRENT_OUT;CURRENT_OUT=a.out
        if a.stop_file and a.stop_file.exists():
            write_json(a.out/'status.json',dict(state='stopped-before-start',pid=os.getpid()));return
        existing_binding=a.spool/'binding.json'
        if existing_binding.exists() and read_json(existing_binding)!={'out':str(a.out)}:raise ValueError('spool belongs to another archive')
        write_json(existing_binding,{'out':str(a.out)})
        # Read-only mount recreation after reboot, source byte verification, then complete partition inventory.
        subprocess.run(['wsl','-d','Ubuntu','--','bash',wslpath(HERE/'mount_archive.sh')],check=True)
        write_json(a.out/'status.json',dict(state='source-verification',pid=os.getpid()))
        source=source_now(SOURCE_MANIFEST,a.out,a.source_verified)
        inv=run_json(['wsl','-d','Ubuntu','--',wslpath(HERE/'cdb_archive'),'inventory',DB]);validate_inventory(inv)
        plans=[dict(id=i,start=s,end=inv['starts'][i+1] if i+1<len(inv['starts']) else None) for i,s in enumerate(inv['starts'])];expected=EXPECTED
        if a.test_prefix_rows:
            q=subprocess.run(['wsl','-d','Ubuntu','--',wslpath(HERE/'cdb_archive'),'chunk',DB,inv['first_key'],'-',str(a.test_prefix_rows)],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
            if q.returncode:raise RuntimeError(q.stderr)
            bound=json.loads(q.stderr.strip().splitlines()[-1]);expected=a.test_prefix_rows
            if bound['records']!=expected:raise ValueError('bounded source size mismatch')
            plans=[dict(id=0,start=inv['first_key'],end=bound['next_key_hex'])]
        identity_obj=dict(schema='chessdb-lossless-parquet-v1',producer=producer(),source_manifest_sha256=source['manifest_sha256'],inventory=inv,plans=plans,expected_records=expected,chunk_rows=a.chunk_rows,zstd=a.zstd,bounded=bool(a.test_prefix_rows))
        identity_path=a.out/'identity.json'
        if identity_path.exists() and read_json(identity_path)!=identity_obj:raise ValueError('source/code/schema/settings changed; use a new archive')
        if not identity_path.exists() and any((a.out/'receipts').glob('r*/c*.json')):raise ValueError('receipts without identity')
        write_json(identity_path,identity_obj);identity=sha(identity_path);clean_orphans(a.out,a.spool)
        for d in ('active','partial','receipts','provenance'):(a.out/d).mkdir(exist_ok=True)
        allowed={f'r{x["id"]:03d}' for x in plans}
        for rp in (a.out/'receipts').rglob('*.json'):
            if rp.parent.parent!=a.out/'receipts' or rp.parent.name not in allowed or not re.fullmatch(r'c\d{6}\.json',rp.name):raise ValueError('unexpected checkpoint file')
        preserve_provenance(a.out,source)
        c=Coordinator(a,inv,identity);c.status('exporting',ranges=len(plans));results=[]
        try:
            with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
                futures=[ex.submit(c.range,plan) for plan in plans]
                try:
                    for f in cf.as_completed(futures):results.append(f.result())
                except KeyboardInterrupt:c.stop.set();print('Stop requested; finishing active shards.',flush=True);results=[f.result() for f in futures]
            total=sum(r['source']['records'] for rs in results for r in rs);complete=all(verify_chain(plan,sorted([r for rs in results for r in rs if r['range']==plan['id']],key=lambda r:r['index']),identity)[1] for plan in plans)
            if complete and not c.stopping():
                check_source_stamps(a.out/'source-verified.json')
                if producer()!=identity_obj['producer']:raise ValueError('frozen producer changed during export')
                if total!=expected:raise ValueError(f'final source count mismatch {total} != {expected}')
                rows=[r for rs in results for r in rs];files={r['file'] for r in rows}
                if {p.relative_to(a.out).as_posix() for p in (a.out/'parquet').rglob('*.parquet')}!=files:raise ValueError('unexpected archive files')
                expected_receipts={f'receipts/r{r["range"]:03d}/c{r["index"]:06d}.json' for r in rows}
                if {p.relative_to(a.out).as_posix() for p in (a.out/'receipts').rglob('*.json')}!=expected_receipts:raise ValueError('unexpected receipt set')
                marker='BOUNDED.TEST.COMPLETE.json' if a.test_prefix_rows else 'ARCHIVE.COMPLETE.json'
                write_json(a.out/marker,dict(identity=identity,records=total,shards=len(rows),parquet_bytes=sum(r['audit']['bytes'] for r in rows),receipts={p.relative_to(a.out).as_posix():sha(p) for p in (a.out/'receipts').rglob('*.json')},provenance_manifest_sha256=sha(a.out/'provenance/manifest.json'),bounded=bool(a.test_prefix_rows),sources_deleted=False))
                c.status('complete',records=total,shards=len(rows),bounded=bool(a.test_prefix_rows))
            else:c.status('stopped-at-checkpoint',records=total)
        except BaseException as e:c.status('failed',error=repr(e));raise
if __name__=='__main__':
    try:main()
    except BaseException as e:
        if globals().get('CURRENT_OUT') is not None:write_json(CURRENT_OUT/'status.json',dict(state='failed',error=repr(e),pid=os.getpid()))
        raise
