"""Explicit real-source destructive-interruption test, confined to newly created test directories.
--release PIN --work NEW_D_PATH --archive NEW_F_PATH --source-verified RECEIPT
All test archives remain labeled bounded, never as complete source snapshots.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys,time
from pathlib import Path
import psutil

def read(p):return json.loads(p.read_text(encoding='utf8'))
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def main():
    p=argparse.ArgumentParser();p.add_argument('--release',type=Path,required=True);p.add_argument('--work',type=Path,required=True);p.add_argument('--archive',type=Path,required=True);p.add_argument('--source-verified',type=Path,required=True);a=p.parse_args()
    a.work.mkdir(parents=True,exist_ok=False);a.archive.mkdir(parents=True,exist_ok=False)
    deadline=time.monotonic()+900
    while not a.source_verified.exists():
        if time.monotonic()>deadline:raise TimeoutError('source preflight not finished')
        time.sleep(2)
    def cmd(name,extra=()):
        return [sys.executable,'-B','-u',str(a.release/'chessdb_archive.py'),'--out',str(a.archive/name),'--spool',str(a.work/(name+'-spool')),'--workers','1','--chunk-rows','500000','--test-prefix-rows','2000000','--source-verified',str(a.source_verified),*extra]
    def run(name,label,extra=(),ok=True):
        with (a.work/(label+'.log')).open('wb') as log:r=subprocess.run(cmd(name,extra),stdout=log,stderr=subprocess.STDOUT,timeout=900)
        text=(a.work/(label+'.log')).read_text(errors='replace')
        if (r.returncode==0)!=ok:raise RuntimeError(label+': '+text[-4000:])
        return text
    def receipts(name):return sorted((a.archive/name/'receipts').glob('r*/c*.json'))
    run('baseline','baseline');assert (a.archive/'baseline/BOUNDED.TEST.COMPLETE.json').exists();print('uninterrupted bounded baseline complete',flush=True)
    with (a.work/'interrupted.log').open('wb') as log:
        process=subprocess.Popen(cmd('resumed'),stdout=log,stderr=subprocess.STDOUT)
        owner=None;checked_duplicate=False;deadline=time.monotonic()+600;old={};children=[]
        while time.monotonic()<deadline:
            if process.poll() is not None:raise RuntimeError('coordinator exited before interruption: '+(a.work/'interrupted.log').read_text()[-3000:])
            status=a.archive/'resumed/status.json'
            if status.exists():
                try:owner=read(status).get('pid',owner)
                except (json.JSONDecodeError,PermissionError):pass
            recs=receipts('resumed')
            if recs and not checked_duplicate:
                run('resumed','duplicate',ok=False);checked_duplicate=True
            active=a.archive/'resumed/active/r000.json'
            if len(recs)>=2 and active.exists():
                active_data=read(active);temp=Path(active_data['attempt'])/'data.parquet.tmp'
                if active_data['index']>=2 and temp.exists() and temp.stat().st_size>0:
                    for rp in recs:
                        r=read(rp);file=a.archive/'resumed'/r['file'];old[r['file']]=(sha(file),file.stat().st_mtime_ns)
                    current=psutil.Process(owner);children=[(x.pid,x.create_time()) for x in current.children(recursive=True)];current.kill();process.wait(timeout=30);break
            time.sleep(.03)
        else:raise TimeoutError('no in-progress shard found')
    assert len(old)>=2
    for _ in range(100):
        survivors=[]
        for pid,created in children:
            try:
                proc=psutil.Process(pid)
                if proc.create_time()==created:survivors.append(pid)
            except psutil.NoSuchProcess:pass
        if not survivors:break
        time.sleep(.1)
    assert not survivors,('orphaned Windows workers',survivors)
    print('killed coordinator during an active shard; completed checkpoints retained; child processes exited',flush=True)
    run('resumed','resumed-finish')
    for name,(digest,mtime) in old.items():
        f=a.archive/'resumed'/name;assert sha(f)==digest and f.stat().st_mtime_ns==mtime
    base={read(p)['file']:read(p)['audit']['file_sha256'] for p in receipts('baseline')}
    resumed={read(p)['file']:read(p)['audit']['file_sha256'] for p in receipts('resumed')}
    assert base==resumed and len(base)==4
    print('resumed archive matches uninterrupted archive byte-for-byte',flush=True)
    status=a.archive/'resumed/status.json';before=sha(status)
    run('resumed','changed-settings',['--zstd','3'],ok=False);assert sha(status)==before
    rp=receipts('resumed')[0];r=read(rp);target=a.archive/'resumed'/r['file'];st=target.stat()
    with target.open('r+b') as f:f.seek(32);value=f.read(1);f.seek(32);f.write(bytes([value[0]^1]));f.flush();os.fsync(f.fileno())
    os.utime(target,ns=(st.st_atime_ns,st.st_mtime_ns))
    text=run('resumed','corruption-check',ok=False);assert 'completed archive bytes changed' in text
    shutil.copyfile(a.archive/'baseline'/r['file'],target);assert sha(target)==r['audit']['file_sha256'];run('resumed','restored')
    free=shutil.disk_usage(a.archive).free/1e9
    text=run('reserve','space-reserve',['--archive-reserve-gb',str(free+1000)],ok=False);assert 'free-space reserve reached' in text
    assert not list((a.archive/'reserve/parquet').glob('r*/*.parquet'))
    assert not list(a.archive.glob('*/ARCHIVE.COMPLETE.json'))
    result=dict(passed=True,records_per_archive=2000000,shards_per_archive=4,completed_shards_retained=len(old),duplicate_coordinator_rejected=True,interruption_children_exited=True,resumed_equals_uninterrupted=True,changed_settings_rejected_without_status_mutation=True,same_size_same_mtime_corruption_rejected=True,corrupted_test_file_restored=True,free_space_reserve_enforced=True,bounded_only=True)
    (a.work/'RESULTS.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
if __name__=='__main__':main()
