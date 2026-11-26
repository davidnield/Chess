"""Native serial/parallel verification equivalence and failure tests (Windows)."""
import argparse, copy, hashlib, json, os, shutil, struct, sys
from pathlib import Path
from unittest.mock import patch


def main():
    p=argparse.ArgumentParser();p.add_argument('--release',type=Path,required=True);p.add_argument('--work',type=Path,required=True);a=p.parse_args()
    a.work.mkdir(parents=True,exist_ok=False);sys.path.insert(0,str(a.release))
    import chessdb_archive_verify as v
    from month_archive import write_json,sha
    root=a.work/'baseline';root.mkdir()
    identity=dict(bounded=True,expected_records=4,producer={'chessdb-archive.exe':sha(a.release/'chessdb-archive.exe')},inventory=dict(starts=['01'],first_key='01',last_key='ff',estimated_records=v.EXPECTED,comparator='leveldb.BytewiseComparator'),plans=[dict(id=0,start='01',end='05')])
    write_json(root/'identity.json',identity);idh=sha(root/'identity.json');receipts={};size=0
    for i in range(2):
        frame=a.work/f'frame{i}.bin'
        with frame.open('wb') as f:
            f.write(b'CDBARC02')
            for k in range(i*2+1,i*2+3):
                row=(bytes([k]),b'\xff\x00value',b'8/8/8/8/8/8/8/8 w - -',b'a1a2:-30001;a0a0:-1;',b'7')
                for value in row:f.write(struct.pack('<I',len(value))+value)
        name=f'parquet/r000/c{i:06d}.parquet';out=root/name;out.parent.mkdir(parents=True,exist_ok=True)
        v.run_json([str(a.release/'chessdb-archive.exe'),'write',str(frame),str(out),'9','2'])
        audit=v.run_json([str(a.release/'chessdb-archive.exe'),'audit',str(out)])
        source={k:audit[k] for k in v.CHECK_FIELDS};source.update(next_key_hex='03' if i==0 else None,range_done=i==1)
        r=dict(identity=idh,range=0,index=i,start='01' if i==0 else '03',end='05',source=source,audit=audit,file=name)
        rp=f'receipts/r000/c{i:06d}.json';write_json(root/rp,r);receipts[rp]=sha(root/rp);size+=audit['bytes']
    write_json(root/'provenance/manifest.json',{})
    marker=dict(identity=idh,bounded=True,records=4,shards=2,parquet_bytes=size,receipts=receipts,provenance_manifest_sha256=sha(root/'provenance/manifest.json'))
    write_json(root/'BOUNDED.TEST.COMPLETE.json',marker)
    def verify(path,**kw):
        settings=dict(workers=2,scratch=a.work/(path.name+'-scratch'),reserve_gb=.001,staging_gb=.01)
        settings.update(kw);return v.verify(path,True,True,**settings)
    serial=v.verify(root,True,True);parallel=verify(root)
    assert {k:x for k,x in serial.items() if k!='workers'}=={k:x for k,x in parallel.items() if k!='workers'}
    tests=['serial/parallel native verification identical; both distance fields and arbitrary raw values retained']
    assert not list((a.work/'baseline-scratch').glob('audit-*'))
    tests.append('successful invocation removes only its empty staging directory')
    def reject(label,fn):
        try:fn()
        except (ValueError,RuntimeError,FileNotFoundError,FileExistsError):tests.append(label);return
        raise AssertionError('accepted '+label)
    def cloned(label):
        out=a.work/label;shutil.copytree(root,out);return out
    bad=cloned('corrupt');f=bad/'parquet/r000/c000000.parquet';st=f.stat();b=bytearray(f.read_bytes());b[len(b)//2]^=1;f.write_bytes(b);os.utime(f,ns=(st.st_atime_ns,st.st_mtime_ns));reject('same-size same-mtime corruption rejected',lambda:verify(bad))
    bad=cloned('missing');(bad/'parquet/r000/c000000.parquet').unlink();reject('missing file rejected',lambda:verify(bad))
    bad=cloned('extra');(bad/'parquet/extra.parquet').write_bytes(b'x');reject('extra file rejected',lambda:verify(bad))
    bad=cloned('receipt');rp=bad/'receipts/r000/c000000.json';r=json.loads(rp.read_text());r['source']['records']=3;write_json(rp,r);reject('receipt mutation rejected',lambda:verify(bad))
    bad=cloned('gap');rp=bad/'receipts/r000/c000001.json';r=json.loads(rp.read_text());r['start']='04';write_json(rp,r);m=copy.deepcopy(marker);m['receipts'][rp.relative_to(bad).as_posix()]=sha(rp);write_json(bad/'BOUNDED.TEST.COMPLETE.json',m);reject('coverage gap rejected despite updated receipt checksum',lambda:verify(bad))
    reject('staging reserve rejected',lambda:verify(root,reserve_gb=1e9))
    reject('staging cap rejected',lambda:verify(root,staging_gb=1e-9))
    reject('overlapping scratch rejected',lambda:verify(root,scratch=root/'scratch'))
    reject('invalid workers rejected',lambda:verify(root,workers=0))
    with patch.object(v,'audit_file',side_effect=RuntimeError('injected native failure')):
        reject('native worker failure propagates',lambda:verify(root))
    assert list((a.work/'baseline-scratch').glob('audit-*/*.parquet'))
    tests.append('failed audit copies retained for inspection')
    target=a.work/'copy.bin';target.write_bytes(b'preserve')
    reject('existing staging output never overwritten',lambda:v.unbuffered_sha(root/'parquet/r000/c000000.parquet',copy_to=target))
    assert target.read_bytes()==b'preserve'
    write_json(a.work/'results.json',dict(passed=len(tests),tests=tests));print(json.dumps(dict(passed=len(tests),tests=tests)))


if __name__=='__main__':
    # run_tests.py runs every _test_*.py with no arguments: then the release comes from CHESSDB_ARCHIVE_RELEASE
    # (a release dir holding chessdb-archive.exe and these modules) and the work dir is a throwaway one.
    if len(sys.argv)==1:
        import shutil,tempfile
        release=os.environ.get('CHESSDB_ARCHIVE_RELEASE')
        if not release:
            print('SKIP  release-pinned harness: pass --release DIR --work NEW_DIR, or set CHESSDB_ARCHIVE_RELEASE');sys.exit(0)
        tmp=tempfile.mkdtemp(prefix='chessdb_archive_test_')
        sys.argv+=['--release',release,'--work',str(Path(tmp)/'work')]
        try:main()
        finally:shutil.rmtree(tmp,ignore_errors=True)
    else:main()
