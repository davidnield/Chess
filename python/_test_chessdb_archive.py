"""Meaningful failure/round-trip tests for the maintained archive coordinator.
python _test_chessdb_archive.py --release DIR --work NEW_DIR
"""
import argparse,copy,hashlib,json,os,struct,subprocess,sys
from pathlib import Path
def main():
    p=argparse.ArgumentParser();p.add_argument('--release',type=Path,required=True);p.add_argument('--work',type=Path,required=True);a=p.parse_args();a.work.mkdir(parents=True,exist_ok=False)
    sys.path.insert(0,str(a.release));import chessdb_archive as ar;import chessdb_archive_source as source
    tests=[]
    def rejects(name,fn):
        try:fn()
        except (ValueError,RuntimeError,FileNotFoundError):tests.append(name);return
        raise AssertionError('accepted '+name)
    data=a.work/'source.bin';data.write_bytes(b'abc');row=('source.bin',3,hashlib.sha256(b'abc').hexdigest());source.verify_file(a.work,row);st=data.stat();data.write_bytes(b'xyz');os.utime(data,ns=(st.st_atime_ns,st.st_mtime_ns));rejects('same-size same-mtime source corruption',lambda:source.verify_file(a.work,row))
    plan=dict(id=0,start='01',end='05');base=dict(records=2,first_key_hex='01',last_key_hex='02',next_key_hex='03',range_done=False,source_logical_sha256='a',stream_sha256='b')
    r=dict(identity='id',range=0,index=0,start='01',end='05',source=base,audit={k:base[k] for k in ar.CHECK_FIELDS})
    assert ar.verify_chain(plan,[r],'id')==('03',False,2);tests.append('valid checkpoint continuation')
    for name,change in [('wrong source identity',lambda x:x.update(identity='other')),('gap in chunk sequence',lambda x:x.update(index=1)),('wrong lower bound',lambda x:x.update(start='02')),('range overrun',lambda x:x['source'].update(last_key_hex='05')),('bad continuation',lambda x:x['source'].update(next_key_hex='02')),('inconsistent terminal marker',lambda x:x['source'].update(range_done=True)),('changed audit',lambda x:x['audit'].update(stream_sha256='wrong'))]:
        x=copy.deepcopy(r);change(x);rejects(name,lambda:ar.verify_chain(plan,[x],'id'))
    rejects('duplicate chunk',lambda:ar.verify_chain(plan,[r,r],'id'))
    frame=a.work/'fixture.bin';rows=[(b'a\0',b'\xff\0',b'',b'',b''),(b'h\xff',b'opaque',b'8/8/8/8/8/8/8/8 w - -',b'a1a2:-30001;a0a0:-1;',b'7')]
    raw=hashlib.sha256();stream=hashlib.sha256()
    with frame.open('wb') as f:
        f.write(b'CDBARC02')
        for row in rows:
            for i,v in enumerate(row):
                b=struct.pack('<I',len(v))+v;f.write(b);stream.update(b)
                if i<2:raw.update(b)
    exe=str(a.release/'chessdb-archive.exe');out=a.work/'fixture.parquet'
    written=ar.run_json([exe,'write',str(frame),str(out),'9','10']);audit=ar.run_json([exe,'audit',str(out)])
    for result in [written,audit]:assert result['records']==2 and result['source_logical_sha256']==raw.hexdigest() and result['stream_sha256']==stream.hexdigest()
    assert ar.unbuffered_sha(out)==audit['file_sha256'];tests.append('native round trip, binary/unknown records, both ply fields, unaligned EOF')
    trunc=a.work/'truncated.bin';trunc.write_bytes(frame.read_bytes()[:-1])
    for name,input_,cap in [('truncated frame',trunc,10),('excess rows',frame,1)]:
        output=a.work/(name.replace(' ','-')+'.parquet');q=subprocess.run([exe,'write',str(input_),str(output),'9',str(cap)],capture_output=True,text=True)
        assert q.returncode and not output.exists();tests.append(name+' not published')
    changed=a.work/'changed.parquet';changed.write_bytes(out.read_bytes());b=bytearray(changed.read_bytes());b[len(b)//2]^=1;changed.write_bytes(b)
    assert ar.unbuffered_sha(changed)!=audit['file_sha256'];tests.append('changed output checksum detected')
    unexpected=a.work/'not-an-attempt';unexpected.mkdir();rejects('unsafe cleanup rejected',lambda:ar.clean_attempt(unexpected,a.work))
    result=dict(passed=len(tests),tests=tests);(a.work/'results.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
