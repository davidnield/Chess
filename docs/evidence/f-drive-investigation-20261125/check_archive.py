"""Read-only archive check: pinned inventory, eight whole shards and query replay."""
import hashlib,json,subprocess,time
from pathlib import Path

out=Path(__file__).resolve().parents[2]/'docs/evidence/f-drive-investigation-20261125'
out.mkdir(parents=True,exist_ok=True)
release=Path('D:/chess/bin/chessdb-archive-query-20261125-v1')
reference=Path('D:/chess/acceptance/chessdb-archive-query-20261125/rust-first.json')

def sha(p):
    with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def save(name,data):
    (out/name).write_text(json.dumps(data,indent=2)+'\n',encoding='utf8')

t=time.perf_counter();report={'scope':'complete file inventory, eight sampled full-file hashes, fixed query replay','passed':False}
try:
    binding=json.loads((release/'binding.json').read_text())
    assert sha(release/'index.json')==binding['index_sha256']
    assert sha(release/'chessdb-archive-query.exe')==binding['executable_sha256']
    index=json.loads((release/'index.json').read_text());root=Path(index['archive'])
    assert sha(root/'ARCHIVE.COMPLETE.json')==binding['marker_sha256']
    report['marker_sha256']=binding['marker_sha256']
    report['inventory_files']=0
    for record in index['files']:
        p=root/record['file'];st=p.stat()
        if st.st_size!=record['bytes'] or st.st_mtime_ns!=record['mtime_ns']:
            raise ValueError(f'inventory changed: {record["file"]}')
        if sha(root/record['receipt'])!=record['receipt_sha256']:
            raise ValueError(f'receipt changed: {record["receipt"]}')
        report['inventory_files']+=1
    report['inventory_seconds']=time.perf_counter()-t
    save('archive-check-progress.json',report)
    report['sample']=[]
    for i in [round(j*(len(index['files'])-1)/7) for j in range(8)]:
        record=index['files'][i];start=time.perf_counter();actual=sha(root/record['file'])
        item=dict(file=record['file'],bytes=record['bytes'],sha256=actual,matches=actual==record['file_sha256'],seconds=time.perf_counter()-start)
        report['sample'].append(item);save('archive-check-progress.json',report)
        if not item['matches']:raise ValueError('sample checksum differs: '+record['file'])
    baseline=json.loads(reference.read_text());inp=out/'queries.jsonl'
    inp.write_text(''.join(json.dumps(dict(fen=r['fen'],id=r['id']))+'\n' for r in baseline['results']),encoding='utf8')
    result=out/'query-results.json';start=time.perf_counter()
    child=subprocess.run([str(release/'chessdb-archive-query.exe'),'query',str(release/'index.json'),str(inp),str(result)],capture_output=True,text=True)
    (out/'query.stderr.log').write_text(child.stderr,encoding='utf8')
    child.check_returncode();got=json.loads(result.read_text())
    assert got['results']==baseline['results'],'point queries differ from accepted reference'
    report.update(passed=True,query_count=len(got['results']),query_seconds=time.perf_counter()-start,query_sha256=sha(result),query_reference_sha256=sha(reference))
except Exception as e:
    report['error']=repr(e)
    raise
finally:
    report['seconds']=time.perf_counter()-t;save('archive-check.json',report)
    print(json.dumps({k:v for k,v in report.items() if k!='sample'},indent=2))
