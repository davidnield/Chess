"""Read-only completion audit; reuse collection hashes, do not rehash bulk payloads."""
from pathlib import Path
import datetime as dt
import hashlib
import json
import pyarrow.parquet as pq

base = Path('D:/chess/mini-pool-20261123')
original = {5, 28, 109, 146, 295, 346, 396, 509}
recipe = '3e8eaf78e5131adba3bdebcb16359a47dff2b061b665ce85fa770dd249a3ec41'
runtime = 'a83f5bdaedc2a1726335f30929f16dbafb1dc411c79ad255c065df4c6de13e53'
def read(p): return json.loads(p.read_text(encoding='utf-8'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()

status = read(base/'queue/status.json')
assert status['state'] == 'bucket-phase-collected'
assert status['completed'] == 504 and status['original_buckets'] == 8
assert status['full_pool_published'] is False
pilot = read(base/'pilot-accepted.json')
assert pilot['identity']['recipe'] == recipe
assert all(pilot['tables'][k]['differences'] == [0, 0] for k in ('edges', 'aux'))
records = []
for i in range(512):
    record = {'bucket': i, 'original': i in original}
    if i in original:
        folder = Path('D:/chess/sweep-inputs/brc1800-p30/work/buckets')
    else:
        state_path = base/f'queue/b{i:03d}.json'
        state = read(state_path)
        assert state['accepted'] and state['bucket'] == i
        dest = Path(state['collected_to'])
        assert dest == Path(f'H:/chess/mini-pools/brc1800-p30/production/b{i:03d}')
        complete_path = dest/'bucket-complete.json'
        complete = read(complete_path)
        assert sha(complete_path) == state['verification']['completion_sha256']
        expected = dict(bundle=state['bundle_sha256'], driver=state.get('worker_driver_sha', '2f0dcff8dd78dbac1fe406934628bc9954fd06b5231c87538fac0b9fce098f9b'), runtime=runtime, recipe=recipe, bucket=i, threads=4, memory='16GB')
        if state.get('optimizer_sha'): expected['optimizer'] = state['optimizer_sha']
        assert complete['identity'] == expected
        assert complete['files'] == state['verification']['files']
        assert set(complete['files']) == {f'work/buckets/b{i:03d}.{s}' for s in ('edges.parquet', 'aux.parquet', 'stats.json')}
        for rel, meta in complete['files'].items():
            assert (dest/rel).stat().st_size == meta['bytes']
        folder = dest/'work/buckets'
        assert sha(folder/f'b{i:03d}.stats.json') == complete['files'][f'work/buckets/b{i:03d}.stats.json']['sha256']
        record.update(state_sha256=sha(state_path), completion_sha256=sha(complete_path))
    stats = read(folder/f'b{i:03d}.stats.json')
    assert stats['bucket'] == i
    assert all(stats[k] == 0 for k in ('other_children', 'other_edges', 'multi_valued', 'coll_surviving'))
    if i not in original:
        assert complete['stats'] == stats == state['verification']['stats']
    for kind, count in [('edges', 'survivors'), ('aux', 'aux_rows')]:
        path = folder/f'b{i:03d}.{kind}.parquet'
        assert pq.read_metadata(path).num_rows == stats[count]
        record[kind+'_rows'] = stats[count]
        record[kind+'_bytes'] = path.stat().st_size
    records.append(record)

result = dict(utc=dt.datetime.now(dt.timezone.utc).isoformat(), completed_utc=status['utc'], buckets=512,
              collected_buckets=504, original_buckets=8, collection_receipt_bindings_passed=True,
              all_output_sizes_and_footer_counts_passed=True, no_other_contract_passed=True,
              payload_hashes='504 collection-time hashes retained and receipt-bound; no new full payload rehash; eight original files checked by existence/footer/contract',
              full_pool_published=False, edges_rows=sum(r['edges_rows'] for r in records),
              aux_rows=sum(r['aux_rows'] for r in records),
              parquet_bytes=sum(r['edges_bytes']+r['aux_bytes'] for r in records), records=records)
out = Path(__file__).with_name('verification.json')
assert not out.exists(), 'Keep the existing audit receipt'
out.write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps({k:v for k,v in result.items() if k!='records'},indent=2))
