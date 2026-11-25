"""Independent SQL source-policy spot check and reader timing for a sparse overlay.

This verifies sampled actual source records, not the already-computed Rust output
against itself. It does not activate a lookup or estimate book-level performance.
"""
from pathlib import Path
import argparse
import time
import duckdb
import numpy as np
import pyarrow as pa
from eval_lookup128 import WideEvalLookup, keys, OVERLAY_ROW
from month_archive import read_json, sha, write_json


def verify(root, output, count=2048, buckets=None):
    root, output = Path(root), Path(output)
    meta_path = root/('lookup.json' if (root/'lookup.json').exists() else 'pilot.json')
    meta = read_json(meta_path)
    selected = set(meta['buckets']) if buckets is None else {f'bkt{b:03d}.npy' for b in buckets}
    if count < 4 or not selected or not selected <= set(meta['buckets']):
        raise ValueError('invalid sample or selected bucket set')
    config = meta['identity']['inputs']['roots']
    provider = WideEvalLookup(root, allow_partial=True)
    records = []
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute('SET threads=2')
    con.execute('SET temp_directory=?', [str(root/'verify-spill')])
    try:
        for name, entry in meta['buckets'].items():
            if name not in selected: continue
            bucket = int(name[3:6])
            arr = np.load(root/name, mmap_mode='r', allow_pickle=False)
            base = np.load(Path(config['base'])/name, mmap_mode='r', allow_pickle=False)
            if arr.dtype != OVERLAY_ROW or sha(root/name) != entry['sha256']:
                raise ValueError('invalid output array')
            rng = np.random.default_rng(20261125+bucket)
            samples = [a[rng.choice(len(a), min(count, len(a)), replace=False)] for a in (arr, base)]
            # Include each changed source and authoritative terminal rows explicitly.
            for code in (6, 7):
                i = np.flatnonzero(arr['source'] == code)
                if len(i): samples.append(arr[rng.choice(i, min(count//4, len(i)), replace=False)])
            i = np.flatnonzero(base['source'] == 3)
            if len(i): samples.append(base[rng.choice(i, min(count//4, len(i)), replace=False)])
            q = np.unique(np.concatenate([keys(a['lo'], a['hi']) for a in samples]))
            index = np.searchsorted(base[['lo', 'hi']], q)
            hit = index < len(base)
            hit[hit] = base[['lo', 'hi']][index[hit]] == q[hit]
            bc = np.full(len(q), np.nan); bs = np.zeros(len(q), np.uint8)
            bc[hit], bs[hit] = base['cp'][index[hit]], base['source'][index[hit]]
            con.register('q', pa.table(dict(lo=q['lo'], hi=q['hi'], base_cp=pa.array(bc, from_pandas=True), base_source=bs)))
            sql = '''WITH normalized AS (
                SELECT q.*,
                  CASE WHEN e.cloud_cp IS NOT NULL THEN greatest(-2000,least(2000,e.cloud_cp))
                       WHEN e.cloud_mate IS NOT NULL THEN sign(e.cloud_mate)*2000 END c,
                  CASE WHEN e.fishnet_cp IS NOT NULL THEN greatest(-2000,least(2000,e.fishnet_cp))
                       WHEN e.fishnet_mate IS NOT NULL THEN sign(e.fishnet_mate)*2000 END f,
                  CASE WHEN m.found AND m.best_score IS NOT NULL THEN
                     greatest(-2000,least(2000,m.best_score * CASE WHEN m.white_to_move THEN 1 ELSE -1 END)) END cb,
                  e.fishnet_tier
                FROM q LEFT JOIN read_parquet(?) e ON q.lo=e.position_hash AND q.hi=e.position_hash_hi
                LEFT JOIN read_parquet(?) m ON q.lo=m.lo AND q.hi=m.hi
            ), classified AS (
                SELECT *, CASE WHEN base_source=3 THEN 3
                    WHEN c IS NOT NULL AND f IS NOT NULL AND fishnet_tier='nnue' AND cb IS NOT NULL THEN 6
                    WHEN c IS NULL AND cb IS NOT NULL THEN 7 ELSE base_source END selected
                FROM normalized
            ) SELECT lo,hi,selected,CASE WHEN selected=6 THEN
                .3/(1+exp(-.00368208*c))+.3/(1+exp(-.00368208*f))+.4/(1+exp(-.00368208*cb))
                WHEN selected=7 THEN 1/(1+exp(-.00368208*cb))
                ELSE 1/(1+exp(-.00368208*base_cp)) END score
                FROM classified ORDER BY lo,hi'''
            expected = con.execute(sql, [str(Path(config['db'])/f'bkt{bucket:03d}.parquet'),
                str(Path(config['menus'])/'positions'/f'bkt{bucket:03d}.parquet')]).to_arrow_table()
            if len(expected) != len(q): raise ValueError('source duplicates')
            begin = time.monotonic()
            cp, src = provider.lookup(q['lo'], q['hi'])
            cold = time.monotonic()-begin
            begin = time.monotonic()
            for _ in range(5): provider.lookup(q['lo'], q['hi'])
            warm = (time.monotonic()-begin)/5
            begin = time.monotonic()
            for _ in range(5): provider.base.lookup(q['lo'], q['hi'])
            base_warm = (time.monotonic()-begin)/5
            score = 1/(1+np.exp(-.00368208*cp))
            want = expected['score'].to_numpy()
            error = float(np.nanmax(abs(want-score)))
            if not np.array_equal(src, expected['selected'].to_numpy()) or error > 1e-12:
                raise ValueError(f'independent real-source policy mismatch: bucket {bucket}, error {error}')
            records.append(dict(bucket=bucket, queried=len(q), max_score_error=error,
                source_counts={str(c): int((src == c).sum()) for c in np.unique(src)},
                cold_seconds=cold, warm_seconds=warm, base_warm_seconds=base_warm))
    finally:
        con.close()
    report = dict(schema='eval-policy-overlay-independent-check-v1', manifest=str(meta_path.resolve()),
        manifest_sha256=sha(meta_path), script_sha256=sha(Path(__file__)), records=records,
        selected_buckets=sorted(selected), manifest_buckets=len(meta['buckets']),
        sampled_positions=sum(r['queried'] for r in records), verified=True,
        production_binding_changed=False)
    write_json(output, report)
    return report


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--overlay', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--sample', type=int, default=2048)
    ap.add_argument('--buckets', type=int, nargs='+', help='bounded independent sample of a full generation')
    a = ap.parse_args()
    print(verify(a.overlay, a.out, a.sample, a.buckets))
