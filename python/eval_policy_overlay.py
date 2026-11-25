"""Build a hash-bound sparse evaluation overlay; scoring is exclusively Rust.

Python orchestrates Arrow/DuckDB I/O and receipts. No source or fallback arrays
are changed. Partial builds publish pilot.json only, never a production lookup.
Full publication requires all 512 verified buckets in the same invocation.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import shutil
import time
import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import book_builder_rs as native
from chessdb_integrity import fingerprint, unchanged
import chessdb_lookup_run as inputs
from eval_arrays import MISSING
from eval_lookup128 import POLICY, OVERLAY_SCHEMA, OVERLAY_ROW, ROW, keys
from eval_lookup_cdb import strictly_sorted
from month_archive import file_lock, read_json, sha, write_json


def producer():
    here = Path(__file__).parent
    if native.EVAL_SOURCE_POLICY != POLICY:
        raise ValueError('wrong native policy version')
    return dict(python={n: sha(here/n) for n in ('eval_policy_overlay.py', 'eval_lookup128.py',
        'eval_lookup_cdb.py', 'chessdb_lookup_run.py', 'chessdb_integrity.py',
        'chessdb_publication.py', 'month_archive.py')}, native=sha(Path(native.__file__)),
        numpy=np.__version__, arrow=pa.__version__, duckdb=duckdb.__version__)


def apply_policy(table, base):
    """Arrow/native boundary. Full keys never pass through floating-point columns."""
    q = keys(table['lo'].to_numpy(), table['hi'].to_numpy())
    idx = np.searchsorted(base[['lo', 'hi']], q)
    hit = idx < len(base)
    hit[hit] = base[['lo', 'hi']][idx[hit]] == q[hit]
    bc = np.full(len(q), np.nan)
    bs = np.zeros(len(q), np.uint8)
    bc[hit], bs[hit] = base['cp'][idx[hit]], base['source'][idx[hit]]
    def floats(name):
        return np.ascontiguousarray(table[name].to_numpy(zero_copy_only=False), np.float64)
    if table['white_to_move'].null_count or table['best_score'].null_count:
        raise ValueError('scored ChessDB row lacks orientation/value')
    args = [floats(n) for n in ('cloud_cp', 'cloud_mate', 'fishnet_cp', 'fishnet_mate')]
    nnue = np.ascontiguousarray(pc.fill_null(pc.equal(table['fishnet_tier'], 'nnue'), False)
                               .to_numpy(zero_copy_only=False), np.uint8)
    cp, score, source = native.eval_source_policy(*args, nnue, floats('best_score'),
        np.ascontiguousarray(table['white_to_move'].to_numpy(zero_copy_only=False), np.uint8), bc, bs)
    use = np.isin(source, [6, 7])
    out = np.empty(int(use.sum()), dtype=OVERLAY_ROW)
    out['lo'], out['hi'], out['cp'], out['source'] = q['lo'][use], q['hi'][use], cp[use], source[use]
    return out, score[use], dict(queried=len(q), changed=len(out),
        ensemble=int((source == 6).sum()), chessdb_only=int((source == 7).sum()),
        preserved_terminal=int((bs == 3).sum()), fallback=int((~use).sum()))


def make_bucket(paths, dest, bucket, *, batch_size=65536, memory='2GB', threads=2):
    """Bounded native join/sort and native scoring; one sparse row per changed key."""
    base = np.load(paths['base'], mmap_mode='r', allow_pickle=False)
    if base.dtype != ROW or not strictly_sorted(base) or (base['lo'] % 512 != bucket).any():
        raise ValueError('invalid base lookup bucket')
    raw = dest.with_suffix('.rows.tmp')
    con = duckdb.connect()
    con.execute('SET memory_limit=?', [memory])
    con.execute('SET threads=?', [threads])
    spill = dest.parent/'spill'
    spill.mkdir(exist_ok=True)
    con.execute('SET temp_directory=?', [str(spill)])
    stats = dict(queried=0, changed=0, ensemble=0, chessdb_only=0, preserved_terminal=0, fallback=0)
    last = None
    rounding = 0.
    started = time.monotonic()
    try:
        relation = con.execute('''SELECT m.lo,m.hi,m.white_to_move,m.best_score,
            e.cloud_cp,e.cloud_mate,e.fishnet_cp,e.fishnet_mate,e.fishnet_tier,e.hash_ambiguous
            FROM read_parquet(?) m LEFT JOIN read_parquet(?) e
              ON m.lo=e.position_hash AND m.hi=e.position_hash_hi
            WHERE m.found AND m.best_score IS NOT NULL ORDER BY m.lo,m.hi''',
            [str(paths['menus']), str(paths['db'])]).to_arrow_reader(batch_size)
        with raw.open('wb') as f:
            for batch in relation:
                table = pa.Table.from_batches([batch])
                q = keys(table['lo'].to_numpy(), table['hi'].to_numpy())
                if len(q) == 0:
                    continue
                first = (int(q['lo'][0]), int(q['hi'][0]))
                if (not strictly_sorted(q) or (q['lo'] % 512 != bucket).any()
                        or (last is not None and first <= last)
                        or pc.any(pc.fill_null(table['hash_ambiguous'], False)).as_py()):
                    raise ValueError('ambiguous, duplicate, unsorted or misplaced source key')
                last = (int(q['lo'][-1]), int(q['hi'][-1]))
                rows, scores, counts = apply_policy(table, base)
                rows.tofile(f)
                for k, v in counts.items(): stats[k] += v
                if len(rows):
                    rounded = native.cp_to_expected_score(np.rint(rows['cp']).copy())
                    rounding = max(rounding, float(np.max(abs(rounded-scores))))
        # NPY's fixed header requires a known row count; raw is an owned temporary.
        staged = dest.with_suffix('.npy.tmp')
        arr = np.lib.format.open_memmap(staged, mode='w+', dtype=OVERLAY_ROW, shape=(stats['changed'],))
        if stats['changed']:
            source = np.memmap(raw, dtype=OVERLAY_ROW, mode='r')
            for off in range(0, len(source), batch_size):
                arr[off:off+batch_size] = source[off:off+batch_size]
            del source
        arr.flush()
        del arr
        staged.replace(dest)
        raw.unlink()
    finally:
        con.close()
    stats.update(rows=stats['changed'], seconds=time.monotonic()-started,
                 integer_rounding_max_score_error=rounding)
    return stats


def run(base, db, menus, out, buckets, *, memory='2GB', threads=2, reserve_gib=0):
    if (not buckets or len(set(buckets)) != len(buckets) or any(not 0 <= b < 512 for b in buckets)
            or threads < 1 or reserve_gib < 0):
        raise ValueError('invalid bucket set')
    if set(buckets) == set(range(512)) and reserve_gib < 250:
        raise ValueError('full build requires the 250 GiB main-PC free-space reserve')
    out = Path(out).resolve()
    # Existing verified input contract binds eval DB, published menus and base book.
    ctx = inputs.context(base, db, menus, out, require_publication=True)
    if ctx['base_meta'].get('variant') != inputs.VERSION:
        raise ValueError('fallback must be the accepted step-1 input-bound generation')
    identity = dict(schema=OVERLAY_SCHEMA, policy=POLICY, producer=producer(),
                    inputs=ctx['identity'], memory=memory, threads=threads)
    out.mkdir(parents=True, exist_ok=True)
    with file_lock(out/'run.lock'):
        ip = out/'_identity.json'
        if ip.exists() and read_json(ip) != identity:
            raise ValueError('different producer/inputs/settings: use a new output root')
        if not ip.exists() and (any(out.glob('bkt*.npy')) or (out/'_done').exists()):
            raise ValueError('unidentified existing output')
        write_json(ip, identity)
        (out/'_done').mkdir(exist_ok=True)
        records = {}
        try:
            for b in sorted(buckets):
                start = time.monotonic()
                paths, fingerprints = inputs.bucket_input(ctx, b)
                dest = out/f'bkt{b:03d}.npy'
                receipt = out/'_done'/f'{b:03d}.json'
                if receipt.exists():
                    r = read_json(receipt)
                    if r['identity'] != identity or r['inputs'] != fingerprints:
                        raise ValueError('bucket input/producer identity changed')
                    fingerprint(dest, r['sha256'])
                else:
                    if shutil.disk_usage(out).free < (reserve_gib+8)*2**30:
                        raise ValueError('free-space reserve plus 8 GiB working margin unavailable')
                    r = make_bucket(paths, dest, b, memory=memory, threads=threads)
                    r.update(identity=identity, inputs=fingerprints, sha256=sha(dest),
                             bytes=dest.stat().st_size, total_seconds=time.monotonic()-start)
                    try:
                        import psutil
                        mem = psutil.Process().memory_info()
                        r['process_peak_rss_bytes'] = getattr(mem, 'peak_wset', mem.rss)
                    except ImportError:
                        pass
                    for n, p in paths.items(): unchanged(p, fingerprints[n])
                    write_json(receipt, r)
                records[dest.name] = r
                write_json(out/'status.json', dict(state='running', completed=len(records),
                    total=len(buckets), last_bucket=b, last_seconds=r['total_seconds']))
                print(f'bucket {b}: {r["rows"]:,} overlay rows, {r["total_seconds"]:.1f}s', flush=True)
            # Recheck every pinned global and bucket stamp before publication.
            for v in ctx['globals'].values(): unchanged(v['path'], v['fingerprint'])
            for name, r in records.items():
                b = int(name[3:6])
                source_roots = {k: Path(v) for k, v in ctx['roots'].items()}
                for n, p in dict(base=source_roots['base']/name,
                    db=source_roots['db']/f'bkt{b:03d}.parquet',
                    menus=source_roots['menus']/'positions'/f'bkt{b:03d}.parquet',
                    menu_receipt=source_roots['menus']/'_done'/f'bkt{b:03d}.json').items():
                    unchanged(p, r['inputs'][n])
            complete = set(buckets) == set(range(512))
            meta = dict(schema=OVERLAY_SCHEMA, policy=POLICY, complete=complete,
                base=dict(root=str(Path(base).resolve()), sha256=sha(Path(base)/'lookup.json')),
                source=ctx['base_meta']['source'], terminal=ctx['base_meta'].get('terminal', False),
                book_meta_sha256=ctx['base_meta']['book_meta_sha256'],
                identity=identity, buckets={n: {k: r[k] for k in ('rows', 'sha256', 'bytes')}
                                            for n, r in records.items()})
            write_json(out/('lookup.json' if complete else 'pilot.json'), meta)
            write_json(out/'status.json', dict(state='verified' if complete else 'pilot-verified',
                completed=len(records), total=len(buckets), complete=complete,
                rows=sum(r['rows'] for r in records.values()), bytes=sum(r['bytes'] for r in records.values()),
                seconds=sum(r['total_seconds'] for r in records.values())))
        except BaseException as e:
            write_json(out/'status.json', dict(state='error', error=repr(e), completed=len(records)))
            raise
    return records


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for n in ('base', 'eval-db', 'menus', 'out'): ap.add_argument('--'+n, type=Path, required=True)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument('--buckets', type=int, nargs='+')
    group.add_argument('--full', action='store_true')
    ap.add_argument('--memory', default='2GB')
    ap.add_argument('--threads', type=int, default=2)
    ap.add_argument('--reserve-gib', type=float, default=250,
                    help='free-space reserve; full builds require at least 250 GiB')
    a = ap.parse_args()
    run(a.base, a.eval_db, a.menus, a.out, list(range(512)) if a.full else a.buckets,
        memory=a.memory, threads=a.threads, reserve_gib=a.reserve_gib)


if __name__ == '__main__': main()
