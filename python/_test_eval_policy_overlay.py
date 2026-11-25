"""Native policy through Parquet materialization, full-key lookup and local overrides."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import book_builder_rs as native
from eval_arrays import MISSING
from eval_lookup128 import ROW, SCHEMA, OVERLAY_ROW, OVERLAY_SCHEMA, POLICY, WideEvalLookup
import eval_policy_overlay as policy
from month_archive import read_json, write_json, sha
from weak_stop import EvalSource

B = 7
LO = 2**62 + B  # exact 128-bit join keys, including colliding low halves


def fixture(root):
    base, db, menus, out = (root/n for n in ('base', 'db', 'menus', 'out'))
    for p in (base, db, menus/'positions', menus/'_done'): p.mkdir(parents=True)
    rows = np.array([(LO, 1, 600, 1), (LO, 2, 500, 2), (LO+512, 1, 100, 1),
                     (LO+1024, 1, 2000, 3), (LO+1536, 1, 70, 1)], dtype=ROW)
    np.save(base/'bkt007.npy', rows)
    write_json(db/'_build.meta.json', {'test': True})
    write_json(base/'lookup.json', dict(schema=SCHEMA, variant=policy.inputs.VERSION,
        source=dict(build_meta_sha256=sha(db/'_build.meta.json')), book_meta_sha256='book',
        buckets={'bkt007.npy': dict(rows=len(rows), sha256=sha(base/'bkt007.npy'))}))
    table = pa.table(dict(position_hash=pa.array(rows['lo'], pa.int64()),
        position_hash_hi=pa.array(rows['hi'], pa.int64()),
        cloud_cp=[600, None, 100, -100, 70], cloud_mate=pa.array([None]*5, pa.int16()),
        fishnet_cp=[-100, 500, 200, -100, None], fishnet_mate=pa.array([None]*5, pa.int16()),
        fishnet_tier=['nnue', 'nnue', 'early', 'nnue', None], hash_ambiguous=[False]*5))
    pq.write_table(table, db/'bkt007.parquet')
    pq.write_table(pa.table(dict(bucket=[B], sha256=[sha(db/'bkt007.parquet')])), db/'_manifest.parquet')
    pq.write_table(pa.table(dict(lo=pa.array([*rows['lo'], LO+2048], pa.int64()),
        hi=pa.array([*rows['hi'], 1], pa.int64()), white_to_move=[False, True, True, True, True, False],
        best_score=[-200, 20, 300, -100, None, 29990], found=[True]*6)), menus/'positions/bkt007.parquet')
    write_json(menus/'_params.json', dict(book_meta_sha256='book'))
    write_json(menus/'_done/bkt007.json', dict(bucket=B, positions_sha256=sha(menus/'positions/bkt007.parquet')))
    entry = dict(receipt=dict(sha256=sha(menus/'_done/bkt007.json')),
                 positions=dict(sha256=sha(menus/'positions/bkt007.parquet')))
    write_json(menus/'_MENUS.COMPLETE.json', dict(schema='chessdb-menu-publication-v1',
        params=dict(sha256=sha(menus/'_params.json')), buckets={str(b): entry for b in range(512)}))
    return base, db, menus, out


class OverlayTest(unittest.TestCase):
    def test_fractional_other_moves_pool_and_independent_audit(self):
        import sys
        import polars as pl
        import pool_from_book as pool
        import pool_from_book_gates as gates
        from _test_pool128 import make_inputs, rows_for
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            book, base = make_inputs(folder)
            overlay = folder/'overlay'; overlay.mkdir()
            files = {}
            for b in range(512):
                name = f'bkt{b:03d}.npy'
                rows = [(1, -2, -300.49, 6)] if b == 1 else []
                np.save(overlay/name, np.array(rows, dtype=OVERLAY_ROW))
                files[name] = dict(rows=len(rows), sha256=sha(overlay/name))
            write_json(overlay/'lookup.json', dict(schema=OVERLAY_SCHEMA,policy=POLICY,complete=True,
                base=dict(root=str(base),sha256=sha(base/'lookup.json')),buckets=files,
                book_meta_sha256=sha(book/'_book.meta.json')))
            args = ['pool_from_book','--book',str(book),'--out-dir',str(folder/'exports'),
                '--work',str(folder/'work'),'--eval-lookup128',str(overlay),
                '--events','Rapid','--elo-bands','1800','--max-ply','5','--min-games','5',
                '--tag','fractional','--tmp-dir',str(folder/'spill')]
            with patch.object(pool,'N_BUCKETS',2), patch.object(pool,'GROUP_SIZE',1), \
                    patch.object(pool,'_pool_run',lambda fn,tasks,*_: [fn(t) for t in tasks]), \
                    patch.object(sys,'argv',args):
                pool.main()
                parsed = pool.build_parser().parse_args()
                _, aux = pool.out_paths(parsed)
                row = rows_for(pl.read_parquet(aux),'position_hash','e4').row(0,named=True)
                self.assertAlmostEqual(row['other_eval_mean'],1/(1+np.exp(.00368208*300.49)),places=14)
                parsed.buckets=[0,1]
                self.assertTrue(gates.gate_reimpl(parsed))
                parsed.seed,parsed.n,parsed.sample_buckets=11,100,2
                self.assertTrue(gates.gate_evals(parsed))

    def test_materialization_lookup_fractional_precision_and_local_precedence(self):
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td))
            r = policy.run(base, db, menus, out, [B])['bkt007.npy']
            self.assertEqual((r['ensemble'], r['chessdb_only'], r['preserved_terminal']), (1, 2, 1))
            self.assertFalse((out/'lookup.json').exists())
            with self.assertRaises(FileNotFoundError): WideEvalLookup(out)
            lookup = WideEvalLookup(out, allow_partial=True)
            low = np.array([LO, LO, LO+512, LO+1024, LO+1536, LO+2048, LO], np.int64)
            high = np.array([1, 2, 1, 1, 1, 1, 3], np.int64)
            cp, src = lookup.lookup(low, high)
            expected = .3/(1+np.exp(-.00368208*600))+.3/(1+np.exp(.00368208*100))+.4/(1+np.exp(-.00368208*200))
            self.assertAlmostEqual(1/(1+np.exp(-.00368208*cp[0])), expected, places=15)
            self.assertNotEqual(cp[0], round(cp[0]))
            self.assertEqual(cp[1:].tolist(), [20., 100., 2000., 70., -2000., MISSING])
            self.assertEqual(src.tolist(), [6, 7, 1, 3, 1, 7, 0])
            # The builder's actual scorer keeps qualified local CP overrides above
            # cached-source values, in both horizon and ordinary modes.
            for boundary in (False, True):
                scorer = EvalSource(True, lookup=lambda _: cp, boundary=boundary, overrides={0: -400.})
                values = scorer(np.arange(len(cp), dtype=np.int64))
                self.assertAlmostEqual(values[0], 1/(1+np.exp(.00368208*400)), places=15)
                self.assertTrue(np.isnan(values[-1]))
            before = sha(out/'bkt007.npy')
            policy.run(base, db, menus, out, [B])
            self.assertEqual(sha(out/'bkt007.npy'), before)
            from eval_policy_overlay_verify import verify
            report = verify(out, Path(td)/'check.json', count=8, buckets=[B])
            self.assertTrue(report['verified'])
            self.assertEqual(report['selected_buckets'], ['bkt007.npy'])
            with self.assertRaisesRegex(ValueError, 'bucket set'):
                verify(out, Path(td)/'bad-check.json', count=8, buckets=[B+1])
            del lookup

    def test_changed_input_and_output_refused(self):
        for target in ('source', 'overlay', 'fallback_manifest', 'fallback_array'):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as td:
                base, db, menus, out = fixture(Path(td))
                policy.run(base, db, menus, out, [B])
                p = dict(source=db/'bkt007.parquet', overlay=out/'bkt007.npy',
                         fallback_manifest=base/'lookup.json', fallback_array=base/'bkt007.npy')[target]
                with p.open('ab') as f: f.write(b'corrupted')
                with self.assertRaises((ValueError, OSError)):
                    if target == 'source': policy.run(base, db, menus, out, [B])
                    else: WideEvalLookup(out, allow_partial=True).lookup([LO], [1])

    def test_duplicate_full_keys_rejected_across_batches(self):
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td))
            p = menus/'positions/bkt007.parquet'
            t = pq.read_table(p)
            pq.write_table(pa.concat_tables([t, t.slice(0, 1)]), p)
            out.mkdir()
            paths = dict(base=base/'bkt007.npy', db=db/'bkt007.parquet', menus=p)
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                policy.make_bucket(paths, out/'bkt007.npy', B, batch_size=1)

    def test_native_mismatched_arrays_and_invalid_flags_rejected(self):
        a = np.array([1.]); u = np.array([1], np.uint8)
        with self.assertRaisesRegex(ValueError, 'length'):
            native.eval_source_policy(a, a, a, a, u, a, u, a[:0], u)
        with self.assertRaisesRegex(ValueError, 'boolean'):
            native.eval_source_policy(a, a, a, a, u*2, a, u, a, u)

    def test_full_publication_and_disk_reserve_fail_closed(self):
        with self.assertRaisesRegex(ValueError, '250 GiB'):
            policy.run('base', 'db', 'menus', 'out', list(range(512)))
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td))
            with patch.object(policy.shutil, 'disk_usage', return_value=SimpleNamespace(free=257*2**30)):
                with self.assertRaisesRegex(ValueError, 'reserve'):
                    policy.run(base, db, menus, out, [B], reserve_gib=250)
            self.assertFalse((out/'bkt007.npy').exists())
            self.assertFalse((out/'pilot.json').exists())


if __name__ == '__main__': unittest.main()
