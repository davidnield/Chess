"""Pool readers fetch their buckets on several threads (sweep.keyed_parquet.run_parallel): the rows and
the read statistics must equal the one-bucket-at-a-time loop's exactly."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import chess
import duckdb
import polars as pl
import sweep.keyed_parquet as kp
from sweep import verified_files as vf
from sweep.keyed_parquet import KeyedParquet
from sweep.count_pool import SUMS
from _test_sweep_execution import edge
import _test_partitioned_inputs as pti
import _test_sweep_count_pool as cpt


def positions():
    """Every position after two plies, plus the start: ~400 parents spread over the 512 buckets."""
    out = [""]
    b = chess.Board()
    for m1 in list(b.legal_moves):
        s1 = b.san(m1); b.push(m1)
        out.append(s1)
        for m2 in list(b.legal_moves)[:10]:
            out.append(f"{s1} {b.san(m2)}")
        b.pop()
    return out


def rows():
    result = []
    for i, path in enumerate(positions()):
        board = chess.Board()
        for san in path.split():
            board.push_san(san)
        for j, m in enumerate(list(board.legal_moves)[:3]):
            result.append(edge(path, board.san(m), 1 + (i * 7 + j) % 13))
    return result


class ParallelFetch(unittest.TestCase):
    def setUp(self):
        vf.forget()

    def compare(self, path, column, keys):
        out = {}
        for threads in (1, 8):
            with patch.object(kp, "FETCH_THREADS", threads), duckdb.connect() as db:
                db.execute("SET threads=2")
                r = KeyedParquet(path, column)
                first = r.fetch(db, keys)
                again = r.fetch(db, keys)                 # second call: readers reused or reopened
                out[threads] = (first, again, dict(r.stats))
                r.close()
        (a1, a2, sa), (b1, b2, sb) = out[1], out[8]
        # DuckDB's semi join returns a bucket's rows in no fixed order even one bucket at a time (two
        # sequential calls differ), so compare the rows as sets; buckets stay joined in bucket order.
        key = [c for c in ("parent_hash", "position_hash", "move_san", "child_hash", "ply") if c in a1.columns]
        a1, a2, b1, b2 = (f.sort(key) for f in (a1, a2, b1, b2))
        self.assertTrue(a1.equals(b1) and a2.equals(b2) and a1.equals(a2))
        self.assertEqual(a1.schema, b1.schema)
        self.assertEqual(sa, sb)
        self.assertGreater(a1.height, 0)
        return a1

    def test_partitioned_pool(self):
        data = rows()
        with tempfile.TemporaryDirectory() as td:
            edges, aux, _, _ = pti.published_fixture(Path(td) / "pub", data)
            keys = [r["parent_hash"] for r in data][::2] + [dict(lo=12345, hi=6789)]   # one missing key
            got = self.compare(edges, "parent_hash", keys)
            self.assertGreater(len({k["lo"] % 512 for k in got["parent_hash"].to_list()}), 50)
            self.compare(aux, "position_hash", keys)

    def test_count_pool(self):
        data = rows()
        frame = pl.DataFrame([dict(parent_hash=r["parent_hash"], child_hash=r["child_hash"], move_san=r["move_san"],
                                   ply=1, **{c: (r[c] if c in r else 0) for c in SUMS}) for r in data])
        with tempfile.TemporaryDirectory() as td:
            path = cpt.compact_fixture(Path(td) / "pool", frame, dict(key_bits=128, min_games=1, max_ply=30))
            keys = [r["parent_hash"] for r in data][1::2]
            got = self.compare(path, "parent_hash", keys)
            wanted = {(k["lo"], k["hi"]) for k in keys}
            self.assertEqual(got.height, sum((r["parent_hash"]["lo"], r["parent_hash"]["hi"]) in wanted for r in data))

    def test_a_failing_bucket_still_fails_in_parallel(self):
        data = rows()
        with tempfile.TemporaryDirectory() as td:
            edges, _, _, _ = pti.published_fixture(Path(td) / "pub", data)
            keys = [r["parent_hash"] for r in data]
            real = KeyedParquet.fetch
            def broken(self, db, ks):
                if self.partitioned is None and self.path.endswith("b007.edges.parquet"):
                    raise ValueError("simulated read failure")
                return real(self, db, ks)
            with patch.object(kp, "FETCH_THREADS", 8), patch.object(KeyedParquet, "fetch", broken), \
                 duckdb.connect() as db:
                r = KeyedParquet(edges, "parent_hash")
                if any(k["lo"] % 512 == 7 for k in keys):
                    with self.assertRaisesRegex(ValueError, "simulated read failure"):
                        r.fetch(db, keys)
                r.close()


if __name__ == "__main__":
    import sys
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
