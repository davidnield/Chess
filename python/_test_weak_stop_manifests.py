"""The legacy weak stop on published manifests (partitioned adult pool, compact weak count pool)
gives exactly the values it gives on the same rows as single full-key files."""
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import unittest

import duckdb
import numpy as np
import polars as pl

from _test_sweep_execution import board, edge, keys
from consumer_keys import PAIR, stored_key
from weak_stop import EvalSource, weak_stop_values

LINES = [("", "e4", 40), ("", "d4", 30), ("", "Nf3", 3), ("e4", "e5", 20), ("e4", "c5", 15),
         ("e4", "e6", 1), ("d4", "d5", 22), ("d4", "Nf6", 6), ("e4 e5", "Nf3", 14), ("e4 e5", "Bc4", 5),
         ("e4 c5", "Nf3", 12), ("e4 c5", "c3", 2), ("d4 d5", "c4", 18), ("d4 d5", "Bf4", 3),
         ("e4 e5 Nf3", "Nc6", 10), ("e4 c5 Nf3", "d6", 7), ("d4 d5 c4", "e6", 9)]
WEAK = [("", "e4", 25), ("", "d4", 9), ("e4", "e5", 11), ("e4", "c5", 30), ("e4 e5", "Bc4", 21),
        ("e4 e5", "Qh5", 4), ("e4 c5", "Nc3", 25), ("d4 d5", "Bf4", 12), ("d4 d5", "c4", 3)]


def ids_of(k, paths):
    frame = pl.DataFrame({"h": [stored_key(board(p), True) for p in paths]}, schema={"h": PAIR})
    return k.encode(frame, "h")["h"].to_numpy()


class WeakStopManifests(unittest.TestCase):
    def test_single_files_and_manifests_agree_exactly(self):
        from _test_partitioned_inputs import published_fixture
        from _test_sweep_count_pool import compact_fixture
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            adult = pl.DataFrame([edge(p, s, n) for p, s, n in LINES])
            weak = pl.DataFrame([edge(p, s, n) for p, s, n in WEAK])
            single_adult, single_weak = root / "adult.parquet", root / "weak.parquet"
            adult.write_parquet(single_adult)
            weak.write_parquet(single_weak)
            meta = dict(key_bits=128, min_games=1, max_ply=4,
                        population=dict(elo_bands=[1000, 1200, 1400, 1600], events=["Blitz", "Rapid", "Classical"]))
            with redirect_stdout(io.StringIO()):
                edges_json, _, _, _ = published_fixture(root / "adult-pub", adult.to_dicts())
                weak_json = compact_fixture(root / "weak-pub", weak, meta)
            results = []
            for pool, weak_pool in ((single_adult, single_weak), (edges_json, weak_json)):
                k = keys()
                xs = ids_of(k, ["e4 e5", "e4 c5", "d4 d5"])          # White to move: our turn as White
                ys = ids_of(k, ["e4", "d4"])                         # Black to move: their turn

                def lookup(ids, k=k):
                    lo = np.array([h["lo"] for h in k.external(pl.Series("h", ids, dtype=pl.Int64)).to_list()])
                    return (lo % 401 - 200).astype(float)
                con = duckdb.connect()
                W, R, c = weak_stop_values(con, xs, ys, pool=str(pool), weak_pool=str(weak_pool),
                                           evals=EvalSource(True, lookup=lookup), min_weak=20,
                                           min_share=0.01, log=lambda *_: None, positions=k)
                con.close()
                ext = lambda d, k=k: {tuple(sorted(k.external(pl.Series("h", [i], dtype=pl.Int64))[0].items())): v
                                      for i, v in d.items()}
                results.append((ext(W), ext(R), dict(c)))
            self.assertEqual(results[0][0], results[1][0])
            self.assertEqual(results[0][1], results[1][1])
            self.assertEqual(results[0][2], results[1][2])
            self.assertGreater(results[0][2]["x_weak"], 0)            # the weak pool decided somewhere
            self.assertGreater(results[0][2]["x_fallback_pool"], 0)   # and the adult pool elsewhere
            self.assertTrue(results[0][0] and results[0][1])

    def test_manifest_needs_full_keys(self):
        con = duckdb.connect()
        with self.assertRaisesRegex(ValueError, "full-key"):
            weak_stop_values(con, np.array([1], np.int64), np.array([], np.int64), pool="x/edges.json",
                             weak_pool="x/pool.json", evals=EvalSource(True, lookup=lambda ids: np.zeros(len(ids))),
                             log=lambda *_: None, positions=None)


if __name__ == "__main__":
    unittest.main()
