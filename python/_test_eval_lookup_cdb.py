"""ChessDB step-1 lookup (eval_lookup_cdb.py): only gaps and pre-2021 fishnet change, on the right scale.

Synthetic base lookup + eval DB rows + ChessDB positions for one bucket. Checks: a gap is filled
(source 4) and a pre-2021 fishnet score replaced (source 5); 2021+ fishnet, cloud and terminal mates
and legacy rows ChessDB does not score are byte-for-byte unchanged; side-to-move scores become White's
view; mate/tablebase claims map to +-2000 and ordinary scores clamp to +-2000; found-but-scoreless rows
and not-found rows add nothing; rows stay sorted by unique full key; inputs from another book, another
eval DB, changed code/inputs in an existing output and a done record that no longer matches are
refused; a partial build writes no lookup.json.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from eval_lookup128 import CLOUD, FISHNET, ROW, SCHEMA, TERMINAL
from eval_lookup_cdb import CHESSDB_GAP, CHESSDB_OVER_LEGACY, build, chessdb_white_cp, strictly_sorted
from month_archive import sha

B = 7                                   # bucket under test: every lo % 512 == 7


def lo(i):
    return 512 * i + B


class Kernel(unittest.TestCase):
    def test_scale(self):
        got = chessdb_white_cp(np.array([120, 120, 29990, -29990, 15000, 2500, -14999, 0]),
                               np.array([True, False, True, True, False, True, False, True]))
        self.assertEqual(got.tolist(), [120, -120, 2000, -2000, -2000, 2000, 2000, 0])


def fixture(td: Path, menus_book="book-sha", db_meta="db-meta"):
    base, db, menus, out = td / "base", td / "db", td / "menus", td / "out"
    for d in (base, db, menus / "positions", menus / "_done"):
        d.mkdir(parents=True)
    (db / "_build.meta.json").write_text(json.dumps({"x": db_meta}), encoding="utf8")
    # base lookup rows: A legacy fishnet, Bn 2021+ fishnet, C cloud, D terminal (legacy in DB), E legacy unscored
    rows = np.array([(lo(1), 11, 300, FISHNET), (lo(2), 22, 40, FISHNET), (lo(3), 33, -15, CLOUD),
                     (lo(4), 44, 2000, TERMINAL), (lo(5), 55, -80, FISHNET)], dtype=ROW)
    np.save(base / f"bkt{B:03d}.npy", rows, allow_pickle=False)
    (base / "lookup.json").write_text(json.dumps({
        "schema": SCHEMA, "book_meta_sha256": "book-sha",
        "source": {"build_meta_sha256": sha(db / "_build.meta.json")}, "buckets": {}}), encoding="utf8")
    pq.write_table(pa.table({"position_hash": [lo(1), lo(2), lo(3), lo(4), lo(5)],
                             "position_hash_hi": [11, 22, 33, 44, 55],
                             "source": ["fishnet", "fishnet", "cloud", "fishnet", "fishnet"],
                             "fishnet_tier": ["classical", "nnue", "nnue", "early", "early"]}),
                   db / f"bkt{B:03d}.parquet")
    # ChessDB: A, Bn, C, D found; F, G new (G a mate claim); H not found; I found but no moves
    pq.write_table(pa.table({
        "lo": pa.array([lo(1), lo(2), lo(3), lo(4), lo(6), lo(7), lo(8), lo(9)], pa.int64()),
        "hi": pa.array([11, 22, 33, 44, 66, 77, 88, 99], pa.int64()),
        "white_to_move": [True, True, True, False, False, True, True, True],
        "found": [True, True, True, True, True, True, False, True],
        "best_score": pa.array([120, 900, 900, None, -50, 29990, None, None], pa.int16())}),
        menus / "positions" / f"bkt{B:03d}.parquet")
    (menus / "_params.json").write_text(json.dumps({"book_meta_sha256": menus_book}), encoding="utf8")
    (menus / "_done" / f"bkt{B:03d}.json").write_text(json.dumps(
        {"positions_sha256": sha(menus / "positions" / f"bkt{B:03d}.parquet")}), encoding="utf8")
    # Bind both source bucket contents, as required by the v2 checkpoint contract.
    bm = json.loads((base / "lookup.json").read_text(encoding="utf8"))
    bm["buckets"] = {f"bkt{B:03d}.npy": {"sha256": sha(base / f"bkt{B:03d}.npy")}}
    (base / "lookup.json").write_text(json.dumps(bm), encoding="utf8")
    pq.write_table(pa.table({"bucket": [B], "sha256": [sha(db / f"bkt{B:03d}.parquet")]}), db / "_manifest.parquet")
    return base, db, menus, out


class Build(unittest.TestCase):
    def test_policy(self):
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td))
            rec = build(base, db, menus, out, [B])[f"bkt{B:03d}.npy"]
            got = {(int(r["lo"]), int(r["hi"])): (int(r["cp"]), int(r["source"]))
                   for r in np.load(out / f"bkt{B:03d}.npy")}
            self.assertEqual(got, {(lo(1), 11): (120, CHESSDB_OVER_LEGACY),      # legacy replaced
                                   (lo(2), 22): (40, FISHNET),                    # 2021+ kept
                                   (lo(3), 33): (-15, CLOUD),                     # cloud kept
                                   (lo(4), 44): (2000, TERMINAL),                 # terminal kept
                                   (lo(5), 55): (-80, FISHNET),                   # legacy, unscored by ChessDB
                                   (lo(6), 66): (50, CHESSDB_GAP),                # black to move: flipped
                                   (lo(7), 77): (2000, CHESSDB_GAP)})             # mate claim
            self.assertEqual((rec["gap_added"], rec["legacy_replaced"], rec["rows"]), (2, 1, 7))
            arr = np.load(out / f"bkt{B:03d}.npy")
            self.assertTrue(strictly_sorted(arr))
            self.assertFalse((out / "lookup.json").exists())              # partial build: no publication
            build(base, db, menus, out, [B])                              # resume: checkpoint reused

    def test_refusals(self):
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td), menus_book="other-book")
            with self.assertRaisesRegex(ValueError, "another explorer book"):
                build(base, db, menus, out, [B])
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td))
            (db / "_build.meta.json").write_text("{}", encoding="utf8")
            with self.assertRaisesRegex(ValueError, "not built from this eval DB"):
                build(base, db, menus, out, [B])
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td))
            build(base, db, menus, out, [B])
            (menus / "_params.json").write_text(json.dumps({"book_meta_sha256": "book-sha", "v": 2}),
                                                encoding="utf8")
            with self.assertRaisesRegex(ValueError, "other inputs or code"):
                build(base, db, menus, out, [B])
        with tempfile.TemporaryDirectory() as td:
            base, db, menus, out = fixture(Path(td))
            (menus / "_done" / f"bkt{B:03d}.json").write_text(json.dumps({"positions_sha256": "0"}),
                                                               encoding="utf8")
            with self.assertRaisesRegex(ValueError, "content differs"):
                build(base, db, menus, out, [B])
            (menus / "_done" / f"bkt{B:03d}.json").unlink()
            with self.assertRaisesRegex(ValueError, "not imported yet"):
                build(base, db, menus, out, [B])


if __name__ == "__main__":
    unittest.main()
