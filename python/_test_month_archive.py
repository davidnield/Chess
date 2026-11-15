"""Integration regressions for the pilot. Set EXPLORER_EXTRACT_EXE to the built executable."""
import json
import os
import random
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import chess
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

import month_archive as archive
import pool_from_book as pool
import run_month_extraction as launcher
from zobrist import zobrist_int64


class PilotTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.source = self.tmp / "source"
        self.stage = self.tmp / "stage"
        self.dest = self.tmp / "archive"
        self.exe = Path(os.environ["EXPLORER_EXTRACT_EXE"])
        rng = random.Random(125)
        board = chess.Board()
        self.moves = []
        for _ in range(52):
            move = rng.choice(sorted(board.legal_moves, key=lambda m: m.uci()))
            self.moves.append(board.san(move))
            board.push(move)
            self.assertIsNone(board.outcome())
        # White wins on time at ply 50. A second Blitz game continues and is
        # drawn at 51, and a parser failure occurs trying to play move 51.
        for event, rows in {
            "Blitz": [(50, 1800, 1., "Time forfeit"), (51, 1800, .5, "Normal"),
                      (-1, 1800, 0., "Normal"), (50, 2200, 0., "Normal")],
            "Rapid": [(50, 1800, 0., "Normal")],
            "Classical": [(52, 1800, 0., "Normal")],
        }.items():
            dest = self.source / "year=2026" / "month=3" / f"event={event}"
            dest.mkdir(parents=True)
            pq.write_table(pa.table({
                "movetext": [" ".join(self.moves[:n]) if n >= 0 else " ".join(self.moves[:50] + ["Qa9"]) for n,*_ in rows],
                "mean_elo": pa.array([r[1] for r in rows], type=pa.int32()),
                "white_score": [r[2] for r in rows], "termination": [r[3] for r in rows],
                "white_title": [""] * len(rows), "black_title": [""] * len(rows),
            }), dest / "part-0.parquet")
        self.args = ["--exe", str(self.exe), "--source", str(self.source), "--stage", str(self.stage),
                     "--dest", str(self.dest), "--months", "2026_3", "--events", "Blitz", "Classical", "Rapid",
                     "--max-ply", "51", "--key128", "--term-slices", "--threads", "1", "--mem-gb", "1",
                     "--month-arg=--passes=1", "--min-stage-free-gb", "0", "--min-dest-free-gb", "0"]

    def drive(self, extra=()):
        return launcher.drive(launcher.parser().parse_args(self.args + list(extra)))

    def test_preflight(self):
        with self.assertRaises(ValueError):
            self.drive(["--dest", str(self.stage / "nested")])
        with self.assertRaises(OSError):
            with archive.root_lock(self.tmp / "lock"), archive.root_lock(self.tmp / "lock"):
                pass
        self.dest.mkdir()
        (self.dest / "_month=2026_3.DONE").touch()
        with self.assertRaisesRegex(ValueError, "without a launcher"):
            self.drive(["--dry-run"])
        self.assertFalse(self.stage.exists())
        missing = self.source / "year=2026/month=3/event=Rapid/part-0.parquet"
        missing.rename(missing.with_suffix(".missing"))
        with self.assertRaisesRegex(ValueError, "missing speeds"):
            self.drive()
        ref = self.tmp / "reference"
        ref.mkdir()
        rp = ref / "month=2026_3.json"
        archive.write_json(rp, {"month": "2026_3", "flags": {"events": ["Blitz", "Classical", "Rapid"]}, "files": [str(p) for p in self.source.rglob("*.parquet")] + [str(missing)]})
        with self.assertRaisesRegex(ValueError, "independent provenance"):
            archive.inventory(self.source, (2026, 3), ["Blitz", "Classical", "Rapid"], ref)
        archive.write_json(rp, {"month": "2026_3", "flags": {"events": ["Blitz", "Classical", "Rapid"]}, "files": [str(p) for p in self.source.rglob("*.parquet")]})
        self.assertEqual(archive.inventory(self.source, (2026, 3), ["Blitz", "Classical", "Rapid"], ref)["absent_events"], ["Rapid"])

    def test_copy_restart_and_hash(self):
        original = launcher.copy_file
        def crash_before_marker(src, dst, verify, *args):
            if dst == self.dest / "_month=2026_3.DONE":
                raise OSError("simulated interruption")
            return original(src, dst, verify, *args)
        with patch.object(launcher, "copy_file", crash_before_marker):
            self.assertEqual(self.drive(), launcher.EXIT_FAIL)
        self.assertTrue((self.stage / "_month=2026_3.DONE").exists())
        self.assertFalse((self.dest / "_month=2026_3.DONE").exists())
        with self.assertRaisesRegex(ValueError, "settings"):
            self.drive(["--max-ply", "50"])
        cfgpath = self.stage / "_extract_params.json"
        cfg = archive.read_json(cfgpath)
        archive.write_json(cfgpath, {**cfg, "max_ply": 30})
        with self.assertRaisesRegex(ValueError, "settings"):
            self.drive()
        archive.write_json(cfgpath, cfg)
        with patch.object(archive, "release", side_effect=OSError("interrupted after publication")):
            self.assertEqual(self.drive(), launcher.EXIT_FAIL)
        with patch.object(launcher.subprocess, "run", side_effect=AssertionError("unnecessary re-extraction")):
            self.assertEqual(self.drive(), 0)
        self.assertFalse((self.stage / "month=2026_3").exists())
        path = next((self.dest / "month=2026_3").rglob("*.parquet"))
        with path.open("r+b") as f:
            f.seek(6)
            x = f.read(1)
            f.seek(6)
            f.write(bytes([x[0] ^ 1]))
        with self.assertRaisesRegex(ValueError, "receipt"):
            self.drive()

    def test_slice_and_horizon(self):
        self.assertEqual(self.drive(), 0)
        book = self.tmp / "book"
        proc = subprocess.run([str(self.exe), "merge", "--months-root", str(self.dest), "--months", "2026_3",
                               "--out", str(book), "--stage-dir", str(self.tmp / "merge_stage"), "--test-one-volume",
                               "--min-free-gb", "0", "--stage-gb", ".1", "--threads", "2"],
                              capture_output=True, text=True, errors="replace")
        self.assertEqual(proc.returncode, 0, proc.stderr[-3500:])
        self.assertEqual(pool.book_schema(book), "book-v3")
        self.assertEqual(pool.check_population(book, ["Blitz"], [1800])["2026_3"], ["Blitz","Classical","Rapid"])
        board = chess.Board()
        for san in self.moves[:50]:
            board.push_san(san)
        h = zobrist_int64(board)
        bk = h % 512
        all_files = [p for i in range(512) for p in pool.slice_files(book, ["Blitz"], [1800], i)]
        with duckdb.connect() as db:
            arr = self.tmp / "arr.parquet"
            db.execute(f"COPY (SELECT child_hash,ply,{','.join(pool.SUMS)} FROM read_parquet(?,hive_partitioning=false) WHERE ply<=50 AND {pool.bucket_of('child_hash')}=?) TO '{arr.as_posix()}'", [all_files,bk])
            # Include arrivals for the complete target bucket (also in the general case).
            pool.derive_flows(db, pool.slice_files(book, ["Blitz"], [1800], bk), arr.as_posix(), 50)
            pool.check_ended_nonnegative(db, "fixture")
            pool.split_endings(db, book, bk, ["Blitz"], [1800], 50)
            self.assertEqual(db.execute("SELECT term_flag_total,term_flag_white_wins,term_normal_total,term_other_black_wins FROM terminal WHERE x=?", [h]).fetchone(), (1,1,0,1))
            self.assertEqual(db.execute("SELECT total,draws FROM d WHERE x=? AND q=51",[h]).fetchone(), (1,1))
            parquet = str(book / "term" / f"bkt{bk:03d}.parquet")
            self.assertEqual(db.execute("SELECT event_id,elo_band,reason,total FROM read_parquet(?) WHERE end_ply=50 AND kind=0 ORDER BY ALL",[parquet]).fetchall(), [(0,1800,1,1),(0,2200,0,1),(4,1800,0,1)])
        with self.assertRaisesRegex(ValueError, "coverage"):
            pool.check_population(book, ["Bullet"], [1800])


if __name__ == "__main__":
    unittest.main()

