"""Pool-bucket verification once per process (sweep.verified_files): same bytes checked, no reader ever
uses an unverified or changed file, and several readers of one publication hash each file once."""
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import duckdb
import polars as pl
from sweep import verified_files as vf
from sweep.stages import atomic_json, file_sha
from sweep.keyed_parquet import KeyedParquet
import _test_sweep_count_pool as cpt


def bump(path, data):
    """Rewrite with the same size and a later mtime (what a silent in-place edit looks like)."""
    st = path.stat()
    path.write_bytes(data)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))


class Verified(unittest.TestCase):
    def setUp(self):
        vf.forget()

    def test_hash_once_then_trust_the_fingerprint(self):
        with tempfile.TemporaryDirectory() as td:
            files = [Path(td) / f"f{i}" for i in range(6)]
            for i, f in enumerate(files):
                f.write_bytes(bytes([i]) * 100_000)
            items = [(f, file_sha(f)) for f in files]
            vf.verify_many(items, threads=4)
            self.assertEqual(vf.stats["hashed_files"], 6)
            vf.verify_many(items, threads=4)
            self.assertEqual((vf.stats["hashed_files"], vf.stats["memo_hits"]), (6, 6))
            self.assertTrue(all(vf.is_verified(f, s) for f, s in items))
            # a different expected hash for the same file is never answered from the memo
            with self.assertRaisesRegex(ValueError, "differs from its manifest"):
                vf.verify_many([(files[0], "0" * 64)])

    def test_changed_or_wrong_file_is_refused_and_not_remembered(self):
        with tempfile.TemporaryDirectory() as td:
            f = Path(td) / "bucket"
            f.write_bytes(b"a" * 1000)
            good = file_sha(f)
            vf.verify_many([(f, good)])
            bump(f, b"b" * 1000)                       # same size, new bytes
            self.assertFalse(vf.is_verified(f, good))
            with self.assertRaisesRegex(ValueError, "differs"):
                vf.verify_many([(f, good)])
            self.assertFalse(vf.is_verified(f, good))
            with self.assertRaisesRegex(ValueError, "differs"):
                vf.verify_many([(f, good)], threads=1)  # still refused on the next try

    def test_two_readers_of_one_publication_hash_each_bucket_once(self):
        fc = cpt.FullCounts()
        with tempfile.TemporaryDirectory() as td, duckdb.connect() as db:
            root = Path(td) / "pool"
            frame = pl.DataFrame(fc.rows()).with_columns(
                parent_hash=pl.struct(lo=pl.col("parent_hash"), hi=pl.col("parent_hash_hi")),
                child_hash=pl.struct(lo=pl.col("child_hash"), hi=pl.col("child_hash_hi")))
            meta = dict(key_bits=128, min_games=1, max_ply=30)
            path = cpt.compact_fixture(root, frame, meta)
            keys = [dict(lo=-512, hi=7), dict(lo=0, hi=3), dict(lo=512, hi=3)]
            calls = []
            real = vf.file_sha
            with patch.object(vf, "file_sha", lambda p: calls.append(Path(p).name) or real(p)):
                a = KeyedParquet(path, "parent_hash"); b = KeyedParquet(path, "parent_hash")
                first = a.fetch(db, keys).sort("move_san", "ply")
                second = b.fetch(db, keys).sort("move_san", "ply")
                a.close(); b.close()
            self.assertEqual(first.to_dicts(), second.to_dicts())
            self.assertEqual(sorted(calls), sorted(set(calls)))        # each bucket hashed once
            self.assertEqual(len(calls), len({k["lo"] % 512 for k in keys}))
            # a bucket rewritten after verification is refused by every reader
            target = root / "b000-r00.parquet"
            bump(target, target.read_bytes()[::-1])
            c = KeyedParquet(path, "parent_hash")
            with self.assertRaisesRegex(ValueError, "hash"):
                c.fetch(db, [dict(lo=0, hi=3)])
            c.close()


class Receipts(unittest.TestCase):
    """CHESS_VERIFY_MODE=receipt (David, 27 Nov 2026): hash once, then trust size + mtime + file ID."""
    def setUp(self):
        vf.forget()
        self.td = tempfile.TemporaryDirectory()
        self.root = Path(self.td.name)
        self.env = patch.dict(os.environ, {"CHESS_VERIFY_MODE": "receipt", "CHESS_VERIFY_SAMPLE": "0",
                                           "CHESS_VERIFY_RECEIPTS": str(self.root / "receipts")})
        self.env.start()
        self.data = self.root / "pool"; self.data.mkdir()
        self.files = [self.data / f"b{i:03d}.parquet" for i in range(5)]
        for i, f in enumerate(self.files):
            f.write_bytes(bytes([i + 1]) * 50_000)
        self.items = [(f, file_sha(f)) for f in self.files]

    def tearDown(self):
        self.env.stop(); vf.forget(); self.td.cleanup()

    def count(self):
        calls = []
        real = vf.file_sha
        return calls, patch.object(vf, "file_sha", lambda p: calls.append(Path(p).name) or real(p))

    def test_default_is_full_and_records_nothing(self):
        with patch.dict(os.environ, {"CHESS_VERIFY_MODE": ""}):
            del os.environ["CHESS_VERIFY_MODE"]
            self.assertEqual(vf.mode(), "full")
            self.assertIsNone(vf.provenance())
            vf.verify_many(self.items)
        self.assertFalse((self.root / "receipts").exists())

    def test_a_later_process_trusts_the_receipt(self):
        vf.verify_many(self.items)                      # first process: full hashes, receipt written
        self.assertEqual(vf.stats["hashed_files"], 5)
        vf.forget()                                     # a new process
        calls, p = self.count()
        with p:
            vf.verify_many(self.items)
        self.assertEqual(calls, [])
        self.assertEqual(vf.stats["receipt_hits"], 5)
        self.assertEqual(vf.provenance()["mode"], "receipt")

    def test_a_rewritten_file_is_hashed_again_and_refused(self):
        vf.verify_many(self.items)
        vf.forget()
        bump(self.files[2], b"x" * 50_000)              # same size, new mtime
        with self.assertRaisesRegex(ValueError, "differs from its manifest"):
            vf.verify_many(self.items)
        vf.forget()
        with self.assertRaisesRegex(ValueError, "differs"):   # its record is gone for good
            vf.verify_many([self.items[2]])

    def test_silent_corruption_is_the_documented_gap_and_the_sample_and_check_close_it(self):
        vf.verify_many(self.items)
        f = self.files[1]; st = f.stat()
        with open(f, "r+b") as h:                       # in place: same size, same file ID
            h.seek(100); h.write(b"\x00")
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))  # mtime put back: looks untouched
        vf.forget()
        vf.verify_many(self.items)                      # accepted by receipt: the gap
        vf.forget()
        with patch.dict(os.environ, {"CHESS_VERIFY_SAMPLE": "1"}):
            with self.assertRaisesRegex(ValueError, "random full check"):
                vf.verify_many(self.items)
        vf.forget()
        with self.assertRaisesRegex(ValueError, "differs"):   # dropped from the receipt
            vf.verify_many([self.items[1]])

    def test_check_rehashes_every_record_and_drops_failures(self):
        vf.verify_many(self.items)
        f = self.files[3]; st = f.stat()
        with open(f, "r+b") as h:
            h.seek(7); h.write(b"\x00")
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
        n, bad = vf.check_directories([self.data])
        self.assertEqual((n, [Path(b).name for b in bad]), (5, [f.name]))
        vf.forget()
        with self.assertRaisesRegex(ValueError, "differs"):
            vf.verify_many([self.items[3]])

    def test_pinned_artifacts_once_per_receipt_but_every_call_in_full_mode(self):
        from sweep import stages
        pins = [dict(path=str(f), sha256=s) for f, s in self.items]
        calls = []
        real = stages.file_sha
        with patch.object(stages, "file_sha", lambda p: calls.append(1) or real(p)),              patch.object(vf, "file_sha", lambda p: calls.append(1) or real(p)):
            stages.artifacts(pins); stages.artifacts(pins)
            self.assertEqual(len(calls), 5)
            with patch.dict(os.environ, {"CHESS_VERIFY_MODE": "full"}):
                calls.clear(); stages.artifacts(pins); stages.artifacts(pins)
                self.assertEqual(len(calls), 10)
        bad = [dict(pins[0], sha256="0" * 64)]
        with self.assertRaisesRegex(ValueError, "artifact changed"):
            stages.artifacts(bad)


if __name__ == "__main__":
    import sys
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
