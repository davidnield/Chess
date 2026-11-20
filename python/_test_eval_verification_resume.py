"""The eval verification receipt and the ply-51 coordinator's reuse of it (2026-11-20; exit 0 = pass).

No exe needed: the verifier and native stages are stubbed at the coordinator's `guarded` / `native`
boundary, so these tests cover the control plane. The verifier's own durable units are tested against
a real build in `_test_verify_evals.py` (run_checkpoint).

Covers: interruption before and after the receipt is published; a failed check publishes nothing; a
lookup interrupted after verification resumes without repeating it; a changed DB, book, source,
verifier, native binary or setting makes the receipt stale (set aside, verification reruns); a corrupt,
tampered or non-passing receipt stops the coordinator instead of being reused or overwritten; inputs
that change while the verifier runs publish nothing.
"""
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import eval_verification as ev
import run_ply51_evals as worker
from month_archive import read_json, write_json

HERE = Path(__file__).resolve().parent


def make_world(root: Path) -> dict:
    book = root / "book"
    book.mkdir()
    (book / "_book.meta.json").write_text('{"book": 1}')
    (book / "_BOOK.DONE").write_text("done")
    cloud, fish = root / "cloud", root / "fishnet"
    cloud.mkdir()
    fish.mkdir()
    (cloud / "data_0000.parquet").write_bytes(b"c" * 10)
    (fish / "standard_rated_2023_01.parquet").write_bytes(b"f" * 10)
    db = root / "db"
    db.mkdir()
    meta = {"schema": "eval-db-v2", "params": {"buckets": "all", "child_sources": "all"},
            "inputs": {"book": str(book), "book_meta_sha256": ev.file_sha(book / "_book.meta.json"),
                       "cloud": str(cloud), "fishnet": str(fish), "cloud_files": ["data_0000.parquet"],
                       "fishnet_files": ["standard_rated_2023_01.parquet"]}}
    write_json(db / "_build.meta.json", meta)
    pq.write_table(pa.table({"bucket": [0], "rows": [1]}), db / "_manifest.parquet")
    (db / "_DONE").write_text("2026-11-20\n")
    (db / "bkt000.parquet").write_bytes(b"b" * 10)
    code = root / "code"
    code.mkdir()
    for name in ev.VERIFIER_FILES:
        (code / name).write_text(f"# {name}\n")
    native = root / "explorer-extract.exe"
    native.write_bytes(b"native")
    return dict(book=book, cloud=cloud, fish=fish, db=db, code=code, native=native)


class IdentityTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.w = make_world(Path(temp.name))
        self.settings = {"seed": 1}

    def ident(self, settings=None):
        w = self.w
        return ev.identity(w["db"], w["book"], w["code"], w["native"], settings or self.settings)

    def test_stable_and_sensitive(self):
        base = self.ident()
        self.assertEqual(ev.differences(base, self.ident()), [])
        bump = lambda p: os.utime(p, ns=(p.stat().st_atime_ns, p.stat().st_mtime_ns + 10**9))
        cases = [
            ("db_hashes", lambda: (self.w["db"] / "_DONE").write_text("other\n")),
            ("db_files", lambda: bump(self.w["db"] / "bkt000.parquet")),
            ("book_meta_sha256", lambda: (self.w["book"] / "_book.meta.json").write_text('{"book": 2}')),
            ("sources", lambda: (self.w["fish"] / "standard_rated_2023_01.parquet").write_bytes(b"f" * 11)),
            ("verifier", lambda: (self.w["code"] / "verify_evals.py").write_text("# changed\n")),
            ("native_sha256", lambda: self.w["native"].write_bytes(b"rebuilt")),
        ]
        for field, change in cases:
            with self.subTest(field=field):
                before = self.ident()
                change()
                self.assertIn(field, ev.differences(before, self.ident()))
        self.assertEqual(ev.differences(self.ident(), self.ident({"seed": 2})), ["settings"])

    def test_receipt_round_trip_and_refusals(self):
        ident = self.ident()
        log = self.w["db"].parent / "verify.log"
        log.write_text("ALL PASS\n")
        good = {"all_pass": True, "checks": 12, "failures": [],
                "db_meta_sha256": ident["db_hashes"]["_build.meta.json"]}
        path = self.w["db"].parent / "receipt.json"
        for bad in ({**good, "all_pass": False}, {**good, "failures": ["x"]}, {**good, "checks": 0},
                    {**good, "db_meta_sha256": "0" * 64}):
            with self.subTest(result=bad):
                with self.assertRaises(ValueError):
                    ev.write_receipt(path, ident, bad, log)
                self.assertFalse(path.exists())
        ev.write_receipt(path, ident, good, log)
        self.assertEqual(ev.load_receipt(path)["identity"], ident)
        body = json.loads(path.read_text())
        for damage in (lambda b: b["result"].update(all_pass=False), lambda b: b.update(schema="other"),
                       lambda b: b["identity"].update(native_sha256="0")):
            copy = json.loads(json.dumps(body))
            damage(copy)
            path.write_text(json.dumps(copy))
            with self.assertRaises(ev.ReceiptError):
                ev.load_receipt(path)
        path.write_text("{")
        with self.assertRaises(ev.ReceiptError):
            ev.load_receipt(path)


class CoordinatorTest(unittest.TestCase):
    """verify_independently() and main() with the verifier/native/lookup boundaries stubbed."""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.w = make_world(root)
        state = root / "state"
        state.mkdir()
        for name, value in (("DB", self.w["db"]), ("BOOK", self.w["book"]), ("PIN", self.w["code"]),
                            ("NATIVE", self.w["native"]), ("STATE", state),
                            ("RECEIPT", state / "verification-receipt.json"), ("LOOKUP", root / "lookup"),
                            ("CLOUD_DIR", self.w["cloud"]), ("FISH_DIR", self.w["fish"])):
            self.enterContext(patch.object(worker, name, value))
        self.verifier_runs = 0
        self.outcome = "pass"

    def fake_guarded(self, args, drives, log=None):
        """Stands in for the subprocesses. The verifier writes --result-json; the lookup writes lookup.json."""
        args = [str(a) for a in args]
        if args[2].endswith("verify_evals.py"):
            self.verifier_runs += 1
            if self.outcome == "interrupt":
                raise subprocess.CalledProcessError(1, args)
            Path(log).write_text("verifier log\n")
            result = Path(args[args.index("--result-json") + 1])
            fails = ["a check"] if self.outcome == "fail" else []
            write_json(result, {"all_pass": not fails, "checks": 30, "failures": fails,
                                "db_meta_sha256": ev.file_sha(worker.DB / "_build.meta.json")})
            if self.outcome == "drift":
                (worker.DB / "bkt000.parquet").write_bytes(b"changed")
            if fails:
                raise subprocess.CalledProcessError(1, args)
        elif args[2].endswith("eval_lookup128.py"):
            if self.outcome == "lookup-interrupt":
                raise subprocess.CalledProcessError(1, args)
            write_json(worker.LOOKUP / "lookup.json", {"buckets": []})
        else:
            raise AssertionError(args)

    def verify(self):
        with patch.object(worker, "guarded", self.fake_guarded), patch.object(worker, "status"):
            worker.verify_independently()

    def test_pass_publishes_then_reuses(self):
        self.verify()
        self.assertTrue(worker.RECEIPT.exists())
        self.verify()
        self.assertEqual(self.verifier_runs, 1)

    def test_interrupt_or_failure_publishes_nothing(self):
        for outcome in ("interrupt", "fail", "drift"):
            with self.subTest(outcome=outcome):
                self.outcome = outcome
                with self.assertRaises((subprocess.CalledProcessError, ValueError)):
                    self.verify()
                self.assertFalse(worker.RECEIPT.exists())
        self.outcome = "pass"
        self.verify()
        self.assertTrue(worker.RECEIPT.exists())

    def test_stale_receipt_is_set_aside(self):
        self.verify()
        old = worker.RECEIPT.read_bytes()
        (worker.DB / "_DONE").write_text("rebuilt\n")
        self.verify()
        self.assertEqual(self.verifier_runs, 2)
        aside = list(worker.STATE.glob("verification-receipt.stale-*.json"))
        self.assertEqual(len(aside), 1)
        self.assertEqual(aside[0].read_bytes(), old)
        self.assertEqual(ev.differences(ev.load_receipt(worker.RECEIPT)["identity"],
                                        ev.identity(worker.DB, worker.BOOK, worker.PIN, worker.NATIVE,
                                                    worker.VERIFY_SETTINGS)), [])

    def test_changed_setting_reruns(self):
        self.verify()
        with patch.object(worker, "VERIFY_SETTINGS", {**worker.VERIFY_SETTINGS, "positive": 5}):
            self.verify()
        self.assertEqual(self.verifier_runs, 2)

    def test_corrupt_receipt_stops(self):
        self.verify()
        worker.RECEIPT.write_text("{not json")
        with self.assertRaises(ev.ReceiptError):
            self.verify()
        self.assertEqual(self.verifier_runs, 1)
        self.assertEqual(worker.RECEIPT.read_text(), "{not json")

    def test_lookup_interrupt_resumes_without_reverifying(self):
        write_json(self.w["code"] / "pin.json", {"files": {}})
        write_json(worker.STATE / "layout.json", {"book_meta_sha256": ev.file_sha(worker.BOOK / "_book.meta.json"),
                                                  "phase_c_drive": "H:"})
        statuses = []
        stubs = dict(check_native=lambda: None, wait_book=lambda: None, attach_child_folder=lambda d: None,
                     native=lambda *a, **k: None, sample_lookup=lambda: {"sampled_eval_rows": 1},
                     guarded=self.fake_guarded, status=lambda stage, **d: statuses.append((stage, d)))
        with patch.multiple(worker, **stubs):
            self.outcome = "lookup-interrupt"
            with self.assertRaises(subprocess.CalledProcessError):
                worker.main()
            self.assertEqual(statuses[-1][0], "stopped", statuses)
            self.assertTrue(worker.RECEIPT.exists())
            self.outcome = "pass"
            worker.main()
        self.assertEqual(self.verifier_runs, 1)
        stages = [s for s, _ in statuses]
        self.assertIn("independent verification reused from its receipt", stages)
        final = statuses[-1]
        self.assertEqual(final[0], "eval DB and lookup verified")
        self.assertEqual(final[1]["verification_receipt_sha256"], ev.file_sha(worker.RECEIPT))


if __name__ == "__main__":
    unittest.main()
