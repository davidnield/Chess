"""Exercise the eval worker's production forecast and runtime disk checks."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

import run_ply51_evals as worker
from month_archive import read_json


class ChildCapacityTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        for name in ("PILOT_DB", "PILOT_WORK", "BOOK", "STATE"):
            path = self.root / name
            path.mkdir()
            self.enterContext(patch.object(worker, name, path))
        (worker.BOOK / "_book.meta.json").write_text("{}")
        pq.write_table(pa.table({"rows": [1], "bytes": [1]}), worker.PILOT_DB / "_manifest.parquet")
        for folder in ("e", "c"):
            directory = worker.PILOT_WORK / folder
            directory.mkdir()
            (directory / "part").write_bytes(b"x" * 5)

    def test_drive_order_and_reserves(self):
        # Eight buckets, 20% margin: 5 sample bytes => 384 production bytes.
        shards = 384
        final = 90 * worker.GB + 1536  # (1 parquet byte + one 19-byte lookup row) * 76.8
        reserve = 150 * worker.GB
        # Net bytes available for children after fixed workloads and the reserve.
        cases = [
            ("H:", shards, shards, shards, 2 * shards),
            ("D:", shards - 1, shards, shards, 2 * shards),
            ("E:", shards - 1, shards - 1, shards, 2 * shards),
            ("F:", shards - 1, shards - 1, shards - 1, shards),
            (None, shards - 1, shards - 1, shards - 1, shards - 1),
            (None, -1, shards, shards, 2 * shards),
            (None, shards, -1, shards, 2 * shards),
        ]
        for expected, h, d, e, f in cases:
            with self.subTest(expected=expected, h=h, d=d, e=e, f=f):
                free = {"H:": reserve + shards + h, "D:": reserve + final + d,
                        "E:": reserve + e, "F:": reserve + f}
                layout = worker.STATE / "layout.json"
                layout.unlink(missing_ok=True)
                with patch.object(worker, "space", side_effect=lambda path: free[str(path)[:2]]):
                    if expected is None:
                        with self.assertRaisesRegex(ValueError, "child shards need|capacity shortfall"):
                            worker.forecast()
                        self.assertFalse(layout.exists())
                    else:
                        self.assertEqual(worker.forecast()["phase_c_drive"], expected)
                        self.assertEqual(read_json(layout)["phase_c_drive"], expected)
                report = read_json(worker.STATE / "capacity.json")
                self.assertEqual(report["phase_c_bytes"], shards)
                self.assertEqual(report["db_lookup_and_spill_bytes"], final)
                self.assertEqual(report["available_after_150GB_reserve"]["F:"], f)

    def test_unavailable_f_allows_other_drives(self):
        def free(path):
            if str(path).startswith("F:"):
                raise OSError("offline")
            return 500 * worker.GB

        with patch.object(worker, "space", side_effect=free):
            plan = worker.forecast()
        self.assertEqual(plan["phase_c_drive"], "H:")
        self.assertNotIn("F:", plan["available_after_150GB_reserve"])

    def test_worker_uses_saved_f_layout_on_restart(self):
        # Stop at the full build boundary. Capture the actual junction and monitored drive.
        from month_archive import sha, write_json
        self.enterContext(patch.object(worker, "PIN", self.root))
        write_json(self.root / "pin.json", {"files": {}})
        write_json(worker.STATE / "layout.json", {
            "book_meta_sha256": sha(worker.BOOK / "_book.meta.json"), "phase_c_drive": "F:"})
        self.enterContext(patch.object(worker, "wait_book"))
        self.enterContext(patch.object(worker, "status"))
        forecast = self.enterContext(patch.object(worker, "forecast"))
        attach = self.enterContext(patch.object(worker, "attach_child_folder"))
        native = self.enterContext(patch.object(worker, "native", side_effect=RuntimeError("test stop")))
        with self.assertRaisesRegex(RuntimeError, "test stop"):
            worker.main()
        forecast.assert_not_called()
        attach.assert_called_once_with("F:")
        native.assert_called_once_with(worker.WORK, worker.DB, drives=[Path("F:/")])

    def test_live_guard_on_selected_f(self):
        child = MagicMock()
        child.poll.return_value = None
        child.__enter__.return_value = child
        target = Path("F:/")
        with patch.object(worker.subprocess, "Popen", return_value=child), \
             patch.object(worker, "space", side_effect=lambda p: (80 * worker.GB - 1
                          if p == target else 500 * worker.GB)):
            with self.assertRaisesRegex(RuntimeError, "below 80 GB"):
                worker.native(worker.WORK, worker.DB, drives=[target])
        child.terminate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
