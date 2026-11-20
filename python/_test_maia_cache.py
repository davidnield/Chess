"""Legacy Maia shard publication regressions; no model, source corpus, or engine needed."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from zipfile import BadZipFile

import numpy as np
import convertibility_eval as cv


IDENTITY = dict(maia_server="fixture-server", maia_batch="fixture-batch",
                maia_python="fixture-python", maia_models=["fixture-model"])


def write_legacy_part(path):
    """The pre-fix completed-shard format must remain readable without rewriting it."""
    np.savez(path, h=np.array([7],np.int64), se=np.array([1450],np.int16),
             oe=np.array([2000],np.int16), ptr=np.array([0,2],np.int64),
             code=np.array([0,1],np.int32), p=np.array([.25,.75],np.float64),
             sans=np.array(["e4","d4"]))


def partial_write(target):
    if hasattr(target,"write"):
        target.write(b"PK\x03\x04");target.flush()
    else:
        Path(target).write_bytes(b"PK\x03\x04")


class MaiaCachePublication(unittest.TestCase):
    def seed(self,root):
        cv.MaiaCache(root,IDENTITY)
        write_legacy_part(root/"part-20000101000000-1-0.npz")
        return cv.MaiaCache(root,IDENTITY)

    def test_legacy_temporary_shards_are_ignored_without_deleting_them(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);self.seed(root)
            old_tmp=root/"part-19990101000000-1-0.tmp.npz"
            old_tmp.write_bytes(b"PK\x03\x04")
            complete_tmp=root/"part-19990101000000-1-1.tmp.npz"
            write_legacy_part(complete_tmp)
            (root/".part-abandoned.tmp").write_bytes(b"partial")
            cache=cv.MaiaCache(root,IDENTITY)
            self.assertEqual(cache.loaded,1)
            self.assertEqual(cache.get([7,8],1450,2000),[{"e4":.25,"d4":.75},None])
            self.assertEqual(old_tmp.read_bytes(),b"PK\x03\x04")
            self.assertTrue(complete_tmp.exists())

    def test_reader_during_partial_write_sees_only_published_shards(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);writer=self.seed(root)
            writer.put(8,1450,2000,{"c4":1.})
            entered=threading.Event();release=threading.Event()
            real_savez=np.savez
            def paused_save(target,**arrays):
                partial_write(target);entered.set()
                if not release.wait(10):raise TimeoutError("test reader did not release writer")
                if hasattr(target,"write"):
                    target.seek(0);target.truncate()
                real_savez(target,**arrays)
            with patch.object(cv.np,"savez",side_effect=paused_save), ThreadPoolExecutor(1) as pool:
                future=pool.submit(writer.flush)
                try:
                    self.assertTrue(entered.wait(10))
                    self.assertEqual(len(list(root.glob("part-*.npz"))),1)
                    reader=cv.MaiaCache(root,IDENTITY)
                    self.assertEqual(reader.get([7,8],1450,2000),[{"e4":.25,"d4":.75},None])
                finally:release.set()
                future.result(timeout=10)
            reader=cv.MaiaCache(root,IDENTITY)
            self.assertEqual(reader.get([8],1450,2000),[{"c4":1.}])
            self.assertEqual(len(list(root.glob('part-*.npz'))),2)
            self.assertFalse(any(p.name.endswith('.tmp') for p in root.iterdir()))

    def test_concurrent_writers_do_not_replace_each_others_completed_shards(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);one=self.seed(root);two=cv.MaiaCache(root,IDENTITY)
            one.put(8,1450,2000,{"c4":1.});two.put(9,1450,2000,{"Nf3":1.})
            barrier=threading.Barrier(2);real_savez=np.savez
            def together(target,**arrays):
                barrier.wait(timeout=10)
                real_savez(target,**arrays)
            with patch.object(cv.time,"strftime",return_value="20261119190000"), \
                    patch.object(cv.time,"time_ns",return_value=123456789), \
                    patch.object(cv.np,"savez",side_effect=together), ThreadPoolExecutor(2) as pool:
                first=pool.submit(one.flush);second=pool.submit(two.flush)
                first.result(timeout=10);second.result(timeout=10)
            reader=cv.MaiaCache(root,IDENTITY)
            self.assertEqual(reader.get([8,9],1450,2000),[{"c4":1.},{"Nf3":1.}])
            self.assertEqual(len(list(root.glob("part-*.npz"))),3)

    def test_failed_write_preserves_old_shards_and_pending_answers_for_retry(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);writer=self.seed(root)
            old=root/"part-20000101000000-1-0.npz";before=old.read_bytes()
            writer.put(8,1450,2200,{"c4":1.})
            def fail(target,**arrays):
                partial_write(target);raise OSError("injected write failure")
            with patch.object(cv.np,"savez",side_effect=fail):
                with self.assertRaisesRegex(OSError,"injected write failure"):writer.flush()
            self.assertEqual(len(writer.new),1)
            self.assertEqual(old.read_bytes(),before)
            self.assertEqual(cv.MaiaCache(root,IDENTITY).get([8],1450,2200),[None])
            self.assertFalse(any(p.name.endswith('.tmp') for p in root.iterdir()))
            writer.flush()
            self.assertEqual(writer.new,[])
            reader=cv.MaiaCache(root,IDENTITY)
            self.assertEqual(reader.get([8],1450,2200),[{"c4":1.}])
            self.assertEqual(reader.get([8],1450,2000),[None])

    def test_failed_rename_never_publishes_or_discards_pending_answers(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);writer=self.seed(root);writer.put(8,1450,2000,{"c4":1.})
            with patch.object(Path,"replace",side_effect=OSError("injected publication failure")):
                with self.assertRaisesRegex(OSError,"injected publication failure"):writer.flush()
            self.assertEqual(len(writer.new),1)
            self.assertEqual(cv.MaiaCache(root,IDENTITY).get([8],1450,2000),[None])
            self.assertFalse(any(p.name.endswith('.tmp') for p in root.iterdir()))
            writer.flush()
            self.assertEqual(cv.MaiaCache(root,IDENTITY).get([8],1450,2000),[{"c4":1.}])

    def test_published_corruption_is_not_silently_skipped(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);self.seed(root)
            (root/"part-20000102000000-1-0.npz").write_bytes(b"PK\x03\x04")
            with self.assertRaises((BadZipFile,ValueError,EOFError,OSError)):
                cv.MaiaCache(root,IDENTITY)

    def test_reopen_preserves_order_ratings_first_answer_and_identity(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);writer=self.seed(root)
            writer.put(7,1450,2000,{"c4":1.})
            writer.put(7,1450,2200,{"Nf3":.1,"c4":.9})
            writer.put(7,1800,2000,{"a3":1.})
            writer.flush()
            reader=cv.MaiaCache(root,IDENTITY)
            self.assertEqual(list(reader.get([7],1450,2000)[0].items()),[("e4",.25),("d4",.75)])
            self.assertEqual(list(reader.get([7],1450,2200)[0].items()),[("Nf3",.1),("c4",.9)])
            self.assertEqual(reader.get([7],1800,2000),[{"a3":1.}])
            with self.assertRaises(SystemExit):
                cv.MaiaCache(root,dict(IDENTITY,maia_models=["different"]))


if __name__=="__main__":unittest.main()
