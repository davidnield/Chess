"""Regression coverage for the input-bound lookup and exclusive coordinator."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from _test_eval_lookup_cdb import fixture, B, lo
from chessdb_lookup_run import context, bucket_work, run, receipt_ids
from month_archive import read_json, write_json, sha

class Resume(unittest.TestCase):
    def setup(self,root):
        base,db,menus,out=fixture(root)
        out.mkdir()
        ctx=context(base,db,menus,out)
        return base,db,menus,out,ctx
    def change_menu(self,menus,receipt=True):
        p=menus/"positions"/f"bkt{B:03d}.parquet"
        t=pq.read_table(p); v=t["best_score"].to_pylist(); v[0]=321
        i=t.schema.get_field_index("best_score")
        t=t.set_column(i,t.schema.field(i),pa.array(v,type=t.schema.field(i).type))
        pq.write_table(t,p)
        if receipt:
            r=menus/"_done"/f"bkt{B:03d}.json"; d=read_json(r); d["positions_sha256"]=sha(p); write_json(r,d)
    def test_updated_input_receipt_is_refused(self):
        with tempfile.TemporaryDirectory() as td:
            base,db,menus,out,ctx=self.setup(Path(td))
            first=bucket_work(ctx,B); self.change_menu(menus)
            with self.assertRaisesRegex(ValueError,"input generation changed"):bucket_work(ctx,B)
            fresh=Path(td)/"fresh"; fresh.mkdir()
            new=bucket_work(context(base,db,menus,fresh),B)
            self.assertNotEqual(first["sha256"],new["sha256"])
            a=np.load(fresh/f"bkt{B:03d}.npy")
            self.assertEqual(int(a[a["lo"]==lo(1)]["cp"][0]),321)
    def test_unupdated_receipt_is_refused(self):
        with tempfile.TemporaryDirectory() as td:
            _,_,menus,_,ctx=self.setup(Path(td))
            bucket_work(ctx,B); self.change_menu(menus,False)
            with self.assertRaisesRegex(ValueError,"content differs"):bucket_work(ctx,B)
    def test_base_same_size_same_mtime_change(self):
        with tempfile.TemporaryDirectory() as td:
            base,_,_,_,ctx=self.setup(Path(td)); bucket_work(ctx,B)
            p=base/f"bkt{B:03d}.npy"; st=p.stat()
            a=np.load(p); a["cp"][0]+=1; np.save(p,a,allow_pickle=False)
            os.utime(p,ns=(st.st_atime_ns,st.st_mtime_ns))
            self.assertEqual(st.st_size,p.stat().st_size)
            with self.assertRaisesRegex(ValueError,"content differs"):bucket_work(ctx,B)
    def test_db_bucket_change(self):
        with tempfile.TemporaryDirectory() as td:
            _,db,_,_,ctx=self.setup(Path(td)); bucket_work(ctx,B)
            p=db/f"bkt{B:03d}.parquet"; p.write_bytes(p.read_bytes()+b"changed")
            with self.assertRaisesRegex(ValueError,"content differs"):bucket_work(ctx,B)
    def test_output_corruption_or_missing(self):
        for missing in (False,True):
            with self.subTest(missing=missing),tempfile.TemporaryDirectory() as td:
                _,_,_,out,ctx=self.setup(Path(td)); bucket_work(ctx,B)
                p=out/f"bkt{B:03d}.npy"
                if missing:p.unlink()
                else:p.write_bytes(b"bad output")
                with self.assertRaises((ValueError,FileNotFoundError)):bucket_work(ctx,B)
    def test_missing_menu_receipt(self):
        with tempfile.TemporaryDirectory() as td:
            _,_,menus,_,ctx=self.setup(Path(td)); bucket_work(ctx,B)
            (menus/"_done"/f"bkt{B:03d}.json").unlink()
            with self.assertRaisesRegex(ValueError,"not imported"):bucket_work(ctx,B)
    def test_interrupted_before_receipt_can_resume(self):
        with tempfile.TemporaryDirectory() as td:
            _,_,_,out,ctx=self.setup(Path(td))
            with patch("chessdb_lookup_run.write_json",side_effect=RuntimeError("interrupted")):
                with self.assertRaisesRegex(RuntimeError,"interrupted"):bucket_work(ctx,B)
            self.assertTrue((out/f"bkt{B:03d}.npy").exists())
            self.assertFalse((out/"_done"/f"{B:03d}.json").exists())
            result=bucket_work(ctx,B)
            self.assertEqual(result["sha256"],sha(out/f"bkt{B:03d}.npy"))
    def test_legacy_receipt_not_upgraded(self):
        with tempfile.TemporaryDirectory() as td:
            _,_,_,out,ctx=self.setup(Path(td)); bucket_work(ctx,B)
            p=out/"_done"/f"{B:03d}.json"; d=read_json(p); d.pop("inputs"); d.pop("receipt_version"); write_json(p,d)
            with self.assertRaisesRegex(ValueError,"legacy or incompatible"):bucket_work(ctx,B)
    def test_extra_bucket_refused(self):
        with tempfile.TemporaryDirectory() as td:
            _,_,_,out,_=self.setup(Path(td))
            for n in ("512.json","garbage.json","07.json"):
                p=out/"_done"/n; write_json(p,{})
                with self.assertRaisesRegex(ValueError,"invalid checkpoint"):receipt_ids(out)
                p.unlink()
    def test_old_comparison(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); base,db,menus,out,ctx=self.setup(root)
            first=bucket_work(ctx,B)
            new=root/"new"; new.mkdir()
            rec=bucket_work(context(base,db,menus,new,old=out),B)
            self.assertTrue(rec["old_comparison"]["matched"])
            self.assertEqual(rec["sha256"],first["sha256"])
            p=out/f"bkt{B:03d}.npy"; a=np.load(p); a["cp"][0]+=1; np.save(p,a)
            r=out/"_done"/f"{B:03d}.json"; d=read_json(r); d["sha256"]=sha(p); write_json(r,d)
            with self.assertRaisesRegex(ValueError,"old/new lookup bytes disagree"):
                bucket_work(context(base,db,menus,new,old=out),B)
    def test_publication_required(self):
        with tempfile.TemporaryDirectory() as td:
            base,db,menus,out,_=self.setup(Path(td))
            with self.assertRaises(FileNotFoundError):context(base,db,menus,out,require_publication=True)

class Coordinator(unittest.TestCase):
    def test_other_process_lock_and_crash_release(self):
        with tempfile.TemporaryDirectory() as td:
            base,db,menus,out=fixture(Path(td)); out.mkdir()
            # Use the real interpreter: terminating a venv launcher can leave its child alive.
            script="import os,sys,time\nf=open(sys.argv[1],'a+b'); f.write(b'0'); f.flush(); f.seek(0)\nif os.name=='nt':\n import msvcrt; msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)\nelse:\n import fcntl; fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)\nprint('locked',flush=True)\ntime.sleep(30)\n"
            proc=subprocess.Popen([sys._base_executable,"-B","-c",script,str(out/"_coordinator.lock")],stdout=subprocess.PIPE,text=True)
            try:
                self.assertEqual(proc.stdout.readline().strip(),"locked")
                with self.assertRaises(OSError):run(base,db,menus,out,[B])
            finally:
                proc.terminate();proc.wait(timeout=10);proc.stdout.close()
            result=run(base,db,menus,out,[B])
            self.assertEqual(len(result),1)
            self.assertFalse((out/"lookup.json").exists())
    def test_changed_global_identity_refused(self):
        with tempfile.TemporaryDirectory() as td:
            base,db,menus,out=fixture(Path(td));run(base,db,menus,out,[B])
            p=menus/"_params.json"; d=read_json(p);d["revision"]=2;write_json(p,d)
            with self.assertRaisesRegex(ValueError,"other inputs or code"):run(base,db,menus,out,[B])

if __name__=="__main__":unittest.main()
