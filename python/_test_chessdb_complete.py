import tempfile
import unittest
from pathlib import Path
from _test_eval_lookup_cdb import fixture,B
from chessdb_lookup_run import context,bucket_work
from chessdb_complete import verify_outputs
from month_archive import write_json,read_json

class Acceptance(unittest.TestCase):
    def make(self,td):
        root=Path(td);base,db,menus,old=fixture(root);old.mkdir()
        bucket_work(context(base,db,menus,old),B)
        new=root/"new";new.mkdir()
        ctx=context(base,db,menus,new,old=old);rec=bucket_work(ctx,B)
        write_json(new/"lookup.json",dict(identity=ctx["identity"],buckets={f"bkt{B:03d}.npy":rec}))
        write_json(new/"_identity.json",ctx["identity"])
        return new,old
    def test_match_and_content_change(self):
        with tempfile.TemporaryDirectory() as td:
            new,old=self.make(td)
            self.assertEqual(verify_outputs(new,old,[B])["old_comparisons"],1)
            (new/f"bkt{B:03d}.npy").write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError,"content differs"):verify_outputs(new,old,[B])
    def test_missing_comparison(self):
        with tempfile.TemporaryDirectory() as td:
            new,old=self.make(td);m=read_json(new/"lookup.json");r=m["buckets"][f"bkt{B:03d}.npy"];r.pop("old_comparison")
            write_json(new/"lookup.json",m);write_json(new/"_done"/f"{B:03d}.json",r)
            with self.assertRaisesRegex(ValueError,"missing/stale"):verify_outputs(new,old,[B])
    def test_missing_bucket_or_receipt_change(self):
        with tempfile.TemporaryDirectory() as td:
            new,old=self.make(td)
            with self.assertRaisesRegex(ValueError,"wrong output bucket set"):verify_outputs(new,old)
            p=new/"_done"/f"{B:03d}.json";r=read_json(p);r["rows"]+=1;write_json(p,r)
            with self.assertRaisesRegex(ValueError,"receipt changed"):verify_outputs(new,old,[B])
if __name__=="__main__":unittest.main()
