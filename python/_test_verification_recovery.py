"""Exact duplicate-check parity and fail-closed recovery-stage receipts."""
from pathlib import Path
import tempfile
import unittest
import subprocess
import duckdb
import polars as pl
import verify_book as vb
from resume_explorer_verification import stage

class Recovery(unittest.TestCase):
    def test_extrema_match_distinct_for_both_position_invariants(self):
        rows=pl.DataFrame(dict(parent_hash=[1,1,1,2,2,3,3,4,4],
                    parent_hash_hi=[10,10,10,20,20,30,31,40,41],
                    parent_epd=["a","a","b","c","c","d","d","e","f"]))
        with duckdb.connect() as db:
            db.register("rows",rows.to_arrow())
            for keys,value in (("parent_hash,parent_hash_hi","parent_epd"),("parent_hash,parent_epd","parent_hash_hi")):
                expected=db.execute(f"SELECT count(*) FROM (SELECT 1 FROM rows GROUP BY {keys} HAVING count(DISTINCT {value})>1)").fetchone()[0]
                actual=vb.mapping_conflicts(db,"rows",keys,value)
                self.assertEqual(actual,expected)
            self.assertEqual(vb.mapping_conflicts(db,"rows","parent_hash,parent_hash_hi","parent_epd"),1)
            self.assertEqual(vb.mapping_conflicts(db,"rows","parent_hash,parent_epd","parent_hash_hi"),1)

    def test_failure_resume_and_corrupt_receipt(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);calls=[]
            def fail(cmd,**kw):
                calls.append(cmd);kw["stdout"].write("interrupted")
                raise subprocess.CalledProcessError(1,cmd)
            options=dict(root=root,identity={"generation":"fixture"},command=["verifier"],guard=lambda:None)
            with self.assertRaises(subprocess.CalledProcessError):
                stage("duplicates",["--dup-buckets","0"],**options,execute=fail)
            self.assertFalse((root/"duplicates/receipt.json").exists())
            def success(cmd,**kw): calls.append(cmd);kw["stdout"].write("ALL PASS")
            self.assertFalse(stage("duplicates",["--dup-buckets","0"],**options,execute=success)["reused"])
            self.assertTrue(stage("duplicates",["--dup-buckets","0"],**options,execute=fail)["reused"])
            self.assertEqual(len(calls),2)
            (root/"duplicates/run.log").write_text("altered",encoding="utf8")
            with self.assertRaisesRegex(ValueError,"stale"):
                stage("duplicates",["--dup-buckets","0"],**options,execute=success)

if __name__=="__main__": unittest.main()
