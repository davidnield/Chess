"""Exact duplicate-check parity and fail-closed recovery-stage receipts."""
from pathlib import Path
import tempfile
import unittest
import subprocess
import duckdb
import polars as pl
import verify_book as vb
from resume_explorer_verification import stage,recovery_checks,run_checked

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

    def test_signed_ranges_preserve_duplicates_and_both_mapping_invariants(self):
        lows=[-(1<<63),-(1<<60)-1,-(1<<60),-1,0,(1<<60)-1,1<<60,(1<<63)-1,None]
        rows=[]
        for low in lows:
            for hi,epd in ((11,"a"),(11,"a"),(11,"b"),(12,"b")):
                rows.append(dict(parent_hash=low,parent_hash_hi=hi,parent_epd=epd,
                                 move_san="e4",event="Blitz",elo_band=1800,ply=1))
        with duckdb.connect() as db:
            db.register("rows",pl.DataFrame(rows).to_arrow())
            expected=vb.duplicate_counts(db,"rows",True)
            directions=(("parent_hash,parent_hash_hi","parent_epd"),("parent_hash,parent_epd","parent_hash_hi"))
            expected_mappings=[vb.mapping_conflicts(db,"rows",k,v) for k,v in directions]
            for count in (1,2,16):
                observed=[0,0];mapping=[0,0]
                for i in range(count):
                    source=vb.shard_source("rows",(i,count))
                    values=vb.duplicate_counts(db,source,True)
                    observed=[a+b for a,b in zip(observed,values)]
                    mapping=[a+vb.mapping_conflicts(db,source,k,v) for a,(k,v) in zip(mapping,directions)]
                self.assertEqual(tuple(observed),expected)
                self.assertEqual(mapping,expected_mappings)
        checks=recovery_checks(16)
        self.assertEqual(len(checks),55)
        for bucket in (0,251,511):
            self.assertEqual({flags[-1] for _,flags in checks if flags[:2]==["--dup-buckets",str(bucket)]},
                             {f"{i}/16" for i in range(16)})

    def test_child_heartbeat_failure_propagates(self):
        import sys
        with tempfile.TemporaryDirectory() as td:
            calls=[]
            with (Path(td)/"log").open("w",encoding="utf8") as output:
                with self.assertRaises(subprocess.CalledProcessError):
                    run_checked([sys.executable,"-c","import time;time.sleep(.05);raise SystemExit(2)"],
                                output=output,pulse=lambda pid,t:calls.append((pid,t)),
                                resource_guard=lambda:None,interval=.02)
            self.assertTrue(calls)

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
