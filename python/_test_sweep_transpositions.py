"""Full-key direct-child verification and retained transposition evidence."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import chess
import chess.engine
import polars as pl
import cloud_feedback as fb
import transposition_search as ts
from consumer_keys import PAIR,stored_key

def edge(line,san,cp):
    b=chess.Board()
    for m in line.split(): b.push_san(m)
    h=stored_key(b,True);epd=b.epd();b.push_san(san)
    return dict(position_hash=h,position_epd=epd,move_san=san,child_hash=stored_key(b,True),
                child_cp_white=cp,nodes=20_000_000,depth=30,engine="fixture",date="fixed",bound="unknown")

class FullKeyTranspositions(unittest.TestCase):
    def test_shared_child_cache_and_direct_score_survives_agreement(self):
        a=edge("Nf3 d5","g3",20);b=edge("g3 d5","Nf3",-40)
        self.assertEqual(a["child_hash"],b["child_hash"])
        frame=pl.DataFrame([a,b],schema={**fb.SCHEMA,"position_hash":PAIR,"child_hash":PAIR})
        calls=[]
        class Engine:
            def configure(self,c): pass
            def analyse(self,b,limit,game):
                calls.append(stored_key(b,True))
                return dict(score=chess.engine.PovScore(chess.engine.Cp(7),chess.WHITE),nodes=limit.nodes,depth=30)
            def quit(self): pass
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);binary=root/"sf";binary.write_bytes(b"engine")
            with patch.object(ts,"SF",binary),patch.object(chess.engine.SimpleEngine,"popen_uci",return_value=Engine()):
                resolved,report=ts.resolve(frame,root/"cache")
                self.assertEqual(calls,[a["child_hash"]])
                self.assertEqual(resolved["transposition_cp_white"].to_list(),[7,7])
                warm,_=ts.resolve(frame.reverse(),root/"cache")
                self.assertTrue(resolved.equals(warm));self.assertEqual(len(calls),1)
                agreed=resolved.with_columns(pl.lit(10).cast(pl.Int32).alias("child_cp_white"))
                kept,_=ts.resolve(agreed,root/"cache")
                self.assertEqual(kept["transposition_cp_white"].to_list(),[7,7])
                bad=frame.with_columns(pl.col("child_hash").struct.with_fields(
                       (pl.field("hi")+1).alias("hi")))
                with self.assertRaisesRegex(ValueError,"mismatch"): ts.resolve(bad,root/"cache")

if __name__=="__main__": unittest.main()
