"""No depth cap, connector risk, actual histories, checkpoint and gate regressions."""
import tempfile
from pathlib import Path
import unittest
import chess
from sweep.continuations import Search,token
from sweep.spec import digest

class Continuations(unittest.TestCase):
    def test_late_risk_crosses_easy_connectors_and_resumes(self):
        b=chess.Board();line="e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6 O-O Be7 Re1 b5 Bb3".split()
        moves={};ply={}
        for i,san in enumerate(line):
            moves[token(b)]=b.parse_san(san).uci();ply[token(b)]=i;b.push_san(san)
        end=token(b);seen=[]
        def analyse(board,required):
            k=token(board);seen.append(k);chosen=moves[k]
            alternatives=[m.uci() for m in board.legal_moves if m.uci()!=chosen][:2]
            cp=-400 if ply[k]==12 else 0
            return dict(top=[dict(uci=chosen,cp_white=0)]+[dict(uci=u,cp_white=cp) for u in alternatives],
                        independent=True,nodes=20_000_000,receipt="fixture")
        def reply(board):
            k=token(board);stop=float(k==end)
            return dict(moves={m.uci():float(not stop and m.uci()==moves[k]) for m in board.legal_moves},stop=stop)
        contract=dict(absolute_floor=.4,relative_limit=.06)
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/"state.json"
            kw=dict(base={},white=True,analyse=analyse,reply=reply,contract=contract,threshold=.001,checkpoint=path)
            first=Search(**kw).run(5)
            self.assertFalse(first["closed"]);self.assertEqual(first["additional_distinct"],0)
            fresh=Search(**kw);report=fresh.run(25)
            self.assertTrue(report["closed"]);self.assertEqual(report["additional_distinct"],7)
            self.assertGreater(report["additional_distinct"],4)
            self.assertEqual(len(seen),7) # no repeated engine work for the checkpoint prefix
            self.assertAlmostEqual(report["unresolved_exposure_bound"],0.)
            self.assertEqual(Search(**kw).run(0)["additional"],report["additional"])
            with self.assertRaisesRegex(ValueError,"recipe changed"):
                Search(**(kw|dict(threshold=.002)))
            with self.assertRaisesRegex(ValueError,"recipe changed"):
                Search(**(kw|dict(metric="covered-maximum")))

    def test_repetition_is_history_based_and_prescriptions_are_deduplicated(self):
        def choose(b):
            return ("g1f3" if b.piece_at(chess.G1) else "f3g1") if b.turn else (
                    "g8f6" if b.piece_at(chess.G8) else "f6g8")
        def analyse(b,required):
            u=choose(b);alt=[m.uci() for m in b.legal_moves if m.uci()!=u][:2]
            return dict(top=[dict(uci=u,cp_white=0)]+[dict(uci=v,cp_white=-200) for v in alt],
                        independent=True,nodes=20_000_000,receipt="fixture")
        def reply(b):
            return dict(moves={m.uci():float(m.uci()==choose(b)) for m in b.legal_moves})
        s=Search(base={},white=True,analyse=analyse,reply=reply,
                 contract=dict(absolute_floor=.4,relative_limit=.06),threshold=.001)
        report=s.run(40)
        self.assertTrue(report["closed"])
        self.assertEqual(report["deepest_ply"],16) # fivefold; first transposition at ply 4 was not terminal
        self.assertEqual(report["metric"],"remaining-maximum")
        self.assertEqual(report["additional_distinct"],2)
        self.assertEqual(report["known_unbooked_exposure"],0.)
        alternate=Search(base={},white=True,analyse=analyse,reply=reply,metric="covered-maximum",
                         contract=dict(absolute_floor=.4,relative_limit=.06),threshold=.001).run(40)
        self.assertTrue(alternate["closed"])
        self.assertEqual(alternate["additional_distinct"],1)
        self.assertGreater(alternate["known_unbooked_exposure"],.1)

    def test_master_hole_and_unknown_engine_evidence_cannot_close(self):
        def reply(b): raise AssertionError("should not reach opponent")
        s=Search(base={},white=True,analyse=None,reply=reply,allowed={},
                 contract=dict(absolute_floor=.4,relative_limit=.06))
        report=s.run(5)
        self.assertFalse(report["closed"]);self.assertEqual(report["blocked_mass"],1.)
        s=Search(base={},white=True,analyse=lambda b,r:dict(top=[],independent=False),reply=reply,
                 contract=dict(absolute_floor=.4,relative_limit=.06))
        with self.assertRaisesRegex(ValueError,"independent top-three"): s.run(1)

if __name__=="__main__": unittest.main()
