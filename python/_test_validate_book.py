"""validate_book: the checker holds a book to the builder's own candidate rule.

  C  classify uses budget_core.final_candidates: an engine proposal is judged in the complete
     set, in both directions, for both colours (a move it pushes out of range is outside_set; the
     proposal itself is in_set), including a proposal whose eval exists only in its file; the
     source-move rules and the owner's fallback come out the same way.
  F  fresh_verdict: the engine's own best move is never a confirmed error, however its two
     searches disagree; a real alternative is compared under one restriction and budget;
     unstable scores are marked.
  L  load_engine_candidates records a disagreement with the cached eval and uses the fresh one.

Run: .venv/Scripts/python.exe python/_test_validate_book.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import chess
import chess.engine
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
import validate_book as vb  # noqa: E402
from stage3_backwards_induction import cp_to_expected_score as ws  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def after(*sans):
    b = chess.Board()
    for s_ in sans:
        b.push_san(s_)
    return b


META = {"slice_prior": 0.5, "gates": False, "max_eval_loss": 30.0, "max_cands": 0}

print("C  classify")
for white in (True, False):
    c = "White" if white else "Black"
    pre = () if white else ("e4",)
    rec, eng = ("e4", "d4") if white else ("e5", "d5")
    sign = 1 if white else -1
    b = after(*pre)
    h, hr, he = zobrist_int64(b), zobrist_int64(after(*pre, rec)), zobrist_int64(after(*pre, eng))
    es = [{"move_san": rec, "child_hash": hr, "total": 100, "white_score_avg": 0.5}]
    ev = {h: ws(0), hr: ws(0), he: ws(sign * 300)}
    eng_m = {h: [(eng, he)]}
    check(f"{c}: the move a stronger proposal pushes out is outside_set",
          vb.classify(h, rec, es, rec, ev, white, META, eng_m, b.epd()) == "outside_set")
    check(f"{c}: the proposal itself is in_set",
          vb.classify(h, eng, es, rec, ev, white, META, eng_m, b.epd()) == "in_set")
    check(f"{c}: without the proposal the recorded move is in_set",
          vb.classify(h, rec, es, rec, ev, white, META, None, b.epd()) == "in_set")
    check(f"{c}: and the unproposed move is outside_set",
          vb.classify(h, eng, es, rec, ev, white, META, None, b.epd()) == "outside_set")
    # the builder agrees, node for node
    root = zobrist_int64(chess.Board())
    edges = {h: [{**es[0], "parent_epd": b.epd()}]}
    if not white:
        edges[root] = [{"move_san": "e4", "child_hash": h, "total": 10, "white_score_avg": 0.5,
                        "parent_epd": chess.Board().epd()}]
    g = bc.build_graph(edges, root, white, rep_moves={h: rec}, eval_ws=ev, gates=False, max_eval_loss=30,
                       strict_source=True, engine_moves=eng_m, prior_strength=1.0, eval_weight=0.0)
    check(f"{c}: build_graph's candidates are the same", [x[0] for x in g.our[h].cands] == [eng],
          str([x[0] for x in g.our[h].cands]))

b = chess.Board()
h, he4, hd4 = zobrist_int64(b), zobrist_int64(after("e4")), zobrist_int64(after("d4"))
es = [{"move_san": "e4", "child_hash": he4, "total": 100, "white_score_avg": 0.5},
      {"move_san": "d4", "child_hash": hd4, "total": 100, "white_score_avg": 0.5}]
ev = {h: ws(200), he4: ws(0), hd4: ws(-50)}
check("all fail: the least-losing move is fallback", vb.classify(h, "e4", es, None, ev, True, META, None, b.epd()) == "fallback")
check("...and the other is outside_set", vb.classify(h, "d4", es, None, ev, True, META, None, b.epd()) == "outside_set")
check("no evals: unknown", vb.classify(h, "e4", es, None, {}, True, META, None, b.epd()) == "unknown")
ev2 = {h: ws(0), he4: ws(0), hd4: ws(-120)}
check("a recorded source move the limit rejects is outside_set (never restored)",
      vb.classify(h, "d4", es, "d4", ev2, True, META, None, b.epd()) == "outside_set")
nf3 = zobrist_int64(after("Nf3"))
check("an unrecorded source move within the limit is in_set",
      vb.classify(h, "Nf3", es[:1], "Nf3", {h: ws(0), he4: ws(0), nf3: ws(10)}, True, META, None, b.epd()) == "in_set")
check("...without an eval it is outside_set",
      vb.classify(h, "Nf3", es[:1], "Nf3", {h: ws(0), he4: ws(0)}, True, META, None, b.epd()) == "outside_set")
check("a check mark in the book's spelling does not matter",
      vb.classify(h, "e4+", es[:1], None, {h: ws(0), he4: ws(0)}, True, META, None, b.epd()) == "in_set")

print("L  proposals from a file")
with tempfile.TemporaryDirectory() as td:
    f = Path(td) / "e.parquet"
    pl.DataFrame({"position_hash": [h, h], "move_san": ["d4", "Nf3"], "child_hash": [hd4, nf3],
                  "child_cp_white": [300, 40]}).write_parquet(f)
    ev3 = {h: ws(0), he4: ws(0), hd4: ws(-50)}
    moves, conf = bc.load_engine_candidates(str(f), ev3)
    check("the proposals load", moves == {h: [("d4", hd4), ("Nf3", nf3)]})
    check("an eval only in the file is used", abs(ev3[nf3] - ws(40)) < 1e-12)
    check("a disagreement is recorded and the fresh eval used",
          len(conf) == 1 and conf[0]["child_hash"] == hd4 and abs(ev3[hd4] - ws(300)) < 1e-12, str(conf))
    check("classify reads the file's eval",
          vb.classify(h, "d4", es[:1], None, ev3, True, META, moves, b.epd()) == "in_set"
          and vb.classify(h, "e4", es[:1], None, ev3, True, META, moves, b.epd()) == "outside_set")

print("F  fresh searches")


class Eng:
    """Scripted searches: free -> (pv move, cp), restricted to a move -> that move's cp (white view)."""

    def __init__(self, free, by_move):
        self.free, self.by_move, self.calls = free, by_move, []

    def configure(self, _):
        self.calls.append("clear")

    def analyse(self, board, limit, root_moves=None):
        if root_moves:
            m = root_moves[0]
            return {"score": chess.engine.PovScore(chess.engine.Cp(self.by_move[board.san(m)]), chess.WHITE),
                    "pv": [m]}
        san, cp = self.free
        return {"score": chess.engine.PovScore(chess.engine.Cp(cp), chess.WHITE), "pv": [board.parse_san(san)]}


b = chess.Board()
r = vb.fresh_verdict(Eng(("e4", 100), {"e4": 0}), b, "e4", 1000, 30, True)
check("the engine's own move: same_move, whatever a second search would say",
      r["fresh"] == "same_move" and r["sf_loss"] == 0, str(r))
r = vb.fresh_verdict(Eng(("d4", 100), {"d4": 100, "e4": 20}), b, "e4", 1000, 30, True)
check("a real alternative, worse by 80 under one restriction: confirmed",
      r["fresh"] == "confirmed" and r["sf_loss"] == 80 and not r["unstable"], str(r))
r = vb.fresh_verdict(Eng(("d4", 100), {"d4": 25, "e4": 20}), b, "e4", 1000, 30, True)
check("a free score the restricted search does not repeat: cleared, marked unstable",
      r["fresh"] == "cleared" and r["unstable"], str(r))
e = Eng(("d4", 100), {"d4": 100, "e4": 20})
vb.fresh_verdict(e, b, "e4", 1000, 30, True)
check("the hash is cleared before every search", e.calls.count("clear") == 3)
r = vb.fresh_verdict(Eng(("d5", -100), {"d5": -100, "e5": -20}), after("e4"), "e5", 1000, 30, False)
check("Black's view: worse by 80", r["fresh"] == "confirmed" and r["sf_loss"] == 80, str(r))

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
