"""Plan facts (annotate.plan) and the v2 validator rules on a tiny hand-built book.

White book: 1.e4; 1...e5 (70%) 2.Nf3, 1...c5 (30%) 2.Nc3; after 2.Nf3, 2...Nc6 (80%)
3.Bc4 and 2...Nf6 (20%) 3.Nxe5. Black book: 1.e4 e5, 1.d4 d5 (the start position is
the opponent's turn for Black). Imports the production code (repo convention).

Run:  .venv/Scripts/python.exe python/_test_annotate_plan.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")

import chess
import polars as pl

from annotate.plan import PlanTree
from annotate.validate import validate, validate_chapter
from zobrist import zobrist_int64

FAILS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAILS
    print(f"  [{'ok ' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
    FAILS += not cond


def board(*sans: str) -> chess.Board:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def h(*sans: str) -> int:
    return zobrist_int64(board(*sans))


def write_inputs(tmp: Path, name: str, book: dict[tuple, str], edges: list[tuple]) -> tuple[str, str]:
    bp, sp = tmp / f"{name}_book.parquet", tmp / f"{name}_stats.parquet"
    pl.DataFrame({"position_hash": [h(*k) for k in book],
                  "position_epd": [board(*k).epd() for k in book],
                  "best_move": list(book.values())}).write_parquet(bp)
    pl.DataFrame({"parent_hash": [h(*p) for p, _, _ in edges],
                  "move_san": [m for _, m, _ in edges],
                  "child_hash": [h(*p, m) for p, m, _ in edges],
                  "total": [t for _, _, t in edges]}).write_parquet(sp)
    return str(bp), str(sp)


tmp = Path(tempfile.mkdtemp(prefix="annotate_plan_"))
wb, ws = write_inputs(tmp, "white",
                      {(): "e4", ("e4", "e5"): "Nf3", ("e4", "c5"): "Nc3",
                       ("e4", "e5", "Nf3", "Nc6"): "Bc4", ("e4", "e5", "Nf3", "Nf6"): "Nxe5"},
                      [(("e4",), "e5", 70), (("e4",), "c5", 30),
                       (("e4", "e5", "Nf3"), "Nc6", 80), (("e4", "e5", "Nf3"), "Nf6", 20)])

print("white plan from the start:")
pw = PlanTree("white", wb, ws).plan(h())
check("continuation follows the most common replies",
      pw["continuation"] == ["e4", "e5", "Nf3", "Nc6", "Bc4"], str(pw["continuation"]))
br = {(b["reply"], b["our_answer"]): b for b in pw["branches"]}
check("1...c5 is a plan change (2.Nc3, not 2.Nf3)",
      ("c5", "Nc3") in br and br[("c5", "Nc3")]["plan_changes"]
      and br[("c5", "Nc3")]["main_line_answer"] == "Nf3" and br[("c5", "Nc3")]["share_pct"] == 30.0,
      str(pw["branches"]))
check("2...Nf6 is a plan change (3.Nxe5, not 3.Bc4), with its line",
      ("Nf6", "Nxe5") in br and br[("Nf6", "Nxe5")]["after"] == ["e4", "e5", "Nf3"])
ideas = {r["token"]: r["reach_pct"] for r in pw["recurring_ideas"]}
check("recurring ideas are reach-weighted",
      ideas == {"Nf3": 70.0, "Bc4": 56.0, "Nc3": 30.0}, str(ideas))
check("an opponent-turn position has no plan", PlanTree("white", wb, ws).plan(h("e4")) is None)

print("\nshares count the games below the floor (aux sidecar):")
pool = tmp / "position_stats_t.parquet"
pl.read_parquet(ws).write_parquet(pool)
zero = {c: [0] for c in ("term_normal_total", "term_flag_total", "term_other_total", "horizon_total")}
pl.DataFrame({"position_hash": [h("e4")], "other_total": [25], "other_edges": [4], **zero}) \
  .write_parquet(tmp / "position_stats_aux_t.parquet")      # 100 surviving + 25 below the floor
pa = PlanTree("white", wb, str(pool)).plan(h())
c5 = next(b for b in pa["branches"] if b["reply"] == "c5")
check("1...c5 is 30 of 125 games, not 30 of 100", c5["share_pct"] == 24.0, str(c5))

print("\nblack: the start position is the opponent's turn:")
bb, bs = write_inputs(tmp, "black", {("e4",): "e5", ("d4",): "d5"},
                      [((), "e4", 60), ((), "d4", 40)])
pb = PlanTree("black", bb, bs)
check("the book is reached through White's first moves",
      pb.plan(h("e4")) is not None and pb.plan(h("e4"))["continuation"] == ["e5"],
      str(pb.plan(h("e4"))))

print("\nvalidator:")
facts = {"line": "1.e4 e5", "our_move": {"san": "Nf3", "idea_token": "Nf3"},
         "candidates": [], "replies": [], "engine": None, "themes": {},
         "plan": {"continuation": ["Nf3", "Nc6", "Bc4"],
                  "branches": [{"after": ["Nf3"], "reply": "Nf6", "share_pct": 20.0,
                                "our_answer": "Nxe5", "main_line_answer": "Bc4",
                                "plan_changes": True}],
                  "recurring_ideas": [{"token": "Bc4", "reach_pct": 56.0}]}}
ok = ("We develop and aim Bc4 at f7. If they play 2...Nf6 (20%), we take with 3.Nxe5 "
      "instead; Bc4 comes in 56% of the lines below.")
check("plan moves and percentages are grounded", validate(ok, None, ["Nf3"], facts) == [],
      str(validate(ok, None, ["Nf3"], facts)))
bad = "Later Ng5 hits f7, and 3...Bc5 is answered 45% of the time."  # Ng5: not legal from either anchor
errs = validate(bad, None, [], facts)
check("an invented move and percentage are flagged",
      any("Ng5" in e for e in errs) and any("45" in e for e in errs), str(errs))
check("chapter intro: grounded text passes",
      validate_chapter("We play 2.Nf3 and 3.Bc4; against 2...Nf6 we grab with 3.Nxe5.", [facts]) == [])
check("chapter intro: an invented move is flagged",
      validate_chapter("The plan is Qh5 and Bxf7.", [facts]) != [])

print("\nboard facts (the QGA position after 12...e6, the 2026-10-26 'piece up' note):")
from annotate.facts import board_facts  # noqa: E402

qga = board(*"d4 d5 c4 dxc4 e4 Nf6 e5 Nd5 Bxc4 Bf5 Nc3 Nb4 Qa4+ N8c6 d5 a6 dxc6 Nc2+ Kf1 b5 "
               "Bxf7+ Kxf7 Qb3+ e6".split())
bf = board_facts(qga, chess.BLACK)
check("material: Black is a pawn down", bf["material"]["balance_for_us"] == -1
      and bf["material"]["summary"] == "we are a pawn down", str(bf["material"]))
check("the forked rook on a1 is their loose piece",
      any(s.startswith("rook on a1 (attacked by knight on c2, undefended") for s in bf["their_loose_pieces"]),
      str(bf["their_loose_pieces"]))
check("both kings stranded", not bf["our_king"]["castled"] and not bf["our_king"]["can_still_castle"]
      and bf["their_king"]["square"] == "f1")
tk = board(*"e4 e5 Nf3 Nc6 Bc4 Nf6 d4 exd4 Ng5 d5 exd5 Na5 Qe2+ Be7 d6 Nxc4 Qxc4 Qxd6 Qxf7+".split())
check("a piece guarded against the king is not loose (Qxf7+ with Ng5)",
      not any("f7" in s for s in board_facts(tk, chess.WHITE)["our_loose_pieces"]))

pos_facts = dict(facts, position_after_our_move=bf)
check("material claim contradicting the board is flagged",
      any("material claim" in e for e in validate("We are a piece up and winning.", None, [], pos_facts)))
check("a claim matching the board passes",
      validate("We are a pawn down, but the rook on a1 is hanging.", None, [], pos_facts) == [])
check("'giving up the queen' is not a material claim",
      validate("The engine's reply is giving up the queen to stop the fork.", None, [], pos_facts) == [])
check("'they are a pawn up' is read from their side",
      validate("They are a pawn up, but their king is stuck.", None, [], pos_facts) == [])

print(f"\n{'ALL PASS' if FAILS == 0 else f'{FAILS} FAILURES'}")
sys.exit(1 if FAILS else 0)
