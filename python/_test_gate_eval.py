"""Synthetic test for --gate-metric eval: an objectively-losing move whose ONLY
recorded opponent reply is favorable (the 3.Bh6 missing-refutation pattern) must be
REJECTED by the eval gate but ACCEPTED by the worst gate. Imports the production
run_backwards_induction (template: _test_winpos.py / _test_stage3_cycles.py).

Fixture (White to move at start):
  good  -> child G, eval 0.60 (sound), recorded reply leads to ~0.55
  trap  -> child T, eval 0.08 (engine says White is lost), but T's ONLY recorded
           reply is favorable (0.90) so value_worst(trap-subtree) ~ 0.90.
The eval gate sees eval(T)=0.08 << prior-floor and rejects `trap`; the worst gate
sees value_worst~0.90 and keeps it. We assert the selected root move differs.
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import run_backwards_induction, zobrist_int64

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def board_after(*sans: str) -> chess.Board:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def edge(parent: chess.Board, san: str, score: float, total: int) -> dict:
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": 0}


def main() -> None:
    start = board_after()
    good_child = board_after("e4")          # sound developing move
    trap_child = board_after("b4")          # stand-in for the "hangs a piece" trap
    trap_reply = board_after("b4", "e5")    # trap's ONLY recorded reply (favorable to us)

    edges = [
        edge(start, "e4", 0.55, 5000),      # good: modest, sound
        edge(start, "b4", 0.90, 300),       # trap: great empirical score...
        edge(trap_child, "e5", 0.90, 300),  # ...because its lone recorded reply is soft
    ]
    # Engine evals (expected white score): e4 sound (0.60), b4 objectively lost (0.08).
    eval_lookup = {
        zobrist_int64(good_child): 0.60,
        zobrist_int64(trap_child): 0.08,
        zobrist_int64(trap_reply): 0.90,
    }
    common = dict(prior_strength=0.0, min_move_games=0, eval_lookup=eval_lookup,
                  eval_weight=1.0, require_eval=True, robustness_floor=0.09)

    # worst gate: value_worst(b4-subtree) ~ 0.90 -> trap PASSES the gate, and with the
    # highest empirical score it wins selection.
    (_v, bm_worst, *_r) = run_backwards_induction(
        edges, "white", gate_metric="worst", **common)
    # eval gate: eval(b4)=0.08 << prior-floor -> trap REJECTED; e4 selected.
    (_v2, bm_eval, *_r2) = run_backwards_induction(
        edges, "white", gate_metric="eval", **common)

    sh = zobrist_int64(start)
    check(bm_worst.get(sh) == "b4",
          f"worst gate keeps the trap (root={bm_worst.get(sh)!r}, expected 'b4')")
    check(bm_eval.get(sh) == "e4",
          f"eval gate rejects the trap, plays sound (root={bm_eval.get(sh)!r}, expected 'e4')")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
