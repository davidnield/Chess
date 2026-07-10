"""Synthetic test for --gate-rel-floor (the RELATIVE refutation gate): a move that
passes the ABSOLUTE eval gate but concedes a large eval margin vs the best sibling
candidate (the 8...Nd4 advantage-squandering pattern) must be REJECTED when the
relative gate is on and ACCEPTED when it is disabled. Imports the production
run_backwards_induction (template: _test_gate_eval.py).

White fixture (White to move at start; prior ~0.61, absolute floor 0.25 -> bar 0.36):
  e4 -> child eval 0.60 (sound leaf; eval-only value 0.60)
  b4 -> child eval 0.41 (passes the absolute gate) but empirically great: its lone
        recorded reply subtree propagates value 0.90, so selection prefers it on value.
  Concession of b4 vs e4 = 0.19 expected score > 0.1 -> relative gate drops it.

Black fixture mirrors the sign asymmetry (best = MIN white score for black).
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
    sh = zobrist_int64(start)

    # ── White fixture ─────────────────────────────────────────────────────────
    sound_child = board_after("e4")
    tempt_child = board_after("b4")
    edges_w = [
        edge(start, "e4", 0.55, 5000),        # sound: modest empirical
        edge(start, "b4", 0.90, 1000),        # tempting: great empirical...
        edge(tempt_child, "e5", 0.90, 1000),  # ...via its recorded reply subtree (propagates 0.90)
    ]
    eval_w = {zobrist_int64(sound_child): 0.60,
              zobrist_int64(tempt_child): 0.41}   # passes absolute bar, concedes 0.19 vs e4
    common = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0,
                  require_eval=True, robustness_floor=0.25, gate_metric="eval")

    (_v, bm_on, *_r) = run_backwards_induction(
        edges_w, "white", eval_lookup=eval_w, gate_rel_floor=0.1, **common)
    (_v2, bm_off, *_r2) = run_backwards_induction(
        edges_w, "white", eval_lookup=eval_w, gate_rel_floor=1.0, **common)
    check(bm_on.get(sh) == "e4",
          f"rel gate 0.1 drops the 0.19-conceding move (root={bm_on.get(sh)!r}, expected 'e4')")
    check(bm_off.get(sh) == "b4",
          f"rel gate disabled keeps old behavior (root={bm_off.get(sh)!r}, expected 'b4')")

    # Guard: concession within the floor (0.60 vs 0.55 = 0.05 < 0.1) -> both survive;
    # the EMPIRICAL winner must be picked (rel gate must not force the engine-best move).
    eval_g = {zobrist_int64(sound_child): 0.60,
              zobrist_int64(tempt_child): 0.55}
    (_v3, bm_guard, *_r3) = run_backwards_induction(
        edges_w, "white", eval_lookup=eval_g, gate_rel_floor=0.1, **common)
    check(bm_guard.get(sh) == "b4",
          f"small concession survives; empirical winner kept (root={bm_guard.get(sh)!r}, expected 'b4')")

    # ── Black fixture (sign asymmetry: best = MIN white score) ────────────────
    e4_node = board_after("e4")
    eh = zobrist_int64(e4_node)
    sound_b = board_after("e4", "e5")
    tempt_b = board_after("e4", "f5")
    edges_b = [
        edge(start, "e4", 0.55, 6000),
        edge(e4_node, "e5", 0.45, 5000),      # sound reply
        edge(e4_node, "f5", 0.10, 1000),      # tempting: opponents crumble...
        edge(tempt_b, "exf5", 0.10, 1000),    # ...(propagates 0.10 white score)
    ]
    eval_b = {zobrist_int64(sound_b): 0.40,   # best for black (lowest white score)
              zobrist_int64(tempt_b): 0.59}   # passes absolute (<= 0.55+0.25) but concedes 0.19
    (_v4, bm_bon, *_r4) = run_backwards_induction(
        edges_b, "black", eval_lookup=eval_b, gate_rel_floor=0.1, **common)
    (_v5, bm_boff, *_r5) = run_backwards_induction(
        edges_b, "black", eval_lookup=eval_b, gate_rel_floor=1.0, **common)
    check(bm_bon.get(eh) == "e5",
          f"black rel gate drops the conceding reply (move={bm_bon.get(eh)!r}, expected 'e5')")
    check(bm_boff.get(eh) == "f5",
          f"black rel gate disabled keeps old behavior (move={bm_boff.get(eh)!r}, expected 'f5')")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
