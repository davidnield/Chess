"""Synthetic test for the Stage-3 IMPERFECT-RECALL model (--recall-midpoint /
--recall-power / --recall-default-reach). A booked move is played only with
probability r(reach); otherwise we wing it and play the population's move, so

    value(node) = r * (prescriptive) + (1 - r) * (population mixture here)

Imports the production run_backwards_induction (template: _test_stage3_learn.py).

Two fixtures:
  A. one our-node, two leaf moves -- pins the blend arithmetic and proves the
     LOCAL argmax cannot move (the fallback term is constant in the candidate).
  B. root -> two opponent replies -> two our-nodes, one a trap that scores well
     only if remembered. Perfect recall prefers the trap; zero recall prefers
     the line that survives being forgotten. This is the whole point of the
     feature: a deep trap stops paying for itself at the ancestor that has to
     walk into it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import (recall_weight, run_backwards_induction,
                                        zobrist_int64)

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def close(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(a - b) <= tol


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


# raw empirical leaf values: prior_strength 0 and eval_weight 0 make a leaf's
# value exactly its white_score_avg, so every number below is hand-computable.
COMMON = dict(prior_strength=0.0, min_move_games=0, eval_weight=0.0,
              require_eval=False)


def test_curve() -> None:
    print("\nA. recall_weight curve")
    m, p = 0.001, 2.0
    check(close(recall_weight(m, m, p), 0.5), "r(midpoint) == 0.5")
    check(recall_weight(0.0, m, p) == 0.0, "r(0) == 0")
    check(recall_weight(0.5, 0.0, p) == 1.0, "midpoint<=0 disables -> r == 1")
    check(recall_weight(0.5, m, 0.0) == 1.0, "power<=0 disables -> r == 1")
    vals = [recall_weight(x, m, p) for x in (1e-9, 1e-6, 1e-4, 1e-3, 1e-2, 1.0)]
    check(all(a < b for a, b in zip(vals, vals[1:])), "strictly increasing in reach")
    check(all(0.0 <= v <= 1.0 for v in vals), "bounded to [0,1]")
    check(vals[0] < 1e-10 and vals[-1] > 0.999999, "saturates at both extremes")
    # the two algebraic branches must agree across the ratio==1 seam
    lo, hi = recall_weight(m * (1 - 1e-9), m, p), recall_weight(m * (1 + 1e-9), m, p)
    check(close(lo, 0.5, 1e-8) and close(hi, 0.5, 1e-8), "branches agree at the seam")
    # reach 1e-6 (the plan export's epsilon) is effectively no recall
    check(recall_weight(1e-6, m, p) < 1e-5, "export epsilon 1e-6 -> r ~ 0")


def test_blend_and_local_argmax() -> None:
    print("\nB. blend arithmetic and local invariance")
    start = board_after()
    sh = zobrist_int64(start)
    # A: 0.60 x 9000   B: 0.50 x 1000   -> mixture 0.59, prescriptive 0.60
    edges = [edge(start, "e4", 0.60, 9000), edge(start, "d4", 0.50, 1000)]
    mix, best = 0.59, 0.60

    (v0, bm0, *_r) = run_backwards_induction(edges, "white", **COMMON)
    check(close(v0[sh], best), f"recall off -> prescriptive {best}")

    def run(reach, mid=0.001, dflt=0.0):
        return run_backwards_induction(
            edges, "white", learn_reach={sh: reach} if reach is not None else None,
            recall_midpoint=mid, recall_default_reach=dflt, **COMMON)

    (v1, bm1, *_r) = run(1.0)                       # r ~ 1
    check(close(v1[sh], best, 1e-5), "reach 1.0 (r~1) -> prescriptive")
    (v2, bm2, *_r) = run(0.0)                       # r = 0
    check(close(v2[sh], mix), f"reach 0 (r=0) -> population mixture {mix}")
    (v3, bm3, *_r) = run(0.001)                     # r = 0.5 exactly
    check(close(v3[sh], 0.5 * best + 0.5 * mix), "reach == midpoint -> half and half")
    (v4, bm4, *_r) = run(None, dflt=0.0)            # absent node -> default reach
    check(close(v4[sh], mix), "node absent from plan export -> default reach applies")
    (v5, bm5, *_r) = run(None, dflt=1.0)
    check(close(v5[sh], best, 1e-5), "--recall-default-reach 1.0 -> prescriptive")

    # the fallback term does not depend on the candidate, so it cannot move the
    # argmax AT this node -- only the value it hands its parent.
    picks = {bm0[sh], bm1[sh], bm2[sh], bm3[sh], bm4[sh], bm5[sh]}
    check(picks == {"e4"}, "local best_move identical at every r (argmax invariant)")


def test_parent_choice_flips() -> None:
    print("\nC. recall flips the PARENT's plan (the point of the feature)")
    start = board_after()
    e4, d4 = board_after("e4"), board_after("d4")
    our_x, our_y = board_after("e4", "e5"), board_after("d4", "d5")
    sh, xh, yh = zobrist_int64(start), zobrist_int64(our_x), zobrist_int64(our_y)

    edges = [
        edge(start, "e4", 0.50, 5000), edge(start, "d4", 0.50, 5000),
        edge(e4, "e5", 0.50, 5000),    edge(d4, "d5", 0.50, 5000),
        # trap: brilliant if remembered (0.80), dreadful if not (mixture 0.305)
        edge(our_x, "Nf3", 0.80, 100), edge(our_x, "Nc3", 0.30, 9900),
        # steady: 0.70 remembered, 0.65 forgotten
        edge(our_y, "c4", 0.70, 5000), edge(our_y, "Nf3", 0.60, 5000),
    ]

    (v0, bm0, *_r) = run_backwards_induction(edges, "white", **COMMON)
    check(close(v0[xh], 0.80) and close(v0[yh], 0.70), "perfect recall: 0.80 vs 0.70")
    check(bm0[sh] == "e4", "perfect recall -> root walks into the trap line")

    # root stays memorable (reach 1.0); the two deep nodes are never rehearsed
    (v1, bm1, *_r) = run_backwards_induction(
        edges, "white", learn_reach={sh: 1.0, xh: 0.0, yh: 0.0},
        recall_midpoint=0.001, **COMMON)
    check(close(v1[xh], 0.305), "zero recall: trap node collapses to 0.305")
    check(close(v1[yh], 0.65), "zero recall: steady node holds 0.65")
    check(bm1[sh] == "d4", "zero recall -> root abandons the trap for the steady line")
    check(bm1[xh] == "Nf3" and bm1[yh] == "c4",
          "the deep nodes still PRESCRIBE their best move (only the value moved)")


def main() -> None:
    test_curve()
    test_blend_and_local_argmax()
    test_parent_choice_flips()
    bad = [l for ok, l in _checks if not ok]
    print(f"\n{len(_checks) - len(bad)}/{len(_checks)} checks passed")
    if bad:
        print("FAILED: " + "; ".join(bad))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
