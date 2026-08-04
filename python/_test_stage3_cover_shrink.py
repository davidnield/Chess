"""Synthetic test for --cover-prior (thin-data shrinkage of cover_eff).

cover_eff = cover_depth / (1 + mem_nodes) is biased HIGH at sparse nodes for the
same reason raw Simpson concentration is: replies that fragmented below the
pool's per-edge min_games never appear, so the survivors renormalise to 100% and
a thin node reads as a narrow, efficient rail. forcingness already corrects this
by shrinking toward a baseline with a pseudocount; cover_eff did not.

Measured shapes this reproduces (children are leaves, so depth 1 either way):

  thin node        1 surviving reply          -> depth 1.0 / (1+1) = 0.500
  well-sampled     2 replies at 90/10         -> depth 1.0 / (1+2) = 0.333

i.e. the thin node outscores the well-sampled one by 50% purely because its
alternatives were pruned. Shrinking both toward 0.22 with pseudocount k, weighted
by each node's own game count, restores the correct ordering.

Fixture (White to move at start): our two candidates lead to those two nodes.
Values are held equal so the coverage term alone decides.
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


COMMON = dict(prior_strength=0.0, min_move_games=0, eval_weight=0.0,
              robustness_floor=1.0, gate_rel_floor=1.0, cover_weight=1.0)


def main() -> None:
    start = board_after()
    sh = zobrist_int64(start)
    thin, dense = board_after("a3"), board_after("e4")      # opponent to move at each

    edges = [
        # our two candidates, identical value so coverage alone decides
        edge(start, "a3", 0.50, 120),
        edge(start, "e4", 0.50, 10000),
        # THIN node: one surviving reply (the others fell below the pool's floor)
        edge(thin, "e5", 0.50, 55),
        # DENSE node: two well-sampled replies, 90/10
        edge(dense, "e5", 0.50, 9000),
        edge(dense, "c5", 0.50, 1000),
    ]

    raw_v, raw_bm, *_ = run_backwards_induction(edges, "white", cover_prior=0.0, **COMMON)
    shr_v, shr_bm, *_ = run_backwards_induction(edges, "white", cover_prior=200.0,
                                                cover_baseline=0.22, **COMMON)
    dflt_v, dflt_bm, *_ = run_backwards_induction(edges, "white", **COMMON)

    check(raw_bm.get(sh) == "a3",
          f"raw ratio picks the THIN line — the bias being fixed "
          f"(root={raw_bm.get(sh)!r}, want 'a3')")
    check(shr_bm.get(sh) == "e4",
          f"shrunk picks the WELL-SAMPLED line (root={shr_bm.get(sh)!r}, want 'e4')")

    keys = [sh, zobrist_int64(thin), zobrist_int64(dense)]
    check(all(raw_v[k] == dflt_v[k] and raw_bm.get(k) == dflt_bm.get(k) for k in keys),
          "omitting the flag is bit-identical to --cover-prior 0")

    # A leaf must keep cover_eff 0 even under shrinkage, or exiting book early
    # would collect the baseline as a bonus.
    leaf_edges = [
        edge(start, "a3", 0.50, 120),            # a3 -> nothing recorded below: bare leaf
        edge(start, "e4", 0.50, 10000),
        edge(dense, "e5", 0.50, 9000),
        edge(dense, "c5", 0.50, 1000),
    ]
    lv0, lb0, *_ = run_backwards_induction(leaf_edges, "white", cover_prior=0.0, **COMMON)
    lv1, lb1, *_ = run_backwards_induction(leaf_edges, "white", cover_prior=200.0,
                                           cover_baseline=0.22, **COMMON)
    check(lb0.get(sh) == "e4" and lb1.get(sh) == "e4",
          f"bare leaf stays unattractive under shrinkage "
          f"(raw={lb0.get(sh)!r}, shrunk={lb1.get(sh)!r}, want 'e4' both)")

    # Baseline 0 must not resurrect the thin line: shrinking toward 0 is still a
    # penalty on sparse nodes, never a bonus.
    z_v, z_bm, *_ = run_backwards_induction(edges, "white", cover_prior=200.0,
                                            cover_baseline=0.0, **COMMON)
    check(z_bm.get(sh) == "e4",
          f"baseline 0 also prefers the well-sampled line (root={z_bm.get(sh)!r})")

    bad = [lbl for ok, lbl in _checks if not ok]
    print(f"\n{len(_checks) - len(bad)}/{len(_checks)} checks passed")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
