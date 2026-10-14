"""Synthetic test for Stage 3 --select-margin (2026-10-09). Imports the production
run_backwards_induction.

Near-ties are decided by sampling noise: every change to the value model so far
flipped decisions whose value gap was ~0.001. With a margin, candidates within it
of the best key are tied and the tie goes to the MOST-PLAYED move.

  A  white root: a4 (score 0.565, 500 games) beats e4 (0.560, 5000 games) by
     0.005 on the key. margin 0 -> a4; margin 0.01 -> e4 (most played);
     margin 0.004 -> a4 (e4 outside the band).
  B  black: after 1.e4, b6 (white score 0.495, 300 games) beats c5 (0.500,
     4000 games) for black by 0.005; margin 0.01 -> c5.
  C  equal game counts inside the band fall back to the key, then SAN, so the
     choice does not depend on input row order.
  D  negative margin is refused.
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import run_backwards_induction, zobrist_int64  # noqa: E402

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def edge(parent: chess.Board, san: str, score: float, total: int) -> dict:
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": 0}


COMMON = dict(prior_strength=0.0, min_move_games=0, robustness_floor=1.0,
              gate_rel_floor=1.0)


def pick(edges, colour, node, **kw):
    _v, bm, *_ = run_backwards_induction(edges, colour, **COMMON, **kw)
    return bm.get(node)


def main() -> None:
    start = chess.Board()
    sh = zobrist_int64(start)

    ea = [edge(start, "a4", 0.565, 500), edge(start, "e4", 0.560, 5000)]
    check(pick(ea, "white", sh) == "a4", "A: no margin -> best key (a4)")
    check(pick(ea, "white", sh, select_margin=0.0) == "a4", "A: margin 0 is the default")
    check(pick(ea, "white", sh, select_margin=0.01) == "e4",
          "A: margin 0.01 -> most-played move in the band (e4)")
    check(pick(ea, "white", sh, select_margin=0.004) == "a4",
          "A: margin 0.004 -> e4 is outside the band (a4)")

    e4n = start.copy(); e4n.push_san("e4")
    eh = zobrist_int64(e4n)
    eb = [edge(start, "e4", 0.55, 9000), edge(e4n, "b6", 0.495, 300),
          edge(e4n, "c5", 0.500, 4000)]
    check(pick(eb, "black", eh) == "b6", "B: black, no margin -> best for black (b6)")
    check(pick(eb, "black", eh, select_margin=0.01) == "c5",
          "B: black, margin 0.01 -> most-played (c5)")

    ec1 = [edge(start, "d4", 0.560, 1000), edge(start, "e4", 0.562, 1000),
           edge(start, "c4", 0.561, 1000)]
    ec2 = list(reversed(ec1))
    p1 = pick(ec1, "white", sh, select_margin=0.01)
    p2 = pick(ec2, "white", sh, select_margin=0.01)
    check(p1 == p2 == "e4",
          f"C: equal counts -> key decides, independent of row order ({p1}, {p2})")

    # E -- the runner_up / margin output columns (last two elements of the result)
    r0 = run_backwards_induction(ea, "white", **COMMON)
    r1 = run_backwards_induction(ea, "white", select_margin=0.01, **COMMON)
    ru0, m0 = r0[-2].get(sh), r0[-1].get(sh)
    ru1, m1 = r1[-2].get(sh), r1[-1].get(sh)
    check(ru0 == "e4" and abs(m0 - 0.005) < 1e-9,
          f"E: runner_up/margin without a band (e4, +0.005; got {ru0}, {m0})")
    check(ru1 == "a4" and abs(m1 + 0.005) < 1e-9,
          f"E: band booked the lower key, so margin is negative (a4, -0.005; got {ru1}, {m1})")

    try:
        pick(ea, "white", sh, select_margin=-0.01)
        check(False, "D: negative margin refused")
    except ValueError:
        check(True, "D: negative margin refused")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
