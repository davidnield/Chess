"""Synthetic test for Stage 3 rebuild stickiness: --incumbent / --stick-margin
(2026-10-17). Imports the production run_backwards_induction.

The previous book's move at a position is kept unless the best candidate's key
beats it by more than stick_margin. Applied after the gates, among the candidates.

  A  white root: a4 (0.565, 500 games) keys 0.005 above e4 (0.560, 5000 games).
     incumbent e4: margin 0.01 keeps e4 (kept_incumbent True, margin -0.005,
     runner-up a4); margin 0.004 or 0 books a4.
  B  incumbent already the best move: nothing changes, kept_incumbent False.
  C  incumbent rejected by the absolute gate (child eval 0.20 < 0.5 - 0.25): never
     kept, however large the margin.
  D  incumbent move not among the candidates at all: ignored.
  E  black mirror: b6 beats c5 by 0.005 for black; incumbent c5 at 0.01 keeps c5.
  F  no incumbent = identical outputs to a build without the feature; a positive
     margin without an incumbent, or a negative margin, is refused.
  G  check marks: an incumbent recorded as 'e4+' matches the candidate 'e4'.
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import (IncumbentMoves,  # noqa: E402
                                        run_backwards_induction, zobrist_int64)

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


def child(parent: chess.Board, san: str) -> int:
    b = parent.copy()
    b.push_san(san)
    return zobrist_int64(b)


COMMON = dict(prior_strength=0.0, min_move_games=0, robustness_floor=1.0,
              gate_rel_floor=1.0)


def run(edges, colour, node, **kw):
    r = run_backwards_induction(edges, colour, **{**COMMON, **kw})
    bm, ru, mg, kept = r[1], r[-3], r[-2], r[-1]
    return bm.get(node), ru.get(node), mg.get(node), bool(kept.get(node))


def main() -> None:
    start = chess.Board()
    sh = zobrist_int64(start)
    ea = [edge(start, "a4", 0.565, 500), edge(start, "e4", 0.560, 5000)]
    inc_e4 = IncumbentMoves([sh], ["e4"])

    m, ru, mg, kept = run(ea, "white", sh, incumbent=inc_e4, stick_margin=0.01)
    check(m == "e4" and kept and ru == "a4" and abs(mg + 0.005) < 1e-9,
          f"A: margin 0.01 keeps the incumbent (e4, kept, runner a4, margin -0.005; "
          f"got {m}, {kept}, {ru}, {mg})")
    m, _, _, kept = run(ea, "white", sh, incumbent=inc_e4, stick_margin=0.004)
    check(m == "a4" and not kept, f"A: margin 0.004 < gap 0.005 books a4 (got {m})")
    m, _, _, kept = run(ea, "white", sh, incumbent=inc_e4, stick_margin=0.0)
    check(m == "a4" and not kept, f"A: margin 0 keeps the incumbent only on exact ties (got {m})")

    m, _, _, kept = run(ea, "white", sh, incumbent=IncumbentMoves([sh], ["a4"]),
                        stick_margin=0.01)
    check(m == "a4" and not kept, f"B: incumbent already best -> unchanged (got {m}, kept={kept})")

    ev = {child(start, "a4"): 0.55, child(start, "e4"): 0.20}
    m, _, _, kept = run(ea, "white", sh, incumbent=inc_e4, stick_margin=0.5,
                        eval_lookup=ev, eval_weight=0.0, robustness_floor=0.25,
                        gate_metric="eval", gate_anchor="even")
    check(m == "a4" and not kept,
          f"C: a gate-rejected incumbent is never kept, even at margin 0.5 (got {m})")

    m, _, _, kept = run(ea, "white", sh, incumbent=IncumbentMoves([sh], ["h4"]),
                        stick_margin=0.5)
    check(m == "a4" and not kept, f"D: incumbent not a candidate -> ignored (got {m})")

    e4n = start.copy(); e4n.push_san("e4")
    eh = zobrist_int64(e4n)
    eb = [edge(start, "e4", 0.55, 9000), edge(e4n, "b6", 0.495, 300),
          edge(e4n, "c5", 0.500, 4000)]
    m, _, _, kept = run(eb, "black", eh, incumbent=IncumbentMoves([eh], ["c5"]),
                        stick_margin=0.01)
    check(m == "c5" and kept, f"E: black keeps its incumbent c5 within 0.01 (got {m})")

    base = run_backwards_induction(ea, "white", **COMMON)
    off = run_backwards_induction(ea, "white", incumbent=None, stick_margin=0.0, **COMMON)
    check(base[1].get(sh) == off[1].get(sh) and base[0].get(sh) == off[0].get(sh)
          and not bool(off[-1].get(sh)),
          "F: no incumbent is identical to the build without the feature")
    for kw, label in (({"stick_margin": 0.01}, "positive margin without an incumbent"),
                      ({"incumbent": inc_e4, "stick_margin": -0.01}, "negative margin")):
        try:
            run_backwards_induction(ea, "white", **COMMON, **kw)
            check(False, f"F: {label} refused")
        except ValueError:
            check(True, f"F: {label} refused")

    m, _, _, kept = run(ea, "white", sh, incumbent=IncumbentMoves([sh], ["e4+"]),
                        stick_margin=0.01)
    check(m == "e4" and kept, f"G: check-marked incumbent 'e4+' matches 'e4' (got {m})")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
