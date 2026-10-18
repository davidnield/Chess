"""Synthetic test for Stage 3 priced constraints: --force-move / --avoid-move
(2026-10-18). Imports the production run_backwards_induction and the CLI parser
helpers.

  A  white root: a4 (0.565) keys above e4 (0.560); forcing e4 books e4.
  B  a forced move bypasses the gates (e4's child eval 0.20 fails the absolute gate).
  C  avoiding a4 books e4; avoiding every move leaves the book (no best move).
  D  black mirror: at 1.e4, b6 beats c5; forcing c5 books c5, avoiding b6 books c5.
  E  a forced move that is not an edge at the position is refused.
  F  no constraints (None or {}) is identical to a build without the feature.
  G  parse_move_constraint / move_constraints: move numbers optional, ': e4' is the
     start position, an illegal LINE or SAN and an opponent-turn position are refused,
     check marks normalise ('Qh5' vs 'Qh5+').
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import (move_constraints,  # noqa: E402
                                        parse_move_constraint,
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


def best(edges, colour, node, **kw):
    r = run_backwards_induction(edges, colour, **{**COMMON, **kw})
    return r[1].get(node)


def refused(fn) -> bool:
    try:
        fn()
    except ValueError:
        return True
    return False


def main() -> None:
    start = chess.Board()
    sh = zobrist_int64(start)
    ea = [edge(start, "a4", 0.565, 500), edge(start, "e4", 0.560, 5000)]

    check(best(ea, "white", sh) == "a4", "A: unconstrained root books a4")
    check(best(ea, "white", sh, forced_moves={sh: "e4"}) == "e4", "A: forcing e4 books e4")

    ev = {child(start, "a4"): 0.55, child(start, "e4"): 0.20}
    gates = dict(eval_lookup=ev, eval_weight=0.0, robustness_floor=0.25,
                 gate_metric="eval", gate_anchor="even")
    check(best(ea, "white", sh, **gates) == "a4", "B: the gate rejects e4 unforced")
    check(best(ea, "white", sh, forced_moves={sh: "e4"}, **gates) == "e4",
          "B: a forced move bypasses the gates")

    check(best(ea, "white", sh, avoid_moves={sh: {"a4"}}) == "e4", "C: avoiding a4 books e4")
    check(best(ea, "white", sh, avoid_moves={sh: {"a4", "e4"}}) is None,
          "C: avoiding every move leaves the book there")

    e4n = start.copy(); e4n.push_san("e4")
    eh = zobrist_int64(e4n)
    eb = [edge(start, "e4", 0.55, 9000), edge(e4n, "b6", 0.480, 300),
          edge(e4n, "c5", 0.500, 4000)]
    check(best(eb, "black", eh) == "b6", "D: black unconstrained books b6")
    check(best(eb, "black", eh, forced_moves={eh: "c5"}) == "c5", "D: black forced c5")
    check(best(eb, "black", eh, avoid_moves={eh: {"b6"}}) == "c5", "D: black avoids b6")

    check(refused(lambda: run_backwards_induction(ea, "white", forced_moves={sh: "h4"},
                                                  **COMMON)),
          "E: a forced move that is not an edge is refused")

    base = run_backwards_induction(ea, "white", **COMMON)
    for kw in ({"forced_moves": None, "avoid_moves": None},
               {"forced_moves": {}, "avoid_moves": {}}):
        off = run_backwards_induction(ea, "white", **COMMON, **kw)
        nodes = [sh, child(start, "a4"), child(start, "e4")]
        check(all(off[0].get(n) == base[0].get(n) and off[1].get(n) == base[1].get(n)
                  for n in nodes),
              f"F: {kw} is identical to the build without the feature")

    b1, s1 = parse_move_constraint("1.e4 c5 2.Nf3 d6: d4")
    b2, s2 = parse_move_constraint("e4 c5 Nf3 d6 :d4")
    check(zobrist_int64(b1) == zobrist_int64(b2) and s1 == s2 == "d4",
          "G: move numbers are optional")
    b0, s0 = parse_move_constraint(": e4")
    check(zobrist_int64(b0) == sh and s0 == "e4", "G: ': e4' is the start position")
    check(refused(lambda: parse_move_constraint("1.e4 e5 2.Ke3: Nf6")), "G: illegal LINE refused")
    check(refused(lambda: parse_move_constraint("1.e4: e4")), "G: illegal SAN refused")
    check(refused(lambda: parse_move_constraint("1.e4 e5")), "G: missing ':' refused")
    check(refused(lambda: move_constraints(["1.e4: c5"], "white")),
          "G: an opponent-turn constraint is refused")
    mc = move_constraints(["1.e4: c5", "1.e4 c5 2.Nf3: d6"], "black")
    check(mc == {eh: ["c5"], zobrist_int64(chess.Board(
        "rnbqkbnr/pp1ppppp/8/2p5/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2")): ["d6"]},
          "G: move_constraints keys each spec by its position")
    fm = chess.Board("rnbqkbnr/pppp1ppp/8/4p3/2B1P3/8/PPPP1PPP/RNBQK1NR w KQkq - 2 3")
    _, sq = parse_move_constraint("1.e4 e5 2.Bc4 Nc6 3.Qh5 Nf6: Qxf7")
    check(sq == "Qxf7#", f"G: SAN is normalised the python-chess way (got {sq})")
    del fm

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
