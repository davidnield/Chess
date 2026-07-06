"""Synthetic test for stage3 run_backwards_induction cycle handling — verifies the
fix for the transposition-loop bug against the EXACT production function (imported,
not copied; template: _test_winpos.py).

The bug: the position graph contains genuine cycles (reversible piece shuffles, e.g.
1.Nf3 Nf6 2.Ng1 Ng8 returns to the exact start position), which Kahn's topological
sort cannot order. Cycle members and all their ancestors fell through to a single
arbitrary-order fallback sweep where children could be unvalued (silently replaced by
leaf/empirical values) and results were frozen at first computation — measured to
mis-select ~10k moves in the pure-eval rep, including 1.g4 over 1.e3 at the root.

Fixture (all REAL positions/hashes so EPDs parse and the root IS the start position):

  START (ours): e3 -> leaf(0.596) | g4 -> leaf(0.545) | Nf3 -> P1 [edge score 0.70,
                a seductive trap: only visible if P1 is unvalued at selection time]
  P1 (opp, after 1.Nf3):        Nf6 -> P2 (total 900) | d5 -> leaf(0.48) (total 100)
  P2 (ours, after 1.Nf3 Nf6):   Ng1 -> P3 (0.50)      | d4 -> leaf(0.52)
  P3 (opp, after 2.Ng1):        Ng8 -> START (the cycle) | d5 -> leaf(0.45)

With prior_strength=0, min_move_games=0, all weights 0, no eval DB, the values are
exact hand arithmetic. {START, P1, P2, P3} is one 4-node SCC. Unique fixpoint:

  V(START) = max(0.596, 0.545, V(P1)) = 0.596  (best move e3)
  V(P3) = 0.5*V(START) + 0.5*0.45 = 0.523
  V(P2) = max(V(P3), 0.52) = 0.523             (best move Ng1 — legitimately!)
  V(P1) = 0.9*V(P2) + 0.1*0.48 = 0.5187
  value_worst: START=0.596 (leaf-e3), P3=min(0.596,0.45)=0.45, P2=0.45, P1=0.45

Run: .venv/Scripts/python.exe python/_test_stage3_cycles.py
(On PRE-FIX code this fails: the arbitrary fallback order values START before P1,
sees the raw 0.70 edge score for Nf3, and selects it.)
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import run_backwards_induction, zobrist_int64

TOL_ANALYTIC = 1e-6
TOL_CONSIST = 1e-9

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
    return {
        "parent_hash": zobrist_int64(parent),
        "child_hash": zobrist_int64(child),
        "move_san": san,
        "parent_epd": parent.epd(),
        "white_score_avg": score,
        "total": total,
        "draws": 0,
    }


def cyclic_fixture() -> tuple[list[dict], dict[str, int]]:
    start = board_after()
    p1 = board_after("Nf3")
    p2 = board_after("Nf3", "Nf6")
    p3 = board_after("Nf3", "Nf6", "Ng1")
    edges = [
        edge(start, "e3", 0.596, 5000),
        edge(start, "g4", 0.545, 5000),
        edge(start, "Nf3", 0.70, 5000),     # seductive raw edge score
        edge(p1, "Nf6", 0.52, 900),
        edge(p1, "d5", 0.48, 100),
        edge(p2, "Ng1", 0.50, 500),
        edge(p2, "d4", 0.52, 500),
        edge(p3, "Ng8", 0.50, 500),         # closes the cycle back to START
        edge(p3, "d5", 0.45, 500),
    ]
    hashes = {"start": zobrist_int64(start), "p1": zobrist_int64(p1),
              "p2": zobrist_int64(p2), "p3": zobrist_int64(p3)}
    # Sanity: the Ng8 edge really does return to the exact start hash.
    assert edges[7]["child_hash"] == hashes["start"]
    return edges, hashes


def acyclic_fixture() -> tuple[list[dict], int]:
    start = board_after()
    edges = [
        edge(start, "a3", 0.51, 500),
        edge(start, "h3", 0.49, 500),
    ]
    return edges, zobrist_int64(start)


def main() -> None:
    common = dict(prior_strength=0.0, min_move_games=0)

    # ── Case 1: the 4-node cycle through the start position ──────────────────
    edges, h = cyclic_fixture()
    (values, best_moves, _bf, _be, _bd, _bc, _cp, memo_pot, _ce, value_worst,
     _vr, _pe, _ps, _prior, _aug) = run_backwards_induction(edges, "white", **common)

    check(len(values) == 4 and all(v == v and abs(v) < 10 for v in values.values()),
          "all 4 cycle positions valued and finite")
    check(best_moves.get(h["start"]) == "e3",
          f"root best_move == 'e3' (got {best_moves.get(h['start'])!r}) — "
          f"the cycle-poisoning trap")
    expected = {"start": 0.596, "p3": 0.523, "p2": 0.523, "p1": 0.5187}
    for name, want in expected.items():
        got = values.get(h[name])
        check(got is not None and abs(got - want) < TOL_ANALYTIC,
              f"V({name}) == {want} (got {got})")
    check(best_moves.get(h["p2"]) == "Ng1",
          f"P2 best_move == 'Ng1' (repetition legitimately best; got "
          f"{best_moves.get(h['p2'])!r})")
    worst_expected = {"start": 0.596, "p3": 0.45, "p2": 0.45, "p1": 0.45}
    for name, want in worst_expected.items():
        got = value_worst.get(h[name])
        check(got is not None and abs(got - want) < TOL_ANALYTIC,
              f"value_worst({name}) == {want} (got {got})")
    # Consistency: our-turn nodes' stored value equals the chosen child's final value
    # (leaf children fall back to the edge score).
    for name, board_sans in (("start", ()), ("p2", ("Nf3", "Nf6"))):
        b = board_after(*board_sans)
        bm = best_moves.get(h[name])
        if bm is None:
            check(False, f"{name} has no best_move")
            continue
        child = b.copy()
        child.push_san(bm)
        ch = zobrist_int64(child)
        child_final = values.get(ch)
        if child_final is None:  # leaf: recover the edge's score
            child_final = next(e["white_score_avg"] for e in edges
                               if e["parent_hash"] == h[name] and e["move_san"] == bm)
        check(abs(values[h[name]] - child_final) < TOL_CONSIST,
              f"{name}: stored value == chosen child's final value")
    check(memo_pot.get(h["start"]) is not None
          and memo_pot[h["start"]] == memo_pot[h["start"]],
          "memo_pot(start) finite")

    # ── Case 2: purely acyclic fixture (guards the untouched Kahn path) ──────
    edges2, start_h = acyclic_fixture()
    (values2, best2, *_r2) = run_backwards_induction(edges2, "white", **common)
    check(abs(values2[start_h] - 0.51) < TOL_ANALYTIC and best2[start_h] == "a3",
          "acyclic fixture: V(start)==0.51, best=='a3'")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} "
          f"({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
