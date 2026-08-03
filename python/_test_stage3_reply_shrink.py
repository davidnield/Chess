"""Synthetic test for --reply-shrink.

The opponent-node mean divides by the SURVIVING replies, so when a position's
alternatives fragment below the pool's min_games the survivors get renormalised
to 100%. Measured on real data: after 1.e4 c5 2.Nf3 d6 3.c3 Nf6 4.Ng5 h6 5.Nf3,
124 games reached the position but only ...Nxe4 (50 games) survived, so a reply
played ~40% of the time was modelled at 100% and its +294cp eval propagated three
plies undiluted — the whole line's value equalled that leaf eval to 6 decimals,
beating 4.Be2 even though Ng5 is 60cp worse.

--reply-shrink blends the mean toward the node's OWN engine eval by the fraction
of reply mass that is missing (coverage c = surviving mass / mass that reached
the node; weight w = 1 - strength*(1-c)).

Fixture (White to move at start), mirroring that shape:
  e4  10,000 games -> node eval 0.55, replies total 9,500  (coverage 0.95)
                        its lone reply leads to a leaf worth 0.60
  d4     100 games -> node eval 0.40, replies total    20  (coverage 0.20)
                        its lone reply leads to a leaf worth 0.95  <- the "blunder"
  strength 0: d4 = 0.95 > e4 = 0.60           -> picks the sparse line
  strength 1: d4 = 0.2*0.95 + 0.8*0.40 = 0.51
              e4 = 0.95*0.60 + 0.05*0.55 = 0.5975 -> picks the dense line
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


COMMON = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0,
              require_eval=True, robustness_floor=1.0, gate_rel_floor=1.0)


def main() -> None:
    start = board_after()
    sh = zobrist_int64(start)
    e4n, d4n = board_after("e4"), board_after("d4")
    e4_leaf, d4_leaf = board_after("e4", "e5"), board_after("d4", "d5")

    edges = [
        edge(start, "e4", 0.55, 10000),
        edge(start, "d4", 0.55, 100),
        edge(e4n, "e5", 0.60, 9500),    # coverage 9500/10000 = 0.95
        edge(d4n, "d5", 0.95, 20),      # coverage   20/100   = 0.20
    ]
    ev = {zobrist_int64(e4n): 0.55, zobrist_int64(d4n): 0.40,
          zobrist_int64(e4_leaf): 0.60, zobrist_int64(d4_leaf): 0.95}

    v0, bm0, *_ = run_backwards_induction(edges, "white", eval_lookup=ev,
                                          reply_shrink=0.0, **COMMON)
    v1, bm1, *_ = run_backwards_induction(edges, "white", eval_lookup=ev,
                                          reply_shrink=1.0, **COMMON)
    vd, bmd, *_ = run_backwards_induction(edges, "white", eval_lookup=ev, **COMMON)

    check(bm0.get(sh) == "d4",
          f"strength 0 reproduces legacy: sparse line wins (root={bm0.get(sh)!r}, want 'd4')")
    check(bm1.get(sh) == "e4",
          f"strength 1 rejects the sparse line (root={bm1.get(sh)!r}, want 'e4')")

    got_d4, got_e4 = v1[zobrist_int64(d4n)], v1[zobrist_int64(e4n)]
    check(abs(got_d4 - 0.51) < 1e-12,
          f"shrunk sparse node = 0.2*0.95 + 0.8*0.40 = 0.51 (got {got_d4:.12f})")
    check(abs(got_e4 - 0.5975) < 1e-12,
          f"shrunk dense node = 0.95*0.60 + 0.05*0.55 = 0.5975 (got {got_e4:.12f})")

    # The default must be the disabled path, bit-for-bit.
    keys = [sh, zobrist_int64(e4n), zobrist_int64(d4n)]
    # best_moves is an _ObjMap view, not a dict — compare per key, not by identity.
    check(all(v0[k] == vd[k] and bm0.get(k) == bmd.get(k) for k in keys),
          "omitting the flag is bit-identical to --reply-shrink 0")

    # Inert when nothing is missing: replies account for every game that arrived.
    full = [
        edge(start, "e4", 0.55, 10000),
        edge(start, "d4", 0.55, 100),
        edge(e4n, "e5", 0.60, 10000),   # coverage 1.0
        edge(d4n, "d5", 0.95, 100),     # coverage 1.0
    ]
    f0, fb0, *_ = run_backwards_induction(full, "white", eval_lookup=ev,
                                          reply_shrink=0.0, **COMMON)
    f1, fb1, *_ = run_backwards_induction(full, "white", eval_lookup=ev,
                                          reply_shrink=1.0, **COMMON)
    check(all(f0[k] == f1[k] and fb0.get(k) == fb1.get(k) for k in keys),
          "full coverage -> strength 1 is bit-identical to strength 0 (w == 1.0)")

    # A node the eval DB does not cover must be left alone even at full strength.
    ev_partial = {k: val for k, val in ev.items() if k != zobrist_int64(d4n)}
    COMMON_NR = {**COMMON, "require_eval": False}
    p0, _pb0, *_ = run_backwards_induction(edges, "white", eval_lookup=ev_partial,
                                           reply_shrink=0.0, **COMMON_NR)
    p1, _pb1, *_ = run_backwards_induction(edges, "white", eval_lookup=ev_partial,
                                           reply_shrink=1.0, **COMMON_NR)
    check(p0[zobrist_int64(d4n)] == p1[zobrist_int64(d4n)],
          "opponent node with no eval is untouched at strength 1")

    bad = [lbl for ok, lbl in _checks if not ok]
    print(f"\n{len(_checks) - len(bad)}/{len(_checks)} checks passed")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
