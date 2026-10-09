"""Synthetic test for score_repertoire's --aux-stats coverage (2026-10-08).
Imports the production score_slice.

Fixture (white): 1.e4 is booked; after 1.e4 the only reply that cleared the
pool's floor is 1...e5 (100 games), which the book covers (2.Nf3 booked).
The aux sidecar says 100 more games continued with below-floor replies.

  A  without aux: every game reaches our 2.Nf3 (booked moves/game 2.0) --
     the survivors are renormalised to 100% of play.
  B  with aux: half the games left prep after 1.e4 (booked 1.5), and the mean
     exit ply falls.
  C  zero extra mass is identical to no aux. (Terminations never enter
     aux_extra: the CLI sums other_total + horizon_total only, because games
     that ENDED at a node did not leave our preparation.)
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from score_repertoire import score_slice  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def main() -> None:
    b0 = chess.Board()
    b1 = b0.copy(); b1.push_san("e4")
    b2 = b1.copy(); b2.push_san("e5")
    b3 = b2.copy(); b3.push_san("Nf3")
    h0, h1, h2, h3 = (zobrist_int64(b) for b in (b0, b1, b2, b3))
    rep = pl.DataFrame({
        "position_hash": [h0, h1, h2, h3],
        "position_epd": [b.epd() for b in (b0, b1, b2, b3)],
        "side_to_move": ["white", "black", "white", "black"],
        "value": [0.55, 0.55, 0.55, 0.55],
        "value_robust": [0.5] * 4, "value_worst": [0.5] * 4,
        "eval_score": [0.52] * 4,
        "best_move": ["e4", None, "Nf3", None],
    })
    stats = pl.DataFrame({
        "parent_hash": [h0, h1, h2], "move_san": ["e4", "e5", "Nf3"],
        "total": [1000, 100, 100], "child_hash": [h1, h2, h3],
    })

    r_no = score_slice(rep, stats, "white", max_ply=4)
    r_aux = score_slice(rep, stats, "white", max_ply=4, aux_extra={h1: 100})
    r_zero = score_slice(rep, stats, "white", max_ply=4, aux_extra={h1: 0})

    # booked moves/game: 1.e4 always (1.0) + 2.Nf3 only where the game is still in book
    check(abs(r_no["booked_moves"] - 2.0) < 1e-9,
          f"A: without aux, every game reaches 2.Nf3 (booked {r_no['booked_moves']:.3f}, want 2.000)")
    check(abs(r_aux["booked_moves"] - 1.5) < 1e-9,
          f"B: with 100 below-floor games, half leave prep before 2.Nf3 "
          f"(booked {r_aux['booked_moves']:.3f}, want 1.500)")
    check(r_aux["prep_depth"] > 0 and r_aux["prep_depth"] < r_no["prep_depth"],
          f"B: and the mean exit ply falls ({r_no['prep_depth']:.2f} -> {r_aux['prep_depth']:.2f})")
    check(abs(r_zero["booked_moves"] - r_no["booked_moves"]) < 1e-12,
          "C: zero extra mass is identical to no aux")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
