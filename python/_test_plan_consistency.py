"""Synthetic test for plan_consistency_report.py: idea_token normalization and
the reach-weighted path-exact DFS (context split, first-occurrence game mass,
augmented-move handling, horizon). Imports the production analyze_slice /
idea_token (template: _test_winpos.py).

Fixture (black perspective, hashes are arbitrary ints except the root):
  root (white to move): 1.e4 60% -> A, 1.d4 40% -> B
  A (our turn): book ...c5 -> A1; A1: 2.Nf3 100% -> A2; A2: book ...Nc6 (no edge:
     augmented-style stop)
  B (our turn): book ...Nf6 -> B1; B1: 2.c4 100% -> B2; B2: book ...c5 (no edge)
Expected:
  ctx e4 (60%): c5 100%, Nc6 100%    ctx d4 (40%): Nf6 100%, c5 100%
  global: c5 100% (both contexts!), Nc6 60%, Nf6 40%
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from plan_consistency_report import GLOBAL_CTX, analyze_slice, idea_token, zobrist_int64

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def main() -> None:
    # ── idea_token unit checks ────────────────────────────────────────────
    cases = [
        ("Nf3", "Nf3"), ("Nbd2", "Nd2"), ("Nxd2", "Nd2"), ("R1e2", "Re2"),
        ("Qh4xe4+", "Qe4"), ("O-O", "O-O"), ("O-O-O+", "O-O-O"),
        ("c5", "c5"), ("cxd5", "cxd5"), ("exd5", "exd5"), ("e8=Q+", "e8=Q"),
        ("Bxf7#", "Bf7"),
    ]
    for san, want in cases:
        got = idea_token(san)
        check(got == want, f"idea_token({san!r}) == {want!r} (got {got!r})")

    # ── DFS fixture ───────────────────────────────────────────────────────
    root = zobrist_int64(chess.Board())
    A, A1, A2 = 101, 102, 103
    B, B1, B2 = 201, 202, 203

    stats = pl.DataFrame({
        "parent_hash": [root, root, A, A1, B, B1],
        "move_san":    ["e4", "d4", "c5", "Nf3", "Nf6", "c4"],
        "total":       [600,  400,  300,  300,   200,   200],
        "child_hash":  [A,    B,    A1,   A2,    B1,    B2],
    }).with_columns(pl.col("parent_hash").cast(pl.Int64),
                    pl.col("child_hash").cast(pl.Int64))

    rep = pl.DataFrame({
        "position_hash": [A, A2, B, B2],
        "side_to_move":  ["black"] * 4,
        "best_move":     ["c5", "Nc6", "Nf6", "c5"],
    }).with_columns(pl.col("position_hash").cast(pl.Int64))

    res = analyze_slice(rep, stats, "black", max_our_moves=12, epsilon=1e-9)
    gm, mm, cm = res["game_mass"], res["move_mass"], res["ctx_mass"]

    def close(a, b):
        return abs(a - b) < 1e-12

    check(close(cm.get("e4", 0), 0.6), f"ctx mass e4 == 0.6 (got {cm.get('e4', 0)})")
    check(close(cm.get("d4", 0), 0.4), f"ctx mass d4 == 0.4 (got {cm.get('d4', 0)})")
    check(close(cm.get(GLOBAL_CTX, 0), 1.0), f"global mass == 1.0 (got {cm.get(GLOBAL_CTX, 0)})")

    check(close(gm.get(("e4", "c5"), 0), 0.6), "vs e4: c5 in 100% of ctx games")
    check(close(gm.get(("e4", "Nc6"), 0), 0.6), "vs e4: Nc6 in 100% of ctx games")
    check(close(gm.get(("d4", "Nf6"), 0), 0.4), "vs d4: Nf6 in 100% of ctx games")
    check(close(gm.get(("d4", "c5"), 0), 0.4), "vs d4: c5 in 100% of ctx games")
    check(close(gm.get((GLOBAL_CTX, "c5"), 0), 1.0), "global: c5 in 100% of games")
    check(close(gm.get((GLOBAL_CTX, "Nc6"), 0), 0.6), "global: Nc6 in 60% of games")
    check(close(gm.get((GLOBAL_CTX, "Nf6"), 0), 0.4), "global: Nf6 in 40% of games")
    check(("e4", "Nf6") not in gm, "vs e4: Nf6 never played")
    check(close(mm.get((GLOBAL_CTX, "c5"), 0), 1.0), "move mass c5 == 1.0 (one per path)")
    check(close(res["pruned"], 0.0), "nothing pruned at epsilon 1e-9")

    # Horizon: with max_our_moves=1 only the first booked move per path counts.
    res1 = analyze_slice(rep, stats, "black", max_our_moves=1, epsilon=1e-9)
    gm1 = res1["game_mass"]
    check(close(gm1.get((GLOBAL_CTX, "c5"), 0), 0.6),
          "horizon 1: only move-1 c5 (vs e4) counted")
    check((GLOBAL_CTX, "Nc6") not in gm1, "horizon 1: move-2 Nc6 not counted")

    # First-occurrence dedup: same token twice on one path counts game mass once
    # but move mass twice. Extend the e4 line: A2 ...Nc6 -> C1, 3.Bb5 -> C2, ...c5?!
    # wait c5 already played at move 1 on this path -> game mass unchanged, move
    # mass grows. (Use a rook shuffle instead: token 'c5' repeated is impossible
    # in real chess, but hash-level fixtures don't replay boards, so any SAN works.)
    stats2 = pl.concat([stats, pl.DataFrame({
        "parent_hash": [A2, C1 := 104],
        "move_san":    ["Nc6", "Bb5"],
        "total":       [300, 300],
        "child_hash":  [C1, C2 := 105],
    }).with_columns(pl.col("parent_hash").cast(pl.Int64),
                    pl.col("child_hash").cast(pl.Int64))])
    rep2 = pl.concat([rep, pl.DataFrame({
        "position_hash": [C2],
        "side_to_move":  ["black"],
        "best_move":     ["c5"],
    }).with_columns(pl.col("position_hash").cast(pl.Int64))])
    res2 = analyze_slice(rep2, stats2, "black", max_our_moves=12, epsilon=1e-9)
    check(close(res2["game_mass"].get(("e4", "c5"), 0), 0.6),
          "repeat token: game mass counted once per path")
    check(close(res2["move_mass"].get(("e4", "c5"), 0), 1.2),
          "repeat token: move mass counted per occurrence")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
