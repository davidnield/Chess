"""
Compare repertoires across forcing-weight variants.

For white: shows recommended first move per (event, elo_band) slice.
For black: shows recommended response to each of white's top-N first moves,
           per (event, elo_band) slice.  (The start position is white's turn,
           so the previous approach of looking up the start hash returned None
           for every black slice.)

Usage:
    .venv/Scripts/python.exe python/compare_forcing_variants.py --year 2016
    .venv/Scripts/python.exe python/compare_forcing_variants.py --year 2016 --top-white-moves 3
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


INT64_MAX = 2**63 - 1
INT64_RANGE = 2**64

DEFAULT_REPERTOIRE_DIR = Path("E:/chess/repertoire")
DEFAULT_STATS_DIR      = Path("E:/chess/position-stats")


def zobrist_int64(b: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(b)
    return h - INT64_RANGE if h > INT64_MAX else h


def recs_at_hash(df: pl.DataFrame, position_hash: int) -> dict:
    """Return {(event, elo_band): row} for all rows at the given position hash."""
    s = (
        df.filter(pl.col("position_hash") == position_hash)
        .select(["event", "elo_band", "best_move", "value", "forcingness"])
        .sort(["event", "elo_band"])
    )
    return {(r["event"], r["elo_band"]): r for r in s.iter_rows(named=True)}


def get_top_white_moves(stats: pl.DataFrame, start_hash: int, top_n: int) -> list[str]:
    """Return the top_n most common white first moves summed across all slices."""
    return (
        stats.filter(pl.col("parent_hash") == start_hash)
        .group_by("move_san")
        .agg(pl.col("total").sum().alias("total"))
        .sort("total", descending=True)
        .head(top_n)
        .get_column("move_san")
        .to_list()
    )


def n_lookup_at_hash(stats: pl.DataFrame, position_hash: int) -> dict:
    """Return {(event, elo_band, move_san): total} for moves played from position_hash."""
    return {
        (r["event"], r["elo_band"], r["move_san"]): r["total"]
        for r in stats.filter(pl.col("parent_hash") == position_hash).iter_rows(named=True)
    }


def print_comparison_table(
    tables: dict[float, dict],     # w -> {(event, elo_band): row}
    slices: list[tuple],
    n_lookup: dict,                 # (event, elo_band, move_san) -> total
) -> None:
    diffs      = {0.10: 0, 0.20: 0}
    diff_drops = {0.10: [], 0.20: []}

    print(f"  {'slice':<22}  {'w=0':<26}  {'w=0.10':<28}  {'w=0.20':<28}")
    print(f"  {'':<22}  {'move val frc n=':<26}  {'move val frc n=':<28}  {'move val frc n=':<28}")
    print("  " + "-" * 110)

    for k in slices:
        ev, eb = k
        row0 = tables.get(0.00, {}).get(k)
        row1 = tables.get(0.10, {}).get(k)
        row2 = tables.get(0.20, {}).get(k)

        def fmt(r, prefix="  "):
            if r is None:
                return f"{prefix}<missing>"
            bm = str(r["best_move"])
            v  = r["value"]
            f  = r["forcingness"] if r["forcingness"] is not None else 0.0
            n  = n_lookup.get((ev, eb, bm), 0) if bm != "None" else 0
            return f"{prefix}{bm:<5} {v:.3f} f={f:.2f} n={n:>7,}"

        flag1 = "  "
        flag2 = "  "
        if row0 and row1 and row1["best_move"] != row0["best_move"]:
            flag1 = "* "
            diffs[0.10] += 1
            diff_drops[0.10].append(row0["value"] - row1["value"])
        if row0 and row2 and row2["best_move"] != row0["best_move"]:
            flag2 = "* "
            diffs[0.20] += 1
            diff_drops[0.20].append(row0["value"] - row2["value"])

        print(f"  {ev:<14} {eb:>5}    "
              f"{fmt(row0):<26}  {fmt(row1, flag1):<28}  {fmt(row2, flag2):<28}")

    n_slices = len(slices)
    print(f"\n  Slices differing from w=0: w=0.10 {diffs[0.10]}/{n_slices}, "
          f"w=0.20 {diffs[0.20]}/{n_slices}")
    for w in [0.10, 0.20]:
        drops = diff_drops[w]
        if drops:
            avg = sum(drops) / len(drops)
            mx  = max(drops)
            print(f"  Avg value drop on differing slices (w={w:.2f}): "
                  f"{avg:+.4f} (max {mx:+.4f}, n={len(drops)})")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--rep-dir",   default=str(DEFAULT_REPERTOIRE_DIR))
    parser.add_argument("--stats-dir", default=str(DEFAULT_STATS_DIR))
    parser.add_argument("--top-white-moves", type=int, default=5,
                        help="White first moves to show for the black comparison (default: 5)")
    args = parser.parse_args()

    rep_dir    = Path(args.rep_dir)
    stats_path = Path(args.stats_dir) / f"position_stats_{args.year}.parquet"

    start_hash = zobrist_int64(chess.Board())
    stats      = pl.read_parquet(stats_path)
    suffixes   = [("w000", 0.00), ("w010", 0.10), ("w020", 0.20)]

    # ── WHITE ─────────────────────────────────────────────────────────────────
    print(f"\n{'='*100}")
    print(f"WHITE REPERTOIRE — FIRST MOVE BY SLICE (year={args.year})")
    print(f"{'='*100}\n")

    white_reps = {}
    for sfx, w in suffixes:
        path = rep_dir / f"repertoire_{args.year}_white_{sfx}.parquet"
        if not path.exists():
            print(f"  MISSING: {path}")
        else:
            white_reps[w] = pl.read_parquet(path)

    if white_reps:
        white_tables = {w: recs_at_hash(df, start_hash) for w, df in white_reps.items()}
        slices       = sorted(next(iter(white_tables.values())).keys())
        print_comparison_table(white_tables, slices, n_lookup_at_hash(stats, start_hash))

    # ── BLACK ─────────────────────────────────────────────────────────────────
    print(f"\n{'='*100}")
    print(f"BLACK REPERTOIRE — RESPONSES BY SLICE (year={args.year})")
    print(f"{'='*100}")

    black_reps = {}
    for sfx, w in suffixes:
        path = rep_dir / f"repertoire_{args.year}_black_{sfx}.parquet"
        if not path.exists():
            print(f"  MISSING: {path}")
        else:
            black_reps[w] = pl.read_parquet(path)

    if not black_reps:
        print("  No black repertoire files found.")
        return

    top_white = get_top_white_moves(stats, start_hash, args.top_white_moves)
    print(f"\n  Showing black's response to white's top {len(top_white)} first moves: "
          f"{', '.join(top_white)}\n")

    for white_san in top_white:
        b = chess.Board()
        b.push(b.parse_san(white_san))
        pos_hash = zobrist_int64(b)

        black_tables = {w: recs_at_hash(df, pos_hash) for w, df in black_reps.items()}
        slices = sorted(next(iter(black_tables.values())).keys())

        print(f"  After 1.{white_san}:")
        print_comparison_table(black_tables, slices, n_lookup_at_hash(stats, pos_hash))
        print()


if __name__ == "__main__":
    main()
