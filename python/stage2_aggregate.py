"""
Stage 2: aggregate Stage 1 edge tables into position-move statistics.

Reads the per-partition edge files written by Stage 1 and groups by
(event, elo_band, parent_hash, move_san), summing result counts. The output
is the canonical empirical opening explorer -- one row per (position, move)
within each (event, elo_band) slice -- which Stage 3's backwards induction
will operate on.

Streaming aggregation via polars `sink_parquet`, so memory is bounded even
when the input is hundreds of GB.

Usage:
    .venv/Scripts/python.exe python/stage2_aggregate.py
    .venv/Scripts/python.exe python/stage2_aggregate.py --min-games 30
    .venv/Scripts/python.exe python/stage2_aggregate.py --max-ply 20
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DEFAULT_INPUT = Path("D:/data/chess/position-moves")
DEFAULT_OUTPUT = Path("D:/data/chess/position-stats/position_stats.parquet")

INT64_MAX = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(board: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(board)
    if h > INT64_MAX:
        h -= INT64_RANGE
    return h


def build_query(input_glob: str, max_ply: int | None, min_games: int):
    edges = pl.scan_parquet(input_glob)

    if max_ply is not None:
        edges = edges.filter(pl.col("ply") <= max_ply)

    aggregated = (
        edges
        .group_by(["event", "elo_band", "parent_hash", "move_san"])
        .agg([
            pl.col("parent_epd").first().alias("parent_epd"),
            pl.col("ply").first().alias("ply"),
            (pl.col("white_score") == 1.0).sum().cast(pl.Int64).alias("white_wins"),
            (pl.col("white_score") == 0.5).sum().cast(pl.Int64).alias("draws"),
            (pl.col("white_score") == 0.0).sum().cast(pl.Int64).alias("black_wins"),
            pl.len().cast(pl.Int64).alias("total"),
        ])
        .filter(pl.col("total") >= min_games)
        .with_columns(
            ((pl.col("white_wins") + pl.col("draws") * 0.5) / pl.col("total"))
            .alias("white_score_avg")
        )
    )
    return aggregated


def print_sanity_checks(output_path: Path):
    stats = pl.read_parquet(output_path)
    print("\n=== Sanity checks ===")
    print(f"Total (event, elo_band, position, move) rows: {len(stats):,}")
    print(f"Distinct positions covered: {stats.select(pl.col('parent_hash').n_unique()).item():,}")

    # Per-event row counts
    print("\nRows per event:")
    print(stats.group_by("event").agg(pl.len().alias("rows")).sort("rows", descending=True))

    # Best first move per (event, elo_band) -- the empirical repertoire root
    start_hash = zobrist_int64(chess.Board())
    start_moves = (
        stats.filter(pl.col("parent_hash") == start_hash)
        .sort(["event", "elo_band", "white_score_avg"], descending=[False, False, True])
        .group_by(["event", "elo_band"], maintain_order=True)
        .head(3)
        .select(["event", "elo_band", "move_san", "total", "white_score_avg"])
    )
    print("\nTop-3 first moves by white_score_avg, per (event, elo_band):")
    print(start_moves)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT),
                        help=f"Directory of Stage 1 edge tables (default: {DEFAULT_INPUT})")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help=f"Output parquet path (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--min-games", type=int, default=10,
                        help="Drop (position, move) cells with fewer than this many games "
                             "(default: 10; raise for production analysis)")
    parser.add_argument("--max-ply", type=int, default=None,
                        help="Truncate edges deeper than this ply before aggregating "
                             "(useful to limit output size; default: no limit)")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    input_glob = str(input_dir / "*.parquet")
    print(f"Input:  {input_dir}")
    print(f"Output: {output_path}")
    print(f"Min games per (position, move): {args.min_games}")
    if args.max_ply:
        print(f"Max ply: {args.max_ply}")

    query = build_query(input_glob, max_ply=args.max_ply, min_games=args.min_games)

    t0 = time.time()
    query.sink_parquet(str(output_path), compression="zstd")
    elapsed = time.time() - t0
    size_mb = output_path.stat().st_size / 1e6
    print(f"\nWrote {output_path} ({size_mb:.1f} MB) in {elapsed:.1f}s")

    print_sanity_checks(output_path)


if __name__ == "__main__":
    main()
