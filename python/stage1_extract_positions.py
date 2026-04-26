"""
Stage 1: position-move edge extraction.

Reads one parquet partition (year/month/event) from the compressed Lichess
dataset and emits one row per ply per game, capturing the position before each
move (Zobrist hash + EPD) and the move played. Aggregating later by
(parent_hash, move_san) collapses transpositions automatically.

Setup (run once):
    uv pip install python-chess polars pyarrow

Usage (single partition, with sanity checks):
    .venv/Scripts/python.exe python/stage1_extract_positions.py \\
        --partition year=2023/month=1/event=Blitz \\
        --limit-games 100000

For parallel processing across all partitions, see stage1_run_all.py.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import chess
import chess.polyglot
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


SOURCE_ROOT = Path("D:/data/chess/standard-chess-games-compressed")
DEFAULT_OUTPUT = Path("D:/data/chess/position-moves")

ANNOT_RE = re.compile(r"\{[^}]*\}")
VAR_RE = re.compile(r"\([^)]*\)")
NAG_RE = re.compile(r"\$\d+")
MOVE_NUM_RE = re.compile(r"^\d+\.+$")
RESULT_TOKENS = {"1-0", "0-1", "1/2-1/2", "*"}

INT64_MAX = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(board: chess.Board) -> int:
    """Polyglot Zobrist hash cast to signed int64 (parquet/polars compatible)."""
    h = chess.polyglot.zobrist_hash(board)
    if h > INT64_MAX:
        h -= INT64_RANGE
    return h


def iter_san_moves(movetext: str):
    """Yield SAN tokens, skipping move numbers, results, and annotations.

    Strips trailing PGN suffix annotations (?, ??, !, !!, ?!, !?) which
    Lichess attaches directly to move tokens (e.g. 'Nh5??', 'cxd4?!').
    """
    text = ANNOT_RE.sub("", movetext)
    text = VAR_RE.sub("", text)
    text = NAG_RE.sub("", text)
    for tok in text.split():
        if tok in RESULT_TOKENS or MOVE_NUM_RE.match(tok):
            continue
        tok = tok.rstrip("?!")
        if not tok:
            continue
        yield tok


PER_GAME_COLS = (
    "game_id", "ply", "parent_hash", "parent_epd",
    "move_san", "white_score", "elo_band", "mean_elo",
)


def extract_positions_into(buf: dict, game_id, movetext, white_score, elo_band,
                           mean_elo, max_ply) -> bool:
    """Walk one game's moves and append rows directly into columnar `buf`.

    `buf` is a dict of column-name -> list, shared across games to avoid the
    per-row dict overhead that previously caused OOM on dense partitions.
    Returns True if the game's movetext encountered a parse error.
    """
    if not movetext:
        return False

    board = chess.Board()
    for ply, san in enumerate(iter_san_moves(movetext), start=1):
        if ply > max_ply:
            break
        parent_hash = zobrist_int64(board)
        parent_epd = board.epd()
        try:
            move = board.parse_san(san)
        except (ValueError, AssertionError):
            return True
        buf["game_id"].append(game_id)
        buf["ply"].append(ply)
        buf["parent_hash"].append(parent_hash)
        buf["parent_epd"].append(parent_epd)
        buf["move_san"].append(san)
        buf["white_score"].append(white_score)
        buf["elo_band"].append(elo_band)
        buf["mean_elo"].append(mean_elo)
        board.push(move)

    return False


def parse_partition_path(src: Path) -> dict:
    """Extract {year, month, event} from a hive-partitioned path."""
    out = {}
    parts = src.parent.parts if src.is_file() else src.parts
    for part in parts:
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if k in ("year", "month"):
            out[k] = int(v)
        elif k == "event":
            out[k] = v
    return out


def stable_output_name(src: Path) -> str:
    parent_dir = src.parent if src.is_file() else src
    parts = [p for p in parent_dir.parts[-3:] if "=" in p]
    if not parts:
        parts = [parent_dir.name]
    return "_".join(parts) + ".parquet"


def resolve_source(partition_arg: str) -> Path:
    src = Path(partition_arg)
    return src if src.is_absolute() else SOURCE_ROOT / src


def _output_schema(year: int | None, month: int | None, event: str | None) -> pa.Schema:
    """Schema for the partition output. Year/month/event are partition constants."""
    return pa.schema([
        pa.field("game_id",     pa.string()),
        pa.field("ply",         pa.int32()),
        pa.field("parent_hash", pa.int64()),
        pa.field("parent_epd",  pa.string()),
        pa.field("move_san",    pa.string()),
        pa.field("white_score", pa.float64()),
        pa.field("elo_band",    pa.int64()),
        pa.field("mean_elo",    pa.float64()),
        pa.field("year",        pa.int32()),
        pa.field("month",       pa.int32()),
        pa.field("event",       pa.string()),
    ])


def _flush_buffer(writer: pq.ParquetWriter, buf: dict, year, month, event):
    """Convert the columnar buffer to a pyarrow Table and write a row group."""
    n = len(buf["game_id"])
    if n == 0:
        return
    table = pa.table({
        **buf,
        "year":  pa.array([year]  * n, type=pa.int32()),
        "month": pa.array([month] * n, type=pa.int32()),
        "event": pa.array([event] * n, type=pa.string()),
    }, schema=writer.schema)
    writer.write_table(table)
    for col in buf:
        buf[col].clear()


# Flush threshold: number of pending rows before writing a row group. 500K rows
# at ~80 bytes/row in arrow ≈ 40 MB peak per worker, which keeps total memory
# safely below the OOM line we hit with the old list-of-dicts accumulator.
FLUSH_ROWS = 500_000


def process_partition(src: Path, output_dir: Path, max_ply: int = 30,
                      limit_games: int | None = None, overwrite: bool = False) -> dict:
    """Process one partition; stream rows to parquet in row-group batches."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / stable_output_name(src)

    if out_path.exists() and not overwrite:
        return {
            "src": str(src),
            "output_path": str(out_path),
            "n_games": 0,
            "n_edges": 0,
            "n_failed": 0,
            "elapsed_sec": 0.0,
            "skipped": True,
        }

    partition_meta = parse_partition_path(src)
    year  = partition_meta.get("year")
    month = partition_meta.get("month")
    event = partition_meta.get("event")
    schema = _output_schema(year, month, event)

    df = pl.read_parquet(
        src,
        columns=["game_id", "movetext", "white_score", "elo_band", "mean_elo"],
    )
    if limit_games:
        df = df.head(limit_games)

    n_games  = len(df)
    n_edges  = 0
    n_failed = 0
    t0 = time.time()

    buf = {col: [] for col in PER_GAME_COLS}
    tmp_path = out_path.with_suffix(".parquet.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    with pq.ParquetWriter(tmp_path, schema, compression="zstd") as writer:
        for record in df.iter_rows(named=True):
            failed = extract_positions_into(
                buf,
                game_id=record["game_id"],
                movetext=record["movetext"],
                white_score=record["white_score"],
                elo_band=record["elo_band"],
                mean_elo=record["mean_elo"],
                max_ply=max_ply,
            )
            if failed:
                n_failed += 1
            if len(buf["game_id"]) >= FLUSH_ROWS:
                n_edges += len(buf["game_id"])
                _flush_buffer(writer, buf, year, month, event)

        # Final flush
        if buf["game_id"]:
            n_edges += len(buf["game_id"])
            _flush_buffer(writer, buf, year, month, event)

    # Atomic rename only on successful completion
    tmp_path.replace(out_path)
    elapsed = time.time() - t0

    return {
        "src": str(src),
        "output_path": str(out_path),
        "n_games": n_games,
        "n_edges": n_edges,
        "n_failed": n_failed,
        "elapsed_sec": elapsed,
        "skipped": False,
    }


def print_sanity_checks(out_path: Path):
    out = pl.read_parquet(out_path)

    print("\n=== Sanity checks ===")
    ply_stats = (
        out.lazy()
        .group_by("ply")
        .agg([
            pl.len().alias("n_edges"),
            pl.col("parent_hash").n_unique().alias("n_distinct_positions"),
        ])
        .with_columns(
            (pl.col("n_edges") / pl.col("n_distinct_positions")).round(1)
            .alias("avg_games_per_position")
        )
        .sort("ply")
        .collect()
    )
    print("\nPositions per ply (avg_games_per_position climbs with transposition convergence):")
    print(ply_stats)

    start_hash = zobrist_int64(chess.Board())
    first_moves = (
        out.lazy()
        .filter(pl.col("parent_hash") == start_hash)
        .group_by("move_san")
        .agg([
            pl.len().alias("n_games"),
            pl.col("white_score").mean().round(4).alias("white_score_avg"),
        ])
        .sort("n_games", descending=True)
        .head(15)
        .collect()
    )
    print("\nFirst-move popularity and white-score (top 15):")
    print(first_moves)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--partition", required=True,
                        help="Path to a parquet partition (relative to SOURCE_ROOT or absolute), "
                             "e.g. 'year=2023/month=1/event=Blitz'")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--max-ply", type=int, default=30,
                        help="Plies per game to extract (default: 30 = 15 moves per side)")
    parser.add_argument("--limit-games", type=int, default=None,
                        help="Process only the first N games (for fast iteration)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Reprocess even if output already exists")
    args = parser.parse_args()

    src = resolve_source(args.partition)
    output_dir = Path(args.output)

    print(f"Source: {src}")
    print(f"Output dir: {output_dir}")
    print(f"Max ply per game: {args.max_ply}")
    if args.limit_games:
        print(f"Game limit: {args.limit_games:,}")

    stats = process_partition(
        src=src,
        output_dir=output_dir,
        max_ply=args.max_ply,
        limit_games=args.limit_games,
        overwrite=args.overwrite,
    )

    if stats["skipped"]:
        print(f"\nSkipped (output already exists): {stats['output_path']}")
        print("Pass --overwrite to reprocess.")
    else:
        rate = stats["n_games"] / stats["elapsed_sec"] if stats["elapsed_sec"] > 0 else 0
        print(f"\nProcessed {stats['n_games']:,} games -> {stats['n_edges']:,} edges "
              f"in {stats['elapsed_sec']:.1f}s ({rate:,.0f} games/sec)")
        if stats["n_failed"]:
            pct = stats["n_failed"] / stats["n_games"] * 100
            print(f"Parse errors: {stats['n_failed']:,} ({pct:.2f}%)")
        size_mb = Path(stats["output_path"]).stat().st_size / 1e6
        print(f"Wrote: {stats['output_path']}  ({size_mb:.1f} MB)")

    print_sanity_checks(Path(stats["output_path"]))


if __name__ == "__main__":
    main()
