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

from zobrist import zobrist_int64  # noqa: F401  (re-export for callers importing from here)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


SOURCE_ROOT = Path("D:/data/chess/standard-chess-games-compressed")
DEFAULT_OUTPUT = Path("E:/chess/position-moves")

ANNOT_RE = re.compile(r"\{[^}]*\}")
VAR_RE = re.compile(r"\([^)]*\)")
NAG_RE = re.compile(r"\$\d+")
MOVE_NUM_RE = re.compile(r"^\d+\.+$")
RESULT_TOKENS = {"1-0", "0-1", "1/2-1/2", "*"}


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
    """Generate a deterministic output filename from a partition path.

    When `src` is a directory (partition-level dispatch), the name is derived
    from the hive partition keys, e.g. ``year=2025_month=4_event=Blitz.parquet``.

    When `src` is a file (file-level dispatch), the source file stem is
    appended to avoid collisions among files in the same partition, e.g.
    ``year=2025_month=4_event=Blitz_part-0.parquet``.
    """
    if src.is_file():
        parent_dir = src.parent
        parts = [p for p in parent_dir.parts[-3:] if "=" in p]
        if not parts:
            parts = [parent_dir.name]
        return "_".join(parts) + "_" + src.stem + ".parquet"
    else:
        parts = [p for p in src.parts[-3:] if "=" in p]
        if not parts:
            parts = [src.name]
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


# Output flush threshold: pending rows before writing a row group. 500K rows
# at ~80 bytes/row in arrow ≈ 40 MB peak per worker on the output side.
FLUSH_ROWS = 500_000

# Input batch size: games loaded into RAM at once per worker. With 5 columns
# (movetext is the heavyweight string), 50K games is a few hundred MB
# decompressed -- safe under 11 concurrent workers even on the 2025 Blitz/
# Bullet partitions (40M+ games, 14-22 GB on disk).
READ_BATCH_GAMES = 50_000

INPUT_COLUMNS = ["game_id", "movetext", "white_score", "elo_band", "mean_elo"]


def _iter_input_files(src: Path) -> list[Path]:
    """Resolve `src` (file or partition directory) to a sorted list of parquet files."""
    if src.is_file():
        return [src]
    files = sorted(src.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {src}")
    return files


def process_partition(src: Path, output_dir: Path, max_ply: int = 30,
                      limit_games: int | None = None, overwrite: bool = False) -> dict:
    """Process one partition; stream input AND output to bound peak memory.

    Input is read in pyarrow row-group batches of `READ_BATCH_GAMES` games at a
    time, so partition size no longer drives RAM usage. Output is flushed every
    `FLUSH_ROWS` rows. Combined, peak memory per worker is ~hundreds of MB
    regardless of whether the partition has 100K or 40M games.
    """
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

    input_files = _iter_input_files(src)

    n_games  = 0
    n_edges  = 0
    n_failed = 0
    t0 = time.time()

    buf = {col: [] for col in PER_GAME_COLS}
    tmp_path = out_path.with_suffix(".parquet.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    done = False
    with pq.ParquetWriter(tmp_path, schema, compression="zstd") as writer:
        for parquet_file in input_files:
            if done:
                break
            pf = pq.ParquetFile(parquet_file)
            for batch in pf.iter_batches(batch_size=READ_BATCH_GAMES, columns=INPUT_COLUMNS):
                for record in batch.to_pylist():
                    if limit_games is not None and n_games >= limit_games:
                        done = True
                        break
                    n_games += 1
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
                if done:
                    break

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
