"""
process_pgn_parquets.py — Clean, enrich, and compress PGN-derived parquet files.

Port of archive/01_processing_pgn_files.R with significantly improved compression:
  • Row group size: 1,000,000 rows (R produced ~400/group due to small Arrow batches)
  • Compression: zstd level 9 (R used level 3)
  • Streaming I/O via PyArrow dataset scanner (constant memory)
  • Optional parallel processing across monthly partitions

These changes typically reduce on-disk size by 40-60% for movetext-heavy workloads
while remaining drop-in compatible with the existing pipeline (stage1+).

Reads:   <source>/year=*/month=*/         raw PGN-derived Hive-partitioned parquets
Writes:  <output>/year=*/month=*/event=*/ cleaned, compressed, 3-level partitioned

All 27 output columns match the R version exactly:
  site, white, black, result, white_title, black_title, white_elo, black_elo,
  white_rating_diff, black_rating_diff, utc_date, utc_time, eco, opening,
  termination, time_control, movetext, game_id, mean_elo, elo_band,
  white_score, move_count, opening_family, eco_class, has_eval, base_time,
  increment

Usage:
    # Process every (year, month) partition not yet in the output:
    python python/process_pgn_parquets.py

    # Reprocess specific months (e.g., October 2024 after upstream PGN fix):
    python python/process_pgn_parquets.py --year 2024 --months 10 --force

    # Custom paths and parallelism:
    python python/process_pgn_parquets.py --source E:/raw --output D:/compressed --workers 2
"""

from __future__ import annotations

import argparse
import gc
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ── defaults ────────────────────────────────────────────────────────────────

DEFAULT_SOURCE = Path("E:/data/chess/standard-chess-games")
DEFAULT_OUTPUT = Path("D:/data/chess/standard-chess-games-compressed")

# ── event mapping (filter + rename) ─────────────────────────────────────────
# Source uses verbose names like "Rated Blitz game"; output uses "Blitz".

EVENT_MAP = {
    "Rated UltraBullet game":   "UltraBullet",
    "Rated Correspondence game":"Correspondence",
    "Rated Classical game":     "Classical",
    "Rated Rapid game":         "Rapid",
    "Rated Bullet game":        "Bullet",
    "Rated Blitz game":         "Blitz",
}

# ── compression settings ────────────────────────────────────────────────────

MAX_ROWS_PER_FILE  = 2_000_000   # same as R version
ROW_GROUP_SIZE     = 1_000_000   # was ~400 in R output → 2,500x larger groups
ZSTD_LEVEL         = 6           # See benchmark_zstd_levels.py: best ratio in
                                 # the "flat-time" tier (1-8 all run ~67-74s).
                                 # zstd-9 costs +15% time for +0.6% smaller
                                 # files, levels 11+ are worse on every axis.
SCANNER_BATCH_SIZE = 500_000     # rows per source-scan batch (memory governor)

# ── output schema (matches R output exactly) ────────────────────────────────
# Partition columns (year/month/event) are included so write_dataset can route
# rows to the right Hive directories; they are stripped from the parquet files.

WRITE_SCHEMA = pa.schema([
    ("site",              pa.string()),
    ("white",             pa.string()),
    ("black",             pa.string()),
    ("result",            pa.string()),
    ("white_title",       pa.string()),
    ("black_title",       pa.string()),
    ("white_elo",         pa.int16()),
    ("black_elo",         pa.int16()),
    ("white_rating_diff", pa.int16()),
    ("black_rating_diff", pa.int16()),
    ("utc_date",          pa.date32()),
    ("utc_time",          pa.time32("ms")),
    ("eco",               pa.string()),
    ("opening",           pa.string()),
    ("termination",       pa.string()),
    ("time_control",      pa.string()),
    ("movetext",          pa.string()),
    ("game_id",           pa.string()),
    ("mean_elo",          pa.int16()),
    ("elo_band",          pa.int16()),
    ("white_score",       pa.float64()),
    ("move_count",        pa.int16()),
    ("opening_family",    pa.string()),
    ("eco_class",         pa.string()),
    ("has_eval",          pa.bool_()),
    ("base_time",         pa.int32()),
    ("increment",         pa.int32()),
    # Hive partition columns (stripped from output files):
    ("year",              pa.int32()),
    ("month",             pa.int32()),
    ("event",             pa.string()),
])

PARTITION_SCHEMA = pa.schema([
    ("year",  pa.int32()),
    ("month", pa.int32()),
    ("event", pa.string()),
])


# ── logging ─────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ── partition discovery ─────────────────────────────────────────────────────

def discover_partitions(source: Path) -> list[tuple[int, int, Path]]:
    """Find all year=*/month=* partitions under source, sorted newest-first."""
    parts = []
    for year_dir in sorted(source.glob("year=*")):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        for month_dir in sorted(year_dir.glob("month=*")):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            parts.append((year, month, month_dir))
    # Newest first (matches R script behaviour)
    parts.sort(key=lambda p: (p[0], p[1]), reverse=True)
    return parts


def output_already_exists(output: Path, year: int, month: int) -> bool:
    """True if any event subdirectory under year=Y/month=M has parquet files."""
    base = output / f"year={year}" / f"month={month}"
    if not base.exists():
        return False
    for event_dir in base.iterdir():
        if event_dir.is_dir() and any(event_dir.glob("*.parquet")):
            return True
    return False


# ── transform ───────────────────────────────────────────────────────────────

def _coerce_source_types(df: pl.DataFrame) -> pl.DataFrame:
    """Best-effort coercion of upstream variability (string vs typed columns)."""
    casts = []

    # UTCDate / UTCTime may arrive as strings depending on upstream parser
    if df.schema["UTCDate"] == pl.Utf8:
        casts.append(pl.col("UTCDate").str.to_date("%Y.%m.%d", strict=False))
    if df.schema["UTCTime"] == pl.Utf8:
        casts.append(pl.col("UTCTime").str.to_time("%H:%M:%S", strict=False))

    # Rating diffs may be strings ("+5", "-12") or ints; cast to Int16 either way
    for col in ("WhiteRatingDiff", "BlackRatingDiff"):
        if col in df.columns and df.schema[col] not in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
            casts.append(pl.col(col).cast(pl.Int16, strict=False))

    return df.with_columns(casts) if casts else df


def transform(df: pl.DataFrame, year: int, month: int) -> pl.DataFrame:
    """
    Apply the column transforms that match the R version's output exactly.
    Returns a DataFrame with all 30 columns (27 data + 3 partition).
    """
    df = _coerce_source_types(df)

    # Stage 1: compute mean_elo so elo_band can depend on it
    df = df.with_columns(
        ((pl.col("WhiteElo").cast(pl.Float64) + pl.col("BlackElo").cast(pl.Float64)) / 2)
            .round(0).cast(pl.Int16).alias("_mean_elo")
    )

    df = df.with_columns(
        # R: cast(floor(mean_elo / 100) * 100, int16())
        ((pl.col("_mean_elo").cast(pl.Float64) / 100).floor() * 100)
            .cast(pl.Int16).alias("_elo_band")
    )

    # Stage 2: select all output columns in the canonical order
    return df.select([
        # ── Direct renames / casts ──
        pl.col("Site").alias("site"),
        pl.col("White").alias("white"),
        pl.col("Black").alias("black"),
        pl.col("Result").alias("result"),
        pl.col("WhiteTitle").alias("white_title"),
        pl.col("BlackTitle").alias("black_title"),
        pl.col("WhiteElo").cast(pl.Int16, strict=False).alias("white_elo"),
        pl.col("BlackElo").cast(pl.Int16, strict=False).alias("black_elo"),
        pl.col("WhiteRatingDiff").cast(pl.Int16, strict=False).alias("white_rating_diff"),
        pl.col("BlackRatingDiff").cast(pl.Int16, strict=False).alias("black_rating_diff"),
        pl.col("UTCDate").alias("utc_date"),
        pl.col("UTCTime").alias("utc_time"),
        pl.col("ECO").alias("eco"),
        pl.col("Opening").alias("opening"),
        pl.col("Termination").alias("termination"),
        pl.col("TimeControl").alias("time_control"),
        pl.col("movetext"),

        # ── Computed fields ──
        pl.col("Site").str.replace("https://lichess.org/", "", literal=True).alias("game_id"),
        pl.col("_mean_elo").alias("mean_elo"),
        pl.col("_elo_band").alias("elo_band"),

        # white_score: R case_when on Result
        pl.when(pl.col("Result") == "1-0").then(pl.lit(1.0))
          .when(pl.col("Result") == "0-1").then(pl.lit(0.0))
          .when(pl.col("Result") == "1/2-1/2").then(pl.lit(0.5))
          .otherwise(None)
          .alias("white_score"),

        # move_count: count of "<digits>. " patterns (full moves, not half-moves)
        pl.col("movetext").str.count_matches(r"\d+\.\s").cast(pl.Int16).alias("move_count"),

        # opening_family: everything before the first ":"
        pl.col("Opening").str.replace(r":.*", "").alias("opening_family"),

        # eco_class: first character of ECO ("B20" → "B")
        pl.col("ECO").str.slice(0, 1).alias("eco_class"),

        # has_eval: does the movetext contain Stockfish evals?
        pl.col("movetext").str.contains("[%eval", literal=True).alias("has_eval"),

        # Time control parsing: "180+2" → base_time=180, increment=2; else null
        pl.when(pl.col("TimeControl").str.contains("+", literal=True))
          .then(pl.col("TimeControl").str.extract(r"^(\d+)\+", 1).cast(pl.Int32, strict=False))
          .otherwise(None)
          .alias("base_time"),

        pl.when(pl.col("TimeControl").str.contains("+", literal=True))
          .then(pl.col("TimeControl").str.extract(r"\+(\d+)$", 1).cast(pl.Int32, strict=False))
          .otherwise(None)
          .alias("increment"),

        # ── Partition columns (must match WRITE_SCHEMA order: year, month, event) ──
        pl.lit(year).cast(pl.Int32).alias("year"),
        pl.lit(month).cast(pl.Int32).alias("month"),

        # event: same case_when as R (only the 6 rated standard events)
        pl.when(pl.col("Event") == "Rated UltraBullet game").then(pl.lit("UltraBullet"))
          .when(pl.col("Event") == "Rated Correspondence game").then(pl.lit("Correspondence"))
          .when(pl.col("Event") == "Rated Classical game").then(pl.lit("Classical"))
          .when(pl.col("Event") == "Rated Rapid game").then(pl.lit("Rapid"))
          .when(pl.col("Event") == "Rated Bullet game").then(pl.lit("Bullet"))
          .when(pl.col("Event") == "Rated Blitz game").then(pl.lit("Blitz"))
          .alias("event"),
    ])


# ── per-partition processing ────────────────────────────────────────────────

def process_partition(year: int, month: int,
                      source_dir: Path, output: Path) -> tuple[bool, str]:
    """
    Stream-process one (year, month) partition: read source → transform → write
    Hive-partitioned output. Returns (success, message).
    """
    if not source_dir.exists():
        return False, f"source missing: {source_dir}"

    t0 = time.time()

    try:
        source_ds = pads.dataset(str(source_dir), format="parquet")

        # Filter to rated standard events at the scan layer (cheap)
        scanner = source_ds.scanner(
            filter=pc.field("Event").isin(list(EVENT_MAP.keys())),
            batch_size=SCANNER_BATCH_SIZE,
        )

        # Generator: read source batch → Polars transform → Arrow batch (cast to schema)
        def transformed_batches():
            for batch in scanner.to_batches():
                if batch.num_rows == 0:
                    continue
                df = pl.from_arrow(batch)
                df = transform(df, year, month)
                tbl = df.to_arrow().cast(WRITE_SCHEMA)
                yield from tbl.to_batches()

        # Wrap as RecordBatchReader so write_dataset knows the schema upfront
        reader = pa.RecordBatchReader.from_batches(WRITE_SCHEMA, transformed_batches())

        write_options = pads.ParquetFileFormat().make_write_options(
            compression="zstd",
            compression_level=ZSTD_LEVEL,
            version="2.6",
            write_statistics=True,
        )

        pads.write_dataset(
            reader,
            base_dir=str(output),
            format="parquet",
            partitioning=pads.partitioning(PARTITION_SCHEMA, flavor="hive"),
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_rows_per_group=ROW_GROUP_SIZE,
            # No min_rows_per_group — measurements show setting it forces extra
            # row group splits and inflates output by ~30%. Letting the writer
            # consolidate up to max gives the best size and read speed.
            file_options=write_options,
            existing_data_behavior="delete_matching",
            basename_template="part-{i}.parquet",
            use_threads=True,
        )

        # Tally what got written
        out_base = output / f"year={year}" / f"month={month}"
        if out_base.exists():
            n_files = sum(1 for _ in out_base.rglob("*.parquet"))
            n_bytes = sum(p.stat().st_size for p in out_base.rglob("*.parquet"))
            size_str = f"{n_files} files, {n_bytes/1e9:.1f} GB"
        else:
            size_str = "no output (empty source?)"

        elapsed = (time.time() - t0) / 60
        return True, f"{elapsed:.1f} min | {size_str}"

    except Exception as e:
        tb = traceback.format_exc()
        return False, f"ERROR after {(time.time()-t0)/60:.1f} min: {e}\n{tb}"
    finally:
        gc.collect()


# ── parallel worker entrypoint (must be picklable) ──────────────────────────

def _worker(args: tuple[int, int, str, str]) -> tuple[int, int, bool, str]:
    year, month, source_dir, output = args
    ok, msg = process_partition(year, month, Path(source_dir), Path(output))
    return year, month, ok, msg


# ── main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Clean & compress PGN-derived parquet partitions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help=f"Source root (default: {DEFAULT_SOURCE})")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help=f"Output root (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--year", type=int, default=None,
                        help="Restrict to a specific year (otherwise: all years)")
    parser.add_argument("--months", type=int, nargs="+", default=None,
                        help="Restrict to specific months (1-12); requires --year "
                             "or applies across all years")
    parser.add_argument("--force", action="store_true",
                        help="Reprocess partitions even if output already exists")
    parser.add_argument("--workers", type=int, default=1,
                        help="Parallel workers across partitions (default: 1; "
                             "I/O bound — 2-3 is usually the sweet spot)")
    parser.add_argument("--list", action="store_true",
                        help="List discovered partitions and exit")
    args = parser.parse_args()

    source = Path(args.source)
    output = Path(args.output)

    if not source.exists():
        log(f"FATAL: source does not exist: {source}")
        sys.exit(1)

    output.mkdir(parents=True, exist_ok=True)

    # Discover and filter
    partitions = discover_partitions(source)
    if args.year is not None:
        partitions = [p for p in partitions if p[0] == args.year]
    if args.months is not None:
        wanted = set(args.months)
        partitions = [p for p in partitions if p[1] in wanted]

    if not partitions:
        log("No partitions matched filters. Exiting.")
        return

    if args.list:
        for y, m, d in partitions:
            existing = "✓ has output" if output_already_exists(output, y, m) else "  needs processing"
            log(f"  {y}-{m:02d}  {existing}  ({d})")
        return

    # Skip already-done partitions unless --force
    if not args.force:
        before = len(partitions)
        partitions = [p for p in partitions
                      if not output_already_exists(output, p[0], p[1])]
        skipped = before - len(partitions)
        if skipped:
            log(f"Skipping {skipped} partition(s) that already have output "
                f"(use --force to reprocess)")

    if not partitions:
        log("Nothing to do.")
        return

    # Summary
    log(f"Source:     {source}")
    log(f"Output:     {output}")
    log(f"Compression: zstd-{ZSTD_LEVEL}, {ROW_GROUP_SIZE:,} rows/group, "
        f"{MAX_ROWS_PER_FILE:,} rows/file")
    log(f"Partitions: {len(partitions)} to process, workers={args.workers}")
    log("")

    t_start = time.time()
    completed = 0
    failed = 0

    if args.workers <= 1:
        # Sequential — easier to read logs
        for i, (year, month, src_dir) in enumerate(partitions, 1):
            avg_min = (time.time() - t_start) / 60 / max(completed, 1)
            eta_h = avg_min * (len(partitions) - i + 1) / 60 if completed > 0 else None
            eta_msg = f" | ETA: {eta_h:.1f}h" if eta_h is not None else ""
            log(f"[{i}/{len(partitions)}] {year}-{month:02d} starting{eta_msg}")

            ok, msg = process_partition(year, month, src_dir, output)
            log(f"  {'OK' if ok else 'FAILED'}: {msg}")
            completed += 1
            if not ok:
                failed += 1
    else:
        # Parallel — useful when source and output are on separate spindles
        tasks = [(y, m, str(d), str(output)) for y, m, d in partitions]
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(_worker, t): (t[0], t[1]) for t in tasks}
            for fut in as_completed(futures):
                year, month = futures[fut]
                try:
                    _, _, ok, msg = fut.result()
                except Exception as e:
                    ok, msg = False, f"worker raised: {e}"
                completed += 1
                status = "OK" if ok else "FAILED"
                log(f"[{completed}/{len(partitions)}] {year}-{month:02d} {status}: {msg}")
                if not ok:
                    failed += 1

    total_h = (time.time() - t_start) / 3600
    log("")
    log(f"=== COMPLETE === {len(partitions)} processed, {failed} failed, "
        f"{total_h:.2f} hours total")


if __name__ == "__main__":
    main()
