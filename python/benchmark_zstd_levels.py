"""
benchmark_zstd_levels.py — Benchmark every zstd compression level on real data.

Reads all of January 2016 (1.74 GB, ~3.88M filtered rows), pre-transforms it
ONCE in memory, then repeatedly writes it under every zstd level (1-22) and a
no-compression baseline. Measures:
  • compression time (write only — transform is done once upfront)
  • on-disk size  + compression ratio vs uncompressed baseline
  • decompression time (full read-back into Arrow Table)
  • compression / decompression throughput in MB/s of uncompressed input

Output goes to D:/zstd_bench_2016_01/level_{N}/ — separate from the production
compressed directory so nothing is overwritten.

Usage:
    python python/benchmark_zstd_levels.py
    python python/benchmark_zstd_levels.py --max-level 19   # skip ultra
    python python/benchmark_zstd_levels.py --keep-output    # don't auto-clean
"""

from __future__ import annotations

import argparse
import csv
import gc
import shutil
import sys
import time
import traceback
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Reuse the production transform so we benchmark the actual deployed pipeline
sys.path.insert(0, str(Path(__file__).parent))
from process_pgn_parquets import (  # noqa: E402
    EVENT_MAP,
    PARTITION_SCHEMA,
    WRITE_SCHEMA,
    transform,
)

SOURCE     = Path("D:/data/chess/standard-chess-games/year=2016/month=01")
BENCH_ROOT = Path("D:/zstd_bench_2016_01")

YEAR  = 2016
MONTH = 1

# Production write settings (held constant across all levels)
ROW_GROUP_SIZE    = 1_000_000
MAX_ROWS_PER_FILE = 2_000_000


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def preload_source() -> pa.Table:
    """Read source, apply the production transform, return one Arrow Table."""
    log(f"Reading + transforming source: {SOURCE}")
    t0 = time.time()

    ds = pads.dataset(str(SOURCE), format="parquet")
    scanner = ds.scanner(
        filter=pc.field("Event").isin(list(EVENT_MAP.keys())),
        batch_size=500_000,
    )

    transformed: list[pa.Table] = []
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        df = pl.from_arrow(batch)
        df = transform(df, YEAR, MONTH)
        transformed.append(df.to_arrow().cast(WRITE_SCHEMA))

    table = pa.concat_tables(transformed)
    del transformed
    gc.collect()

    elapsed = time.time() - t0
    log(f"  → {table.num_rows:,} rows, in-memory {table.nbytes/1e9:.2f} GB "
        f"(read+transform: {elapsed:.1f}s)")
    return table


def write_one_level(table: pa.Table, outdir: Path,
                    codec: str, level: int | None) -> float:
    """Write the table at one compression setting. Return wall-clock seconds."""
    if outdir.exists():
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if level is None:
        write_options = pads.ParquetFileFormat().make_write_options(
            compression=codec,            # "none"
            version="2.6",
            write_statistics=True,
        )
    else:
        write_options = pads.ParquetFileFormat().make_write_options(
            compression=codec,            # "zstd"
            compression_level=level,
            version="2.6",
            write_statistics=True,
        )

    t0 = time.time()
    pads.write_dataset(
        table,
        base_dir=str(outdir),
        format="parquet",
        partitioning=pads.partitioning(PARTITION_SCHEMA, flavor="hive"),
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_rows_per_group=ROW_GROUP_SIZE,
        file_options=write_options,
        existing_data_behavior="overwrite_or_ignore",
        basename_template="part-{i}.parquet",
        use_threads=True,
    )
    return time.time() - t0


def measure_size(outdir: Path) -> tuple[int, int]:
    files = list(outdir.rglob("*.parquet"))
    return len(files), sum(p.stat().st_size for p in files)


def measure_decompression(outdir: Path) -> tuple[float, int]:
    """Force a full read-back; return (seconds, rows_decoded)."""
    t0 = time.time()
    ds = pads.dataset(str(outdir), format="parquet")
    tbl = ds.to_table()              # fully materialize
    n = tbl.num_rows
    # Touch every column to defeat any lazy decoding
    for col in tbl.column_names:
        _ = tbl.column(col).nbytes
    elapsed = time.time() - t0
    del tbl
    gc.collect()
    return elapsed, n


def fmt_size(n: int) -> str:
    if n >= 1e9:
        return f"{n/1e9:.2f} GB"
    return f"{n/1e6:.1f} MB"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-level", type=int, default=22,
        help="Highest zstd level to test (default: 22 — ultra mode is SLOW)")
    parser.add_argument("--min-level", type=int, default=1,
        help="Lowest zstd level to test (default: 1)")
    parser.add_argument("--skip-baseline", action="store_true",
        help="Skip the uncompressed baseline run")
    parser.add_argument("--keep-output", action="store_true",
        help="Keep all level subdirectories on disk (default: cleaned at end)")
    args = parser.parse_args()

    if not SOURCE.exists():
        log(f"FATAL: source missing: {SOURCE}")
        sys.exit(1)

    BENCH_ROOT.mkdir(parents=True, exist_ok=True)

    # ── Pre-transform ───────────────────────────────────────────────────
    table = preload_source()
    in_memory_mb = table.nbytes / 1e6
    log(f"In-memory uncompressed reference: {in_memory_mb:.0f} MB")

    # ── Build settings list ─────────────────────────────────────────────
    settings: list[tuple[str, str, int | None]] = []
    if not args.skip_baseline:
        settings.append(("none", "none", None))
    for lvl in range(args.min_level, args.max_level + 1):
        settings.append((f"zstd-{lvl:02d}", "zstd", lvl))

    log(f"Running {len(settings)} compression settings...")
    log("")

    # ── Header ──────────────────────────────────────────────────────────
    header = (
        f"{'Setting':<10} {'Files':>5} {'Comp time':>10} {'Size':>11} "
        f"{'Ratio':>8} {'C MB/s':>9} {'Decomp':>9} {'D MB/s':>9}"
    )
    sep = "─" * len(header)
    print(header)
    print(sep)

    results: list[dict] = []
    baseline_size: int | None = None

    for label, codec, level in settings:
        outdir = BENCH_ROOT / label
        try:
            comp_time = write_one_level(table, outdir, codec, level)
            n_files, size_bytes = measure_size(outdir)
            decomp_time, _ = measure_decompression(outdir)

            if codec == "none" and baseline_size is None:
                baseline_size = size_bytes

            # Throughput is measured against the in-memory uncompressed size
            comp_mbs   = in_memory_mb / comp_time   if comp_time   > 0 else 0
            decomp_mbs = in_memory_mb / decomp_time if decomp_time > 0 else 0

            # Ratio: how much smaller than the no-compression baseline?
            if baseline_size and codec != "none":
                ratio = baseline_size / size_bytes
            elif codec == "none":
                ratio = 1.0
            else:
                ratio = float("nan")

            results.append({
                "setting":       label,
                "codec":         codec,
                "level":         level if level is not None else "",
                "comp_seconds":  round(comp_time, 2),
                "size_bytes":    size_bytes,
                "size_mb":       round(size_bytes / 1e6, 2),
                "ratio_vs_uncompressed": round(ratio, 3) if ratio == ratio else "",
                "decomp_seconds":     round(decomp_time, 2),
                "comp_mb_per_sec":    round(comp_mbs, 1),
                "decomp_mb_per_sec":  round(decomp_mbs, 1),
                "n_files":            n_files,
            })

            print(
                f"{label:<10} {n_files:>5} {comp_time:>9.1f}s {fmt_size(size_bytes):>11} "
                f"{ratio:>7.3f}x {comp_mbs:>8.1f} {decomp_time:>8.1f}s {decomp_mbs:>8.1f}",
                flush=True,
            )

        except Exception as e:
            print(f"{label:<10} FAILED: {e}")
            traceback.print_exc()
            results.append({
                "setting":       label,
                "codec":         codec,
                "level":         level if level is not None else "",
                "comp_seconds":  None,
                "size_bytes":    None,
                "size_mb":       None,
                "ratio_vs_uncompressed": None,
                "decomp_seconds":     None,
                "comp_mb_per_sec":    None,
                "decomp_mb_per_sec":  None,
                "n_files":            None,
                "error":              str(e),
            })

        gc.collect()

    # ── Persist results ─────────────────────────────────────────────────
    csv_path = BENCH_ROOT / "results.csv"
    if results:
        all_keys: list[str] = []
        seen = set()
        for r in results:
            for k in r:
                if k not in seen:
                    seen.add(k); all_keys.append(k)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=all_keys)
            w.writeheader()
            w.writerows(results)
        log(f"\nResults written to: {csv_path}")

    # ── Highlights ──────────────────────────────────────────────────────
    valid = [r for r in results if r.get("size_bytes") and r.get("codec") == "zstd"]
    if valid:
        best_ratio = max(valid, key=lambda r: r["ratio_vs_uncompressed"])
        fastest    = min(valid, key=lambda r: r["comp_seconds"])
        log("")
        log("Highlights (zstd only):")
        log(f"  Best ratio:    {best_ratio['setting']:<10} "
            f"{best_ratio['ratio_vs_uncompressed']:.3f}x in "
            f"{best_ratio['comp_seconds']:.1f}s")
        log(f"  Fastest comp:  {fastest['setting']:<10} "
            f"{fastest['ratio_vs_uncompressed']:.3f}x in "
            f"{fastest['comp_seconds']:.1f}s")

        # "Sweet spot": good ratio, reasonable time. Pick the level with the
        # best (ratio² / time) — heuristic that favours ratio modestly over speed.
        scored = sorted(valid,
            key=lambda r: -(r["ratio_vs_uncompressed"] ** 2 / max(r["comp_seconds"], 0.1)))
        log(f"  Sweet spot:    {scored[0]['setting']:<10} "
            f"{scored[0]['ratio_vs_uncompressed']:.3f}x in "
            f"{scored[0]['comp_seconds']:.1f}s")

    # ── Cleanup ─────────────────────────────────────────────────────────
    if not args.keep_output:
        log("\nCleaning up level subdirectories (use --keep-output to retain)...")
        for label, _, _ in settings:
            outdir = BENCH_ROOT / label
            if outdir.exists():
                shutil.rmtree(outdir, ignore_errors=True)
        log("Done.")
    else:
        log(f"\nLevel directories retained under {BENCH_ROOT}")


if __name__ == "__main__":
    main()
