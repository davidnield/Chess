"""
Stage 2: aggregate Stage 1 edge tables into position-move statistics.

Reads the per-partition edge files written by Stage 1 and groups by
(event, elo_band, parent_hash, move_san), summing result counts. The output
is the canonical empirical opening explorer -- one row per (position, move)
within each (event, elo_band) slice -- which Stage 3's backwards induction
will operate on.

Memory-bounded chunked aggregation: each input partition is aggregated
individually into a per-chunk intermediate parquet, then a final merge step
re-aggregates across chunks. This caps peak memory at one-partition-worth
(~15-25 GB on the largest 2016 partitions) regardless of the total dataset
size, avoiding the OOM that single-shot streaming hits on year-scale inputs
(observed at 1.85B edges in 2016 -- crashed Polars with a Rust allocation
panic).

Per-chunk intermediates are written atomically (.tmp + rename) and reused
across runs -- re-running after a crash skips chunks whose intermediate
already exists.

Usage:
    .venv/Scripts/python.exe python/stage2_aggregate.py
    .venv/Scripts/python.exe python/stage2_aggregate.py --min-games 30
    .venv/Scripts/python.exe python/stage2_aggregate.py --max-ply 20
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DEFAULT_INPUT = Path("E:/chess/position-moves")
DEFAULT_OUTPUT = Path("E:/chess/position-stats/position_stats.parquet")

# Default elo bands: approximate quintile boundaries computed from
# year=2025/month=4 mean_elo across all events, rounded to the nearest 100.
# Same boundaries are used for every event so that values across slices remain
# directly comparable; the resulting per-event mass is uneven (Bullet skews
# higher, Correspondence concentrates around the median) but every band has
# substantial coverage in every event.
#
# Quintile mass with these defaults (2025/04 reference month):
#   <1300        20.6%   labelled 1100
#   1300-1499    15.5%   labelled 1400
#   1500-1799    28.2%   labelled 1650  (wider band; spans 1.5 quintiles)
#   1800-1999    16.9%   labelled 1900
#   >=2000       18.8%   labelled 2200
DEFAULT_ELO_BOUNDARIES = [1300, 1500, 1800, 2000]
DEFAULT_ELO_LABELS     = [1100, 1400, 1650, 1900, 2200]

INT64_MAX = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(board: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(board)
    if h > INT64_MAX:
        h -= INT64_RANGE
    return h


def make_band_expr(boundaries: list[int], labels: list[int]) -> pl.Expr:
    """Build an elo_band expression that buckets mean_elo into the given bands.

    `boundaries` defines N split points; `labels` must have length N+1, one
    representative integer per band. Buckets are half-open: [-inf, b0), [b0, b1),
    ..., [b_{N-1}, +inf).
    """
    if len(labels) != len(boundaries) + 1:
        raise ValueError(
            f"labels must have len(boundaries)+1 entries; "
            f"got {len(labels)} labels and {len(boundaries)} boundaries"
        )
    expr = pl.lit(labels[-1], dtype=pl.Int64)
    for boundary, label in zip(reversed(boundaries), reversed(labels[:-1])):
        expr = (
            pl.when(pl.col("mean_elo") < boundary)
              .then(pl.lit(label, dtype=pl.Int64))
              .otherwise(expr)
        )
    return expr.alias("elo_band")


# Files larger than this are processed in row-group batches to avoid
# Polars sink_parquet segfaults on very large group-by hash tables.
# 5 GB is conservative; the crash threshold was ~19 GB on the April 2025
# Blitz partition.
LARGE_FILE_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5 GB


def _agg_exprs() -> list[pl.Expr]:
    """Aggregation expressions shared by both small and large chunk paths."""
    return [
        pl.col("parent_epd").first().alias("parent_epd"),
        pl.col("ply").first().alias("ply"),
        (pl.col("white_score") == 1.0).sum().cast(pl.Int64).alias("white_wins"),
        (pl.col("white_score") == 0.5).sum().cast(pl.Int64).alias("draws"),
        (pl.col("white_score") == 0.0).sum().cast(pl.Int64).alias("black_wins"),
        pl.len().cast(pl.Int64).alias("total"),
    ]


def _re_agg_exprs() -> list[pl.Expr]:
    """Re-aggregation expressions for merging sub-batch intermediates."""
    return [
        pl.col("parent_epd").first().alias("parent_epd"),
        pl.col("ply").first().alias("ply"),
        pl.col("white_wins").sum().alias("white_wins"),
        pl.col("draws").sum().alias("draws"),
        pl.col("black_wins").sum().alias("black_wins"),
        pl.col("total").sum().alias("total"),
    ]


def aggregate_chunk(
    input_path: Path,
    intermediate_path: Path,
    max_ply: int | None,
    boundaries: list[int],
    labels: list[int],
) -> None:
    """Aggregate a single input partition into a per-chunk intermediate.

    Writes atomically via .tmp + rename. The intermediate carries summed
    counts (white_wins, draws, black_wins, total) but does NOT apply the
    min_games filter -- that's deferred to the merge step so cross-chunk
    totals can accumulate before being thresholded. white_score_avg is also
    deferred (it's a function of the merged sums).

    For files exceeding LARGE_FILE_THRESHOLD, reads row groups in batches
    to avoid Polars segfaults on massive streaming group-by operations.
    """
    file_size = input_path.stat().st_size
    if file_size > LARGE_FILE_THRESHOLD:
        _aggregate_chunk_batched(input_path, intermediate_path, max_ply,
                                 boundaries, labels)
        return

    tmp_path = intermediate_path.with_suffix(".parquet.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    edges = pl.scan_parquet(str(input_path))
    if max_ply is not None:
        edges = edges.filter(pl.col("ply") <= max_ply)
    edges = edges.with_columns(make_band_expr(boundaries, labels))

    aggregated = (
        edges
        .group_by(["event", "elo_band", "parent_hash", "move_san"])
        .agg(_agg_exprs())
    )
    aggregated.sink_parquet(str(tmp_path), compression="zstd")
    tmp_path.replace(intermediate_path)


def _aggregate_chunk_batched(
    input_path: Path,
    intermediate_path: Path,
    max_ply: int | None,
    boundaries: list[int],
    labels: list[int],
    batch_row_groups: int = 50,
) -> None:
    """Batched aggregation for very large files.

    Reads `batch_row_groups` row groups at a time using pyarrow, aggregates
    each batch with Polars, and writes each batch as a separate intermediate
    file. Does NOT re-aggregate — the downstream per-slice merge handles
    cross-batch deduplication (it groups by the same keys anyway).

    This avoids Polars segfaults that occur when trying to group_by or
    sink_parquet on 15+ GB of data in a single operation. Tested: Polars
    crashes at ~50 GB working set on Windows with 128 GB RAM.

    Output: multiple intermediate files named
    ``{input_stem}_batch{NNN}.parquet`` in the intermediates directory.
    A sentinel file (the intermediate_path itself, 0 bytes) signals completion.
    """
    import pyarrow.parquet as pq

    # Sentinel: if the .done marker exists, all batches were already written
    sentinel = intermediate_path.with_suffix(".done")
    if sentinel.exists():
        return

    intermediate_dir = intermediate_path.parent
    stem = intermediate_path.stem  # e.g. "year=2025_month=4_event=Bullet"

    pf = pq.ParquetFile(str(input_path))
    n_row_groups = pf.metadata.num_row_groups
    n_batches = (n_row_groups + batch_row_groups - 1) // batch_row_groups
    print(f"      Large file ({input_path.stat().st_size / 1e9:.1f} GB, "
          f"{n_row_groups} row groups) → {n_batches} sub-batches "
          f"(no re-aggregation)", flush=True)

    group_keys = ["event", "elo_band", "parent_hash", "move_san"]

    for batch_idx in range(n_batches):
        out_file = intermediate_dir / f"{stem}_batch{batch_idx:03d}.parquet"
        if out_file.exists():
            print(f"      Sub-batch {batch_idx+1}/{n_batches}: SKIP (exists)",
                  flush=True)
            continue

        start_rg = batch_idx * batch_row_groups
        end_rg = min(start_rg + batch_row_groups, n_row_groups)

        # Read row groups into a pyarrow Table, convert to Polars
        table = pf.read_row_groups(list(range(start_rg, end_rg)))
        df = pl.from_arrow(table)
        del table  # free pyarrow memory

        if max_ply is not None:
            df = df.filter(pl.col("ply") <= max_ply)
        df = df.with_columns(make_band_expr(boundaries, labels))

        agg = (
            df.group_by(group_keys)
            .agg(_agg_exprs())
        )
        del df  # free input memory

        out_tmp = out_file.with_suffix(".parquet.tmp")
        agg.write_parquet(out_tmp, compression="zstd")
        out_tmp.replace(out_file)
        print(f"      Sub-batch {batch_idx+1}/{n_batches}: "
              f"rg {start_rg}-{end_rg-1} → {out_file.stat().st_size / 1e6:.1f} MB",
              flush=True)
        del agg

    # Write a sentinel so we know this chunk is fully processed.
    # Use .done extension so it doesn't get picked up by *.parquet globs.
    sentinel = intermediate_path.with_suffix(".done")
    sentinel.write_bytes(b"")
    print(f"      All {n_batches} sub-batches written to intermediates dir",
          flush=True)


def merge_intermediates(
    intermediate_glob: str,
    output_path: Path,
    min_games: int,
    slice_dir: Path,
) -> None:
    """Merge per-chunk intermediates into the final position-move statistics.

    Two-step strategy to keep memory tightly bounded:

      1. Per-slice on-disk merge: discover the (event, elo_band) slices,
         then merge ONE slice at a time, writing each slice's result to its
         own parquet file. Memory between slices drops back to baseline --
         no DataFrame accumulation.
      2. Concat-from-disk: scan all per-slice files and stream them into the
         final output parquet via sink_parquet.

    Earlier strategies failed:
      - Single-shot scan+group_by+sink_parquet over all intermediates
        (~33 GB on year=2016) OOMed the system around the 50% mark.
      - Per-slice in-memory merge (collect each slice, accumulate, concat)
        OOMed around slice 4 of 20 because Polars' arena allocator doesn't
        release memory between collect() calls -- working-set grew unbounded.

    Per-slice intermediates are written atomically and reused: a re-run after
    a crash skips slices whose file already exists.
    """
    tmp_path = output_path.with_suffix(".parquet.tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    slice_dir.mkdir(parents=True, exist_ok=True)

    # Discover slices present in the intermediates.
    # Use pyarrow file-by-file to avoid Polars segfaulting on large glob scans.
    # We read only the two key columns from each file — fast even with 80+ files.
    import pyarrow.parquet as pq

    print("    Discovering slices (per-file scan)...", flush=True)
    intermediate_dir_path = Path(intermediate_glob).parent
    input_files = sorted(intermediate_dir_path.glob("*.parquet"))
    seen: set[tuple] = set()
    for f in input_files:
        try:
            tbl = pq.read_table(str(f), columns=["event", "elo_band"])
            df = pl.from_arrow(tbl)
            for row in df.select(["event", "elo_band"]).unique().iter_rows():
                seen.add(row)
        except Exception as e:
            print(f"    WARNING: could not read slices from {f.name}: {e}", flush=True)
    slices = sorted(seen)
    print(f"    Found {len(slices)} (event, elo_band) slices to merge",
          flush=True)

    # ── Step 1: per-slice on-disk merge (with child_hash) ────────────────────
    for ev, eb in slices:
        slice_file = slice_dir / f"slice_{ev}_elo{eb}.parquet"
        if slice_file.exists():
            size_mb = slice_file.stat().st_size / 1e6
            print(f"    [{ev:<14} elo {eb:>5}]  SKIP (exists, {size_mb:.1f} MB)",
                  flush=True)
            continue
        slice_tmp = slice_file.with_suffix(".parquet.tmp")
        if slice_tmp.exists():
            slice_tmp.unlink()
        t0 = time.time()

        # Aggregate this slice eagerly, reading one intermediate file at a time
        # via pyarrow to avoid Polars segfaults on large glob scans.
        # Accumulate into a running group_by after each file to cap memory.
        group_keys = ["event", "elo_band", "parent_hash", "move_san"]
        acc_frames = []
        for f in input_files:
            try:
                tbl = pq.read_table(
                    str(f),
                    filters=[("event", "=", ev), ("elo_band", "=", eb)],
                )
            except Exception:
                tbl = pq.read_table(str(f))
            chunk = pl.from_arrow(tbl).filter(
                (pl.col("event") == ev) & (pl.col("elo_band") == eb)
            )
            del tbl
            if chunk.is_empty():
                continue
            acc_frames.append(chunk)
            # Eagerly re-aggregate every 20 files to keep memory bounded
            if len(acc_frames) >= 20:
                acc_frames = [
                    pl.concat(acc_frames)
                    .group_by(group_keys)
                    .agg(_re_agg_exprs())
                ]

        if not acc_frames:
            continue  # no data for this slice

        slice_df = (
            pl.concat(acc_frames)
            .group_by(group_keys)
            .agg(_re_agg_exprs())
            .filter(pl.col("total") >= min_games)
            .with_columns(
                ((pl.col("white_wins") + pl.col("draws") * 0.5) / pl.col("total"))
                .alias("white_score_avg")
            )
        )

        # Compute child_hash via Python iteration with a per-slice cache.
        # Cache is keyed by (parent_epd, move_san) so positions reached by
        # multiple paths share a single zobrist evaluation. None on parse
        # failure (matches the prior compute_child_hashes behavior in
        # stage3_backwards_induction.py).
        parent_epds = slice_df["parent_epd"].to_list()
        move_sans   = slice_df["move_san"].to_list()
        cache: dict[tuple[str, str], int | None] = {}
        child_hashes: list[int | None] = []
        for epd, san in zip(parent_epds, move_sans):
            key = (epd, san)
            if key not in cache:
                try:
                    b = chess.Board(epd)
                    b.push(b.parse_san(san))
                    h_raw = chess.polyglot.zobrist_hash(b)
                    cache[key] = h_raw - INT64_RANGE if h_raw > INT64_MAX else h_raw
                except Exception:
                    cache[key] = None
            child_hashes.append(cache[key])
        slice_df = slice_df.with_columns(
            pl.Series("child_hash", child_hashes, dtype=pl.Int64)
        )

        slice_df.write_parquet(slice_tmp, compression="zstd")
        slice_tmp.replace(slice_file)
        size_mb = slice_file.stat().st_size / 1e6
        n_null = sum(1 for h in child_hashes if h is None)
        print(f"    [{ev:<14} elo {eb:>5}]  -> {size_mb:>6.1f} MB  "
              f"({time.time()-t0:.1f}s, {len(child_hashes):,} edges, "
              f"{n_null:,} parse failures)", flush=True)

    # ── Step 2: concat per-slice files into final output ─────────────────────
    # Use pyarrow for concat to avoid Polars sink_parquet segfaults on many files.
    import pyarrow.parquet as pq_out
    slice_files = sorted(slice_dir.glob("slice_*.parquet"))
    print(f"    Concatenating {len(slice_files)} per-slice files into final output...",
          flush=True)
    t0 = time.time()
    writer = None
    for sf in slice_files:
        tbl = pq_out.read_table(str(sf))
        if writer is None:
            writer = pq_out.ParquetWriter(str(tmp_path), tbl.schema,
                                          compression="zstd")
        writer.write_table(tbl)
        del tbl
    if writer:
        writer.close()
    tmp_path.replace(output_path)
    print(f"    Concat done in {time.time()-t0:.1f}s", flush=True)


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
                             "AT MERGE TIME (default: 10). Per-chunk aggregations are NOT "
                             "filtered, so cross-chunk totals can accumulate before threshold.")
    parser.add_argument("--max-ply", type=int, default=None,
                        help="Truncate edges deeper than this ply before aggregating "
                             "(useful to limit output size; default: no limit)")
    parser.add_argument("--elo-boundaries", type=int, nargs="+",
                        default=DEFAULT_ELO_BOUNDARIES,
                        help=f"Split points for binning mean_elo into elo_band. "
                             f"Default {DEFAULT_ELO_BOUNDARIES} produces approximate "
                             f"quintiles based on 2025/04 data.")
    parser.add_argument("--elo-labels", type=int, nargs="+",
                        default=DEFAULT_ELO_LABELS,
                        help=f"Representative rating per band; must be len(boundaries)+1. "
                             f"Default {DEFAULT_ELO_LABELS}.")
    parser.add_argument("--intermediate-dir", default=None,
                        help="Directory for per-chunk intermediate parquets. "
                             "Default: <output>.intermediates/")
    parser.add_argument("--keep-intermediates", action="store_true",
                        help="Don't delete intermediate files after merge "
                             "(useful for debugging or incremental re-runs)")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.intermediate_dir:
        intermediate_dir = Path(args.intermediate_dir)
    else:
        # e.g. position_stats_2016.parquet -> position_stats_2016.intermediates/
        intermediate_dir = output_path.with_suffix(".intermediates")
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    # Per-slice merge outputs go to a sibling directory; resumable across runs.
    slice_dir = output_path.with_suffix(".slices")

    input_files = sorted(input_dir.glob("*.parquet"), key=lambda p: p.stat().st_size)
    if not input_files:
        print(f"ERROR: no parquet files found in {input_dir}")
        sys.exit(1)

    print(f"Input:           {input_dir}  ({len(input_files)} partitions)")
    print(f"Intermediate:    {intermediate_dir}")
    print(f"Output:          {output_path}")
    print(f"Min games:       {args.min_games} (applied at merge)")
    if args.max_ply:
        print(f"Max ply:         {args.max_ply}")
    print(f"Elo boundaries:  {args.elo_boundaries}")
    print(f"Elo labels:      {args.elo_labels}")

    # ── Per-chunk aggregation ────────────────────────────────────────────────
    print(f"\nAggregating {len(input_files)} partitions into per-chunk intermediates...")
    t_chunks = time.time()
    n_skipped = 0
    n_processed = 0
    for i, input_file in enumerate(input_files, 1):
        intermediate_file = intermediate_dir / input_file.name
        # For batched large files, the sentinel is .done instead of .parquet
        done_sentinel = intermediate_file.with_suffix(".done")
        if intermediate_file.exists() or done_sentinel.exists():
            n_skipped += 1
            print(f"  [{i:>2}/{len(input_files)}] SKIP (exists): {input_file.name}",
                  flush=True)
            continue
        t0 = time.time()
        aggregate_chunk(
            input_file,
            intermediate_file,
            max_ply=args.max_ply,
            boundaries=args.elo_boundaries,
            labels=args.elo_labels,
        )
        # For batched files, intermediate_file won't exist (sub-batches do)
        if intermediate_file.exists():
            in_mb  = input_file.stat().st_size        / 1e6
            out_mb = intermediate_file.stat().st_size / 1e6
        else:
            in_mb = input_file.stat().st_size / 1e6
            out_mb = 0.0  # batched: many output files
        n_processed += 1
        print(f"  [{i:>2}/{len(input_files)}] {input_file.name}  "
              f"{in_mb:>7.1f} MB -> {out_mb:>5.1f} MB  ({time.time()-t0:.1f}s)",
              flush=True)
    print(f"  Per-chunk done in {time.time()-t_chunks:.1f}s "
          f"({n_processed} processed, {n_skipped} skipped)", flush=True)

    # ── Merge ────────────────────────────────────────────────────────────────
    print(f"\nMerging intermediates into {output_path}...", flush=True)
    print(f"    Per-slice files:  {slice_dir}", flush=True)
    t0 = time.time()
    merge_intermediates(
        str(intermediate_dir / "*.parquet"),
        output_path,
        min_games=args.min_games,
        slice_dir=slice_dir,
    )
    elapsed = time.time() - t0
    size_mb = output_path.stat().st_size / 1e6
    print(f"  Merge done in {elapsed:.1f}s  ({size_mb:.1f} MB)", flush=True)

    # ── Cleanup intermediates and per-slice files ────────────────────────────
    if not args.keep_intermediates:
        print(f"\nRemoving {intermediate_dir}/ ...", flush=True)
        shutil.rmtree(intermediate_dir)
        if slice_dir.exists():
            print(f"Removing {slice_dir}/ ...", flush=True)
            shutil.rmtree(slice_dir)

    print_sanity_checks(output_path)


if __name__ == "__main__":
    main()
