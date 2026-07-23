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

from zobrist import INT64_MAX, INT64_RANGE, zobrist_int64

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
    # Use collect().write_parquet() instead of sink_parquet() — the Polars
    # streaming lazy sink_parquet() has a known Windows instability where it
    # silently produces no output file (no exception raised), causing the
    # subsequent tmp_path.replace() to raise FileNotFoundError.  The eager
    # collect()+write_parquet() path is what _aggregate_chunk_batched already
    # uses and is reliable.  Files below LARGE_FILE_THRESHOLD are at most
    # ~1–2 GB compressed; decompressed + aggregated they are well within the
    # available RAM so collecting eagerly is safe.
    aggregated.collect().write_parquet(str(tmp_path), compression="zstd")
    if not tmp_path.exists():
        raise RuntimeError(
            f"write_parquet produced no output for {input_path.name} — "
            f"check disk space on {tmp_path.parent.anchor}"
        )
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
    # Periodic gc + pyarrow memory-pool release prevents the C++ allocator
    # fragmentation that silently segfaults this loop after a few hundred
    # pq.read_table() calls on Windows (observed 0xC0000005 on 507-file 2024
    # discovery — no traceback, no error log, just process death).
    import gc as _disc_gc
    import pyarrow as _disc_pa
    for i, f in enumerate(input_files, 1):
        try:
            tbl = pq.read_table(str(f), columns=["event", "elo_band"])
            df = pl.from_arrow(tbl)
            for row in df.select(["event", "elo_band"]).unique().iter_rows():
                seen.add(row)
            del tbl, df
        except Exception as e:
            print(f"    WARNING: could not read slices from {f.name}: {e}", flush=True)
        # Cleanup every 25 files. Also print a heartbeat every 100 files so
        # we can tell whether a stalled-looking discovery is actually crashed.
        if i % 25 == 0:
            _disc_gc.collect()
            try:
                _disc_pa.default_memory_pool().release_unused()
            except Exception:
                pass
        if i % 100 == 0 or i == len(input_files):
            print(f"      scanned {i:>4}/{len(input_files)} files, "
                  f"{len(seen)} unique slices so far", flush=True)
    slices = sorted(seen)
    print(f"    Found {len(slices)} (event, elo_band) slices to merge",
          flush=True)

    # ── Step 1a: SINGLE-PASS PARTITIONING ────────────────────────────────────
    # Read each intermediate file ONCE, splitting its rows by (event, elo_band)
    # into per-slice fragment files. This replaces an earlier strategy that
    # scanned every intermediate once per slice (30 × 302 = 9,060 reads),
    # which fragmented PyArrow's memory pool and hung at 93 GB RAM on Windows.
    # Now we read each file exactly once (302 reads total) and write small
    # per-slice fragments which are aggregated independently downstream.
    import gc as _gc
    import shutil as _shutil
    import pyarrow as _pa

    fragments_root = slice_dir / "_fragments"
    partition_done = fragments_root / ".partition_complete"
    fragments_root.mkdir(parents=True, exist_ok=True)

    # Skip partitioning entirely if every slice file already exists, or if
    # the partition_done sentinel marks an earlier complete partition pass.
    all_slice_files = [slice_dir / f"slice_{ev}_elo{eb}.parquet" for ev, eb in slices]
    if all(sf.exists() for sf in all_slice_files):
        print("    All slice files already exist — skipping partitioning",
              flush=True)
    elif partition_done.exists():
        print("    Partition phase already complete (sentinel found) — "
              "skipping straight to aggregation", flush=True)
    else:
        # No sentinel = the previous attempt either never started or didn't
        # finish. Blow away any partial fragments to avoid mixing stale data
        # with fresh — partitioning is fast enough (one-pass) that redoing it
        # is cheaper than reasoning about partial-write recovery.
        if fragments_root.exists():
            stale = list(fragments_root.iterdir())
            if stale:
                print(f"    Clearing {len(stale)} partial fragment dir(s) "
                      f"from a prior run...", flush=True)
                _shutil.rmtree(fragments_root, ignore_errors=True)
            fragments_root.mkdir(parents=True, exist_ok=True)
        print(f"\n    ── Partition phase: distributing {len(input_files)} "
              f"intermediates across {len(slices)} slice fragment dirs ──",
              flush=True)
        t_part = time.time()
        for fi, f in enumerate(input_files, 1):
            t0 = time.time()
            df = pl.read_parquet(str(f))
            # partition_by gives one DataFrame per (event, elo_band) group
            parts = df.partition_by(["event", "elo_band"], as_dict=True)
            del df
            for key, sub in parts.items():
                ev_part, eb_part = key  # tuple of partition values
                ev_part = str(ev_part)
                eb_part = int(eb_part)
                fd = fragments_root / f"slice_{ev_part}_elo{eb_part}"
                fd.mkdir(parents=True, exist_ok=True)
                out     = fd / f"{f.stem}.parquet"
                out_tmp = fd / f"{f.stem}.parquet.tmp"
                # Drop the partition-key columns; the slice dir name encodes them
                sub.drop("event", "elo_band").write_parquet(
                    str(out_tmp), compression="zstd"
                )
                out_tmp.replace(out)
            del parts
            _gc.collect()
            if fi % 10 == 0:
                try:
                    _pa.default_memory_pool().release_unused()
                except Exception:
                    pass
            print(f"    [{fi:>3}/{len(input_files)}] {f.name}  "
                  f"partitioned in {time.time()-t0:.1f}s", flush=True)
        partition_done.touch()
        print(f"    Partition phase done in {(time.time()-t_part)/60:.1f} min",
              flush=True)

    # ── Step 1b: Per-slice aggregation via DuckDB (tier-based) ───────────────
    # Earlier attempts at single-pass DuckDB aggregation OOM'd or thrashed on
    # the largest slices (Blitz elo 1650 at 27.4 GB compressed → working set
    # exceeded 100 GB). Solution: tier the work. For slices with > TIER_BATCH
    # fragments, aggregate them in batches first (each writing a small tier-1
    # parquet, ~80-130 MB), then a final DuckDB pass aggregates the tier-1s.
    # Each individual query's input is now bounded so memory is bounded too.
    import duckdb as _duckdb
    print(f"\n    ── Aggregation phase: building {len(slices)} slice files "
          f"via DuckDB (tier-based) ──", flush=True)
    duckdb_tmp_dir = slice_dir / "_duckdb_tmp"
    duckdb_tmp_dir.mkdir(parents=True, exist_ok=True)
    TIER_BATCH         = 10  # max fragments per single DuckDB tier-1 call
    SUB_BATCH          = 2   # files merged per intermediate (tier-N>1) call
    MAX_FINAL_INPUTS   = 1   # cap on inputs to the final HAVING pass.
    SHARDING_THRESHOLD = 25 * 1024**3  # 25 GiB total compressed input.
    # Above this, _ddb_agg auto-switches to _ddb_agg_sharded which partitions
    # the GROUP BY work by parent_hash MOD 8 → ~1/8 the working-set memory
    # per shard, at the cost of 8× scans of the input. Discovered on
    # 2026-05-26 after Blitz/elo1650 OOM'd at every tunable setting: its
    # tier-5 files held 1.83 BILLION rows with 1.56 B unique parent_hash
    # values (measured), requiring ~150-200 GB of hash-table working set —
    # more than memory_limit at any level. Sharding is the only structural
    # fix; tuning MAX_FINAL_INPUTS / threads / memory_limit cannot help.
    # Dropped from 4 → 2 on 2026-05-25, then 2 → 1 on 2026-05-26 after
    # Blitz/elo1650 OOM'd at every escalation. Trace:
    #   - MAX=4: final pass on 3 tier-4 files (58 GB) → OOM at 102 GiB
    #   - MAX=2: final pass on 2 tier-5 files (56 GB) → OOM at 102 GiB
    # 5% dedup per tier means tier-N file sizes barely shrink no matter
    # how many tiers we add. MAX=1 changes the strategy: the merge of
    # the last two files happens WITHOUT a HAVING clause (just a plain
    # tier merge), then the final HAVING pass runs on a single
    # already-fully-aggregated file (each (parent_hash, move_san) key
    # appears exactly once) — that scan is essentially free.
    # After tier-1 we have N files. We then iteratively sub-batch (each pass
    # merges SUB_BATCH files of the prior tier) until the next-tier count is
    # ≤ MAX_FINAL_INPUTS, and only then run the HAVING filter on that small
    # set. This bounds the FINAL aggregation's working set — the prior
    # 80 GB OOM happened with 12 tier-2 files (~55 GB compressed) feeding
    # the final dedup directly. SUB_BATCH=2 matches the previous TIER2_BATCH
    # value so any pre-existing _tier2_*.parquet files on disk are reused.

    def _ddb_agg_sharded(con, in_files: list[Path], out_path: Path,
                         apply_min_games: bool, n_shards: int = 8) -> None:
        """Sharded GROUP BY for inputs whose unique-key cardinality blows memory.

        Partitions the aggregation N ways by ``((parent_hash % N) + N) % N``
        (always in [0, N) regardless of signed-int sign). Each shard sees the
        FULL data for the parent_hash buckets it owns, so per-shard HAVING
        is mathematically equivalent to a global HAVING applied after a
        single-pass aggregation. Memory per shard is ~(1/N) of the
        single-pass requirement. Cost is N full scans of the input.

        Shards are resumable: existing ``{stem}_shard_NN.parquet`` files are
        skipped. After all shards build successfully they are concatenated
        into ``out_path`` (a no-op aggregate since the buckets are disjoint)
        and the shard files are deleted.
        """
        in_list = ", ".join(f"'{str(p).replace(chr(92), '/')}'" for p in in_files)
        having = f"HAVING SUM(total) >= {int(min_games)}" if apply_min_games else ""
        shard_dir = out_path.parent / "_shards_tmp"
        shard_dir.mkdir(parents=True, exist_ok=True)
        total_size_gb = sum(p.stat().st_size for p in in_files) / 1e9
        print(f"        [shard-mode] {len(in_files)} input file(s), "
              f"{total_size_gb:.1f} GB total, {n_shards} shards "
              f"(having={'on' if apply_min_games else 'off'})", flush=True)

        shard_paths: list[Path] = []
        for shard_idx in range(n_shards):
            shard_path = shard_dir / f"{out_path.stem}_shard_{shard_idx:02d}.parquet"
            if shard_path.exists():
                shard_paths.append(shard_path)
                print(f"        [shard {shard_idx+1}/{n_shards}] SKIP existing "
                      f"({shard_path.stat().st_size/1e6:.1f} MB)", flush=True)
                continue
            shard_tmp = shard_path.with_suffix(".parquet.tmp")
            if shard_tmp.exists():
                shard_tmp.unlink()
            t_s = time.time()
            con.execute(f"""
                COPY (
                    SELECT
                        parent_hash,
                        move_san,
                        any_value(parent_epd) AS parent_epd,
                        any_value(ply)        AS ply,
                        SUM(white_wins)::BIGINT AS white_wins,
                        SUM(draws)::BIGINT      AS draws,
                        SUM(black_wins)::BIGINT AS black_wins,
                        SUM(total)::BIGINT      AS total
                    FROM read_parquet([{in_list}])
                    WHERE ((parent_hash % {n_shards}) + {n_shards}) % {n_shards} = {shard_idx}
                    GROUP BY parent_hash, move_san
                    {having}
                ) TO '{str(shard_tmp).replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            shard_tmp.replace(shard_path)
            shard_paths.append(shard_path)
            print(f"        [shard {shard_idx+1}/{n_shards}] -> "
                  f"{shard_path.stat().st_size/1e6:.1f} MB ({time.time()-t_s:.1f}s)",
                  flush=True)

        # Concat shards into final output. Buckets are disjoint by
        # construction, so this is just a parquet-level union — no further
        # aggregation needed and no memory pressure.
        shard_list = ", ".join(f"'{str(p).replace(chr(92), '/')}'" for p in shard_paths)
        out_tmp = out_path.with_suffix(".parquet.tmp")
        if out_tmp.exists():
            out_tmp.unlink()
        con.execute(f"""
            COPY (SELECT * FROM read_parquet([{shard_list}]))
            TO '{str(out_tmp).replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        out_tmp.replace(out_path)
        # Cleanup shard files only after successful concat (atomic rename above)
        for p in shard_paths:
            p.unlink(missing_ok=True)
        try:
            shard_dir.rmdir()  # only succeeds if empty
        except OSError:
            pass

    def _ddb_agg(con, in_files: list[Path], out_path: Path,
                 apply_min_games: bool) -> None:
        """Run the GROUP BY parent_hash, move_san aggregation; write out_path.

        Auto-dispatches to ``_ddb_agg_sharded`` when total input exceeds
        SHARDING_THRESHOLD — the unique-key working set scales roughly with
        input size, and large inputs OOM at any tunable memory_limit.
        """
        total_size = sum(p.stat().st_size for p in in_files)
        if total_size > SHARDING_THRESHOLD:
            _ddb_agg_sharded(con, in_files, out_path, apply_min_games, n_shards=8)
            return
        # DuckDB accepts a list literal for read_parquet
        in_list = ", ".join(f"'{str(p).replace(chr(92), '/')}'" for p in in_files)
        out = str(out_path).replace("\\", "/")
        having = f"HAVING SUM(total) >= {int(min_games)}" if apply_min_games else ""
        con.execute(f"""
            COPY (
                SELECT
                    parent_hash,
                    move_san,
                    any_value(parent_epd) AS parent_epd,
                    any_value(ply)        AS ply,
                    SUM(white_wins)::BIGINT AS white_wins,
                    SUM(draws)::BIGINT      AS draws,
                    SUM(black_wins)::BIGINT AS black_wins,
                    SUM(total)::BIGINT      AS total
                FROM read_parquet([{in_list}])
                GROUP BY parent_hash, move_san
                {having}
            ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    for slice_idx, (ev, eb) in enumerate(slices, 1):
        slice_file = slice_dir / f"slice_{ev}_elo{eb}.parquet"
        if slice_file.exists():
            size_mb = slice_file.stat().st_size / 1e6
            print(f"    [{slice_idx:>2}/{len(slices)}] [{ev:<14} elo {eb:>5}]  "
                  f"SKIP (exists, {size_mb:.1f} MB)", flush=True)
            continue

        fd = fragments_root / f"slice_{ev}_elo{eb}"
        fragment_files = sorted(fd.glob("*.parquet")) if fd.exists() else []
        # Filter out any tier outputs (tier-1, tier-2, ... and the recursive
        # sub-batch outputs) to avoid feeding them as input. The earlier code
        # only excluded `_tier1_*`, which silently let prior-run `_tier2_*` /
        # `_tier3_*` / `_tier4_*` files (multi-GB each) be treated as raw
        # fragments on a resume — caused a 102 GB DuckDB OOM on May 24 2026
        # when slice_Blitz_elo1650 picked up three 17–20 GB tier-4 files as
        # "fragments" and tried to aggregate them in a 10-input tier-1 batch.
        fragment_files = [p for p in fragment_files if not p.name.startswith("_tier")]
        if not fragment_files:
            print(f"    [{slice_idx:>2}/{len(slices)}] [{ev:<14} elo {eb:>5}]  "
                  f"no fragments — skipping", flush=True)
            continue

        slice_tmp = slice_file.with_suffix(".parquet.tmp")
        if slice_tmp.exists():
            slice_tmp.unlink()
        agg_tmp = slice_dir / f"_agg_{ev}_elo{eb}.parquet"
        if agg_tmp.exists():
            agg_tmp.unlink()

        t0 = time.time()
        n_frags = len(fragment_files)
        n_frag_bytes = sum(p.stat().st_size for p in fragment_files)
        n_batches = (n_frags + TIER_BATCH - 1) // TIER_BATCH
        print(f"    [{slice_idx:>2}/{len(slices)}] [{ev:<14} elo {eb:>5}]  "
              f"aggregating {n_frags} fragments ({n_frag_bytes/1e9:.1f} GB) "
              f"via DuckDB in {n_batches} tier(s)...", flush=True)

        con = _duckdb.connect()
        try:
            # memory_limit lowered 110 → 90 GB on 2026-05-26 after a tier-6
            # merge of 2 tier-5 files (56 GB compressed) died with a bare
            # "Allocation failure" — no size, suggesting Windows couldn't
            # satisfy the request (DuckDB at ~100 GB + Python + FS cache
            # squeezed the OS). Lower cap forces DuckDB to spill earlier.
            # CLAUDE.md warned 80 GB caused tier-3 OOMs in the past — 90
            # leaves a margin above that threshold while reclaiming ~20 GB
            # of OS headroom on a 128 GB box. Confirmed safe for the
            # remaining smaller slices (all ≤ 48 GB fragments, well under
            # the cardinality of Blitz/elo1650 which triggered this).
            con.execute("SET memory_limit='90GB';")
            con.execute(f"SET temp_directory='{str(duckdb_tmp_dir).replace(chr(92), '/')}';")
            # threads=2 (was 6 → 4 → 2 over successive Blitz/elo1650
            # attempts). Per CLAUDE.md "fewer threads = smaller per-thread
            # hash table partitions". At threads=2 we have minimal
            # parallel-hashtable merging overhead. Slower wall time but
            # the only setting we haven't OOM'd at yet.
            con.execute("SET threads=2;")
            con.execute("SET preserve_insertion_order=false;")

            if n_batches == 1:
                # Small slice — one query, write straight to agg_tmp
                _ddb_agg(con, fragment_files, agg_tmp, apply_min_games=True)
            else:
                # Tier-based: aggregate fragments in TIER_BATCH-sized chunks,
                # then aggregate the tier-1 outputs in a final pass with HAVING.
                #
                # Stale-tier-1 guard via sidecar JSON: if n_batches has changed
                # since the last run (e.g. TIER_BATCH was reduced, or more
                # fragments were added so the same TIER_BATCH yields a different
                # count), existing tier-1 files cover different fragment
                # boundaries and are INCOMPATIBLE — even if all their indices
                # are still < the new n_batches (the old index-based check
                # missed exactly this case).  A small sidecar JSON next to the
                # tier-1 files records the n_batches used to build them; any
                # mismatch — or the *absence* of a sidecar — triggers a full
                # purge.  Absence means either a first run with this new code
                # (files may be from an old TIER_BATCH run) or a crash before
                # the sidecar was written; both cases are safer to rebuild.
                import json as _json
                tier1_sidecar = fd / "_tier1_meta.json"
                existing_t1 = sorted(fd.glob("_tier1_*.parquet"))
                needs_purge = False
                purge_reason = ""
                if tier1_sidecar.exists():
                    try:
                        meta = _json.loads(tier1_sidecar.read_text())
                        if meta.get("n_batches") != n_batches:
                            needs_purge = True
                            purge_reason = (
                                f"sidecar n_batches={meta.get('n_batches')} "
                                f"!= current {n_batches}"
                            )
                    except Exception:
                        needs_purge = True
                        purge_reason = "sidecar unreadable"
                elif existing_t1:
                    # No sidecar: files exist but were built without boundary
                    # tracking — cannot verify they match current n_batches.
                    needs_purge = True
                    purge_reason = (
                        f"no sidecar; {len(existing_t1)} tier-1 file(s) of "
                        f"unknown provenance (TIER_BATCH may have changed)"
                    )
                if needs_purge:
                    # Tier-1 boundaries changed: tier-1 files are stale, and
                    # SO ARE all downstream tier-N>1 files (each one was built
                    # by SUB_BATCH-pairing the previous tier's now-stale
                    # boundaries). If we left them in place the recursive
                    # sub-batching loop would silently SKIP-existing them and
                    # produce incorrect aggregates from mismatched inputs.
                    # Purge unconditionally on any boundary mismatch, even if
                    # no tier-1 files happen to be on disk right now.
                    stale_downstream = sorted(fd.glob("_tier*_[0-9]*.parquet"))
                    stale_downstream = [p for p in stale_downstream
                                        if p not in existing_t1]
                    print(f"        [stale tier purge] {purge_reason} "
                          f"→ deleting {len(existing_t1)} tier-1 file(s), "
                          f"{len(stale_downstream)} downstream tier file(s), "
                          f"+ sidecar", flush=True)
                    for stale in existing_t1:
                        stale.unlink(missing_ok=True)
                    for stale in stale_downstream:
                        stale.unlink(missing_ok=True)
                    tier1_sidecar.unlink(missing_ok=True)
                    existing_t1 = []
                # Write/update sidecar before the loop so a crash mid-build
                # leaves it in place; next run compares and purges if needed.
                tier1_sidecar.write_text(_json.dumps({"n_batches": n_batches}))

                tier1_files = []
                for b_idx in range(n_batches):
                    t_b = time.time()
                    batch = fragment_files[b_idx*TIER_BATCH
                                           :(b_idx+1)*TIER_BATCH]
                    tier1_path = fd / f"_tier1_{b_idx:03d}.parquet"
                    if tier1_path.exists():
                        # Resume support: skip an already-built tier-1
                        print(f"        tier-1 {b_idx+1}/{n_batches}: "
                              f"SKIP existing ({tier1_path.stat().st_size/1e6:.1f} MB)",
                              flush=True)
                        tier1_files.append(tier1_path)
                        continue
                    tier1_tmp = tier1_path.with_suffix(".parquet.tmp")
                    if tier1_tmp.exists():
                        tier1_tmp.unlink()
                    _ddb_agg(con, batch, tier1_tmp, apply_min_games=False)
                    tier1_tmp.replace(tier1_path)
                    tier1_files.append(tier1_path)
                    print(f"        tier-1 {b_idx+1}/{n_batches}: "
                          f"{len(batch)} frags -> "
                          f"{tier1_path.stat().st_size/1e6:.1f} MB "
                          f"({time.time()-t_b:.1f}s)", flush=True)

                # ── Recursive sub-batching to bound the final HAVING pass ───
                # Iteratively merge tier-N files in groups of SUB_BATCH (no
                # HAVING) until the next-tier count is ≤ MAX_FINAL_INPUTS,
                # then run the final HAVING pass on those few files.
                #
                # Early-exit on cardinality: if the largest sub-batch would
                # exceed SHARDING_THRESHOLD, building the next tier would
                # itself OOM (the merge has the same hash-table requirement
                # as the final pass). In that case skip ahead — _ddb_agg
                # will auto-shard the final pass on whatever current_files
                # we have, which is correct because the per-shard HAVING
                # filter is equivalent to a global one.
                current_files: list[Path] = list(tier1_files)
                current_tier: int = 1
                while len(current_files) > MAX_FINAL_INPUTS:
                    max_sub_size = max(
                        sum(p.stat().st_size for p in current_files[i:i+SUB_BATCH])
                        for i in range(0, len(current_files), SUB_BATCH)
                    )
                    if max_sub_size > SHARDING_THRESHOLD:
                        print(f"        [skip-to-shard] next sub-batch would "
                              f"be {max_sub_size/1e9:.1f} GB > "
                              f"{SHARDING_THRESHOLD/1e9:.1f} GB threshold — "
                              f"going directly to sharded final pass on "
                              f"{len(current_files)} tier-{current_tier} "
                              f"files", flush=True)
                        break
                    next_tier = current_tier + 1
                    n_sub = (len(current_files) + SUB_BATCH - 1) // SUB_BATCH
                    next_files: list[Path] = []
                    for sb_idx in range(n_sub):
                        t_sb = time.time()
                        sub = current_files[sb_idx*SUB_BATCH
                                            :(sb_idx+1)*SUB_BATCH]
                        sub_path = fd / f"_tier{next_tier}_{sb_idx:03d}.parquet"
                        if sub_path.exists():
                            print(f"        tier-{next_tier} {sb_idx+1}/{n_sub}: "
                                  f"SKIP existing "
                                  f"({sub_path.stat().st_size/1e6:.1f} MB)",
                                  flush=True)
                            next_files.append(sub_path)
                            continue
                        sub_tmp = sub_path.with_suffix(".parquet.tmp")
                        if sub_tmp.exists():
                            sub_tmp.unlink()
                        _ddb_agg(con, sub, sub_tmp, apply_min_games=False)
                        sub_tmp.replace(sub_path)
                        next_files.append(sub_path)
                        print(f"        tier-{next_tier} {sb_idx+1}/{n_sub}: "
                              f"{len(sub)} tier-{current_tier} files -> "
                              f"{sub_path.stat().st_size/1e6:.1f} MB "
                              f"({time.time()-t_sb:.1f}s)", flush=True)
                    current_files = next_files
                    current_tier = next_tier

                # Final HAVING-filtered pass (auto-sharded inside _ddb_agg
                # if total input > SHARDING_THRESHOLD)
                t_final = time.time()
                _ddb_agg(con, current_files, agg_tmp, apply_min_games=True)
                final_label = (f"tier-{current_tier+1} (final)"
                               if current_tier > 1
                               else "tier-2 (final)")
                print(f"        {final_label}: {len(current_files)} "
                      f"tier-{current_tier} files -> "
                      f"{agg_tmp.stat().st_size/1e6:.1f} MB "
                      f"({time.time()-t_final:.1f}s)", flush=True)

                # Sweep cleanup: remove ALL intermediate tier files for this
                # slice now that agg_tmp is built. This includes tier-1
                # through tier-N, regardless of how deep the recursion went.
                for stale in fd.glob("_tier*_*.parquet"):
                    try:
                        stale.unlink()
                    except Exception:
                        pass
                try:
                    tier1_sidecar.unlink(missing_ok=True)
                except Exception:
                    pass
        finally:
            con.close()
        agg_mb = agg_tmp.stat().st_size / 1e6
        print(f"        DuckDB pre-aggregation -> {agg_mb:.1f} MB "
              f"({time.time()-t0:.1f}s total)", flush=True)

        # Now load the pre-aggregated slice into Polars to add the
        # event/elo_band/white_score_avg/child_hash columns.
        slice_df = pl.read_parquet(str(agg_tmp)).with_columns(
            pl.lit(ev).alias("event"),
            pl.lit(eb).cast(pl.Int64).alias("elo_band"),
            ((pl.col("white_wins") + pl.col("draws") * 0.5) / pl.col("total"))
            .alias("white_score_avg")
        )

        # Compute child_hash via Python iteration with a per-slice cache.
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
        print(f"    [{slice_idx:>2}/{len(slices)}] [{ev:<14} elo {eb:>5}]  "
              f"-> {size_mb:>6.1f} MB ({time.time()-t0:.1f}s, "
              f"{len(child_hashes):,} edges, {n_null:,} parse failures)",
              flush=True)

        # Free the fragments for this slice — saves disk during long runs.
        # Also remove the DuckDB pre-aggregation file (we have the final slice now).
        try:
            _shutil.rmtree(fd, ignore_errors=True)
        except Exception:
            pass
        try:
            if agg_tmp.exists():
                agg_tmp.unlink()
        except Exception:
            pass
        del slice_df, parent_epds, move_sans, cache, child_hashes
        _gc.collect()
        try:
            _pa.default_memory_pool().release_unused()
        except Exception:
            pass

    # Cleanup: drop the now-empty fragments root if all slices succeeded
    try:
        if fragments_root.exists():
            remaining = [d for d in fragments_root.iterdir()
                         if d.is_dir() and any(d.glob("*.parquet"))]
            if not remaining:
                _shutil.rmtree(fragments_root, ignore_errors=True)
    except Exception:
        pass
    # And the duckdb temp/spill directory
    try:
        if duckdb_tmp_dir.exists():
            _shutil.rmtree(duckdb_tmp_dir, ignore_errors=True)
    except Exception:
        pass

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
