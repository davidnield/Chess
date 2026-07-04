"""
build_pooled_stats.py — fast, reusable combined position-stats + relative crush
histogram, pooled to a SINGLE slice (event='Pooled', elo_band=0).

This is the CANONICAL pooler for new datasets: it fuses extraction + elo/event
filtering + pooling straight from the source parquets. (build_combined_slice.py is the
OTHER pooling path — it re-pools stats that were already aggregated per (event,elo_band).
Use that only when you already have per-band position_stats and just want to combine them.)

Fused single-pass design (replaces the old Stage-1 raw-edge dump + Stage-2 tier
merge, whose tier/shard/fragment machinery was ~OOM-defensive scaffolding for the
1.56-billion-key multi-band slices — eliminated here by filtering + pooling):

  For each source parquet, BEFORE any move parsing, drop games with mean_elo
  below --min-elo (only ~1/3 of Blitz, ~1/5 of Rapid, ~1/6 of Classical qualify
  at 1800 -> ~75% of work skipped). Replay each surviving game ONCE and, per
  chunk, accumulate BOTH:
    - position-stats   : (parent_hash, move_san) -> white/draw/black/total
    - relative crush   : (parent_hash, move_san, move_bucket) -> n/white/black
      move_bucket = clip(move_count - (ply-1)//2, 1, 60) for a decisive NORMAL
      win, else 0  (full moves from THIS position to the decisive end — same
      formula as build_crush_stats.py).
  Each chunk writes two SMALL pre-aggregated partials (never the multi-GB per-ply
  edge files). Two final single-pass DuckDB GROUP BYs merge the partials. No
  tiers — filtering + pooling keep cardinality bounded.

Reusable: parameterized by --start-year/--end-year/--months, --min-elo, --events,
--min-games, --max-ply. Resumable: per-chunk partial skip-gate + atomic .tmp
writes; re-run for a new month or a different filter = another invocation.

Usage:
  # full target build (2019-2025, Blitz+Rapid+Classical, mean_elo>=1800)
  .venv/Scripts/python.exe python/build_pooled_stats.py --start-year 2019 --end-year 2025

  # single month, extract phase only (benchmarking)
  .venv/Scripts/python.exe python/build_pooled_stats.py --start-year 2023 --end-year 2023 \
      --months 1 --phase extract --workers 11

  # merge phase only (after partials exist)
  .venv/Scripts/python.exe python/build_pooled_stats.py --start-year 2019 --end-year 2025 --phase merge
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import chess
import chess.polyglot
import polars as pl
import pyarrow.parquet as pq

# Reuse the proven SAN tokenizer + signed-int64 Zobrist from Stage 1 (sibling module).
from stage1_extract_positions import iter_san_moves, zobrist_int64

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

SOURCE_ROOT = Path("D:/data/chess/standard-chess-games-compressed")
STATS_DIR = Path("E:/chess/position-stats")
POOL_EVENT = "Pooled"
POOL_ELO = 0

# (year, month) partitions to skip: 2024-10 source is corrupt (broken move numbering).
SKIP_PARTITIONS = {(2024, 10)}

# Source columns needed for the fused pass. No game_id needed (we aggregate, never
# join per-game crush facts). movetext is the heavyweight string.
SRC_COLUMNS = ["movetext", "white_score", "termination", "move_count", "mean_elo"]
READ_BATCH_GAMES = 50_000
# Compact the per-chunk accumulator every N batches to bound peak memory (the
# concat+regroup keeps the running frame near the chunk's unique-key count).
COMPACT_EVERY = 8


# ── per-chunk fused extraction ────────────────────────────────────────────────

def _agg_ps(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("parent_hash", "move_san").agg(
        pl.col("parent_epd").first().alias("parent_epd"),
        pl.col("ply").first().alias("ply"),
        pl.col("ws").eq(1.0).sum().cast(pl.Int64).alias("white_wins"),
        pl.col("ws").eq(0.5).sum().cast(pl.Int64).alias("draws"),
        pl.col("ws").eq(0.0).sum().cast(pl.Int64).alias("black_wins"),
        pl.len().cast(pl.Int64).alias("total"),
    )


def _agg_ps_resum(df: pl.DataFrame) -> pl.DataFrame:
    # Re-aggregate already-aggregated ps frames (sum the counts; keep a representative epd/ply).
    return df.group_by("parent_hash", "move_san").agg(
        pl.col("parent_epd").first().alias("parent_epd"),
        pl.col("ply").first().alias("ply"),
        pl.col("white_wins").sum().alias("white_wins"),
        pl.col("draws").sum().alias("draws"),
        pl.col("black_wins").sum().alias("black_wins"),
        pl.col("total").sum().alias("total"),
    )


def _agg_crush(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("parent_hash", "move_san", "move_bucket").agg(
        pl.len().cast(pl.Int64).alias("n"),
        pl.col("wn").sum().cast(pl.Int64).alias("white_wins"),
        pl.col("bn").sum().cast(pl.Int64).alias("black_wins"),
    )


def _agg_crush_resum(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("parent_hash", "move_san", "move_bucket").agg(
        pl.col("n").sum().alias("n"),
        pl.col("white_wins").sum().alias("white_wins"),
        pl.col("black_wins").sum().alias("black_wins"),
    )


def _new_buf() -> dict:
    return {k: [] for k in
            ("parent_hash", "move_san", "parent_epd", "ply", "ws", "wn", "bn", "move_bucket")}


def classify_maxply(tiers: dict | None, w1: str | None, b1: str | None,
                    cap: int) -> int:
    """Asymmetric-depth tier for a game, keyed on its first two SAN moves.

    30/18/10-ply tiers (configurable) seeded from existing >=1800 frequency stats:
      - W1 in the main set {e4,d4,c4,Nf3} + a common Black reply (P(b1|w1) >= deep) -> deep_ply
      - W1 in main set + a mid reply (mid <= P(b1|w1) < deep)                       -> mid_ply
      - W1 an offbeat-but-common first move (P(w1) >= deep, not in main set)        -> mid_ply
      - everything else (rare first moves / rare replies)                          -> shallow_ply
    tiers=None disables pruning (full `cap`).
    """
    if tiers is None:
        return cap
    if w1 in tiers["four"]:
        if b1 is not None and b1 in tiers["deep_resp"].get(w1, ()):
            return tiers["deep_ply"]
        if b1 is not None and b1 in tiers["mid_resp"].get(w1, ()):
            return tiers["mid_ply"]
        return tiers["shallow_ply"]
    if w1 in tiers["common_white_other"]:
        return tiers["mid_ply"]
    return tiers["shallow_ply"]


def _walk_game(buf: dict, movetext, white_score, white_norm, black_norm,
               move_count, tiers, max_ply_cap) -> bool:
    """Append one game's per-ply rows into the columnar buffer, capping depth by the
    asymmetric tier (classified from the first two SAN moves). Returns True on parse error.

    NOTE: move_bucket still uses the game's REAL move_count (full length from source),
    so the relative-crush horizon is unaffected by where we truncate extraction.
    """
    if not movetext:
        return False
    toks = list(iter_san_moves(movetext))
    if not toks:
        return False
    w1 = toks[0]
    b1 = toks[1] if len(toks) > 1 else None
    maxply = min(classify_maxply(tiers, w1, b1, max_ply_cap), max_ply_cap, len(toks))
    decisive = white_norm or black_norm
    board = chess.Board()
    for ply in range(1, maxply + 1):
        san = toks[ply - 1]
        ph = zobrist_int64(board)
        epd = board.epd()
        try:
            move = board.parse_san(san)
        except (ValueError, AssertionError):
            return True
        if decisive and move_count is not None:
            b = move_count - ((ply - 1) // 2)
            b = 1 if b < 1 else (60 if b > 60 else b)
        else:
            b = 0
        buf["parent_hash"].append(ph)
        buf["move_san"].append(san)
        buf["parent_epd"].append(epd)
        buf["ply"].append(ply)
        buf["ws"].append(white_score)
        buf["wn"].append(1 if white_norm else 0)
        buf["bn"].append(1 if black_norm else 0)
        buf["move_bucket"].append(b)
        board.push(move)
    return False


_BUF_SCHEMA = {
    "parent_hash": pl.Int64, "move_san": pl.Utf8, "parent_epd": pl.Utf8,
    "ply": pl.Int32, "ws": pl.Float64, "wn": pl.Int32, "bn": pl.Int32,
    "move_bucket": pl.Int32,
}


def extract_file(src_file: Path, ps_out: Path, crush_out: Path,
                 min_elo: int, max_ply: int, tiers: dict | None,
                 limit_games: int | None = None) -> dict:
    """Fused per-file extractor: filter -> replay once -> pre-aggregated ps + crush partials."""
    if ps_out.exists() and crush_out.exists():
        return {"file": src_file.name, "skipped": True, "games": 0, "sec": 0.0}

    t0 = time.time()
    n_games = n_kept = n_failed = 0
    ps_parts: list[pl.DataFrame] = []
    crush_parts: list[pl.DataFrame] = []
    buf = _new_buf()

    def flush_batch():
        if not buf["parent_hash"]:
            return
        df = pl.DataFrame(buf, schema=_BUF_SCHEMA)
        ps_parts.append(_agg_ps(df))
        crush_parts.append(_agg_crush(df))
        for v in buf.values():
            v.clear()

    def compact():
        nonlocal ps_parts, crush_parts
        if len(ps_parts) > 1:
            ps_parts = [_agg_ps_resum(pl.concat(ps_parts))]
        if len(crush_parts) > 1:
            crush_parts = [_agg_crush_resum(pl.concat(crush_parts))]

    pf = pq.ParquetFile(src_file)
    batch_i = 0
    done = False
    for batch in pf.iter_batches(batch_size=READ_BATCH_GAMES, columns=SRC_COLUMNS):
        for rec in batch.to_pylist():
            if limit_games is not None and n_games >= limit_games:
                done = True
                break
            n_games += 1
            me = rec["mean_elo"]
            if me is None or me < min_elo:
                continue
            ws = rec["white_score"]
            if ws is None:
                continue
            n_kept += 1
            term = rec["termination"]
            normal = term == "Normal"
            white_norm = normal and ws == 1.0
            black_norm = normal and ws == 0.0
            if _walk_game(buf, rec["movetext"], ws, white_norm, black_norm,
                          rec["move_count"], tiers, max_ply):
                n_failed += 1
        flush_batch()
        batch_i += 1
        if batch_i % COMPACT_EVERY == 0:
            compact()
        if done:
            break

    compact()
    ps_df = ps_parts[0] if ps_parts else pl.DataFrame(schema={
        "parent_hash": pl.Int64, "move_san": pl.Utf8, "parent_epd": pl.Utf8,
        "ply": pl.Int32, "white_wins": pl.Int64, "draws": pl.Int64,
        "black_wins": pl.Int64, "total": pl.Int64})
    crush_df = crush_parts[0] if crush_parts else pl.DataFrame(schema={
        "parent_hash": pl.Int64, "move_san": pl.Utf8, "move_bucket": pl.Int32,
        "n": pl.Int64, "white_wins": pl.Int64, "black_wins": pl.Int64})

    ps_out.parent.mkdir(parents=True, exist_ok=True)
    ps_tmp = ps_out.with_suffix(".parquet.tmp")
    cr_tmp = crush_out.with_suffix(".parquet.tmp")
    ps_df.write_parquet(ps_tmp, compression="zstd")
    crush_df.write_parquet(cr_tmp, compression="zstd")
    ps_tmp.replace(ps_out)
    cr_tmp.replace(crush_out)
    return {"file": src_file.name, "skipped": False, "games": n_games, "kept": n_kept,
            "failed": n_failed, "ps_rows": ps_df.height, "crush_rows": crush_df.height,
            "sec": time.time() - t0}


# Per-worker globals set once by the pool initializer (avoids re-pickling the tier
# tables and config on every task).
_W: dict = {}


def _init_worker(min_elo: int, max_ply: int, tiers: dict | None) -> None:
    _W["min_elo"] = min_elo
    _W["max_ply"] = max_ply
    _W["tiers"] = tiers


def _worker(task: tuple) -> dict:
    src_str, ps_str, cr_str, limit = task
    return extract_file(Path(src_str), Path(ps_str), Path(cr_str),
                        _W["min_elo"], _W["max_ply"], _W["tiers"], limit)


# ── depth-tier seeding (from existing >=1800 frequency stats) ───────────────────

def build_depth_tiers(seed_path: Path, four: list[str], deep_thresh: float,
                      mid_thresh: float, deep_ply: int, mid_ply: int,
                      shallow_ply: int) -> dict:
    """Derive the asymmetric-depth tier tables from an existing position_stats file.

    P(w1) from the start position; P(b1|w1) using the exact denominator = the
    start->w1 edge total (= number of games that played w1).
    """
    st = pl.read_parquet(seed_path, columns=["parent_hash", "move_san", "total"])
    start = zobrist_int64(chess.Board())
    sd = st.filter(pl.col("parent_hash") == start)
    tot = sd["total"].sum()
    wtot = {r["move_san"]: r["total"] for r in sd.iter_rows(named=True)}
    four_set = set(four)
    common_white_other = {m for m, t in wtot.items()
                          if m not in four_set and t / tot >= deep_thresh}
    deep_resp: dict[str, set] = {}
    mid_resp: dict[str, set] = {}
    for w1 in four:
        denom = wtot.get(w1, 0)
        deep_resp[w1], mid_resp[w1] = set(), set()
        if not denom:
            continue
        b = chess.Board()
        b.push_san(w1)
        h = zobrist_int64(b)
        for r in st.filter(pl.col("parent_hash") == h).iter_rows(named=True):
            f = r["total"] / denom
            if f >= deep_thresh:
                deep_resp[w1].add(r["move_san"])
            elif f >= mid_thresh:
                mid_resp[w1].add(r["move_san"])
    return {"four": four_set, "deep_resp": deep_resp, "mid_resp": mid_resp,
            "common_white_other": common_white_other,
            "deep_ply": deep_ply, "mid_ply": mid_ply, "shallow_ply": shallow_ply}


# ── discovery ─────────────────────────────────────────────────────────────────

def discover_source_files(start_year: int, end_year: int, months: list[int] | None,
                          events: list[str]) -> list[tuple[Path, int, int, str]]:
    out = []
    for year in range(start_year, end_year + 1):
        ydir = SOURCE_ROOT / f"year={year}"
        if not ydir.is_dir():
            continue
        for mdir in sorted(ydir.glob("month=*")):
            month = int(mdir.name.split("=")[1])
            if months and month not in months:
                continue
            if (year, month) in SKIP_PARTITIONS:
                continue
            for ev in events:
                edir = mdir / f"event={ev}"
                if not edir.is_dir():
                    continue
                for f in sorted(edir.glob("*.parquet")):
                    out.append((f, year, month, ev))
    return out


def partial_name(src_file: Path, year: int, month: int, event: str, kind: str) -> str:
    return f"year={year}_month={month}_event={event}_{src_file.stem}.{kind}.parquet"


# ── final merges (DuckDB, single pass) ─────────────────────────────────────────

def _sql_path(p) -> str:
    """Forward-slashed path string for embedding in DuckDB SQL (Windows backslashes
    break the SQL string literal). One idiom for every merge-phase query below."""
    return str(p).replace("\\", "/")


def _duck(threads: int, mem: str, tmp: Path):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}';")
    con.execute(f"SET temp_directory='{_sql_path(tmp)}';")
    con.execute(f"SET threads={threads};")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=false;")
    return con


_MONTH_RE = re.compile(r"year=(\d+)_month=(\d+)_event=")

# The final cross-month GROUP BYs are two-phase external aggregations:
#   Phase A: stream-partition every monthly file into N key-hash buckets
#            (COPY ... PARTITION_BY, no GROUP BY -> near-zero memory).
#   Phase B: GROUP BY each bucket's slices in RAM (state ~ total/N).
# Rationale (measured, July 2026): ~6B unique (parent_hash, move_san) keys ->
# ~700 GB aggregate state. Monolithic GROUP BYs OOMed at 90/100 GB limits, and
# direct 8-bucket passes OOMed at 60 GB with ZERO spill written — DuckDB 1.5's
# out-of-core aggregation never engaged for this query shape, and even working
# spill would push ~2x state through temp. Two-phase does 2 passes total instead
# of N re-scans. All rows of a key share a parent_hash, so every key lands wholly
# in one bucket and the min_games HAVING remains a true final-pass filter.
N_MERGE_BUCKETS = 32


def _bucket_expr(col: str) -> str:
    # Arithmetic bucketing (not hash()) so resumed runs assign identical buckets
    # regardless of DuckDB version. Zobrist hashes are uniform in the low bits;
    # the double-modulo folds negative int64 values into [0, N).
    return f"(({col} % {N_MERGE_BUCKETS}) + {N_MERGE_BUCKETS}) % {N_MERGE_BUCKETS}"


def _run_copy_query(task: tuple) -> tuple:
    """Run ONE COPY query in a fresh process, then exit (same allocator-isolation
    rationale as _consolidate_one_month). Atomic tmp->rename: output exists only
    when complete. Targets may be files (single COPY) or directories
    (COPY ... PARTITION_BY)."""
    sql, out_tmp_str, out_str, threads, mem, tmp_str, label = task
    out_tmp, out = Path(out_tmp_str), Path(out_str)
    if out_tmp.is_dir():
        shutil.rmtree(out_tmp)
    else:
        out_tmp.unlink(missing_ok=True)
    t0 = time.time()
    con = _duck(threads, mem, Path(tmp_str))
    try:
        con.execute(sql)
    finally:
        con.close()
    out_tmp.replace(out)
    size = (sum(f.stat().st_size for f in out.rglob("*") if f.is_file())
            if out.is_dir() else out.stat().st_size)
    return label, size, time.time() - t0


def _partition_by_bucket(src_dir: Path, kind: str, part_dir: Path,
                         threads: int, mem: str, tmp_dir: Path) -> None:
    """Phase A: stream-split every monthly <kind> file into N_MERGE_BUCKETS
    hive partitions (part_dir/month=Y_M/bkt=N/*.parquet). Pure COPY, no GROUP BY,
    so memory stays at partition-writer buffers. Skip-gated per month on the
    final month dir; in-flight writes go to _tmp_month=* (never matched by the
    Phase-B glob) and are cleaned on restart."""
    part_dir.mkdir(parents=True, exist_ok=True)
    for stale in part_dir.glob("_tmp_month=*"):
        shutil.rmtree(stale, ignore_errors=True)
    months = {}
    for f in src_dir.glob(f"*.{kind}.parquet"):
        m = re.match(r"year=(\d+)_month=(\d+)", f.name)
        if m:
            months[(int(m.group(1)), int(m.group(2)))] = f
    tasks = []
    for (y, mo), f in sorted(months.items()):
        out = part_dir / f"month={y}_{mo}"
        if out.exists():
            continue
        out_tmp = part_dir / f"_tmp_month={y}_{mo}"
        sql = f"""
            COPY (
                SELECT *, {_bucket_expr("parent_hash")} AS bkt
                FROM read_parquet('{_sql_path(f)}')
            ) TO '{_sql_path(out_tmp)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (bkt))
        """
        tasks.append((sql, str(out_tmp), str(out), threads, mem, str(tmp_dir),
                      f"partition {kind} {y}/{mo}"))
    print(f"  Phase A ({kind}): {len(months)} months, {len(tasks)} to partition",
          flush=True)
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
        for fut in as_completed([ex.submit(_run_copy_query, t) for t in tasks]):
            label, size, secs = fut.result()
            print(f"    {label}: {size/1e6:.0f} MB ({secs:.0f}s)", flush=True)


def _consolidate_one_month(task: tuple) -> tuple:
    """Run ONE month's SUM-only GROUP BY in a fresh process, then exit.

    Per-month process isolation is REQUIRED on Windows. Running every month's
    DuckDB GROUP BY in a single long-lived process degrades throughput ~3x over a
    handful of same-size months (measured: 785s -> 2115s across four flat ~5.5 GB
    months, zero spill) — native-allocator fragmentation from repeated multi-GB
    buffer alloc/free, the same gremlin CLAUDE.md documents as 0xC0000005, here
    surfacing as slowdown rather than a segfault. A fresh process per month resets
    it, holding throughput flat. The DuckDB query is byte-identical to the in-line
    version it replaced.
    """
    kind, grp, sums, in_files, out_str, threads, mem, tmp_str, label = task
    out = Path(out_str)
    out_tmp = out.with_suffix(".parquet.tmp")
    in_list = ", ".join(f"'{_sql_path(Path(p))}'" for p in in_files)
    t0 = time.time()
    con = _duck(threads, mem, Path(tmp_str))
    try:
        con.execute(f"""
            COPY (
                SELECT {grp}, {sums}
                FROM read_parquet([{in_list}])
                GROUP BY {grp}
            ) TO '{_sql_path(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    out_tmp.replace(out)
    return label, len(in_files), out.stat().st_size, time.time() - t0


def consolidate_monthly(partial_dir: Path, threads: int, mem: str,
                        tmp_base: Path | None = None) -> Path:
    """SUM-only per-(year,month) consolidation of the per-file partials.

    NO min_games filter here — pure summation, so a position split across files/
    events/months can still reach the threshold at the FINAL merge. Collapses ~1800
    file-partials to ~80 monthly partials (within-month dedup), making the final
    GROUP BY tractable and crash-safe. Resumable: skip-gated per month, atomic write.
    Returns the monthly directory.
    """
    mdir = partial_dir / "_monthly"
    mdir.mkdir(parents=True, exist_ok=True)
    tmp_dir = (tmp_base or partial_dir) / "_merge_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    for kind, grp, sums in (
        ("ps", "parent_hash, move_san",
         "any_value(parent_epd) AS parent_epd, any_value(ply) AS ply, "
         "SUM(white_wins)::BIGINT AS white_wins, SUM(draws)::BIGINT AS draws, "
         "SUM(black_wins)::BIGINT AS black_wins, SUM(total)::BIGINT AS total"),
        ("crush", "parent_hash, move_san, move_bucket",
         "SUM(n)::BIGINT AS n, SUM(white_wins)::BIGINT AS white_wins, "
         "SUM(black_wins)::BIGINT AS black_wins"),
    ):
        months: dict[tuple[int, int], list[Path]] = {}
        for f in partial_dir.glob(f"*.{kind}.parquet"):
            m = _MONTH_RE.search(f.name)
            if m:
                months.setdefault((int(m.group(1)), int(m.group(2))), []).append(f)
        todo = [(ym, fs) for ym, fs in sorted(months.items())
                if not (mdir / f"year={ym[0]}_month={ym[1]}.{kind}.parquet").exists()]
        print(f"  consolidate {kind}: {len(months)} months, {len(todo)} to build", flush=True)
        if not todo:
            continue
        # Per-month process isolation: each GROUP BY runs in a worker that is recycled
        # after one task (max_tasks_per_child=1), resetting the native-allocator
        # fragmentation that otherwise degrades throughput ~3x over a few months.
        # max_workers=1 keeps months sequential — each may use the full --mem budget.
        tasks = [
            (kind, grp, sums, [str(p) for p in files],
             str(mdir / f"year={y}_month={mo}.{kind}.parquet"),
             threads, mem, str(tmp_dir), f"{y}/{mo}")
            for (y, mo), files in todo
        ]
        with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
            for fut in as_completed([ex.submit(_consolidate_one_month, t) for t in tasks]):
                label, nfiles, size, secs = fut.result()
                print(f"    {label} {kind}: {nfiles} files -> "
                      f"{size/1e6:.0f} MB ({secs:.0f}s)", flush=True)
    return mdir


def merge_position_stats(src_dir: Path, partial_dir: Path, out_path: Path, min_games: int,
                         threads: int, mem: str, tmp_base: Path | None = None) -> None:
    if out_path.exists():
        print(f"SKIP ps merge: {out_path.name} exists")
        return
    tmp_dir = (tmp_base or partial_dir) / "_merge_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    # Durable GROUP BY checkpoint. The HAVING-filtered aggregation is the expensive part;
    # the single-threaded child_hash loop that follows can run for many minutes and is a
    # crash risk. Write the aggregation to a temp then atomically rename to the checkpoint,
    # so it exists ONLY when complete — a crash in the child_hash loop then resumes from
    # here instead of re-running the whole multi-billion-row GROUP BY.
    agg_ckpt = out_path.with_name(out_path.stem + "_agg.parquet")
    bdir = out_path.with_name(out_path.stem + "_agg_buckets")
    pdir = out_path.with_name(out_path.stem + "_agg_parts")
    if agg_ckpt.exists():
        print(f"  reusing ps GROUP BY checkpoint {agg_ckpt.name}; adding child_hash...", flush=True)
    else:
        agg_write = out_path.with_name(out_path.stem + "_agg.parquet.tmp")
        bdir.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        _partition_by_bucket(src_dir, "ps", pdir, threads, mem, tmp_dir)
        tasks = []
        for i in range(N_MERGE_BUCKETS):
            bout = bdir / f"bucket_{i}.parquet"
            if bout.exists():
                continue
            sql = f"""
                COPY (
                    SELECT parent_hash, move_san,
                           any_value(parent_epd) AS parent_epd,
                           any_value(ply)        AS ply,
                           SUM(white_wins)::BIGINT AS white_wins,
                           SUM(draws)::BIGINT      AS draws,
                           SUM(black_wins)::BIGINT AS black_wins,
                           SUM(total)::BIGINT      AS total
                    FROM read_parquet('{_sql_path(pdir)}/month=*/bkt={i}/*.parquet')
                    GROUP BY parent_hash, move_san
                    HAVING SUM(total) >= {int(min_games)}
                ) TO '{_sql_path(bout.with_suffix(".parquet.tmp"))}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            tasks.append((sql, str(bout.with_suffix(".parquet.tmp")), str(bout),
                          threads, mem, str(tmp_dir), f"ps bucket {i}"))
        print(f"  Phase B (ps): GROUP BY in {N_MERGE_BUCKETS} hash buckets "
              f"({len(tasks)} to build)...", flush=True)
        with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
            for fut in as_completed([ex.submit(_run_copy_query, t) for t in tasks]):
                label, size, secs = fut.result()
                print(f"    {label}: {size/1e6:.0f} MB ({secs/60:.1f} min)", flush=True)
        # Buckets are disjoint: the checkpoint is a cheap streaming concat.
        agg_write.unlink(missing_ok=True)
        con = _duck(threads, mem, tmp_dir)
        try:
            con.execute(f"""
                COPY (SELECT * FROM read_parquet('{_sql_path(bdir)}/bucket_*.parquet'))
                TO '{_sql_path(agg_write)}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        finally:
            con.close()
        agg_write.replace(agg_ckpt)
        shutil.rmtree(bdir, ignore_errors=True)
        shutil.rmtree(pdir, ignore_errors=True)  # ~E: footprint of the inputs; free before crush
        print(f"  ps GROUP BY done in {(time.time()-t0)/60:.1f} min; adding child_hash...", flush=True)

    # child_hash via python-chess (cached over unique (epd, san)) + final stamping.
    df = pl.read_parquet(agg_ckpt)
    INT64_MAX, INT64_RANGE = 2**63 - 1, 2**64
    cache: dict[tuple[str, str], int | None] = {}
    child = []
    for epd, san in zip(df["parent_epd"].to_list(), df["move_san"].to_list()):
        key = (epd, san)
        h = cache.get(key, "miss")
        if h == "miss":
            try:
                b = chess.Board(epd)
                b.push(b.parse_san(san))
                hh = chess.polyglot.zobrist_hash(b)
                h = hh - INT64_RANGE if hh > INT64_MAX else hh
            except Exception:
                h = None
            cache[key] = h
        child.append(h)
    df = df.with_columns(
        pl.Series("child_hash", child, dtype=pl.Int64),
        pl.lit(POOL_EVENT).alias("event"),
        pl.lit(POOL_ELO, dtype=pl.Int64).alias("elo_band"),
        ((pl.col("white_wins") + 0.5 * pl.col("draws")) / pl.col("total")).alias("white_score_avg"),
    )
    out_tmp = out_path.with_suffix(".parquet.tmp")
    df.write_parquet(out_tmp, compression="zstd")
    out_tmp.replace(out_path)
    agg_ckpt.unlink(missing_ok=True)
    # Repeated here for the checkpoint-resume path: a crash between the checkpoint
    # rename and the else-branch cleanup leaves ~390 GB of partition files behind,
    # which starved the crush merge of disk once already.
    shutil.rmtree(bdir, ignore_errors=True)
    shutil.rmtree(pdir, ignore_errors=True)
    print(f"  position-stats: {out_path.name} ({out_path.stat().st_size/1e6:.0f} MB, "
          f"{df.height:,} rows)", flush=True)


def merge_crush(src_dir: Path, partial_dir: Path, ps_path: Path, out_path: Path,
                threads: int, mem: str, tmp_base: Path | None = None) -> None:
    if out_path.exists():
        print(f"SKIP crush merge: {out_path.name} exists")
        return
    tmp_dir = (tmp_base or partial_dir) / "_merge_duckdb_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_tmp = out_path.with_suffix(".parquet.tmp")
    ps = _sql_path(ps_path)
    t0 = time.time()
    # Two-phase like the ps merge: Phase A partitions the monthly crush files by
    # parent_hash bucket; Phase B runs each bucket's GROUP BY + SEMI JOIN to the
    # surviving position-stats keys (collapse the tail), with the join build side
    # filtered to the same bucket.
    bdir = out_path.with_name(out_path.stem + "_buckets")
    bdir.mkdir(parents=True, exist_ok=True)
    pdir = out_path.with_name(out_path.stem + "_parts")
    _partition_by_bucket(src_dir, "crush", pdir, threads, mem, tmp_dir)
    tasks = []
    for i in range(N_MERGE_BUCKETS):
        bout = bdir / f"bucket_{i}.parquet"
        if bout.exists():
            continue
        sql = f"""
            COPY (
                SELECT c.parent_hash, c.move_san, c.move_bucket,
                       SUM(c.n)::BIGINT          AS n,
                       SUM(c.white_wins)::BIGINT AS white_wins,
                       SUM(c.black_wins)::BIGINT AS black_wins,
                       '{POOL_EVENT}' AS event, {POOL_ELO} AS elo_band
                FROM read_parquet('{_sql_path(pdir)}/month=*/bkt={i}/*.parquet') c
                SEMI JOIN (SELECT DISTINCT parent_hash, move_san FROM read_parquet('{ps}')
                           WHERE {_bucket_expr("parent_hash")} = {i}) k
                  ON c.parent_hash = k.parent_hash AND c.move_san = k.move_san
                GROUP BY c.parent_hash, c.move_san, c.move_bucket
            ) TO '{_sql_path(bout.with_suffix(".parquet.tmp"))}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        tasks.append((sql, str(bout.with_suffix(".parquet.tmp")), str(bout),
                      threads, mem, str(tmp_dir), f"crush bucket {i}"))
    print(f"  Phase B (crush): GROUP BY in {N_MERGE_BUCKETS} hash buckets "
          f"({len(tasks)} to build)...", flush=True)
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
        for fut in as_completed([ex.submit(_run_copy_query, t) for t in tasks]):
            label, size, secs = fut.result()
            print(f"    {label}: {size/1e6:.0f} MB ({secs/60:.1f} min)", flush=True)
    out_tmp.unlink(missing_ok=True)
    con = _duck(threads, mem, tmp_dir)
    try:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{_sql_path(bdir)}/bucket_*.parquet'))
            TO '{_sql_path(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    out_tmp.replace(out_path)
    shutil.rmtree(bdir, ignore_errors=True)
    shutil.rmtree(pdir, ignore_errors=True)
    print(f"  crush-rel: {out_path.name} ({out_path.stat().st_size/1e6:.0f} MB) "
          f"in {(time.time()-t0)/60:.1f} min", flush=True)


# ── orchestration ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start-year", type=int, required=True)
    ap.add_argument("--end-year", type=int, required=True)
    ap.add_argument("--months", type=int, nargs="*", default=None,
                    help="Restrict to these months (default: all present).")
    ap.add_argument("--min-elo", type=int, default=1800)
    ap.add_argument("--events", nargs="+", default=["Blitz", "Rapid", "Classical"])
    ap.add_argument("--min-games", type=int, default=50)
    ap.add_argument("--max-ply", type=int, default=30)
    ap.add_argument("--workers", type=int, default=11)
    ap.add_argument("--threads", type=int, default=8, help="DuckDB threads for the merge phase.")
    ap.add_argument("--mem", default="48GB", help="DuckDB memory_limit for the merge phase.")
    ap.add_argument("--phase", choices=["extract", "merge", "all"], default="all")
    ap.add_argument("--limit-games", type=int, default=None, help="Per-file game cap (benchmarking).")
    ap.add_argument("--partial-dir", default=None, help="Override the partials directory.")
    ap.add_argument("--tmp-dir", default=None,
                    help="Override the DuckDB merge-phase temp/spill directory "
                         "(default: <partial-dir>/_merge_duckdb_tmp). Point this at a "
                         "drive with ample free space if the default drive is tight.")
    ap.add_argument("--tag", default=None, help="Override the output filename tag.")
    # ── asymmetric-depth prune (seeded from existing >=1800 frequency stats) ──
    ap.add_argument("--no-prune", action="store_true",
                    help="Disable asymmetric-depth pruning (extract every line to --max-ply).")
    ap.add_argument("--seed-stats",
                    default=str(STATS_DIR / "position_stats_pooled_1900_2200_brc.parquet"),
                    help="Existing position_stats used to seed first-move/response frequencies.")
    ap.add_argument("--main-moves", nargs="+", default=["e4", "d4", "c4", "Nf3"],
                    help="White first moves kept at full --deep-ply depth.")
    ap.add_argument("--deep-thresh", type=float, default=0.01,
                    help="Freq cutoff (>=) for deep tier: P(w1) for offbeat first moves, "
                         "P(b1|w1) for replies to main moves. Default 1/100.")
    ap.add_argument("--mid-thresh", type=float, default=0.0025,
                    help="Freq cutoff (>=) for mid tier. Default 1/400.")
    ap.add_argument("--deep-ply", type=int, default=30)
    ap.add_argument("--mid-ply", type=int, default=18)
    ap.add_argument("--shallow-ply", type=int, default=10)
    args = ap.parse_args()

    ev_tag = "".join(e[0].lower() for e in args.events)  # brc
    tag = args.tag or f"ge{args.min_elo}_{args.start_year}_{args.end_year}_{ev_tag}"
    partial_dir = Path(args.partial_dir) if args.partial_dir else \
        STATS_DIR / f"_pooled_partials_{tag}"
    ps_out = STATS_DIR / f"position_stats_pooled_{tag}.parquet"
    crush_out = STATS_DIR / f"crush_hist_rel_pooled_{tag}.parquet"

    print(f"Target: events={args.events} mean_elo>={args.min_elo} "
          f"years {args.start_year}-{args.end_year} months={args.months or 'all'}")
    print(f"Partials: {partial_dir}")
    print(f"Outputs:  {ps_out.name} | {crush_out.name}", flush=True)

    # Build the asymmetric-depth tier tables (unless disabled).
    tiers = None
    if not args.no_prune:
        seed = Path(args.seed_stats)
        if not seed.exists():
            sys.exit(f"FATAL: --seed-stats not found: {seed} (use --no-prune to skip pruning)")
        tiers = build_depth_tiers(seed, args.main_moves, args.deep_thresh, args.mid_thresh,
                                  args.deep_ply, args.mid_ply, args.shallow_ply)
        print(f"\nDepth prune (seed={seed.name}):")
        print(f"  {args.deep_ply}-ply: main {sorted(tiers['four'])} + replies P(b1|w1)>="
              f"{args.deep_thresh}")
        for w1 in args.main_moves:
            print(f"      vs {w1}: {sorted(tiers['deep_resp'].get(w1, []))}")
        print(f"  {args.mid_ply}-ply: offbeat first moves {sorted(tiers['common_white_other'])} "
              f"+ mid replies " + ", ".join(
                  f"{w1}:{sorted(tiers['mid_resp'].get(w1, []))}" for w1 in args.main_moves
                  if tiers['mid_resp'].get(w1)))
        print(f"  {args.shallow_ply}-ply: everything else\n", flush=True)
    else:
        print("\nDepth prune: DISABLED (full depth)\n", flush=True)

    if args.phase in ("extract", "all"):
        files = discover_source_files(args.start_year, args.end_year, args.months, args.events)
        partial_dir.mkdir(parents=True, exist_ok=True)
        tasks = []
        for f, y, m, ev in files:
            ps_p = partial_dir / partial_name(f, y, m, ev, "ps")
            cr_p = partial_dir / partial_name(f, y, m, ev, "crush")
            if ps_p.exists() and cr_p.exists():
                continue
            tasks.append((str(f), str(ps_p), str(cr_p), args.limit_games))
        print(f"Extract: {len(files)} source files, {len(tasks)} to process, "
              f"{args.workers} workers\n", flush=True)
        t0 = time.time()
        done = tot_games = tot_kept = 0
        if tasks:
            with ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker,
                                     initargs=(args.min_elo, args.max_ply, tiers)) as ex:
                futs = [ex.submit(_worker, t) for t in tasks]
                for fut in as_completed(futs):
                    r = fut.result()
                    done += 1
                    tot_games += r.get("games", 0)
                    tot_kept += r.get("kept", 0)
                    if done % 10 == 0 or done == len(tasks):
                        el = time.time() - t0
                        rate = tot_games / el if el else 0
                        print(f"  [{done}/{len(tasks)}] {r['file']}: "
                              f"{r.get('kept',0):,}/{r.get('games',0):,} kept, "
                              f"ps={r.get('ps_rows',0):,} crush={r.get('crush_rows',0):,} "
                              f"({r['sec']:.0f}s) | {rate:,.0f} games/s agg | "
                              f"{el/60:.1f} min", flush=True)
        el = time.time() - t0
        print(f"\nExtract done: {tot_kept:,}/{tot_games:,} games kept in {el/60:.1f} min "
              f"({tot_games/el if el else 0:,.0f} games/s)\n", flush=True)

    if args.phase in ("merge", "all"):
        tmp_base = Path(args.tmp_dir) if args.tmp_dir else None
        # Stage 1: SUM-only per-month consolidation (NO min_games filter).
        print("Consolidating per-month (sum only, no filter)...", flush=True)
        monthly_dir = consolidate_monthly(partial_dir, args.threads, args.mem, tmp_base)
        # Stage 2: final global merge — min_games applied here ONCE, over all months.
        print("Final merge: position-stats (min_games applied here)...", flush=True)
        merge_position_stats(monthly_dir, partial_dir, ps_out, args.min_games,
                             args.threads, args.mem, tmp_base)
        print("Final merge: crush histogram...", flush=True)
        merge_crush(monthly_dir, partial_dir, ps_out, crush_out, args.threads, args.mem,
                   tmp_base)
        shutil.rmtree((tmp_base or partial_dir) / "_merge_duckdb_tmp", ignore_errors=True)
        print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
