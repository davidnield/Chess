"""
benchmark_fullmonth_ingest.py — Phase 3 of the 2026-07 parquet-recipe benchmark.

Phases 0-2 (benchmark_compression_matrix.py) screened codec/level/page/enc/sort/
movetext on small in-RAM subsets and landed on an emerging recipe:

    zstd-19 · 1024 KB pages · E0 default encodings · M1 (strip %clk/%eval comments)
    · sort (S2 eco/opening/movetext  OR  S3 mean_elo//200/eco/movetext)

Phase 3 validates that recipe at FULL-MONTH scale, where two things Phases 0-2
could not test finally bite:

  1. Bigger-than-RAM streaming — m2025_03 is 66 GB raw / ~40 GB transformed, so the
     writer must stream, never hold the month in memory. This script clones the
     PRODUCTION streaming path from process_pgn_parquets.py (RecordBatchReader ->
     write_dataset) and measures the two things that gate the recipe:
       Gate 1  sustained write throughput >= 7 MB/s raw (2x margin on the 1-month
               re-compression floor).  zstd-19 is heavy — this is the real risk.
       Gate 2  warm full-scan read >= 0.25x the raw-scan baseline.
  2. Which sort strategy pays, and by how much, on a REAL consumer:
       strategy A  per-file sort  — buffer <=2 M rows, sort, write; bounded memory.
       strategy B  global sort    — transform to a temp dataset, then DuckDB
                                    ORDER BY streamed via fetch_record_batch into a
                                    pyarrow writer (keeps codec/page/RG control).
                                    Adopt B only if it buys >= 2 % size over A.
     S3 (elo-banded) also gives row-group elo-locality; the consumer-read timing
     below measures whether that yields real mean_elo>=1800 pushdown, or whether
     replay/scan CPU swamps it (in which case S2's slightly-better ratio wins).

COLD-CACHE CHECK WITHOUT A REBOOT.  Every read in Phases 0-2 was warm (the file was
still in the OS standby list from the write that produced it), which inflates read
MB/s. Phase 3 gets a genuine COLD read for free, no reboot and no RAMMap, by
exploiting the run's own I/O volume:

    * the cold-check month (m2019_06, ~20 GB raw) is recompressed FIRST, at the top
      of the run;
    * then the whole main-month matrix runs — writing + rescanning m2025_03 several
      times pushes *far more than physical RAM* (128 GB) through the file cache,
      which evicts the cold-check month from the standby list;
    * the cold-check month is read LAST. Because it was flushed out in between, that
      read comes off disk cold. A warm re-read immediately after quantifies the
      cold/warm gap.

  The eviction is guaranteed: one m2025_03 recompress + rescan alone moves > 128 GB,
  and the matrix does several. The script prints the intervening cache-churn volume
  so the cold read's validity is auditable.

Datasets are the same staged raw HuggingFace months under E:/bench/src/<id>/ used by
Phases 0-2 (F: is a slow USB disk, never read here). Recompressed outputs land under
E:/bench/out_p3/<config>/ and are deleted once measured (the CSV is the record).
Results append to E:/bench/parquet_recipe_202607/fullmonth_results.csv (resume-by-key).

Usage:
    # Dry run — print the ordered plan (writes/reads/cold check) and exit:
    .venv/Scripts/python.exe python/benchmark_fullmonth_ingest.py --dry-run

    # Small correctness smoke on the 1.8 GB staged month (minutes, validates every
    # strategy + the cold flow without touching the 66 GB month):
    .venv/Scripts/python.exe python/benchmark_fullmonth_ingest.py --smoke

    # The real Phase-3 run (full 66 GB month + cold check). Run SOLO — no other
    # heavy job (CLAUDE.md memory-contention rule) and for clean throughput gates:
    .venv/Scripts/python.exe python/benchmark_fullmonth_ingest.py

    # Also run the ACTUAL build_pooled_stats extract (replay CPU + real pushdown)
    # against each recompressed month, via a throwaway D: year=1900 partition:
    .venv/Scripts/python.exe python/benchmark_fullmonth_ingest.py --real-extract

    # Subset the write matrix by config name substring:
    .venv/Scripts/python.exe python/benchmark_fullmonth_ingest.py --only A_S3
"""
from __future__ import annotations

import argparse
import csv
import gc
import json
import shutil
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import polars as pl
import psutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from process_pgn_parquets import (  # production transform + shape — measured verbatim
    EVENT_MAP, MAX_ROWS_PER_FILE, PARTITION_SCHEMA, ROW_GROUP_SIZE,
    SCANNER_BATCH_SIZE, WRITE_SCHEMA, transform,
)
from benchmark_compression_matrix import (  # Phase 0-2 helpers — one source of truth
    DATASETS, PROJECTIONS, SRC_ROOT, sorted_table, strip_movetext, write_options,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Windows PyArrow allocator degradation (CLAUDE.md): a tight loop pulling hundreds
# of batches out of a multi-GB scan eventually corrupts/exhausts the native
# allocator and the process dies with no traceback (observed here as a hard
# STATUS_STACK_OVERFLOW on the very first 70 GB raw-baseline scan). Every batch
# loop below calls this every GC_EVERY iterations to release the arena before
# it degrades.
GC_EVERY = 25


def _pace(i: int) -> None:
    if i % GC_EVERY == 0:
        gc.collect()
        pa.default_memory_pool().release_unused()


OUT_ROOT   = Path("E:/bench/out_p3")
TMP_ROOT   = Path("E:/bench/_p3tmp")        # strategy-B transform-before-sort scratch
DUCK_TMP   = Path("E:/bench/duck_tmp")      # DuckDB external-sort spill (fast NVMe)
RESULT_DIR = Path("E:/bench/parquet_recipe_202607")
RESULTS    = RESULT_DIR / "fullmonth_results.csv"

# Throwaway D: partition the optional --real-extract path routes the recompressed
# month through, so the hardcoded-source production consumer reads it unmodified.
PROD_ROOT  = Path("D:/data/chess/standard-chess-games-compressed")
FAKE_YEAR  = 1900

DATA_COLUMNS = [n for n in WRITE_SCHEMA.names if n not in ("year", "month", "event")]
DUCK_MEM   = "48GB"
READ_REPS  = 3

# ── the write matrix ─────────────────────────────────────────────────────────
# control = current prod recipe (the improvement + consumer-speed denominator).
# A_* / B_* = the emerging recipe under the two sort-at-scale strategies; S2 vs S3
# resolves whether elo-pushdown or pure clustering ratio wins.

CONFIGS = [
    dict(name="control", codec="zstd", level=6,  page_kb=1024, m1=False, sort="S0", strategy="stream"),
    dict(name="A_S3",    codec="zstd", level=19, page_kb=1024, m1=True,  sort="S3", strategy="perfile"),
    dict(name="A_S2",    codec="zstd", level=19, page_kb=1024, m1=True,  sort="S2", strategy="perfile"),
    dict(name="B_S3",    codec="zstd", level=19, page_kb=1024, m1=True,  sort="S3", strategy="duckdb"),
]
MAIN_MONTH = "m2025_03"   # 66 GB — the bigger-than-RAM validation + consumer target
COLD_MONTH = "m2019_06"   # ~20 GB — written first, read cold last (eviction trick)
COLD_CONFIG = "A_S3"      # cold check uses the leading recipe candidate
SMOKE_MONTH = "m2016_01"  # 1.8 GB — correctness smoke for every strategy


# ── peak-RSS sampler ─────────────────────────────────────────────────────────

class PeakRSS:
    """Sample this process's RSS in a background thread; expose the peak (GB).
    All writers here run in-process (pyarrow threads / in-process DuckDB), so the
    main-process RSS is the whole footprint."""

    def __init__(self, interval: float = 0.25):
        self.interval = interval
        self.peak = 0
        self._stop = threading.Event()
        self._proc = psutil.Process()

    def _run(self):
        while not self._stop.is_set():
            self.peak = max(self.peak, self._proc.memory_info().rss)
            self._stop.wait(self.interval)

    def __enter__(self):
        self.peak = self._proc.memory_info().rss
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join(timeout=2)


# ── write-option builders (row_group_size handled per writer kind) ───────────

def pq_kwargs(c: dict) -> dict:
    """pyarrow.parquet.write_table / ParquetWriter kwargs for config c (includes
    row_group_size — valid for write_table; popped by the ParquetWriter path)."""
    return write_options({"codec": c["codec"], "level": c["level"], "rg": ROW_GROUP_SIZE,
                          "page_kb": c["page_kb"], "enc": "E0"},
                         sorted_by_movetext=c["sort"] in ("S2", "S3"))


def dataset_write_options(c: dict):
    """pads write_dataset file_options (no row_group_size — write_dataset uses
    max_rows_per_group)."""
    kw = pq_kwargs(c)
    kw.pop("row_group_size", None)
    return pads.ParquetFileFormat().make_write_options(**kw)


# ── the transformed-batch stream (production transform, optional M1 strip) ────

def stream_tables(ds_id: str, m1: bool):
    """Yield WRITE_SCHEMA arrow tables (with event col) straight from staged raw
    files — the exact production transform, then optional M1 comment strip. Constant
    memory: one scan batch at a time."""
    src = SRC_ROOT / ds_id
    files = sorted(str(p) for p in src.glob("*.parquet"))
    if not files:
        raise SystemExit(f"FATAL: no staged files under {src}")
    year, month = DATASETS[ds_id]
    scanner = pads.dataset(files, format="parquet").scanner(
        filter=pc.field("Event").isin(list(EVENT_MAP.keys())), batch_size=SCANNER_BATCH_SIZE)
    # NB: do NOT _pace() here. write_stream drives this generator through a
    # RecordBatchReader consumed by write_dataset's C++ (threaded) writer, which
    # holds buffered batches referencing the arrow memory pool. Calling
    # release_unused()/gc.collect() mid-pull frees in-flight memory and crashes the
    # writer (STATUS_STACK_OVERFLOW). Per-step process isolation (orchestrate) is
    # the write path's degradation defense; production writes 70 GB months through
    # this same write_dataset with no pacing. Pacing stays only in the read loops.
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        tbl = transform(pl.from_arrow(batch), year, month).to_arrow().cast(WRITE_SCHEMA)
        if m1:
            # strip_movetext returns large_string movetext; cast back so every yielded
            # table shares one schema (BYTE_ARRAY on disk is identical either way).
            tbl = strip_movetext(tbl).cast(WRITE_SCHEMA)
        yield tbl


# ── the three write strategies ───────────────────────────────────────────────

def _coalesced_batches(ds_id: str, m1: bool, target_rows: int = 256_000):
    """Coalesce stream_tables' many tiny scan-batch tables into a few large record
    batches. The staged raw HuggingFace files have ~1000-row row groups, so the
    filtered scanner yields THOUSANDS of ~750-row batches. Feeding write_dataset
    that many micro-batches drives deep Acero threaded-task nesting that overflows a
    worker-thread stack on Windows (hard STATUS_STACK_OVERFLOW, no traceback) —
    reproducibly so when the process is a subprocess-spawned child, which is how the
    orchestrator runs each step. Coalescing to ~target_rows-row batches removes both
    the crash and a large throughput tax. Memory stays bounded (one target_rows
    table in flight) regardless of month size."""
    buf: list = []
    n = 0
    for tbl in stream_tables(ds_id, m1):
        buf.append(tbl)
        n += tbl.num_rows
        if n >= target_rows:
            combined = pa.concat_tables(buf).combine_chunks()
            yield from combined.to_batches(max_chunksize=target_rows)
            buf, n = [], 0
    if buf:
        combined = pa.concat_tables(buf).combine_chunks()
        yield from combined.to_batches(max_chunksize=target_rows)


def write_stream(ds_id: str, c: dict, out_dir: Path) -> int:
    """S0 streaming write — the production write_dataset path (control recipe)."""
    reader = pa.RecordBatchReader.from_batches(
        WRITE_SCHEMA, _coalesced_batches(ds_id, c["m1"]))
    pads.write_dataset(
        reader, base_dir=str(out_dir), format="parquet",
        partitioning=pads.partitioning(PARTITION_SCHEMA, flavor="hive"),
        max_rows_per_file=MAX_ROWS_PER_FILE, max_rows_per_group=ROW_GROUP_SIZE,
        file_options=dataset_write_options(c), existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet", use_threads=True)
    return sum(1 for _ in out_dir.rglob("*.parquet"))


def write_perfile(ds_id: str, c: dict, out_dir: Path) -> int:
    """Strategy A — per-event buffer, flush a sorted <=2 M-row file whenever the
    buffer fills. Bounded memory (~one output file per event in flight)."""
    kw = pq_kwargs(c)
    buf: dict[str, list] = defaultdict(list)
    rows: dict[str, int] = defaultdict(int)
    fidx: dict[str, int] = defaultdict(int)
    n_written = 0

    def flush(ev: str, whole: bool):
        nonlocal n_written
        if not buf[ev]:
            return
        merged = pa.concat_tables(buf[ev]).combine_chunks()
        while merged.num_rows >= MAX_ROWS_PER_FILE or (whole and merged.num_rows > 0):
            take = min(MAX_ROWS_PER_FILE, merged.num_rows)
            piece = sorted_table(merged.slice(0, take), c["sort"])
            ev_dir = out_dir / f"event={ev}"
            ev_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(piece, ev_dir / f"part-{fidx[ev]}.parquet", **kw)
            fidx[ev] += 1
            n_written += take
            merged = merged.slice(take)
            if not whole and merged.num_rows < MAX_ROWS_PER_FILE:
                break
        buf[ev] = [merged] if merged.num_rows else []
        rows[ev] = merged.num_rows

    for tbl in stream_tables(ds_id, c["m1"]):
        for ev in tbl["event"].unique().to_pylist():
            sub = tbl.filter(pc.equal(tbl["event"], ev)).drop_columns(["event"])
            buf[ev].append(sub)
            rows[ev] += sub.num_rows
            if rows[ev] >= MAX_ROWS_PER_FILE:
                flush(ev, whole=False)
    for ev in list(buf):
        flush(ev, whole=True)
    return n_written


def write_duckdb(ds_id: str, c: dict, out_dir: Path) -> int:
    """Strategy B — transform to a temp per-event dataset, then DuckDB ORDER BY
    streamed through a pyarrow writer (keeps codec/page/RG control; DuckDB does the
    external sort with spill to fast NVMe)."""
    tmp = TMP_ROOT / c["name"]
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    # Pass 1: stream transform -> light temp dataset (cheap zstd-1, no sort).
    # Coalesced batches (see _coalesced_batches) — the raw micro-batches crash the
    # spawned-child write_dataset just as in write_stream.
    tcfg = dict(c, codec="zstd", level=1, page_kb=1024)
    reader = pa.RecordBatchReader.from_batches(
        WRITE_SCHEMA, _coalesced_batches(ds_id, c["m1"]))
    pads.write_dataset(
        reader, base_dir=str(tmp), format="parquet",
        partitioning=pads.partitioning(PARTITION_SCHEMA, flavor="hive"),
        max_rows_per_file=5_000_000, max_rows_per_group=ROW_GROUP_SIZE,
        file_options=dataset_write_options(tcfg), existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet", use_threads=True)

    order = ({"S2": "eco, opening, movetext",
              "S3": "CAST(floor(mean_elo/200.0)*200 AS SMALLINT), eco, movetext"}[c["sort"]])
    kw = pq_kwargs(c)
    rg = kw.pop("row_group_size")
    cols = ", ".join(f'"{n}"' for n in DATA_COLUMNS)

    DUCK_TMP.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")

    n_written = 0
    for ev_dir in sorted((tmp / f"year={DATASETS[ds_id][0]}" / f"month={DATASETS[ds_id][1]}").glob("event=*")):
        ev = ev_dir.name.split("=", 1)[1]
        glob = (ev_dir / "*.parquet").as_posix()
        rel = con.execute(f"SELECT {cols} FROM read_parquet('{glob}') ORDER BY {order}")
        rb = rel.fetch_record_batch(ROW_GROUP_SIZE)
        oev = out_dir / f"event={ev}"
        oev.mkdir(parents=True, exist_ok=True)
        writer = None
        fidx = rows_in_file = 0
        pending: list = []
        pending_rows = 0

        def roll_group(force_close: bool):
            nonlocal writer, fidx, rows_in_file, pending, pending_rows
            if not pending:
                return
            grp = pa.Table.from_batches(pending)
            pending = []
            pending_rows = 0
            if writer is not None and rows_in_file + grp.num_rows > MAX_ROWS_PER_FILE:
                writer.close()
                writer = None
            if writer is None:
                writer = pq.ParquetWriter(oev / f"part-{fidx}.parquet", grp.schema, **kw)
                fidx += 1
                rows_in_file = 0
            writer.write_table(grp, row_group_size=rg)
            rows_in_file += grp.num_rows

        for batch in rb:
            if batch.num_rows == 0:
                continue
            pending.append(batch)
            pending_rows += batch.num_rows
            n_written += batch.num_rows
            if pending_rows >= rg:
                roll_group(False)
        roll_group(True)
        if writer is not None:
            writer.close()
    con.close()
    shutil.rmtree(tmp, ignore_errors=True)
    return n_written


STRATEGIES = {"stream": write_stream, "perfile": write_perfile, "duckdb": write_duckdb}


# ── size / metadata accounting + consumer-shaped reads ───────────────────────

def measure_size(out_dir: Path) -> dict:
    files = list(out_dir.rglob("*.parquet"))
    size = sum(f.stat().st_size for f in files)
    n_rg = skippable = total_rows = 0
    for f in files:
        md = pq.ParquetFile(f).metadata
        n_rg += md.num_row_groups
        total_rows += md.num_rows
        elo_i = next(i for i in range(md.num_columns) if md.schema.column(i).name == "mean_elo")
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            st = rg.column(elo_i).statistics
            if st is not None and st.has_min_max and st.max < 1800:
                skippable += rg.num_rows
    return {"size_bytes": size, "n_row_groups": n_rg, "rows": total_rows,
            "skippable_row_frac": round(skippable / total_rows, 4) if total_rows else 0.0}


def timed_scan(out_dir: Path, cols=None, elo_filter=False) -> tuple[float, float]:
    """Median wall + decoded-MB/s over READ_REPS warm scans, streamed in batches like
    the real consumers (never a single to_table() that would spike RAM on 40 GB)."""
    ds = pads.dataset(str(out_dir), format="parquet", partitioning="hive")
    filt = (pc.field("mean_elo") >= 1800) if elo_filter else None
    times, nbytes = [], 0
    for _ in range(READ_REPS):
        t = time.time()
        n = 0
        for i, batch in enumerate(
                ds.scanner(columns=cols, filter=filt, batch_size=SCANNER_BATCH_SIZE).to_batches(), 1):
            _pace(i)
            n += batch.nbytes
        times.append(time.time() - t)
        nbytes = n
    med = statistics.median(times)
    return round(med, 2), round(nbytes / 1e6 / med, 0) if med else 0.0


def cold_scan(out_dir: Path) -> float:
    """ONE cold pass (no repeats — the first read is the cold one)."""
    ds = pads.dataset(str(out_dir), format="parquet", partitioning="hive")
    t = time.time()
    for i, _ in enumerate(ds.scanner(batch_size=SCANNER_BATCH_SIZE).to_batches(), 1):
        _pace(i)
    return round(time.time() - t, 2)


# ── optional real production extract (replay CPU + real pushdown) ─────────────

def real_extract(out_dir: Path, ds_id: str, workers: int) -> float:
    """Route the recompressed month through a throwaway D: year=1900 partition and
    run the ACTUAL build_pooled_stats extract against it, unmodified. Returns wall
    seconds. Cleans up the fake partition and its partials afterward."""
    _, month = DATASETS[ds_id]
    fake = PROD_ROOT / f"year={FAKE_YEAR}" / f"month={month}"
    if fake.exists():
        shutil.rmtree(fake)
    fake.mkdir(parents=True)
    # out_dir's event=* dirs sit at different depths depending on strategy: the
    # control (write_stream) writer partitions by the full year=/month=/event=
    # schema, while A/B (write_perfile/write_duckdb) write event=-only. A plain
    # copytree would nest control's tree an extra year=/month= level under fake
    # and build_pooled_stats.py wouldn't find it — so walk to the event=* dirs
    # themselves and flatten them directly under fake, regardless of depth.
    for ev_dir in out_dir.rglob("event=*"):
        if ev_dir.is_dir():
            shutil.copytree(ev_dir, fake / ev_dir.name, dirs_exist_ok=True)
    partials = RESULT_DIR / f"_p3_extract_partials_{ds_id}"
    if partials.exists():
        shutil.rmtree(partials)
    cmd = [sys.executable, str(Path(__file__).parent / "build_pooled_stats.py"),
           "--start-year", str(FAKE_YEAR), "--end-year", str(FAKE_YEAR),
           "--months", str(month), "--phase", "extract", "--no-prune",
           "--workers", str(workers), "--partial-dir", str(partials)]
    t = time.time()
    rc = subprocess.run(cmd).returncode
    el = round(time.time() - t, 1)
    shutil.rmtree(fake.parent, ignore_errors=True)
    shutil.rmtree(partials, ignore_errors=True)
    if rc != 0:
        print(f"    !! real-extract rc={rc}", flush=True)
    return el


# ── raw-scan baseline (the 1/4-gate denominator) ─────────────────────────────

def raw_baseline(ds_id: str) -> dict:
    src = SRC_ROOT / ds_id
    files = sorted(str(p) for p in src.glob("*.parquet"))
    raw_bytes = sum(Path(f).stat().st_size for f in files)
    ds = pads.dataset(files, format="parquet")
    t = time.time()
    nbytes = 0
    for i, b in enumerate(ds.scanner(batch_size=SCANNER_BATCH_SIZE).to_batches(), 1):
        _pace(i)
        nbytes += b.nbytes
    secs = time.time() - t
    return {"raw_bytes": raw_bytes, "uncompressed_bytes": nbytes,
            "scan_s": round(secs, 1), "scan_mb_s": round(nbytes / 1e6 / secs, 0)}


# ── results IO ───────────────────────────────────────────────────────────────

def existing_keys() -> set[str]:
    if not RESULTS.exists():
        return set()
    with open(RESULTS, newline="", encoding="utf-8") as f:
        return {r["key"] for r in csv.DictReader(f)}


def append_row(row: dict) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    new = not RESULTS.exists()
    with open(RESULTS, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)


# ── one config on one month: write (instrumented) + read timings ─────────────

def run_write(ds_id: str, c: dict, raw_bytes: int, real_extract_workers: int | None) -> dict:
    out_dir = OUT_ROOT / f"{ds_id}__{c['name']}"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    print(f"  [{ds_id}/{c['name']}] {c['strategy']} "
          f"{c['codec']}-{c['level']} {c['sort']} M{'1' if c['m1'] else '0'} — writing…",
          flush=True)
    t0 = time.time()
    with PeakRSS() as rss:
        n = STRATEGIES[c["strategy"]](ds_id, c, out_dir)
    write_s = time.time() - t0
    meta = measure_size(out_dir)
    raw_mb_s = round(raw_bytes / 1e6 / write_s, 2)

    rfull_s, rfull_mb = timed_scan(out_dir)
    rpool_s, rpool_mb = timed_scan(out_dir, cols=PROJECTIONS["pooled"])
    rflt_s, rflt_mb = timed_scan(out_dir, cols=PROJECTIONS["pooled"], elo_filter=True)

    row = {"key": f"{ds_id}|{c['name']}", "dataset": ds_id, "config": c["name"],
           "strategy": c["strategy"], "codec": c["codec"], "level": c["level"],
           "page_kb": c["page_kb"], "sort": c["sort"], "m1": int(c["m1"]),
           "rows": n, "write_s": round(write_s, 1), "write_raw_mb_s": raw_mb_s,
           "peak_rss_gb": round(rss.peak / 1e9, 2),
           "size_bytes": meta["size_bytes"],
           "bytes_per_game": round(meta["size_bytes"] / n, 1) if n else 0,
           "n_row_groups": meta["n_row_groups"],
           "skippable_row_frac": meta["skippable_row_frac"],
           "read_full_s": rfull_s, "read_full_mb_s": rfull_mb,
           "read_pooled_s": rpool_s, "read_pooled_mb_s": rpool_mb,
           "read_pooled_flt_s": rflt_s, "read_pooled_flt_mb_s": rflt_mb,
           "extract_s": ""}
    if real_extract_workers is not None:
        row["extract_s"] = real_extract(out_dir, ds_id, real_extract_workers)

    print(f"     write {write_s/60:.1f} min = {raw_mb_s} MB/s raw | peak RSS "
          f"{row['peak_rss_gb']} GB | size {meta['size_bytes']/1e9:.2f} GB "
          f"({row['bytes_per_game']} b/g) | full-read {rfull_s}s ({rfull_mb} MB/s) "
          f"| pooled+elo {rflt_s}s (skip {meta['skippable_row_frac']:.0%})"
          + (f" | extract {row['extract_s']}s" if row['extract_s'] != "" else ""),
          flush=True)
    return row, out_dir


# ── the ordered Phase-3 sequence ─────────────────────────────────────────────

def plan(configs: list[dict], main_month: str, cold_month: str) -> list[str]:
    steps = [f"0. raw-scan baseline for {main_month} (the 1/4 read-gate denominator)"]
    steps.append(f"1. COLD-CHECK WRITE: recompress {cold_month} with '{COLD_CONFIG}' "
                 f"(written now, read cold at the very end)")
    for i, c in enumerate(configs, start=2):
        steps.append(f"{i}. {main_month} / {c['name']}: {c['strategy']} write "
                     f"(zstd-{c['level']} {c['sort']} M{'1' if c['m1'] else '0'}) + "
                     f"consumer-read timings")
    steps.append(f"{len(configs)+2}. COLD READ: scan the {cold_month} output written in "
                 f"step 1 — now evicted by the intervening >128 GB of I/O — then a warm "
                 f"re-read to quantify the cold/warm gap")
    return steps


# ── per-step execution (each heavy step runs in its OWN fresh process) ────────
#
# The whole ordered sequence used to run in ONE long-lived process. On this
# Windows box that is the documented death trap (CLAUDE.md: long-lived processes
# doing repeated multi-GB alloc/free cycles degrade or die with no traceback —
# observed here as a hard STATUS_STACK_OVERFLOW deep into the run, after a 70 GB
# baseline read + a full cold-write had already churned the allocator). Production
# (process_pgn_parquets.py) never hits this because it isolates each month in its
# OWN ProcessPoolExecutor worker — one month = one task = a fresh process with a
# clean allocator. We mirror that: the orchestrator spawns each heavy step
# (baseline, cold-write, every config write, cold-read) as a fresh `--step`
# subprocess, so allocator / stack / working-set fully reset between steps. Every
# step is skip-gated (baseline JSON / cold-write sentinel / CSV key), so a crash
# resumes without redoing completed work.

def _baseline_json(month: str) -> Path:
    return RESULT_DIR / f"p3_baseline_{month}.json"


def _cold_dir(cold_month: str) -> Path:
    cold_cfg = next(c for c in CONFIGS if c["name"] == COLD_CONFIG)
    return OUT_ROOT / f"{cold_month}__cold_{cold_cfg['name']}"


def _cold_sentinel(cold_month: str) -> Path:
    # Marks a COMPLETE cold-write, so a partial dir left by a crash isn't skipped.
    return _cold_dir(cold_month) / ".cold_done"


def step_baseline(main_month: str) -> None:
    jp = _baseline_json(main_month)
    if jp.exists():
        print(f"[baseline] {jp.name} exists — skipping.", flush=True)
        return
    print(f"=== Phase 3 baseline === {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    base = raw_baseline(main_month)
    print(f"raw-scan baseline {main_month}: {base['scan_mb_s']} MB/s uncompressed "
          f"(Gate-2 floor = {base['scan_mb_s']*0.25:.0f} MB/s); raw {base['raw_bytes']/1e9:.1f} GB",
          flush=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    jp.write_text(json.dumps({"dataset": main_month, **base}, indent=2), encoding="utf-8")


def step_cold_write(cold_month: str) -> None:
    cold_dir = _cold_dir(cold_month)
    if _cold_sentinel(cold_month).exists():
        print(f"[cold-write] {cold_dir.name} already complete — skipping.", flush=True)
        return
    cold_cfg = next(c for c in CONFIGS if c["name"] == COLD_CONFIG)
    if cold_dir.exists():
        shutil.rmtree(cold_dir)          # drop any partial from an earlier crash
    cold_dir.mkdir(parents=True)
    print(f"[cold-check] writing {cold_month} with {COLD_CONFIG} (to be read cold last)…", flush=True)
    tW = time.time()
    STRATEGIES[cold_cfg["strategy"]](cold_month, cold_cfg, cold_dir)
    cw = measure_size(cold_dir)
    _cold_sentinel(cold_month).write_text(json.dumps(cw), encoding="utf-8")
    print(f"    cold-check month written: {cw['size_bytes']/1e9:.2f} GB "
          f"in {(time.time()-tW)/60:.1f} min", flush=True)


def step_run(main_month: str, configs: list[dict], rx: int | None) -> None:
    jp = _baseline_json(main_month)
    if not jp.exists():
        sys.exit(f"FATAL: baseline {jp} missing — run --step baseline first.")
    raw_bytes = json.loads(jp.read_text(encoding="utf-8"))["raw_bytes"]
    done = existing_keys()
    for c in configs:
        key = f"{main_month}|{c['name']}"
        if key in done:
            print(f"[{key}] already in results — skipping.", flush=True)
            continue
        row, out_dir = run_write(main_month, c, raw_bytes, rx)
        append_row(row)
        shutil.rmtree(out_dir, ignore_errors=True)


def step_cold_read(cold_month: str, main_month: str) -> None:
    key = f"{cold_month}|COLD"
    if key in existing_keys():
        print(f"[{key}] already in results — skipping.", flush=True)
        return
    cold_dir = _cold_dir(cold_month)
    sentinel = _cold_sentinel(cold_month)
    if not sentinel.exists():
        sys.exit(f"FATAL: cold-write sentinel {sentinel} missing — run --step cold-write first.")
    cold_written = json.loads(sentinel.read_text(encoding="utf-8"))
    cold_cfg = next(c for c in CONFIGS if c["name"] == COLD_CONFIG)

    # Reconstruct the intervening-churn diagnostic from the CSV (the config writes
    # + read passes done between cold-write and now, which is what evicted the cold
    # month from the OS file cache). Best-effort: it only annotates whether enough
    # I/O moved to trust the cold read.
    churn = json.loads(_baseline_json(main_month).read_text(encoding="utf-8"))["uncompressed_bytes"]
    if RESULTS.exists():
        with open(RESULTS, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["dataset"] == main_month and r.get("size_bytes"):
                    churn += int(r["size_bytes"]) * 10
    ram = psutil.virtual_memory().total
    print(f"[cold-check] intervening cache churn ~{churn/1e9:.0f} GB vs "
          f"{ram/1e9:.0f} GB RAM — the cold month is evicted; reading it cold…", flush=True)
    cold_s = cold_scan(cold_dir)
    warm_s, warm_mb = timed_scan(cold_dir)
    cold_mb = round(cold_written["size_bytes"] / 1e6 / cold_s, 0) if cold_s else 0
    append_row({"key": key, "dataset": cold_month, "config": "cold_check",
                "strategy": cold_cfg["strategy"], "codec": cold_cfg["codec"],
                "level": cold_cfg["level"], "page_kb": cold_cfg["page_kb"],
                "sort": cold_cfg["sort"], "m1": int(cold_cfg["m1"]),
                "rows": cold_written["rows"], "write_s": "", "write_raw_mb_s": "",
                "peak_rss_gb": "", "size_bytes": cold_written["size_bytes"],
                "bytes_per_game": "", "n_row_groups": cold_written["n_row_groups"],
                "skippable_row_frac": cold_written["skippable_row_frac"],
                "read_full_s": cold_s, "read_full_mb_s": cold_mb,
                "read_pooled_s": warm_s, "read_pooled_mb_s": warm_mb,
                "read_pooled_flt_s": "", "read_pooled_flt_mb_s": "",
                "extract_s": "churn_gb=%.0f" % (churn / 1e9)})
    print(f"    COLD full-scan {cold_s}s ({cold_mb} MB/s) vs WARM re-read {warm_s}s "
          f"({warm_mb} MB/s) — cold/warm ratio {cold_s/warm_s:.1f}x", flush=True)
    shutil.rmtree(cold_dir, ignore_errors=True)


def orchestrate(configs: list[dict], main_month: str, cold_month: str,
                smoke: bool, real_extract: bool, extract_workers: int) -> None:
    """Drive the ordered sequence as isolated subprocesses — the parent does NO
    heavy lifting (just spawns + waits), so it never degrades; each child does one
    heavy step in a fresh process and dies. Resumable: a crashed step is retried on
    the next launch (all steps skip-gate on their own completed output)."""
    common = (["--smoke"] if smoke else [])
    rx = (["--real-extract", "--extract-workers", str(extract_workers)]
          if real_extract else [])
    steps: list[tuple[str, list[str]]] = [("baseline", []), ("cold-write", [])]
    steps += [("run", ["--only", c["name"]] + rx) for c in configs]
    steps.append(("cold-read", []))

    t_start = time.time()
    print(f"=== Phase 3 {'SMOKE ' if smoke else ''}orchestrator === "
          f"{time.strftime('%Y-%m-%d %H:%M:%S')} — {len(steps)} isolated steps", flush=True)
    self = str(Path(__file__).resolve())
    for name, extra in steps:
        cmd = [sys.executable, "-u", self, "--step", name] + common + extra
        print(f"\n--- step '{name}' {' '.join(extra)} → fresh process ---", flush=True)
        rc = subprocess.run(cmd).returncode
        if rc != 0:
            sys.exit(f"FATAL: step '{name}' {' '.join(extra)} exited {rc} "
                     f"(0x{rc & 0xFFFFFFFF:08X}). Completed steps are skip-gated — "
                     f"relaunch to resume from here.")
    print(f"\n=== Phase 3 done in {(time.time()-t_start)/3600:.2f} h. Results: {RESULTS}",
          flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Print the ordered plan and exit.")
    ap.add_argument("--smoke", action="store_true",
                    help=f"Correctness run on {SMOKE_MONTH} (small; validates every strategy "
                         f"+ the cold flow) instead of the 66 GB month.")
    ap.add_argument("--only", default=None, help="Substring filter on config names.")
    ap.add_argument("--real-extract", action="store_true",
                    help="Also run the actual build_pooled_stats extract per config "
                         "(replay CPU + real pushdown) via a throwaway D: year=1900 partition.")
    ap.add_argument("--extract-workers", type=int, default=9,
                    help="Workers for --real-extract (default 9; the OOM-safe count).")
    ap.add_argument("--step", choices=["baseline", "cold-write", "run", "cold-read"],
                    default=None,
                    help="Internal: run ONE isolated step in this process (spawned by the "
                         "orchestrator). Not for manual use — launch with no --step to run "
                         "the whole sequence with per-step process isolation.")
    args = ap.parse_args()

    main_month = SMOKE_MONTH if args.smoke else MAIN_MONTH
    cold_month = SMOKE_MONTH if args.smoke else COLD_MONTH
    configs = [c for c in CONFIGS if not args.only or args.only in c["name"]]
    if not configs:
        sys.exit(f"no configs match --only {args.only!r}")

    if args.dry_run:
        print(f"Phase-3 plan  (main={main_month}, cold-check={cold_month}, "
              f"{len(configs)} write configs; each step runs in its OWN process):\n")
        for s in plan(configs, main_month, cold_month):
            print("  " + s)
        print(f"\nOutputs -> {OUT_ROOT} (deleted after measurement). "
              f"Results -> {RESULTS}")
        print("Launch the real run with no flags (SOLO). Add --real-extract for the "
              "production-consumer replay timing.")
        return

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    rx = args.extract_workers if args.real_extract else None

    # Child mode: do exactly one heavy step in this fresh process, then exit.
    if args.step == "baseline":
        step_baseline(main_month)
    elif args.step == "cold-write":
        step_cold_write(cold_month)
    elif args.step == "run":
        step_run(main_month, configs, rx)
    elif args.step == "cold-read":
        step_cold_read(cold_month, main_month)
    else:
        # Parent mode: orchestrate the isolated per-step subprocesses.
        orchestrate(configs, main_month, cold_month,
                    args.smoke, args.real_extract, args.extract_workers)


if __name__ == "__main__":
    main()
