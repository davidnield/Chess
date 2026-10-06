"""Bucket an EPD-complete consolidated month into the EPD backfill's output layout.

WHY THIS EXISTS
---------------
backfill_epd.py exists only because the extract writes parent_epd through ply 16.
An extract run at --epd-max-ply 30 (B1) already carries an EPD on every row, so
its consolidated month needs no replay at all: only the backfill's LAST two
stages, which split the rows by hash bucket and then write and verify each
bucket. This runs exactly those, through the backfill's own functions
(_copy_query, _write_bucket_task, _run_pool, _write_manifest), so the A2 merge
reads the result without knowing which tool produced it:

    <out>/month=Y_M/bkt=i/part-0000.parquet          PS_COLS, plain string
    <out>/_manifest/month=Y_M.parquet                the same 26-field manifest
    <out>/_conflicts/month=Y_M/bucket_month.parquet  hashes with two EPDs
    <out>/_month=Y_M.DONE                            written LAST

The replay and quarantine fields of the manifest are 0, 0.0 or "{}": nothing
was replayed and nothing may be quarantined.

GATES, ALL FATAL
----------------
  - The input has no NULL parent_epd (row-group null_count, else a COUNT).
    A month extracted at the default --epd-max-ply 16 is refused with the count;
    that month needs backfill_epd.py instead.
  - No bucket quarantines a row. With no replay there is no edge and no
    unreachable quarantine, so the only reason left is parity: an EPD whose side
    to move contradicts ply % 2. When the extract computed every EPD itself that
    is a bug, not a collision.
  - Book rows and the four summed columns equal the input exactly, no output row
    has a NULL EPD, and every bucket file reads back in full.

A parent_hash carrying two EPDs is reported in _conflicts, never fatal: those are
64-bit collision twins, which B1 keeps with their true EPDs for the A2 merge to
resolve (the backfill quarantines them instead, which is why one book must come
from one producer).

Resumable like the backfill: a sentinel skips the month, the bucketed rows are
checkpointed in the work dir, and a stale _tmp_month=* is discarded.

Usage:
    python bucket_month.py --monthly-dir <partial-dir>\\_monthly --out <dir>
                           --work-dir <fast local dir> [--buckets 512]
                           [--months 2024_6 ...] [--workers N] [--threads T]
                           [--mem M] [--tmp-dir D]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
import backfill_epd as bf
from build_pooled_stats import _bucket_expr, _duck, _sql_path
from consolidate_reclaim import totals

CONFLICT_KIND = "parent-epd"
CONFLICT_FILE = "bucket_month.parquet"

# The manifest every producer writes, in order. backfill_epd builds its row with
# pa.Table.from_pylist, so these are the types Python ints, floats and strings
# infer to; _test_bucket_month.py holds both producers to this list.
MANIFEST_FIELDS: tuple[tuple[str, str], ...] = (
    ("year", "int64"), ("month", "int64"), ("files", "int64"), ("bytes", "int64"),
    ("rows", "int64"), ("ply1_games", "int64"), ("total", "int64"),
    ("white_wins", "int64"), ("draws", "int64"), ("black_wins", "int64"),
    ("buckets", "int64"), ("edges_replayed", "int64"),
    ("positions_resolved", "int64"), ("replays_per_sec", "double"),
    ("mismatches", "int64"), ("conflicts", "int64"), ("unresolved", "int64"),
    ("quarantine_edges", "int64"), ("quarantine_rows", "int64"),
    ("quarantine_total", "int64"), ("quarantine_white_wins", "int64"),
    ("quarantine_draws", "int64"), ("quarantine_black_wins", "int64"),
    ("quarantine_by_reason", "string"), ("unreachable_positions", "int64"),
    ("seconds", "double"),
)


def null_epds(monthly: Path, threads: int, mem: str, tmp: Path) -> tuple[int, str]:
    """NULL parent_epd rows in the input, and how they were counted.

    The row-group statistics answer this from the footer when every row group
    carries a null count; a file written without them costs one scan instead."""
    with pq.ParquetFile(monthly) as pf:
        md = pf.metadata
        j = next((k for k in range(md.num_columns)
                  if md.schema.column(k).path == "parent_epd"), None)
        if j is None:
            raise RuntimeError(f"{monthly.name}: no parent_epd column")
        n, complete = 0, True
        for i in range(md.num_row_groups):
            st = md.row_group(i).column(j).statistics
            if st is None or not st.has_null_count:
                complete = False
                break
            n += st.null_count
    if complete:
        return n, "row-group statistics"
    con = _duck(threads, mem, tmp)
    try:
        n = con.execute(f"SELECT COUNT(*) FILTER (WHERE parent_epd IS NULL) "
                        f"FROM read_parquet('{_sql_path(monthly)}')").fetchone()[0]
    finally:
        con.close()
    return int(n), "COUNT(*) scan"


def _bucket_task(task: tuple) -> tuple:
    """One bucket: the backfill's write-and-verify, then the bucket's conflicts.

    Conflicts are read off the file just written, so they describe the book as
    the merge will see it."""
    res = bf._write_bucket_task(task)
    out, threads, mem, tmp_s = Path(task[3]), task[8], task[9], task[10]
    con = _duck(threads, mem, Path(tmp_s))
    try:
        rows = con.execute(f"""
            SELECT parent_hash, MIN(parent_epd), MAX(parent_epd)
            FROM read_parquet('{_sql_path(out)}')
            GROUP BY parent_hash
            HAVING MIN(parent_epd) <> MAX(parent_epd)
        """).fetchall()
    finally:
        con.close()
    return res, [{"hash": h, "epd_a": a, "epd_b": b, "kind": CONFLICT_KIND}
                 for h, a, b in rows]


def bucket_month(monthly: Path, year: int, month: int, out_dir: Path,
                 work_root: Path, nb: int, workers: int, threads: int, mem: str,
                 tmp: Path) -> dict:
    tag = f"{year}_{month}"
    sentinel = out_dir / f"_month={tag}.DONE"
    if sentinel.exists():
        print(f"  {year}/{month}: already done", flush=True)
        return {}
    work = work_root / f"month={tag}"
    params = {"buckets": nb, "monthly": str(monthly),
              "size": monthly.stat().st_size, "producer": "bucket_month"}
    pfile = work / "_params.json"
    if pfile.exists() and json.loads(pfile.read_text()) != params:
        bf._rmtree(work)
    work.mkdir(parents=True, exist_ok=True)
    pfile.write_text(json.dumps(params), encoding="utf-8")
    bf._rmtree(out_dir / f"_tmp_month={tag}")

    t_month = time.time()
    # Same split as the backfill: --threads/--mem describe the machine, and the
    # per-bucket steps run `workers` at once.
    per_worker_mem = f"{max(bf._mem_gb(mem) / max(workers, 1), 1.0):.1f}GB"
    task_threads = max(1, threads // max(workers, 1))
    print(f"  {year}/{month}: {monthly.stat().st_size/1e9:,.1f} GB, {nb} buckets, "
          f"{workers} workers x ({task_threads} threads, {per_worker_mem})", flush=True)

    # 0 ── preflight: an EPD on every row, or this is the wrong tool
    n_null, how = null_epds(monthly, threads, mem, tmp)
    if n_null:
        raise RuntimeError(
            f"{year}/{month}: {n_null:,} input rows have a NULL parent_epd ({how}). "
            f"bucket_month needs an EPD-complete month: extract with "
            f"--epd-max-ply 30, or make this one complete with backfill_epd.py")
    print(f"    preflight: 0 NULL parent_epd ({how})", flush=True)

    # 1 ── rows, bucketed exactly as merge Phase A would have (backfill stage 5)
    rows_dir = work / "rows"
    if not bf._done(work, "rows"):
        sql = f"""
            COPY (SELECT *, {_bucket_expr("parent_hash", nb)} AS bkt
                  FROM read_parquet('{_sql_path(monthly)}'))
            TO '{_sql_path(work / "_tmp_rows")}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (bkt))
        """
        size, secs, _ = bf._run_isolated(
            bf._copy_query, (sql, str(work / "_tmp_rows"), str(rows_dir), threads,
                             mem, str(tmp)))
        print(f"    rows bucketed: {size/1e9:,.1f} GB ({secs:,.0f}s)", flush=True)
        bf._mark(work, "rows")

    # 2 ── write + verify, parity on and nothing to join or quarantine
    t0 = time.time()
    out_tmp = out_dir / f"_tmp_month={tag}"
    q_dir = work / "q"
    bf._rmtree(q_dir)
    tasks = [(b, [str(f) for f in fs], [],
              str(out_tmp / f"bkt={b}" / "part-0000.parquet"),
              str(q_dir / f"bkt={b}.parquet"), [], [], True,
              task_threads, per_worker_mem, str(tmp))
             for b, fs in sorted(bf._bucket_files(rows_dir, "bkt=*/*.parquet").items())]
    out = bf._run_pool(_bucket_task, tasks, workers)
    res = [r for r, _ in out]
    conflicts = [c for _, cs in out for c in cs]
    n_rows = sum(r[1] for r in res)
    got = tuple(sum(r[2][j] for r in res) for j in range(len(bf.SUM_COLS)))
    n_null_out = sum(r[3] for r in res)
    n_bytes = sum(r[4] for r in res)
    n_q = sum(r[6] for r in res)
    if n_q:
        by_reason: dict[str, int] = {}
        for r in res:
            for why, (n, _) in r[8].items():
                by_reason[why] = by_reason.get(why, 0) + n
        raise RuntimeError(
            f"{year}/{month}: {n_q:,} rows failed the write gate ({by_reason}) -- "
            f"see {q_dir}. Every EPD here came from the extract, so a parity "
            f"violation is a bug, not a collision.")

    con = _duck(threads, mem, tmp)
    try:
        want = tuple(int(x) for x in totals(con, monthly, bf.SUM_COLS))
        in_rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_sql_path(monthly)}')").fetchone()[0]
        ply1 = con.execute(
            f"SELECT SUM(total)::HUGEINT FROM read_parquet('{_sql_path(monthly)}') "
            f"WHERE ply = 1").fetchone()[0]
    finally:
        con.close()
    if n_null_out:
        raise RuntimeError(f"{year}/{month}: {n_null_out:,} output rows have a NULL "
                           f"parent_epd")
    if n_rows != in_rows:
        raise RuntimeError(f"{year}/{month}: {n_rows:,} rows out != {in_rows:,} in")
    if got != want:
        broken = [f"{c}: in {w:,} != out {g:,}"
                  for c, w, g in zip(bf.SUM_COLS, want, got) if w != g]
        raise RuntimeError(f"{year}/{month}: conservation broken — {'; '.join(broken)}")

    # 3 ── promote, then the manifest, then the report, then the sentinel LAST
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"month={tag}"
    bf._rmtree(final)
    out_tmp.replace(final)
    secs = time.time() - t_month
    man = {"year": year, "month": month, "files": len(res), "bytes": n_bytes,
           "rows": n_rows, "ply1_games": int(ply1 or 0),
           "total": got[0], "white_wins": got[1], "draws": got[2],
           "black_wins": got[3], "buckets": nb,
           "edges_replayed": 0, "positions_resolved": 0, "replays_per_sec": 0.0,
           "mismatches": 0, "conflicts": len(conflicts), "unresolved": 0,
           "quarantine_edges": 0, "quarantine_rows": 0, "quarantine_total": 0,
           "quarantine_white_wins": 0, "quarantine_draws": 0,
           "quarantine_black_wins": 0, "quarantine_by_reason": "{}",
           "unreachable_positions": 0, "seconds": float(secs)}
    assert list(man) == [f for f, _ in MANIFEST_FIELDS], "manifest field order drifted"
    bf._write_manifest(out_dir, tag, man)
    report = out_dir / "_conflicts" / f"month={tag}" / CONFLICT_FILE
    bf._write_report(report, conflicts, bf._CONFLICT_SCHEMA)
    sentinel.write_text(json.dumps(man, indent=2), encoding="utf-8")
    bf._rmtree(work)
    print(f"    wrote {len(res)} buckets, {n_rows:,} rows, {n_bytes/1e9:,.1f} GB "
          f"({time.time()-t0:,.0f}s); {len(conflicts):,} conflicts; month "
          f"{secs/60:.1f} min", flush=True)
    return man


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--monthly-dir", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--work-dir", type=Path, default=None,
                    help="Scratch for the bucketed rows, ~1x the month. Fast local "
                         "storage, never a USB spinning disk. Default <out>/_work.")
    ap.add_argument("--buckets", type=int, default=bf.DEFAULT_BUCKETS)
    ap.add_argument("--months", nargs="*", default=None, metavar="Y_M")
    ap.add_argument("--workers", type=int, default=0,
                    help="Bucket writers. Default: logical cores - 1.")
    ap.add_argument("--threads", type=int, default=8,
                    help="DuckDB threads for the whole-month COPY.")
    ap.add_argument("--mem", default="16GB",
                    help="DuckDB memory budget, split across --workers for the "
                         "per-bucket steps.")
    ap.add_argument("--tmp-dir", type=Path, default=None)
    a = ap.parse_args()

    if not a.monthly_dir.is_dir():
        print(f"FATAL: no monthly dir at {a.monthly_dir}")
        return 1
    if a.buckets < 1 or a.buckets & (a.buckets - 1):
        print(f"FATAL: --buckets must be a power of two, got {a.buckets}")
        return 1
    work_root = a.work_dir or (a.out / "_work")
    tmp = a.tmp_dir or (work_root / "_duck_tmp")
    workers = a.workers or max((os.cpu_count() or 2) - 1, 1)
    tmp.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)
    a.out.mkdir(parents=True, exist_ok=True)

    months = bf.discover_months(a.monthly_dir, a.months)
    if not months:
        print(f"FATAL: no year=*_month=*.ps.parquet in {a.monthly_dir}")
        return 1
    print(f"monthly  : {a.monthly_dir}")
    print(f"out      : {a.out}")
    print(f"work     : {work_root}   (duckdb temp {tmp})")
    print(f"months   : {len(months)}  buckets {a.buckets}  workers {workers}  mem {a.mem}")
    print(f"free     : out {shutil.disk_usage(a.out).free/1e9:,.0f} GB, "
          f"work {shutil.disk_usage(work_root).free/1e9:,.0f} GB\n", flush=True)
    t0 = time.time()
    for y, mo, f in months:
        bucket_month(f, y, mo, a.out, work_root, a.buckets, workers, a.threads,
                     a.mem, tmp)
    print(f"\n{len(months)} months in {(time.time()-t0)/60:,.1f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())
