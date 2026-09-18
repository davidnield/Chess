"""Make a consolidated month EPD-complete, and write it pre-bucketed for the merge.

WHY THIS EXISTS
---------------
The extract stores `parent_epd` only through ply 16 (`EPD_MAX_PLY` in
build_pooled_stats): past the opening window the value is never displayed, costs
~19.55 B/row, and `board.epd()` was over half of replay time. For the banded
explorer book that shortcut has to be undone — Stage 3's gate crashes on a node
with no EPD, and the collision audit has nothing to compare.

Every game's path lies inside one month, so a month can be made EPD-complete on
its own: replay forward from the ply-<=16 EPDs the extract did keep. Doing it per
month, on the machine that owns the month, means the final book comes out
complete with no rewrite of a multi-TB artifact afterwards.

The same pass writes the month already split into the merge's hash buckets, which
is exactly what `_partition_by_bucket` (merge Phase A) would have produced. The
merge reads this with --parts-dir and skips Phase A entirely.

WHAT IT COSTS (measured, 2024-06 at home, 6C/12T)
-------------------------------------------------
    1,355,187,048 rows -> 1.03B distinct positions, 1.06B distinct edges
    92% of positions occur in exactly ONE game and carry 70% of the rows
    replay: 8,325 edges/s/core, 40-46k/s across 11 workers
        Board(epd) 45.7us + parse_san 10.3 + push/pop 15.9 + epd() 37.9
        + zobrist_int64 10.3  =  120us per edge
    => ~6.5 h per full-size month, and parent_epd adds ~17.5 B/row on disk

There is no cheap version of this: the cost IS the one-game tail, and re-replaying
the source games instead measures out the same. Two thirds of the time is the
EPD <-> Board round trip that carries state from one ply to the next.

HOW IT WORKS, PER MONTH
-----------------------
  edges     one streaming split of the month into edges/ply=K/pb=B (no GROUP BY)
  seeds     per bucket, the parent_hash -> parent_epd the extract already wrote
  levels    for ply k = 1..30, every bucket in parallel: join the level's edges to
            the known EPDs of that bucket, replay each move with python-chess,
            verify zobrist_int64(child) == child_hash, and emit the child EPDs.
            The children scatter across buckets, so each level ends with one
            re-partition of what it found.
  fixpoint  `ply` in a consolidated row is any_value(), so a key can carry a ply
            from a transposed occurrence and miss its level. Sweep whatever is
            still missing against the whole known table until nothing moves.
  write     LEFT JOIN each bucket's rows to the known table, COALESCE the EPD in,
            verify, then rename the month into place.

Every stage is skip-gated on its own marker, so a killed run resumes at the stage
it died in rather than repeating hours of replay. --fresh forces a rebuild.

Usage:
    python backfill_epd.py --monthly-dir <partial-dir>\\_monthly
                           --out <dir> --work-dir <fast local dir>
                           [--buckets 512] [--months 2024_6 ...]
                           [--workers N] [--threads T] [--mem M] [--tmp-dir D]

    # home
    python backfill_epd.py --monthly-dir E:\\chess\\explorer_banded_home\\monthly_2026
        --out F:\\chess\\explorer_banded\\parts --work-dir D:\\chess\\_epd_work
        --workers 11 --mem 40GB --tmp-dir D:\\chess_duckdb_tmp
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import chess
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import (_bucket_expr, _duck, _mem_bytes,
                                _peak_commit_bytes, _sql_path)
from consolidate_reclaim import read_fully, totals
from zobrist import zobrist_int64

# The merge's bucket count for the explorer book (spec A2). Every hash bucket
# here is one bucket there, so no re-partitioning happens between the two.
DEFAULT_BUCKETS = 512

# Column order of a consolidated ps monthly. The output must match it exactly:
# merge Phase B selects these by name, and a reordered file would still read but
# would stop being diff-able against the input.
PS_COLS = ("parent_hash", "move_san", "event", "elo_band", "parent_epd",
           "child_hash", "child_eval", "ply", "white_wins", "draws",
           "black_wins", "total")
SUM_COLS = ("total", "white_wins", "draws", "black_wins")

MONTH_RE = re.compile(r"^year=(\d+)_month=(\d+)\.ps\.parquet$")

# Transposition stragglers converge in one or two sweeps; more than this means
# the edge graph is not what we think it is, and looping is the wrong answer.
MAX_SWEEPS = 6

# DuckDB keeps one writer open per partition value; the default cap of 100 makes
# a 512-way split thrash. Raising it trades a little memory for one pass.
_MAX_OPEN_FILES = 1024


# ── small helpers ─────────────────────────────────────────────────────────────

def _read_list(paths) -> str:
    """A read_parquet(...) argument from explicit paths — never a bare glob.

    DuckDB raises on a glob that matches no file, and at shallow plies most
    buckets are legitimately empty, so the caller checks the list instead."""
    return "[" + ", ".join(f"'{_sql_path(p)}'" for p in paths) + "]"


def _marker(work: Path, name: str) -> Path:
    return work / f"_{name}.DONE"


def _done(work: Path, name: str) -> bool:
    return _marker(work, name).exists()


def _mark(work: Path, name: str) -> None:
    _marker(work, name).write_text(time.strftime("%Y-%m-%d %H:%M:%S"),
                                   encoding="utf-8")


def _rmtree(p: Path) -> None:
    shutil.rmtree(p, ignore_errors=True)


def _dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())


def _run_pool(fn, tasks: list, workers: int) -> list:
    """Run tasks in parallel and FAIL FAST.

    ProcessPoolExecutor's context exit is shutdown(wait=True), so the obvious
    submit-all + as_completed loop makes the first exception wait for every
    queued task to run. That is what silently burned ~7 h of home's 2026
    consolidation. Cancelling the queue first bounds the wait to the tasks
    already in flight.

    NO max_tasks_per_child. It HANGS on Python 3.11 + Windows spawn: the pilot
    stopped dead after exactly 352 of 512 seed tasks — 11 workers x 32 tasks —
    with every worker exited at its limit, no replacement spawned, no exception,
    and the parent waiting forever on futures that could never run. Worker
    recycling is not needed here anyway: every caller builds a fresh pool for one
    stage or one ply and tears it down after, so no worker outlives a level. The
    long-lived-process fragmentation that _consolidate_one_month documents is
    handled by _run_isolated, which gives each heavy DuckDB query its own process.
    """
    if not tasks:
        return []
    out = []
    with ProcessPoolExecutor(max_workers=min(workers, len(tasks))) as ex:
        futs = [ex.submit(fn, t) for t in tasks]
        try:
            for f in futs:
                out.append(f.result())
        except BaseException:
            ex.shutdown(wait=False, cancel_futures=True)
            raise
    return out


def _run_isolated(fn, task):
    """One DuckDB query in a fresh process. Per-query process isolation is the
    Windows allocator-fragmentation defence _consolidate_one_month documents;
    submitting one task at a time is the fail-fast half."""
    with ProcessPoolExecutor(max_workers=1) as ex:
        return ex.submit(fn, task).result()


def _write_pairs(path: Path, hashes: list, epds: list) -> int:
    """(hash, epd) to parquet, atomically. Returns rows written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    tmp.unlink(missing_ok=True)
    pq.write_table(pa.table({"h": pa.array(hashes, pa.int64()),
                             "epd": pa.array(epds, pa.string())}),
                   tmp, compression="zstd")
    tmp.replace(path)
    return len(hashes)


def _write_report(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path,
                   compression="zstd")


_MISMATCH_SCHEMA = pa.schema([("parent_hash", pa.int64()),
                              ("parent_epd", pa.string()),
                              ("move_san", pa.string()),
                              ("child_hash", pa.int64()),
                              ("computed_hash", pa.int64()),
                              ("computed_epd", pa.string()),
                              ("reason", pa.string())])
_CONFLICT_SCHEMA = pa.schema([("hash", pa.int64()), ("epd_a", pa.string()),
                              ("epd_b", pa.string()), ("kind", pa.string())])


# ── stage 1: split the month into per-(ply, bucket) edge files ────────────────

def _copy_query(task: tuple) -> tuple:
    """Run ONE COPY in a fresh process, tmp dir -> rename. Targets are
    directories (COPY ... PARTITION_BY), so the rename is the atomic step that
    makes the stage marker meaningful."""
    sql, out_tmp_s, out_s, threads, mem, tmp_s = task
    out_tmp, out = Path(out_tmp_s), Path(out_s)
    _rmtree(out_tmp)
    t0 = time.time()
    con = _duck(threads, mem, Path(tmp_s))
    try:
        con.execute(f"SET partitioned_write_max_open_files={_MAX_OPEN_FILES};")
        con.execute(sql)
    finally:
        con.close()
    _rmtree(out)
    out_tmp.replace(out)
    return _dir_bytes(out), time.time() - t0, _peak_commit_bytes()


def _stage_edges(monthly: Path, work: Path, nb: int, threads: int, mem: str,
                 tmp: Path) -> None:
    """month -> edges/ply=K/pb=B/*.parquet.

    Two passes, not one PARTITION_BY (ply, pb): 30 x 512 partitions in a single
    COPY would hold thousands of writers open. By ply first, then each ply by
    bucket, no COPY ever opens more than `nb` of them.

    parent_epd rides along so the seed pass never has to touch the month again.
    """
    if _done(work, "edges"):
        return
    by_ply = work / "edges_ply"
    if not _done(work, "edges_ply"):
        sql = f"""
            COPY (SELECT parent_hash, move_san, child_hash, ply, parent_epd
                  FROM read_parquet('{_sql_path(monthly)}'))
            TO '{_sql_path(work / "_tmp_edges_ply")}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (ply))
        """
        size, secs, peak = _run_isolated(
            _copy_query, (sql, str(work / "_tmp_edges_ply"), str(by_ply),
                          threads, mem, str(tmp)))
        print(f"    edges by ply: {size/1e9:,.1f} GB ({secs:,.0f}s"
              f"{'' if peak is None else f', peak {peak/1e9:.1f} GB'})",
              flush=True)
        _mark(work, "edges_ply")

    edges = work / "edges"
    tmp_edges = work / "_tmp_edges"
    _rmtree(tmp_edges)
    tmp_edges.mkdir(parents=True, exist_ok=True)
    t0, total = time.time(), 0
    for pdir in sorted(by_ply.glob("ply=*")):
        k = pdir.name.split("=")[1]
        sql = f"""
            COPY (SELECT parent_hash, move_san, child_hash, parent_epd,
                         {_bucket_expr("parent_hash", nb)} AS pb
                  FROM read_parquet('{_sql_path(pdir)}/*.parquet'))
            TO '{_sql_path(tmp_edges / f"_tmp_ply={k}")}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (pb))
        """
        size, _, _ = _run_isolated(
            _copy_query, (sql, str(tmp_edges / f"_tmp_ply={k}"),
                          str(tmp_edges / f"ply={k}"), threads, mem, str(tmp)))
        total += size
    _rmtree(edges)
    tmp_edges.replace(edges)
    _rmtree(by_ply)
    _marker(work, "edges_ply").unlink(missing_ok=True)
    print(f"    edges by (ply, bucket): {total/1e9:,.1f} GB "
          f"({time.time()-t0:,.0f}s)", flush=True)
    _mark(work, "edges")


# ── stage 2: seeds — the EPDs the extract already wrote ───────────────────────

def _seed_task(task: tuple) -> tuple:
    """One bucket's parent_hash -> parent_epd, from every ply of that bucket.

    A key's parent_epd and its ply both come through any_value() in
    consolidation, so a row can carry ply > 16 AND a non-null EPD (measured:
    148k such rows in 2024-06). Seeding on "parent_epd IS NOT NULL" rather than
    on the ply is therefore the only correct rule.
    """
    b, edge_files, out_s, threads, mem, tmp_s = task
    con = _duck(threads, mem, Path(tmp_s))
    try:
        t = con.execute(f"""
            SELECT parent_hash, MIN(parent_epd) AS a, MAX(parent_epd) AS b
            FROM read_parquet({_read_list(edge_files)})
            WHERE parent_epd IS NOT NULL
            GROUP BY parent_hash
        """).fetch_arrow_table()
    finally:
        con.close()
    hs, es = t.column(0).to_pylist(), t.column(1).to_pylist()
    bs = t.column(2).to_pylist()
    conflicts = [{"hash": h, "epd_a": a, "epd_b": c, "kind": "seed"}
                 for h, a, c in zip(hs, es, bs) if a != c]
    _write_pairs(Path(out_s), hs, es)
    return b, len(hs), conflicts


# ── stage 3: the level loop — the replay ──────────────────────────────────────

def _replay_task(task: tuple) -> tuple:
    """One (level, bucket): join the level's edges to that bucket's known EPDs,
    replay every move, and emit the children's EPDs.

    The parent board is rebuilt from its EPD and reused across that parent's
    moves (push/pop), because Board(epd) is 46us of the 120us per edge.

    A parent's OWN hash is never checked against its EPD. Polyglot mixes in the
    en-passant file whenever a pawn is merely adjacent to the ep square, while
    board.epd() prints the square only when the capture is legal — so the two
    legitimately disagree on a pinned-ep position. A child reached by push()
    carries the true ep state, which is what makes the child check sound. And a
    parent rebuilt without an illegal ep square yields identical children: the
    only move that state could change is an ep capture, which was illegal.
    """
    (k, b, edge_files, known_files, need_files, out_s, threads, mem,
     tmp_s) = task
    t0 = time.time()
    semi = ""
    if need_files:
        semi = (f"SEMI JOIN (SELECT h FROM read_parquet("
                f"{_read_list(need_files)})) n ON e.c0 = n.h")
    con = _duck(threads, mem, Path(tmp_s))
    try:
        t = con.execute(f"""
            WITH e AS (
                SELECT parent_hash, move_san,
                       MIN(child_hash) AS c0, MAX(child_hash) AS c1
                FROM read_parquet({_read_list(edge_files)})
                GROUP BY parent_hash, move_san
            ), k AS (
                SELECT h, MIN(epd) AS epd
                FROM read_parquet({_read_list(known_files)})
                GROUP BY h
            )
            SELECT e.parent_hash, k.epd, list(e.move_san) AS sans,
                   list(e.c0) AS c0s, list(e.c1) AS c1s
            FROM e JOIN k ON e.parent_hash = k.h {semi}
            GROUP BY e.parent_hash, k.epd
        """).fetch_arrow_table()
    finally:
        con.close()
    parents = t.column(0).to_pylist()
    epds = t.column(1).to_pylist()
    sans = t.column(2).to_pylist()
    c0s = t.column(3).to_pylist()
    c1s = t.column(4).to_pylist()

    found: dict[int, str] = {}
    bad: list[dict] = []
    conflicts: list[dict] = []
    n_edges = 0
    for ph, epd, mv_sans, mv_c0, mv_c1 in zip(parents, epds, sans, c0s, c1s):
        # " 0 1": an EPD has no clocks. Neither the hash nor legality uses them,
        # and the 6-field parse measured faster than the 4-field one.
        board = chess.Board(epd + " 0 1")
        push, pop, parse, board_epd = (board.push, board.pop, board.parse_san,
                                       board.epd)
        for san, c0, c1 in zip(mv_sans, mv_c0, mv_c1):
            n_edges += 1
            if c1 != c0:
                conflicts.append({"hash": ph, "epd_a": epd, "epd_b": san,
                                  "kind": "edge-child"})
            try:
                move = parse(san)
            except (ValueError, AssertionError) as exc:
                bad.append({"parent_hash": ph, "parent_epd": epd,
                            "move_san": san, "child_hash": c0,
                            "computed_hash": None, "computed_epd": None,
                            "reason": f"parse: {type(exc).__name__}"})
                continue
            push(move)
            ce, ch = board_epd(), zobrist_int64(board)
            pop()
            if ch != c0:
                bad.append({"parent_hash": ph, "parent_epd": epd,
                            "move_san": san, "child_hash": c0,
                            "computed_hash": ch, "computed_epd": ce,
                            "reason": "hash"})
                continue
            prev = found.get(c0)
            if prev is None:
                found[c0] = ce
            elif prev != ce:
                conflicts.append({"hash": c0, "epd_a": prev, "epd_b": ce,
                                  "kind": "child"})
    _write_pairs(Path(out_s), list(found.keys()), list(found.values()))
    return (k, b, n_edges, len(found), bad, conflicts, time.time() - t0,
            _peak_commit_bytes())


def _repartition_new(task: tuple) -> tuple:
    """One level's discoveries, re-split by the CHILD's bucket.

    A child almost never lands in its parent's bucket, so this shuffle is what
    makes the next level's join local again. Duplicates across levels are left
    in place (a transposition can be discovered twice); every reader of the
    known table de-duplicates with GROUP BY h, and the end-of-month conflict
    pass is what decides whether two EPDs for one hash are a collision.
    """
    src_files, out_tmp_s, out_s, nb, threads, mem, tmp_s = task
    sql = f"""
        COPY (SELECT h, epd, {_bucket_expr("h", nb)} AS bkt
              FROM read_parquet({_read_list(src_files)}))
        TO '{_sql_path(Path(out_tmp_s))}'
        (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (bkt))
    """
    return _copy_query((sql, out_tmp_s, out_s, threads, mem, tmp_s))


# ── stage 4: write the month out, bucketed and EPD-complete ───────────────────

def _write_bucket_task(task: tuple) -> tuple:
    """One output bucket: rows LEFT JOIN known, EPD coalesced in, then verified.

    Verification happens here, while the file is still warm: full row-group read,
    the four conserved sums, and the null-EPD count. The month-level gate then
    only has to add them up and compare against the input.
    """
    (i, row_files, known_files, out_s, threads, mem, tmp_s) = task
    out = Path(out_s)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    tmp.unlink(missing_ok=True)
    if known_files:
        cols = ", ".join(f"r.{c}" if c != "parent_epd"
                         else "COALESCE(r.parent_epd, k.epd) AS parent_epd"
                         for c in PS_COLS)
        src = (f"read_parquet({_read_list(row_files)}) r "
               f"LEFT JOIN (SELECT h, MIN(epd) AS epd "
               f"FROM read_parquet({_read_list(known_files)}) GROUP BY h) k "
               f"ON r.parent_hash = k.h")
    else:
        # Only reachable for a bucket whose every row already carried an EPD.
        cols = ", ".join(f"r.{c}" for c in PS_COLS)
        src = f"read_parquet({_read_list(row_files)}) r"
    t0 = time.time()
    con = _duck(threads, mem, Path(tmp_s))
    try:
        con.execute(f"""
            COPY (SELECT {cols} FROM {src})
            TO '{_sql_path(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        tmp.replace(out)
        sums = ", ".join(f"SUM({c})::HUGEINT" for c in SUM_COLS)
        stats = con.execute(f"""
            SELECT COUNT(*), {sums},
                   COUNT(*) FILTER (WHERE parent_epd IS NULL)
            FROM read_parquet('{_sql_path(out)}')
        """).fetchone()
    finally:
        con.close()
    good, why = read_fully(out)
    if not good:
        raise RuntimeError(f"bucket {i} unreadable after write — {why}")
    return (i, int(stats[0]), tuple(int(x) for x in stats[1:5]), int(stats[5]),
            out.stat().st_size, time.time() - t0)


# ── the month ─────────────────────────────────────────────────────────────────

def _bucket_files(root: Path, pattern: str) -> dict[int, list[Path]]:
    """bucket -> its files, for a hive layout with the bucket anywhere in it."""
    out: dict[int, list[Path]] = {}
    for f in root.glob(pattern):
        m = re.search(r"(?:pb|bkt)=(\d+)", str(f))
        if m:
            out.setdefault(int(m.group(1)), []).append(f)
    return out


def backfill_month(monthly: Path, year: int, month: int, out_dir: Path,
                   work_root: Path, nb: int, workers: int, threads: int,
                   mem: str, tmp: Path, fresh: bool) -> dict:
    tag = f"{year}_{month}"
    sentinel = out_dir / f"_month={tag}.DONE"
    if sentinel.exists():
        print(f"  {year}/{month}: already done", flush=True)
        return {}
    work = work_root / f"month={tag}"
    params = {"buckets": nb, "monthly": str(monthly),
              "size": monthly.stat().st_size}
    pfile = work / "_params.json"
    if fresh or (pfile.exists() and json.loads(pfile.read_text()) != params):
        _rmtree(work)
    work.mkdir(parents=True, exist_ok=True)
    pfile.write_text(json.dumps(params), encoding="utf-8")
    _rmtree(out_dir / f"_tmp_month={tag}")

    t_month = time.time()
    # --threads and --mem describe the WHOLE machine; the per-bucket steps run
    # `workers` of them at once, so each gets a share. One DuckDB thread apiece
    # is right when the work in the worker is single-threaded python-chess.
    per_worker_mem = f"{max(_mem_gb(mem) / max(workers, 1), 1.0):.1f}GB"
    task_threads = max(1, threads // max(workers, 1))
    print(f"  {year}/{month}: {monthly.stat().st_size/1e9:,.1f} GB, "
          f"{nb} buckets, {workers} workers x ({task_threads} threads, "
          f"{per_worker_mem})", flush=True)

    # 1 ── edges
    _stage_edges(monthly, work, nb, threads, mem, tmp)
    edges = work / "edges"
    plies = sorted(int(p.name.split("=")[1]) for p in edges.glob("ply=*"))
    known = work / "known"

    # 2 ── seeds
    if not _done(work, "seeds"):
        t0 = time.time()
        per_bucket: dict[int, list[Path]] = {}
        for pd_ in edges.glob("ply=*/pb=*"):
            per_bucket.setdefault(int(pd_.name.split("=")[1]), []).extend(
                sorted(pd_.glob("*.parquet")))
        tasks = [(b, [str(f) for f in fs],
                  str(known / "lvl=seed" / f"bkt={b}" / "data.parquet"),
                  task_threads, per_worker_mem, str(tmp))
                 for b, fs in sorted(per_bucket.items())]
        res = _run_pool(_seed_task, tasks, workers)
        seeds = sum(r[1] for r in res)
        conflicts = [c for r in res for c in r[2]]
        _write_report(work / "conflicts" / "seed.parquet", conflicts,
                      _CONFLICT_SCHEMA)
        print(f"    seeds: {seeds:,} positions from the extract "
              f"({len(conflicts):,} conflicts, {time.time()-t0:,.0f}s)",
              flush=True)
        _mark(work, "seeds")

    # 3 ── levels
    stats = {"edges_replayed": 0, "resolved": 0, "replay_secs": 0.0,
             "mismatches": 0, "conflicts": 0}
    counts = json.loads((work / "_counts.json").read_text()) if (
        work / "_counts.json").exists() else stats
    stats.update(counts)
    for k in plies:
        if _done(work, f"lvl={k}"):
            continue
        t0 = time.time()
        edge_b = _bucket_files(edges / f"ply={k}", "pb=*/*.parquet")
        tasks = []
        for b, fs in sorted(edge_b.items()):
            kf = sorted(known.glob(f"lvl=*/bkt={b}/*.parquet"))
            if not kf:
                continue
            tasks.append((k, b, [str(f) for f in fs], [str(f) for f in kf],
                          None,
                          str(work / "new" / f"lvl={k}" / f"from={b}.parquet"),
                          task_threads, per_worker_mem, str(tmp)))
        res = _run_pool(_replay_task, tasks, workers)
        n_edges = sum(r[2] for r in res)
        n_found = sum(r[3] for r in res)
        bad = [x for r in res for x in r[4]]
        conf = [x for r in res for x in r[5]]
        peak = max((r[7] or 0) for r in res) if res else 0
        _write_report(work / "mismatch" / f"lvl={k}.parquet", bad,
                      _MISMATCH_SCHEMA)
        _write_report(work / "conflicts" / f"lvl={k}.parquet", conf,
                      _CONFLICT_SCHEMA)
        if bad:
            raise RuntimeError(
                f"{year}/{month} ply {k}: {len(bad):,} hash mismatches — see "
                f"{work / 'mismatch' / f'lvl={k}.parquet'}. The first is "
                f"{bad[0]}")
        if n_found:
            src = sorted((work / "new" / f"lvl={k}").glob("*.parquet"))
            _run_isolated(_repartition_new,
                          ([str(f) for f in src],
                           str(work / "_tmp_known_lvl"),
                           str(known / f"lvl={k}"), nb, threads, mem, str(tmp)))
        _rmtree(work / "new" / f"lvl={k}")
        stats["edges_replayed"] += n_edges
        stats["resolved"] += n_found
        stats["replay_secs"] += time.time() - t0
        stats["conflicts"] += len(conf)
        (work / "_counts.json").write_text(json.dumps(stats), encoding="utf-8")
        rate = n_edges / max(time.time() - t0, 1e-9)
        print(f"    ply {k:>2}: {len(tasks):>4} buckets, {n_edges:>13,} edges, "
              f"{n_found:>13,} resolved ({time.time()-t0:>6,.0f}s, "
              f"{rate:>7,.0f}/s, peak {peak/1e9:.1f} GB)", flush=True)
        _mark(work, f"lvl={k}")

    # 4 ── fixpoint: keys whose any_value(ply) sent them to the wrong level
    #
    # A consolidated key carries any_value(ply), so a (parent, move) seen at two
    # depths can be filed under the deeper one and miss the level where its
    # parent became known. Sweeping what is still missing against the WHOLE known
    # table catches those; the count must reach zero, because every position that
    # appears as a parent in the month was itself reached by an edge in the same
    # month.
    rnd = 0
    while True:
        rnd += 1
        need_dir = work / "need" / f"round={rnd}"
        cnt = work / f"_need={rnd}.json"
        if not cnt.exists():
            n = _missing_positions(edges, known, need_dir, task_threads,
                                   per_worker_mem, tmp, workers)
            cnt.write_text(str(n), encoding="utf-8")
        n_need = int(cnt.read_text())
        if n_need == 0:
            break
        if rnd > MAX_SWEEPS:
            raise RuntimeError(
                f"{year}/{month}: {n_need:,} positions still have no EPD after "
                f"{MAX_SWEEPS} sweeps — see {need_dir}")
        if not _done(work, f"sweep={rnd}"):
            t0 = time.time()
            need_files = [str(f) for f in sorted(need_dir.glob("*.parquet"))]
            tasks = []
            for b in sorted(_bucket_files(edges, "ply=*/pb=*/*.parquet")):
                fs = sorted(edges.glob(f"ply=*/pb={b}/*.parquet"))
                kf = sorted(known.glob(f"lvl=*/bkt={b}/*.parquet"))
                if not fs or not kf:
                    continue
                tasks.append((f"s{rnd}", b, [str(f) for f in fs],
                              [str(f) for f in kf], need_files,
                              str(work / "new" / f"sweep={rnd}" /
                                  f"from={b}.parquet"),
                              task_threads, per_worker_mem, str(tmp)))
            res = _run_pool(_replay_task, tasks, workers)
            n_found = sum(r[3] for r in res)
            bad = [x for r in res for x in r[4]]
            _write_report(work / "mismatch" / f"sweep={rnd}.parquet", bad,
                          _MISMATCH_SCHEMA)
            if bad:
                raise RuntimeError(f"{year}/{month} sweep {rnd}: "
                                   f"{len(bad):,} hash mismatches")
            if not n_found:
                raise RuntimeError(
                    f"{year}/{month}: {n_need:,} positions have no EPD and no "
                    f"replayable edge reaches them — see {need_dir}")
            src = sorted((work / "new" / f"sweep={rnd}").glob("*.parquet"))
            _run_isolated(_repartition_new,
                          ([str(f) for f in src], str(work / "_tmp_known_sw"),
                           str(known / f"lvl=s{rnd}"), nb, threads, mem,
                           str(tmp)))
            _rmtree(work / "new" / f"sweep={rnd}")
            stats["edges_replayed"] += sum(r[2] for r in res)
            stats["resolved"] += n_found
            stats["replay_secs"] += time.time() - t0
            stats["conflicts"] += sum(len(r[5]) for r in res)
            (work / "_counts.json").write_text(json.dumps(stats),
                                               encoding="utf-8")
            print(f"    sweep {rnd}: {n_need:,} positions short, {n_found:,} "
                  f"resolved ({time.time()-t0:,.0f}s)", flush=True)
            _mark(work, f"sweep={rnd}")

    # 5 ── rows, bucketed exactly as merge Phase A would have
    rows_dir = work / "rows"
    if not _done(work, "rows"):
        sql = f"""
            COPY (SELECT *, {_bucket_expr("parent_hash", nb)} AS bkt
                  FROM read_parquet('{_sql_path(monthly)}'))
            TO '{_sql_path(work / "_tmp_rows")}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (bkt))
        """
        size, secs, _ = _run_isolated(
            _copy_query, (sql, str(work / "_tmp_rows"), str(rows_dir), threads,
                          mem, str(tmp)))
        print(f"    rows bucketed: {size/1e9:,.1f} GB ({secs:,.0f}s)",
              flush=True)
        _mark(work, "rows")

    # 6 ── write + verify
    t0 = time.time()
    out_tmp = out_dir / f"_tmp_month={tag}"
    tasks = []
    for b, fs in sorted(_bucket_files(rows_dir, "bkt=*/*.parquet").items()):
        kf = sorted(known.glob(f"lvl=*/bkt={b}/*.parquet"))
        tasks.append((b, [str(f) for f in fs], [str(f) for f in kf],
                      str(out_tmp / f"bkt={b}" / "part-0000.parquet"),
                      task_threads, per_worker_mem, str(tmp)))
    res = _run_pool(_write_bucket_task, tasks, workers)
    n_rows = sum(r[1] for r in res)
    got = tuple(sum(r[2][j] for r in res) for j in range(len(SUM_COLS)))
    n_null = sum(r[3] for r in res)
    n_bytes = sum(r[4] for r in res)

    con = _duck(threads, mem, tmp)
    try:
        want = tuple(int(x) for x in totals(con, monthly, SUM_COLS))
        in_rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_sql_path(monthly)}')"
        ).fetchone()[0]
        ply1 = con.execute(
            f"SELECT SUM(total)::HUGEINT FROM read_parquet("
            f"'{_sql_path(monthly)}') WHERE ply = 1").fetchone()[0]
    finally:
        con.close()
    if n_null:
        raise RuntimeError(f"{year}/{month}: {n_null:,} output rows still have "
                           f"a NULL parent_epd")
    if n_rows != in_rows:
        raise RuntimeError(f"{year}/{month}: {n_rows:,} rows out != "
                           f"{in_rows:,} in")
    if got != want:
        broken = [f"{c}: in {w:,} != out {g:,}"
                  for c, w, g in zip(SUM_COLS, want, got) if w != g]
        raise RuntimeError(f"{year}/{month}: conservation broken — "
                           f"{'; '.join(broken)}")
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"month={tag}"
    _rmtree(final)
    out_tmp.replace(final)

    secs = time.time() - t_month
    man = {"year": year, "month": month, "files": len(res), "bytes": n_bytes,
           "rows": n_rows, "ply1_games": int(ply1),
           "total": got[0], "white_wins": got[1], "draws": got[2],
           "black_wins": got[3], "buckets": nb,
           "edges_replayed": stats["edges_replayed"],
           "positions_resolved": stats["resolved"],
           "replays_per_sec": stats["edges_replayed"] /
                              max(stats["replay_secs"], 1e-9),
           "mismatches": 0, "conflicts": stats["conflicts"], "unresolved": 0,
           "seconds": secs}
    _write_manifest(out_dir, tag, man)
    for name, src in (("conflicts", work / "conflicts"),
                      ("mismatch", work / "mismatch")):
        if src.exists() and any(src.iterdir()):
            dst = out_dir / f"_{name}" / f"month={tag}"
            _rmtree(dst)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src, dst)
    sentinel.write_text(json.dumps(man, indent=2), encoding="utf-8")
    _rmtree(work)
    print(f"    wrote {len(res)} buckets, {n_rows:,} rows, {n_bytes/1e9:,.1f} GB "
          f"({time.time()-t0:,.0f}s); month {secs/3600:.2f} h, "
          f"{man['replays_per_sec']:,.0f} replays/s", flush=True)
    return man


def _missing_positions(edges: Path, known: Path, out: Path, threads: int,
                       mem: str, tmp: Path, workers: int) -> int:
    """Distinct parents with no EPD yet, written per bucket for the next sweep."""
    _rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    tasks = []
    for b in sorted(_bucket_files(edges, "ply=*/pb=*/*.parquet")):
        fs = sorted(edges.glob(f"ply=*/pb={b}/*.parquet"))
        kf = sorted(known.glob(f"lvl=*/bkt={b}/*.parquet"))
        tasks.append((b, [str(f) for f in fs], [str(f) for f in kf],
                      str(out / f"bkt={b}.parquet"), threads, mem, str(tmp)))
    return sum(_run_pool(_missing_task, tasks, workers))


def _missing_task(task: tuple) -> int:
    b, edge_files, known_files, out_s, threads, mem, tmp_s = task
    con = _duck(threads, mem, Path(tmp_s))
    try:
        src = f"read_parquet({_read_list(edge_files)})"
        if known_files:
            sql = f"""
                SELECT DISTINCT e.parent_hash AS h FROM {src} e
                ANTI JOIN read_parquet({_read_list(known_files)}) k
                  ON e.parent_hash = k.h
            """
        else:
            sql = f"SELECT DISTINCT parent_hash AS h FROM {src}"
        t = con.execute(sql).fetch_arrow_table()
    finally:
        con.close()
    if t.num_rows:
        Path(out_s).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(t, out_s, compression="zstd")
    return t.num_rows


def _write_manifest(out_dir: Path, tag: str, man: dict) -> None:
    p = out_dir / "_manifest" / f"month={tag}.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".parquet.tmp")
    pq.write_table(pa.Table.from_pylist([man]), tmp, compression="zstd")
    tmp.replace(p)


def _mem_gb(mem: str) -> float:
    return _mem_bytes(mem) / 1e9


# ── entry point ───────────────────────────────────────────────────────────────

def discover_months(mdir: Path, want: list[str] | None
                    ) -> list[tuple[int, int, Path]]:
    out = []
    for f in sorted(mdir.glob("*.ps.parquet")):
        m = MONTH_RE.match(f.name)
        if not m:
            continue
        y, mo = int(m.group(1)), int(m.group(2))
        if want and f"{y}_{mo}" not in want:
            continue
        out.append((y, mo, f))
    return sorted(out)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--monthly-dir", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--work-dir", type=Path, default=None,
                    help="Scratch for the edge/known/row splits, ~5x the "
                         "month. Put it on fast local storage — NOT on a USB "
                         "spinning disk. Default <out>/_work.")
    ap.add_argument("--buckets", type=int, default=DEFAULT_BUCKETS)
    ap.add_argument("--months", nargs="*", default=None, metavar="Y_M")
    ap.add_argument("--workers", type=int, default=0,
                    help="Replay processes. Default: logical cores - 1.")
    ap.add_argument("--threads", type=int, default=8,
                    help="DuckDB threads for the whole-month COPY stages.")
    ap.add_argument("--mem", default="16GB",
                    help="DuckDB memory budget. Split across --workers for the "
                         "per-bucket steps, used whole for the COPY stages.")
    ap.add_argument("--tmp-dir", type=Path, default=None)
    ap.add_argument("--fresh", action="store_true",
                    help="Discard any resumable work dir and rebuild.")
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

    months = discover_months(a.monthly_dir, a.months)
    if not months:
        print(f"FATAL: no year=*_month=*.ps.parquet in {a.monthly_dir}")
        return 1
    print(f"monthly  : {a.monthly_dir}")
    print(f"out      : {a.out}")
    print(f"work     : {work_root}   (duckdb temp {tmp})")
    print(f"months   : {len(months)}  buckets {a.buckets}  workers {workers}  "
          f"mem {a.mem}")
    print(f"free     : out {shutil.disk_usage(a.out).free/1e9:,.0f} GB, "
          f"work {shutil.disk_usage(work_root).free/1e9:,.0f} GB\n", flush=True)

    t0 = time.time()
    done = 0
    for y, mo, f in months:
        backfill_month(f, y, mo, a.out, work_root, a.buckets, workers,
                       a.threads, a.mem, tmp, a.fresh)
        done += 1
    print(f"\n{done} months in {(time.time()-t0)/3600:,.2f} h")
    print("Next: ship <out> to the merge host and run build_pooled_stats.py "
          "--phase merge --parts-dir <out>.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
