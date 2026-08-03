"""Winpos crush histogram over a pooled edge universe (task #27 phase 2).

The pool is selected by --stats / --out / --start-year / --end-year; defaults
track the current canonical pool. ALWAYS rebuild every year of a pool in one
uniform run, and never mix partials produced against different --stats files:
winpos_sql's `keys` JOIN is an INNER join that decides which edges get ANY
histogram row at all, so partials built against a smaller edge set under-count
edges present in the larger one — silently, since the output is still valid
parquet. This is not hypothetical: the original 2024/25 partials were keyed to
position_stats_pooled_1900_2200_brc.parquet (4.7M edges) and had to be discarded
when the canonical universe grew, rather than be reused. Re-replaying the
overlapping years is cheap next to getting this wrong.

The .intermediates partials are named after the SOURCE FILE only, so they carry
no record of which --stats they were built against. Treat an .intermediates dir
as belonging to exactly one pool; to retarget, point --out somewhere new (which
moves .intermediates with it) rather than reusing an existing dir.

Per-file worker (fused, mirrors build_pooled_stats.py's extract_file): read a
source parquet in batches, filter mean_elo >= 1800 BEFORE any move parsing (the
dominant speedup — build_pooled_stats.py measured ~75% of games skipped), then
for each surviving game in ONE python-chess replay (to ply<=30, matching the
depth of the canonical position-stats and of the historical position-moves-*-all
dirs): emit pm rows (game_id, ply, parent_hash, move_san, elo_band) AND crush-fact
rows (game_id, white_win_normal, black_win_normal, move_count) — both come from
data already in hand, no second read. These are written to small SCRATCH parquets
(deleted immediately after use, never persisted) and fed straight into
build_crush_winpos.winpos_sql — the EXACT tested query (see _test_winpos.py) —
against a per-worker DuckDB connection preloaded with the canonical `keys` and
`win_w`/`win_b` (unified eval DB, terminal-aware) temp tables. Output: one
resumable, atomically-written .winpos.parquet partial per source file.

Final merge: two-phase external aggregation (partition-by-parent_hash-bucket,
then per-bucket GROUP BY) — the same pattern build_pooled_stats.py's final merges
needed at this row-count scale (imported directly: _duck/_bucket_expr/
_run_copy_query/N_MERGE_BUCKETS), since a monolithic GROUP BY over the combined
7-year partials is expected to face the same ~6B-key wall the position-stats
merge did.

Output: --out (default crush_hist_relwin_pooled_ge1800_2013_2025_brc.parquet),
plus <out-stem>.intermediates/ (resumable per-source-file partials) and
<out-stem>_scratch/ (deleted on completion).

Usage:
    # one month against the default pool — prints the per-file rate, no merge
    .venv/Scripts/python.exe python/build_crush_winpos_phase2.py --smoke-test 2019 1

    # full run against the defaults
    .venv/Scripts/python.exe python/build_crush_winpos_phase2.py --workers 10 --threads 1

    # explicit pool (what the defaults above expand to)
    .venv/Scripts/python.exe python/build_crush_winpos_phase2.py \
        --start-year 2013 --end-year 2025 \
        --stats E:/chess/position-stats/position_stats_pooled_ge1800_2013_2025_brc.parquet \
        --out   E:/chess/position-stats/crush_hist_relwin_pooled_ge1800_2013_2025_brc.parquet
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import chess
import polars as pl
import pyarrow.parquet as pq

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).parent))
from stage1_extract_positions import iter_san_moves, zobrist_int64
from zobrist import IncrementalZobrist
from build_crush_winpos import winpos_sql
from build_pooled_stats import (discover_source_files, _duck, _sql_path,
                                _bucket_expr, _run_copy_query, N_MERGE_BUCKETS)

SD = Path("E:/chess/position-stats")
DEFAULT_STATS = SD / "position_stats_pooled_ge1800_2013_2025_brc.parquet"
DEFAULT_OUT = SD / "crush_hist_relwin_pooled_ge1800_2013_2025_brc.parquet"
DEFAULT_EVAL_DB = Path("E:/chess/unified_eval_db.parquet")
DEFAULT_START_YEAR, DEFAULT_END_YEAR = 2013, 2025
# Derived from --out in main(), and re-derived inside every worker by
# _init_worker: on Windows the pool spawns, so workers re-import this module and
# see these defaults — an assignment made in main() does NOT reach them.
INTER = DEFAULT_OUT.with_name(DEFAULT_OUT.stem + ".intermediates")
SCRATCH = DEFAULT_OUT.with_name(DEFAULT_OUT.stem + "_scratch")
TMP_DIR = Path("D:/chess_duckdb_tmp")
MIN_ELO = 1800
MAX_PLY = 30
EVENTS = ["Blitz", "Rapid", "Classical"]
SRC_COLUMNS = ["game_id", "movetext", "white_score", "termination", "move_count",
              "mean_elo", "elo_band"]
READ_BATCH_GAMES = 50_000
THRESH_CP = 300


# ── per-file fused extraction + winpos aggregation ──────────────────────────

def _walk_game_winpos(pm_buf: dict, game_id, movetext, elo_band, max_ply: int,
                      hasher=None) -> bool:
    """Same replay shape as build_pooled_stats._walk_game, but no crush-bucket
    computation here — that happens in winpos_sql from the crush-fact table.

    `hasher` is an optional reused IncrementalZobrist (see build_pooled_stats.
    _walk_game); omitting it reproduces the original full-rescan path exactly.
    No EPD memo here — this walk never materializes EPDs.
    """
    if not movetext:
        return False
    toks = list(iter_san_moves(movetext))
    if not toks:
        return False
    maxply = min(max_ply, len(toks))
    board = chess.Board()
    if hasher is None:
        get_hash = lambda: zobrist_int64(board)
        push = board.push
    else:
        hasher.reset(board)
        get_hash = lambda: hasher.current(board)
        push = lambda mv: hasher.push_move(board, mv)
    for ply in range(1, maxply + 1):
        san = toks[ply - 1]
        ph = get_hash()
        try:
            move = board.parse_san(san)
        except (ValueError, AssertionError):
            return True
        pm_buf["game_id"].append(game_id)
        pm_buf["ply"].append(ply)
        pm_buf["parent_hash"].append(ph)
        pm_buf["move_san"].append(san)
        pm_buf["elo_band"].append(elo_band)
        push(move)
    return False


_CON = None


def _init_worker(threads: int, keys_pq: str, winw_pq: str, winb_pq: str,
                 inter: str, scratch: str) -> None:
    import duckdb
    global _CON, INTER, SCRATCH
    # Spawned workers re-imported this module and hold the module-level defaults;
    # take the run's real directories from initargs (same reason keys/win_* are
    # passed rather than recomputed).
    INTER, SCRATCH = Path(inter), Path(scratch)
    _CON = duckdb.connect()
    _CON.execute(f"SET threads={threads};")
    _CON.execute("SET enable_progress_bar=false;")
    _CON.execute("SET preserve_insertion_order=false;")
    for name, pq_path in (("keys", keys_pq), ("win_w", winw_pq), ("win_b", winb_pq)):
        _CON.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM "
                     f"read_parquet('{_sql_path(Path(pq_path))}')")


def _process_file(task: tuple) -> str:
    src_str, label = task
    src = Path(src_str)
    partial = INTER / f"{label}.winpos.parquet"
    if partial.exists():
        return f"SKIP {label}"

    t0 = time.time()
    pm_buf = {"game_id": [], "ply": [], "parent_hash": [], "move_san": [], "elo_band": []}
    crush_rows = {"game_id": [], "white_win_normal": [], "black_win_normal": [],
                 "move_count": []}
    n_games = n_kept = 0
    hasher = IncrementalZobrist(chess.Board())   # reused across every game in the file
    pf = pq.ParquetFile(src)
    for batch in pf.iter_batches(batch_size=READ_BATCH_GAMES, columns=SRC_COLUMNS):
        for rec in batch.to_pylist():
            n_games += 1
            me = rec["mean_elo"]
            if me is None or me < MIN_ELO:
                continue
            ws = rec["white_score"]
            if ws is None:
                continue
            n_kept += 1
            term = rec["termination"]
            normal = term == "Normal"
            gid = rec["game_id"]
            crush_rows["game_id"].append(gid)
            crush_rows["white_win_normal"].append(1 if (normal and ws == 1.0) else 0)
            crush_rows["black_win_normal"].append(1 if (normal and ws == 0.0) else 0)
            crush_rows["move_count"].append(rec["move_count"])
            _walk_game_winpos(pm_buf, gid, rec["movetext"], rec["elo_band"], MAX_PLY,
                              hasher)

    SCRATCH.mkdir(parents=True, exist_ok=True)
    pm_tmp = SCRATCH / f"{label}.pm.parquet"
    crush_tmp = SCRATCH / f"{label}.crush.parquet"
    try:
        pl.DataFrame(pm_buf, schema={
            "game_id": pl.Utf8, "ply": pl.Int32, "parent_hash": pl.Int64,
            "move_san": pl.Utf8, "elo_band": pl.Int64,
        }).write_parquet(pm_tmp)
        pl.DataFrame(crush_rows, schema={
            "game_id": pl.Utf8, "white_win_normal": pl.Int32,
            "black_win_normal": pl.Int32, "move_count": pl.Int32,
        }).write_parquet(crush_tmp)

        ptmp = partial.with_suffix(".parquet.tmp")
        ptmp.unlink(missing_ok=True)
        _CON.execute(f"COPY ({winpos_sql(str(pm_tmp), str(crush_tmp))}) TO "
                     f"'{_sql_path(ptmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        ptmp.replace(partial)
    finally:
        pm_tmp.unlink(missing_ok=True)
        crush_tmp.unlink(missing_ok=True)
    return (f"{label}: {n_games:,} games ({n_kept:,} kept) -> "
            f"{partial.stat().st_size/1e6:.1f} MB ({time.time()-t0:.0f}s)")


# ── final two-phase merge ───────────────────────────────────────────────────

def final_merge(out: Path, inter: Path, threads: int, mem: str) -> None:
    if out.exists():
        print(f"SKIP final merge: {out.name} exists", flush=True)
        return
    bdir = out.with_name(out.stem + "_buckets")
    bdir.mkdir(parents=True, exist_ok=True)
    glob = _sql_path(inter) + "/*.winpos.parquet"
    tasks = []
    for i in range(N_MERGE_BUCKETS):
        bout = bdir / f"bucket_{i}.parquet"
        if bout.exists():
            continue
        sql = f"""
            COPY (
                SELECT 'Pooled' AS event, 0 AS elo_band,
                       parent_hash, move_san, move_bucket,
                       SUM(n)::BIGINT AS n, SUM(white_wins)::BIGINT AS white_wins,
                       SUM(black_wins)::BIGINT AS black_wins
                FROM read_parquet('{glob}')
                WHERE {_bucket_expr("parent_hash")} = {i}
                GROUP BY parent_hash, move_san, move_bucket
            ) TO '{_sql_path(bout.with_suffix(".parquet.tmp"))}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        tasks.append((sql, str(bout.with_suffix(".parquet.tmp")), str(bout),
                      threads, mem, str(TMP_DIR), f"winpos bucket {i}"))
    print(f"Final merge: {N_MERGE_BUCKETS} hash buckets, {len(tasks)} to build...",
          flush=True)
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
        for fut in as_completed([ex.submit(_run_copy_query, t) for t in tasks]):
            label, size, secs = fut.result()
            print(f"  {label}: {size/1e6:.0f} MB ({secs/60:.1f} min)", flush=True)
    out_tmp = out.with_suffix(".parquet.tmp")
    out_tmp.unlink(missing_ok=True)
    con = _duck(threads, mem, TMP_DIR)
    try:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{_sql_path(bdir)}/bucket_*.parquet'))
            TO '{_sql_path(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    out_tmp.replace(out)
    shutil.rmtree(bdir, ignore_errors=True)
    print(f"Wrote {out.name} ({out.stat().st_size/1e6:.0f} MB)", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--threads", type=int, default=1,
                    help="DuckDB threads PER WORKER for the small per-file winpos query.")
    ap.add_argument("--merge-threads", type=int, default=8)
    ap.add_argument("--merge-mem", default="45GB")
    ap.add_argument("--smoke-test", nargs=2, metavar=("YEAR", "MONTH"), type=int,
                    default=None, help="Restrict to one (year, month) for a quick test.")
    ap.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    ap.add_argument("--end-year", type=int, default=DEFAULT_END_YEAR)
    ap.add_argument("--stats", default=str(DEFAULT_STATS),
                    help=f"Pooled position-stats parquet whose (parent_hash, move_san) "
                         f"pairs form the `keys` universe. This is an INNER join: an "
                         f"edge absent here gets NO histogram row, so pointing this at "
                         f"a different pool than --out silently under-covers rather "
                         f"than failing (default: {DEFAULT_STATS.name}).")
    ap.add_argument("--out", default=str(DEFAULT_OUT),
                    help=f"Output histogram; its stem also names the .intermediates "
                         f"and _scratch dirs (default: {DEFAULT_OUT.name}).")
    ap.add_argument("--eval-db", default=str(DEFAULT_EVAL_DB),
                    help=f"Stockfish eval DB for the >= +{THRESH_CP}cp win event "
                         f"(default: {DEFAULT_EVAL_DB.name}).")
    args = ap.parse_args()

    global INTER, SCRATCH
    stats, out, eval_db = Path(args.stats), Path(args.out), Path(args.eval_db)
    INTER = out.with_name(out.stem + ".intermediates")
    SCRATCH = out.with_name(out.stem + "_scratch")
    for label, p in (("--stats", stats), ("--eval-db", eval_db)):
        if not p.exists():
            sys.exit(f"{label} not found: {p}")
    # Log the resolved pool: partials are keyed only by source file, so a run
    # aimed at the wrong --stats leaves no trace in the filenames.
    print(f"stats:   {stats.name}\nout:     {out.name}\neval-db: {eval_db.name}\n"
          f"years:   {args.start_year}-{args.end_year}  ply<={MAX_PLY}  "
          f"elo>={MIN_ELO}  thresh={THRESH_CP}cp\n", flush=True)

    INTER.mkdir(parents=True, exist_ok=True)
    prep = INTER / "_prep"
    prep.mkdir(exist_ok=True)
    keys_pq, winw_pq, winb_pq = (prep / "keys.parquet", prep / f"win_w_{THRESH_CP}.parquet",
                                 prep / f"win_b_{THRESH_CP}.parquet")
    import duckdb
    con = duckdb.connect()
    con.execute("SET threads=4; SET enable_progress_bar=false;")
    for out_pq, sql in (
        (keys_pq, f"SELECT DISTINCT parent_hash, move_san FROM read_parquet('{_sql_path(stats)}')"),
        (winw_pq, f"SELECT position_hash FROM read_parquet('{_sql_path(eval_db)}') "
                  f"WHERE eval_cp >= {THRESH_CP}"),
        (winb_pq, f"SELECT position_hash FROM read_parquet('{_sql_path(eval_db)}') "
                  f"WHERE eval_cp <= -{THRESH_CP}"),
    ):
        if not out_pq.exists():
            tmp = out_pq.with_suffix(".parquet.tmp")
            con.execute(f"COPY ({sql}) TO '{_sql_path(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            tmp.replace(out_pq)
        n = con.execute(f"SELECT count(*) FROM read_parquet('{_sql_path(out_pq)}')").fetchone()[0]
        print(f"  prep {out_pq.name}: {n:,} rows", flush=True)
    con.close()

    if args.smoke_test:
        y, mo = args.smoke_test
        files = discover_source_files(y, y, [mo], EVENTS)
    else:
        files = discover_source_files(args.start_year, args.end_year, None, EVENTS)
    tasks = [(str(f), f"year={y}_month={mo}_event={ev}_{f.stem}")
            for (f, y, mo, ev) in files]
    todo = [t for t in tasks if not (INTER / f"{t[1]}.winpos.parquet").exists()]
    print(f"{len(tasks)} source files; {len(todo)} remaining; "
          f"{args.workers} workers x {args.threads} threads\n", flush=True)

    t_all = time.time()
    done = 0
    if todo:
        with ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker,
                                 initargs=(args.threads, str(keys_pq), str(winw_pq),
                                           str(winb_pq), str(INTER), str(SCRATCH))) as ex:
            futs = [ex.submit(_process_file, t) for t in todo]
            for fut in as_completed(futs):
                done += 1
                if done % 5 == 0 or done == len(todo):
                    print(f"  [{done}/{len(todo)}] {fut.result()}", flush=True)
    print(f"\nAll {done}/{len(todo)} partials done in {(time.time()-t_all)/60:.1f} min.",
          flush=True)
    shutil.rmtree(SCRATCH, ignore_errors=True)

    if not args.smoke_test:
        final_merge(out, INTER, args.merge_threads, args.merge_mem)
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
