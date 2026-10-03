"""
pool_from_book.py — the Stage-3 pool and its aux sidecar, built from the banded
explorer book instead of from a fresh extract.

Spec: Chess Blog Posts/docs/pool-from-book-spec.md (2026-10-03). The book's read
contract is <book>/README.md.

Outputs, both drop-ins for what Stage 3 and budget_core read today (same schemas,
a different population):

  position_stats_pooled_<tag>.parquet       edges (parent_hash, move_san), summed
                                            over the chosen slices at ply <= --max-ply,
                                            kept when SUM(total) >= --min-games.
                                            event='Pooled', elo_band=0, as before.
  position_stats_aux_pooled_<tag>.parquet   one row per pool parent: other_* (the
                                            below-floor moves, evals looked up by
                                            child_hash), term_other_* and horizon_*
                                            (derived exactly from the move flows).

plus a <file>.meta.json beside each, and <work>/_POOL.DONE last.

Why the term/horizon buckets are DERIVED and not read: the book's term table is
pooled over every event and elo band, so it describes a different population.
Ply is in the book's key, so the flows answer exactly, slice by slice:

    A(x, p) = games that reached x on their p-th move    (rows by child_hash at ply p)
    D(x, q) = games that played their q-th move from x   (rows by parent_hash at ply q)
    ended(x, p) = A(x, p) - D(x, p+1)        1 <= p <= max_ply
    horizon(x)  = D(x, max_ply + 1)

The book's cap (30) is above ours, so the ply max_ply+1 rows exist and the split
at the cap is exact. A game whose movetext failed to parse after ply p shows up as
ended at its last parsed position (the old extract counted it nowhere). Games with
no moves never touch a move row, so the root's end_ply-0 mass is not derivable.

The termination REASON is not in the move rows, so the whole ended mass goes to
term_other_*; term_normal_* and term_flag_* are 0 and the aux meta says
term_reasons='pooled'. Stage 3 and budget_core refuse to exclude time-forfeit
flags on such a sidecar (excluding them would silently drop nothing).

Collision hashes (<book>/_collisions.parquet) are dropped as pool PARENTS, like
the eval arrays and Stage 3's hash-keyed dicts must; they stay in D and A summed
over their EPDs, so the flow identities still balance. No twin can reach the
floor book-wide; that is asserted, not assumed.

Phases, each resumable (atomic .tmp -> rename, sentinel per unit):
  arrivals   rows at ply <= max_ply, pre-summed by (child_hash, ply) within groups
             of source buckets and partitioned by the CHILD's bucket, so each
             output bucket can read its own arrivals.  <work>/arrivals/gNNN/cb=i/
  buckets    per output bucket, in a fresh process (DuckDB decays across big
             GROUP BYs in one process): edges, survivors, below-floor rows with
             batched eval lookups, D, ended, horizon, the aux rows. Any negative
             ended(x, p) component raises.                <work>/buckets/bNNN.*
  finalize   concatenate, sort, schema-check, write the two outputs and their
             metas, then <work>/_POOL.DONE.

Every parameter is on the command line and locked in <work>/_params.json: a re-run
with different values refuses rather than mixing populations in one work dir.

Usage (pin a copy under D:/chess/bin first; never run long jobs from a worktree):
  python pool_from_book.py --events Blitz Rapid Classical --elo-bands 2000 2200 2500 \\
      --max-ply 20 --min-games 50 --tag ge2000_2013_2026_brc_p20 \\
      --work H:/chess/pool_work/ge2000_2013_2026_brc_p20 --workers 3 --threads 4 --mem 8GB

  # a subset of buckets (smoke / gates); finalize refuses until all 512 exist
  python pool_from_book.py ... --phase buckets --buckets 0 156 511
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BOOK = Path("E:/chess/position-stats/explorer_banded_2013_2026")
STATS_DIR = Path("E:/chess/position-stats")
EVAL_ARRAYS = Path("D:/chess/eval_arrays_full")
TMP_DIR = Path("D:/chess_duckdb_tmp")
N_BUCKETS = 512            # the book's partitioning: rem_euclid(parent_hash, 512)
GROUP_SIZE = 16            # source buckets per arrivals-partitioning process
POOL_EVENT = "Pooled"
POOL_ELO = 0
SUMS = ("white_wins", "draws", "black_wins", "total")
PRODUCER = "pool_from_book"

# The old pool/aux files' arrow schemas, which these outputs must match exactly
# (large_string included: Stage 3 and sixteen other readers never see a difference).
POOL_SCHEMA = [("parent_hash", "int64"), ("move_san", "large_string"),
               ("parent_epd", "large_string"), ("child_hash", "int64"),
               ("ply", "int32"), ("white_wins", "int64"), ("draws", "int64"),
               ("black_wins", "int64"), ("total", "int64"),
               ("event", "large_string"), ("elo_band", "int64"),
               ("white_score_avg", "double")]
_GROUPS = ("term_normal", "term_flag", "term_other", "horizon")
AUX_SCHEMA = ([("position_hash", "int64")]
              + [(f"{g}_{s}", "int64") for g in _GROUPS
                 for s in ("total", "white_wins", "draws", "black_wins")]
              + [("other_total", "int64"), ("other_white_wins", "int64"),
                 ("other_draws", "int64"), ("other_black_wins", "int64"),
                 ("other_edges", "int32"), ("other_eval_mean", "double"),
                 ("other_eval_min", "double"), ("other_eval_max", "double"),
                 ("other_eval_cov", "double")])


def bucket_of(col: str, n: int = N_BUCKETS) -> str:
    """rem_euclid(col, n) in SQL: the book's bucket rule."""
    return f"((({col}) % {n}) + {n}) % {n}"


def _p(path) -> str:
    """Forward-slashed path for a DuckDB SQL literal."""
    return str(path).replace("\\", "/")


def _duck(threads: int, mem: str, tmp: Path):
    """A connection spilling into its OWN subdirectory of `tmp`. DuckDB's spill
    file names are not unique per process, so concurrent processes sharing one
    temp_directory can read each other's blocks: gate 1 (2 workers spilling
    beside the 4-worker build) died on a per-(position, ply) SUM(total) of 5.9e31,
    which a fresh, unshared rerun of the same query does not reproduce."""
    import atexit
    import duckdb
    tmp = Path(tmp) / f"pid{os.getpid()}"
    tmp.mkdir(parents=True, exist_ok=True)
    atexit.register(shutil.rmtree, tmp, True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{_p(tmp)}'")
    con.execute(f"SET threads={int(threads)}")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, obj) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def slice_files(book: Path, events, bands, i: int) -> list[str]:
    """The book files of bucket i for the chosen slices. A slice with no row in a
    bucket has no file there (README: 'one file per (slice, bucket) with a row')."""
    out = []
    for e in events:
        for b in bands:
            f = book / "ps" / f"event={e}" / f"elo_band={b}" / f"bkt{i:03d}.parquet"
            if f.exists():
                out.append(_p(f))
    return out


_BOOK_TYPES = {"parent_hash": "BIGINT", "parent_epd": "VARCHAR", "move_san": "VARCHAR",
               "child_hash": "BIGINT", "ply": "INTEGER", "white_wins": "BIGINT",
               "draws": "BIGINT", "black_wins": "BIGINT", "total": "BIGINT"}


def _read(files: list[str], cols: str, where: str) -> str:
    if not files:
        # A bucket no chosen slice reaches (small populations): an empty, typed
        # relation, so every later step runs unchanged and writes empty outputs.
        return "(SELECT " + ", ".join(f"NULL::{_BOOK_TYPES[c.strip()]} AS {c.strip()}"
                                      for c in cols.split(",")) + " WHERE FALSE)"
    # hive_partitioning off: event/elo_band are in-file columns too (README).
    lst = ", ".join(f"'{f}'" for f in files)
    return (f"(SELECT {cols} FROM read_parquet([{lst}], hive_partitioning=false) "
            f"WHERE {where})")


# ── phase 1: arrivals ──────────────────────────────────────────────────────────

def arrivals_dir(work: Path) -> Path:
    return work / "arrivals"


# The arrivals depend on the population and the cap, never on the floor or the tag,
# so a floor variant may read another work dir's partition (--arrivals-from).
ARRIVALS_KEYS = ("book", "events", "elo_bands", "max_ply")


def resolve_arrivals(a) -> Path:
    if not a.arrivals_from:
        return arrivals_dir(Path(a.work))
    src = Path(a.arrivals_from)
    p = json.loads((src / "_params.json").read_text(encoding="utf-8"))
    mine = json.loads((Path(a.work) / "_params.json").read_text(encoding="utf-8"))
    drift = [k for k in ARRIVALS_KEYS if p.get(k) != mine.get(k)]
    if drift:
        sys.exit(f"FATAL: --arrivals-from {src} has a different {drift}; its arrivals "
                 f"describe another population")
    missing = [g for g in range(N_BUCKETS // GROUP_SIZE)
               if not (arrivals_dir(src) / f"g{g:03d}").exists()]
    if missing:
        sys.exit(f"FATAL: --arrivals-from {src} is incomplete (groups {missing[:5]}...)")
    return arrivals_dir(src)


def _arrivals_group(task: tuple) -> tuple:
    """One group of source buckets: their rows at ply <= max_ply, summed by
    (child_hash, ply), COPYed partitioned by the child's bucket. Fresh process."""
    (book, events, bands, src_buckets, max_ply, child_buckets, out_tmp, out,
     threads, mem, tmp) = task
    out_tmp, out = Path(out_tmp), Path(out)
    if out_tmp.exists():
        shutil.rmtree(out_tmp)
    out_tmp.parent.mkdir(parents=True, exist_ok=True)
    files = [f for i in src_buckets for f in slice_files(Path(book), events, bands, i)]
    t0 = time.time()
    if not files:
        out_tmp.mkdir(parents=True)
        out_tmp.replace(out)
        return out.name, 0, time.time() - t0
    where = f"ply <= {int(max_ply)}"
    if child_buckets is not None:
        where += f" AND {bucket_of('child_hash')} IN ({', '.join(map(str, child_buckets))})"
    s = ", ".join(f"SUM({c})::BIGINT AS {c}" for c in SUMS)
    con = _duck(threads, mem, Path(tmp))
    try:
        n = con.execute(f"""
            COPY (
                SELECT {bucket_of('child_hash')} AS cb, child_hash, ply::INTEGER AS ply, {s}
                FROM {_read(files, 'child_hash, ply, ' + ', '.join(SUMS), where)}
                GROUP BY child_hash, ply
            ) TO '{_p(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (cb))
        """).fetchone()[0]
    finally:
        con.close()
    if not out_tmp.exists():          # zero rows: COPY ... PARTITION_BY writes nothing
        out_tmp.mkdir(parents=True)
    out_tmp.replace(out)
    return out.name, int(n), time.time() - t0


def phase_arrivals(a, child_buckets: list[int] | None = None,
                   adir: Path | None = None) -> None:
    adir = adir or arrivals_dir(Path(a.work))
    adir.mkdir(parents=True, exist_ok=True)
    tasks = []
    for g in range(0, N_BUCKETS, GROUP_SIZE):
        out = adir / f"g{g // GROUP_SIZE:03d}"
        if out.exists():
            continue
        tasks.append((str(a.book), a.events, a.elo_bands,
                      list(range(g, g + GROUP_SIZE)), a.max_ply, child_buckets,
                      str(adir / f"_tmp_g{g // GROUP_SIZE:03d}"), str(out),
                      a.threads, a.mem, str(a.tmp_dir)))
    n_groups = N_BUCKETS // GROUP_SIZE
    print(f"arrivals: {n_groups} groups of {GROUP_SIZE} source buckets, "
          f"{len(tasks)} to build -> {adir}", flush=True)
    _pool_run(_arrivals_group, tasks, a.workers,
              lambda r: f"  {r[0]}: {r[1]:,} (child, ply) rows ({r[2]:.0f}s)")


def _pool_run(fn, tasks, workers: int, fmt) -> list:
    """Every task in a FRESH process (max_tasks_per_child=1): a long-lived DuckDB
    connection decays 5-20x across big GROUP BYs (CLAUDE.md)."""
    out = []
    if not tasks:
        return out
    with ProcessPoolExecutor(max_workers=max(1, workers), max_tasks_per_child=1) as ex:
        futs = [ex.submit(fn, t) for t in tasks]
        done = 0
        for fut in as_completed(futs):
            r = fut.result()          # a failed task raises here and stops the run
            done += 1
            out.append(r)
            print(f"[{done}/{len(tasks)}] " + fmt(r), flush=True)
    return out


# ── phase 2: per output bucket ─────────────────────────────────────────────────

def bucket_paths(work: Path, i: int) -> dict[str, Path]:
    d = work / "buckets"
    return {"edges": d / f"b{i:03d}.edges.parquet",
            "aux": d / f"b{i:03d}.aux.parquet",
            "stats": d / f"b{i:03d}.stats.json"}   # renamed LAST: the sentinel


def derive_flows(con, files: list[str], arrivals_glob: str | None, max_ply: int) -> None:
    """Create TEMP tables d (departures), a (arrivals) and en (ended) on `con`.

    d(x, q): parent_hash x, ply q <= max_ply+1, summed over every EPD and move.
    a(x, p): child_hash x, ply p <= max_ply, from the arrivals partition.
    en(x, p) = a(x, p) - d(x, p+1), 1 <= p <= max_ply, on the full outer join.
    Shared by the builder and by gate 1 (which runs it on all 54 slices against
    the book's own term table)."""
    s = ", ".join(f"SUM({c})::BIGINT AS {c}" for c in SUMS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE d AS
        SELECT parent_hash AS x, ply::INTEGER AS q, {s}
        FROM {_read(files, 'parent_hash, ply, ' + ', '.join(SUMS), f'ply <= {max_ply + 1}')}
        GROUP BY parent_hash, ply
    """)
    if arrivals_glob is None:
        con.execute(f"CREATE OR REPLACE TEMP TABLE a AS SELECT 0::BIGINT AS x, "
                    f"0::INTEGER AS p, {', '.join(f'0::BIGINT AS {c}' for c in SUMS)} "
                    f"WHERE FALSE")
    else:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE a AS
            SELECT child_hash AS x, ply::INTEGER AS p, {s}
            FROM read_parquet('{arrivals_glob}', hive_partitioning=false)
            GROUP BY child_hash, ply
        """)
    diff = ", ".join(f"(COALESCE(a.{c}, 0) - COALESCE(d.{c}, 0))::BIGINT AS {c}"
                     for c in SUMS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE en AS
        SELECT COALESCE(a.x, d.x) AS x, COALESCE(a.p, d.q - 1)::INTEGER AS p, {diff}
        FROM a FULL OUTER JOIN (SELECT * FROM d WHERE q BETWEEN 2 AND {max_ply + 1}) d
          ON d.x = a.x AND d.q = a.p + 1
    """)


def check_ended_nonnegative(con, label: str) -> None:
    neg = " OR ".join(f"{c} < 0" for c in SUMS)
    n = con.execute(f"SELECT COUNT(*) FROM en WHERE {neg}").fetchone()[0]
    if n:
        rows = con.execute(f"SELECT * FROM en WHERE {neg} LIMIT 10").fetchall()
        raise RuntimeError(f"{label}: {n:,} (position, ply) cells with a NEGATIVE "
                           f"ended component — arrivals < departures is impossible, "
                           f"so the flows are wrong. Sample: {rows}")


def _bucket_task(task: tuple) -> tuple:
    (i, book, events, bands, max_ply, min_games, eval_arrays, work, adir, coll_list,
     root_hash, threads, mem, tmp) = task
    t0 = time.time()
    from eval_arrays import MISSING, lookup_evals, open_eval_arrays
    # Imported, never redefined: the cp -> expected-score curve must be the one
    # Stage 3 blends evals with (build_pooled_stats does the same).
    from stage3_backwards_induction import LICHESS_CP_SCALE
    import pyarrow as pa

    work = Path(work)
    paths = bucket_paths(work, i)
    tmps = {k: v.with_name(v.name + ".tmp") for k, v in paths.items()}
    for v in tmps.values():
        v.unlink(missing_ok=True)
    files = slice_files(Path(book), events, bands, i)
    adir = Path(adir)
    agl = adir / "g*" / f"cb={i}" / "*.parquet"
    has_arr = any(adir.glob(f"g*/cb={i}/*.parquet"))
    st: dict = {"bucket": i, "files": len(files)}
    con = _duck(threads, mem, Path(tmp))
    try:
        con.execute("CREATE TEMP TABLE coll (h BIGINT)")
        if coll_list:
            con.executemany("INSERT INTO coll VALUES (?)", [(int(h),) for h in coll_list])
        s = ", ".join(f"SUM({c})::BIGINT AS {c}" for c in SUMS)
        # Edges over the pool's plies: ply leaves the key (MIN kept), EPD and child
        # are carried with MIN and MAX so single-valuedness can be ASSERTED.
        con.execute(f"""
            CREATE TEMP TABLE e AS
            SELECT parent_hash, move_san,
                   MIN(parent_epd) AS parent_epd, MAX(parent_epd) AS epd_max,
                   MIN(child_hash) AS child_hash, MAX(child_hash) AS child_max,
                   MIN(ply)::INTEGER AS ply, {s}
            FROM {_read(files, 'parent_hash, parent_epd, move_san, child_hash, ply, '
                        + ', '.join(SUMS), f'ply <= {max_ply}')}
            GROUP BY parent_hash, move_san
        """)
        con.execute("ALTER TABLE e ADD COLUMN is_coll BOOLEAN")
        con.execute("UPDATE e SET is_coll = parent_hash IN (SELECT h FROM coll)")
        (st["edges_all"], st["mass_all"], st["edges_prefloor"], st["mass_prefloor"],
         st["coll_edges"], st["coll_mass"], st["coll_parents"], st["coll_surviving"],
         st["multi_valued"]) = [int(v or 0) for v in con.execute(f"""
            SELECT COUNT(*), SUM(total),
                   COUNT(*) FILTER (WHERE NOT is_coll), SUM(total) FILTER (WHERE NOT is_coll),
                   COUNT(*) FILTER (WHERE is_coll), SUM(total) FILTER (WHERE is_coll),
                   COUNT(DISTINCT parent_hash) FILTER (WHERE is_coll),
                   COUNT(*) FILTER (WHERE is_coll AND total >= {min_games}),
                   COUNT(*) FILTER (WHERE NOT is_coll AND (parent_epd <> epd_max
                                                           OR child_hash <> child_max))
            FROM e""").fetchone()]
        if st["multi_valued"]:
            bad = con.execute("SELECT parent_hash, move_san, parent_epd, epd_max, child_hash, "
                              "child_max FROM e WHERE NOT is_coll AND (parent_epd <> epd_max "
                              "OR child_hash <> child_max) LIMIT 5").fetchall()
            raise RuntimeError(f"bucket {i}: {st['multi_valued']} non-collision edges with "
                               f"more than one parent_epd/child_hash: {bad}")
        if st["coll_surviving"]:
            raise RuntimeError(f"bucket {i}: {st['coll_surviving']} collision-parent edges "
                               f"reach the {min_games}-game floor; dropping them would lose "
                               f"real pool edges — the spec's premise is broken")

        # Survivors.
        con.execute(f"""
            COPY (SELECT parent_hash, move_san, parent_epd, child_hash, ply,
                         white_wins, draws, black_wins, total
                  FROM e WHERE NOT is_coll AND total >= {min_games})
            TO '{_p(tmps['edges'])}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Below-floor children's evals, one batched lookup for the bucket.
        ch = con.execute(f"SELECT DISTINCT child_hash FROM e "
                         f"WHERE NOT is_coll AND total < {min_games}").fetchnumpy()["child_hash"]
        ch = np.asarray(ch, dtype=np.int64)
        t_ev = time.time()
        mm_h, mm_c = open_eval_arrays(Path(eval_arrays))
        cp = lookup_evals(ch, mm_h, mm_c)
        st["eval_lookup_s"] = round(time.time() - t_ev, 1)
        hit = cp != MISSING                       # MISSING is masked, never a value
        st["other_children"] = int(ch.shape[0])
        st["other_children_covered"] = int(hit.sum())
        ev = pa.table({"child_hash": pa.array(ch[hit], pa.int64()),
                       "cp": pa.array(cp[hit].astype(np.int32), pa.int32())})
        con.register("ev_arrow", ev)
        con.execute("CREATE TEMP TABLE ev AS SELECT * FROM ev_arrow")
        con.unregister("ev_arrow")
        # Per-edge expected score FIRST, then the games-weighted mean: a mean of
        # cp converted afterwards is a different, wrong quantity.
        es = f"(1.0 / (1.0 + exp(-{LICHESS_CP_SCALE} * ev.cp::DOUBLE)))"
        con.execute(f"""
            CREATE TEMP TABLE o AS
            SELECT e.parent_hash AS position_hash,
                   SUM(e.total)::BIGINT       AS other_total,
                   SUM(e.white_wins)::BIGINT  AS other_white_wins,
                   SUM(e.draws)::BIGINT       AS other_draws,
                   SUM(e.black_wins)::BIGINT  AS other_black_wins,
                   COUNT(*)::INTEGER          AS other_edges,
                   SUM(CASE WHEN ev.cp IS NULL THEN 0 ELSE e.total * {es} END)
                     / NULLIF(SUM(CASE WHEN ev.cp IS NULL THEN 0 ELSE e.total END), 0)
                                              AS other_eval_mean,
                   MIN({es})                  AS other_eval_min,
                   MAX({es})                  AS other_eval_max,
                   SUM(CASE WHEN ev.cp IS NULL THEN 0 ELSE e.total END)
                     / NULLIF(SUM(e.total), 0)::DOUBLE AS other_eval_cov
            FROM e LEFT JOIN ev ON ev.child_hash = e.child_hash
            WHERE NOT e.is_coll AND e.total < {min_games}
            GROUP BY e.parent_hash
        """)

        # Flows: departures, arrivals, ended. The gate is part of the build.
        derive_flows(con, files, _p(agl) if has_arr else None, max_ply)
        check_ended_nonnegative(con, f"bucket {i}")

        con.execute(f"""
            CREATE TEMP TABLE keys AS
            SELECT DISTINCT parent_hash AS position_hash FROM e
            WHERE NOT is_coll AND total >= {min_games}
        """)
        tcols = ", ".join(f"SUM({c})::BIGINT AS {c}" for c in SUMS)
        zero = lambda g: ", ".join(f"0::BIGINT AS {g}_{c}"
                                   for c in ("total", "white_wins", "draws", "black_wins"))
        tsel = lambda g, al: ", ".join(f"COALESCE({al}.{c}, 0)::BIGINT AS {g}_{c}"
                                       for c in ("total", "white_wins", "draws", "black_wins"))
        con.execute(f"""
            COPY (
                WITH t AS (SELECT x, {tcols} FROM en GROUP BY x),
                     hz AS (SELECT x, {', '.join(SUMS)} FROM d WHERE q = {max_ply + 1})
                SELECT k.position_hash,
                       {zero('term_normal')}, {zero('term_flag')},
                       {tsel('term_other', 't')}, {tsel('horizon', 'hz')},
                       COALESCE(o.other_total, 0)::BIGINT      AS other_total,
                       COALESCE(o.other_white_wins, 0)::BIGINT AS other_white_wins,
                       COALESCE(o.other_draws, 0)::BIGINT      AS other_draws,
                       COALESCE(o.other_black_wins, 0)::BIGINT AS other_black_wins,
                       COALESCE(o.other_edges, 0)::INTEGER     AS other_edges,
                       o.other_eval_mean, o.other_eval_min, o.other_eval_max,
                       COALESCE(o.other_eval_cov, 0.0)::DOUBLE AS other_eval_cov
                FROM keys k
                LEFT JOIN t  ON t.x  = k.position_hash
                LEFT JOIN hz ON hz.x = k.position_hash
                LEFT JOIN o  ON o.position_hash = k.position_hash
            ) TO '{_p(tmps['aux'])}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Accounting for the meta and the gates.
        q = lambda sql: [int(v or 0) for v in con.execute(sql).fetchone()]
        (st["survivors"], st["survivor_mass"], st["pool_parents"],
         st["other_edges"], st["other_mass"]) = q(f"""
            SELECT COUNT(*) FILTER (WHERE total >= {min_games}),
                   SUM(total) FILTER (WHERE total >= {min_games}),
                   COUNT(DISTINCT parent_hash) FILTER (WHERE total >= {min_games}),
                   COUNT(*) FILTER (WHERE total < {min_games}),
                   SUM(total) FILTER (WHERE total < {min_games})
            FROM e WHERE NOT is_coll""")
        (st["aux_rows"], st["aux_other_mass"], st["aux_term_mass"],
         st["aux_horizon_mass"]) = q(f"""
            SELECT COUNT(*), SUM(other_total), SUM(term_other_total), SUM(horizon_total)
            FROM read_parquet('{_p(tmps['aux'])}')""")
        (st["other_covered_mass"], st["aux_other_covered_mass"]) = q(f"""
            SELECT SUM(e.total),
                   SUM(e.total) FILTER (WHERE e.parent_hash IN (SELECT position_hash FROM keys))
            FROM e JOIN ev ON ev.child_hash = e.child_hash
            WHERE NOT e.is_coll AND e.total < {min_games}""")
        (st["ended_mass_all"], st["ended_cells"]) = q("SELECT SUM(total), COUNT(*) FROM en")
        st["horizon_mass_all"] = q(f"SELECT SUM(total) FROM d WHERE q = {max_ply + 1}")[0]
        st["arrivals_mass"] = q("SELECT SUM(total) FROM a")[0]
        st["departures_mass_le_cap"] = q(f"SELECT SUM(total) FROM d WHERE q <= {max_ply}")[0]
        if root_hash is not None and int(root_hash) % N_BUCKETS == i:   # Python % = rem_euclid
            r = int(root_hash)
            st["root_ply1_games"] = q(f"SELECT SUM(total) FROM d WHERE x = {r} AND q = 1")[0]
            st["root_departures_le_cap"] = q(
                f"SELECT SUM(total) FROM d WHERE x = {r} AND q <= {max_ply}")[0]
            st["root_term_mass"] = q(f"SELECT SUM(total) FROM en WHERE x = {r}")[0]
    finally:
        con.close()
    st["secs"] = round(time.time() - t0, 1)
    tmps["edges"].replace(paths["edges"])
    tmps["aux"].replace(paths["aux"])
    _write_json(tmps["stats"], st)
    tmps["stats"].replace(paths["stats"])     # last: the bucket's sentinel
    return i, st


def bucket_done(work: Path, i: int) -> bool:
    return all(p.exists() for p in bucket_paths(work, i).values())


def read_collisions(book: Path) -> list[int]:
    import duckdb
    con = duckdb.connect()
    try:
        return [int(r[0]) for r in con.execute(
            f"SELECT DISTINCT parent_hash FROM read_parquet('{_p(book / '_collisions.parquet')}')"
        ).fetchall()]
    finally:
        con.close()


def root_hash() -> int:
    import chess
    from stage1_extract_positions import zobrist_int64
    return int(zobrist_int64(chess.Board()))


def phase_buckets(a, buckets: list[int]) -> None:
    work = Path(a.work)
    (work / "buckets").mkdir(parents=True, exist_ok=True)
    coll = read_collisions(Path(a.book))
    rh = root_hash()
    adir = resolve_arrivals(a)
    todo = [i for i in buckets if not bucket_done(work, i)]
    print(f"buckets: {len(buckets)} requested, {len(todo)} to build "
          f"({len(coll)} collision hashes dropped as parents)", flush=True)
    tasks = [(i, str(a.book), a.events, a.elo_bands, a.max_ply, a.min_games,
              str(a.eval_arrays), str(work), str(adir), coll, rh, a.threads, a.mem,
              str(a.tmp_dir))
             for i in todo]
    _pool_run(_bucket_task, tasks, a.workers,
              lambda r: (f"bucket {r[0]:3d}: {r[1]['survivors']:,} survivors / "
                         f"{r[1]['edges_prefloor']:,} edges, {r[1]['pool_parents']:,} parents, "
                         f"eval {r[1]['eval_lookup_s']}s ({r[1]['secs']:.0f}s)"))


# ── phase 3: finalize ──────────────────────────────────────────────────────────

def out_paths(a) -> tuple[Path, Path]:
    d = Path(a.out_dir)
    return (d / f"position_stats_pooled_{a.tag}.parquet",
            d / f"position_stats_aux_pooled_{a.tag}.parquet")


def meta_path(p: Path) -> Path:
    return p.with_name(p.name + ".meta.json")


def _schema_list(path: Path) -> list[tuple[str, str]]:
    import pyarrow.parquet as pq
    return [(f.name, str(f.type)) for f in pq.read_schema(path)]


def git_commit() -> str:
    pinned = Path(__file__).resolve().parent / "PINNED_COMMIT"
    if pinned.exists():
        return pinned.read_text(encoding="utf-8").strip()
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                              cwd=Path(__file__).resolve().parent, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def sum_stats(work: Path) -> dict:
    tot: dict = {}
    for i in range(N_BUCKETS):
        st = json.loads(bucket_paths(work, i)["stats"].read_text(encoding="utf-8"))
        for k, v in st.items():
            if k in ("bucket", "secs", "eval_lookup_s"):
                continue
            tot[k] = tot.get(k, 0) + v
    return tot


def phase_finalize(a, timings: dict) -> None:
    import polars as pl
    work = Path(a.work)
    missing = [i for i in range(N_BUCKETS) if not bucket_done(work, i)]
    if missing:
        sys.exit(f"finalize: {len(missing)} buckets not built (first: {missing[:10]})")
    pool_out, aux_out = out_paths(a)
    done = work / "_POOL.DONE"
    if done.exists() and pool_out.exists() and aux_out.exists():
        print(f"SKIP finalize: {done} exists")
        return
    t0 = time.time()
    if not pool_out.exists():
        df = pl.read_parquet([str(bucket_paths(work, i)["edges"]) for i in range(N_BUCKETS)])
        df = (df.with_columns(
                  pl.lit(POOL_EVENT).alias("event"),
                  pl.lit(POOL_ELO, dtype=pl.Int64).alias("elo_band"),
                  ((pl.col("white_wins") + 0.5 * pl.col("draws")) / pl.col("total"))
                  .alias("white_score_avg"))
                .select([c for c, _ in POOL_SCHEMA])
                .sort(["parent_hash", "move_san"]))
        tmp = pool_out.with_name(pool_out.name + ".tmp")
        df.write_parquet(tmp, compression="zstd")
        got = _schema_list(tmp)
        if got != POOL_SCHEMA:
            tmp.unlink()
            raise RuntimeError(f"pool schema drift: {got}")
        tmp.replace(pool_out)
        print(f"  {pool_out.name}: {df.height:,} edges", flush=True)
        del df
    if not aux_out.exists():
        df = pl.read_parquet([str(bucket_paths(work, i)["aux"]) for i in range(N_BUCKETS)])
        df = df.select([c for c, _ in AUX_SCHEMA]).sort("position_hash")
        if df["position_hash"].n_unique() != df.height:
            raise RuntimeError("aux: duplicate position_hash rows")
        tmp = aux_out.with_name(aux_out.name + ".tmp")
        df.write_parquet(tmp, compression="zstd")
        got = _schema_list(tmp)
        if got != AUX_SCHEMA:
            tmp.unlink()
            raise RuntimeError(f"aux schema drift: {got}")
        tmp.replace(aux_out)
        print(f"  {aux_out.name}: {df.height:,} positions", flush=True)
        del df
    timings["finalize_s"] = round(time.time() - t0, 1)
    write_metas(a, timings)
    done.write_text(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), encoding="utf-8")
    print(f"finalize: metas written, {done}", flush=True)


def write_metas(a, timings: dict) -> None:
    """The two .meta.json sidecars. Re-runnable (--phase meta) so the gate
    verdicts in <work>/_gates.json can be stamped in after the gates run."""
    work = Path(a.work)
    pool_out, aux_out = out_paths(a)
    st = sum_stats(work)
    import pyarrow.parquet as pq
    n_pool = pq.ParquetFile(pool_out).metadata.num_rows
    n_aux = pq.ParquetFile(aux_out).metadata.num_rows
    if n_pool != st["survivors"] or n_aux != st["aux_rows"] or n_aux != st["pool_parents"]:
        raise RuntimeError(f"row counts disagree with the bucket stats: pool {n_pool:,} vs "
                           f"{st['survivors']:,}, aux {n_aux:,} vs {st['aux_rows']:,} / "
                           f"{st['pool_parents']:,} parents")
    book = Path(a.book)
    mp = json.loads((book / "_merge_params.json").read_text(encoding="utf-8"))
    ip = mp.get("input_params", {})
    from eval_arrays import read_meta
    gate = {}
    gp = work / "_gates.json"
    if gp.exists():
        gate = json.loads(gp.read_text(encoding="utf-8"))
    common = {
        "producer": PRODUCER,
        "git_commit": git_commit(),
        "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "population": {
            "events": a.events, "elo_bands": a.elo_bands,
            "mean_elo_min": min(a.elo_bands),
            "book_filters": {k: ip.get(k) for k in
                             ("min_elo", "exclude_bots", "excluded_terminations",
                              "max_ply", "events")},
            "months": [mp["months"][0], mp["months"][-1]] if mp.get("months") else None,
            "n_months": len(mp.get("months", [])),
            "rating_gap_cap": None,
        },
        "max_ply": a.max_ply, "min_games": a.min_games, "term_reasons": "pooled",
        "event": POOL_EVENT, "elo_band": POOL_ELO,
        "book": str(book),
        "book_meta_sha256": _sha256(book / "_book.meta.json"),
        "collisions_sha256": _sha256(book / "_collisions.parquet"),
        "eval_arrays": str(a.eval_arrays),
        "eval_arrays_fingerprint": read_meta(Path(a.eval_arrays)),
        "params": json.loads((work / "_params.json").read_text(encoding="utf-8")),
        "arrivals_from": str(a.arrivals_from) if a.arrivals_from else None,
        "timings_s": timings,
        "counts": {
            "edges_before_floor": st["edges_prefloor"],
            "edges_before_floor_incl_collisions": st["edges_all"],
            "survivors": st["survivors"], "survivor_mass": st["survivor_mass"],
            "pool_parents": st["pool_parents"], "aux_rows": st["aux_rows"],
            "mass_le_cap_excl_collisions": st["mass_prefloor"],
            "other_edges": st["other_edges"], "other_mass": st["other_mass"],
            "aux_other_mass": st["aux_other_mass"],
            "other_eval_cov": (st["other_covered_mass"] / st["other_mass"]
                               if st["other_mass"] else None),
            "aux_other_eval_cov": (st["aux_other_covered_mass"] / st["aux_other_mass"]
                                   if st["aux_other_mass"] else None),
            "aux_term_mass": st["aux_term_mass"], "aux_horizon_mass": st["aux_horizon_mass"],
            "collision_parents_dropped": st["coll_parents"],
            "collision_edges_dropped": st["coll_edges"],
            "collision_mass_dropped": st["coll_mass"],
            "root_ply1_games": st.get("root_ply1_games"),
            "root_departures_le_cap": st.get("root_departures_le_cap"),
            "root_term_mass": st.get("root_term_mass"),
            # Gate 1 (all 54 slices, its buckets): derived ended - the book's term
            # table. Parse failures, which the old extract counted nowhere.
            "parse_failure_residual_gate1": gate.get("term_vs_book", {}).get("residual"),
            "parse_failure_residual_share_gate1":
                gate.get("term_vs_book", {}).get("residual_share"),
            "book_end_ply0_games_gate1_buckets":
                gate.get("term_vs_book", {}).get("book_end_ply0_total_in_buckets"),
        },
        "gates": gate,
    }
    _write_json(meta_path(pool_out), {**common, "file": pool_out.name, "role": "edges"})
    _write_json(meta_path(aux_out), {**common, "file": aux_out.name, "role": "aux",
                                     "pool_file": pool_out.name})


# ── orchestration ──────────────────────────────────────────────────────────────

LOCKED = ("book", "events", "elo_bands", "max_ply", "min_games", "eval_arrays", "tag")


def lock_params(a) -> None:
    work = Path(a.work)
    work.mkdir(parents=True, exist_ok=True)
    p = work / "_params.json"
    cur = {k: (str(getattr(a, k)) if isinstance(getattr(a, k), Path) else getattr(a, k))
           for k in LOCKED}
    if p.exists():
        old = json.loads(p.read_text(encoding="utf-8"))
        if old != cur:
            sys.exit(f"FATAL: {p} locks different parameters:\n  locked {old}\n  asked  {cur}\n"
                     f"Use a new --work dir for a different population.")
    else:
        _write_json(p, cur)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--book", type=Path, default=BOOK)
    ap.add_argument("--events", nargs="+", required=True)
    ap.add_argument("--elo-bands", nargs="+", type=int, required=True)
    ap.add_argument("--max-ply", type=int, required=True)
    ap.add_argument("--min-games", type=int, required=True)
    ap.add_argument("--eval-arrays", type=Path, default=EVAL_ARRAYS)
    ap.add_argument("--tag", required=True,
                    help="Output names: position_stats[_aux]_pooled_<tag>.parquet")
    ap.add_argument("--work", type=Path, required=True, help="Scratch (H:/chess/pool_work/...)")
    ap.add_argument("--out-dir", type=Path, default=STATS_DIR)
    ap.add_argument("--tmp-dir", type=Path, default=TMP_DIR, help="DuckDB spill (never F:)")
    ap.add_argument("--threads", type=int, default=4, help="DuckDB threads per process")
    ap.add_argument("--mem", default="8GB", help="DuckDB memory_limit per process")
    ap.add_argument("--workers", type=int, default=3, help="Concurrent fresh processes")
    ap.add_argument("--phase", choices=["arrivals", "buckets", "finalize", "meta", "all"],
                    default="all")
    ap.add_argument("--arrivals-from", type=Path, default=None,
                    help="Another work dir whose arrivals to reuse (same book, slices and "
                         "cap; checked against its _params.json). A floor variant needs no "
                         "arrivals phase of its own.")
    ap.add_argument("--buckets", nargs="*", type=int, default=None,
                    help="Restrict the buckets phase (smoke runs, gates)")
    return ap


def main() -> None:
    a = build_parser().parse_args()
    a.events = list(a.events)
    a.elo_bands = sorted(int(b) for b in a.elo_bands)
    if "F:" in str(a.tmp_dir).upper()[:2]:
        sys.exit("FATAL: DuckDB temp on F: (USB HDD) is forbidden")
    lock_params(a)
    from eval_arrays import verify_eval_arrays
    print(f"eval arrays: {verify_eval_arrays(Path(a.eval_arrays))}", flush=True)
    tpath = Path(a.work) / "_timings.json"
    timings = json.loads(tpath.read_text(encoding="utf-8")) if tpath.exists() else {}
    if a.phase in ("arrivals", "all") and not a.arrivals_from:
        t0 = time.time()
        phase_arrivals(a)
        timings["arrivals_s"] = round(timings.get("arrivals_s", 0) + time.time() - t0, 1)
        _write_json(tpath, timings)
    if a.phase in ("buckets", "all"):
        t0 = time.time()
        phase_buckets(a, a.buckets if a.buckets else list(range(N_BUCKETS)))
        timings["buckets_s"] = round(timings.get("buckets_s", 0) + time.time() - t0, 1)
        _write_json(tpath, timings)
    if a.phase in ("finalize", "all"):
        phase_finalize(a, timings)
        _write_json(tpath, timings)
    if a.phase == "meta":
        write_metas(a, timings)
        print("metas rewritten (gates from _gates.json)", flush=True)


if __name__ == "__main__":
    main()
