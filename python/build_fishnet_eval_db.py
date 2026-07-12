"""Aggregate the Lichess fishnet-evals dump into a per-position eval table and unify
it with the cloud-eval DB (lichess_eval_db.parquet).

Inputs
------
- F:/chess/fishnet-evals/standard_rated_YYYY_MM.parquet  (fen, cp, mate, move)
  One file per dump month, 2016-01..2025-03, excluding known-bad 2016-12/2020-07/2020-08
  (see manifest.txt). Semantics (verified 2026-07-02): fen is the position BEFORE the
  human's move; cp/mate evaluate the fen, White-POV; one row per position-occurrence
  per analyzed game (NOT deduplicated); FENs are 6-field with the en-passant square
  present only when a legal ep capture exists (python-chess `epd()` convention, so
  epd == first 4 FEN fields verbatim).
- E:/chess/position-stats/position_stats_pooled_ge1800_2019_2025_brc.parquet — defines
  the position UNIVERSE (parents + children of every repertoire edge). Fishnet rows
  outside the universe are dropped at scan time; nothing else is ever aggregated.
- E:/chess/lichess_eval_db.parquet — the deep cloud-eval DB (build_lichess_eval_db.py).

Aggregation policy (locked 2026-07-02)
--------------------------------------
Every row is unified to one scale BEFORE aggregation: cp -> clamp(+-2000),
mate -> sign(mate)*2000 (mate distance is irrelevant downstream: all consumers are
threshold tests and the cloud DB caps at +-2000 already); mate=0 (side to move already
mated) resolves by the mated side. Per-position eval = MEDIAN of unified values within
the era tier — replication across independent fishnet runs is the robustness mechanism
(contrast build_lichess_eval_db.py's deepest-best-PV, correct there because cloud rows
are different PVs, not repeated measurements).

Union policy (amended 2026-07-12): cloud-preferred, EXCEPT where the fishnet median is
saturated at +-EVAL_CAP with n >= 5 and the cloud value has the opposite sign — those
cloud rows are mate-blind prefer-cp artifacts (the cloud aggregation kept the best
non-mating PV's cp when the principal line was a mate), concentrated on exactly the
trap positions a sharp repertoire cares about (source='fishnet-decisive').

Era tiers: NNUE = 2021-01+ (fishnet 2.0 switched to SF12 NNUE on 2020-12-17; the mixed
2020_12 file is conservatively classical). A position uses the NNUE median when it has
any NNUE rows; otherwise falls back to the classical median, flagged via `era`.

Union: cloud-preferred. Output rows carry provenance (source, fishnet n, era) so the
precedence policy can be re-derived cheaply without re-scanning the dump.

Architecture (the proven two-phase external-agg pattern from build_pooled_stats.py)
-----------------------------------------------------------------------------------
  Step 0  universe_epd_hash.parquet: (epd, position_hash) for every parent (from the
          stats directly) and every child (child EPD derived by one python-chess replay
          per stats row; child_hash already computed by the merge). Checkpointed.
  Phase A per month (process-isolated, skip-gated, atomic dir rename): scan, project
          epd = first 4 FEN fields, JOIN universe, unify eval, stream-partition rows
          (position_hash, val, era) into N_BUCKETS hash buckets. No GROUP BY.
  Phase B per bucket: median/count per (position_hash, era-tier), then tier selection.
  Union   with the cloud DB -> unified output.

Outputs (E:/chess/)
-------------------
- fishnet_eval_agg.parquet   (position_hash, eval_cp, n, era)
- unified_eval_db.parquet    (position_hash, eval_cp, source, cloud_depth?, fishnet_n)
  Schema is a superset of lichess_eval_db.parquet's (position_hash, eval_cp) — existing
  consumers (stage3 --eval-db, build_crush_winpos) read those two columns by name.

Usage
-----
    .venv/Scripts/python.exe python/build_fishnet_eval_db.py            # all steps, resumable
    .venv/Scripts/python.exe python/build_fishnet_eval_db.py --months 2016_01   # smoke test
    .venv/Scripts/python.exe python/build_fishnet_eval_db.py --threads 8 --mem 45GB
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

FISHNET_DIR = Path("F:/chess/fishnet-evals")
STATS = Path("E:/chess/position-stats/position_stats_pooled_ge1800_2019_2025_brc.parquet")
CLOUD_DB = Path("E:/chess/lichess_eval_db.parquet")
OUT_AGG = Path("E:/chess/fishnet_eval_agg.parquet")
OUT_UNIFIED = Path("E:/chess/unified_eval_db.parquet")
WORK = Path("E:/chess/_fishnet_agg_work")   # universe + Phase A partitions + buckets

EVAL_CAP = 2000
NNUE_FROM = (2021, 1)          # first pure-NNUE dump month (2020_12 is mixed -> classical)
N_BUCKETS = 32
_MONTH_RE = re.compile(r"standard_rated_(\d{4})_(\d{2})\.parquet$")


def _duck(threads: int, mem: str):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads={threads};")
    con.execute(f"SET memory_limit='{mem}';")
    con.execute("SET preserve_insertion_order=false;")
    return con


def _sql_path(p: Path) -> str:
    return str(p).replace("\\", "/")


def _bucket_expr(col: str) -> str:
    # Deterministic arithmetic bucketing; stable across DuckDB versions on resume.
    return f"(({col} % {N_BUCKETS}) + {N_BUCKETS}) % {N_BUCKETS}"


# ── Step 0: position universe (epd -> position_hash) ───────────────────────────

def build_universe(threads: int, mem: str) -> Path:
    """Parents come straight from the stats; child EPDs are derived by replaying each
    stats row once (child_hash is already in the stats — no re-hashing). Resumable via
    the output skip-gate; ~24M replays, single process, ~30-60 min."""
    out = WORK / "universe_epd_hash.parquet"
    if out.exists():
        print(f"SKIP universe: {out.name} exists", flush=True)
        return out
    WORK.mkdir(parents=True, exist_ok=True)
    import chess
    import polars as pl
    t0 = time.time()
    df = pl.read_parquet(STATS, columns=["parent_epd", "parent_hash", "move_san", "child_hash"])
    parents = (df.select(pl.col("parent_epd").alias("epd"),
                         pl.col("parent_hash").alias("position_hash"))
                 .unique(subset=["position_hash"]))
    print(f"  parents: {parents.height:,}", flush=True)

    child_epd: list[str | None] = []
    cache: dict[tuple[str, str], str | None] = {}
    for epd, san in zip(df["parent_epd"].to_list(), df["move_san"].to_list()):
        key = (epd, san)
        v = cache.get(key, "miss")
        if v == "miss":
            try:
                b = chess.Board(epd)
                b.push(b.parse_san(san))
                v = b.epd()
            except Exception:
                v = None
            cache[key] = v
        child_epd.append(v)
    children = (df.with_columns(pl.Series("epd", child_epd, dtype=pl.Utf8))
                  .filter(pl.col("epd").is_not_null())
                  .select("epd", pl.col("child_hash").alias("position_hash"))
                  .unique(subset=["position_hash"]))
    print(f"  children: {children.height:,} ({(time.time()-t0)/60:.1f} min)", flush=True)

    uni = pl.concat([parents, children]).unique(subset=["position_hash"])
    tmp = out.with_suffix(".parquet.tmp")
    uni.write_parquet(tmp, compression="zstd")
    tmp.replace(out)
    print(f"  universe: {uni.height:,} positions -> {out.name} "
          f"({(time.time()-t0)/60:.1f} min total)", flush=True)
    return out


# ── Phase A: per-month filter + unify + partition ──────────────────────────────

def _phase_a_one_month(task: tuple) -> tuple:
    """One month in a fresh process (allocator isolation). Atomic dir rename."""
    month_file, out_str, tmp_str, uni_str, era, threads, mem, label = task
    out, out_tmp = Path(out_str), Path(tmp_str)
    if out_tmp.is_dir():
        shutil.rmtree(out_tmp)
    t0 = time.time()
    con = _duck(threads, mem)
    try:
        con.execute(f"""
            COPY (
                SELECT u.position_hash,
                       CAST(CASE
                            WHEN f.cp IS NOT NULL THEN
                                 LEAST({EVAL_CAP}, GREATEST(-{EVAL_CAP}, f.cp))
                            WHEN f.mate > 0 THEN {EVAL_CAP}
                            WHEN f.mate < 0 THEN -{EVAL_CAP}
                            ELSE CASE WHEN contains(f.fen, ' w ')
                                      THEN -{EVAL_CAP} ELSE {EVAL_CAP} END
                            END AS SMALLINT)      AS val,
                       CAST({era} AS TINYINT)     AS era,
                       {_bucket_expr("u.position_hash")} AS bkt
                FROM read_parquet('{_sql_path(Path(month_file))}') f
                JOIN read_parquet('{uni_str}') u
                  ON u.epd = regexp_extract(f.fen, '^(\\S+ \\S+ \\S+ \\S+)', 1)
            ) TO '{_sql_path(out_tmp)}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (bkt))
        """)
    finally:
        con.close()
    out_tmp.replace(out)
    size = sum(f.stat().st_size for f in out.rglob("*") if f.is_file())
    return label, size, time.time() - t0


def phase_a(months: list[tuple[int, int, Path]], universe: Path,
            threads: int, mem: str) -> Path:
    pdir = WORK / "parts"
    pdir.mkdir(parents=True, exist_ok=True)
    for stale in pdir.glob("_tmp_month=*"):
        shutil.rmtree(stale, ignore_errors=True)
    tasks = []
    for y, m, f in months:
        out = pdir / f"month={y}_{m:02d}"
        if out.exists():
            continue
        era = 1 if (y, m) >= NNUE_FROM else 0
        tasks.append((str(f), str(out), str(pdir / f"_tmp_month={y}_{m:02d}"),
                      _sql_path(universe), era, threads, mem, f"{y}-{m:02d}"))
    print(f"Phase A: {len(months)} months, {len(tasks)} to build", flush=True)
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
        for fut in as_completed([ex.submit(_phase_a_one_month, t) for t in tasks]):
            label, size, secs = fut.result()
            print(f"  {label}: {size/1e6:.0f} MB ({secs/60:.1f} min)", flush=True)
    return pdir


# ── Phase B: per-bucket tiered median ──────────────────────────────────────────

def _phase_b_one_bucket(task: tuple) -> tuple:
    pdir_str, bout_str, i, threads, mem, label = task
    bout = Path(bout_str)
    tmp = bout.with_suffix(".parquet.tmp")
    tmp.unlink(missing_ok=True)
    t0 = time.time()
    con = _duck(threads, mem)
    try:
        con.execute(f"""
            COPY (
                WITH agg AS (
                    SELECT position_hash,
                           median(val) FILTER (era = 1) AS med_nnue,
                           count(*)    FILTER (era = 1) AS n_nnue,
                           median(val) FILTER (era = 0) AS med_class,
                           count(*)    FILTER (era = 0) AS n_class
                    FROM read_parquet('{pdir_str}/month=*/bkt={i}/*.parquet')
                    GROUP BY position_hash
                )
                SELECT position_hash,
                       CAST(round(CASE WHEN n_nnue > 0 THEN med_nnue ELSE med_class END)
                            AS INTEGER)                       AS eval_cp,
                       CAST(CASE WHEN n_nnue > 0 THEN n_nnue ELSE n_class END
                            AS BIGINT)                        AS n,
                       CASE WHEN n_nnue > 0 THEN 'nnue' ELSE 'classical' END AS era
                FROM agg
            ) TO '{_sql_path(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    tmp.replace(bout)
    return label, bout.stat().st_size, time.time() - t0


def phase_b(pdir: Path, threads: int, mem: str) -> None:
    if OUT_AGG.exists():
        print(f"SKIP Phase B: {OUT_AGG.name} exists", flush=True)
        return
    bdir = WORK / "buckets"
    bdir.mkdir(parents=True, exist_ok=True)
    tasks = []
    for i in range(N_BUCKETS):
        bout = bdir / f"bucket_{i}.parquet"
        if bout.exists():
            continue
        tasks.append((_sql_path(pdir), str(bout), i, threads, mem, f"bucket {i}"))
    print(f"Phase B: {N_BUCKETS} buckets, {len(tasks)} to build", flush=True)
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as ex:
        for fut in as_completed([ex.submit(_phase_b_one_bucket, t) for t in tasks]):
            label, size, secs = fut.result()
            print(f"  {label}: {size/1e6:.0f} MB ({secs/60:.1f} min)", flush=True)
    tmp = OUT_AGG.with_suffix(".parquet.tmp")
    tmp.unlink(missing_ok=True)
    con = _duck(threads, mem)
    try:
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{_sql_path(bdir)}/bucket_*.parquet'))
            TO '{_sql_path(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    tmp.replace(OUT_AGG)
    print(f"fishnet agg: {OUT_AGG.name} "
          f"({OUT_AGG.stat().st_size/1e6:.0f} MB)", flush=True)


# ── Terminal positions + union with the cloud DB ──────────────────────────────

def build_terminal_evals(threads: int, mem: str) -> Path:
    """Exact evals for terminal universe positions (checkmate = ±EVAL_CAP by the
    mated side, stalemate = 0). Neither source ever evaluates them (fishnet dumps
    annotate per-move so the post-mate position gets nothing; nobody engine-analyzes
    a finished game), yet without them --require-eval selection is BLIND to the
    mate-delivering move itself — e.g. it recommended 8...Qxc3 over 8...Qc1# in the
    Englund trap. Candidates are exactly the universe positions missing from both
    sources (~7k), so this is seconds of work."""
    out = WORK / "terminal_evals.parquet"
    if out.exists():
        print(f"SKIP terminal evals: {out.name} exists", flush=True)
        return out
    import chess
    import polars as pl
    uni = WORK / "universe_epd_hash.parquet"
    con = _duck(threads, mem)
    try:
        rows = con.execute(f"""
            SELECT u.epd, u.position_hash
            FROM read_parquet('{_sql_path(uni)}') u
            ANTI JOIN read_parquet('{_sql_path(OUT_AGG)}') f USING (position_hash)
            ANTI JOIN read_parquet('{_sql_path(CLOUD_DB)}') c USING (position_hash)
        """).fetchall()
    finally:
        con.close()
    term_h, term_cp = [], []
    n_mate = n_stale = 0
    for epd, h in rows:
        try:
            b = chess.Board(epd)
        except ValueError:
            continue
        if b.is_checkmate():
            term_h.append(h)
            term_cp.append(-EVAL_CAP if b.turn == chess.WHITE else EVAL_CAP)
            n_mate += 1
        elif b.is_stalemate():
            term_h.append(h)
            term_cp.append(0)
            n_stale += 1
    df = pl.DataFrame({"position_hash": pl.Series(term_h, dtype=pl.Int64),
                       "eval_cp": pl.Series(term_cp, dtype=pl.Int32)})
    tmp = out.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(out)
    print(f"terminal evals: {len(rows):,} uncovered universe positions -> "
          f"{n_mate:,} checkmates, {n_stale:,} stalemates, "
          f"{len(rows) - n_mate - n_stale:,} live-but-unanalyzed (skipped)", flush=True)
    return out


def build_unified(threads: int, mem: str) -> None:
    if OUT_UNIFIED.exists():
        print(f"SKIP unified: {OUT_UNIFIED.name} exists", flush=True)
        return
    term_pq = build_terminal_evals(threads, mem)
    tmp = OUT_UNIFIED.with_suffix(".parquet.tmp")
    tmp.unlink(missing_ok=True)
    con = _duck(threads, mem)
    t0 = time.time()
    try:
        con.execute(f"""
            COPY (
                -- Cloud-preferred, EXCEPT where the fishnet median is decisive
                -- (saturated at +-EVAL_CAP, i.e. mate/decisive) with >=5 replicates
                -- and the cloud eval sits on the OTHER side of 0. The cloud
                -- aggregation is mate-blind (prefer-cp keeps the runner-up PV's cp
                -- when the principal line is a mate row), which scored e.g. the
                -- Scholar's-mate-in-1 position at -128 and a mate-in-1-against at
                -- +143 — exactly the positions the Stage-3 refutation gate exists
                -- for. 1,177 in-DAG sign-flips measured 2026-07-12.
                SELECT c.position_hash,
                       CASE WHEN abs(f.eval_cp) = {EVAL_CAP} AND f.n >= 5
                                 AND sign(f.eval_cp) != sign(c.eval_cp)
                            THEN f.eval_cp ELSE c.eval_cp END AS eval_cp,
                       CASE WHEN abs(f.eval_cp) = {EVAL_CAP} AND f.n >= 5
                                 AND sign(f.eval_cp) != sign(c.eval_cp)
                            THEN 'fishnet-decisive' ELSE 'cloud' END AS source,
                       f.n AS fishnet_n, f.era AS fishnet_era
                FROM read_parquet('{_sql_path(CLOUD_DB)}') c
                LEFT JOIN read_parquet('{_sql_path(OUT_AGG)}') f USING (position_hash)
                UNION ALL
                SELECT f.position_hash, f.eval_cp,
                       'fishnet' AS source, f.n AS fishnet_n, f.era AS fishnet_era
                FROM read_parquet('{_sql_path(OUT_AGG)}') f
                ANTI JOIN read_parquet('{_sql_path(CLOUD_DB)}') c USING (position_hash)
                UNION ALL
                SELECT t.position_hash, t.eval_cp,
                       'terminal' AS source, CAST(NULL AS BIGINT) AS fishnet_n,
                       CAST(NULL AS VARCHAR) AS fishnet_era
                FROM read_parquet('{_sql_path(term_pq)}') t
            ) TO '{_sql_path(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
    finally:
        con.close()
    tmp.replace(OUT_UNIFIED)
    print(f"unified: {OUT_UNIFIED.name} ({OUT_UNIFIED.stat().st_size/1e6:.0f} MB, "
          f"{(time.time()-t0)/60:.1f} min)", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--mem", default="45GB")
    ap.add_argument("--months", nargs="*", default=None,
                    help="Restrict to these dump months, e.g. 2016_01 2024_12 (smoke test).")
    ap.add_argument("--keep-work", action="store_true",
                    help="Keep the Phase A/B work dirs after success (default: delete).")
    args = ap.parse_args()

    months = []
    for f in sorted(FISHNET_DIR.glob("standard_rated_*.parquet")):
        m = _MONTH_RE.search(f.name)
        if not m:
            continue
        y, mo = int(m.group(1)), int(m.group(2))
        if args.months and f"{y}_{mo:02d}" not in args.months:
            continue
        months.append((y, mo, f))
    if not months:
        sys.exit("FATAL: no fishnet month files matched")
    print(f"Months: {len(months)} ({months[0][0]}-{months[0][1]:02d} .. "
          f"{months[-1][0]}-{months[-1][1]:02d})", flush=True)

    uni = build_universe(args.threads, args.mem)
    if OUT_AGG.exists():
        # Phase A partitions exist only to feed Phase B; once the aggregate is
        # checkpointed (and parts possibly cleaned), don't re-partition 415 GB
        # just to rebuild the union step.
        print(f"SKIP Phase A: {OUT_AGG.name} exists", flush=True)
    else:
        pdir = phase_a(months, uni, args.threads, args.mem)
        phase_b(pdir, args.threads, args.mem)
    build_unified(args.threads, args.mem)
    if not args.keep_work:
        shutil.rmtree(WORK / "parts", ignore_errors=True)
        shutil.rmtree(WORK / "buckets", ignore_errors=True)
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
