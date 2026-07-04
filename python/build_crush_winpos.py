"""
Build crush_hist_RELWIN ("winpos"): the eval-enhanced relative crush histogram —
per-edge histogram of WINNING-POSITION-ACHIEVED events, schema-identical to
crush_hist_rel so Stage 3 consumes it unchanged (--crush-db just points here).

Event definition (restores the original crush intent — mate, resignation, OR a
winning eval achieved): per game per side, ONE event =

    EARLIEST of:
      (a) the first position reached strictly AFTER the edge whose Stockfish
          eval is >= +--thresh-cp for our side (lichess_eval_db; decisive evals
          are capped at ±2000 there, so mate-class evals are included), and
      (b) the decisive game end by mate/resignation (termination='Normal').

    move_bucket = clip(event_full_move - (ply-1)//2, 1, 60)   [same as crush_hist_rel]
    All games through the edge contribute n=1 at bucket 0 (the denominator);
    Stage 3 only ever sums n across buckets, so where n lives is immaterial.

Guarantees (task #27; verified by _test_winpos.py against this exact SQL):
  1. No double counting — min() of the two event types: a game that crosses +3
     at move 10 and resigns at move 18 lands ONE count, in the move-10 bucket.
  2. Ever-achieved, not frontier-eval — ALL positions along the game's path are
     checked, so a +3 later given away still counts. (Backwards induction cannot
     provide this: propagated value is an expectation, not a first-passage.)
  3. Downstream-only — only crossings strictly after the edge ply count, so a
     game already winning BEFORE an edge does not credit it (else every move
     inside a won position, including the one that throws it away, inherits
     credit). Terminations satisfy this automatically; crossings need the
     strict > filter. Position at pm ply c = position after c-1 plies, hence
     "after our move at ply p" == c > p.
  4. Coverage — eval-crossings fire only where the eval DB covers the position;
     mate/resignation events fire for ALL games. Uncovered (rare) lines are
     structurally biased against, consistent with --crush-baseline zero.
  5. Crossings beyond the extraction depth (ply 30) are unobservable; the
     decisive-end component (which sees the true game length) covers that tail.

Inputs (2024–2025 only; 2019–2023 has no per-game files and needs the fused-
extractor replay — task #27 phase 2, only if this variant wins the A/B):
  E:/chess/position-moves-{2024,2025}-all/           (game_id, ply, parent_hash, …)
  E:/chess/crush-per-game-v2/                        per-game decisive flags + move_count
  E:/chess/lichess_eval_db.parquet                   position_hash -> eval_cp
  position_stats_pooled_1900_2200_brc.parquet        surviving pooled edge keys

Output: E:/chess/position-stats/crush_hist_relwin_pooled_1900_2200_brc.parquet
        stamped event='Pooled', elo_band=0 — A/B-comparable with
        crush_hist_rel_pooled_1900_2200_brc.parquet on identical Stage 3 flags
        (rates sit on a HIGHER scale: re-sweep crush_weight/crush_prior).

NOTE: heavy E: reader — do NOT run while the 2019-2025 pooled merge is running.
Usage: python build_crush_winpos.py [--workers 6] [--threads 2] [--thresh-cp 300]
"""
from __future__ import annotations
import argparse
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")

PM_DIRS = [Path("E:/chess/position-moves-2024-all"),
           Path("E:/chess/position-moves-2025-all")]
CRUSH_PG_DIR = Path("E:/chess/crush-per-game-v2")
EVAL_DB = Path("E:/chess/lichess_eval_db.parquet")
POOLED_STATS = Path("E:/chess/position-stats/position_stats_pooled_1900_2200_brc.parquet")
OUT = Path("E:/chess/position-stats/crush_hist_relwin_pooled_1900_2200_brc.parquet")
INTER = Path("E:/chess/position-stats/crush_hist_relwin_pooled_1900_2200_brc.intermediates")
TMP_DIR = Path("E:/chess/position-stats/_crush_winpos_duckdb_tmp")
EVENTS = ("Blitz", "Rapid", "Classical")
FNAME_RE = re.compile(r"year=(\d+)_month=(\d+)_event=([A-Za-z]+)")
MIN_BAND = 1800    # pm-side elo_band floor == the >=1800 pool population
SENTINEL = 9999    # "no event" full-move sentinel (> any real move_count)


def winpos_sql(pm_path: str, crush_path: str, min_band: int = MIN_BAND) -> str:
    """The per-file event/histogram query. Expects three temp tables in the
    connection: keys(parent_hash, move_san), win_w(position_hash),
    win_b(position_hash). Factored out so _test_winpos.py runs THIS query.

    Per game side s: ev_s = LEAST(first winning position strictly after the
    edge (full move = crossing_ply // 2), decisive-normal end move). One event
    per game per side -> no double counting by construction. Bucket 0 rows
    carry n only.
    """
    q = lambda p: str(p).replace("\\", "/")
    return f"""
    WITH pm AS (
        SELECT game_id, ply, parent_hash, move_san
        FROM read_parquet('{q(pm_path)}')
        WHERE elo_band >= {int(min_band)}
    ),
    cw AS (  -- per game: sorted plies whose position is winning for White
        SELECT pm.game_id, list_sort(list(pm.ply)) AS cps
        FROM pm JOIN win_w w ON pm.parent_hash = w.position_hash
        GROUP BY pm.game_id
    ),
    cb AS (
        SELECT pm.game_id, list_sort(list(pm.ply)) AS cps
        FROM pm JOIN win_b w ON pm.parent_hash = w.position_hash
        GROUP BY pm.game_id
    ),
    g0 AS (
        SELECT e.parent_hash, e.move_san, e.ply,
               list_filter(cw.cps, x -> x > e.ply)[1] AS cw_ply,
               list_filter(cb.cps, x -> x > e.ply)[1] AS cb_ply,
               c.white_win_normal AS wwn, c.black_win_normal AS bwn,
               c.move_count AS mc
        FROM pm e
        JOIN keys k  ON e.parent_hash = k.parent_hash AND e.move_san = k.move_san
        JOIN read_parquet('{q(crush_path)}') c ON e.game_id = c.game_id
        LEFT JOIN cw ON e.game_id = cw.game_id
        LEFT JOIN cb ON e.game_id = cb.game_id
    ),
    g AS (
        SELECT parent_hash, move_san, ply,
               LEAST(COALESCE(cw_ply // 2, {SENTINEL}),
                     CASE WHEN wwn = 1 THEN mc ELSE {SENTINEL} END) AS ev_w,
               LEAST(COALESCE(cb_ply // 2, {SENTINEL}),
                     CASE WHEN bwn = 1 THEN mc ELSE {SENTINEL} END) AS ev_b
        FROM g0
    )
    SELECT parent_hash, move_san, move_bucket,
           SUM(n)::BIGINT AS n,
           SUM(w)::BIGINT AS white_wins,
           SUM(b)::BIGINT AS black_wins
    FROM (
        SELECT parent_hash, move_san, 0 AS move_bucket, 1 AS n, 0 AS w, 0 AS b
        FROM g
        UNION ALL
        SELECT parent_hash, move_san,
               LEAST(GREATEST(ev_w - (ply - 1) // 2, 1), 60), 0, 1, 0
        FROM g WHERE ev_w < {SENTINEL}
        UNION ALL
        SELECT parent_hash, move_san,
               LEAST(GREATEST(ev_b - (ply - 1) // 2, 1), 60), 0, 0, 1
        FROM g WHERE ev_b < {SENTINEL}
    )
    GROUP BY parent_hash, move_san, move_bucket
    """


# ── worker ──────────────────────────────────────────────────────────────────

_CON = None


def _init_worker(threads: int, keys_pq: str, winw_pq: str, winb_pq: str):
    import duckdb
    global _CON
    _CON = duckdb.connect()
    _CON.execute(f"SET threads={threads};")
    _CON.execute("SET enable_progress_bar=false;")
    _CON.execute("SET preserve_insertion_order=false;")
    for name, pq in (("keys", keys_pq), ("win_w", winw_pq), ("win_b", winb_pq)):
        _CON.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM "
                     f"read_parquet('{pq.replace(chr(92), '/')}')")


def _process_file(pmf_str: str, y: int, mo: int, ev: str) -> str:
    pmf = Path(pmf_str)
    partial = INTER / (pmf.stem + ".winpos.parquet")
    if partial.exists():
        return f"SKIP {pmf.name}"
    crushf = CRUSH_PG_DIR / f"year={y}_month={mo}_event={ev}.parquet"
    if not crushf.exists():
        return f"WARN no crush-per-game for {y}/{mo}/{ev}"
    ptmp = partial.with_suffix(".parquet.tmp")
    if ptmp.exists():
        ptmp.unlink()
    t0 = time.time()
    _CON.execute(f"COPY ({winpos_sql(str(pmf), str(crushf))}) TO "
                 f"'{str(ptmp).replace(chr(92), '/')}' "
                 f"(FORMAT PARQUET, COMPRESSION ZSTD)")
    ptmp.replace(partial)
    return f"{pmf.name} -> {partial.stat().st_size/1e6:.1f} MB ({time.time()-t0:.0f}s)"


# ── main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--thresh-cp", type=int, default=300,
                    help="Winning-position eval threshold in centipawns (default +300).")
    ap.add_argument("--eval-db", default="E:/chess/unified_eval_db.parquet",
                    help="position_hash -> eval_cp parquet (default: the unified "
                         "cloud+fishnet DB from build_fishnet_eval_db.py; pass "
                         "E:/chess/lichess_eval_db.parquet for cloud-only).")
    args = ap.parse_args()
    eval_db = Path(args.eval_db)
    if OUT.exists():
        print(f"SKIP: {OUT.name} already exists ({OUT.stat().st_size/1e6:.1f} MB)")
        return
    INTER.mkdir(parents=True, exist_ok=True)

    # One-time prep: pooled edge keys + winning-position hash sets (skip-gated).
    import duckdb
    prep = INTER / "_prep"
    prep.mkdir(exist_ok=True)
    keys_pq = prep / "keys.parquet"
    winw_pq = prep / f"win_w_{args.thresh_cp}.parquet"
    winb_pq = prep / f"win_b_{args.thresh_cp}.parquet"
    con = duckdb.connect()
    con.execute("SET threads=4; SET enable_progress_bar=false;")
    qp = lambda p: str(p).replace("\\", "/")
    for out_pq, sql in (
        (keys_pq, f"SELECT DISTINCT parent_hash, move_san FROM read_parquet('{qp(POOLED_STATS)}')"),
        (winw_pq, f"SELECT position_hash FROM read_parquet('{qp(eval_db)}') WHERE eval_cp >= {args.thresh_cp}"),
        (winb_pq, f"SELECT position_hash FROM read_parquet('{qp(eval_db)}') WHERE eval_cp <= -{args.thresh_cp}"),
    ):
        if not out_pq.exists():
            tmp = out_pq.with_suffix(".parquet.tmp")
            con.execute(f"COPY ({sql}) TO '{qp(tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            tmp.replace(out_pq)
        n = con.execute(f"SELECT count(*) FROM read_parquet('{qp(out_pq)}')").fetchone()[0]
        print(f"  prep {out_pq.name}: {n:,} rows")
    con.close()

    pm_files = []
    for d in PM_DIRS:
        for f in sorted(d.glob("*.parquet")):
            m = FNAME_RE.search(f.name)
            if m and m.group(3) in EVENTS:
                pm_files.append((str(f), int(m.group(1)), int(m.group(2)), m.group(3)))
    todo = [t for t in pm_files
            if not (INTER / (Path(t[0]).stem + ".winpos.parquet")).exists()]
    print(f"{len(pm_files)} files total; {len(todo)} remaining; "
          f"{args.workers} workers x {args.threads} threads\n", flush=True)

    t_all = time.time()
    done = 0
    if todo:
        with ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker,
                                 initargs=(args.threads, str(keys_pq), str(winw_pq),
                                           str(winb_pq))) as ex:
            futs = [ex.submit(_process_file, *t) for t in todo]
            for fut in as_completed(futs):
                done += 1
                if done % 10 == 0 or done == len(todo):
                    print(f"  [{done}/{len(todo)}] {fut.result()}", flush=True)
    print(f"\nAll partials done in {(time.time()-t_all)/60:.1f} min. Merging...", flush=True)

    # Final merge: sum across partials, stamp the pooled slice keys.
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='48GB';")
    con.execute(f"SET temp_directory='{qp(TMP_DIR)}';")
    con.execute("SET threads=8;")
    con.execute("SET enable_progress_bar=false;")
    out_tmp = OUT.with_suffix(".parquet.tmp")
    if out_tmp.exists():
        out_tmp.unlink()
    con.execute(f"""
        COPY (
            SELECT 'Pooled' AS event, 0 AS elo_band,
                   parent_hash, move_san, move_bucket,
                   SUM(n)::BIGINT          AS n,
                   SUM(white_wins)::BIGINT AS white_wins,
                   SUM(black_wins)::BIGINT AS black_wins
            FROM read_parquet('{qp(INTER)}/*.winpos.parquet')
            GROUP BY parent_hash, move_san, move_bucket
        ) TO '{qp(out_tmp)}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    out_tmp.replace(OUT)
    con.close()
    import shutil
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    print(f"Wrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)", flush=True)


if __name__ == "__main__":
    main()
