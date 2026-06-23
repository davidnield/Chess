"""
Build crush_hist: per-edge MOVE-COUNT HISTOGRAM of decisive wins, keyed like
position_stats so Stage 3 can LEFT JOIN it.

For each position-moves file (E:/chess/position-moves-20{24,25}-all, has game_id):
  1. INNER JOIN to the surviving position_stats edge keys (event,elo_band,
     parent_hash,move_san) — collapses the ~1.5B-key long tail to the survivors,
     so no OOM-monster / no sharding.
  2. JOIN the matching per-game crush facts (crush-per-game-v2) by game_id.
  3. GROUP BY the 4 keys + move_bucket, where
        move_bucket = least(greatest(move_count,1),60)  for decisive (Normal) wins,
                    = 0                                   for every non-decisive game,
     emitting n=COUNT(*), white_wins=SUM(white_win_normal), black_wins=SUM(black_win_normal).
Partial per file (resumable); then a final group-sum across partials.

Why a histogram (v2): storing the move-count distribution per edge (rather than a
single pre-weighted crush sum) lets Stage 3 apply ANY earliness-decay shape
(exponential half-life, horizon, linear) as a free re-weight at load time — retuning
sharpness never requires re-running this multi-hour aggregation again.

Per edge, Stage 3 reconstructs:
    total_games   = SUM(n)                               over all buckets (denominator)
    white_crush   = SUM(white_wins * w(bucket))          over buckets >= 1 (numerator)
    black_crush   = SUM(black_wins * w(bucket))          over buckets >= 1
with w(bucket) the chosen decay weight.

Parallel: the back-half 2024 Blitz part-files are large, so partitions run across
a process pool. Each worker builds the position_stats key table once (initializer).

Scope: Blitz + Rapid + Classical. position_stats is untouched.
Output: E:/chess/position-stats/crush_hist_2024_2025.parquet

Usage: python build_crush_stats.py [--workers 8] [--threads 2]
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
POSITION_STATS = Path("E:/chess/position-stats/position_stats_2024_2025.parquet")
OUT = Path("E:/chess/position-stats/crush_hist_2024_2025.parquet")
INTER = Path("E:/chess/position-stats/crush_hist_2024_2025.intermediates")
TMP_DIR = Path("E:/chess/position-stats/_crush_duckdb_tmp")
EVENTS = ("Blitz", "Rapid", "Classical")
FNAME_RE = re.compile(r"year=(\d+)_month=(\d+)_event=([A-Za-z]+)")

# Per-worker DuckDB connection with the keys table preloaded.
_CON = None


def _init_worker(threads: int):
    import duckdb
    global _CON
    _CON = duckdb.connect()
    _CON.execute(f"SET threads={threads};")
    _CON.execute("SET enable_progress_bar=false;")
    _CON.execute("SET preserve_insertion_order=false;")
    _CON.execute(f"""
        CREATE TEMP TABLE keys AS
        SELECT event, elo_band, parent_hash, move_san
        FROM read_parquet('{str(POSITION_STATS).replace(chr(92), '/')}')
        WHERE event IN {EVENTS}
    """)


def _process_file(pmf_str: str, y: int, mo: int, ev: str) -> str:
    pmf = Path(pmf_str)
    partial = INTER / (pmf.stem + ".crush.parquet")
    if partial.exists():
        return f"SKIP {pmf.name}"
    crushf = CRUSH_PG_DIR / f"year={y}_month={mo}_event={ev}.parquet"
    if not crushf.exists():
        return f"WARN no crush-per-game for {y}/{mo}/{ev}"
    ptmp = partial.with_suffix(".parquet.tmp")
    if ptmp.exists():
        ptmp.unlink()
    t0 = time.time()
    _CON.execute(f"""
        COPY (
            SELECT pm.event, pm.elo_band, pm.parent_hash, pm.move_san,
                   CASE WHEN c.white_win_normal = 1 OR c.black_win_normal = 1
                        THEN least(greatest(c.move_count, 1), 60) ELSE 0 END AS move_bucket,
                   COUNT(*)::BIGINT             AS n,
                   SUM(c.white_win_normal)::BIGINT AS white_wins,
                   SUM(c.black_win_normal)::BIGINT AS black_wins
            FROM read_parquet('{str(pmf).replace(chr(92), '/')}') pm
            JOIN keys k
              ON pm.event = k.event AND pm.elo_band = k.elo_band
             AND pm.parent_hash = k.parent_hash AND pm.move_san = k.move_san
            JOIN read_parquet('{str(crushf).replace(chr(92), '/')}') c
              ON pm.game_id = c.game_id
            GROUP BY pm.event, pm.elo_band, pm.parent_hash, pm.move_san, move_bucket
        ) TO '{str(ptmp).replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    ptmp.replace(partial)
    return f"{pmf.name} -> {partial.stat().st_size/1e6:.1f} MB ({time.time()-t0:.0f}s)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--threads", type=int, default=2)
    args = ap.parse_args()
    if OUT.exists():
        print(f"SKIP: {OUT.name} already exists ({OUT.stat().st_size/1e6:.1f} MB)")
        return
    INTER.mkdir(parents=True, exist_ok=True)

    pm_files = []
    for d in PM_DIRS:
        for f in sorted(d.glob("*.parquet")):
            m = FNAME_RE.search(f.name)
            if m and m.group(3) in EVENTS:
                pm_files.append((str(f), int(m.group(1)), int(m.group(2)), m.group(3)))
    todo = [t for t in pm_files
            if not (INTER / (Path(t[0]).stem + ".crush.parquet")).exists()]
    print(f"{len(pm_files)} files total; {len(todo)} remaining; "
          f"{args.workers} workers x {args.threads} threads\n", flush=True)

    t_all = time.time()
    done = 0
    if todo:
        with ProcessPoolExecutor(max_workers=args.workers,
                                 initializer=_init_worker,
                                 initargs=(args.threads,)) as ex:
            futs = [ex.submit(_process_file, *t) for t in todo]
            for fut in as_completed(futs):
                done += 1
                if done % 10 == 0 or done == len(todo):
                    print(f"  [{done}/{len(todo)}] {fut.result()}", flush=True)
    print(f"\nAll partials done in {(time.time()-t_all)/60:.1f} min. Merging...", flush=True)

    # Final merge (single process; bounded cardinality).
    import duckdb
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='48GB';")
    con.execute(f"SET temp_directory='{str(TMP_DIR).replace(chr(92), '/')}';")
    con.execute("SET threads=8;")
    con.execute("SET enable_progress_bar=false;")
    out_tmp = OUT.with_suffix(".parquet.tmp")
    if out_tmp.exists():
        out_tmp.unlink()
    con.execute(f"""
        COPY (
            SELECT event, elo_band, parent_hash, move_san, move_bucket,
                   SUM(n)::BIGINT          AS n,
                   SUM(white_wins)::BIGINT AS white_wins,
                   SUM(black_wins)::BIGINT AS black_wins
            FROM read_parquet('{str(INTER).replace(chr(92), '/')}/*.parquet')
            GROUP BY event, elo_band, parent_hash, move_san, move_bucket
        ) TO '{str(out_tmp).replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    out_tmp.replace(OUT)
    con.close()
    import shutil
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    print(f"Wrote {OUT} ({OUT.stat().st_size/1e6:.1f} MB)", flush=True)


if __name__ == "__main__":
    main()
