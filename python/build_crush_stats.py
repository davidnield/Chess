"""
Build crush_hist_REL: per-edge RELATIVE-HORIZON histogram of decisive wins, keyed
like position_stats so Stage 3 can LEFT JOIN it.

This is the RELATIVE variant of the crush histogram. The original crush_hist bucketed
by the game's ABSOLUTE length (move_count), so a deep edge only got credit for games
that ended by an absolute early move — the signal decayed to noise with depth. Here
move_bucket is the number of full moves from THIS position to the decisive end:

    move_bucket = least(greatest(move_count - (ply-1)//2, 1), 60)   for decisive wins
                = 0                                                  for non-decisive games

i.e. "how many more moves until we (or they) deliver the decisive result, measured
FROM this edge." Because move_count sees the real game end (well past the ply-30 edge
cap), a move-12 edge can be credited for a kill at move 25 — which the absolute metric
structurally could not. Per-instance pm.ply makes this correct under transposition
(the same position reached at different plies in different games buckets per instance).

For each position-moves file (E:/chess/position-moves-20{24,25}-all, has game_id + ply):
  1. INNER JOIN to the surviving position_stats edge keys (event,elo_band,
     parent_hash,move_san) — collapses the long tail to the survivors.
  2. JOIN the matching per-game crush facts (crush-per-game-v2) by game_id.
  3. GROUP BY the 4 keys + the RELATIVE move_bucket, emitting n / white_wins / black_wins.
Partial per file (resumable); then a final group-sum across partials.

Storing the distribution (not a pre-weighted sum) keeps the discount/horizon shape a
free re-weight at Stage 3 load time. Stage 3's relative-propagated crush mode consumes
this: per edge it forms a discounted relative crush and an immediate-window crush, then
propagates crush_potential through the DAG (see stage3_backwards_induction.py).

Parallel: large 2024 Blitz part-files run across a process pool; each worker builds the
position_stats key table once (initializer).

Scope: Blitz + Rapid + Classical. position_stats and the absolute crush_hist are untouched.
Output: E:/chess/position-stats/crush_hist_rel_2024_2025.parquet

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
# RELATIVE-horizon histogram: move_bucket = full moves from THIS position to the
# game's decisive end (not the game's absolute length). Written to a new file so
# the absolute crush_hist_2024_2025.parquet (crush@15 reps) stays reproducible.
OUT = Path("E:/chess/position-stats/crush_hist_rel_2024_2025.parquet")
INTER = Path("E:/chess/position-stats/crush_hist_rel_2024_2025.intermediates")
TMP_DIR = Path("E:/chess/position-stats/_crush_rel_duckdb_tmp")
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
                   -- RELATIVE moves-to-end: full moves from THIS position (reached
                   -- at ply pm.ply, so (ply-1)//2 full moves already completed) to
                   -- the decisive game end (c.move_count full moves). Per-instance
                   -- ply handles transpositions correctly. 0 = non-decisive game.
                   CASE WHEN c.white_win_normal = 1 OR c.black_win_normal = 1
                        THEN least(greatest(c.move_count - ((pm.ply - 1) // 2), 1), 60)
                        ELSE 0 END AS move_bucket,
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
