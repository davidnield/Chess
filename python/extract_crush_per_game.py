"""
Stage 1.5: per-game "crush" facts (fast, columns-only — no movetext/chess).

For each source partition (year/month/event) with event in {Blitz,Rapid,Classical},
emit (game_id, white_win_normal, black_win_normal, move_count) where a "crush" is a
decisive win by mate or resignation:

    white_win_normal = 1 if (white_score == 1.0 and termination == 'Normal') else 0
    black_win_normal = 1 if (white_score == 0.0 and termination == 'Normal') else 0
    move_count       = full moves at which the game ended (verified: clean_plies ≈ 2*mc)

termination == 'Normal' = checkmate or resignation; 'Time forfeit' (flag) is excluded.

NOTE (v2): we deliberately store the RAW move_count + decisive flags rather than a
pre-baked earliness weight. build_crush_stats.py then aggregates these into a per-edge
MOVE-COUNT HISTOGRAM, so the earliness-decay shape (exponential half-life, horizon,
linear, …) becomes a free re-weight at Stage 3 load time — no heavy re-aggregation
needed to retune it.

Output: E:/chess/crush-per-game-v2/year=Y_month=M_event=E.parquet  (atomic, skip-gate).
Partitions are derived from the position-moves combined dirs so they align with
what build_crush_stats.py will join against.

Usage: python extract_crush_per_game.py
"""
from __future__ import annotations
import argparse
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")

SOURCE_ROOT = Path("D:/data/chess/standard-chess-games-compressed")
PM_DIRS = [Path("E:/chess/position-moves-2024-all"),
           Path("E:/chess/position-moves-2025-all")]
OUT_DIR = Path("E:/chess/crush-per-game-v2")
EVENTS = {"Blitz", "Rapid", "Classical"}
FNAME_RE = re.compile(r"year=(\d+)_month=(\d+)_event=([A-Za-z]+)")


def partitions_from_pm() -> list[tuple[int, int, str]]:
    parts: set[tuple[int, int, str]] = set()
    for d in PM_DIRS:
        if not d.exists():
            continue
        for f in d.glob("*.parquet"):
            m = FNAME_RE.search(f.name)
            if m and m.group(3) in EVENTS:
                parts.add((int(m.group(1)), int(m.group(2)), m.group(3)))
    return sorted(parts)


def process_one(args: tuple) -> str:
    """Worker: extract crush for one (year,month,event) partition. Own DuckDB.

    The source parquets have tiny row groups (~130 rows) so the scan is
    CPU-bound on metadata/decompression; running partitions in parallel (this
    is one worker) is what makes it tractable.
    """
    import duckdb
    y, mo, ev, threads = args
    out = OUT_DIR / f"year={y}_month={mo}_event={ev}.parquet"
    if out.exists():
        return f"SKIP {out.name} (exists)"
    src = SOURCE_ROOT / f"year={y}" / f"month={mo}" / f"event={ev}"
    if not any(src.glob("*.parquet")):
        return f"WARN no source for {y}/{mo}/{ev}"
    glob_path = str(src / "*.parquet").replace("\\", "/")
    tmp = out.with_suffix(".parquet.tmp")
    if tmp.exists():
        tmp.unlink()
    t0 = time.time()
    con = duckdb.connect()
    con.execute(f"SET threads={threads};")
    con.execute("SET enable_progress_bar=false;")
    con.execute(f"""
        COPY (
            SELECT
                game_id,
                CASE WHEN white_score = 1.0 AND termination = 'Normal'
                     THEN 1 ELSE 0 END::TINYINT AS white_win_normal,
                CASE WHEN white_score = 0.0 AND termination = 'Normal'
                     THEN 1 ELSE 0 END::TINYINT AS black_win_normal,
                move_count::INTEGER AS move_count
            FROM read_parquet('{glob_path}')
        ) TO '{str(tmp).replace(chr(92), '/')}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()
    tmp.replace(out)
    return f"{out.name}: {out.stat().st_size/1e6:.1f} MB ({time.time()-t0:.0f}s)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8,
                    help="Parallel partition workers (default 8 on a 12-core box)")
    ap.add_argument("--threads", type=int, default=2,
                    help="DuckDB threads per worker (default 2)")
    args = ap.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    parts = partitions_from_pm()
    todo = [(y, mo, ev, args.threads) for (y, mo, ev) in parts]
    print(f"{len(parts)} partitions, {args.workers} workers x {args.threads} threads\n",
          flush=True)
    t_all = time.time()
    done = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process_one, a): a for a in todo}
        for fut in as_completed(futs):
            print("  " + fut.result(), flush=True)
            done += 1
    print(f"\nAll {done}/{len(parts)} partitions in {(time.time()-t_all)/60:.1f} min.")


if __name__ == "__main__":
    main()
