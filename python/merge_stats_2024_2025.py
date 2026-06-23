"""
Merge the two finished per-year position-stats files into a combined
2024+2025 stats file, by summing counts over (event, elo_band, parent_hash,
move_san). Drop-in schema match for Stage 3.

The inputs are already aggregated (post min_games), so this is a light
GROUP-BY SUM over ~58M rows — no 100GB blowup. child_hash/parent_epd/ply
are identical for a given key across years, so any_value() is exact.
white_score_avg is recomputed from the summed counts.

Approximation: a position dropped from BOTH years by the per-year min_games=50
filter (e.g. 30+30 games) is absent. Irrelevant for opening repertoires
(those positions have thousands+ games).

Usage: python merge_stats_2024_2025.py
"""
from __future__ import annotations
import os
import sys
import time
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import duckdb

STATS_DIR = Path("E:/chess/position-stats")
IN_2024 = STATS_DIR / "position_stats_2024.parquet"
IN_2025 = STATS_DIR / "position_stats_2025.parquet"
OUT = STATS_DIR / "position_stats_2024_2025.parquet"
TMP_DIR = STATS_DIR / "_merge_duckdb_tmp"


def main():
    if OUT.exists():
        print(f"SKIP: {OUT.name} already exists ({OUT.stat().st_size/1e9:.2f} GB)")
        return
    for p in (IN_2024, IN_2025):
        if not p.exists():
            print(f"FATAL: missing input {p}")
            sys.exit(1)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    out_tmp = OUT.with_suffix(".parquet.tmp")
    if out_tmp.exists():
        out_tmp.unlink()

    a = str(IN_2024).replace("\\", "/")
    b = str(IN_2025).replace("\\", "/")
    o = str(out_tmp).replace("\\", "/")

    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET memory_limit='48GB';")
    con.execute(f"SET temp_directory='{str(TMP_DIR).replace(chr(92), '/')}';")
    con.execute("SET threads=4;")
    con.execute("SET preserve_insertion_order=false;")
    print(f"Merging:\n  {IN_2024.name} ({IN_2024.stat().st_size/1e9:.2f} GB)\n"
          f"  {IN_2025.name} ({IN_2025.stat().st_size/1e9:.2f} GB)\n-> {OUT.name}",
          flush=True)
    con.execute(f"""
        COPY (
            SELECT
                parent_hash,
                move_san,
                any_value(parent_epd)   AS parent_epd,
                any_value(ply)          AS ply,
                SUM(white_wins)::BIGINT AS white_wins,
                SUM(draws)::BIGINT      AS draws,
                SUM(black_wins)::BIGINT AS black_wins,
                SUM(total)::BIGINT      AS total,
                event,
                elo_band,
                (SUM(white_wins) + 0.5 * SUM(draws)) / SUM(total) AS white_score_avg,
                any_value(child_hash)   AS child_hash
            FROM read_parquet(['{a}', '{b}'])
            GROUP BY event, elo_band, parent_hash, move_san
        ) TO '{o}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()
    out_tmp.replace(OUT)
    # cleanup scratch
    import shutil
    shutil.rmtree(TMP_DIR, ignore_errors=True)

    import polars as pl
    n = pl.scan_parquet(OUT).select(pl.len()).collect().item()
    print(f"DONE in {(time.time()-t0)/60:.1f} min -> {OUT.name} "
          f"({OUT.stat().st_size/1e9:.2f} GB, {n:,} rows)", flush=True)


if __name__ == "__main__":
    main()
