"""
Pool the games of 6 (event, elo_band) sets into ONE synthetic slice, so Stage 3
builds a single repertoire on the combined data (more games -> more sensitivity to
repertoire differences) instead of 6 separate slices.

Sets pooled: event in {Blitz, Rapid, Classical} x elo_band in {1900, 2200}.
Output slice key: event='Pooled', elo_band=0.

Two outputs (both consumed by Stage 3 as a normal single-slice file):
  position_stats_pooled_1900_2200_brc.parquet
      - GROUP BY parent_hash, move_san across the 6 sets; SUM the count columns;
        recompute white_score_avg; any_value the per-position constants. Mirrors
        merge_stats_2024_2025.py exactly, but collapses event+elo instead of years.
  crush_hist_pooled_1900_2200_brc.parquet
      - GROUP BY parent_hash, move_san, move_bucket across the 6 sets; SUM n/white_wins/
        black_wins. Stage 3's crush loader then sums buckets 1..horizon for crush@H.

Atomic write + skip-gate. DuckDB (bounded cardinality; no OOM).

Usage: python build_combined_slice.py
"""
from __future__ import annotations
import sys
import time
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import duckdb

STATS_DIR = Path("E:/chess/position-stats")
IN_STATS = STATS_DIR / "position_stats_2024_2025.parquet"
IN_CRUSH = STATS_DIR / "crush_hist_2024_2025.parquet"
OUT_STATS = STATS_DIR / "position_stats_pooled_1900_2200_brc.parquet"
OUT_CRUSH = STATS_DIR / "crush_hist_pooled_1900_2200_brc.parquet"
TMP_DIR = STATS_DIR / "_pool_duckdb_tmp"

EVENTS = ("Blitz", "Rapid", "Classical")
ELOS = (1900, 2200)
POOL_EVENT = "Pooled"
POOL_ELO = 0


def _con():
    con = duckdb.connect()
    con.execute("SET memory_limit='48GB';")
    con.execute(f"SET temp_directory='{str(TMP_DIR).replace(chr(92), '/')}';")
    con.execute("SET threads=6;")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=false;")
    return con


def build_stats():
    if OUT_STATS.exists():
        print(f"SKIP stats: {OUT_STATS.name} exists ({OUT_STATS.stat().st_size/1e9:.2f} GB)")
        return
    tmp = OUT_STATS.with_suffix(".parquet.tmp")
    if tmp.exists():
        tmp.unlink()
    src = str(IN_STATS).replace("\\", "/")
    out = str(tmp).replace("\\", "/")
    t0 = time.time()
    con = _con()
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
                '{POOL_EVENT}'          AS event,
                {POOL_ELO}              AS elo_band,
                (SUM(white_wins) + 0.5 * SUM(draws)) / SUM(total) AS white_score_avg,
                any_value(child_hash)   AS child_hash
            FROM read_parquet('{src}')
            WHERE event IN {EVENTS} AND elo_band IN {ELOS}
            GROUP BY parent_hash, move_san
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()
    tmp.replace(OUT_STATS)
    import polars as pl
    n = pl.scan_parquet(OUT_STATS).select(pl.len()).collect().item()
    print(f"stats: {OUT_STATS.name} ({OUT_STATS.stat().st_size/1e9:.2f} GB, {n:,} rows) "
          f"in {(time.time()-t0)/60:.1f} min")


def build_crush():
    if OUT_CRUSH.exists():
        print(f"SKIP crush: {OUT_CRUSH.name} exists ({OUT_CRUSH.stat().st_size/1e9:.2f} GB)")
        return
    tmp = OUT_CRUSH.with_suffix(".parquet.tmp")
    if tmp.exists():
        tmp.unlink()
    src = str(IN_CRUSH).replace("\\", "/")
    out = str(tmp).replace("\\", "/")
    t0 = time.time()
    con = _con()
    con.execute(f"""
        COPY (
            SELECT
                '{POOL_EVENT}'          AS event,
                {POOL_ELO}              AS elo_band,
                parent_hash,
                move_san,
                move_bucket,
                SUM(n)::BIGINT          AS n,
                SUM(white_wins)::BIGINT AS white_wins,
                SUM(black_wins)::BIGINT AS black_wins
            FROM read_parquet('{src}')
            WHERE event IN {EVENTS} AND elo_band IN {ELOS}
            GROUP BY parent_hash, move_san, move_bucket
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()
    tmp.replace(OUT_CRUSH)
    import polars as pl
    n = pl.scan_parquet(OUT_CRUSH).select(pl.len()).collect().item()
    print(f"crush: {OUT_CRUSH.name} ({OUT_CRUSH.stat().st_size/1e9:.2f} GB, {n:,} rows) "
          f"in {(time.time()-t0)/60:.1f} min")


def main():
    for p in (IN_STATS, IN_CRUSH):
        if not p.exists():
            sys.exit(f"FATAL: missing input {p}")
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Pooling events {EVENTS} x elo {ELOS} -> event='{POOL_EVENT}', elo_band={POOL_ELO}\n",
          flush=True)
    build_stats()
    build_crush()
    import shutil
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    print("\nDone.")


if __name__ == "__main__":
    main()
