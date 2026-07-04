"""
Pool the games of (event, elo_band) sets into ONE synthetic slice, so Stage 3
builds a single repertoire on the combined data (more games -> more sensitivity to
repertoire differences) instead of separate slices.

NOTE — two pooling paths coexist. This script pools EXISTING per-(event,elo_band)
position_stats (e.g. position_stats_2024_2025.parquet) into the synthetic slice. For
NEW datasets the canonical pooler is build_pooled_stats.py, which fuses extraction +
filtering + pooling straight from the source parquets and emits the single slice
directly (no separate per-band stats). Use this script only to re-pool stats that were
already aggregated per band.

Sets pooled: event in {Blitz, Rapid, Classical} x elo_band in --elos (default 1900 2200).
Output slice key: event='Pooled', elo_band=0. Output filenames carry the band tag, so
band sets coexist:
  --elos 1900 2200       -> position_stats_pooled_1900_2200_brc.parquet (canonical)
  --elos 1650 1900 2200  -> position_stats_pooled_1650_1900_2200_brc.parquet

Three outputs (each consumed by Stage 3 as a normal single-slice file):
  position_stats_pooled_<tag>_brc.parquet
      - GROUP BY parent_hash, move_san across the sets; SUM the count columns;
        recompute white_score_avg; any_value the per-position constants.
  crush_hist_pooled_<tag>_brc.parquet / crush_hist_rel_pooled_<tag>_brc.parquet
      - GROUP BY parent_hash, move_san, move_bucket; SUM n/white_wins/black_wins.

Atomic write + skip-gate. DuckDB (bounded cardinality; no OOM).

Usage:
    python build_combined_slice.py                        # default 1900+2200
    python build_combined_slice.py --elos 1650 1900 2200  # wider pool
"""
from __future__ import annotations
import argparse
import sys
import time
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import duckdb

STATS_DIR = Path("E:/chess/position-stats")
IN_STATS = STATS_DIR / "position_stats_2024_2025.parquet"
IN_CRUSH = STATS_DIR / "crush_hist_2024_2025.parquet"
IN_CRUSH_REL = STATS_DIR / "crush_hist_rel_2024_2025.parquet"
TMP_DIR = STATS_DIR / "_pool_duckdb_tmp"

EVENTS = ("Blitz", "Rapid", "Classical")
POOL_EVENT = "Pooled"
POOL_ELO = 0


def band_tag(elos) -> str:
    return "_".join(str(int(e)) for e in sorted(elos))


def out_paths(elos):
    t = band_tag(elos)
    return (STATS_DIR / f"position_stats_pooled_{t}_brc.parquet",
            STATS_DIR / f"crush_hist_pooled_{t}_brc.parquet",
            STATS_DIR / f"crush_hist_rel_pooled_{t}_brc.parquet")


def _elo_in(elos) -> str:
    # Render an SQL IN-list without a trailing comma (so single-band works too).
    return "(" + ", ".join(str(int(e)) for e in sorted(elos)) + ")"


def _con():
    con = duckdb.connect()
    con.execute("SET memory_limit='48GB';")
    con.execute(f"SET temp_directory='{str(TMP_DIR).replace(chr(92), '/')}';")
    con.execute("SET threads=6;")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=false;")
    return con


def build_stats(out_stats: Path, elos):
    if out_stats.exists():
        print(f"SKIP stats: {out_stats.name} exists ({out_stats.stat().st_size/1e9:.2f} GB)")
        return
    tmp = out_stats.with_suffix(".parquet.tmp")
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
            WHERE event IN {EVENTS} AND elo_band IN {_elo_in(elos)}
            GROUP BY parent_hash, move_san
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()
    tmp.replace(out_stats)
    import polars as pl
    n = pl.scan_parquet(out_stats).select(pl.len()).collect().item()
    print(f"stats: {out_stats.name} ({out_stats.stat().st_size/1e9:.2f} GB, {n:,} rows) "
          f"in {(time.time()-t0)/60:.1f} min")


def build_crush(in_path: Path, out_path: Path, elos, label: str):
    """Pool a crush histogram (absolute or relative — same schema) into the slice.
    Bucket semantics are opaque here; we just SUM the per-bucket counts."""
    if out_path.exists():
        print(f"SKIP {label}: {out_path.name} exists ({out_path.stat().st_size/1e9:.2f} GB)")
        return
    tmp = out_path.with_suffix(".parquet.tmp")
    if tmp.exists():
        tmp.unlink()
    src = str(in_path).replace("\\", "/")
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
            WHERE event IN {EVENTS} AND elo_band IN {_elo_in(elos)}
            GROUP BY parent_hash, move_san, move_bucket
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()
    tmp.replace(out_path)
    import polars as pl
    n = pl.scan_parquet(out_path).select(pl.len()).collect().item()
    print(f"{label}: {out_path.name} ({out_path.stat().st_size/1e9:.2f} GB, {n:,} rows) "
          f"in {(time.time()-t0)/60:.1f} min")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--elos", type=int, nargs="+", default=[1900, 2200],
                    help="Elo bands to pool (default: 1900 2200). e.g. --elos 1650 1900 2200")
    args = ap.parse_args()
    elos = tuple(sorted(args.elos))
    out_stats, out_crush, out_crush_rel = out_paths(elos)

    for p in (IN_STATS, IN_CRUSH):
        if not p.exists():
            sys.exit(f"FATAL: missing input {p}")
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Pooling events {EVENTS} x elo {elos} -> event='{POOL_EVENT}', elo_band={POOL_ELO}\n",
          flush=True)
    build_stats(out_stats, elos)
    build_crush(IN_CRUSH, out_crush, elos, "crush(abs)")
    # Relative histogram is produced by the separate heavy build_crush_stats pass;
    # pool it too once present (for Stage 3's relative-propagated crush mode).
    if IN_CRUSH_REL.exists():
        build_crush(IN_CRUSH_REL, out_crush_rel, elos, "crush(rel)")
    else:
        print(f"NOTE: {IN_CRUSH_REL.name} not present yet — skipping relative pool. "
              f"Re-run after build_crush_stats.py finishes.")
    import shutil
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    print("\nDone.")


if __name__ == "__main__":
    main()
