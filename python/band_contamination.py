"""How much do mean-rating bands mix players of other strengths? (follow-up review R6)

The explorer book's elo_band is the game's MEAN rating, so the weak table built from mean
1000-1799 games also holds moves by stronger and weaker players. The owner kept mean bands
(2026-10-31) on the strength of this measurement; this script is it, kept so the numbers can
be reproduced. Population: one month of rated Blitz/Rapid/Classical games from the canonical
D: source, bots and Abandoned/Rules-infraction games dropped (the extract's filters).

Reports the within-game rating gap (median, 90th percentile), the share of moves in mean
1000-1799 games made by a player rated 1800+ or under 1000 (each game's moves split equally
between its players: move_count is in full moves), and the share of 1000-1799 players' moves
that fall outside the band. Global rates: they do not bound the mix at any position.

    .venv/Scripts/python.exe python/band_contamination.py [--year 2026 --month 6]
"""
from __future__ import annotations

import argparse
import json
import sys

import duckdb

sys.path.insert(0, "python")
from build_pooled_stats import discover_source_files  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--month", type=int, default=6)
    ap.add_argument("--events", default="Blitz,Rapid,Classical")
    a = ap.parse_args()
    files = [str(f).replace("\\", "/") for f, y, m, _e in
             discover_source_files(a.year, a.year, None, a.events.split(",")) if m == a.month]
    con = duckdb.connect()
    con.execute("SET threads=8; SET memory_limit='16GB'")
    src = "[" + ",".join(f"'{f}'" for f in files) + "]"
    # moves per player: move_count is in full moves, which each player makes (to within one)
    q = f"""
    WITH g AS (
      SELECT white_elo w, black_elo b, (white_elo + black_elo) / 2.0 mean_elo,
             abs(white_elo - black_elo) gap, move_count n
      FROM read_parquet({src})
      WHERE white_score IS NOT NULL AND white_elo IS NOT NULL AND black_elo IS NOT NULL
        AND coalesce(white_title, '') <> 'BOT' AND coalesce(black_title, '') <> 'BOT'
        AND coalesce(termination, '') NOT IN ('Abandoned', 'Rules infraction')),
    mv AS (
      SELECT mean_elo, w elo, n moves FROM g
      UNION ALL SELECT mean_elo, b, n FROM g)
    SELECT
      (SELECT count(*) FROM g) games,
      (SELECT quantile_cont(gap, 0.5) FROM g) gap_median,
      (SELECT quantile_cont(gap, 0.9) FROM g) gap_p90,
      (SELECT sum(moves) FILTER (WHERE elo >= 1800) / sum(moves) FROM mv
         WHERE mean_elo >= 1000 AND mean_elo < 1800) band_moves_by_1800_plus,
      (SELECT sum(moves) FILTER (WHERE elo < 1000) / sum(moves) FROM mv
         WHERE mean_elo >= 1000 AND mean_elo < 1800) band_moves_by_under_1000,
      (SELECT sum(moves) FILTER (WHERE mean_elo < 1000 OR mean_elo >= 1800) / sum(moves) FROM mv
         WHERE elo >= 1000 AND elo < 1800) players_1000_1799_moves_outside_band
    """
    cols = ["games", "gap_median", "gap_p90", "band_moves_by_1800_plus",
            "band_moves_by_under_1000", "players_1000_1799_moves_outside_band"]
    row = dict(zip(cols, con.execute(q).fetchone()))
    print(json.dumps({"year": a.year, "month": a.month, "events": a.events, "files": len(files),
                      **{k: (float(v) if v is not None else None) for k, v in row.items()}}, indent=1))


if __name__ == "__main__":
    main()
