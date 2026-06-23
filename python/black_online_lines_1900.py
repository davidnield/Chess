"""
On-line Black repertoire lines at Blitz 1900 — no what-if nodes.

For each opponent first move (e4, d4, c4, Nf3), walk the ACTUAL line:
  - Black-to-move nodes  -> Black repertoire's best_move
  - White-to-move nodes  -> most common empirical reply (position_stats, by total)
So White plays what you'll actually face; Black plays its real recommendation.
Reports per (year, f, e) at elo 1900.

Usage: python black_online_lines_1900.py
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

REP_DIR = Path("E:/chess/repertoire")
STATS = {
    "2024": Path("E:/chess/position-stats/position_stats_2024.parquet"),
    "2025": Path("E:/chess/position-stats/position_stats_2025.parquet"),
}
ELO = 1900
PARAMS = [(0.0, 0.0), (0.0, 0.50), (0.0, 1.0), (0.30, 0.0), (0.30, 0.50), (0.30, 1.0)]
OPP_FIRST = ["e4", "d4", "c4", "Nf3"]
MAXPLY = 14


def tag(fw, ew):
    return f"f{int(fw*100):03d}_e{int(ew*100):03d}"


def load_black_rep(year, fw, ew):
    p = REP_DIR / f"repertoire_{year}_black_{tag(fw,ew)}.parquet"
    df = pl.read_parquet(p, columns=["event", "elo_band", "position_epd",
                                      "best_move"])
    df = df.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO)
                   & pl.col("best_move").is_not_null())
    return {r["position_epd"]: r["best_move"] for r in df.iter_rows(named=True)}


def load_most_common_white(year):
    """{parent_epd: most-played move_san} for Blitz/1900 (White-to-move only
    needed, but we keep all and use as needed)."""
    df = pl.read_parquet(STATS[year], columns=["event", "elo_band",
                                               "parent_epd", "move_san", "total"])
    df = df.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO))
    df = df.sort("total", descending=True).group_by("parent_epd",
                                                     maintain_order=True).first()
    return {r["parent_epd"]: r["move_san"] for r in df.iter_rows(named=True)}


def trace(opp_first, brep, wcommon):
    board = chess.Board()
    board.push_san(opp_first)
    sans = [opp_first]
    for _ in range(MAXPLY - 1):
        if board.turn == chess.BLACK:
            mv = brep.get(board.epd())
        else:
            mv = wcommon.get(board.epd())
        if not mv:
            break
        try:
            board.push_san(mv)
        except Exception:
            break
        sans.append(mv)
    # format
    out = []
    for i, s in enumerate(sans):
        if i % 2 == 0:
            out.append(f"{i//2+1}.{s}")
        else:
            out.append(s)
    return " ".join(out)


def main():
    for year in STATS:
        wcommon = load_most_common_white(year)
        print(f"\n{'='*92}\nBLACK on-line lines — Blitz 1900 — {year} "
              f"(White = most common reply)\n{'='*92}")
        for (fw, ew) in PARAMS:
            brep = load_black_rep(year, fw, ew)
            print(f"\n-- {tag(fw,ew)} --")
            for opp in OPP_FIRST:
                print(f"  1.{opp:3} : {trace(opp, brep, wcommon)}")


if __name__ == "__main__":
    main()
