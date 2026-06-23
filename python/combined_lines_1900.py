"""
On-line 1900 lines for the combined 2024+2025 grid, e=0.50, f in {0,0.1,0.2,0.3}.
White rep drives White moves; Black rep drives Black moves; the opponent's
replies are the most common empirical move from the combined stats.

Usage: python combined_lines_1900.py
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

REP_DIR = Path("E:/chess/repertoire")
STATS = Path("E:/chess/position-stats/position_stats_2024_2025.parquet")
ELO = 1900
FW = [0.00, 0.10, 0.20, 0.30]
EW = 0.50
MAXPLY = 14
OPP_FIRST = ["e4", "d4", "c4", "Nf3"]


def tag(fw):
    return f"f{int(fw*100):03d}_e{int(EW*100):03d}"


def load_rep(persp, fw):
    p = REP_DIR / f"repertoire_2024_2025_{persp}_{tag(fw)}.parquet"
    df = pl.read_parquet(p, columns=["event", "elo_band", "position_epd", "best_move"])
    df = df.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO)
                   & pl.col("best_move").is_not_null())
    return {r["position_epd"]: r["best_move"] for r in df.iter_rows(named=True)}


def load_most_common():
    df = pl.read_parquet(STATS, columns=["event", "elo_band", "parent_epd",
                                         "move_san", "total"])
    df = df.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO))
    df = df.sort("total", descending=True).group_by("parent_epd",
                                                    maintain_order=True).first()
    return {r["parent_epd"]: r["move_san"] for r in df.iter_rows(named=True)}


def fmt(sans):
    out = []
    for i, s in enumerate(sans):
        out.append(f"{i//2+1}.{s}" if i % 2 == 0 else s)
    return " ".join(out)


def trace_white(wrep, common):
    board = chess.Board()
    sans = []
    for _ in range(MAXPLY):
        mv = wrep.get(board.epd()) if board.turn == chess.WHITE else common.get(board.epd())
        if not mv:
            break
        try:
            board.push_san(mv)
        except Exception:
            break
        sans.append(mv)
    return fmt(sans)


def trace_black(opp, brep, common):
    board = chess.Board()
    board.push_san(opp)
    sans = [opp]
    for _ in range(MAXPLY - 1):
        mv = brep.get(board.epd()) if board.turn == chess.BLACK else common.get(board.epd())
        if not mv:
            break
        try:
            board.push_san(mv)
        except Exception:
            break
        sans.append(mv)
    return fmt(sans)


def main():
    common = load_most_common()
    print(f"\n{'='*94}\nWHITE on-line line — combined 2024+2025, Blitz 1900, e=0.50"
          f"\n{'='*94}")
    for fw in FW:
        wrep = load_rep("white", fw)
        print(f"  f={fw:.1f}: {trace_white(wrep, common)}")
    print(f"\n{'='*94}\nBLACK on-line lines — combined 2024+2025, Blitz 1900, e=0.50"
          f"\n{'='*94}")
    for fw in FW:
        brep = load_rep("black", fw)
        print(f"\n  -- f={fw:.1f} --")
        for opp in OPP_FIRST:
            print(f"    1.{opp:3} : {trace_black(opp, brep, common)}")


if __name__ == "__main__":
    main()
