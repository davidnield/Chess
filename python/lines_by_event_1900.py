"""
How the f=0.1, e=0.5 combined repertoire at elo 1900 differs by time control.
Traces on-line lines (own rep moves; opponent = most common reply IN THAT EVENT)
for White and for Black vs each opening move, across Bullet/Blitz/Rapid/Classical.

Usage: python lines_by_event_1900.py
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
TAG = sys.argv[1] if len(sys.argv) > 1 else "f010_e050"
EVENTS = ["Bullet", "Blitz", "Rapid", "Classical"]
MAXPLY = 12
OPP = ["e4", "d4", "c4", "Nf3"]

# load 1900 stats (all events) once
stats = pl.read_parquet(STATS, columns=["event", "elo_band", "parent_epd",
                                        "move_san", "total"]).filter(pl.col("elo_band") == ELO)


def most_common_map(event):
    d = stats.filter(pl.col("event") == event)
    d = d.sort("total", descending=True).group_by("parent_epd", maintain_order=True).first()
    return {r["parent_epd"]: r["move_san"] for r in d.iter_rows(named=True)}


def rep_map(persp, event):
    p = REP_DIR / f"repertoire_2024_2025_{persp}_{TAG}.parquet"
    df = pl.read_parquet(p, columns=["event", "elo_band", "position_epd", "best_move"])
    df = df.filter((pl.col("event") == event) & (pl.col("elo_band") == ELO)
                   & pl.col("best_move").is_not_null())
    return {r["position_epd"]: r["best_move"] for r in df.iter_rows(named=True)}


def fmt(sans):
    return " ".join(f"{i//2+1}.{s}" if i % 2 == 0 else s for i, s in enumerate(sans))


def trace_white(wrep, common):
    b = chess.Board(); sans = []
    for _ in range(MAXPLY):
        mv = wrep.get(b.epd()) if b.turn == chess.WHITE else common.get(b.epd())
        if not mv:
            break
        try:
            b.push_san(mv)
        except Exception:
            break
        sans.append(mv)
    return fmt(sans) if sans else "(no data)"


def trace_black(opp, brep, common):
    b = chess.Board(); b.push_san(opp); sans = [opp]
    for _ in range(MAXPLY - 1):
        mv = brep.get(b.epd()) if b.turn == chess.BLACK else common.get(b.epd())
        if not mv:
            break
        try:
            b.push_san(mv)
        except Exception:
            break
        sans.append(mv)
    return fmt(sans)


def main():
    print(f"Combined 2024+2025  f=0.10 e=0.50  elo {ELO}  — lines by time control\n")
    for event in EVENTS:
        common = most_common_map(event)
        wrep = rep_map("white", event)
        brep = rep_map("black", event)
        nrep = len(brep)
        print(f"{'='*86}\n{event.upper()}  (Black rep has {nrep:,} positions at 1900)\n{'='*86}")
        print(f"  WHITE: {trace_white(wrep, common)}")
        for opp in OPP:
            print(f"  BLACK 1.{opp:3}: {trace_black(opp, brep, common)}")
        print()


if __name__ == "__main__":
    main()
