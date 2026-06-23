"""
Score x forcing scan of Black's replies to 1.Nf3 (combined Blitz stats).

For each Black reply m: games, Black empirical score, and the Simpson
concentration of White's reply distribution at the resulting position
(= the 'forcingness' Stage 3 uses). Find moves that are both sound and forcing.

Usage: python nf3_forcing_scan.py [elo]
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

STATS = Path("E:/chess/position-stats/position_stats_2024_2025.parquet")
ELO = int(sys.argv[1]) if len(sys.argv) > 1 else 1900

stats = pl.read_parquet(STATS, columns=["event", "elo_band", "parent_epd",
                                        "move_san", "total", "white_score_avg"])
stats = stats.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO))

after_nf3 = chess.Board(); after_nf3.push_san("Nf3")
NF3 = after_nf3.epd()

# Black replies to 1.Nf3
replies = stats.filter(pl.col("parent_epd") == NF3).sort("total", descending=True)

# overall Black baseline (mean over all Black-reply edges weighted by games)
tot_all = replies["total"].sum()
base_black = 1 - (replies["white_score_avg"] * replies["total"]).sum() / tot_all
print(f"1.Nf3 menu @ Blitz {ELO}  (Black baseline score over all replies "
      f"= {base_black:.3f})\n")
print(f"{'reply':>6} {'games':>9} {'Bscore':>7} {'forcing':>8} {'topWhiteReply':>16}")
print("-" * 52)

rows = []
for r in replies.head(16).iter_rows(named=True):
    m = r["move_san"]
    games = r["total"]
    bscore = 1 - r["white_score_avg"]
    b = after_nf3.copy()
    try:
        b.push_san(m)
    except Exception:
        continue
    wr = stats.filter(pl.col("parent_epd") == b.epd()).sort("total", descending=True)
    if wr.height == 0:
        simpson, topw = float("nan"), "-"
        continue
    T = wr["total"].sum()
    simpson = ((wr["total"] / T) ** 2).sum()
    topw = wr.row(0, named=True)
    topw_s = f"{topw['move_san']}({topw['total']/T*100:.0f}%)"
    rows.append((m, games, bscore, simpson, topw_s))

# sort by a simple sound+forcing view: print as-is (by games), then highlight
for (m, games, bscore, simpson, topw_s) in rows:
    flag = ""
    if bscore >= base_black and simpson >= 0.30:
        flag = "  <== sound & forcing"
    elif bscore >= base_black:
        flag = "  (sound, not forcing)"
    elif simpson >= 0.40:
        flag = "  (forcing, unsound)"
    print(f"{m:>6} {games:>9,} {bscore:>7.3f} {simpson:>8.3f} {topw_s:>16}{flag}")
