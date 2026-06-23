"""
Does a line hold its score as the time control lengthens?
Compare empirical score across event types (Bullet->Blitz->Rapid->Classical)
for key lines, as a proxy for 'trivially refuted (decays)' vs
'genuinely sharp (holds)'. Combined 2024+2025 stats.

Usage: python event_decay_probe.py [elo]   (elo=0 -> sum all bands)
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

STATS = Path("E:/chess/position-stats/position_stats_2024_2025.parquet")
ELO = int(sys.argv[1]) if len(sys.argv) > 1 else 1900
EVENTS = ["Bullet", "Blitz", "Rapid", "Classical"]


def epd_after(*sans):
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b.epd()


# (label, parent_epd, move, perspective)
PROBES = [
    ("1.Nf3 g5   (Black)",  epd_after("Nf3"), "g5",  "black"),
    ("1.Nf3 c5   (Black)",  epd_after("Nf3"), "c5",  "black"),
    ("1.Nf3 d6   (Black)",  epd_after("Nf3"), "d6",  "black"),
    ("1.Nf3 b6   (Black)",  epd_after("Nf3"), "b6",  "black"),
    ("BDG 1.d4 d5 2.e4 (White)", epd_after("d4", "d5"), "e4", "white"),
    ("QG  1.d4 d5 2.c4 (White)", epd_after("d4", "d5"), "c4", "white"),
    ("Scand 1.e4 d5 (Black)", epd_after("e4"), "d5", "black"),
    ("Englund 1.d4 e5 (Black)", epd_after("d4"), "e5", "black"),
]

cols = ["event", "elo_band", "parent_epd", "move_san", "total", "white_score_avg"]
df = pl.read_parquet(STATS, columns=cols)
if ELO != 0:
    df = df.filter(pl.col("elo_band") == ELO)

print(f"Score by event {'(elo '+str(ELO)+')' if ELO else '(all elos summed)'} "
      f"— score shown from the mover's POV; (games)\n")
hdr = f"{'line':<28}" + "".join(f"{e:>16}" for e in EVENTS)
print(hdr)
print("-" * len(hdr))
for label, pe, mv, persp in PROBES:
    cells = []
    for ev in EVENTS:
        r = df.filter((pl.col("event") == ev) & (pl.col("parent_epd") == pe)
                      & (pl.col("move_san") == mv))
        if r.height == 0:
            cells.append(f"{'-':>16}")
            continue
        # weighted across elo bands if summing
        tot = r["total"].sum()
        wsc = (r["white_score_avg"] * r["total"]).sum() / tot
        score = wsc if persp == "white" else 1 - wsc
        cells.append(f"{score:.3f} ({tot:>6,}){'':>1}"[:16].rjust(16))
    print(f"{label:<28}" + "".join(cells))
