"""
Branching probe of the 1.Nf3 g5 line for the combined Black repertoire.

After 1.Nf3 g5, branch on White's top-2 most-common replies (combined Blitz
stats) at each of the next 3 White moves; show Black's repertoire answer plus
game counts and empirical scores at every node.

Usage: python g5_tree_probe.py [elo] [fw]
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

REP_DIR = Path("E:/chess/repertoire")
STATS = Path("E:/chess/position-stats/position_stats_2024_2025.parquet")
ELO = int(sys.argv[1]) if len(sys.argv) > 1 else 1900
FW = float(sys.argv[2]) if len(sys.argv) > 2 else 0.10
EW = 0.50
WHITE_MOVES_TO_BRANCH = 3   # White's moves 2,3,4
TOPN = 2


def tag(fw):
    return f"f{int(fw*100):03d}_e{int(EW*100):03d}"


# combined Blitz/ELO stats slice (kept in memory for fast per-node filtering)
stats = pl.read_parquet(STATS, columns=["event", "elo_band", "parent_epd",
                                        "move_san", "total", "white_score_avg"])
stats = stats.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO))

brepdf = pl.read_parquet(REP_DIR / f"repertoire_2024_2025_black_{tag(FW)}.parquet",
                         columns=["event", "elo_band", "position_epd", "best_move"])
brepdf = brepdf.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO)
                       & pl.col("best_move").is_not_null())
BREP = {r["position_epd"]: r["best_move"] for r in brepdf.iter_rows(named=True)}


def edge(epd, mv):
    """(total, white_score_avg) for a given parent_epd + move, or None."""
    r = stats.filter((pl.col("parent_epd") == epd) & (pl.col("move_san") == mv))
    if r.height == 0:
        return None
    rr = r.row(0, named=True)
    return rr["total"], rr["white_score_avg"]


def top_white(epd, n=TOPN):
    r = stats.filter(pl.col("parent_epd") == epd).sort("total", descending=True).head(n)
    return [(x["move_san"], x["total"], x["white_score_avg"]) for x in r.iter_rows(named=True)]


def walk(board, white_moves_left, prefix):
    """At a White-to-move node: branch top-2, then Black replies, recurse."""
    if white_moves_left == 0:
        return
    wmoves = top_white(board.epd())
    if not wmoves:
        print(f"{prefix}(no White data at this node)")
        return
    movenum = board.fullmove_number
    for i, (wm, wtot, wsc) in enumerate(wmoves):
        last = (i == len(wmoves) - 1)
        branch = "└─" if last else "├─"
        b2 = board.copy()
        try:
            b2.push_san(wm)
        except Exception:
            continue
        print(f"{prefix}{branch} {movenum}.{wm}  [W {wtot:,} games, W-score {wsc:.3f}]")
        cont = prefix + ("   " if last else "│  ")
        # Black reply from repertoire
        brep = BREP.get(b2.epd())
        if brep is None:
            print(f"{cont}   Black: (no repertoire move — off-book)")
            continue
        be = edge(b2.epd(), brep)
        if be:
            btot, bwsc = be
            print(f"{cont}   Black: ...{brep}  [{btot:,} games, Black-score {1-bwsc:.3f}]")
        else:
            print(f"{cont}   Black: ...{brep}  [no stats]")
        b3 = b2.copy()
        try:
            b3.push_san(brep)
        except Exception:
            continue
        walk(b3, white_moves_left - 1, cont + "   ")


def main():
    board = chess.Board()
    board.push_san("Nf3")
    board.push_san("g5")
    print(f"Combined 2024+2025  Black rep  Blitz {ELO}  f={FW:.2f} e={EW:.2f}")
    print(f"Line: 1.Nf3 g5  (then White top-{TOPN} at each of next "
          f"{WHITE_MOVES_TO_BRANCH} moves)\n")
    g5edge = edge(chess.Board().fen() and chess.Board().epd(), "Nf3")
    # show how popular g5 itself is
    after_nf3 = chess.Board(); after_nf3.push_san("Nf3")
    ge = edge(after_nf3.epd(), "g5")
    if ge:
        print(f"context: 1.Nf3 g5 has {ge[0]:,} games, Black-score {1-ge[1]:.3f}\n")
    walk(board, WHITE_MOVES_TO_BRANCH, "")


if __name__ == "__main__":
    main()
