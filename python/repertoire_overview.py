"""
High-level overview of a built repertoire: our moves vs the opponent's TOP-2
most common replies at each of their turns, 3 opponent-branch-points deep.
White starts from move 1; Black starts from each of 1.e4 / 1.d4 / 1.c4 / 1.Nf3.

Usage:
  python repertoire_overview.py --white <whiterep.parquet> --black <blackrep.parquet>
                                [--stats <stats>] [--event Blitz] [--elo 1900] [--depth 3]
"""
from __future__ import annotations
import argparse
import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

ap = argparse.ArgumentParser()
ap.add_argument("--white", required=True)
ap.add_argument("--black", required=True)
ap.add_argument("--stats", default="E:/chess/position-stats/position_stats_2024_2025.parquet")
ap.add_argument("--event", default="Blitz")
ap.add_argument("--elo", type=int, default=1900)
ap.add_argument("--depth", type=int, default=3)
args = ap.parse_args()


def load_rep(path):
    df = pl.read_parquet(path, columns=["event", "elo_band", "position_epd", "best_move"])
    df = df.filter((pl.col("event") == args.event) & (pl.col("elo_band") == args.elo)
                   & pl.col("best_move").is_not_null())
    return {r["position_epd"]: r["best_move"] for r in df.iter_rows(named=True)}


WREP = load_rep(args.white)
BREP = load_rep(args.black)

st = pl.read_parquet(args.stats, columns=["event", "elo_band", "parent_epd",
                                          "move_san", "total", "white_score_avg"])
st = st.filter((pl.col("event") == args.event) & (pl.col("elo_band") == args.elo))
TOP: dict[str, list] = {}
for r in st.sort("total", descending=True).iter_rows(named=True):
    TOP.setdefault(r["parent_epd"], [])
    if len(TOP[r["parent_epd"]]) < 2:
        TOP[r["parent_epd"]].append((r["move_san"], r["total"], r["white_score_avg"]))


def mnum(board, san):
    return f"{board.fullmove_number}.{san}" if board.turn == chess.WHITE else f"{board.fullmove_number}...{san}"


def expand_opp(board, our_color, rep, depth, prefix):
    """board: opponent to move. Branch their top-2; show our reply; recurse."""
    if depth == 0:
        return
    moves = TOP.get(board.epd(), [])
    if not moves:
        return
    for i, (san, g, ws) in enumerate(moves):
        last = (i == len(moves) - 1)
        conn = "└─" if last else "├─"
        cont = "   " if last else "│  "
        opp_is_white = (board.turn == chess.WHITE)
        sc = ws if opp_is_white else 1 - ws
        b = board.copy()
        omv = mnum(b, san)
        b.push_san(san)
        our = rep.get(b.epd())
        if our:
            omv2 = mnum(b, our)
            b2 = b.copy(); b2.push_san(our)
            print(f"{prefix}{conn} {omv} ({g/1000:.0f}k {sc:.0%})  {omv2}")
            expand_opp(b2, our_color, rep, depth - 1, prefix + cont)
        else:
            print(f"{prefix}{conn} {omv} ({g/1000:.0f}k {sc:.0%})  (off-book)")


print(f"REPERTOIRE OVERVIEW — {args.event} elo {args.elo}, "
      f"opponent top-2 each move, {args.depth} deep\n")

# ── White ──
print("=" * 72)
b = chess.Board()
w1 = WREP.get(b.epd())
print(f"WHITE  (our 1st move: 1.{w1})")
print("=" * 72)
if w1:
    b.push_san(w1)
    expand_opp(b, chess.WHITE, WREP, args.depth, "")

# ── Black ──
print("\n" + "=" * 72)
print("BLACK  (reply to each of White's main first moves)")
print("=" * 72)
for first in ["e4", "d4", "c4", "Nf3"]:
    b = chess.Board(); b.push_san(first)
    reply = BREP.get(b.epd())
    if not reply:
        print(f"\n  1.{first}  ->  (off-book)"); continue
    b.push_san(reply)
    print(f"\n  1.{first} {reply}:")
    expand_opp(b, chess.BLACK, BREP, args.depth, "  ")
