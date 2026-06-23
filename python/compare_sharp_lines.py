"""
Compare named sharp/gambit lines on the goal's real axes, reading a Stage 3
repertoire file (with the new value / value_robust / decisiveness columns) plus
the stats file (games + empirical score).

For each named line, walks the move sequence and prints per-position:
  value        -- expected white score vs AVERAGE opponent (trap value)
  value_robust -- value along opponent's BEST reply (the refutation test)
  decisiveness -- 1 - draw rate of the move played there (sharpness/fast wins)
  games, score -- from the stats file (score shown from the side-to-move POV)

Usage:
  python compare_sharp_lines.py --repertoire <rep.parquet> --stats <stats.parquet>
                                [--event Blitz] [--elo 1900]
"""
from __future__ import annotations
import argparse
import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

# (name, perspective-it-belongs-to, [SAN moves])
LINES = [
    ("Blackmar-Diemer",  "white", ["d4", "d5", "e4"]),
    ("Danish Gambit",    "white", ["e4", "e5", "d4", "exd4", "c3"]),
    ("Albin Countergambit", "black", ["d4", "d5", "c4", "e5"]),
    ("Cambridge Springs", "black", ["d4", "d5", "c4", "e6", "Nc3", "Nf6", "Bg5", "Nbd7", "e3", "c6", "Nf3", "Qa5"]),
    ("Italian (Bc4)",    "white", ["e4", "e5", "Nf3", "Nc6", "Bc4"]),
    ("Evans Gambit",     "white", ["e4", "e5", "Nf3", "Nc6", "Bc4", "Bc5", "b4"]),
    ("1.Nf3 g5",         "black", ["Nf3", "g5"]),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repertoire", required=True)
    ap.add_argument("--stats", default="E:/chess/position-stats/position_stats_2024_2025.parquet")
    ap.add_argument("--event", default="Blitz")
    ap.add_argument("--elo", type=int, default=1900)
    args = ap.parse_args()

    rep = pl.read_parquet(args.repertoire,
                          columns=["event", "elo_band", "position_epd", "side_to_move",
                                   "value", "value_robust", "decisiveness", "best_move"])
    rep = rep.filter((pl.col("event") == args.event) & (pl.col("elo_band") == args.elo))
    R = {r["position_epd"]: r for r in rep.iter_rows(named=True)}

    st = pl.read_parquet(args.stats, columns=["event", "elo_band", "parent_epd",
                                              "move_san", "total", "white_score_avg", "draws"])
    st = st.filter((pl.col("event") == args.event) & (pl.col("elo_band") == args.elo))
    # edge lookup: (parent_epd, move) -> (total, white_score_avg, draws)
    E = {(r["parent_epd"], r["move_san"]): r for r in st.iter_rows(named=True)}

    print(f"Sharp-line comparison — {args.repertoire.split(chr(47))[-1]}  "
          f"{args.event} elo {args.elo}\n")
    for name, persp, sans in LINES:
        print(f"== {name}  ({persp}) ==")
        b = chess.Board()
        ok = True
        for i, san in enumerate(sans):
            parent_epd = b.epd()
            edge = E.get((parent_epd, san))
            try:
                b.push_san(san)
            except Exception:
                print(f"    (illegal move {san})"); ok = False; break
            row = R.get(b.epd())  # metrics at the resulting position
            mover = "W" if i % 2 == 0 else "B"
            g = f"{edge['total']:>8,}" if edge else "       -"
            # score from the mover's POV
            sc = (edge["white_score_avg"] if mover == "W" else 1 - edge["white_score_avg"]) if edge else None
            dec = (1 - edge["draws"] / edge["total"]) if edge and edge["total"] else None
            vr = row["value_robust"] if row else None
            vm = row["value"] if row else None
            num = f"{i//2+1}.{'' if mover=='W' else '..'}"
            print(f"   {num:>5}{san:5} games={g} score={sc:.3f}" if sc is not None
                  else f"   {num:>5}{san:5} games={g} score=  -  ",
                  end="")
            print(f" decis={dec:.3f}" if dec is not None else " decis=  -  ", end="")
            print(f" | pos value={vm:.3f} robust={vr:.3f}" if (vm is not None and vr is not None)
                  else " | pos (not in repertoire slice)")
        print()


if __name__ == "__main__":
    main()
