"""Repertoire overview for Blitz 2200, f030_e050, across all three datasets."""
import polars as pl, sys
sys.stdout.reconfigure(encoding="utf-8")
REP = "E:/chess/repertoire"
DATASETS = ["2016", "2025_jan", "2025_apr"]
VTAG = "f030_e050"
EVENT = "Blitz"
ELO = 2200

# ── Load ────────────────────────────────────────────────────────────────────
frames = {}
for ds in DATASETS:
    for persp in ["white", "black"]:
        path = f"{REP}/repertoire_{ds}_{persp}_{VTAG}.parquet"
        df = pl.read_parquet(path).filter(
            (pl.col("event") == EVENT) & (pl.col("elo_band") == ELO)
        )
        frames[(ds, persp)] = df
print(f"Loaded {sum(v.height for v in frames.values()):,} total rows\n")

def best(df: pl.DataFrame, board_part: str, our_side: str):
    """Return (best_move, value) for the first our-turn match.
    our_side should be 'white' or 'black' (matching the side_to_move column).
    """
    rows = df.filter(
        pl.col("position_epd").str.starts_with(board_part) &
        (pl.col("side_to_move") == our_side) &
        pl.col("best_move").is_not_null()
    )
    if rows.is_empty():
        return None, None
    r = rows.sort("value", descending=(our_side=="white")).row(0, named=True)
    return r["best_move"], r["value"]

SEP = "─" * 72

# ══════════════════════════════════════════════════════════════════════════
print(f"\n{'═'*72}")
print(f"  BLITZ 2200  |  weights={VTAG}  |  {EVENT}")
print(f"{'═'*72}")

# ── White's first move ───────────────────────────────────────────────────
START = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — First move")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], START, "white")
    print(f"  {ds:>12}:  {m or '?':>4}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── Black vs 1.e4 ────────────────────────────────────────────────────────
E4_POS = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR"  # after 1.e4, Black to move
print(f"\n{SEP}")
print("AS BLACK — Response to 1.e4")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"black")], E4_POS, "black")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── Black vs 1.d4 ────────────────────────────────────────────────────────
D4_POS = "rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS BLACK — Response to 1.d4")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"black")], D4_POS, "black")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── Black vs 1.Nf3 ───────────────────────────────────────────────────────
NF3_POS = "rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R"
print(f"\n{SEP}")
print("AS BLACK — Response to 1.Nf3")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"black")], NF3_POS, "black")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── Black vs 1.c4 ────────────────────────────────────────────────────────
C4_POS = "rnbqkbnr/pppppppp/8/8/2P5/8/PP1PPPPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS BLACK — Response to 1.c4")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"black")], C4_POS, "black")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.e4 e5 ─────────────────────────────────────────────────────
# After 1.e4 e5 — White to move (ep square may be e6 or -)
E4E5_BOARD = "rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.e4 e5 (Open Game)")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], E4E5_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.e4 c5 ─────────────────────────────────────────────────────
E4C5_BOARD = "rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.e4 c5 (Sicilian)")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], E4C5_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.e4 e6 (French) ─────────────────────────────────────────────
E4E6_BOARD = "rnbqkbnr/pppp1ppp/4p3/8/4P3/8/PPPP1PPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.e4 e6 (French)")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], E4E6_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.e4 c6 (Caro-Kann) ──────────────────────────────────────────
E4C6_BOARD = "rnbqkbnr/pp1ppppp/2p5/8/4P3/8/PPPP1PPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.e4 c6 (Caro-Kann)")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], E4C6_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.e4 d5 (Scandinavian) ──────────────────────────────────────
E4D5_BOARD = "rnbqkbnr/ppp1pppp/8/3p4/4P3/8/PPPP1PPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.e4 d5 (Scandinavian)")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], E4D5_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.d4 d5 ──────────────────────────────────────────────────────
D4D5_BOARD = "rnbqkbnr/ppp1pppp/8/3p4/3P4/8/PPP1PPPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.d4 d5")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], D4D5_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.d4 Nf6 ─────────────────────────────────────────────────────
D4NF6_BOARD = "rnbqkb1r/pppppppp/5n2/8/3P4/8/PPP1PPPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.d4 Nf6")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], D4NF6_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.d4 e5 ──────────────────────────────────────────────────────
D4E5_BOARD = "rnbqkbnr/pppp1ppp/8/4p3/3P4/8/PPP1PPPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.d4 e5 (Englund Gambit)")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], D4E5_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── White vs 1.d4 e6 ──────────────────────────────────────────────────────
D4E6_BOARD = "rnbqkbnr/pppp1ppp/4p3/8/3P4/8/PPP1PPPP/RNBQKBNR"
print(f"\n{SEP}")
print("AS WHITE — Response to 1.d4 e6")
print(SEP)
for ds in DATASETS:
    m, v = best(frames[(ds,"white")], D4E6_BOARD, "white")
    print(f"  {ds:>12}:  {m or '?':>5}  (val={v:.4f})" if m else f"  {ds:>12}:  ?")

# ── Also show the actual sample lines from the log files ─────────────────
print(f"\n{'═'*72}")
print("SAMPLE LINES from Stage 3 logs (Blitz 1650, as comparison)")
print(f"{'═'*72}")

import re
from pathlib import Path
LOG_DIR = Path("C:/Users/David/Documents/Chess/logs/convergence")

for ds in DATASETS:
    for persp in ["white", "black"]:
        logfile = LOG_DIR / f"stage3_{ds}_{persp}_{VTAG}.log"
        if not logfile.exists():
            continue
        text = logfile.read_text(encoding="utf-8")
        m = re.search(r"Sample line.*?:\n\s+(.*)", text)
        if m:
            print(f"  {ds}/{persp}: {m.group(1).strip()}")

print()
