"""
Deep-dive on the Blitz elo-1900 band: variance in recommendations across
f (0, 0.30), e (0, 0.50, 1.0), and year (2024, 2025), for White and Black.

Outputs:
  (A) Decision matrix — for each opening-defining position, the move chosen in
      each of the 12 (year,f,e) cells, with the position value.
  (B) Principal lines — repertoire-vs-repertoire PV (White rep supplies White
      moves, Black rep of same params supplies Black replies) for selected cells.

Usage: python repertoire_1900_variance.py
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

REP_DIR = Path("E:/chess/repertoire")
ELO = 1900
PARAMS = [(0.0, 0.0), (0.0, 0.50), (0.0, 1.0), (0.30, 0.0), (0.30, 0.50), (0.30, 1.0)]
YEARS = ["2024", "2025"]


def tag(fw, ew):
    return f"f{int(fw*100):03d}_e{int(ew*100):03d}"


def epd(*sans):
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b.epd()


WHITE_PROBES = {
    "1st move":        epd(),
    "vs 1...e5":       epd("e4", "e5"),
    "vs Sicilian":     epd("e4", "c5"),
    "vs French":       epd("e4", "e6"),
    "vs Caro":         epd("e4", "c6"),
    "vs 1...d5":       epd("d4", "d5"),
    "vs 1...Nf6":      epd("d4", "Nf6"),
    "d4Nf6c4e6 (Nimzo?)": epd("d4", "Nf6", "c4", "e6"),
    "d4Nf6c4g6 (KID?)":   epd("d4", "Nf6", "c4", "g6"),
    "QGD d4d5c4e6":    epd("d4", "d5", "c4", "e6"),
}
BLACK_PROBES = {
    "vs 1.e4":         epd("e4"),
    "vs 1.d4":         epd("d4"),
    "vs 1.c4":         epd("c4"),
    "vs 1.Nf3":        epd("Nf3"),
    "1.e4 e5 2.Nf3":   epd("e4", "e5", "Nf3"),
    "1.e4 c5 2.Nf3":   epd("e4", "c5", "Nf3"),
    "1.d4 Nf6 2.c4":   epd("d4", "Nf6", "c4"),
    "...e6 3.Nc3 (Nimzo?)": epd("d4", "Nf6", "c4", "e6", "Nc3"),
    "...g6 3.Nc3 (Grunf?)": epd("d4", "Nf6", "c4", "g6", "Nc3"),
    "1.d4 d5 2.c4":    epd("d4", "d5", "c4"),
}


def load_lut(persp, year, fw, ew):
    """Return {epd: (move, value)} for Blitz/1900 our-turn rows."""
    p = REP_DIR / f"repertoire_{year}_{persp}_{tag(fw,ew)}.parquet"
    if not p.exists():
        return None
    df = pl.read_parquet(p, columns=["event", "elo_band", "position_epd",
                                      "best_move", "value"])
    df = df.filter((pl.col("event") == "Blitz") & (pl.col("elo_band") == ELO)
                   & pl.col("best_move").is_not_null())
    return {r["position_epd"]: (r["best_move"], r["value"])
            for r in df.iter_rows(named=True)}


def decision_matrix(persp, probes):
    print(f"\n{'='*100}\n(A) {persp.upper()} — Blitz 1900 decision matrix "
          f"(move @ position value)\n{'='*100}")
    # header
    cols = [(y, fw, ew) for y in YEARS for (fw, ew) in PARAMS]
    luts = {}
    for (y, fw, ew) in cols:
        luts[(y, fw, ew)] = load_lut(persp, y, fw, ew) or {}
    hdr = "decision".ljust(22)
    for (y, fw, ew) in cols:
        hdr += f"{y[2:]}/{tag(fw,ew)[1:]}".rjust(11)
    print(hdr)
    for label, ep in probes.items():
        line = label.ljust(22)
        moves_seen = []
        for (y, fw, ew) in cols:
            mv, val = luts[(y, fw, ew)].get(ep, (None, None))
            moves_seen.append(mv)
            line += (mv or "-").rjust(11)
        # variance metric: distinct non-null moves
        distinct = len(set(m for m in moves_seen if m))
        line += f"   [{distinct} distinct]"
        print(line)


def trace_pv(year, fw, ew, max_ply=12):
    """Repertoire-vs-repertoire PV: white rep -> white moves, black rep -> black."""
    w = load_lut("white", year, fw, ew) or {}
    b = load_lut("black", year, fw, ew) or {}
    board = chess.Board()
    sans = []
    for ply in range(max_ply):
        lut = w if board.turn == chess.WHITE else b
        rec = lut.get(board.epd())
        if not rec or rec[0] is None:
            break
        mv = rec[0]
        try:
            board.push_san(mv)
        except Exception:
            break
        sans.append(mv)
    # format as 1.e4 e5 2.Nf3 ...
    out = []
    for i, s in enumerate(sans):
        if i % 2 == 0:
            out.append(f"{i//2+1}.{s}")
        else:
            out.append(s)
    return " ".join(out)


def main():
    decision_matrix("white", WHITE_PROBES)
    decision_matrix("black", BLACK_PROBES)
    print(f"\n{'='*100}\n(B) PRINCIPAL LINES (rep vs rep, same params) — Blitz 1900"
          f"\n{'='*100}")
    for year in YEARS:
        for (fw, ew) in PARAMS:
            pv = trace_pv(year, fw, ew)
            print(f"{year} {tag(fw,ew)}: {pv}")


if __name__ == "__main__":
    main()
