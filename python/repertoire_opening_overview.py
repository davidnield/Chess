"""
High-level opening overview of the repertoire across params / elo / year.

For Blitz, both perspectives, both 2024 and 2025, all 6 (fw,ew) param combos,
all 5 elo bands: probe OUR recommended move at a fixed set of canonical
opening-defining positions (generated via python-chess so the EPD keys match
position_epd exactly). Prints compact, greppable lines.

Usage:
    python repertoire_opening_overview.py [white|black]
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import chess

REP_DIR = Path("E:/chess/repertoire")
ELOS = [1100, 1400, 1650, 1900, 2200]
FW = [0.0, 0.30]
EW = [0.0, 0.50, 1.0]
YEARS = ["2024", "2025"]


def epd(*sans: str) -> str:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b.epd()


# Canonical White-to-move probes (characterize White's systems)
WHITE_PROBES = {
    "1.": epd(),                              # first move
    "vs 1...e5":   epd("e4", "e5"),           # Open game choice
    "vs Sicilian": epd("e4", "c5"),           # anti-Sicilian choice
    "vs French":   epd("e4", "e6"),
    "vs Caro":     epd("e4", "c6"),
    "vs 1...d5":   epd("d4", "d5"),           # QG / London / BDG
    "vs 1...Nf6":  epd("d4", "Nf6"),
    "d4Nf6c4e6_3": epd("d4","Nf6","c4","e6"), # allow Nimzo (Nc3) or avoid
    "d4Nf6c4g6_3": epd("d4","Nf6","c4","g6"), # vs KID/Grunfeld
    "d4d5c4e6_3":  epd("d4","d5","c4","e6"),  # QGD main
}

# Canonical Black-to-move probes (characterize Black's defenses)
BLACK_PROBES = {
    "vs 1.e4":  epd("e4"),                                   # defense to e4
    "vs 1.d4":  epd("d4"),                                   # defense to d4
    "vs 1.c4":  epd("c4"),                                   # vs English
    "vs 1.Nf3": epd("Nf3"),
    "e4e5Nf3_2": epd("e4","e5","Nf3"),                       # Nc6 / Petroff / Philidor
    "e4c5Nf3_2": epd("e4","c5","Nf3"),                       # Sicilian flavor
    "d4Nf6c4_2": epd("d4","Nf6","c4"),                       # e6/g6/c5 Benoni
    "d4Nf6c4e6Nc3_3": epd("d4","Nf6","c4","e6","Nc3"),       # Nimzo (Bb4) / QGD (d5)
    "d4Nf6c4g6Nc3_3": epd("d4","Nf6","c4","g6","Nc3"),       # Grunfeld (d5) / KID (Bg7)
    "d4d5c4_2": epd("d4","d5","c4"),                         # QGD/Slav/QGA
}


def probe_file(path: Path, probes: dict[str, str]) -> dict:
    """Return {elo: {label: (move, value)}} for the probe EPDs."""
    epds = list(set(probes.values()))
    df = pl.read_parquet(path, columns=["event", "elo_band", "position_epd",
                                         "best_move", "value"])
    df = df.filter((pl.col("event") == "Blitz")
                   & pl.col("position_epd").is_in(epds)
                   & pl.col("best_move").is_not_null())
    out: dict = {e: {} for e in ELOS}
    # build lookup
    lut = {}
    for r in df.iter_rows(named=True):
        lut[(r["elo_band"], r["position_epd"])] = (r["best_move"], r["value"])
    for e in ELOS:
        for label, ep in probes.items():
            out[e][label] = lut.get((e, ep), (None, None))
    return out


def main():
    persp = sys.argv[1] if len(sys.argv) > 1 else "white"
    probes = WHITE_PROBES if persp == "white" else BLACK_PROBES
    labels = list(probes.keys())

    for year in YEARS:
        for fw in FW:
            for ew in EW:
                tag = f"f{int(fw*100):03d}_e{int(ew*100):03d}"
                path = REP_DIR / f"repertoire_{year}_{persp}_{tag}.parquet"
                if not path.exists():
                    print(f"MISSING {path.name}")
                    continue
                res = probe_file(path, probes)
                for e in ELOS:
                    cells = []
                    for lab in labels:
                        mv, _ = res[e][lab]
                        cells.append(f"{lab}={mv if mv else '-'}")
                    print(f"{year} {persp:5} {tag} elo{e}: " + " | ".join(cells))
    print("LABELS:", " ; ".join(labels))


if __name__ == "__main__":
    main()
