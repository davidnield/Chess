"""
Quick trace of recommended lines at Blitz elo_band=2200 for key positions.
Compares 2016, 2025_jan, and 2025_apr repertoires.

Usage:
    python repertoire_trace.py [--fw 0.30 --ew 0.50]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

REP_DIR = Path("E:/chess/repertoire")

# Key positions to check (EPD format used by stage3)
# We'll discover the exact EPD format from the parquet itself.
STARTING_EPD_CANDIDATES = [
    "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
    "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
]

def load_slice(path: Path, event: str = "Blitz", elo_band: int = 2200) -> pl.DataFrame:
    df = pl.read_parquet(str(path))
    return df.filter(
        (pl.col("event") == event) & (pl.col("elo_band") == elo_band)
    )


def find_position(df: pl.DataFrame, epd_substr: str) -> pl.DataFrame:
    """Find rows whose position_epd contains the given substring."""
    return df.filter(pl.col("position_epd").str.contains(epd_substr, literal=True))


def get_best_move(df: pl.DataFrame, epd_substr: str) -> tuple[str | None, float | None, str | None]:
    """Return (best_move, value, full_epd) for the given position."""
    rows = find_position(df, epd_substr)
    if rows.is_empty():
        return None, None, None
    row = rows.row(0, named=True)
    return row.get("best_move"), row.get("value"), row.get("position_epd")


def get_move_options(df: pl.DataFrame, epd_substr: str, top_n: int = 5) -> list[dict]:
    """Get all moves at this position (all our-turn rows with this EPD), sorted by value."""
    rows = find_position(df, epd_substr)
    if rows.is_empty():
        return []
    # Only show rows where best_move is not null (our-turn)
    rows = rows.filter(pl.col("best_move").is_not_null())
    if rows.is_empty():
        return []
    row = rows.row(0, named=True)
    return [{"best_move": row.get("best_move"), "value": row.get("value"), "epd": row.get("position_epd")}]


def schema_check(path: Path):
    """Print schema and a few sample EPDs."""
    df = pl.read_parquet(str(path))
    print(f"\nSchema: {df.schema}")
    print(f"Columns: {df.columns}")
    # Sample EPDs to understand format
    sample = df.filter(pl.col("best_move").is_not_null()).head(3)
    for row in sample.iter_rows(named=True):
        print(f"  EPD: {row.get('position_epd')!r}  move={row.get('best_move')}  val={row.get('value'):.4f}")


def trace_line_from_start(df_w: pl.DataFrame, df_b: pl.DataFrame,
                           start_epd_substr: str, max_ply: int = 10) -> list[str]:
    """
    Trace the recommended line from a starting position, alternating
    white/black perspectives.
    Returns list of "moveN. white_move black_move" strings.
    """
    moves = []
    current_epd_substr = start_epd_substr
    for ply in range(max_ply):
        if ply % 2 == 0:
            # White to move
            move, val, full_epd = get_best_move(df_w, current_epd_substr)
        else:
            # Black to move
            move, val, full_epd = get_best_move(df_b, current_epd_substr)
        if move is None:
            break
        moves.append((ply, move, val, full_epd))
        # Next position: we need the EPD after this move.
        # We can't compute it directly without a chess engine, but we can
        # look for it in the opponent's parquet by finding children.
        # For now, stop after the first move per perspective.
        break  # We'll do it per-position instead
    return moves


def get_response_to(df: pl.DataFrame, after_move_epd_part: str) -> tuple[str | None, float | None]:
    """Find the recommended response at a position identified by a partial EPD."""
    m, v, _ = get_best_move(df, after_move_epd_part)
    return m, v


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fw", type=float, default=0.30)
    parser.add_argument("--ew", type=float, default=0.50)
    parser.add_argument("--event", default="Blitz")
    parser.add_argument("--elo", type=int, default=2200)
    parser.add_argument("--schema-only", action="store_true")
    args = parser.parse_args()

    fw_tag = f"f{int(args.fw*100):03d}"
    ew_tag = f"e{int(args.ew*100):03d}"
    vtag = f"{fw_tag}_{ew_tag}"

    datasets = ["2016", "2025_jan", "2025_apr"]

    # First pass: check schema from one file
    sample_path = REP_DIR / f"repertoire_2025_apr_white_{vtag}.parquet"
    if not sample_path.exists():
        print(f"ERROR: {sample_path} not found")
        sys.exit(1)

    if args.schema_only:
        schema_check(sample_path)
        return

    # Print schema info first
    schema_check(sample_path)

    # Load all datasets
    data = {}
    for ds in datasets:
        for persp in ["white", "black"]:
            p = REP_DIR / f"repertoire_{ds}_{persp}_{vtag}.parquet"
            if not p.exists():
                print(f"  MISSING: {p.name}")
                continue
            df = load_slice(p, args.event, args.elo)
            data[(ds, persp)] = df
            print(f"  Loaded {ds}/{persp}: {df.height:,} positions")

    print(f"\n{'='*72}")
    print(f"REPERTOIRE OVERVIEW — {args.event} elo={args.elo}  weights={vtag}")
    print(f"{'='*72}")

    # ── Starting position: White's first move ────────────────────────────────
    print(f"\n{'─'*72}")
    print("WHITE'S FIRST MOVE (starting position)")
    print(f"{'─'*72}")

    # Find starting position EPD
    start_epd = None
    for cand in STARTING_EPD_CANDIDATES:
        for ds in datasets:
            k = (ds, "white")
            if k not in data:
                continue
            rows = find_position(data[k], cand[:30])  # partial match
            if not rows.is_empty():
                start_epd = rows.row(0, named=True).get("position_epd")
                break
        if start_epd:
            break

    if not start_epd:
        # Try to find it by looking for shortest EPD with w to move
        for ds in datasets:
            k = (ds, "white")
            if k not in data:
                continue
            our_turn = data[k].filter(pl.col("best_move").is_not_null())
            # Starting pos should be among shortest EPDs
            with_len = our_turn.with_columns(
                pl.col("position_epd").str.len_bytes().alias("epd_len")
            ).sort("epd_len")
            if not with_len.is_empty():
                start_epd = with_len.row(0, named=True).get("position_epd")
                break

    if start_epd:
        print(f"  Starting EPD: {start_epd!r}")
        for ds in datasets:
            k = (ds, "white")
            if k not in data:
                continue
            m, v, _ = get_best_move(data[k], start_epd[:35])
            print(f"  {ds:>12}:  {m}  (value={v:.4f})" if m else f"  {ds:>12}:  not found")
    else:
        print("  Could not find starting position in parquet.")

    # ── Black responses to common White first moves ──────────────────────────
    print(f"\n{'─'*72}")
    print("BLACK'S RESPONSE TO COMMON WHITE FIRST MOVES")
    print(f"{'─'*72}")

    # We need to find positions after 1.e4, 1.d4, 1.Nf3, 1.c4
    # These will be in the BLACK perspective parquet (black's turn)
    # EPD patterns for positions after common first moves:
    after_e4_pattern = "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b"
    after_d4_pattern = "rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR b"
    after_nf3_pattern = "rnbqkbnr/pppppppp/8/8/8/5N2/PPPPPPPP/RNBQKB1R b"
    after_c4_pattern = "rnbqkbnr/pppppppp/8/8/2P5/8/PP1PPPPP/RNBQKBNR b"
    after_b4_pattern = "rnbqkbnr/pppppppp/8/8/1P6/8/P1PPPPPP/RNBQKBNR b"
    after_nc3_pattern = "rnbqkbnr/pppppppp/8/8/8/2N5/PPPPPPPP/R1BQKBNR b"

    move_patterns = [
        ("1.e4",  after_e4_pattern),
        ("1.d4",  after_d4_pattern),
        ("1.Nf3", after_nf3_pattern),
        ("1.c4",  after_c4_pattern),
        ("1.b4",  after_b4_pattern),
        ("1.Nc3", after_nc3_pattern),
    ]

    for move_name, pattern in move_patterns:
        found_any = False
        for ds in datasets:
            k = (ds, "black")
            if k not in data:
                continue
            m, v, _ = get_best_move(data[k], pattern[:45])
            if m:
                if not found_any:
                    print(f"\n  After {move_name}:")
                    found_any = True
                print(f"    {ds:>12}: Black plays  {m}  (value={v:.4f})")

    # ── Depth-2 White responses ──────────────────────────────────────────────
    # After 1.e4 e5, 1.e4 c5, 1.e4 e6, 1.e4 c6, 1.d4 d5, 1.d4 Nf6
    print(f"\n{'─'*72}")
    print("WHITE'S SECOND MOVE RESPONSES")
    print(f"{'─'*72}")

    second_move_patterns = [
        ("1.e4 e5",  "rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w"),
        ("1.e4 c5",  "rnbqkbnr/pp1ppppp/8/2p5/4P3/8/PPPP1PPP/RNBQKBNR w"),
        ("1.e4 e6",  "rnbqkbnr/pppp1ppp/4p3/8/4P3/8/PPPP1PPP/RNBQKBNR w"),
        ("1.e4 c6",  "rnbqkbnr/pp1ppppp/2p5/8/4P3/8/PPPP1PPP/RNBQKBNR w"),
        ("1.e4 d5",  "rnbqkbnr/ppp1pppp/8/3p4/4P3/8/PPPP1PPP/RNBQKBNR w"),
        ("1.e4 d6",  "rnbqkbnr/ppp1pppp/3p4/8/4P3/8/PPPP1PPP/RNBQKBNR w"),
        ("1.d4 d5",  "rnbqkbnr/ppp1pppp/8/3p4/3P4/8/PPP1PPPP/RNBQKBNR w"),
        ("1.d4 Nf6", "rnbqkb1r/pppppppp/5n2/8/3P4/8/PPP1PPPP/RNBQKBNR w"),
        ("1.d4 f5",  "rnbqkbnr/ppppp1pp/8/5p2/3P4/8/PPP1PPPP/RNBQKBNR w"),
        ("1.d4 e6",  "rnbqkbnr/pppp1ppp/4p3/8/3P4/8/PPP1PPPP/RNBQKBNR w"),
    ]

    for move_seq, pattern in second_move_patterns:
        found_any = False
        for ds in datasets:
            k = (ds, "white")
            if k not in data:
                continue
            m, v, _ = get_best_move(data[k], pattern[:45])
            if m:
                if not found_any:
                    print(f"\n  After {move_seq}:")
                    found_any = True
                print(f"    {ds:>12}: White plays  {m}  (value={v:.4f})")

    # ── Sample lines from logs ───────────────────────────────────────────────
    print(f"\n{'─'*72}")
    print("FULL SAMPLE LINES (from stage3 logs, Blitz 1650)")
    print(f"{'─'*72}")


if __name__ == "__main__":
    main()
