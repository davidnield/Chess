"""
Convergence comparison: compare repertoires across datasets.

Loads matching repertoire files from different datasets (e.g. 2016, Jan 2025,
Apr 2025) and computes agreement rates, value correlations, and key
disagreements to assess statistical convergence and meta drift.

Usage:
    .venv/Scripts/python.exe python/convergence_compare.py
    .venv/Scripts/python.exe python/convergence_compare.py --repertoire-dir E:/chess/repertoire
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_REP_DIR = Path("E:/chess/repertoire")

# ── Filename parsing ─────────────────────────────────────────────────────────

FILENAME_RE = re.compile(
    r"^repertoire_(?P<dataset>[a-z0-9_]+?)_(?P<perspective>white|black)"
    r"_f(?P<fw>\d{3})_e(?P<ew>\d{3})\.parquet$"
)


def parse_filename(name: str) -> dict | None:
    """Parse repertoire filename into metadata dict."""
    m = FILENAME_RE.match(name)
    if not m:
        return None
    return {
        "dataset":     m.group("dataset"),
        "perspective": m.group("perspective"),
        "fw":          int(m.group("fw")) / 100,
        "ew":          int(m.group("ew")) / 100,
    }


def ply_from_epd(epd: str) -> int:
    """Extract ply number from an EPD/FEN string.

    FEN has 6 fields: pieces side castling ep halfmove fullmove.
    EPD may have 4 fields (no halfmove/fullmove).
    Ply = (fullmove - 1) * 2 + (1 if black to move else 0).
    """
    parts = epd.split()
    side = parts[1] if len(parts) > 1 else "w"
    try:
        fullmove = int(parts[5]) if len(parts) >= 6 else 1
    except (ValueError, IndexError):
        fullmove = 1
    return (fullmove - 1) * 2 + (1 if side == "b" else 0)


# ── Comparison logic ─────────────────────────────────────────────────────────

def compare_pair(
    df_a: pl.DataFrame,
    df_b: pl.DataFrame,
    label_a: str,
    label_b: str,
    lines: list[str],
) -> dict:
    """Compare two repertoire DataFrames, appending results to `lines`.

    Both DataFrames are expected to have: position_hash, position_epd,
    side_to_move, value, best_move, forcingness, eval_score.

    Returns a summary dict with key metrics.
    """
    # Use the largest (event, elo_band) slice for a clean comparison.
    # Pick the slice present in BOTH frames with the most positions.
    slices_a = df_a.select(["event", "elo_band"]).unique()
    slices_b = df_b.select(["event", "elo_band"]).unique()
    common_slices = slices_a.join(slices_b, on=["event", "elo_band"])

    if common_slices.is_empty():
        lines.append(f"  No common (event, elo_band) slices — skipping.")
        return {}

    # Pick the slice with the most positions in df_a
    best_slice = None
    best_count = 0
    for row in common_slices.iter_rows(named=True):
        ev, eb = row["event"], row["elo_band"]
        count = df_a.filter(
            (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        ).height
        if count > best_count:
            best_count = count
            best_slice = (ev, eb)

    ev, eb = best_slice
    a = df_a.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
    b = df_b.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))

    lines.append(f"  Slice: {ev} / elo {eb:,}")
    lines.append(f"  Positions: {label_a}={a.height:,}, {label_b}={b.height:,}")

    # Join on position_hash
    joined = a.join(b, on="position_hash", suffix="_b")
    lines.append(f"  Shared positions: {joined.height:,}")

    if joined.is_empty():
        lines.append("  No shared positions — cannot compare.")
        return {}

    # ── Value comparison (all positions) ──────────────────────────────────
    vals_a = joined["value"].to_list()
    vals_b = joined["value_b"].to_list()
    abs_diffs = [abs(va - vb) for va, vb in zip(vals_a, vals_b)]
    mean_abs_diff = sum(abs_diffs) / len(abs_diffs)

    # Pearson correlation
    n = len(vals_a)
    ma = sum(vals_a) / n
    mb = sum(vals_b) / n
    cov = sum((va - ma) * (vb - mb) for va, vb in zip(vals_a, vals_b)) / n
    std_a = (sum((va - ma) ** 2 for va in vals_a) / n) ** 0.5
    std_b = (sum((vb - mb) ** 2 for vb in vals_b) / n) ** 0.5
    pearson = cov / (std_a * std_b) if std_a > 0 and std_b > 0 else 0.0

    lines.append(f"  Value correlation (Pearson): {pearson:.4f}")
    lines.append(f"  Value mean |diff|: {mean_abs_diff:.4f}")

    # ── Move agreement (our-turn positions only) ──────────────────────────
    our_turn = joined.filter(
        pl.col("best_move").is_not_null() & pl.col("best_move_b").is_not_null()
    )
    n_our = our_turn.height

    if n_our == 0:
        lines.append("  No our-turn positions in common — cannot compare moves.")
        return {"pearson": pearson, "mean_abs_diff": mean_abs_diff}

    agree = our_turn.filter(pl.col("best_move") == pl.col("best_move_b"))
    n_agree = agree.height
    pct = n_agree / n_our * 100

    lines.append(f"  Our-turn positions: {n_our:,}")
    lines.append(f"  Move agreement: {n_agree:,}/{n_our:,} ({pct:.1f}%)")

    # ── Depth-stratified agreement ────────────────────────────────────────
    our_with_ply = our_turn.with_columns(
        pl.col("position_epd").map_elements(ply_from_epd, return_dtype=pl.Int32).alias("ply")
    )

    lines.append(f"\n  Depth-stratified move agreement:")
    lines.append(f"  {'Ply':>6}  {'Agree':>6}  {'Total':>6}  {'Rate':>7}")
    lines.append(f"  {'─'*6}  {'─'*6}  {'─'*6}  {'─'*7}")

    ply_buckets = [(0, 2), (3, 4), (5, 6), (7, 8), (9, 10), (11, 12), (13, 30)]
    for lo, hi in ply_buckets:
        bucket = our_with_ply.filter(
            (pl.col("ply") >= lo) & (pl.col("ply") <= hi)
        )
        if bucket.is_empty():
            continue
        bucket_agree = bucket.filter(pl.col("best_move") == pl.col("best_move_b")).height
        bucket_total = bucket.height
        label = f"{lo}-{hi}" if lo != hi else str(lo)
        rate = bucket_agree / bucket_total * 100 if bucket_total > 0 else 0
        lines.append(f"  {label:>6}  {bucket_agree:>6}  {bucket_total:>6}  {rate:>6.1f}%")

    # ── Top disagreements ─────────────────────────────────────────────────
    disagree = our_with_ply.filter(pl.col("best_move") != pl.col("best_move_b"))
    if disagree.height > 0:
        # Sort by ply (lower ply = more important, more games)
        top = disagree.sort("ply").head(20)
        lines.append(f"\n  Top {min(20, disagree.height)} disagreements (by ply):")
        lines.append(f"  {'Ply':>4}  {'Move A':>8}  {'Val A':>6}  {'Move B':>8}  {'Val B':>6}  EPD")
        lines.append(f"  {'─'*4}  {'─'*8}  {'─'*6}  {'─'*8}  {'─'*6}  {'─'*40}")
        for row in top.iter_rows(named=True):
            lines.append(
                f"  {row['ply']:>4}  {row['best_move']:>8}  {row['value']:.4f}"
                f"  {row['best_move_b']:>8}  {row['value_b']:.4f}"
                f"  {row['position_epd'][:50]}"
            )

    return {
        "pearson": pearson,
        "mean_abs_diff": mean_abs_diff,
        "n_shared": joined.height,
        "n_our_turn": n_our,
        "n_agree": n_agree,
        "agree_pct": pct,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--repertoire-dir", default=str(DEFAULT_REP_DIR),
                        help=f"Directory containing repertoire parquet files "
                             f"(default: {DEFAULT_REP_DIR})")
    parser.add_argument("--output", default=None,
                        help="Write report to this file (default: stdout only)")
    args = parser.parse_args()

    rep_dir = Path(args.repertoire_dir)
    if not rep_dir.exists():
        print(f"ERROR: repertoire directory not found: {rep_dir}")
        sys.exit(1)

    # ── Discover repertoire files ─────────────────────────────────────────
    files: dict[tuple, dict] = {}  # (dataset, perspective, fw, ew) -> {meta, path}
    for f in sorted(rep_dir.glob("repertoire_*.parquet")):
        meta = parse_filename(f.name)
        if meta is None:
            continue
        key = (meta["dataset"], meta["perspective"], meta["fw"], meta["ew"])
        files[key] = {"meta": meta, "path": f}

    datasets = sorted(set(k[0] for k in files))
    perspectives = sorted(set(k[1] for k in files))
    weight_combos = sorted(set((k[2], k[3]) for k in files))

    print(f"Repertoire directory: {rep_dir}")
    print(f"Datasets found: {', '.join(datasets)}")
    print(f"Perspectives: {', '.join(perspectives)}")
    print(f"Weight combinations: {len(weight_combos)}")
    print(f"Total files: {len(files)}")

    if len(datasets) < 2:
        print("\nERROR: Need at least 2 datasets to compare.")
        sys.exit(1)

    # ── Define comparison pairs ───────────────────────────────────────────
    # Order matters: first pair is the most interesting (same-era convergence)
    pair_defs = [
        ("2025_jan", "2025_apr", "Same-era convergence"),
        ("2016",     "2025_apr", "Cross-era (2016 vs Apr 2025)"),
        ("2016",     "2025_jan", "Cross-era (2016 vs Jan 2025)"),
    ]

    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("  CONVERGENCE ANALYSIS REPORT")
    lines.append("=" * 72)

    summary_rows: list[dict] = []

    for perspective in perspectives:
        lines.append(f"\n{'#' * 72}")
        lines.append(f"# Perspective: {perspective.upper()}")
        lines.append(f"{'#' * 72}")

        for fw, ew in weight_combos:
            lines.append(f"\n{'─' * 72}")
            lines.append(f"  forcing_weight={fw:.2f}  eval_weight={ew:.2f}")
            lines.append(f"{'─' * 72}")

            for ds_a, ds_b, pair_label in pair_defs:
                key_a = (ds_a, perspective, fw, ew)
                key_b = (ds_b, perspective, fw, ew)

                if key_a not in files or key_b not in files:
                    continue

                lines.append(f"\n  ▸ {pair_label}: {ds_a} vs {ds_b}")

                df_a = pl.read_parquet(str(files[key_a]["path"]))
                df_b = pl.read_parquet(str(files[key_b]["path"]))

                metrics = compare_pair(df_a, df_b, ds_a, ds_b, lines)

                if metrics:
                    summary_rows.append({
                        "perspective": perspective,
                        "fw": fw,
                        "ew": ew,
                        "pair": f"{ds_a} vs {ds_b}",
                        **metrics,
                    })

    # ── Summary table ─────────────────────────────────────────────────────
    if summary_rows:
        lines.append(f"\n{'=' * 72}")
        lines.append("  SUMMARY")
        lines.append(f"{'=' * 72}")
        lines.append(
            f"  {'Perspective':>11}  {'fw':>5}  {'ew':>5}  {'Pair':>28}  "
            f"{'Agree%':>7}  {'Pearson':>8}  {'|Δv|':>6}"
        )
        lines.append(f"  {'─' * 11}  {'─' * 5}  {'─' * 5}  {'─' * 28}  {'─' * 7}  {'─' * 8}  {'─' * 6}")
        for r in summary_rows:
            agree = f"{r.get('agree_pct', 0):.1f}%" if "agree_pct" in r else "n/a"
            lines.append(
                f"  {r['perspective']:>11}  {r['fw']:>5.2f}  {r['ew']:>5.2f}  {r['pair']:>28}  "
                f"{agree:>7}  {r.get('pearson', 0):>8.4f}  {r.get('mean_abs_diff', 0):>6.4f}"
            )

    # ── Output ────────────────────────────────────────────────────────────
    report = "\n".join(lines)
    print(report)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print(f"\nReport written to {out_path}")


if __name__ == "__main__":
    main()
