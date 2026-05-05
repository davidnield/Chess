"""
Stage 3+ analysis: how often does the recommended repertoire actually cover
the user's games?

For each (event, elo_band) slice we walk the recommended tree forward from
the start position, tracking the fraction of opponents' games that stay in
our prepared lines at each ply.

Algorithm (forward flow on the position DAG):

    At each our-turn node we play best_move; weight passes through 1:1.
    At each opponent-turn node we split the incoming weight by the empirical
    distribution of opp moves (move.total / sum(move.total over all replies)).
    Weight that flows to a child we cover stays in the tree; weight on moves
    leading outside our repertoire is treated as a leak.

Outputs per slice:
  * Coverage curve at ply 4 / 8 / 12 / 16  (cumulative % of games still in tree)
  * Top 5 exit points -- positions where the largest absolute number of games
    leave the tree, with the top 3 unrecognized opponent replies
  * Tree size: distinct positions reached, our-turn decisions, max depth
    where >= --min-games games are still in the tree

Cross-slice summary table at the end shows ply-8 and ply-16 coverage per slice.

Usage:
    .venv/Scripts/python.exe python/analyze_coverage.py \\
        --repertoire E:/chess/repertoire/repertoire_2016_white_w000.parquet \\
        --stats      E:/chess/position-stats/position_stats_2016.parquet \\
        --perspective white

    # Capture the report to a file:
    .venv/Scripts/python.exe python/analyze_coverage.py ... --output report.txt
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64

# Plies at which to report coverage in the per-slice summary.
REPORT_PLIES = (4, 8, 12, 16)


def zobrist_int64(b: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(b)
    return h - INT64_RANGE if h > INT64_MAX else h


# ── Per-slice coverage walk ────────────────────────────────────────────────

def analyze_slice(
    rep_slice: pl.DataFrame,
    stats_slice: pl.DataFrame,
    perspective: str,
    max_depth: int,
    min_games: int,
) -> dict:
    """Walk the recommended tree for one slice and return coverage stats."""
    our_color  = chess.WHITE if perspective == "white" else chess.BLACK
    start_hash = zobrist_int64(chess.Board())

    rep_idx = {r["position_hash"]: r for r in rep_slice.iter_rows(named=True)}

    # Index opponent moves: parent_hash -> [(san, total, child_hash), ...]
    opp_idx: dict[int, list[tuple[str, int, int | None]]] = defaultdict(list)
    for r in stats_slice.iter_rows(named=True):
        opp_idx[r["parent_hash"]].append(
            (r["move_san"], r["total"], r.get("child_hash"))
        )

    # Slice-level total games: sum of all first-move totals from start.
    slice_total = sum(t for _, t, _ in opp_idx.get(start_hash, []))

    # Forward-flow BFS by depth. weight_at[(depth, position_hash)] accumulates
    # the fraction of starting games reaching that node via our prepared lines.
    weight_at: dict[tuple[int, int], float] = {(0, start_hash): 1.0}
    by_depth: dict[int, list[int]] = defaultdict(list)
    by_depth[0].append(start_hash)

    # Path tracking for exit-point reporting (move sequence to each position).
    path_to: dict[int, list[str]] = {start_hash: []}

    # Leak records: (depth, position_hash, abs_games_leaking, top_3_uncovered)
    exits: list[tuple[int, int, float, list[tuple[str, int]]]] = []

    our_turn_positions: set[int] = set()

    for depth in range(max_depth):
        for ph in by_depth[depth]:
            weight_here = weight_at.get((depth, ph), 0.0)
            if weight_here == 0:
                continue

            row = rep_idx.get(ph)
            if row is None:
                # Outside our prep -- can happen if rep doesn't have this exact
                # opp-turn position (rare; means upstream walk missed it).
                continue

            board = chess.Board(row["position_epd"])
            is_our_turn = board.turn == our_color

            if is_our_turn:
                our_turn_positions.add(ph)
                best = row["best_move"]
                if best is None:
                    continue
                try:
                    board.push(board.parse_san(best))
                except (ValueError, AssertionError):
                    continue
                ch = zobrist_int64(board)
                key = (depth + 1, ch)
                if key not in weight_at:
                    weight_at[key] = 0.0
                    by_depth[depth + 1].append(ch)
                    if ch not in path_to:
                        path_to[ch] = path_to[ph] + [best]
                weight_at[key] += weight_here
            else:
                # Opponent's turn -- split by empirical distribution.
                moves = opp_idx.get(ph, [])
                total_at_node = sum(t for _, t, _ in moves)
                if total_at_node == 0:
                    continue

                covered_total = 0
                uncovered: list[tuple[str, int]] = []
                for san, t, ch in moves:
                    if ch is not None and ch in rep_idx:
                        covered_total += t
                        frac = t / total_at_node
                        key = (depth + 1, ch)
                        if key not in weight_at:
                            weight_at[key] = 0.0
                            by_depth[depth + 1].append(ch)
                            if ch not in path_to:
                                path_to[ch] = path_to[ph] + [san]
                        weight_at[key] += weight_here * frac
                    else:
                        uncovered.append((san, t))

                if uncovered:
                    leak_frac     = sum(t for _, t in uncovered) / total_at_node
                    abs_leaking   = weight_here * slice_total * leak_frac
                    uncovered.sort(key=lambda x: -x[1])
                    exits.append((depth, ph, abs_leaking, uncovered[:3]))

    # ── Aggregations ──────────────────────────────────────────────────────────
    coverage_by_ply: dict[int, float] = {}
    for d in range(max_depth + 1):
        coverage_by_ply[d] = sum(weight_at.get((d, ph), 0.0) for ph in by_depth[d])

    # Max depth where >= min_games games are still in the tree (in absolute terms)
    max_useful_depth = 0
    for d in sorted(coverage_by_ply.keys()):
        if coverage_by_ply[d] * slice_total >= min_games:
            max_useful_depth = d

    # Tree size
    distinct_positions = len({ph for d in by_depth for ph in by_depth[d]})

    # Top 5 exits by absolute games
    exits.sort(key=lambda x: -x[2])
    top_exits = []
    for depth, ph, abs_leaking, top3 in exits[:5]:
        line = path_to.get(ph, [])
        top_exits.append({
            "depth":       depth,
            "position":    ph,
            "line_san":    line,
            "n_leaking":   abs_leaking,
            "top3":        top3,
        })

    return {
        "slice_total":        slice_total,
        "coverage_by_ply":    coverage_by_ply,
        "max_useful_depth":   max_useful_depth,
        "distinct_positions": distinct_positions,
        "our_turn_decisions": len(our_turn_positions),
        "top_exits":          top_exits,
    }


# ── Reporting ──────────────────────────────────────────────────────────────

def render_slice_report(ev: str, eb: int, result: dict) -> str:
    lines: list[str] = []
    lines.append(f"\n{'='*100}")
    lines.append(f"SLICE: {ev}  elo_band {eb}")
    lines.append(f"{'='*100}")
    lines.append(f"  slice_total games:       {result['slice_total']:>12,}")
    lines.append(f"  distinct positions:      {result['distinct_positions']:>12,}")
    lines.append(f"  our-turn decisions:      {result['our_turn_decisions']:>12,}")
    lines.append(f"  max useful depth:        ply {result['max_useful_depth']}  "
                 f"(games-in-tree drops below threshold beyond this)")

    cov = result["coverage_by_ply"]
    lines.append(f"\n  Coverage curve (cumulative % of games still in our tree):")
    for p in REPORT_PLIES:
        if p in cov:
            pct  = cov[p] * 100
            absn = cov[p] * result["slice_total"]
            lines.append(f"    ply {p:>2}:  {pct:>5.1f}%   ({absn:>12,.0f} games)")

    if result["top_exits"]:
        lines.append(f"\n  Top {len(result['top_exits'])} exit points (positions where the most games leave our tree):")
        for i, ex in enumerate(result["top_exits"], 1):
            line = " ".join(ex["line_san"]) if ex["line_san"] else "(start)"
            top3 = ", ".join(f"{san}({t:,})" for san, t in ex["top3"])
            lines.append(f"    {i}. ply {ex['depth']}: {line}")
            lines.append(f"       leaks ~{ex['n_leaking']:>12,.0f} games. "
                         f"Top uncovered replies: {top3}")
    return "\n".join(lines)


def render_summary_table(per_slice: dict) -> str:
    """Cross-slice ply-8 / ply-16 coverage table."""
    lines = []
    lines.append(f"\n{'='*100}")
    lines.append("CROSS-SLICE COVERAGE SUMMARY")
    lines.append(f"{'='*100}\n")
    lines.append(f"  {'event':<14} {'elo':>5}   {'games':>12}   "
                 f"{'ply 4':>7}  {'ply 8':>7}  {'ply 12':>7}  {'ply 16':>7}   "
                 f"{'max':>5}")
    lines.append("  " + "-" * 88)
    for (ev, eb), r in sorted(per_slice.items()):
        cov = r["coverage_by_ply"]
        row = f"  {ev:<14} {eb:>5}   {r['slice_total']:>12,}"
        for p in REPORT_PLIES:
            pct = cov.get(p, 0.0) * 100
            row += f"   {pct:>5.1f}%"
        row += f"    ply {r['max_useful_depth']}"
        lines.append(row)
    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--repertoire", required=True,
                        help="Path to a Stage 3 repertoire parquet")
    parser.add_argument("--stats",      required=True,
                        help="Path to the matching Stage 2 stats parquet "
                             "(must include child_hash column)")
    parser.add_argument("--perspective", choices=["white", "black"], required=True)
    parser.add_argument("--min-games",   type=int, default=100,
                        help="Threshold for 'max useful depth' (default: 100)")
    parser.add_argument("--max-ply",     type=int, default=20,
                        help="Walk depth in plies from start (default: 20)")
    parser.add_argument("--output",      default=None,
                        help="Optional path to also write the rendered report to")
    args = parser.parse_args()

    rep_path   = Path(args.repertoire)
    stats_path = Path(args.stats)

    print(f"Repertoire:    {rep_path}")
    print(f"Stats:         {stats_path}")
    print(f"Perspective:   {args.perspective}")
    print(f"Max ply:       {args.max_ply}")
    print(f"Min games:     {args.min_games}\n")

    rep   = pl.read_parquet(rep_path)
    stats = pl.read_parquet(stats_path)

    if "child_hash" not in stats.columns:
        print("ERROR: stats parquet missing child_hash column. "
              "Re-run Stage 2 with the updated stage2_aggregate.py.",
              file=sys.stderr)
        sys.exit(2)

    slices = (
        rep.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
    )

    per_slice: dict[tuple[str, int], dict] = {}
    chunks: list[str] = []

    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        rep_slice = rep.filter(
            (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        )
        stats_slice = stats.filter(
            (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        )
        result = analyze_slice(
            rep_slice, stats_slice,
            perspective=args.perspective,
            max_depth=args.max_ply,
            min_games=args.min_games,
        )
        per_slice[(ev, eb)] = result
        chunks.append(render_slice_report(ev, eb, result))

    chunks.append(render_summary_table(per_slice))
    full_report = "\n".join(chunks)
    print(full_report)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(full_report, encoding="utf-8")
        print(f"\nReport saved to: {out}")


if __name__ == "__main__":
    main()
