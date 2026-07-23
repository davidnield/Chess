"""
Repertoire scorecard -- roll the per-position Stage-3 metrics up into a few
frequency-weighted, whole-repertoire numbers, in the spirit of Chessbook's
Effectiveness / Soundness statistics.

Why this exists
---------------
Stage 3 already stores, per position, the propagated values (all as expected
WHITE score in [0,1]):
  * value         -- expected white_score vs the AVERAGE (empirical) opponent
  * value_robust  -- expected white_score along the opponent's BEST defence
  * value_worst   -- our PREPARED book vs the opponent's best defence (the gate)
  * eval_score    -- raw Stockfish expected white_score at this position (or null)

Because backwards induction averages children at opponent nodes by empirical
frequency, the value at the START position is ALREADY the frequency-weighted
expected score of the whole tree -- i.e. exactly Chessbook's "Effectiveness"
(value@start), with value_robust@start a best-defence "Soundness" and
value_worst@start the worst-case our-book number we gate on. This script
surfaces those rollups and, when the matching Stage-2 stats are supplied, runs a
forward-flow pass (the analyze_coverage.py walk) that adds:
  * Soundness (freq-weighted eval) -- reach-weighted mean engine eval of the
    positions actually reached (the Chessbook-faithful frequency-weighted eval),
  * Coverage -- fraction of games still in book at ply 8 / 16,
  * Prep depth -- reach-weighted mean ply at which a game leaves our prep,
  * Size -- our-turn decisions to memorise, booked moves/game, distinct positions.

Use it to compare whole repertoire VARIANTS at a glance -- e.g. when sweeping
--cover-weight: "A: eff 54.8% / sound 52.1% / 238 lines" vs
"B: eff 54.1% / sound 53.0% / 181 lines". It is a REPORTING layer only -- it
changes no selection. Every white-score quantity is flipped to our perspective
(black: x -> 1-x).

Usage:
    # Headline rollups + raw size (rep only -- fast, tiny memory):
    .venv/Scripts/python.exe python/score_repertoire.py \\
        --repertoire E:/chess/repertoire/repertoire_pooled_white_sharp.parquet \\
        --perspective white

    # Full scorecard (+ coverage, freq-weighted eval Soundness, reach-weighted size):
    .venv/Scripts/python.exe python/score_repertoire.py \\
        --repertoire E:/chess/repertoire/repertoire_pooled_white_sharp.parquet \\
        --perspective white \\
        --stats E:/chess/position-stats/position_stats_pooled_ge1800_2019_2025_brc.parquet

    # Capture to a file and tag the machine-readable SUMMARY line:
    .venv/Scripts/python.exe python/score_repertoire.py ... --label A_cw0 --output card.txt
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

from zobrist import zobrist_int64

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def to_us(x: float | None, white: bool) -> float | None:
    """Flip an expected-WHITE-score quantity to our perspective (black: 1-x)."""
    if x is None:
        return None
    return x if white else 1.0 - x


# ── Per-slice scoring ──────────────────────────────────────────────────────

def score_slice(
    rep_slice: pl.DataFrame,
    stats_slice: pl.DataFrame | None,
    perspective: str,
    max_ply: int,
) -> dict:
    """Score one (event, elo_band) slice.

    Headline rollups (eff / sound_bp / worst) come straight from the propagated
    START-position row -- they need no stats. When stats_slice is supplied a
    forward-flow walk adds the reach-weighted coverage / eval-Soundness / size.
    """
    white      = perspective == "white"
    our_color  = chess.WHITE if white else chess.BLACK
    start_hash = zobrist_int64(chess.Board())

    rep_idx = {r["position_hash"]: r for r in rep_slice.iter_rows(named=True)}

    # ── Headline rollups from the propagated root row ──────────────────────
    root     = rep_idx.get(start_hash)
    eff      = to_us(root["value"], white)            if root else None
    sound_bp = to_us(root.get("value_robust"), white) if root else None
    worst    = to_us(root.get("value_worst"), white)  if root else None

    # ── Raw size from the rep alone (no stats needed) ──────────────────────
    decisions = sum(1 for r in rep_idx.values()
                    if r.get("best_move") is not None
                    and r["side_to_move"] == perspective)
    positions = len(rep_idx)

    out: dict = {
        "eff": eff, "sound_bp": sound_bp, "worst": worst,
        "decisions": decisions, "positions": positions,
        # Filled only when stats are supplied:
        "sound_freq": None, "eval_cov": None, "cov8": None, "cov16": None,
        "prep_depth": None, "booked_moves": None, "opp_decisions": None,
        "slice_total": None, "reached_positions": None,
    }
    if stats_slice is None:
        return out

    # ── Forward-flow walk (analyze_coverage.py pattern) ────────────────────
    # opp_idx: parent_hash -> [(san, total, child_hash), ...]
    opp_idx: dict[int, list[tuple[str, int, int | None]]] = defaultdict(list)
    for r in stats_slice.iter_rows(named=True):
        opp_idx[r["parent_hash"]].append(
            (r["move_san"], r["total"], r.get("child_hash"))
        )
    slice_total = sum(t for _, t, _ in opp_idx.get(start_hash, []))

    weight_at: dict[tuple[int, int], float] = {(0, start_hash): 1.0}
    by_depth: dict[int, list[int]] = defaultdict(list)
    by_depth[0].append(start_hash)

    reach: dict[int, float] = defaultdict(float)
    exit_depth_w = 0.0   # Σ exit_weight * depth   (Σ exit_weight == 1.0 by conservation)
    booked       = 0.0   # Σ reach over our-turn nodes  = booked moves / game
    opp_dec      = 0.0   # Σ reach over opponent nodes

    for depth in range(max_ply):
        for ph in by_depth[depth]:
            w = weight_at.get((depth, ph), 0.0)
            if w == 0:
                continue
            reach[ph] += w

            row = rep_idx.get(ph)
            if row is None:
                exit_depth_w += w * depth            # reached a non-prepared position
                continue

            board = chess.Board(row["position_epd"])
            if board.turn == our_color:
                best = row.get("best_move")
                if best is None:
                    exit_depth_w += w * depth         # frontier leaf (our move truncated)
                    continue
                try:
                    board.push(board.parse_san(best))
                except (ValueError, AssertionError):
                    exit_depth_w += w * depth
                    continue
                booked += w                            # a prepared move we actually play
                ch  = zobrist_int64(board)
                key = (depth + 1, ch)
                if key not in weight_at:
                    weight_at[key] = 0.0
                    by_depth[depth + 1].append(ch)
                weight_at[key] += w
            else:
                opp_dec += w
                moves = opp_idx.get(ph, [])
                tot   = sum(t for _, t, _ in moves)
                if tot == 0:
                    exit_depth_w += w * depth
                    continue
                covered_w = 0.0
                for _san, t, ch in moves:
                    if ch is not None and ch in rep_idx:
                        frac       = t / tot
                        covered_w += frac
                        key        = (depth + 1, ch)
                        if key not in weight_at:
                            weight_at[key] = 0.0
                            by_depth[depth + 1].append(ch)
                        weight_at[key] += w * frac
                uncovered = 1.0 - covered_w            # opponent left our prep
                if uncovered > 1e-12:
                    exit_depth_w += w * uncovered * depth

    # Residual weight still in book at the walk horizon exits there.
    for ph in by_depth.get(max_ply, []):
        w = weight_at.get((max_ply, ph), 0.0)
        if w > 0:
            reach[ph]    += w
            exit_depth_w += w * max_ply

    def cov(d: int) -> float:
        return sum(weight_at.get((d, ph), 0.0) for ph in by_depth.get(d, []))

    # Frequency-weighted engine Soundness: reach-weighted mean eval over reached
    # positions that HAVE an eval (Chessbook's frequency-weighted eval metric).
    num = den = den_all = 0.0
    for ph, wt in reach.items():
        den_all += wt
        row = rep_idx.get(ph)
        if row is None:
            continue
        ev = row.get("eval_score")
        if ev is None:
            continue
        num += wt * to_us(ev, white)
        den += wt

    out.update({
        "sound_freq":        (num / den) if den > 0 else None,
        "eval_cov":          (den / den_all) if den_all > 0 else None,
        "cov8":              cov(8),
        "cov16":             cov(16),
        "prep_depth":        exit_depth_w,           # Σ exit_weight == 1 → mean exit ply
        "booked_moves":      booked,
        "opp_decisions":     opp_dec,
        "slice_total":       slice_total,
        "reached_positions": len(reach),
    })
    return out


# ── Reporting ──────────────────────────────────────────────────────────────

def _pct(x: float | None) -> str:
    return f"{x * 100:5.1f}%" if x is not None else "   n/a"


def _num(x: float | None, fmt: str = ",.0f") -> str:
    return format(x, fmt) if x is not None else "n/a"


def render_card(ev: str, eb: int, perspective: str, r: dict) -> str:
    L: list[str] = []
    L.append(f"\n{'=' * 78}")
    L.append(f"SLICE: {ev}  elo_band {eb}   (perspective: {perspective})")
    L.append(f"{'=' * 78}")
    L.append(f"  Effectiveness  (freq-wtd expected score, vs avg opponent) {_pct(r['eff'])}")
    L.append(f"  Soundness      (best defence -- value_robust)            {_pct(r['sound_bp'])}")
    if r["sound_freq"] is not None:
        L.append(f"  Soundness      (freq-wtd eval of reached positions)     {_pct(r['sound_freq'])}"
                 f"   (eval cover {_pct(r['eval_cov']).strip()})")
    L.append(f"  Worst-case     (our book vs best defence -- the gate)    {_pct(r['worst'])}")
    if r["cov8"] is not None:
        L.append("")
        L.append(f"  Coverage       ply  8 / 16   {_pct(r['cov8'])} / {_pct(r['cov16'])}")
        L.append(f"  Prep depth     (reach-wtd mean exit ply)                {r['prep_depth']:5.1f}")
        L.append(f"  Booked moves / game (reach-wtd)                         {r['booked_moves']:5.1f}")
    L.append("")
    L.append(f"  Size           best-move nodes (whole tree)              {r['decisions']:>9,}")
    L.append(f"                 distinct positions (whole tree)           {r['positions']:>9,}")
    if r["reached_positions"] is not None:
        L.append(f"                 positions reached from start (the real size) {r['reached_positions']:>6,}")
    if r["slice_total"] is not None:
        L.append(f"  Slice games                                             {r['slice_total']:>12,}")
    return "\n".join(L)


def summary_line(label: str, ev: str, eb: int, r: dict) -> str:
    """One machine-readable line per slice (easy to grep/collect in a sweep)."""
    def g(k):
        v = r.get(k)
        return "nan" if v is None else f"{v:.4f}"
    return (f"SUMMARY {label} event={ev} elo={eb} "
            f"eff={g('eff')} sound_bp={g('sound_bp')} sound_freq={g('sound_freq')} "
            f"worst={g('worst')} cov8={g('cov8')} cov16={g('cov16')} "
            f"prep_depth={g('prep_depth')} booked={g('booked_moves')} "
            f"decisions={r['decisions']} positions={r['positions']}")


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repertoire", required=True, help="Stage 3 repertoire parquet.")
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--stats", default=None,
                    help="Matching Stage 2 stats parquet (needs child_hash). When omitted, "
                         "only the headline rollups + raw size are reported.")
    ap.add_argument("--max-ply", type=int, default=30,
                    help="Forward-walk horizon in plies (default: 30 = whole tree).")
    ap.add_argument("--label", default=None,
                    help="Tag for the machine-readable SUMMARY line (default: rep stem).")
    ap.add_argument("--output", default=None, help="Also write the rendered report here.")
    args = ap.parse_args()

    rep_path = Path(args.repertoire)
    label    = args.label or rep_path.stem

    print(f"Repertoire:    {rep_path}")
    print(f"Perspective:   {args.perspective}")
    if args.stats:
        print(f"Stats:         {args.stats}")
    print(f"Max ply:       {args.max_ply}\n")

    rep = pl.read_parquet(rep_path)

    stats = None
    if args.stats:
        stats = pl.read_parquet(args.stats)
        if "child_hash" not in stats.columns:
            sys.exit("ERROR: stats parquet missing child_hash column "
                     "(re-run Stage 2 / build_pooled_stats.py).")

    slices = rep.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])

    cards: list[str] = []
    summaries: list[str] = []
    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        rep_slice = rep.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
        stats_slice = (stats.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
                       if stats is not None else None)
        r = score_slice(rep_slice, stats_slice, args.perspective, args.max_ply)
        if r["decisions"] == 0 and r["positions"] > 0:
            print(f"  WARNING: 0 decisions for {ev}/elo{eb} -- wrong --perspective?",
                  file=sys.stderr)
        cards.append(render_card(ev, eb, args.perspective, r))
        summaries.append(summary_line(label, ev, eb, r))

    report = "\n".join(cards) + "\n\n" + "\n".join(summaries)
    print(report)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
        print(f"\nReport saved to: {out}")


if __name__ == "__main__":
    main()
