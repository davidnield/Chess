"""
Stage 3: backwards induction on the position DAG.

Takes the aggregated (event, elo_band, position, move) statistics from Stage 2
and computes the best opening repertoire for the specified perspective using
backwards induction:

  - At our turn:        pick the move that maximises (white) or minimises
                        (black) the propagated expected white_score
  - At opponent's turn: weight their moves by empirical game-count frequencies

Terminal leaves (edges whose child position has no children in our dataset)
use a Beta-Binomial posterior mean instead of the raw empirical score:

    smoothed = (k * mu_slice + score_avg * n) / (k + n)

where mu_slice is the per-(event, elo_band) average white-score from the
starting position and k is the prior strength (--prior-strength, default 30).
This pulls noisy small-sample leaves toward the slice mean, preventing the max
operator from cherry-picking lucky 10-game samples. Internal nodes inherit
smoothing transitively through their children.

Transpositions are handled correctly: positions are processed in topological
order (Kahn's algorithm on the position DAG), so each position is valued once
regardless of the number of paths leading to it.

Selection optionally adds a forcing bonus to value when picking our move:

    score = value ± forcing_weight * forcingness(child)

forcingness is the Simpson concentration of the opponent's empirical reply
distribution at the child position. forcing_weight=0 (default) reproduces
pure value-maximising behaviour; small positive values bias selection toward
moves that narrow the opponent's reasonable response set -- aligning with the
goal of building a low-memorisation, "forcing" repertoire.

NOTE on opponent modelling: at opponent-turn positions the algorithm uses
their EMPIRICAL move distribution, not optimal play. As a result the
white-perspective and black-perspective values do NOT sum to 1 in general --
each side's number is "expected score IF I play optimally and my opponent
plays like a typical player at this elo". This is the right framing for a
human-vs-human repertoire.

Output columns per (event, elo_band, position):
  value         -- expected white_score under backwards-induction play
  best_move     -- our recommended move at our-turn positions; null at opp turn
  forcingness   -- Simpson concentration of opp's reply distribution after
                   our recommended move; null at opponent-turn positions

Usage:
    .venv/Scripts/python.exe python/stage3_backwards_induction.py
    .venv/Scripts/python.exe python/stage3_backwards_induction.py --perspective black
    .venv/Scripts/python.exe python/stage3_backwards_induction.py --prior-strength 60
    .venv/Scripts/python.exe python/stage3_backwards_induction.py --forcing-weight 0.05
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DEFAULT_INPUT  = Path("D:/data/chess/position-stats/position_stats.parquet")
DEFAULT_OUTPUT = Path("D:/data/chess/repertoire/repertoire.parquet")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(board: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


def smoothed_score(empirical: float, n: int, prior: float, k: float) -> float:
    """Beta-Binomial posterior mean: (k * prior + empirical * n) / (k + n)."""
    return (k * prior + empirical * n) / (k + n)


def forcingness(opp_moves: list[dict]) -> float:
    """Concentration of opponent's empirical reply distribution at a position.

    Simpson's index = sum(p_i^2) where p_i = total_i / sum(total). Range [0, 1]:
      1.0 = forced (single reply); 0.5 = two equal replies; 1/N = N equal.
    Used to score a candidate move m by the forcingness of the position it
    leaves the opponent in.
    """
    if not opp_moves:
        return 0.0
    s = sum(m["total"] for m in opp_moves)
    if s == 0:
        return 0.0
    return sum((m["total"] / s) ** 2 for m in opp_moves)


def compute_slice_prior(edges: list[dict], start_hash: int, min_games: int = 100) -> float:
    """Average white_score across all games in this slice (from the starting position).

    Falls back to 0.5 if the slice has fewer than `min_games` total games at the
    starting position -- in that case the empirical prior is itself too noisy.
    """
    score_sum = 0.0
    game_sum  = 0
    for e in edges:
        if e["parent_hash"] == start_hash:
            score_sum += e["white_score_avg"] * e["total"]
            game_sum  += e["total"]
    if game_sum < min_games:
        return 0.5
    return score_sum / game_sum


def compute_child_hashes(df: pl.DataFrame) -> pl.DataFrame:
    """Append child_hash column by applying each move to its parent EPD."""
    child_hashes: list[int | None] = []
    for row in df.iter_rows(named=True):
        try:
            board = chess.Board(row["parent_epd"])
            move  = board.parse_san(row["move_san"])
            board.push(move)
            child_hashes.append(zobrist_int64(board))
        except (ValueError, AssertionError):
            child_hashes.append(None)
    return df.with_columns(pl.Series("child_hash", child_hashes, dtype=pl.Int64))


def run_backwards_induction(
    edges: list[dict],
    perspective: str,  # "white" or "black"
    prior_strength: float = 30.0,
    forcing_weight:  float = 0.0,
) -> tuple[dict[int, float], dict[int, str | None], dict[int, float | None],
           dict[int, str], dict[int, chess.Color], float]:
    """
    Value every position reachable in `edges` and pick our best move at each.

    Selection at our turn maximises (white) or minimises (black)
        value ± forcing_weight * forcingness(child)
    where forcingness is the Simpson concentration of the opponent's reply
    distribution at the resulting position. forcing_weight=0 reproduces pure
    value-maximising behaviour; small positive values break ties in favour of
    moves that narrow the opponent's reasonable response set.

    Returns:
        values         -- position_hash -> expected white_score
        best_moves     -- position_hash -> best move_san (None at opponent's turn)
        best_forcing   -- position_hash -> forcingness of chosen move (None at opp turn)
        position_epd   -- position_hash -> EPD string
        position_side  -- position_hash -> chess.WHITE or chess.BLACK
        slice_prior    -- empirical white_score from starting position
    """
    our_color   = chess.WHITE if perspective == "white" else chess.BLACK
    sign        = 1.0 if perspective == "white" else -1.0
    start_hash  = zobrist_int64(chess.Board())
    slice_prior = compute_slice_prior(edges, start_hash)

    # ── Build adjacency ───────────────────────────────────────────────────────
    # children[ph] holds every (move, child, empirical_score, game_count) edge.
    # position_meta caches EPD and side-to-move to avoid repeated Board() calls.
    children:      dict[int, list[dict]]        = defaultdict(list)
    position_epd:  dict[int, str]               = {}
    position_side: dict[int, chess.Color]       = {}

    for e in edges:
        if e["child_hash"] is None:
            continue
        ph = e["parent_hash"]
        children[ph].append({
            "move_san":  e["move_san"],
            "child_hash": e["child_hash"],
            "score_avg":  e["white_score_avg"],
            "total":      e["total"],
        })
        if ph not in position_epd:
            position_epd[ph]  = e["parent_epd"]
            position_side[ph] = chess.Board(e["parent_epd"]).turn

    all_positions = set(position_epd)

    # ── Topological sort (Kahn's) ─────────────────────────────────────────────
    # pending[ph] = child hashes (inside our dataset) not yet valued
    pending: dict[int, set[int]] = {
        ph: {m["child_hash"] for m in children[ph] if m["child_hash"] in all_positions}
        for ph in all_positions
    }
    parents_of: dict[int, set[int]] = defaultdict(set)
    for ph in all_positions:
        for m in children[ph]:
            ch = m["child_hash"]
            if ch in all_positions:
                parents_of[ch].add(ph)

    queue: deque[int] = deque(ph for ph in all_positions if not pending[ph])

    # ── Backwards induction ───────────────────────────────────────────────────
    values:       dict[int, float]              = {}
    best_moves:   dict[int, str | None]         = {}
    best_forcing: dict[int, float | None]       = {}

    while queue:
        ph       = queue.popleft()
        our_turn = (position_side[ph] == our_color)

        # Build move tuples: (san, value, total, forcingness-of-resulting-position).
        # Forcingness is the concentration of opponent's reply distribution at the
        # CHILD position -- a property of where the move leaves the opponent.
        move_vals = []
        for m in children[ph]:
            ch = m["child_hash"]
            if ch in values:
                child_val = values[ch]
            else:
                child_val = smoothed_score(m["score_avg"], m["total"], slice_prior, prior_strength)
            forcing = forcingness(children.get(ch, []))
            move_vals.append((m["move_san"], child_val, m["total"], forcing))

        if not move_vals:
            values[ph]       = slice_prior
            best_moves[ph]   = None
            best_forcing[ph] = None
        elif our_turn:
            # Maximise (white) / minimise (black) value with a forcing bonus.
            # Both sides prefer forcing, so the bonus is added in our favour
            # via the `sign` factor.
            key = lambda x, s=sign, w=forcing_weight: s * x[1] + w * x[3]
            best = max(move_vals, key=key)
            san, val, _, frc = best
            values[ph]       = val
            best_moves[ph]   = san
            best_forcing[ph] = frc
        else:
            # Opponent: weighted average over their empirical move distribution.
            total = sum(t for _, _, t, _ in move_vals)
            val   = sum(v * t for _, v, t, _ in move_vals) / total if total else slice_prior
            values[ph]       = val
            best_moves[ph]   = None
            best_forcing[ph] = None

        for parent in parents_of[ph]:
            pending[parent].discard(ph)
            if not pending[parent] and parent not in values:
                queue.append(parent)

    # Fallback for positions in cycles or unreachable in the topological pass.
    # Use smoothed empirical scores for child values (since propagated values may
    # not exist for cycle-mates) and still pick a best move at our-turn positions.
    for ph in all_positions - set(values):
        mvs = children[ph]
        if not mvs:
            values[ph]       = slice_prior
            best_moves[ph]   = None
            best_forcing[ph] = None
            continue

        move_vals = []
        for m in mvs:
            ch = m["child_hash"]
            child_val = values.get(
                ch, smoothed_score(m["score_avg"], m["total"], slice_prior, prior_strength)
            )
            forcing = forcingness(children.get(ch, []))
            move_vals.append((m["move_san"], child_val, m["total"], forcing))

        if position_side[ph] == our_color:
            key = lambda x, s=sign, w=forcing_weight: s * x[1] + w * x[3]
            san, val, _, frc = max(move_vals, key=key)
            values[ph]       = val
            best_moves[ph]   = san
            best_forcing[ph] = frc
        else:
            total = sum(t for _, _, t, _ in move_vals)
            val   = sum(v * t for _, v, t, _ in move_vals) / total if total else slice_prior
            values[ph]       = val
            best_moves[ph]   = None
            best_forcing[ph] = None

    return values, best_moves, best_forcing, position_epd, position_side, slice_prior


def print_best_line(
    result_index: dict[int, dict],
    stats_index:  dict[int, list[dict]],  # position_hash -> list of {move_san, total}
    start_hash:   int,
    perspective:  str,
    max_depth:    int = 12,
):
    """Walk the recommended line, showing opponent's most-played response."""
    our_color = chess.WHITE if perspective == "white" else chess.BLACK
    ph        = start_hash
    tokens: list[str] = []
    move_num = 1

    for _ in range(max_depth):
        r = result_index.get(ph)
        if r is None:
            break
        board = chess.Board(r["position_epd"])

        if board.turn == our_color:
            move = r["best_move"]
            if move is None:
                break
        else:
            # Opponent: pick their most common empirical response
            opp_moves = stats_index.get(ph, [])
            if not opp_moves:
                break
            move = max(opp_moves, key=lambda m: m["total"])["move_san"]

        # Format with move number
        if board.turn == chess.WHITE:
            tokens.append(f"{move_num}.")
            move_num += 1
        tokens.append(move)

        try:
            board.push(board.parse_san(move))
            ph = zobrist_int64(board)
        except (ValueError, AssertionError):
            break

    return " ".join(tokens)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input",       default=str(DEFAULT_INPUT))
    parser.add_argument("--output",      default=str(DEFAULT_OUTPUT))
    parser.add_argument("--perspective", choices=["white", "black"], default="white",
                        help="Whose repertoire to compute (default: white)")
    parser.add_argument("--prior-strength", type=float, default=30.0,
                        help="Beta-Binomial pseudocount for terminal-leaf smoothing. "
                             "Higher = more shrinkage toward slice prior. "
                             "0 disables smoothing. Default: 30")
    parser.add_argument("--forcing-weight", type=float, default=0.0,
                        help="Bonus added to value when picking our move, scaled by the "
                             "forcingness (Simpson concentration in [0,1]) of the resulting "
                             "opponent position. 0 = pure value (default). 0.05 only breaks "
                             "near-ties; 0.2+ noticeably trades win rate for forcing lines.")
    parser.add_argument("--event",       help="Filter to a single event")
    parser.add_argument("--elo-band",    type=int, help="Filter to a single elo band")
    args = parser.parse_args()

    input_path  = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Input:           {input_path}")
    print(f"Output:          {output_path}")
    print(f"Perspective:     {args.perspective}")
    print(f"Prior strength:  {args.prior_strength}")
    print(f"Forcing weight:  {args.forcing_weight}")

    stats = pl.read_parquet(input_path)
    stats = stats.filter(pl.col("elo_band").is_not_null())

    if args.event:
        stats = stats.filter(pl.col("event") == args.event)
    if args.elo_band is not None:
        stats = stats.filter(pl.col("elo_band") == args.elo_band)

    print(f"Loaded {len(stats):,} edges, "
          f"{stats['parent_hash'].n_unique():,} distinct positions")

    print("Computing child hashes...")
    t0    = time.time()
    stats = compute_child_hashes(stats)
    print(f"  done in {time.time() - t0:.1f}s")

    slices   = stats.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
    all_rows: list[dict] = []
    t_total  = time.time()

    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        mask   = (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        edges  = stats.filter(mask).to_dicts()

        t1 = time.time()
        values, best_moves, best_forcing, pos_epd, pos_side, slice_prior = run_backwards_induction(
            edges, args.perspective,
            prior_strength=args.prior_strength,
            forcing_weight=args.forcing_weight,
        )
        elapsed = time.time() - t1

        n_our = sum(1 for m in best_moves.values() if m is not None)
        print(f"  {ev} / elo {eb:>6,}: {len(values):>5,} positions, "
              f"{n_our:>4,} our-turn, prior={slice_prior:.3f} in {elapsed:.2f}s")

        for ph, val in values.items():
            all_rows.append({
                "event":         ev,
                "elo_band":      eb,
                "position_hash": ph,
                "position_epd":  pos_epd[ph],
                "side_to_move":  "white" if pos_side[ph] == chess.WHITE else "black",
                "value":         val,
                "best_move":     best_moves.get(ph),
                "forcingness":   best_forcing.get(ph),
            })

    result = (
        pl.from_dicts(all_rows)
        .sort(["event", "elo_band", "position_hash"])
    )
    result.write_parquet(str(output_path), compression="zstd")

    size_mb      = output_path.stat().st_size / 1e6
    elapsed_total = time.time() - t_total
    print(f"\nWrote {output_path}  ({size_mb:.1f} MB) in {elapsed_total:.1f}s")
    print(f"Total rows: {len(result):,}")

    # ── Sanity checks ─────────────────────────────────────────────────────────
    start_hash = zobrist_int64(chess.Board())

    # Recommended first move per slice
    first_moves = (
        result
        .filter(pl.col("position_hash") == start_hash)
        .select(["event", "elo_band", "best_move", "value"])
        .sort(["event", "elo_band"])
    )
    print(f"\nRecommended first move per (event, elo_band) [{args.perspective}'s repertoire]:")
    print(first_moves)

    # Sample line for the most-populated elo band
    pop = (
        result
        .group_by(["event", "elo_band"])
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .row(0, named=True)
    )
    ev0, eb0 = pop["event"], pop["elo_band"]
    result_idx  = {
        r["position_hash"]: r
        for r in result.filter(
            (pl.col("event") == ev0) & (pl.col("elo_band") == eb0)
        ).iter_rows(named=True)
    }
    stats_idx: dict[int, list[dict]] = defaultdict(list)
    for row in stats.filter(
        (pl.col("event") == ev0) & (pl.col("elo_band") == eb0)
    ).iter_rows(named=True):
        stats_idx[row["parent_hash"]].append(
            {"move_san": row["move_san"], "total": row["total"]}
        )

    line = print_best_line(result_idx, stats_idx, start_hash, args.perspective)
    print(f"\nSample line ({ev0}, elo {eb0:,}, {args.perspective}):")
    print(f"  {line}")


if __name__ == "__main__":
    main()
