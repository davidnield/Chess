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
starting position and k is the prior strength (--prior-strength, default 100).
This pulls noisy small-sample leaves toward the slice mean, preventing the max
operator from cherry-picking lucky 10-game samples. Internal nodes inherit
smoothing transitively through their children.

Transpositions are handled correctly: positions are processed in topological
order (Kahn's algorithm on the position DAG), so each position is valued once
regardless of the number of paths leading to it.

Selection optionally adds forcing and opponent-error bonuses when picking our move:

    score = sign * value + forcing_weight * forcingness + error_weight * opponent_error

forcingness is the Simpson concentration of the opponent's empirical reply
distribution at the child position. forcing_weight=0 (default) reproduces
pure value-maximising behaviour; small positive values bias selection toward
moves that narrow the opponent's reasonable response set.

opponent_error measures how much centipawn-equivalent loss opponents typically
incur at the resulting position — the gap between their best reply (by
Stockfish) and what they actually play, weighted by frequency. error_weight=0
(default) disables it; positive values bias toward "tricky" positions where
opponents blunder even when many replies are available.

The eval_weight for Stockfish leaf blending can optionally vary by sample
size (--eval-weight-k > 0), trusting Stockfish more at sparse positions
while flooring at --eval-weight-min at well-sampled positions.

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
import math
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


DEFAULT_INPUT  = Path("E:/chess/position-stats/position_stats.parquet")
DEFAULT_OUTPUT = Path("E:/chess/repertoire/repertoire.parquet")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(board: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


def smoothed_score(empirical: float, n: int, prior: float, k: float) -> float:
    """Beta-Binomial posterior mean: (k * prior + empirical * n) / (k + n)."""
    return (k * prior + empirical * n) / (k + n)


# Lichess WDL model: converts centipawns to expected white score [0, 1].
# Coefficient from https://lichess.org/page/accuracy — calibrated to Lichess data.
LICHESS_CP_SCALE = 0.00368208


def cp_to_expected_score(cp: int) -> float:
    """Convert centipawns to expected white score using the Lichess sigmoid.

    At cp=0: 0.500,  cp=100: 0.591,  cp=300: 0.751,  cp=10000 (mate): ~1.0.
    """
    return 1.0 / (1.0 + math.exp(-LICHESS_CP_SCALE * cp))


def forcingness(
    opp_moves: list[dict],
    k_f:      float = 200.0,
    baseline: float = 0.30,
) -> float:
    """Concentration of opponent's empirical reply distribution at a position,
    Bayesian-smoothed against small-sample bias.

    Raw Simpson's index = sum(p_i^2) with p_i = total_i / sum(total). Range [0, 1]:
      1.0 = forced (single reply); 0.5 = two equal replies; 1/N = N equal.

    Sample-size bias: with few observations, even a genuinely-uniform position
    looks concentrated (Simpson=1.0 is automatic when only one reply has been
    seen). To avoid the forcing bonus chasing data sparsity, we shrink toward
    `baseline` with pseudocount `k_f`:

        smoothed = (k_f * baseline + raw_simpson * N) / (k_f + N)

    where N is the total number of replies in the data. Defaults: k_f=200,
    baseline=0.30 (a typical opening-position forcingness). Pass k_f=0 to
    disable smoothing.
    """
    if not opp_moves:
        return baseline
    n = sum(m["total"] for m in opp_moves)
    if n == 0:
        return baseline
    raw_simpson = sum((m["total"] / n) ** 2 for m in opp_moves)
    return (k_f * baseline + raw_simpson * n) / (k_f + n)


def effective_eval_weight(ew_max: float, ew_min: float, k: float, n: int) -> float:
    """Dynamic eval weight that decreases with sample size, floored at ew_min.

    Formula:  ew_min + (ew_max - ew_min) * k / (k + n)

    At n=0:    returns ew_max  (full Stockfish trust for unseen positions)
    At n→∞:    returns ew_min  (floor, never drops below this)
    At n=k:    returns (ew_min + ew_max) / 2  (half-life)

    When ew_max <= 0 or k <= 0, returns ew_max (disabled / fixed-weight mode).
    When ew_min >= ew_max, returns ew_max (no dynamic range).
    """
    if ew_max <= 0 or k <= 0 or ew_min >= ew_max:
        return ew_max
    return ew_min + (ew_max - ew_min) * k / (k + n)


def opponent_error(
    opp_moves:    list[dict],
    eval_lookup:  dict[int, float] | None,
    opp_is_white: bool,
    k_e:          float = 200.0,
    baseline:     float = 0.0,
) -> float:
    """Expected score loss from the opponent's typical replies vs their best.

    For each of the opponent's empirical moves at this position, looks up the
    Stockfish expected score of the resulting grandchild position.  The "error"
    is the gap between the opponent's best available reply (by Stockfish) and
    what they actually play, weighted by empirical frequency.

    Returns a non-negative value in expected-score units (0-1 scale).
    Higher = opponents make bigger mistakes at this position.
    Smoothed with a Bayesian pseudocount (same pattern as forcingness).

    opp_moves:    children[ch] — opponent's available moves at position ch
    eval_lookup:  position_hash -> expected white score (from Lichess eval DB)
    opp_is_white: True if the opponent (the side moving at this position) is White
    k_e:          smoothing pseudocount (higher = more shrinkage toward baseline)
    baseline:     prior error assumption (0.0 = assume no error with no data)
    """
    if not opp_moves or not eval_lookup:
        return baseline

    # Collect evals for each opponent reply (grandchild positions).
    eval_replies: list[tuple[int, float]] = []  # (game_count, expected_white_score)
    for m in opp_moves:
        gc = m["child_hash"]
        if gc in eval_lookup:
            eval_replies.append((m["total"], eval_lookup[gc]))

    if not eval_replies:
        return baseline

    # Best reply from the opponent's perspective.
    if opp_is_white:
        best_eval = max(ev for _, ev in eval_replies)   # White wants highest
    else:
        best_eval = min(ev for _, ev in eval_replies)    # Black wants lowest

    # Weighted error: how much worse opponents play vs their best option.
    n = sum(t for t, _ in eval_replies)
    if opp_is_white:
        raw_error = sum(t * (best_eval - ev) for t, ev in eval_replies) / n
    else:
        raw_error = sum(t * (ev - best_eval) for t, ev in eval_replies) / n

    raw_error = max(0.0, raw_error)

    # Bayesian smoothing (same pattern as forcingness).
    return (k_e * baseline + raw_error * n) / (k_e + n)


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


def run_backwards_induction(
    edges: list[dict],
    perspective: str,  # "white" or "black"
    prior_strength:   float = 500.0,
    forcing_weight:   float = 0.0,
    forcing_prior:    float = 200.0,
    forcing_baseline: float = 0.30,
    min_move_games:   int = 500,
    eval_lookup:      dict[int, float] | None = None,
    eval_weight:      float = 0.0,
    eval_weight_min:  float = 0.0,
    eval_weight_k:    float = 0.0,
    error_weight:     float = 0.0,
    error_prior:      float = 200.0,
) -> tuple[dict[int, float], dict[int, str | None], dict[int, float | None],
           dict[int, float | None],
           dict[int, str], dict[int, chess.Color], float]:
    """
    Value every position reachable in `edges` and pick our best move at each.

    **Leaf blending**: at leaf positions (those with no backwards-induction
    value from deeper nodes), the smoothed empirical score is blended with
    the Stockfish expected white score:

        leaf_value = (1 - ew) * empirical + ew * stockfish

    The blending weight `ew` is either a fixed scalar (eval_weight) or varies
    dynamically by position sample size when eval_weight_k > 0:

        ew = eval_weight_min + (eval_weight - eval_weight_min) * k / (k + n)

    This trusts Stockfish more at sparse positions and the empirical data
    more at well-sampled positions, with a floor at eval_weight_min.

    **Selection** at our turn maximises (white) or minimises (black):

        sign * value + forcing_weight * forcingness + error_weight * opponent_error

    forcingness: Simpson concentration of opponent's reply distribution.
    opponent_error: expected score loss from opponent's typical replies vs
    their best move (by Stockfish eval). Higher = opponents tend to blunder.

    Returns:
        values         -- position_hash -> expected white_score (blended at leaves)
        best_moves     -- position_hash -> best move_san (None at opponent's turn)
        best_forcing   -- position_hash -> forcingness of chosen move (None at opp turn)
        best_error     -- position_hash -> opponent_error of chosen move (None at opp turn)
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
    best_error:   dict[int, float | None]       = {}

    while queue:
        ph       = queue.popleft()
        our_turn = (position_side[ph] == our_color)

        # Build move tuples: (san, value, total, forcingness, opp_err, child_hash).
        # Forcingness and opponent_error are properties of the CHILD position
        # (where the opponent must reply after our move).
        #
        # LEAF BLENDING: when a child position has no backwards-induction
        # value yet (i.e. it's a leaf), we blend the smoothed empirical
        # score with the Stockfish expected score.  The blending weight is
        # either fixed (eval_weight) or dynamic based on sample size.
        move_vals = []
        for m in children[ph]:
            ch = m["child_hash"]
            if ch in values:
                # Already valued (blended from below) — use as-is.
                child_val = values[ch]
            else:
                # Leaf: blend empirical + Stockfish (with dynamic weight).
                emp = smoothed_score(m["score_avg"], m["total"], slice_prior, prior_strength)
                if eval_weight > 0 and eval_lookup and ch in eval_lookup:
                    # n_games at child = total games across all moves at that position
                    n_child = sum(m2["total"] for m2 in children.get(ch, []))
                    ew = effective_eval_weight(eval_weight, eval_weight_min,
                                              eval_weight_k, n_child)
                    child_val = (1.0 - ew) * emp + ew * eval_lookup[ch]
                else:
                    child_val = emp
            frc = forcingness(children.get(ch, []), forcing_prior, forcing_baseline)
            ch_moves = children.get(ch, [])
            # Opponent at ch is the side NOT equal to our_color.
            opp_err = opponent_error(
                ch_moves, eval_lookup, opp_is_white=(our_color == chess.BLACK),
                k_e=error_prior,
            ) if error_weight > 0 else 0.0
            move_vals.append((m["move_san"], child_val, m["total"], frc, opp_err, ch))

        if not move_vals:
            values[ph]       = slice_prior
            best_moves[ph]   = None
            best_forcing[ph] = None
            best_error[ph]   = None
        elif our_turn:
            # Maximise (white) / minimise (black) value with forcing + error bonus.
            # Child values already incorporate Stockfish at the leaves, so no
            # additional SF blending is needed here — it propagates naturally.
            #
            # Sample-size floor: restrict our pick to moves with at least
            # `min_move_games` opponent games, so we don't recommend lines
            # whose value rests on a noisy small sample. Fall back to the
            # full move set if every candidate is below threshold (rare;
            # only happens for sparse deep positions).
            candidates = [mv for mv in move_vals if mv[2] >= min_move_games]
            if not candidates:
                candidates = move_vals

            key = (lambda x, s=sign, fw=forcing_weight, ew=error_weight:
                   s * x[1] + fw * x[3] + ew * x[4])
            best = max(candidates, key=key)
            san, val, _, frc, oerr, _ = best
            values[ph]       = val  # propagate blended value
            best_moves[ph]   = san
            best_forcing[ph] = frc
            best_error[ph]   = oerr
        else:
            # Opponent: weighted average over their empirical move distribution.
            # No sample-size filter here -- we model the opponent's full
            # observed distribution, including their rare moves.
            total = sum(t for _, _, t, _, _, _ in move_vals)
            val   = sum(v * t for _, v, t, _, _, _ in move_vals) / total if total else slice_prior
            values[ph]       = val
            best_moves[ph]   = None
            best_forcing[ph] = None
            best_error[ph]   = None

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
            best_error[ph]   = None
            continue

        move_vals = []
        for m in mvs:
            ch = m["child_hash"]
            if ch in values:
                child_val = values[ch]
            else:
                # Leaf: blend empirical + Stockfish (same as main loop).
                emp = smoothed_score(m["score_avg"], m["total"], slice_prior, prior_strength)
                if eval_weight > 0 and eval_lookup and ch in eval_lookup:
                    n_child = sum(m2["total"] for m2 in children.get(ch, []))
                    ew = effective_eval_weight(eval_weight, eval_weight_min,
                                              eval_weight_k, n_child)
                    child_val = (1.0 - ew) * emp + ew * eval_lookup[ch]
                else:
                    child_val = emp
            frc = forcingness(children.get(ch, []), forcing_prior, forcing_baseline)
            ch_moves = children.get(ch, [])
            opp_err = opponent_error(
                ch_moves, eval_lookup, opp_is_white=(our_color == chess.BLACK),
                k_e=error_prior,
            ) if error_weight > 0 else 0.0
            move_vals.append((m["move_san"], child_val, m["total"], frc, opp_err, ch))

        if position_side[ph] == our_color:
            # Same sample-size floor as the main loop above (with fallback).
            candidates = [mv for mv in move_vals if mv[2] >= min_move_games]
            if not candidates:
                candidates = move_vals

            key = (lambda x, s=sign, fw=forcing_weight, ew=error_weight:
                   s * x[1] + fw * x[3] + ew * x[4])
            san, val, _, frc, oerr, _ = max(candidates, key=key)
            values[ph]       = val
            best_moves[ph]   = san
            best_forcing[ph] = frc
            best_error[ph]   = oerr
        else:
            total = sum(t for _, _, t, _, _, _ in move_vals)
            val   = sum(v * t for _, v, t, _, _, _ in move_vals) / total if total else slice_prior
            values[ph]       = val
            best_moves[ph]   = None
            best_forcing[ph] = None
            best_error[ph]   = None

    return values, best_moves, best_forcing, best_error, position_epd, position_side, slice_prior


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
    parser.add_argument("--prior-strength", type=float, default=500.0,
                        help="Beta-Binomial pseudocount for terminal-leaf smoothing. "
                             "Higher = more shrinkage toward slice prior. "
                             "0 disables smoothing. Default: 500. (Earlier default "
                             "of 100 left small-sample lines like Na3 with n=153 "
                             "winning the argmax against more-played alternatives.)")
    parser.add_argument("--min-move-games", type=int, default=500,
                        help="At our-turn positions, restrict the move pick to "
                             "candidates with at least this many opponent games "
                             "in the input data. Falls back to all moves if every "
                             "candidate is below threshold. 0 disables. Default: 500")
    parser.add_argument("--forcing-weight", type=float, default=0.0,
                        help="Bonus added to value when picking our move, scaled by the "
                             "forcingness (Simpson concentration in [0,1]) of the resulting "
                             "opponent position. 0 = pure value (default). 0.05 only breaks "
                             "near-ties; 0.2+ noticeably trades win rate for forcing lines.")
    parser.add_argument("--forcing-prior", type=float, default=200.0,
                        help="Pseudocount for Bayesian smoothing of Simpson's index. "
                             "Higher = more shrinkage toward forcing-baseline. "
                             "0 disables smoothing (raw Simpson; biased high at small N). "
                             "Default: 200")
    parser.add_argument("--forcing-baseline", type=float, default=0.30,
                        help="Baseline forcingness to shrink low-sample positions toward. "
                             "0.30 is roughly typical for opening positions. Default: 0.30")
    parser.add_argument("--eval-db", default=None,
                        help="Path to eval DB parquet from build_eval_db.py. When provided, "
                             "Stockfish evals are used in move selection and written to the "
                             "output's eval_score column.")
    parser.add_argument("--eval-weight", type=float, default=0.0,
                        help="Blending weight for Stockfish eval at leaf positions. "
                             "When --eval-weight-k > 0, this is the MAXIMUM weight "
                             "(used at sparse positions). "
                             "0.0 = pure empirical (default, backward compatible). "
                             "1.0 = pure Stockfish. 0.3 = 70%% empirical + 30%% Stockfish.")
    parser.add_argument("--eval-weight-min", type=float, default=0.0,
                        help="Floor for dynamic eval weight. The effective eval weight "
                             "decreases with sample size but never drops below this. "
                             "Requires --eval-weight-k > 0 to activate dynamic mode. "
                             "0.0 = no floor (default). 0.3 recommended for most uses.")
    parser.add_argument("--eval-weight-k", type=float, default=0.0,
                        help="Half-life parameter for dynamic eval weight. At n=k games, "
                             "effective weight = midpoint of --eval-weight and "
                             "--eval-weight-min. 0 = fixed weight (default). "
                             "5000 = typical opening positions are well past half-life.")
    parser.add_argument("--error-weight", type=float, default=0.0,
                        help="Bonus for opponent error in move selection. Measures how much "
                             "opponents typically blunder at the resulting position (based "
                             "on Stockfish evals of their actual replies vs their best). "
                             "0.0 = disabled (default). Requires --eval-db to be set.")
    parser.add_argument("--error-prior", type=float, default=200.0,
                        help="Pseudocount for Bayesian smoothing of opponent error. "
                             "Higher = more shrinkage toward zero at small sample sizes. "
                             "Default: 200")
    parser.add_argument("--event",       help="Filter to a single event")
    parser.add_argument("--elo-band",    type=int, help="Filter to a single elo band")
    args = parser.parse_args()

    input_path  = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Input:             {input_path}")
    print(f"Output:            {output_path}")
    print(f"Perspective:       {args.perspective}")
    print(f"Prior strength:    {args.prior_strength}")
    print(f"Min move games:    {args.min_move_games}")
    print(f"Forcing weight:    {args.forcing_weight}")
    print(f"Forcing prior:     {args.forcing_prior}")
    print(f"Forcing baseline:  {args.forcing_baseline}")
    print(f"Eval DB:           {args.eval_db or '(none)'}")
    print(f"Eval weight:       {args.eval_weight}")
    if args.eval_weight_k > 0:
        print(f"Eval weight min:   {args.eval_weight_min}")
        print(f"Eval weight k:     {args.eval_weight_k}")
    print(f"Error weight:      {args.error_weight}")
    if args.error_weight > 0:
        print(f"Error prior:       {args.error_prior}")

    # ── Load eval DB (position_hash -> expected white score) ──────────────
    eval_lookup: dict[int, float] = {}
    if args.eval_db:
        edb_path = Path(args.eval_db)
        if not edb_path.exists():
            print(f"WARNING: eval DB not found at {edb_path} — proceeding without evals.")
        else:
            edb_raw = pl.read_parquet(str(edb_path))
            eval_lookup = {
                r["position_hash"]: cp_to_expected_score(r["eval_cp"])
                for r in edb_raw.iter_rows(named=True)
            }
            print(f"Loaded {len(eval_lookup):,} Stockfish evals from {edb_path}")
            if args.eval_weight <= 0:
                print("  (eval_weight=0 — evals will appear in output but not "
                      "influence move selection)")

    stats = pl.read_parquet(input_path)
    stats = stats.filter(pl.col("elo_band").is_not_null())

    if args.event:
        stats = stats.filter(pl.col("event") == args.event)
    if args.elo_band is not None:
        stats = stats.filter(pl.col("elo_band") == args.elo_band)

    print(f"Loaded {len(stats):,} edges, "
          f"{stats['parent_hash'].n_unique():,} distinct positions")

    # child_hash is now provided by Stage 2 — no per-edge re-parsing needed.
    if "child_hash" not in stats.columns:
        raise RuntimeError(
            "Stats parquet is missing the child_hash column. Re-run Stage 2 "
            "with the updated stage2_aggregate.py to populate it."
        )

    slices   = stats.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
    all_rows: list[dict] = []
    t_total  = time.time()

    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        mask   = (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        edges  = stats.filter(mask).to_dicts()

        t1 = time.time()
        values, best_moves, best_forcing, best_err, pos_epd, pos_side, slice_prior = run_backwards_induction(
            edges, args.perspective,
            prior_strength=args.prior_strength,
            forcing_weight=args.forcing_weight,
            forcing_prior=args.forcing_prior,
            forcing_baseline=args.forcing_baseline,
            min_move_games=args.min_move_games,
            eval_lookup=eval_lookup if eval_lookup else None,
            eval_weight=args.eval_weight,
            eval_weight_min=args.eval_weight_min,
            eval_weight_k=args.eval_weight_k,
            error_weight=args.error_weight,
            error_prior=args.error_prior,
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
                "opponent_error": best_err.get(ph),
                "eval_score":    eval_lookup.get(ph),
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
