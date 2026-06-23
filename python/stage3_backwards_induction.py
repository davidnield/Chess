"""
Stage 3: backwards induction on the position DAG.

Takes the aggregated (event, elo_band, position, move) statistics from Stage 2
and computes the best opening repertoire for the specified perspective. The goal
is a SHARP-but-SOUND blitz repertoire: fast wins where the data supports them,
without lines that collapse if the opponent finds the refutation.

Each position is valued by backwards induction in topological order (Kahn's
algorithm on the DAG), so transpositions are valued exactly once. TWO values
propagate per position:

  - value (mean):    expected white_score vs the AVERAGE (empirical) opponent.
                     The primary objective — this is what wins blitz games and
                     keeps trap value (people fumble dangerous lines).
  - value_robust:    expected white_score along the opponent's BEST reply (the
                     critical line). Drives the refutation gate. At opponent
                     nodes we follow their best reply (engine eval where the
                     resulting position is covered, else the empirically
                     best-for-them non-rare edge); at leaves it equals the mean.

Terminal leaves use a Beta-Binomial posterior mean instead of the raw empirical
score:  smoothed = (k * mu_slice + score_avg * n) / (k + n), with mu_slice the
slice's start-position white-score and k = --prior-strength. This pulls noisy
small-sample leaves toward the slice mean. Leaves optionally blend in a
Stockfish eval (--eval-weight; may vary by sample size via --eval-weight-k).

SELECTION at our turn maximises (white) / minimises (black):

    score = sign * value + crush_weight   * crush_rate
                         + decisiveness_weight * (1 - draw_rate)
                         + error_weight   * opponent_error
                         + forcing_weight * forcingness

restricted to a REFUTATION GATE: a move is eligible only if value_robust stays
within --robustness-floor of the slice prior (white: >= prior - floor; black:
<= prior + floor). This drops lines refuted by best defence (1...g5) while
keeping lines that hold (Blackmar-Diemer). floor >= 1.0 disables the gate. If
every candidate is gated, the gate is dropped for that node (sparse-tail fallback).

  crush_rate   -- the PRIMARY sharpness driver: fraction of games through the
                  edge where OUR side wins decisively (mate/resignation) by
                  full-move --crush-horizon (flat, result-based). Empirical-Bayes
                  shrunk toward the slice-mean crush with pseudocount --crush-prior,
                  so thin-sample edges can't manufacture a crush bonus. Requires
                  --crush-db (the crush_hist histogram) and --crush-weight > 0.
  decisiveness -- 1 - draws/total of the edge (legacy knob; ~flat in blitz).
  opponent_error -- expected score the opponent leaves on the table vs their
                  Stockfish-best reply, frequency-weighted. Requires --eval-db.
  forcingness  -- Simpson concentration of the opponent's replies (legacy; the
                  crush term superseded it as the sharpness driver).

--force-root-move commits OUR first move at the start position (e.g. e4/d4/Nf3),
letting the rest of the tree (and crush) sharpen the continuations.

NOTE on opponent modelling: at opponent-turn positions the MEAN value uses their
EMPIRICAL move distribution, not optimal play, so white- and black-perspective
values do NOT sum to 1 -- each is "expected score IF I play optimally and my
opponent plays like a typical player at this elo". The right framing for a
human-vs-human repertoire; the robustness gate is what guards the worst case.

Output columns per (event, elo_band, position): value, best_move (null at opp
turn), value_robust, crush_rate, decisiveness, opponent_error, forcingness,
eval_score.

Usage:
    .venv/Scripts/python.exe python/stage3_backwards_induction.py --perspective black
    # The canonical pooled repertoires are built by python/build_pooled_reps.py.
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


def crush(our_crush_sum: float | None, n_games: int | None,
          k_c: float = 20000.0, baseline: float = 0.0) -> float:
    """Flat result-based rate at which OUR side wins fast (mate/resignation).

    our_crush_sum is the count of OUR decisive wins (mate/resignation) through this
    edge that land on or before full-move `--crush-horizon` — a FLAT cutoff, no
    earliness decay — summed from the crush_hist histogram in main(). Dividing by
    n_games (all games through the edge) gives crush_rate = "fraction of games we
    crush by move H from here."

    Empirical-Bayes shrunk toward `baseline` with pseudocount k_c. Callers pass
    baseline = the SLICE-MEAN crush (compute_slice_mean_crush), NOT 0, so a
    thin-sample edge defaults to the mean (no spurious bonus) and value decides
    there; only well-sampled edges keep a real deviation. k_c large (default
    20000) is deliberate — see compute_slice_mean_crush / the crush-metric-design
    note. Returns `baseline` when the edge has no crush data (uncovered slice).
    """
    if not n_games or n_games <= 0 or our_crush_sum is None:
        return baseline
    raw = our_crush_sum / n_games
    return (k_c * baseline + raw * n_games) / (k_c + n_games)


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


def compute_slice_mean_crush(edges: list[dict], start_hash: int, perspective: str,
                             min_games: int = 100, fallback: float = 0.02) -> float:
    """Slice-wide OUR-crush rate (by-move-H decisive-win fraction), measured at the
    starting position. This is the empirical-Bayes prior mean that per-edge crush
    shrinks toward — analogous to compute_slice_prior for the win rate. Summing the
    start-position edges is exact: every game passes through exactly one first move,
    so Σ(our_crush_sum)/Σ(crush_games) = the slice's overall early-crush rate.

    Falls back to `fallback` if the slice has no crush data at the start.
    """
    key = "white_crush_sum" if perspective == "white" else "black_crush_sum"
    crush_sum = 0.0
    game_sum  = 0
    for e in edges:
        if e["parent_hash"] == start_hash and e.get("crush_games"):
            cs = e.get(key)
            if cs is not None:
                crush_sum += cs
                game_sum  += e["crush_games"]
    if game_sum < min_games:
        return fallback
    return crush_sum / game_sum


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
    decisiveness_weight: float = 0.0,
    robustness_floor: float = 1.0,
    robust_eval_weight: float = 1.0,
    crush_weight:     float = 0.0,
    crush_prior:      float = 200.0,
    force_root_move:  str | None = None,
) -> tuple[dict[int, float], dict[int, str | None], dict[int, float | None],
           dict[int, float | None], dict[int, float | None], dict[int, float | None],
           dict[int, float | None], dict[int, str], dict[int, chess.Color], float]:
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

    **Dual value.** Two values propagate per position:
      - value (mean): expected white_score vs the AVERAGE opponent (empirical
        reply distribution). Primary selection objective — wins games.
      - value_robust: value along the opponent's BEST reply (critical line).
        At opponent nodes we follow their best reply (Stockfish eval where the
        resulting position is in eval_lookup, else the empirically best-for-them
        edge among non-rare moves); at leaves value_robust == mean.

    **Selection** at our turn maximises (white) / minimises (black):

        sign * value + decisiveness_weight * (1 - draw_rate)
                     + error_weight * opponent_error
                     + forcing_weight * forcingness

    restricted to a *refutation gate*: a move is eligible only if value_robust
    is within `robustness_floor` of the slice prior (white: >= prior - floor;
    black: <= prior + floor). Drops lines that collapse against best defence
    (1...g5) while keeping lines that hold (Blackmar-Diemer). floor=1.0 disables
    the gate (legacy). If every move is gated, the gate is dropped for that node.

    decisiveness = 1 - draws/total (sharp, non-drawish → fast wins).
    opponent_error: expected score loss from opponent's typical replies vs
    their best (Stockfish). forcingness: Simpson concentration (legacy).

    Returns:
        values         -- position_hash -> expected white_score (mean, blended at leaves)
        best_moves     -- position_hash -> best move_san (None at opponent's turn)
        best_forcing   -- position_hash -> forcingness of chosen move (None at opp turn)
        best_error     -- position_hash -> opponent_error of chosen move (None at opp turn)
        best_decis     -- position_hash -> decisiveness of chosen move (None at opp turn)
        best_crush     -- position_hash -> crush_rate of chosen move (None at opp turn)
        values_robust  -- position_hash -> value along opponent's best reply (all positions)
        position_epd   -- position_hash -> EPD string
        position_side  -- position_hash -> chess.WHITE or chess.BLACK
        slice_prior    -- empirical white_score from starting position
    """
    our_color   = chess.WHITE if perspective == "white" else chess.BLACK
    sign        = 1.0 if perspective == "white" else -1.0
    start_hash  = zobrist_int64(chess.Board())
    slice_prior = compute_slice_prior(edges, start_hash)
    # Empirical-Bayes prior mean for crush: thin-sample edges shrink toward this
    # slice-wide crush rate (not 0), so noise can't manufacture a crush bonus.
    slice_mean_crush = compute_slice_mean_crush(edges, start_hash, perspective)

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
            "draws":      e.get("draws", 0),
            "white_crush_sum": e.get("white_crush_sum"),
            "black_crush_sum": e.get("black_crush_sum"),
            "crush_games":     e.get("crush_games"),
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
    values:        dict[int, float]        = {}   # expected vs AVERAGE opponent
    values_robust: dict[int, float]        = {}   # value along opponent's BEST reply
    best_moves:    dict[int, str | None]   = {}
    best_forcing:  dict[int, float | None] = {}
    best_error:    dict[int, float | None] = {}
    best_decis:    dict[int, float | None] = {}
    best_crush:    dict[int, float | None] = {}
    opp_is_white = (our_color == chess.BLACK)

    def build_move_vals(ph):
        """One dict per child edge: mean value, robust value, decisiveness,
        forcingness, opponent_error, and the opponent's preference for the reply."""
        mvs = []
        for m in children[ph]:
            ch = m["child_hash"]
            emp = smoothed_score(m["score_avg"], m["total"], slice_prior, prior_strength)
            covered = bool(eval_lookup) and ch in eval_lookup
            # MEAN value (vs average opponent — the trap-value objective):
            # propagated where valued, else empirical + (low) eval leaf blend.
            if ch in values:
                child_val = values[ch]
            elif eval_weight > 0 and covered:
                n_child = sum(m2["total"] for m2 in children.get(ch, []))
                ew = effective_eval_weight(eval_weight, eval_weight_min,
                                           eval_weight_k, n_child)
                child_val = (1.0 - ew) * emp + ew * eval_lookup[ch]
            else:
                child_val = emp
            # ROBUST value (drives the refutation gate). The engine eval of the
            # position THIS move reaches already assumes best play onward, so it
            # IS the objective robustness measure — use it directly where covered
            # (blended by robust_eval_weight). Only where uncovered do we fall
            # back to the propagated critical line, then to empirical. This is
            # what lets the gate see a line is lost vs best play even when it
            # scores fine empirically (e.g. 1...g5 → eval +1.2 for White).
            if covered:
                child_robust = ((1.0 - robust_eval_weight) * emp
                                + robust_eval_weight * eval_lookup[ch])
            elif ch in values:
                child_robust = values_robust.get(ch, child_val)
            else:
                child_robust = emp
            ch_moves = children.get(ch, [])
            frc  = forcingness(ch_moves, forcing_prior, forcing_baseline)
            oerr = opponent_error(ch_moves, eval_lookup, opp_is_white=opp_is_white,
                                  k_e=error_prior) if error_weight > 0 else 0.0
            tot  = m["total"]
            dec  = 1.0 - (m.get("draws", 0) / tot) if tot else 0.0
            # Crush rate: earliness-weighted rate at which WE win fast after this
            # move (from crush_stats, joined into the edge by main()). 0 when no
            # crush data. The PRIMARY sharpness driver.
            our_crush_sum = (m.get("white_crush_sum") if our_color == chess.WHITE
                             else m.get("black_crush_sum"))
            cr = (crush(our_crush_sum, m.get("crush_games"), crush_prior, slice_mean_crush)
                  if crush_weight > 0 else 0.0)
            # Opponent's preference for THIS reply on the white-expected-score
            # scale (eval where covered, else empirical edge score).
            pref = eval_lookup[ch] if (eval_lookup and ch in eval_lookup) else m["score_avg"]
            mvs.append({"san": m["move_san"], "val": child_val, "robust": child_robust,
                        "total": tot, "frc": frc, "opp_err": oerr, "dec": dec,
                        "crush": cr, "pref": pref})
        return mvs

    def passes_gate(mv):
        # Within robustness_floor of the prior assuming the opponent's best reply.
        if our_color == chess.WHITE:
            return mv["robust"] >= slice_prior - robustness_floor
        return mv["robust"] <= slice_prior + robustness_floor

    def select_our(mvs):
        base  = [mv for mv in mvs if mv["total"] >= min_move_games] or mvs
        gated = [mv for mv in base if passes_gate(mv)]
        cands = gated or base
        keyf = lambda mv: (sign * mv["val"] + crush_weight * mv["crush"]
                           + decisiveness_weight * mv["dec"]
                           + error_weight * mv["opp_err"] + forcing_weight * mv["frc"])
        return max(cands, key=keyf)

    def opp_robust(mvs):
        # Opponent plays their best reply (restricted to non-rare moves); we
        # inherit the robust value of that critical line.
        base = [mv for mv in mvs if mv["total"] >= min_move_games] or mvs
        if not base:
            return slice_prior
        best = (max(base, key=lambda mv: mv["pref"]) if opp_is_white
                else min(base, key=lambda mv: mv["pref"]))
        return best["robust"]

    def value_node(ph):
        mvs = build_move_vals(ph)
        if not mvs:
            values[ph] = values_robust[ph] = slice_prior
            best_moves[ph] = best_forcing[ph] = best_error[ph] = None
            best_decis[ph] = best_crush[ph] = None
            return
        if position_side[ph] == our_color:
            if force_root_move and ph == start_hash:
                forced = [mv for mv in mvs if mv["san"] == force_root_move]
                if not forced:
                    raise ValueError(
                        f"--force-root-move {force_root_move!r} is not a legal/known "
                        f"edge at the start position for this slice.")
                b = forced[0]
            else:
                b = select_our(mvs)
            values[ph]        = b["val"]
            values_robust[ph] = b["robust"]
            best_moves[ph]    = b["san"]
            best_forcing[ph]  = b["frc"]
            best_error[ph]    = b["opp_err"]
            best_decis[ph]    = b["dec"]
            best_crush[ph]    = b["crush"]
        else:
            # Opponent: mean over their empirical distribution (value);
            # critical-line follow for value_robust.
            total = sum(mv["total"] for mv in mvs)
            values[ph] = (sum(mv["val"] * mv["total"] for mv in mvs) / total
                          if total else slice_prior)
            values_robust[ph] = opp_robust(mvs)
            best_moves[ph] = best_forcing[ph] = best_error[ph] = None
            best_decis[ph] = best_crush[ph] = None

    while queue:
        ph = queue.popleft()
        value_node(ph)
        for parent in parents_of[ph]:
            pending[parent].discard(ph)
            if not pending[parent] and parent not in values:
                queue.append(parent)

    # Fallback for positions in cycles / unreachable in the topological pass.
    for ph in all_positions - set(values):
        value_node(ph)

    return (values, best_moves, best_forcing, best_error,
            best_decis, best_crush, values_robust, position_epd, position_side, slice_prior)


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
                        help="Path to eval DB parquet from build_lichess_eval_db.py. When "
                             "provided, Stockfish evals are used in move selection and written "
                             "to the output's eval_score column.")
    parser.add_argument("--eval-mate-cp", type=int, default=3000,
                        help="Drop eval_db entries with |eval_cp| >= this at load. RETIRED "
                             "safety: the old eval DB stamped +-10000 mate sentinels on quiet "
                             "positions (1.e4 read +10000), which this dropped. The rebuilt "
                             "build_lichess_eval_db.py caps decisive evals at +-2000, so nothing "
                             "reaches 3000 and this guard is now inert. Kept as a cheap backstop.")
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
    parser.add_argument("--decisiveness-weight", type=float, default=0.0,
                        help="Sharpness bonus = weight * (1 - draw_rate) of the move. "
                             "Rewards non-drawish, decisive lines (fast wins). "
                             "0.0 = disabled (default).")
    parser.add_argument("--robustness-floor", type=float, default=1.0,
                        help="Refutation gate: a move is eligible only if its value "
                             "against the opponent's BEST reply stays within this margin "
                             "of the slice prior (white: >= prior - floor; black: <= "
                             "prior + floor). Drops lines refuted by best defence (1...g5) "
                             "while keeping lines that hold (Blackmar-Diemer). "
                             "1.0 = gate disabled / legacy behaviour (default); 0.03 = tight.")
    parser.add_argument("--robust-eval-weight", type=float, default=1.0,
                        help="Eval weight used ONLY for the robust (critical-line) value "
                             "that drives the refutation gate. Decoupled from --eval-weight "
                             "so the mean objective can stay empirical (trap value) while the "
                             "gate uses objective eval. 1.0 = pure eval where covered (default), "
                             "empirical where not. Requires --eval-db for effect.")
    parser.add_argument("--crush-db", default=None,
                        help="Path to crush_stats parquet (from build_crush_stats.py). "
                             "When set with --crush-weight>0, adds the crush term (rate of "
                             "early decisive wins by mate/resignation) to move selection.")
    parser.add_argument("--crush-weight", type=float, default=0.0,
                        help="Weight on crush_rate in selection — the PRIMARY sharpness driver. "
                             "Rewards moves that lead to fast opponent resignations/mates "
                             "(earlier = higher). 0.0 = disabled (default). Try 0.3.")
    parser.add_argument("--crush-prior", type=float, default=20000.0,
                        help="Pseudocount (in games) for empirical-Bayes shrinkage of crush_rate "
                             "toward the SLICE-MEAN crush rate. An edge needs ~this many games "
                             "before its raw crush is trusted; thin-sample edges default to the "
                             "mean (no spurious crush bonus). Default 20000 (tuned: keeps "
                             "well-sampled gambits like the Danish/Smith-Morra while routing "
                             "thin-sample lines onto sound mainlines).")
    parser.add_argument("--crush-horizon", type=int, default=15,
                        help="Crush = fraction of games through an edge where OUR side wins "
                             "decisively (mate/resignation) by this FULL-MOVE number, flat (no "
                             "earliness decay). Default 15. Computed from the crush_hist histogram "
                             "by summing decisive-win buckets 1..horizon. (Ignored for legacy "
                             "crush_stats files that store a pre-baked weight.)")
    parser.add_argument("--force-root-move", default=None,
                        help="Commit OUR first move at the start position to this SAN "
                             "(e.g. 'e4' or 'd4'), letting the rest of the tree (and crush) "
                             "sharpen the continuations. Only meaningful for --perspective white. "
                             "Errors if the move isn't a known edge for a slice.")
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
    print(f"Decisiveness wt:   {args.decisiveness_weight}")
    print(f"Robustness floor:  {args.robustness_floor}"
          f"{'  (gate disabled)' if args.robustness_floor >= 1.0 else ''}")
    if args.robustness_floor < 1.0:
        print(f"Robust eval wt:    {args.robust_eval_weight}")
    print(f"Crush weight:      {args.crush_weight}"
          f"{'  (crush DB: ' + str(args.crush_db) + ')' if args.crush_weight > 0 else ''}")

    # ── Load eval DB (position_hash -> expected white score) ──────────────
    eval_lookup: dict[int, float] = {}
    if args.eval_db:
        edb_path = Path(args.eval_db)
        if not edb_path.exists():
            print(f"WARNING: eval DB not found at {edb_path} — proceeding without evals.")
        else:
            edb_raw = pl.read_parquet(str(edb_path))
            # Drop mate-class sentinels (|cp| >= eval_mate_cp, e.g. the +-10000 Lichess
            # mate codes). These are corrupt for quiet opening positions (e.g. 1.e4 reads
            # +10000) and would trivially pass the refutation gate / dominate the eval
            # blend. Dropping them reverts those positions to empirical (real deep mates
            # are ~winning empirically too). Full fix = rebuild eval_db with a correct
            # mate->cp mapping (separate task).
            n_raw = len(edb_raw)
            edb_raw = edb_raw.filter(pl.col("eval_cp").abs() < args.eval_mate_cp)
            n_drop = n_raw - len(edb_raw)
            # Vectorized sigmoid (= cp_to_expected_score) + zip, far faster than
            # iter_rows over ~300M entries.
            edb_raw = edb_raw.with_columns(
                (1.0 / (1.0 + (-LICHESS_CP_SCALE * pl.col("eval_cp")).exp())).alias("_es"))
            eval_lookup = dict(zip(edb_raw["position_hash"].to_list(),
                                   edb_raw["_es"].to_list()))
            print(f"Loaded {len(eval_lookup):,} Stockfish evals from {edb_path} "
                  f"(dropped {n_drop:,} mate-class |cp|>={args.eval_mate_cp})")
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

    # ── Crush overlay: LEFT JOIN crush_stats so each edge carries the crush sums.
    # Missing keys → nulls → crush_rate 0 (no effect). Only when --crush-weight>0.
    if args.crush_weight > 0 and args.crush_db:
        cdb_path = Path(args.crush_db)
        if not cdb_path.exists():
            print(f"WARNING: crush DB not found at {cdb_path} — proceeding without crush.")
        else:
            crush_df = pl.read_parquet(str(cdb_path))
            keys = ["event", "elo_band", "parent_hash", "move_san"]
            if "move_bucket" in crush_df.columns:
                # Histogram → flat result-based crush@horizon: count OUR decisive wins
                # (mate/resignation) landing on or before move `--crush-horizon`, with a
                # flat weight (no earliness decay). bucket 0 is the non-decisive remainder
                # and contributes only to the denominator (crush_games = total games).
                H = args.crush_horizon
                inwin = (pl.col("move_bucket") >= 1) & (pl.col("move_bucket") <= H)
                crush_df = crush_df.group_by(keys).agg(
                    pl.when(inwin).then(pl.col("white_wins")).otherwise(0)
                      .sum().cast(pl.Float64).alias("white_crush_sum"),
                    pl.when(inwin).then(pl.col("black_wins")).otherwise(0)
                      .sum().cast(pl.Float64).alias("black_crush_sum"),
                    pl.col("n").sum().alias("crush_games"))
                print(f"Crush = fraction of games with a decisive win by move {H} "
                      f"(flat, result-based) from {cdb_path}")
            else:
                crush_df = crush_df.select(
                    keys + ["white_crush_sum", "black_crush_sum", "crush_games"])
            stats = stats.join(crush_df, on=keys, how="left")
            n_cov = stats.filter(pl.col("crush_games").is_not_null()).height
            print(f"Joined crush data from {cdb_path}: "
                  f"{n_cov:,}/{len(stats):,} edges have crush data")
    if "white_crush_sum" not in stats.columns:
        # Ensure columns exist so to_dicts() yields them (None) when crush is off.
        stats = stats.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("white_crush_sum"),
            pl.lit(None, dtype=pl.Float64).alias("black_crush_sum"),
            pl.lit(None, dtype=pl.Int64).alias("crush_games"),
        )

    slices   = stats.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
    all_rows: list[dict] = []
    t_total  = time.time()

    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        mask   = (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        edges  = stats.filter(mask).to_dicts()

        t1 = time.time()
        (values, best_moves, best_forcing, best_err, best_decis, best_crush, vals_robust,
         pos_epd, pos_side, slice_prior) = run_backwards_induction(
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
            decisiveness_weight=args.decisiveness_weight,
            robustness_floor=args.robustness_floor,
            robust_eval_weight=args.robust_eval_weight,
            crush_weight=args.crush_weight,
            crush_prior=args.crush_prior,
            force_root_move=args.force_root_move,
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
                "decisiveness":  best_decis.get(ph),
                "crush_rate":    best_crush.get(ph),
                "value_robust":  vals_robust.get(ph),
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
