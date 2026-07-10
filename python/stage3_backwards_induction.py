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
                         + cover_weight   * coverage_efficiency
                         - memo_weight    * memorization_cost

restricted to a REFUTATION GATE: a move is eligible only if its value along the
opponent's BEST defence stays within --robustness-floor of the slice prior (white:
>= prior - floor; black: <= prior + floor). The gate metric is value_worst by default
(our PREPARED book vs best defence; --gate-metric worst) or value_robust (objective
best-play-by-both-sides eval; --gate-metric robust). This drops lines refuted by best
defence (1...g5) while keeping lines that hold (Blackmar-Diemer). floor >= 1.0 disables
the gate. If every candidate is gated, the gate is dropped for that node (sparse-tail fallback).

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
  coverage_efficiency -- covered opponent-decision DEPTH (reach-weighted) per
                  memorized prepared BRANCH in the subtree a move enters. The propagated,
                  mass-weighted generalization of forcingness: rewards forcing /
                  consolidating lines that keep much of the opponent's mass on prepared
                  rails with few lines to learn, and penalizes fan-out. A bare leaf scores
                  0 (no coverage), so it can't be gamed by leaving book early. Prepared =
                  replies with >= --cover-min-games. Requires --cover-weight > 0.
  memorization_cost -- propagated value-at-stake-if-you-forget (criticality of the
                  chosen move vs the natural move + downstream); --memo-weight penalty.
                  Complements coverage_efficiency (criticality vs volume of lines).

--force-root-move commits OUR first move at the start position (e.g. e4/d4/Nf3),
letting the rest of the tree (and crush) sharpen the continuations.

NOTE on opponent modelling: at opponent-turn positions the MEAN value uses their
EMPIRICAL move distribution, not optimal play, so white- and black-perspective
values do NOT sum to 1 -- each is "expected score IF I play optimally and my
opponent plays like a typical player at this elo". The right framing for a
human-vs-human repertoire; the robustness gate is what guards the worst case.

Output columns per (event, elo_band, position): value, best_move (null at opp
turn), value_robust, value_worst, crush_rate, crush_potential, memo_cost, cover_eff,
decisiveness, opponent_error, forcingness, eval_score.

Usage:
    .venv/Scripts/python.exe python/stage3_backwards_induction.py --perspective black
    # The canonical pooled repertoires are built by python/build_pooled_reps.py.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from collections import Counter, defaultdict, deque
from pathlib import Path

import chess
import chess.polyglot
import numpy as np
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


# Context key for context-free plan-prior rows (must match plan_consistency_report).
LEARN_GLOBAL_CTX = "(all games)"


def idea_token(san: str) -> str:
    """Normalize a SAN into an 'idea token' for the learnability plan prior.
    Piece moves become piece + destination (captures and disambiguators
    stripped — the idea is WHERE the piece goes); pawn pushes are the
    destination square; pawn captures keep their source file (cxd5 and exd5
    are different ideas); castling is kept verbatim; promotions keep the
    promotion piece. Selection (this module) and measurement
    (plan_consistency_report.py) MUST share this definition."""
    s = san.rstrip("+#")
    if s.startswith("O-O"):
        return s
    if s[0] in "NBRQK":
        dest = s.split("=")[0][-2:]
        return s[0] + dest
    return s  # pawn move: "c5", "cxd5", "e8=Q"


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


def compute_slice_mean_crush(edges: list[dict], start_hash: int, sum_col: str,
                             min_games: int = 100, fallback: float = 0.02) -> float:
    """Slice-wide OUR-crush rate for the per-edge crush sum column `sum_col`,
    measured at the starting position — the empirical-Bayes prior mean that the
    per-edge crush shrinks toward (analogous to compute_slice_prior for the win
    rate). Summing the start-position edges is exact: every game passes through
    exactly one first move, so Σ(sum_col)/Σ(crush_games) = the slice's overall rate.

    `sum_col` is e.g. "white_crush_sum" (absolute), "white_imm_sum" or
    "white_dfull_sum" (relative-propagated). Falls back to `fallback` if the slice
    has no crush data at the start.
    """
    crush_sum = 0.0
    game_sum  = 0
    for e in edges:
        if e["parent_hash"] == start_hash and e.get("crush_games"):
            cs = e.get(sum_col)
            if cs is not None:
                crush_sum += cs
                game_sum  += e["crush_games"]
    if game_sum < min_games:
        return fallback
    return crush_sum / game_sum


def _tarjan_sccs(nodes: set[int], succ: dict[int, list[int]]) -> list[list[int]]:
    """Iterative Tarjan over the subgraph induced by `nodes` (successors already
    restricted to `nodes` by the caller). Returns strongly connected components in
    REVERSE TOPOLOGICAL order of the condensation — every SCC reachable from S is
    emitted before S — which is exactly the children-first processing order the
    backwards induction needs. Explicit (node, child_index) stack frames: the
    input can be millions of nodes, so recursion is not an option."""
    index: dict[int, int] = {}
    low: dict[int, int] = {}
    on_stack: set[int] = set()
    stack: list[int] = []
    sccs: list[list[int]] = []
    counter = 0
    for root in nodes:
        if root in index:
            continue
        work = [(root, 0)]
        while work:
            node, ci = work[-1]
            if ci == 0:
                index[node] = low[node] = counter
                counter += 1
                stack.append(node)
                on_stack.add(node)
            kids = succ.get(node, ())
            advanced = False
            while ci < len(kids):
                ch = kids[ci]
                ci += 1
                if ch not in index:
                    work[-1] = (node, ci)
                    work.append((ch, 0))
                    advanced = True
                    break
                if ch in on_stack:
                    low[node] = min(low[node], index[ch])
            if advanced:
                continue
            work.pop()
            if low[node] == index[node]:
                scc = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    scc.append(w)
                    if w == node:
                        break
                sccs.append(scc)
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
    return sccs


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
    gate_metric:      str = "worst",
    gate_rel_floor:   float = 1.0,
    robust_eval_weight: float = 1.0,
    crush_weight:     float = 0.0,
    crush_prior:      float = 200.0,
    crush_mode:       str = "absolute",
    crush_gamma:      float = 0.8,
    crush_imm_window: int = 2,
    crush_baseline:   str = "mean",
    force_root_move:  str | None = None,
    require_eval:     bool = False,
    memo_weight:      float = 0.0,
    memo_prior:       float = 200.0,
    memo_baseline:    float = 0.02,
    memo_leave:       float = 0.0,
    cover_weight:     float = 0.0,
    cover_min_games:  int = 0,
    augment_engine:   bool = False,
    full_eval_hashes: "np.ndarray | None" = None,
    full_eval_es:     "np.ndarray | None" = None,
    learn_prior:      dict[tuple[str, str], float] | None = None,
    learn_ctx:        dict[int, str] | None = None,
    learn_reach:      dict[int, float] | None = None,
    learn_ctx_share:  dict[str, float] | None = None,
    learn_depth:      dict[int, int] | None = None,
    learn_delta_main: float = 0.005,
    learn_delta_rare: float = 0.04,
    learn_reach_pivot: float = 0.02,
    learn_ctx_pivot:  float = 0.05,
    learn_depth_horizon: int = 6,
) -> tuple[dict[int, float], dict[int, str | None], dict[int, float | None],
           dict[int, float | None], dict[int, float | None], dict[int, float | None],
           dict[int, float], dict[int, float], dict[int, float],
           dict[int, float], dict[int, float | None], dict[int, str],
           dict[int, chess.Color], float, dict[int, bool]]:
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
        best_crush     -- position_hash -> crush_rate of chosen move (None at opp turn).
                          In relative-propagated mode this is the LOCAL relative crush
                          (dfull) of the chosen move; in absolute mode the crush@H rate.
        crush_pot      -- position_hash -> propagated crush_potential (relative mode only;
                          empty dict in absolute mode). All positions (our + opp).
        memo_pot       -- position_hash -> propagated memorization cost. Our nodes:
                          shrunk deviation penalty of the chosen move + child's memo;
                          opp nodes: reach (frequency) weighted expected child memo.
                          memo_pot[start] telescopes to E[value-at-stake-from-forgetting
                          per game] = the repertoire's total memorization burden.
        cover_effs     -- position_hash -> coverage efficiency = covered opponent-decision
                          DEPTH (reach-weighted) per memorized prepared BRANCH below here.
                          High = forcing/consolidating (much opponent mass kept on rails with
                          few lines to learn); low = fan-out. The cover_weight term rewards it.
        value_worst    -- position_hash -> OUR-book value assuming the opponent plays the
                          reply WORST for us at every node (among non-rare moves). Unlike
                          values_robust (which substitutes the objective engine eval at
                          covered nodes), this propagates OUR actual chosen moves, so it is
                          a true worst-case of the repertoire and is <= value (sign-aware).
        values_robust  -- position_hash -> value along opponent's best reply (all positions)
        position_epd   -- position_hash -> EPD string
        position_side  -- position_hash -> chess.WHITE or chess.BLACK
        slice_prior    -- empirical white_score from starting position
    """
    our_color   = chess.WHITE if perspective == "white" else chess.BLACK
    sign        = 1.0 if perspective == "white" else -1.0
    start_hash  = zobrist_int64(chess.Board())
    slice_prior = compute_slice_prior(edges, start_hash)
    gamma_hop = crush_gamma ** 0.5   # per-PLY discount (one propagation hop = one ply)
    # Empirical-Bayes prior means for crush: thin-sample edges shrink toward the
    # slice-wide rate (not 0), so noise can't manufacture a crush bonus. Absolute
    # mode shrinks the crush@H rate; relative mode shrinks the immediate-window and
    # discounted-full rates separately (different scales).
    # crush_baseline="zero" shrinks each edge's crush toward 0 (assume NO crush until
    # proven by many games), not toward the slice mean. This stops a thin-sample edge
    # from defaulting to the average crush and out-pointing a well-sampled below-average
    # move (the 9.f4-over-9.Qg6+ artifact). With baseline 0 the pseudocount must stay
    # large or selection-biased thin lines slip through — hence crush_prior is swept.
    col = "white" if our_color == chess.WHITE else "black"
    if crush_baseline == "zero":
        slice_mean_crush = slice_mean_imm = slice_mean_dfull = 0.0
    else:
        slice_mean_crush = compute_slice_mean_crush(edges, start_hash, f"{col}_crush_sum")
        slice_mean_imm   = compute_slice_mean_crush(edges, start_hash, f"{col}_imm_sum")
        slice_mean_dfull = compute_slice_mean_crush(edges, start_hash, f"{col}_dfull_sum")

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
            "white_imm_sum":   e.get("white_imm_sum"),
            "black_imm_sum":   e.get("black_imm_sum"),
            "white_dfull_sum": e.get("white_dfull_sum"),
            "black_dfull_sum": e.get("black_dfull_sum"),
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
    best_aug:      dict[int, bool]         = {}   # True where the chosen move came from engine augmentation
    crush_pot:     dict[int, float]        = {}   # propagated crush_potential (relative mode)
    memo_pot:      dict[int, float]        = {}   # propagated memorization cost (all positions)
    value_worst:   dict[int, float]        = {}   # OUR-book value vs the opponent's BEST defence
    cover_depth:   dict[int, float]        = {}   # reach-weighted covered opponent-decision depth
    mem_nodes:     dict[int, float]        = {}   # unweighted count of prepared opponent branches
    opp_is_white = (our_color == chess.BLACK)
    relative_crush = (crush_mode == "relative-propagated" and crush_weight > 0)
    # Engine-candidate augmentation is only live when the caller supplied the full
    # (un-prefiltered) eval DB as sorted arrays — the prefiltered eval_lookup can't
    # see legal-but-unplayed children (they're outside the input DAG).
    _augment = bool(augment_engine) and full_eval_hashes is not None
    _aug_cache: dict[int, list[dict]] = {}
    rel_gated_nodes: set[int] = set()   # our-turn nodes where the relative gate pruned >=1 move
                                        # (a set, not a counter: cycle fixpoint sweeps revisit nodes)
    _learn = learn_prior is not None and learn_delta_rare > 0.0
    learn_override_nodes: set[int] = set()  # nodes where the plan-prior tiebreak changed the pick

    def learn_delta(ph):
        # δ window for the learnability tiebreak: loose ONLY at SHALLOW-but-RARE
        # nodes — the opponent's offbeat opening choices (1.b3, the King's Gambit)
        # where habitual development should replace memorized nuance. Deep nodes
        # and off-walk nodes stay at the tight δ: they are individually rare but
        # collectively carry most of the tree's mass, and letting them all concede
        # is what bled ~1.4%% effectiveness in the v1 calibration.
        if learn_depth is not None:
            d = learn_depth.get(ph)
            if d is None or d >= learn_depth_horizon:
                return learn_delta_main
        r = (learn_reach or {}).get(ph, 0.0)
        t = min(1.0, r / learn_reach_pivot) if learn_reach_pivot > 0 else 1.0
        return learn_delta_rare + (learn_delta_main - learn_delta_rare) * t

    def learn_freq(ph, san):
        # Plan-prior frequency of this move's idea: the node's dominant-context
        # habit shrunk toward the GLOBAL habit by context prevalence. A rare
        # context (share << learn_ctx_pivot) mostly inherits the global habits —
        # its own pass-1 oddities (a precise maneuver vs a 1% sideline) must NOT
        # self-reinforce; a common context (vs 1.e4 / 1.d4) keeps its own plans.
        tok = idea_token(san)
        gf = learn_prior.get((LEARN_GLOBAL_CTX, tok), 0.0)
        ctx = (learn_ctx or {}).get(ph)
        if ctx is None:
            return gf
        cf = learn_prior.get((ctx, tok), 0.0)
        if learn_ctx_share is None or learn_ctx_pivot <= 0:
            lam = 1.0
        else:
            lam = min(1.0, learn_ctx_share.get(ctx, 0.0) / learn_ctx_pivot)
        return lam * cf + (1.0 - lam) * gf

    def shrunk_dev(mv_val, nat_val, n_p):
        # Vertical memorization cost: value you'd lose by forgetting our book move
        # and playing the most-played (natural) move instead — from OUR perspective
        # (sign-corrected, floored at 0). Bayesian-shrunk toward a small baseline by
        # the node's sample size, so a thin position can't look spuriously cheap (or
        # expensive); same shrink pattern as crush()/forcingness().
        d_raw = max(0.0, sign * (mv_val - nat_val))
        return ((memo_prior * memo_baseline + d_raw * n_p) / (memo_prior + n_p)
                if (memo_prior + n_p) > 0 else d_raw)

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
            ng = m.get("crush_games")
            # ABSOLUTE crush rate: flat decisive-win-by-move-H rate for this edge,
            # shrunk toward the slice mean. A per-edge bonus, NOT propagated.
            our_crush_sum = (m.get("white_crush_sum") if our_color == chess.WHITE
                             else m.get("black_crush_sum"))
            cr = (crush(our_crush_sum, ng, crush_prior, slice_mean_crush)
                  if (crush_weight > 0 and not relative_crush) else 0.0)
            # RELATIVE-PROPAGATED crush: LineCrush = hazard of crushing within the
            # immediate window + (1-hazard)·γ_hop·(child's propagated crush_potential);
            # at the tree frontier (child not valued) fall back to the discounted
            # empirical tail dfull, which captures kills beyond the ply-30 tree.
            line_crush = 0.0
            dfull = 0.0
            if relative_crush:
                imm_sum   = (m.get("white_imm_sum") if our_color == chess.WHITE
                             else m.get("black_imm_sum"))
                dfull_sum = (m.get("white_dfull_sum") if our_color == chess.WHITE
                             else m.get("black_dfull_sum"))
                imm   = crush(imm_sum,   ng, crush_prior, slice_mean_imm)
                dfull = crush(dfull_sum, ng, crush_prior, slice_mean_dfull)
                if ch in crush_pot:
                    line_crush = imm + (1.0 - imm) * gamma_hop * crush_pot[ch]
                else:
                    line_crush = dfull   # frontier / leaf: empirical discounted tail
            # Opponent's preference for THIS reply on the white-expected-score
            # scale (eval where covered, else empirical edge score).
            pref = eval_lookup[ch] if (eval_lookup and ch in eval_lookup) else m["score_avg"]
            mvs.append({"san": m["move_san"], "val": child_val, "robust": child_robust,
                        "worst": value_worst.get(ch, child_val),
                        "total": tot, "frc": frc, "opp_err": oerr, "dec": dec,
                        "crush": cr, "line_crush": line_crush, "dfull": dfull, "pref": pref,
                        "covered": covered, "child": ch})
        return mvs

    def gate_rv(mv):
        # The robustness measure the refutation gates compare. gate_metric picks it:
        #   "eval"   = the ENGINE EVAL of the position the move reaches (Lichess deep eval,
        #              expected-score form). Rejects objectively-unsound moves EVEN WHEN NO
        #              REFUTATION APPEARS IN THE DATA — the engine's best-play assessment is
        #              independent of whether opponents actually punished the move. This is
        #              the fix for the missing-refutation blind spot (e.g. 3.Bh6 hangs a
        #              bishop: eval −4.3, but its only recorded reply declines it, so 'worst'
        #              waves it through). Falls back to value_worst only where the position
        #              is not eval-covered (engine blind → keep the empirical guard).
        #   "worst"  = value_worst — OUR prepared book vs the opponent's best RECORDED reply.
        #              Blind to refutations absent from the data (see 'eval').
        #   "robust" = value_robust — propagated best-play-by-both (legacy; optimistic).
        if gate_metric == "eval":
            ev = eval_lookup.get(mv["child"]) if eval_lookup else None
            return ev if ev is not None else mv["worst"]
        return mv["worst"] if gate_metric == "worst" else mv["robust"]

    def passes_gate(mv):
        # ABSOLUTE refutation gate: a candidate move is eligible only if its
        # robustness measure stays within robustness_floor of the slice prior.
        rv = gate_rv(mv)
        if our_color == chess.WHITE:
            return rv >= slice_prior - robustness_floor
        return rv <= slice_prior + robustness_floor

    def apply_rel_gate(ph, cands_list):
        """RELATIVE refutation gate: drop candidates conceding more than
        gate_rel_floor (expected-score units) vs the BEST candidate's gate value.
        Catches advantage-squandering moves the absolute gate can't — moves that
        stay above the absolute bar but give back a won position because opponents
        usually misplay them (e.g. 8...Nd4 in the Bxf7+ Italian: empirically great,
        but concedes −2.1 → +0.1 vs best play while 8...Qd7 keeps it all). The best
        candidate always survives, so a non-empty list never empties. Moves without
        a genuine eval (gate_metric='eval', uncovered child) are EXEMPT — their rv
        is a propagated empirical value, not comparable to sibling engine evals."""
        if gate_rel_floor >= 1.0 or len(cands_list) < 2:
            return cands_list
        rvs = [(gate_rv(mv), gate_metric != "eval" or mv["covered"])
               for mv in cands_list]
        elig = [rv for rv, ok in rvs if ok]
        if not elig:
            return cands_list
        if our_color == chess.WHITE:
            best = max(elig)
            kept = [mv for mv, (rv, ok) in zip(cands_list, rvs)
                    if not ok or rv >= best - gate_rel_floor]
        else:
            best = min(elig)
            kept = [mv for mv, (rv, ok) in zip(cands_list, rvs)
                    if not ok or rv <= best + gate_rel_floor]
        if len(kept) < len(cands_list):
            rel_gated_nodes.add(ph)
        return kept

    def crush_term(mv):
        # Selection crush contribution: the propagated LineCrush in relative mode,
        # else the flat per-edge crush@H.
        return mv["line_crush"] if relative_crush else mv["crush"]

    def cover_eff(ch):
        # Coverage efficiency of the subtree entered by playing into child `ch`: covered
        # opponent-decision DEPTH (reach-weighted) per memorized BRANCH. High = forcing /
        # consolidating (much of the opponent's mass kept on rails with few lines to learn);
        # low = fan-out (many branches, mass leaks to the rare tail). A bare leaf / out-of-book
        # child has depth 0 -> eff 0, so it can't be gamed by exiting book early.
        return cover_depth.get(ch, 0.0) / (1.0 + mem_nodes.get(ch, 0.0))

    def augmented_candidates(ph):
        """Engine-move rescue for a FORCED-LOSING node (used only when every recorded
        move fails the gate). Offers legal — possibly never-played — moves whose
        resulting position the FULL eval DB scores acceptably: a 1-ply lookahead over
        the eval DB. Fixes the missing-improvement blind spot where the sole recorded
        continuation is objectively lost but a better move exists unplayed (e.g. the
        6...c6 rescue in a deep Englund line). Returns only gate-passing moves as
        eval-only move-val dicts (no empirical stats, no crush). Memoized per node."""
        cached = _aug_cache.get(ph)
        if cached is not None:
            return cached
        board = chess.Board(position_epd[ph])
        existing = {m["child_hash"] for m in children[ph]}   # dedup by child, not SAN
        sans, child_hashes = [], []
        for mv in board.legal_moves:
            san = board.san(mv)          # SAN is computed at the pre-push position
            board.push(mv)
            ch = zobrist_int64(board)
            board.pop()
            if ch in existing:
                continue
            sans.append(san)
            child_hashes.append(ch)
        out: list[dict] = []
        if child_hashes:
            keys = np.asarray(child_hashes, dtype=np.int64)
            idx = np.clip(np.searchsorted(full_eval_hashes, keys), 0,
                          len(full_eval_hashes) - 1)
            hit = full_eval_hashes[idx] == keys
            for san, ch, i, ok in zip(sans, child_hashes, idx, hit):
                if not ok:
                    continue
                es = float(full_eval_es[i])
                # eval-only dict mirroring build_move_vals' shape. worst/robust=es so
                # passes_gate (which falls back to mv["worst"] when the child isn't in
                # the prefiltered eval_lookup) gates on the engine eval directly.
                cand = {"san": san, "val": es, "robust": es, "worst": es,
                        "total": 0, "frc": 0.0, "opp_err": 0.0, "dec": 0.0,
                        "crush": 0.0, "line_crush": 0.0, "dfull": 0.0, "pref": es,
                        "covered": True, "child": ch, "aug": True}
                if passes_gate(cand):
                    out.append(cand)
        _aug_cache[ph] = out
        return out

    def select_our(ph, mvs):
        base  = [mv for mv in mvs if mv["total"] >= min_move_games] or mvs
        if require_eval:
            # Only consider moves whose resulting position is in the eval DB.
            # If none is, we're past the eval-DB frontier → no eligible move
            # (the caller truncates our recommendation here).
            cov = [mv for mv in base if mv["covered"]]
            if not cov:
                return None
            base = cov
        gated = [mv for mv in base if passes_gate(mv)]
        gated = apply_rel_gate(ph, gated)
        if not gated and _augment:
            # Forced-losing node: no recorded move passes the gate. Try engine-move
            # rescues (legal, possibly unplayed, eval-covered, gate-passing) before
            # falling back to the least-bad recorded move.
            aug = augmented_candidates(ph)
            if aug:
                # prefer gate-passing engine moves over gate-failing base; the
                # relative gate applies among the rescues too
                gated = apply_rel_gate(ph, aug)
        cands = gated or base
        # Memorization penalty (Lagrangian of a memo budget): forgetting cost of this
        # move (shrunk deviation vs the natural move) + the reach-weighted downstream
        # cost already propagated into the child. memo_weight=0 → term vanishes.
        n_p = sum(mv["total"] for mv in mvs) or 0
        nat_val = max(mvs, key=lambda mv: mv["total"])["val"]
        keyf = lambda mv: (sign * mv["val"] + crush_weight * crush_term(mv)
                           + decisiveness_weight * mv["dec"]
                           + error_weight * mv["opp_err"] + forcing_weight * mv["frc"]
                           + cover_weight * cover_eff(mv["child"])
                           - memo_weight * (shrunk_dev(mv["val"], nat_val, n_p)
                                            + memo_pot.get(mv["child"], memo_leave)))
        best = max(cands, key=keyf)
        if _learn and len(cands) > 1:
            # LEARNABILITY tiebreak: among candidates within δ of the best selection
            # key (near-optimal by our own objective — gates already applied, so all
            # are sound), prefer the move whose IDEA we play most often in this
            # context. Bounded concession per node; δ scales with node rarity.
            bk = keyf(best)
            delta = learn_delta(ph)
            elig = [mv for mv in cands if keyf(mv) >= bk - delta]
            if len(elig) > 1:
                pick = max(elig, key=lambda mv: (learn_freq(ph, mv["san"]), keyf(mv)))
                if pick is not best and (learn_freq(ph, pick["san"])
                                         > learn_freq(ph, best["san"])):
                    learn_override_nodes.add(ph)
                    best = pick
        return best

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
            values[ph] = values_robust[ph] = value_worst[ph] = slice_prior
            best_moves[ph] = best_forcing[ph] = best_error[ph] = None
            best_decis[ph] = best_crush[ph] = None
            if relative_crush:
                crush_pot[ph] = 0.0
            # A childless node at OUR turn = we've left book (no prepared continuation)
            # → leaving-book penalty; at the opponent's turn it's a terminal (game over).
            memo_pot[ph] = memo_leave if position_side[ph] == our_color else 0.0
            cover_depth[ph] = mem_nodes[ph] = 0.0  # no opponent decisions below → 0 coverage, 0 lines
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
                b = select_our(ph, mvs)
            if b is None:
                # require_eval and no candidate reaches an evaluated position: we
                # are past the eval-DB frontier. Truncate our recommendation here
                # (no best_move) and value the node by its own eval if present,
                # else the slice prior — a clean stop where engine knowledge ends.
                own = eval_lookup.get(ph) if eval_lookup else None
                v = own if own is not None else slice_prior
                values[ph] = values_robust[ph] = value_worst[ph] = v
                best_moves[ph] = best_forcing[ph] = best_error[ph] = None
                best_decis[ph] = best_crush[ph] = None
                if relative_crush:
                    crush_pot[ph] = 0.0
                # We are out of book here — assign the leaving-book penalty.
                memo_pot[ph] = memo_leave
                cover_depth[ph] = mem_nodes[ph] = 0.0  # truncated → nothing prepared below
                return
            values[ph]        = b["val"]
            values_robust[ph] = b["robust"]
            best_moves[ph]    = b["san"]
            best_aug[ph]      = b.get("aug", False)
            best_forcing[ph]  = b["frc"]
            best_error[ph]    = b["opp_err"]
            best_decis[ph]    = b["dec"]
            # crush_rate column: local relative crush (dfull) in relative mode, else crush@H.
            best_crush[ph]    = b["dfull"] if relative_crush else b["crush"]
            if relative_crush:
                # crush_potential of THIS position = the line we actually play.
                crush_pot[ph] = b["line_crush"]
            # memo cost of THIS node = forgetting cost of the chosen move + child's memo
            # (out-of-book child → leaving-book penalty).
            n_p = sum(mv["total"] for mv in mvs) or 0
            nat_val = max(mvs, key=lambda mv: mv["total"])["val"]
            memo_pot[ph] = (shrunk_dev(b["val"], nat_val, n_p)
                            + memo_pot.get(b["child"], memo_leave))
            # worst-case value: we play our book move; inherit the child's worst-case
            # (leaf/out-of-book child falls back to that edge's leaf value, like `value`).
            value_worst[ph] = value_worst.get(b["child"], b["val"])
            # coverage: our move adds no opponent branching — inherit the chosen child's.
            cover_depth[ph] = cover_depth.get(b["child"], 0.0)
            mem_nodes[ph]   = mem_nodes.get(b["child"], 0.0)
        else:
            # Opponent: mean over their empirical distribution (value);
            # critical-line follow for value_robust.
            total = sum(mv["total"] for mv in mvs)
            values[ph] = (sum(mv["val"] * mv["total"] for mv in mvs) / total
                          if total else slice_prior)
            values_robust[ph] = opp_robust(mvs)
            best_moves[ph] = best_forcing[ph] = best_error[ph] = None
            if relative_crush:
                # crush_potential = EXPECTED LineCrush over their empirical replies.
                crush_pot[ph] = (sum(mv["line_crush"] * mv["total"] for mv in mvs) / total
                                 if total else 0.0)
            best_decis[ph] = best_crush[ph] = None
            # memo cost = reach (frequency) weighted expected memo over their replies
            # (an out-of-book child contributes the leaving-book penalty, not 0).
            memo_pot[ph] = (sum(memo_pot.get(mv["child"], memo_leave) * mv["total"] for mv in mvs)
                            / total if total else 0.0)
            # worst-case value: opponent plays the reply WORST for us, among non-rare
            # moves (so a 1-game freak can't define it); we then follow our book.
            base_w = [mv for mv in mvs if mv["total"] >= min_move_games] or mvs
            vws = [value_worst.get(mv["child"], mv["val"]) for mv in base_w]
            value_worst[ph] = ((min(vws) if our_color == chess.WHITE else max(vws))
                               if vws else slice_prior)
            # coverage: covered opponent-decision DEPTH (reach-weighted) and the count of
            # prepared reply-branches to memorize (unweighted). Replies below cover_min_games
            # are not prepared → their mass is NOT covered (lowers cover_depth) and they add
            # no branch. Denominator is ALL replies, so the rare tail genuinely leaks away.
            prep = [mv for mv in mvs if mv["total"] >= cover_min_games]
            cover_depth[ph] = (sum((mv["total"] / total) * (1.0 + cover_depth.get(mv["child"], 0.0))
                                   for mv in prep) if total else 0.0)
            mem_nodes[ph] = (sum(1.0 + (mv["total"] / total) * mem_nodes.get(mv["child"], 0.0)
                                 for mv in prep) if total else 0.0)

    while queue:
        ph = queue.popleft()
        value_node(ph)
        for parent in parents_of[ph]:
            pending[parent].discard(ph)
            if not pending[parent] and parent not in values:
                queue.append(parent)

    # ── Cycle handling ────────────────────────────────────────────────────────
    # The queue drains without valuing exactly the positions from which a CYCLE is
    # reachable (reversible piece-shuffle loops recorded with >= min_games break the
    # DAG assumption — e.g. 1.Nf3 Nf6 2.Ng1 Ng8 returns to the exact start hash).
    # The old code swept them once in arbitrary set order, silently substituting
    # leaf values for unvalued children and freezing the results — measured to
    # mis-select ~10k moves (incl. the root) on the 2019-25 pool. Instead: Tarjan
    # SCCs over the leftover subgraph (emitted in reverse topological order of the
    # condensation, i.e. children-first), value singleton SCCs exactly as the queue
    # phase would, and fixpoint-iterate inside each multi-node SCC. memo_pot /
    # cover_depth / mem_nodes are deliberately EXCLUDED from the convergence gate:
    # memo_pot accumulates additively along the chosen chain, so a converged
    # best-move chain that stays inside an SCC (repetition legitimately best)
    # diverges there by construction — tracked separately, never gating.
    leftover = all_positions - set(values)
    if leftover:
        print(f"  cycles: {len(leftover):,}/{len(all_positions):,} positions "
              f"unreached by the topological queue (cycle members + ancestors)",
              flush=True)
        sub_succ = {ph: [m["child_hash"] for m in children[ph]
                         if m["child_hash"] in leftover]
                    for ph in leftover}
        sccs = _tarjan_sccs(leftover, sub_succ)
        multi = [s for s in sccs if len(s) > 1]
        hist = Counter(len(s) for s in multi)
        print(f"  cycles: {len(multi):,} true SCCs (size histogram "
              f"{dict(sorted(hist.items()))}), "
              f"{len(sccs) - len(multi):,} acyclic ancestors", flush=True)

        # Gauss-Seidel value iteration over an SCC does not always converge: the
        # max-selection (`value`) and the argmin coupling (`value_worst`) can chase
        # each other into a bounded LIMIT CYCLE (observed amplitude up to ~0.07; more
        # sweeps make it worse, not better). Nothing diverges — all quantities are in
        # [0,1] — it just oscillates. The fix is DAMPING (successive under-relaxation):
        # each update is blended with the prior value, V' = V + α(f(V) - V). This
        # preserves the fixpoint (V = f(V) is unchanged) but collapses oscillation —
        # a pure 2-cycle {x,y} damps to its midpoint in one step. Convergence sweeps
        # are damped; a final UNDAMPED freeze sweep restores exact best_move /
        # value_worst consistency once neighbours are settled.
        # memo_pot / cover_depth / mem_nodes stay OUT of the gate: memo_pot grows
        # additively along a chosen chain that stays in-cycle (repetition legit best),
        # so it diverges there by construction — tracked, never gating.
        # EPS is a PRACTICAL convergence threshold, not machine-zero: values live in
        # [0,1] and repertoire decisions turn on differences of ~1e-2, so a residual
        # below 1e-7 is settled for every purpose. It is only sound to stop early
        # because DAMPING guarantees a monotone (non-oscillating) approach — a residual
        # in this band is the slow-contraction tail of a high-repetition cycle, never
        # a see-saw parked mid-swing. SWEEP_CAP is sized so even the slowest observed
        # cycle (effective discount ~0.98 under damping) reaches EPS.
        EPS, SWEEP_CAP, ALPHA = 1e-7, 1500, 0.5
        gate_dicts = (values, values_robust, value_worst, crush_pot)

        def _sweep(members: list[int], alpha: float = 1.0) -> float:
            resid = 0.0
            for ph in members:
                before = [d.get(ph) for d in gate_dicts]
                value_node(ph)
                for old, d in zip(before, gate_dicts):
                    new = d.get(ph)
                    if old is None or new is None:
                        if old is not new:
                            resid = float("inf")
                    else:
                        if alpha < 1.0:
                            new = old + alpha * (new - old)
                            d[ph] = new
                        resid = max(resid, abs(new - old))
            return resid

        total_sweeps = n_noncvg = 0
        worst_resid = 0.0
        for scc in sccs:
            if len(scc) == 1:
                value_node(scc[0])
                continue
            if len(scc) > 10_000:
                print(f"  WARNING: unexpectedly large SCC ({len(scc):,} nodes) "
                      f"near {position_epd.get(scc[0], '?')!r}", flush=True)
            members = sorted(scc)          # deterministic sweep order
            resid = float("inf")
            for _ in range(SWEEP_CAP):
                resid = _sweep(members, ALPHA)
                total_sweeps += 1
                if resid < EPS:
                    break
            if resid >= EPS:
                n_noncvg += 1
                print(f"  WARNING: SCC of {len(members)} did not converge "
                      f"(residual {resid:.2e}) near "
                      f"{position_epd.get(members[0], '?')!r}", flush=True)
            worst_resid = max(worst_resid, min(resid, 1.0))
            # Undamped freeze pass: re-derive every member's best_move and derived
            # outputs from the (now settled) neighbour values in one consistent sweep.
            _sweep(members)
            total_sweeps += 1
        print(f"  cycles: fixpoint done — {total_sweeps:,} sweeps, "
              f"max residual {worst_resid:.2e}, "
              f"{n_noncvg} non-converged SCC(s)", flush=True)

    if gate_rel_floor < 1.0:
        print(f"  relative gate (floor {gate_rel_floor}) pruned moves at "
              f"{len(rel_gated_nodes):,} nodes", flush=True)
    if _learn:
        print(f"  learnability tiebreak (δ {learn_delta_main}/{learn_delta_rare}, "
              f"pivot {learn_reach_pivot}) overrode the pick at "
              f"{len(learn_override_nodes):,} nodes", flush=True)

    # Per-position coverage efficiency = covered opponent-decision depth per memorized
    # branch (the quantity the selection key rewards). Surfaced for output/inspection.
    cover_effs = {ph: cover_depth.get(ph, 0.0) / (1.0 + mem_nodes.get(ph, 0.0))
                  for ph in all_positions}
    return (values, best_moves, best_forcing, best_error,
            best_decis, best_crush, crush_pot, memo_pot, cover_effs, value_worst,
            values_robust, position_epd, position_side, slice_prior, best_aug)


def print_best_line(
    result_index,        # dict-like: .get(hash) -> row dict (see _LazyIndex in main)
    stats_index,         # dict-like: .get(hash, []) -> list of {move_san, total}
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
    parser.add_argument("--gate-metric", choices=["eval", "worst", "robust"], default="worst",
                        help="Which robustness measure the refutation gate uses. 'worst' "
                             "(DEFAULT): value_worst — our PREPARED book vs the opponent's "
                             "best defence at every node (conditioned on the moves we will "
                             "actually play). 'robust': value_robust — objective engine eval "
                             "assuming best play by BOTH sides (legacy; can pass lines that are "
                             "only sound if WE also play engine-perfect onward). value_worst is "
                             "<= value_robust, so the same --robustness-floor binds tighter — "
                             "re-tune the floor when switching.")
    parser.add_argument("--gate-rel-floor", type=float, default=0.1,
                        help="RELATIVE refutation gate: a candidate is dropped if its gate "
                             "value (engine eval of the reached position, for --gate-metric "
                             "eval) concedes more than this margin (expected-score units, "
                             "~0.1 ≈ 110cp near equality) vs the BEST candidate at the same "
                             "node. Stops booking moves that squander an advantage because "
                             "opponents usually misplay them (e.g. 8...Nd4 giving back "
                             "-2.1 → +0.1 in the Bxf7+ Italian trap line, where 8...Qd7 "
                             "keeps it). Baseline = best recorded candidate (or best engine "
                             "rescue when augmentation fires); moves without eval coverage "
                             "are exempt. DEFAULT ON at 0.1; >= 1.0 disables.")
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
                        help="ABSOLUTE-mode only. Crush = fraction of games through an edge where "
                             "OUR side wins decisively by this FULL-MOVE number, flat. Default 15. "
                             "Summed from the crush_hist histogram buckets 1..horizon.")
    parser.add_argument("--crush-mode", choices=["absolute", "relative-propagated"],
                        default="absolute",
                        help="absolute (default, back-compat): per-edge crush@--crush-horizon from "
                             "the absolute crush_hist; used as a flat additive bonus, NOT propagated. "
                             "relative-propagated: consume the RELATIVE crush_hist_rel (move_bucket = "
                             "moves from THIS position to the decisive end) and propagate a "
                             "crush_potential through the DAG (LineCrush/NodeCrush) so a move is "
                             "rewarded for steering into crushing positions at any depth.")
    parser.add_argument("--crush-gamma", type=float, default=0.8,
                        help="relative-propagated only. Per-FULL-MOVE discount (earlier crush = "
                             "better). Applied to relative buckets for the discounted edge crush and, "
                             "as gamma**0.5 per ply, across propagation hops. Default 0.8.")
    parser.add_argument("--crush-imm-window", type=int, default=2,
                        help="relative-propagated only. Immediate-window W (full moves): the per-edge "
                             "'we crush almost now' hazard = shrunk share of games decisive within W "
                             "moves of the edge. Default 2.")
    parser.add_argument("--crush-baseline", choices=["mean", "zero"], default="mean",
                        help="What the per-edge crush shrinks toward. 'mean' (default, back-compat): "
                             "the slice-mean crush — a thin edge defaults to AVERAGE crushiness, which "
                             "can out-point a well-sampled below-average move (the 9.f4-over-9.Qg6+ "
                             "artifact). 'zero': assume NO crush until proven by many games — thin "
                             "edges contribute ~0 and value/sample decide. With 'zero' keep --crush-prior "
                             "large or selection-biased thin lines slip through.")
    parser.add_argument("--memo-weight", type=float, default=0.0,
                        help="Penalty weight on propagated MEMORIZATION cost in selection. "
                             "Memo cost of a move = its shrunk deviation penalty (value lost "
                             "if you forget the book move and play the most-played natural "
                             "move) + the reach-weighted downstream memo already propagated "
                             "into the child. Acts as the Lagrange multiplier of a memo "
                             "budget: 0 = disabled (default, back-compat); higher = steer "
                             "toward forcing / forgiving low-maintenance lines. memo_cost is "
                             "always written to the output column regardless.")
    parser.add_argument("--memo-prior", type=float, default=200.0,
                        help="Pseudocount (games) for Bayesian shrinkage of the per-node "
                             "deviation penalty toward --memo-baseline. Thin nodes shrink "
                             "toward the baseline so sparse data can't look spuriously cheap "
                             "(few observed replies = falsely 'forcing'). Default 200.")
    parser.add_argument("--memo-baseline", type=float, default=0.02,
                        help="Baseline per-node deviation penalty (expected-score units) that "
                             "thin nodes shrink toward. Default 0.02.")
    parser.add_argument("--memo-leave-cost", type=float, default=0.0,
                        help="Memorization cost charged when OUR recommendation LEAVES BOOK "
                             "(require-eval truncation or a childless/out-of-book node). Without "
                             "it, leaving prepared theory reads memo 0 — 'cheapest' — which is "
                             "backwards. A positive value makes lines that dump you out of book "
                             "early expensive (felt only when --memo-weight>0). 0.0 = disabled "
                             "(default, back-compat). Try ~0.1 (≈ a full main line's memo).")
    parser.add_argument("--cover-weight", type=float, default=0.0,
                        help="Weight on COVERAGE EFFICIENCY in selection: covered opponent-"
                             "decision depth (reach-weighted) per memorized prepared branch in "
                             "the subtree a move enters. Steers toward forcing / consolidating "
                             "lines that keep much of the opponent's mass on prepared rails with "
                             "few lines to learn (the propagated, mass-weighted generalization of "
                             "forcingness). A bare leaf scores 0, so it can't be gamed by leaving "
                             "book early. 0.0 = disabled (default, back-compat); needs a sweep to "
                             "tune (lives on a different scale than value/crush).")
    parser.add_argument("--cover-min-games", type=int, default=0,
                        help="Min games for an opponent reply to count as a PREPARED branch in "
                             "the coverage metric (a 'line you must learn'). Replies below it are "
                             "treated as un-prepared: their mass is not covered and they add no "
                             "branch. With --min-move-games 0 (the sharp recipe) set this higher "
                             "(~25-50) so 1-game freak replies aren't counted as memorization "
                             "burden. Default 0.")
    parser.add_argument("--require-eval", action="store_true",
                        help="At our turn, consider ONLY candidate moves whose resulting "
                             "position is present in the eval DB; if none is, truncate our "
                             "recommendation at that node (no best_move). Pair with "
                             "--eval-weight 1.0 for a purely engine-eval-driven repertoire "
                             "that stops where the eval DB's coverage ends. Requires --eval-db.")
    parser.add_argument("--augment-engine", action=argparse.BooleanOptionalAction, default=True,
                        help="At FORCED-LOSING nodes (every recorded move fails the eval "
                             "gate), expand candidates with legal but possibly-unplayed moves "
                             "whose resulting position the FULL eval DB scores acceptably "
                             "(a 1-ply lookahead over the eval DB). Fixes the missing-"
                             "improvement blind spot where the sole recorded continuation is "
                             "objectively lost but a better move exists unplayed. DEFAULT ON; "
                             "pass --no-augment-engine to disable. Needs --eval-db (the full DB "
                             "is loaded as sorted arrays, ~4.8 GB); no-ops without it. "
                             "Recommended with --gate-metric eval.")
    parser.add_argument("--plan-prior", default=None,
                        help="LEARNABILITY plan-prior parquet (ctx, token, game_freq) from "
                             "plan_consistency_report.py --export-prefix: the reach-weighted "
                             "%% of games in which each idea-token (piece destination / pawn "
                             "break) is played, per opponent-first-move context. Enables the "
                             "learnability TIEBREAK: among candidates within a δ window of "
                             "the best selection key (all sound — gates already applied), "
                             "pick the most habitual idea instead of the raw argmax. "
                             "Requires --plan-reach. Off by default.")
    parser.add_argument("--plan-reach", default=None,
                        help="Companion reach parquet (position_hash, ctx, reach) from the "
                             "same --export-prefix run: per-node dominant context + fraction "
                             "of games reaching the node. Drives the δ scaling below.")
    parser.add_argument("--learn-delta-main", type=float, default=0.005,
                        help="δ window (selection-key units ≈ expected score) at COMMON "
                             "nodes (reach >= --learn-reach-pivot): main lines stay sharp. "
                             "Default 0.005.")
    parser.add_argument("--learn-delta-rare", type=float, default=0.04,
                        help="δ window at zero-reach (RARE / unreached) nodes: rare lines "
                             "collapse onto habitual ideas — precision demand proportional "
                             "to how often you face the line (the 'stop memorizing King's "
                             "Gambit nuance' knob). Linear interpolation in reach between "
                             "the two δs. 0 disables the tiebreak. Default 0.04.")
    parser.add_argument("--learn-reach-pivot", type=float, default=0.02,
                        help="Reach fraction at/above which a node counts as fully COMMON "
                             "(δ = --learn-delta-main). Default 0.02 (2%% of games).")
    parser.add_argument("--learn-ctx-pivot", type=float, default=0.05,
                        help="Context share (fraction of all games under the opponent's "
                             "first move) at/above which a context's OWN habits fully "
                             "define 'habitual'. Rarer contexts shrink linearly toward "
                             "the GLOBAL habits, so a 1%% sideline (1.b3) is steered to "
                             "your normal development instead of self-reinforcing its "
                             "pass-1 oddities. Default 0.05.")
    parser.add_argument("--learn-depth-horizon", type=int, default=6,
                        help="The loose δ applies only within the first N of OUR moves "
                             "(min_our_depth from the reach parquet). Deeper nodes — and "
                             "nodes absent from the pass-1 walk — always use "
                             "--learn-delta-main: the offbeat-openings problem is shallow, "
                             "while deep nodes carry most of the tree's mass and "
                             "compounding concessions there is what costs effectiveness. "
                             "Default 6.")
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
    if args.require_eval:
        print(f"Require eval:      ON (candidates restricted to eval-covered positions)")
    print(f"Augment engine:    {'ON (engine-move rescue at forced-losing nodes; full eval DB)' if args.augment_engine else 'OFF (--no-augment-engine)'}")
    if args.memo_weight > 0:
        print(f"Memo weight:       {args.memo_weight}  (prior={args.memo_prior}, "
              f"baseline={args.memo_baseline}, leave-cost={args.memo_leave_cost})")
    if args.cover_weight > 0:
        print(f"Cover weight:      {args.cover_weight}  (min-games={args.cover_min_games}; "
              f"coverage depth per memorized branch)")
    print(f"Error weight:      {args.error_weight}")
    if args.error_weight > 0:
        print(f"Error prior:       {args.error_prior}")
    print(f"Decisiveness wt:   {args.decisiveness_weight}")
    print(f"Robustness floor:  {args.robustness_floor}"
          f"{'  (gate disabled)' if args.robustness_floor >= 1.0 else ''}")
    if args.robustness_floor < 1.0:
        print(f"Robust eval wt:    {args.robust_eval_weight}")
    print(f"Rel gate floor:    {args.gate_rel_floor}"
          f"{'  (rel gate disabled)' if args.gate_rel_floor >= 1.0 else ''}")
    print(f"Crush weight:      {args.crush_weight}"
          f"{'  (crush DB: ' + str(args.crush_db) + ')' if args.crush_weight > 0 else ''}")
    if args.crush_weight > 0:
        print(f"Crush mode:        {args.crush_mode}"
              + (f"  (γ={args.crush_gamma}, imm-window={args.crush_imm_window})"
                 if args.crush_mode == "relative-propagated" else f"  (horizon={args.crush_horizon})"))
        print(f"Crush prior:       {args.crush_prior}  (baseline={args.crush_baseline})")

    # ── Load learnability plan prior (ctx/token game frequencies + node reach) ──
    learn_prior = learn_ctx = learn_reach = learn_ctx_share = learn_depth = None
    if args.plan_prior or args.plan_reach:
        if not (args.plan_prior and args.plan_reach):
            sys.exit("FATAL: --plan-prior and --plan-reach must be given together "
                     "(both come from plan_consistency_report.py --export-prefix).")
        pp, pr = Path(args.plan_prior), Path(args.plan_reach)
        for p in (pp, pr):
            if not p.exists():
                sys.exit(f"FATAL: missing plan file {p}")
        prior_df = pl.read_parquet(pp)
        if "ctx_share" not in prior_df.columns:
            sys.exit(f"FATAL: {pp.name} lacks the ctx_share column — regenerate it with "
                     "the current plan_consistency_report.py --export-prefix.")
        learn_prior = {(c, t): f for c, t, f in
                       zip(prior_df["ctx"], prior_df["token"], prior_df["game_freq"])}
        learn_ctx_share = dict(zip(prior_df["ctx"], prior_df["ctx_share"]))
        reach_df = pl.read_parquet(pr)
        if "min_our_depth" not in reach_df.columns:
            sys.exit(f"FATAL: {pr.name} lacks the min_our_depth column — regenerate it "
                     "with the current plan_consistency_report.py --export-prefix.")
        learn_ctx = dict(zip(reach_df["position_hash"], reach_df["ctx"]))
        learn_reach = dict(zip(reach_df["position_hash"], reach_df["reach"]))
        learn_depth = dict(zip(reach_df["position_hash"], reach_df["min_our_depth"]))
        print(f"Plan prior:        {len(learn_prior):,} (ctx, idea) frequencies "
              f"({len(learn_ctx_share):,} contexts) from {pp.name}; "
              f"reach for {len(learn_reach):,} nodes from {pr.name}")
        print(f"Learn δ:           main {args.learn_delta_main} / rare {args.learn_delta_rare} "
              f"(reach pivot {args.learn_reach_pivot}, ctx pivot {args.learn_ctx_pivot}, "
              f"depth horizon {args.learn_depth_horizon})")

    # ── Load eval DB (position_hash -> expected white score) ──────────────
    eval_lookup: dict[int, float] = {}
    full_eval_hashes = full_eval_es = None   # full DB as sorted arrays (engine augmentation)
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
            # Engine augmentation needs the FULL (un-DAG-filtered) eval DB — its rescue
            # moves reach positions OUTSIDE the input DAG, so the prefiltered eval_lookup
            # can't see them. Snapshot the full mate-filtered DB as sorted numpy arrays
            # (~4.8 GB: int64 hash + float32 es) BEFORE the DAG semi-join below prunes it.
            if args.augment_engine:
                fh = edb_raw["position_hash"].to_numpy()
                fe = edb_raw["_es"].to_numpy().astype(np.float32)
                order = np.argsort(fh)
                full_eval_hashes = np.ascontiguousarray(fh[order])
                full_eval_es = np.ascontiguousarray(fe[order])
                print(f"Loaded {len(full_eval_hashes):,} evals as sorted arrays for engine "
                      f"augmentation (full DB, "
                      f"~{(full_eval_hashes.nbytes + full_eval_es.nbytes)/1e9:.1f} GB)")
            # Keep only evals for positions that can appear in THIS run's DAG
            # (parents ∪ children of the input edges). Dict-ifying the full ~388M-row
            # DB costs ~40+ GB of python objects and starved run_backwards_induction
            # into MemoryError once the input reached 23M edges (2019-2025 pool).
            _h = pl.scan_parquet(str(input_path))
            _need = pl.concat([
                _h.select(pl.col("parent_hash").alias("position_hash")),
                _h.select(pl.col("child_hash").alias("position_hash")).drop_nulls(),
            ]).unique().collect()
            n_prefilter = len(edb_raw)
            edb_raw = edb_raw.join(_need, on="position_hash", how="semi")
            eval_lookup = dict(zip(edb_raw["position_hash"].to_list(),
                                   edb_raw["_es"].to_list()))
            print(f"Loaded {len(eval_lookup):,} Stockfish evals from {edb_path} "
                  f"(dropped {n_drop:,} mate-class |cp|>={args.eval_mate_cp}; "
                  f"{n_prefilter - len(edb_raw):,} outside the input DAG)")
            if args.eval_weight <= 0:
                print("  (eval_weight=0 — evals will appear in output but not "
                      "influence move selection)")

    if args.augment_engine and full_eval_hashes is None:
        print("WARNING: --augment-engine set but no usable --eval-db — augmentation "
              "will no-op (needs the full eval DB).")

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

    # ── Crush overlay: LEFT JOIN the crush histogram so each edge carries crush
    # sums. Missing keys → nulls → no crush effect. Only when --crush-weight>0.
    if args.crush_weight > 0 and args.crush_db:
        cdb_path = Path(args.crush_db)
        if not cdb_path.exists():
            print(f"WARNING: crush DB not found at {cdb_path} — proceeding without crush.")
        elif "move_bucket" not in pl.scan_parquet(str(cdb_path)).collect_schema().names():
            # Legacy pre-baked crush_stats (no histogram): absolute mode only.
            crush_df = pl.read_parquet(str(cdb_path)).select(
                ["event", "elo_band", "parent_hash", "move_san",
                 "white_crush_sum", "black_crush_sum", "crush_games"])
            stats = stats.join(crush_df, on=["event", "elo_band", "parent_hash", "move_san"],
                               how="left")
        else:
            # The histogram can be ~1B rows (10+ GB parquet); an eager polars
            # read_parquet + group_by segfaults (0xC0000005) at that scale on Windows.
            # Reduce per-edge in DuckDB (streaming scan, bounded memory) and hand
            # polars only the ~one-row-per-edge result.
            import duckdb
            keys = ["event", "elo_band", "parent_hash", "move_san"]
            cdb_sql = str(cdb_path).replace("\\", "/")
            con = duckdb.connect()
            con.execute("SET preserve_insertion_order=false; SET memory_limit='40GB';")
            if args.crush_mode == "relative-propagated":
                # Relative histogram (move_bucket = full moves from THIS position to the
                # decisive end). Per edge precompute, for both colours:
                #   *_imm_sum  = decisive wins within W moves (the "crush almost now" hazard)
                #   *_dfull_sum= Σ γ^bucket · decisive wins over all buckets (discounted tail)
                # Stage 3 then shrinks these per edge and propagates crush_potential.
                W = args.crush_imm_window
                crush_df = pl.from_arrow(con.execute(f"""
                    SELECT event, elo_band, parent_hash, move_san,
                           CAST(SUM(CASE WHEN move_bucket BETWEEN 1 AND {int(W)}
                                    THEN white_wins ELSE 0 END) AS DOUBLE) AS white_imm_sum,
                           CAST(SUM(CASE WHEN move_bucket BETWEEN 1 AND {int(W)}
                                    THEN black_wins ELSE 0 END) AS DOUBLE) AS black_imm_sum,
                           SUM(CASE WHEN move_bucket >= 1 THEN white_wins
                               * POW({args.crush_gamma}, move_bucket) ELSE 0 END) AS white_dfull_sum,
                           SUM(CASE WHEN move_bucket >= 1 THEN black_wins
                               * POW({args.crush_gamma}, move_bucket) ELSE 0 END) AS black_dfull_sum,
                           CAST(SUM(n) AS BIGINT) AS crush_games
                    FROM read_parquet('{cdb_sql}')
                    GROUP BY event, elo_band, parent_hash, move_san
                """).arrow())
                print(f"Crush = RELATIVE-PROPAGATED (γ={args.crush_gamma}, imm-window="
                      f"{W} moves) from {cdb_path}")
            else:
                # Absolute crush@horizon: flat count of OUR decisive wins by move H.
                H = args.crush_horizon
                crush_df = pl.from_arrow(con.execute(f"""
                    SELECT event, elo_band, parent_hash, move_san,
                           CAST(SUM(CASE WHEN move_bucket BETWEEN 1 AND {int(H)}
                                    THEN white_wins ELSE 0 END) AS DOUBLE) AS white_crush_sum,
                           CAST(SUM(CASE WHEN move_bucket BETWEEN 1 AND {int(H)}
                                    THEN black_wins ELSE 0 END) AS DOUBLE) AS black_crush_sum,
                           CAST(SUM(n) AS BIGINT) AS crush_games
                    FROM read_parquet('{cdb_sql}')
                    GROUP BY event, elo_band, parent_hash, move_san
                """).arrow())
                print(f"Crush = absolute decisive-win rate by move {H} (flat) from {cdb_path}")
            con.close()
            stats = stats.join(crush_df, on=keys, how="left")
            n_cov = stats.filter(pl.col("crush_games").is_not_null()).height
            print(f"Joined crush data: {n_cov:,}/{len(stats):,} edges have crush data")
    # Ensure every crush column the edge dict reads exists (None when absent), so
    # to_dicts() yields them regardless of mode / whether crush is on.
    for col, dt in [("white_crush_sum", pl.Float64), ("black_crush_sum", pl.Float64),
                    ("white_imm_sum", pl.Float64), ("black_imm_sum", pl.Float64),
                    ("white_dfull_sum", pl.Float64), ("black_dfull_sum", pl.Float64),
                    ("crush_games", pl.Int64)]:
        if col not in stats.columns:
            stats = stats.with_columns(pl.lit(None, dtype=dt).alias(col))

    slices   = stats.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
    all_rows: list[dict] = []
    t_total  = time.time()

    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        mask   = (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        edges  = stats.filter(mask).to_dicts()

        t1 = time.time()
        (values, best_moves, best_forcing, best_err, best_decis, best_crush, crushpot,
         memopot, covereff, worstvals, vals_robust, pos_epd, pos_side, slice_prior,
         bestaug) = run_backwards_induction(
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
            gate_metric=args.gate_metric,
            gate_rel_floor=args.gate_rel_floor,
            robust_eval_weight=args.robust_eval_weight,
            crush_weight=args.crush_weight,
            crush_prior=args.crush_prior,
            crush_mode=args.crush_mode,
            crush_gamma=args.crush_gamma,
            crush_imm_window=args.crush_imm_window,
            crush_baseline=args.crush_baseline,
            force_root_move=args.force_root_move,
            require_eval=args.require_eval,
            memo_weight=args.memo_weight,
            memo_prior=args.memo_prior,
            memo_baseline=args.memo_baseline,
            memo_leave=args.memo_leave_cost,
            cover_weight=args.cover_weight,
            cover_min_games=args.cover_min_games,
            augment_engine=args.augment_engine,
            full_eval_hashes=full_eval_hashes,
            full_eval_es=full_eval_es,
            learn_prior=learn_prior,
            learn_ctx=learn_ctx,
            learn_reach=learn_reach,
            learn_ctx_share=learn_ctx_share,
            learn_depth=learn_depth,
            learn_delta_main=args.learn_delta_main,
            learn_delta_rare=args.learn_delta_rare,
            learn_reach_pivot=args.learn_reach_pivot,
            learn_ctx_pivot=args.learn_ctx_pivot,
            learn_depth_horizon=args.learn_depth_horizon,
        )
        elapsed = time.time() - t1

        n_our = sum(1 for m in best_moves.values() if m is not None)
        start_h = zobrist_int64(chess.Board())
        print(f"  {ev} / elo {eb:>6,}: {len(values):>5,} positions, "
              f"{n_our:>4,} our-turn, prior={slice_prior:.3f}, "
              f"memo(start)={memopot.get(start_h, float('nan')):.4f} in {elapsed:.2f}s")
        if args.augment_engine:
            n_aug = sum(1 for v in bestaug.values() if v)
            sample = next((pos_epd[ph] for ph, v in bestaug.items() if v), None)
            print(f"    engine-augmented recommendations: {n_aug:,}"
                  + (f"  (e.g. {sample})" if sample else ""))

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
                "crush_potential": crushpot.get(ph),
                "memo_cost":     memopot.get(ph),
                "cover_eff":     covereff.get(ph),
                "value_worst":   worstvals.get(ph),
                "value_robust":  vals_robust.get(ph),
                "eval_score":    eval_lookup.get(ph),
                "augmented":     bestaug.get(ph, False),
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

    # print_best_line only performs ~2x max_depth point lookups, so resolve each
    # .get() with a targeted filter instead of materializing the whole slice as
    # python dicts — the eager indexes MemoryError'd at 13M rows (2019-25 pool).
    class _LazyIndex:
        """Duck-types the dict the walker expects: .get(hash) -> row dict
        (row_mode) or list of {move_san, total} (list mode)."""
        def __init__(self, df: pl.DataFrame, key_col: str, row_mode: bool):
            self.df, self.key_col, self.row_mode = df, key_col, row_mode
            self.mask = (pl.col("event") == ev0) & (pl.col("elo_band") == eb0)

        def get(self, ph, default=None):
            sub = self.df.filter(self.mask & (pl.col(self.key_col) == ph))
            if sub.height == 0:
                return default
            if self.row_mode:
                return sub.row(0, named=True)
            return [{"move_san": r["move_san"], "total": r["total"]}
                    for r in sub.iter_rows(named=True)]

    result_idx = _LazyIndex(result, "position_hash", row_mode=True)
    stats_idx = _LazyIndex(stats, "parent_hash", row_mode=False)

    line = print_best_line(result_idx, stats_idx, start_hash, args.perspective)
    print(f"\nSample line ({ev0}, elo {eb0:,}, {args.perspective}):")
    print(f"  {line}")


if __name__ == "__main__":
    main()
