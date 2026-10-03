"""
Stage 3: backwards induction on the position DAG.

Takes the aggregated (event, elo_band, position, move) statistics from Stage 2
and computes the best opening repertoire for the specified perspective: the best
expected score the data supports, without lines that collapse if the opponent
finds the refutation.

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

    score = sign * value + decisiveness_weight * (1 - draw_rate)
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

  decisiveness -- 1 - draws/total of the edge (legacy knob; ~flat in blitz).
  opponent_error -- expected score the opponent leaves on the table vs their
                  Stockfish-best reply, frequency-weighted. Requires --eval-db.
  forcingness  -- Simpson concentration of the opponent's replies (legacy).
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
letting the rest of the tree choose the continuations.

Crush (a bonus for reaching a winning position early, from the crush/winpos
histograms) was removed on 2026-10-02, along with its flags and output columns.

NOTE on opponent modelling: at opponent-turn positions the MEAN value uses their
EMPIRICAL move distribution, not optimal play, so white- and black-perspective
values do NOT sum to 1 -- each is "expected score IF I play optimally and my
opponent plays like a typical player at this elo". The right framing for a
human-vs-human repertoire; the robustness gate is what guards the worst case.

Output columns per (event, elo_band, position): value, best_move (null at opp
turn), value_robust, value_worst, memo_cost, cover_eff, decisiveness,
opponent_error, forcingness, eval_score, augmented.

Usage:
    .venv/Scripts/python.exe python/stage3_backwards_induction.py --perspective black
    # The canonical pooled repertoires are built by python/build_sharp_reps.py.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import time
from collections import Counter, defaultdict, deque
from pathlib import Path

import chess
import chess.polyglot
import numpy as np
import polars as pl

from zobrist import zobrist_int64

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


# Env-gated phase-boundary memory ledger (STAGE3_MEM_PROFILE=1). Prints current
# process RSS at each pipeline phase so the columnar refactor can be checked
# against the pre-refactor dict/list-of-dict footprint. No-ops when unset.
_MEM_PROFILE = os.environ.get("STAGE3_MEM_PROFILE") == "1"


def _memlog(label: str) -> None:
    if not _MEM_PROFILE:
        return
    try:
        import psutil
        rss = psutil.Process().memory_info().rss / 1e9
        print(f"  [mem] {label}: RSS={rss:.1f} GB", flush=True)
    except Exception:
        pass


DEFAULT_INPUT  = Path("E:/chess/position-stats/position_stats.parquet")
DEFAULT_OUTPUT = Path("E:/chess/repertoire/repertoire.parquet")


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


# ── eval source loading ───────────────────────────────────────────────────────

def _es_expr(col: str = "eval_cp") -> pl.Expr:
    """cp -> expected White score (= cp_to_expected_score) as the ONE Polars expression every
    eval path uses: its float64 values are what eval_lookup holds."""
    return 1.0 / (1.0 + (-LICHESS_CP_SCALE * pl.col(col)).exp())


def _es_lut(cap: int) -> tuple[np.ndarray, np.ndarray]:
    """(float64, float32) expected scores for cp = -cap..cap, computed BY _es_expr on an Int32
    series, so indexing the table is bit-identical to applying the expression row by row (and the
    float32 table to the legacy path's .astype(np.float32))."""
    f64 = (pl.DataFrame({"eval_cp": pl.Series(range(-cap, cap + 1), dtype=pl.Int32)})
           .select(_es_expr().alias("_es"))["_es"].to_numpy())
    return f64, f64.astype(np.float32)


class _EsView:
    """full_eval_es over memory-mapped arrays: es[i] = lut32[cp[i]], the float32 the legacy path
    stored in RAM. augmented_candidates indexes it with a scalar; arrays work too."""

    def __init__(self, mm_cp, lut32: np.ndarray, cap: int):
        self._cp, self._lut, self._cap = mm_cp, lut32, cap

    def __len__(self) -> int:
        return len(self._cp)

    def __getitem__(self, i):
        if isinstance(i, (int, np.integer)):
            return self._lut[int(self._cp[i]) + self._cap]
        return self._lut[np.asarray(self._cp[i]).astype(np.int32) + self._cap]


# Engine augmentation's lookups (calls, keys, seconds): with memory-mapped arrays they can page in.
AUG_STATS = {"calls": 0, "keys": 0, "secs": 0.0}


def need_hashes(input_path) -> np.ndarray:
    """Every position the input DAG can ask an eval for: parents ∪ children of its edges."""
    h = pl.scan_parquet(str(input_path))
    need = pl.concat([h.select(pl.col("parent_hash").alias("position_hash")),
                      h.select(pl.col("child_hash").alias("position_hash")).drop_nulls()]).unique().collect()
    return need["position_hash"].to_numpy().astype(np.int64)


def load_evals(eval_db, input_path, *, eval_mate_cp: int, augment: bool, eval_weight: float
               ) -> tuple[dict, object, object]:
    """(eval_lookup, full_eval_hashes, full_eval_es) from --eval-db, which is either
      - a parquet (position_hash, eval_cp): the legacy path, unchanged; or
      - an eval-arrays directory (python/eval_arrays.py): verified against its source, then
        memory-mapped. eval_lookup holds the DAG's positions; engine augmentation searches the
        mapped arrays in place instead of a full in-RAM copy (~4.8 GB for the old DB; the new
        eval DB's arrays are ~59 GB and are never loaded).
    An eval DB DIRECTORY (explorer-extract evals, bkt*.parquet) is refused: build its arrays first.
    A missing path WARNs and returns no evals, as before."""
    eval_lookup: dict[int, float] = {}
    full_eval_hashes = full_eval_es = None
    edb_path = Path(eval_db)
    if not edb_path.exists():
        print(f"WARNING: eval DB not found at {edb_path} — proceeding without evals.")
        return eval_lookup, None, None
    if (edb_path / "eval_hash.npy").is_file():
        from eval_arrays import DIR_KIND, MISSING, lookup_evals, open_eval_arrays, read_meta, verify_eval_arrays
        status = verify_eval_arrays(edb_path, adopt=False)
        mm_h, mm_c = open_eval_arrays(edb_path)
        meta = read_meta(edb_path) or {}
        # Arrays built from an eval DB directory are capped at +-2000 by their builder; a legacy
        # pair is measured (400M int16, cheap).
        cap = 2000 if meta.get("kind") == DIR_KIND else int(np.abs(np.asarray(mm_c, dtype=np.int32)).max())
        if cap >= eval_mate_cp:
            sys.exit(f"FATAL: eval arrays {edb_path} reach |cp| {cap} >= --eval-mate-cp {eval_mate_cp}; "
                     f"the arrays path cannot drop those rows from engine augmentation.")
        lut64, lut32 = _es_lut(cap)
        need = need_hashes(input_path)
        cps = lookup_evals(need, mm_h, mm_c)
        hit = cps != MISSING
        eval_lookup = dict(zip(need[hit].tolist(), lut64[cps[hit].astype(np.int32) + cap].tolist()))
        if augment:
            full_eval_hashes, full_eval_es = mm_h, _EsView(mm_c, lut32, cap)
            print(f"Engine augmentation searches the memory-mapped arrays in place "
                  f"({len(mm_h):,} entries)")
        print(f"Loaded {len(eval_lookup):,} Stockfish evals from {edb_path} (eval arrays: {status}; "
              f"{int((~hit).sum()):,} of {need.size:,} DAG positions have no eval)")
    elif edb_path.is_dir():
        sys.exit(f"FATAL: {edb_path} is an eval DB directory; build its arrays first:\n"
                 f"    .venv/Scripts/python.exe python/eval_arrays.py --eval-db {edb_path} --out-dir <arrays dir>\n"
                 f"then pass --eval-db <arrays dir>.")
    else:
        edb_raw = pl.read_parquet(str(edb_path))
        # Drop mate-class sentinels (|cp| >= eval_mate_cp, e.g. the +-10000 Lichess
        # mate codes). These are corrupt for quiet opening positions (e.g. 1.e4 reads
        # +10000) and would trivially pass the refutation gate / dominate the eval
        # blend. Dropping them reverts those positions to empirical (real deep mates
        # are ~winning empirically too). Full fix = rebuild eval_db with a correct
        # mate->cp mapping (separate task).
        n_raw = len(edb_raw)
        edb_raw = edb_raw.filter(pl.col("eval_cp").abs() < eval_mate_cp)
        n_drop = n_raw - len(edb_raw)
        # Vectorized sigmoid (= cp_to_expected_score) + zip, far faster than
        # iter_rows over ~300M entries.
        edb_raw = edb_raw.with_columns(_es_expr().alias("_es"))
        # Engine augmentation needs the FULL (un-DAG-filtered) eval DB — its rescue
        # moves reach positions OUTSIDE the input DAG, so the prefiltered eval_lookup
        # can't see them. Snapshot the full mate-filtered DB as sorted numpy arrays
        # (~4.8 GB: int64 hash + float32 es) BEFORE the DAG semi-join below prunes it.
        if augment:
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
              f"(dropped {n_drop:,} mate-class |cp|>={eval_mate_cp}; "
              f"{n_prefilter - len(edb_raw):,} outside the input DAG)")
    if eval_weight <= 0:
        print("  (eval_weight=0 — evals will appear in output but not "
              "influence move selection)")
    return eval_lookup, full_eval_hashes, full_eval_es


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


def recall_weight(reach: float, midpoint: float, power: float) -> float:
    """Probability we actually REMEMBER the booked move at a node we reach with
    per-game probability `reach`.  Hill curve:  r = (f/m)^p / (1 + (f/m)^p).

    At reach = midpoint:  r = 0.5.  Monotone increasing, r(0) = 0, r -> 1.

    midpoint <= 0 DISABLES the model and returns 1.0 (perfect recall), which is
    what makes the flag an exact no-op when unset: every blend below collapses
    to the prescriptive value it already had.

    The two branches are algebraically identical; each is the one that stays
    accurate on its side of the midpoint, since reach here spans many decades
    (the plan export prunes at 1e-6, three decades below the default midpoint).
    """
    if midpoint <= 0 or power <= 0:
        return 1.0                       # disabled: perfect recall
    if reach <= 0:
        return 0.0
    ratio = reach / midpoint
    if ratio <= 1:
        v = ratio ** power
        return v / (1.0 + v)
    return 1.0 / (1.0 + (1.0 / ratio) ** power)


def aux_opp_mix(term_tot: float, term_sum: float,
                oth_tot: float, oth_sum: float,
                oth_eval: float, oth_cov: float,
                hor_tot: float, hor_sum: float,
                hor_eval: float | None,
                eval_weight: float, eval_weight_min: float = 0.0,
                eval_weight_k: float = 0.0,
                horizon_mode: str = "empirical") -> tuple[float, float]:
    """WHITE-SCORE (numerator, denominator) one position's aux buckets add to an
    OPPONENT node's mean. Callers in other perspectives flip with `den - num`,
    which is exact because the mix is linear in the bucket sums.

    SHARED BY BOTH ENGINES (2026-09-04). Stage 3 and budget_core each grew their
    own copy and they had drifted: the budget builder used the OTHER bucket's raw
    empirical score while Stage 3 blended `other_eval_mean` at a coverage-weighted
    eval weight. That is a difference in the objective being optimised, not in
    budget allocation, so the four-arm comparison was partly measuring it. One
    implementation, imported by both.

    The three buckets are NOT interchangeable and are deliberately valued by
    different rules:
      term     the game ended here. A finished game's result is a fact, so it
               enters at its empirical score with no engine opinion at all.
      other    replies pruned under min_games. Their outcomes ARE observed but
               their continuations are not, so the engine gets a say -- weighted
               by n_eff = oth_tot * oth_cov, the eval-COVERED mass, because
               `other_eval_mean` only describes the edges the eval DB knows.
               Passing the whole bucket would claim the engine's opinion rests on
               1/cov times the evidence it has, and would be most wrong on
               exactly the thin lines this bucket exists to describe.
      horizon  the extract's ply cap cut the game off. The OUTCOME is observed
               and only the path is missing, so 'empirical' (the default) trusts
               it; 'eval' substitutes the node's own engine score.

    A NaN `oth_eval` means no eval-covered child, and must fall back to the
    empirical mean rather than read as 0.0 (dead lost).
    """
    num = den = 0.0
    if term_tot:
        num += term_sum          # a finished game's result is a fact
        den += term_tot
    if oth_tot:
        emp_o = oth_sum / oth_tot
        if oth_eval == oth_eval and eval_weight > 0:      # not NaN
            n_eff = oth_tot * oth_cov
            w_o = effective_eval_weight(eval_weight, eval_weight_min,
                                        eval_weight_k, n_eff)
            v_o = (1.0 - w_o) * emp_o + w_o * oth_eval
        else:
            v_o = emp_o
        num += v_o * oth_tot
        den += oth_tot
    if hor_tot:
        if horizon_mode == "eval" and hor_eval is not None:
            v_h = hor_eval
        else:
            v_h = hor_sum / hor_tot
        num += v_h * hor_tot
        den += hor_tot
    return num, den


def aux_our_blend(value: float, cont_mass: float, crack_mass: float,
                  win_score: float) -> float:
    """Fold opponent collapses BEFORE our move into an our-node's value.

    `1.e4 e5 {Black resigns}` ends at a White-to-move position, so it lands on
    one of OUR nodes. The node's value is otherwise purely prescriptive -- "what
    our book gets from here" -- which is right for the games that continued, but
    some fraction of arrivals never gave us a move to play at all. Those are ours.

    Unit-agnostic: pass `win_score` in the caller's convention (1.0 for a White
    book in white-score units, 0.0 for a Black one, always 1.0 for engines that
    work in our-perspective units).

    cont_mass IS THE WHOLE CONTINUING POPULATION, not just the surviving edges
    (corrected 2026-09-04, found by external review). Stage 3 used to pass only
    the sum of surviving outgoing edges, which omits games that continued through
    below-floor OTHER moves and through the extract's horizon. Our policy
    replaces those games' historical move choices, but it does not make them stop
    arriving, so leaving them out shrinks the denominator and over-weights the
    resignations -- worst precisely at thin nodes, where the surviving edges are
    the smallest share of arrivals. With 100 surviving continuations, 900 OTHER
    continuations and 100 collapses against a prepared value of 0.6, the old
    denominator gave 0.8 where the arrival-weighted answer is
    (1000*0.6 + 100)/1100 = 0.63636.

    Non-crack terminations (our-node losses and draws) are deliberately NOT in
    either term: a game we lost with us to move ABANDONED the book, and charging
    the recipe for a resignation it never recommended would be worse than
    dropping it. That is a modelling choice, and a stated one -- see the loader.
    """
    total = cont_mass + crack_mass
    if crack_mass <= 0.0 or total <= 0.0:
        return value
    return (value * cont_mass + win_score * crack_mass) / total


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


# ── Columnar backing store (memory refactor) ────────────────────────────────────
# The backwards induction is held over dense node ids (a sorted int64 hash array +
# a hash->id dict) with per-node values in numpy arrays and the edge list in CSR
# form, instead of the old dict-of-list-of-dicts `children` and ~13 dict[int,float]
# value maps (which peaked ~100 GB on the 2019-25 pool). These thin adapters expose
# exactly the dict semantics the induction body relies on, so that body is unchanged
# — only construction / topology / output touch the arrays directly.
class _NodeSpace:
    __slots__ = ("hash", "idx")

    def __init__(self, hash_arr: "np.ndarray", idx: dict[int, int]):
        self.hash = hash_arr     # int64[N], id -> position_hash (sorted ascending)
        self.idx = idx           # position_hash -> id (dense [0, N))


class _ArrMap:
    """dict[int, float] view over a float64 array indexed by node id. NaN sentinel
    marks an unset key (a position not yet valued / not internal)."""
    __slots__ = ("sp", "arr")

    def __init__(self, sp: _NodeSpace, arr: "np.ndarray"):
        self.sp = sp
        self.arr = arr

    def __getitem__(self, h):
        return float(self.arr[self.sp.idx[h]])

    def __setitem__(self, h, v):
        self.arr[self.sp.idx[h]] = v

    def get(self, h, default=None):
        i = self.sp.idx.get(h)
        if i is None:
            return default
        v = self.arr[i]
        return default if v != v else float(v)   # v != v  → NaN → unset

    def __contains__(self, h):
        i = self.sp.idx.get(h)
        if i is None:
            return False
        v = self.arr[i]
        return v == v

    def __iter__(self):
        arr, hs = self.arr, self.sp.hash
        for i in range(arr.shape[0]):
            if arr[i] == arr[i]:
                yield int(hs[i])

    def items(self):
        arr, hs = self.arr, self.sp.hash
        for i in range(arr.shape[0]):
            v = arr[i]
            if v == v:
                yield int(hs[i]), float(v)

    def values(self):
        arr = self.arr
        for i in range(arr.shape[0]):
            v = arr[i]
            if v == v:
                yield float(v)

    def __len__(self):
        return int(np.count_nonzero(~np.isnan(self.arr)))


class _ObjMap:
    """dict[int, object] view over a Python list indexed by node id (EPD strings,
    side-to-move, chosen SAN). A missing hash -> default; a stored None stays None."""
    __slots__ = ("sp", "lst")

    def __init__(self, sp: _NodeSpace, lst: list):
        self.sp = sp
        self.lst = lst

    def __getitem__(self, h):
        return self.lst[self.sp.idx[h]]

    def __setitem__(self, h, v):
        self.lst[self.sp.idx[h]] = v

    def get(self, h, default=None):
        i = self.sp.idx.get(h)
        return self.lst[i] if i is not None else default

    def values(self):
        return iter(self.lst)


class _BoolMap:
    """dict[int, bool] view over a bool array indexed by node id (best_aug)."""
    __slots__ = ("sp", "arr")

    def __init__(self, sp: _NodeSpace, arr: "np.ndarray"):
        self.sp = sp
        self.arr = arr

    def __setitem__(self, h, v):
        self.arr[self.sp.idx[h]] = v

    def get(self, h, default=False):
        i = self.sp.idx.get(h)
        return bool(self.arr[i]) if i is not None else default

    def values(self):
        return (bool(x) for x in self.arr)

    def items(self):
        hs = self.sp.hash
        return ((int(hs[i]), bool(self.arr[i])) for i in range(self.arr.shape[0]))


class _CSRChildren:
    """dict[int, list[dict]] view: children[parent_hash] -> the parent's edge dicts
    (same keys the old build produced), materialized on demand from CSR arrays. A
    leaf / unknown hash -> [] (matches the old defaultdict(list))."""
    __slots__ = ("sp", "off", "e_child_hash", "e_san", "e_score", "e_total",
                 "e_draws")

    def __init__(self, sp, off, e_child_hash, e_san, e_score, e_total, e_draws):
        self.sp = sp
        self.off = off
        self.e_child_hash = e_child_hash
        self.e_san = e_san
        self.e_score = e_score
        self.e_total = e_total
        self.e_draws = e_draws

    def _rows(self, i):
        lo, hi = int(self.off[i]), int(self.off[i + 1])
        san, ch = self.e_san, self.e_child_hash
        sc, tot, dr = self.e_score, self.e_total, self.e_draws
        out = []
        for j in range(lo, hi):
            out.append({
                "move_san":  san[j],
                "child_hash": int(ch[j]),
                "score_avg": float(sc[j]),
                "total":     int(tot[j]),
                "draws":     int(dr[j]),
            })
        return out

    def __getitem__(self, h):
        i = self.sp.idx.get(h)
        return self._rows(i) if i is not None else []

    def get(self, h, default=None):
        i = self.sp.idx.get(h)
        if i is not None:
            return self._rows(i)
        return [] if default is None else default


# Edge columns the induction needs, with their dtypes. Missing columns (tests may
# omit some) are added as null so the CSR build sees a uniform schema.
_EDGE_COLS = {
    "parent_hash": pl.Int64, "move_san": pl.Utf8, "child_hash": pl.Int64,
    "white_score_avg": pl.Float64, "total": pl.Int64, "draws": pl.Int64,
    "parent_epd": pl.Utf8,
}


def _as_edges_df(edges) -> "pl.DataFrame":
    """Accept the caller's polars slice OR a list[dict] (synthetic test callers) and
    return a frame with every edge column present (missing → null, draws null → 0)."""
    if isinstance(edges, pl.DataFrame):
        df = edges
    else:
        df = pl.from_dicts(list(edges)) if edges else pl.DataFrame()
    add = [pl.lit(None, dtype=dt).alias(c)
           for c, dt in _EDGE_COLS.items() if c not in df.columns]
    if add:
        df = df.with_columns(add)
    return df.with_columns(pl.col("draws").fill_null(0))


# Columns that carry None in the old per-row dicts (opponent-turn / no-move nodes,
# uncovered eval) and so must serialize as null, not NaN. All other float columns are
# always set for a valued node.
_NULLABLE_OUT = ["forcingness", "opponent_error", "decisiveness", "eval_score"]


def _slice_frame(ev, eb, values, best_moves, best_forcing, best_error, best_decis,
                 memo_pot, cover_eff, value_worst,
                 values_robust, position_epd, position_side, best_aug, eval_lookup):
    """Build one slice's output rows straight from the numpy-backed value arrays
    (memory refactor Step 5 — replaces the 13M-row list[dict] + pl.from_dicts). Emits
    the exact legacy schema: NaN sentinels in the nullable diagnostic columns become
    null, and unvalued nodes (value NaN) are dropped, matching `values.items()`."""
    hashes = values.sp.hash
    n = int(hashes.shape[0])
    side = np.where(np.asarray(position_side.lst, dtype=bool), "white", "black")
    if eval_lookup:
        get = eval_lookup.get
        eval_arr = np.fromiter((get(int(h), np.nan) for h in hashes.tolist()),
                               dtype=np.float64, count=n)
    else:
        eval_arr = np.full(n, np.nan, dtype=np.float64)
    df = pl.DataFrame({
        "event":           np.full(n, ev),
        "elo_band":        np.full(n, eb, dtype=np.int64),
        "position_hash":   hashes,
        "position_epd":    position_epd.lst,
        "side_to_move":    side,
        "value":           values.arr,
        "best_move":       best_moves.lst,
        "forcingness":     best_forcing.arr,
        "opponent_error":  best_error.arr,
        "decisiveness":    best_decis.arr,
        "memo_cost":       memo_pot.arr,
        "cover_eff":       cover_eff.arr,
        "value_worst":     value_worst.arr,
        "value_robust":    values_robust.arr,
        "eval_score":      eval_arr,
        "augmented":       best_aug.arr,
    })
    df = df.with_columns([pl.col(c).fill_nan(None) for c in _NULLABLE_OUT])
    return df.filter(pl.col("value").is_not_nan())


def run_backwards_induction(
    edges,                        # polars DataFrame slice, or list[dict] (tests)
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
    gate_anchor:      str = "slice-prior",
    gate_rel_floor:   float = 1.0,
    gate_rel_baseline: str = "candidates",
    gate_rel_own_margin: float = 0.02,
    robust_eval_weight: float = 1.0,
    force_root_move:  str | None = None,
    require_eval:     bool = False,
    memo_weight:      float = 0.0,
    memo_prior:       float = 200.0,
    memo_baseline:    float = 0.02,
    memo_leave:       float = 0.0,
    cover_weight:     float = 0.0,
    cover_min_games:  int = 0,
    cover_prior:      float = 0.0,
    cover_baseline:   float = 0.22,
    cover_leave_cost: float = 0.0,
    cover_mass_shrink: float = 0.0,
    self_error_weight: float = 0.0,
    reply_shrink:     float = 0.0,
    aux:              "pl.DataFrame | None" = None,
    aux_term_flags:   bool = True,
    aux_horizon:      str = "empirical",
    aux_parts:        str = "term,other,horizon",
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
    recall_midpoint:  float = 0.0,
    recall_power:     float = 2.0,
    recall_default_reach: float = 0.0,
) -> tuple[dict[int, float], dict[int, str | None], dict[int, float | None],
           dict[int, float | None], dict[int, float | None],
           dict[int, float], dict[int, float],
           dict[int, float], dict[int, float | None], dict[int, str],
           dict[int, chess.Color], float, dict[int, bool]]:
    """
    Value every position reachable in `edges` and pick our best move at each.

    **Leaf blending**: at leaf positions (those with no backwards-induction
    value from deeper nodes), the smoothed empirical score is blended with
    the Stockfish expected white score:

        leaf_value = (1 - ew) * empirical + ew * stockfish

    The blending weight `ew` is either a fixed scalar (eval_weight) or varies
    dynamically by leaf sample size when eval_weight_k > 0:

        ew = eval_weight_min + (eval_weight - eval_weight_min) * k / (k + n)

    with n = games on the leaf edge (the sample size behind the empirical
    estimate). This trusts Stockfish more at sparse leaves and the empirical
    data more at well-sampled ones, with a floor at eval_weight_min.

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
    df = _as_edges_df(edges)
    start_rows = df.filter(pl.col("parent_hash") == start_hash).to_dicts()
    slice_prior = compute_slice_prior(start_rows, start_hash)

    # ── Build the columnar graph (CSR) + id-indexed value arrays ───────────────
    # Internal nodes = positions with >=1 resolved-child edge (child_hash NOT NULL,
    # matching the old skip); leaves (children that are never a parent) get id -1 and
    # flow through the empirical/eval leaf path exactly as before. Edge order within a
    # parent is preserved (stable sort) so move-selection tie-breaks are unchanged.
    df2 = df.filter(pl.col("child_hash").is_not_null())
    parent_h = df2["parent_hash"].to_numpy()
    child_h  = df2["child_hash"].to_numpy().astype(np.int64)
    E = int(parent_h.shape[0])
    node_hash = np.unique(parent_h).astype(np.int64)      # sorted internal-node hashes
    N = int(node_hash.shape[0])
    idx = {int(h): i for i, h in enumerate(node_hash.tolist())}
    sp = _NodeSpace(node_hash, idx)

    if N and E:
        parent_id = np.searchsorted(node_hash, parent_h).astype(np.int64)
        cpos = np.clip(np.searchsorted(node_hash, child_h), 0, N - 1)
        child_id = np.where(node_hash[cpos] == child_h, cpos, -1).astype(np.int64)
        order = np.argsort(parent_id, kind="stable")
    else:
        parent_id = np.zeros(0, np.int64)
        child_id = np.zeros(0, np.int64)
        order = np.zeros(0, np.int64)

    e_child_hash = child_h[order]
    e_child_id   = child_id[order]
    _san         = df2["move_san"].to_list()
    e_san        = [_san[k] for k in order.tolist()]
    e_score      = df2["white_score_avg"].to_numpy().astype(np.float64)[order]
    e_total      = df2["total"].to_numpy().astype(np.int64)[order]
    e_draws      = df2["draws"].to_numpy().astype(np.int64)[order]

    p_sorted = parent_id[order]
    child_off = np.zeros(N + 1, np.int64)
    if N:
        np.cumsum(np.bincount(p_sorted, minlength=N), out=child_off[1:])
    children = _CSRChildren(sp, child_off, e_child_hash, e_san, e_score, e_total,
                            e_draws)

    # EPD + side per internal node (any edge of the node — all share the parent
    # position). Side read from the EPD field == chess.Board(epd).turn, no board build.
    epd_all = df2["parent_epd"].to_list()
    position_epd_lst: list = [None] * N
    position_side_lst: list = [None] * N
    for i in range(N):
        epd = epd_all[int(order[int(child_off[i])])]
        position_epd_lst[i] = epd
        position_side_lst[i] = chess.WHITE if epd.split(" ", 2)[1] == "w" else chess.BLACK
    position_epd  = _ObjMap(sp, position_epd_lst)
    position_side = _ObjMap(sp, position_side_lst)

    # Per-node value stores: numpy arrays (NaN = unset) behind dict-compatible views,
    # so the induction body below is unchanged.
    values        = _ArrMap(sp, np.full(N, np.nan))   # expected vs AVERAGE opponent
    values_robust = _ArrMap(sp, np.full(N, np.nan))   # value along opponent's BEST reply
    value_worst   = _ArrMap(sp, np.full(N, np.nan))   # OUR-book value vs opponent's BEST defence
    self_err_pot  = _ArrMap(sp, np.full(N, np.nan))   # propagated OUR-error cost (--self-error-weight)
    memo_pot      = _ArrMap(sp, np.full(N, np.nan))   # propagated memorization cost
    cover_depth   = _ArrMap(sp, np.full(N, np.nan))   # reach-weighted covered opp-decision depth
    mem_nodes     = _ArrMap(sp, np.full(N, np.nan))   # count of prepared opponent branches
    cover_games   = _ArrMap(sp, np.full(N, np.nan))   # recorded games at the node (--cover-prior)
    best_forcing  = _ArrMap(sp, np.full(N, np.nan))
    best_error    = _ArrMap(sp, np.full(N, np.nan))
    best_decis    = _ArrMap(sp, np.full(N, np.nan))
    best_aug      = _BoolMap(sp, np.zeros(N, dtype=bool))
    best_moves    = _ObjMap(sp, [None] * N)
    values_arr = values.arr    # direct handle for the topological drain / cycle phase

    # ── Topological sort (Kahn's) over node ids ────────────────────────────────
    # pending_count[i] = out-edges of node i whose child is itself internal. Each
    # (parent, child) pair is unique (distinct legal moves reach distinct zobrist
    # positions), so a count equals the old set of in-DAG child hashes. Reverse CSR
    # (par_off/par_idx) yields a valued child's parents for the drain.
    internal_edge = e_child_id >= 0
    pending_count = (np.bincount(p_sorted[internal_edge], minlength=N).astype(np.int64)
                     if N else np.zeros(0, np.int64))
    rc_child = e_child_id[internal_edge]
    rc_parent = p_sorted[internal_edge]
    rorder = np.argsort(rc_child, kind="stable")
    par_off = np.zeros(N + 1, np.int64)
    if N:
        np.cumsum(np.bincount(rc_child[rorder], minlength=N), out=par_off[1:])
    par_idx = rc_parent[rorder].astype(np.int64)

    # In-edge game mass per node: how many games actually REACHED this position.
    # The opponent-node average below divides by the sum of the node's SURVIVING
    # replies, so replies that fragmented below the pool's min_games are silently
    # renormalised onto the ones that lived. reached_mass supplies the honest
    # denominator, and (out_mass / reached_mass) is the fraction of the real reply
    # distribution the model can actually see. Only edges to internal nodes carry
    # in-mass; roots have none (guarded at the use site).
    reached_mass = (np.bincount(e_child_id[internal_edge],
                                weights=e_total[internal_edge].astype(np.float64),
                                minlength=N)
                    if N else np.zeros(0, np.float64))

    # ── aux sidecar: the mass a node's OUTGOING edges cannot see ───────────────
    # Three populations, each needing different treatment, aligned to node ids.
    # Absent -> zeros, so a node with no aux row behaves exactly as before and
    # aux=None is an exact no-op.
    #
    #   TERM     the game ENDED here. Its result is a FACT, so it enters the mean
    #            at its empirical score. Split by reason upstream so a time
    #            forfeit (a statement about the clock) stays separable from a
    #            mate/resignation (a statement about the position); both are
    #            counted by default because the edge scores they are averaged
    #            against already count time forfeits, and filtering one side of a
    #            mean but not the other is a worse defect than the one it fixes.
    #   OTHER    replies below the pool's per-edge floor. Empirical score blended
    #            with the bucket's aggregate engine eval by the SAME
    #            effective_eval_weight leaf edges use.
    #   HORIZON  the game was still running when the ply cap stopped the replay.
    #            Valued empirically by default: unlike a pruned reply, its outcome
    #            IS observed — only the path is missing — so it is evidence about
    #            this node in exactly the way a leaf edge's white_score_avg is.
    #            --aux-horizon eval takes the node's engine eval instead.
    # Allocated ONLY when the sidecar is supplied: seven float64[N] arrays is
    # ~875 MB on the 15.6M-position pool, and the default path must not pay it.
    # --aux-parts selects which populations are restored. Needed because they are
    # wildly different sizes — on 2018-01 the other-moves bucket carried 1,253,751
    # of edge mass against 4,224 for terminations — so "aux on" measures the bucket
    # unless the parts can be separated.
    # Recall model. OFF unless --recall-midpoint is positive; reach comes from
    # the SAME pass-1 export the learnability tiebreak already consumes, so this
    # adds no new plumbing and inherits that export's bounds (--max-our-moves,
    # --epsilon). Nodes absent from it are genuinely beyond those bounds, where
    # the curve is ~0 anyway, so recall_default_reach=0.0 is self-consistent.
    _recall = recall_midpoint > 0.0
    _recall_reach = learn_reach or {}

    _parts = {p.strip().lower() for p in (aux_parts or "").split(",") if p.strip()}
    _bad = _parts - {"term", "other", "horizon"}
    if _bad:
        raise ValueError(f"--aux-parts: unknown {sorted(_bad)}; "
                         f"choose from term, other, horizon")
    _aux = aux is not None and N > 0 and bool(_parts)
    aux_term_tot = aux_term_sum = aux_oth_tot = aux_oth_sum = None
    aux_hor_tot = aux_hor_sum = aux_oth_eval = aux_crack = aux_oth_cov = None
    if _aux:
        aux_crack = np.zeros(N)      # OUR-node collapses; see the our-turn note below
        aux_oth_cov = np.zeros(N)    # eval-covered FRACTION of the bucket's mass
        aux_term_tot = np.zeros(N); aux_term_sum = np.zeros(N)
        aux_oth_tot  = np.zeros(N); aux_oth_sum  = np.zeros(N)
        aux_hor_tot  = np.zeros(N); aux_hor_sum  = np.zeros(N)
        aux_oth_eval = np.full(N, np.nan)
        ah = aux["position_hash"].to_numpy().astype(np.int64)
        pos = np.clip(np.searchsorted(node_hash, ah), 0, max(N - 1, 0))
        keep = node_hash[pos] == ah          # aux rows for positions we do not
        rows = pos[keep]                     # value are simply irrelevant
        # One row per position is the sidecar's contract (merge_aux_stats groups
        # by parent_hash off a DISTINCT key set). Asserted because the scatter
        # below is `arr[rows] +=`, which SILENTLY KEEPS ONLY THE LAST write for a
        # repeated index rather than accumulating — a duplicate would quietly
        # under-count the very mass this table exists to restore.
        if rows.shape[0] != np.unique(rows).shape[0]:
            raise ValueError("--aux-stats has duplicate position_hash rows; "
                             "the sidecar must carry exactly one row per position")
        def _col(name):
            return aux[name].to_numpy().astype(np.float64)[keep] if name in aux.columns \
                else np.zeros(int(keep.sum()))
        groups = ["term_normal", "term_other"] + (["term_flag"] if aux_term_flags else [])
        if "term" in _parts:
            for g in groups:
                aux_term_tot[rows] += _col(f"{g}_total")
                aux_term_sum[rows] += _col(f"{g}_white_wins") + 0.5 * _col(f"{g}_draws")
        # Terminations at OUR node, where the SIDE TO MOVE WON — i.e. the opponent
        # resigned before we had played. `1.e4 e5 {Black resigns}` ends at a
        # White-to-move position, so it lands on one of OUR nodes and the whole
        # aux row there used to go unread. Splitting matters because the two halves
        # mean opposite things:
        #   we lost (measured 2,054 of 2,442 on 2018-01) -> the game ABANDONED the
        #     book; including it would charge the recipe for a resignation it never
        #     recommended.
        #   they cracked (168) -> a genuine collapse, and exactly the evidence the
        #     objective exists to reward.
        # Only the second is counted. Draws are deliberately left out: a draw with
        # us to move is usually one we agreed to, which is a deviation, and the
        # data cannot separate that from stalemate or repetition.
        if "term" in _parts:
            crack_col = "white_wins" if our_color == chess.WHITE else "black_wins"
            for g in groups:
                aux_crack[rows] += _col(f"{g}_{crack_col}")
        if "other" in _parts:
            aux_oth_tot[rows] = _col("other_total")
            aux_oth_sum[rows] = _col("other_white_wins") + 0.5 * _col("other_draws")
            aux_oth_cov[rows] = _col("other_eval_cov")
        if "horizon" in _parts:
            aux_hor_tot[rows] = _col("horizon_total")
            aux_hor_sum[rows] = _col("horizon_white_wins") + 0.5 * _col("horizon_draws")
        # A bucket with no eval-covered child has other_eval_mean NULL. Polars
        # renders that as NaN in to_numpy() for a Float64 column, which is already
        # the sentinel the read site tests for — but that is a property of the
        # dtype, not a guarantee, so the null mask is applied explicitly. Getting
        # it wrong would read "no engine opinion" as an expected score of 0.0,
        # i.e. dead lost, on exactly the thin lines the bucket exists to describe.
        if "other" in _parts and "other_eval_mean" in aux.columns:
            aux_oth_eval[rows] = _col("other_eval_mean")
            _null = aux["other_eval_mean"].is_null().to_numpy()[keep]
            aux_oth_eval[rows[_null]] = np.nan

    queue: deque[int] = deque(int(node_hash[i]) for i in np.nonzero(pending_count == 0)[0])
    _memlog("post graph-build (CSR + value arrays)")

    # ── Backwards induction (value stores are the numpy-backed views built above) ─
    opp_is_white = (our_color == chess.BLACK)
    # Self-error is OFF by default, and every site that touches it is gated on
    # this flag, so the default recipe computes and pays for nothing new — that
    # is what makes the no-op equivalence check exact.
    _self_err = self_error_weight > 0 and bool(eval_lookup)
    _reply_shrink = reply_shrink > 0 and bool(eval_lookup)
    # No eval_lookup needed: this correction is pure reply mass, not engine eval.
    _cover_mass = cover_mass_shrink > 0
    # Engine-candidate augmentation is only live when the caller supplied the full
    # (un-prefiltered) eval DB as sorted arrays — the prefiltered eval_lookup can't
    # see legal-but-unplayed children (they're outside the input DAG).
    _augment = bool(augment_engine) and full_eval_hashes is not None
    _aug_cache: dict[int, list[dict]] = {}
    rel_gated_nodes: set[int] = set()   # our-turn nodes where the relative gate pruned >=1 move
                                        # (a set, not a counter: cycle fixpoint sweeps revisit nodes)
    _rel_own_eval = gate_rel_baseline == "own-eval"
    rel_own_nodes: set[int] = set()     # nodes where the OWN-EVAL raise was part of the cut
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
        # expensive); same shrink pattern as forcingness().
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
                # Sample size of the empirical estimate = games on THIS edge
                # (what smoothed_score saw). The old sum over children.get(ch)
                # was always 0 here — this branch only fires for out-of-DAG
                # leaves, which by definition have no outgoing edges — so
                # dynamic mode silently degenerated to the fixed max weight.
                ew = effective_eval_weight(eval_weight, eval_weight_min,
                                           eval_weight_k, m["total"])
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
            # Opponent's preference for THIS reply on the white-expected-score
            # scale (eval where covered, else empirical edge score).
            pref = eval_lookup[ch] if (eval_lookup and ch in eval_lookup) else m["score_avg"]
            mvs.append({"san": m["move_san"], "val": child_val, "robust": child_robust,
                        "worst": value_worst.get(ch, child_val),
                        "total": tot, "frc": frc, "opp_err": oerr, "dec": dec,
                        "pref": pref, "covered": covered, "child": ch})
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

    # Anchor the ABSOLUTE gate sits on. "slice-prior" (default, legacy) uses the
    # slice's empirical white score — measured 0.5183 on the 2019-25 pool, which
    # makes the gate ASYMMETRIC BY COLOUR: White is held to >= -89.5 cp while
    # Black is allowed up to +131.0 cp, i.e. Black's repertoire may book positions
    # 41.5 cp worse than White's may. "even" anchors both at 0.5 (a symmetric
    # +-110.1 cp at floor 0.1). See _test_stage3_gate_anchor.py.
    gate_anchor_val = slice_prior if gate_anchor == "slice-prior" else 0.5

    def passes_gate(mv):
        # ABSOLUTE refutation gate: a candidate move is eligible only if its
        # robustness measure stays within robustness_floor of the anchor.
        rv = gate_rv(mv)
        if our_color == chess.WHITE:
            return rv >= gate_anchor_val - robustness_floor
        return rv <= gate_anchor_val + robustness_floor

    def apply_rel_gate(ph, cands_list, own_eval=True):
        """RELATIVE refutation gate: drop candidates conceding more than
        gate_rel_floor (expected-score units) vs the baseline gate value.
        Catches advantage-squandering moves the absolute gate can't — moves that
        stay above the absolute bar but give back a won position because opponents
        usually misplay them (e.g. 8...Nd4 in the Bxf7+ Italian: empirically great,
        but concedes −2.1 → +0.1 vs best play while 8...Qd7 keeps it all).

        Baseline (gate_rel_baseline):
          "candidates" — the best candidate in the list. It always survives, so a
              non-empty list never empties.
          "own-eval"  — additionally raised to the NODE'S OWN engine eval minus
              gate_rel_own_margin (with us to move, the node eval IS the engine's
              best-legal-move assessment — a 0-ply full-legal-move baseline). This
              CAN empty the list when every recorded move concedes vs an unplayed
              engine move; the emptied set then flows into the engine-augmentation
              rescue at the call site. The margin absorbs parent/child eval-depth
              inconsistency in the DB. Not applied to the rescue set itself
              (own_eval=False there): rescues realize the legal-move baseline
              directly, and gating them against an inconsistent parent eval could
              only push selection back to a worse recorded move.

        Moves without a genuine eval (gate_metric='eval', uncovered child) are
        EXEMPT — their rv is a propagated empirical value, not comparable."""
        if gate_rel_floor >= 1.0 or not cands_list:
            return cands_list
        base_rv = None
        if own_eval and _rel_own_eval and eval_lookup:
            e = eval_lookup.get(ph)
            if e is not None:
                base_rv = (e - gate_rel_own_margin if our_color == chess.WHITE
                           else e + gate_rel_own_margin)
        if base_rv is None and len(cands_list) < 2:
            return cands_list
        rvs = [(gate_rv(mv), gate_metric != "eval" or mv["covered"])
               for mv in cands_list]
        elig = [rv for rv, ok in rvs if ok]
        if not elig:
            return cands_list
        if our_color == chess.WHITE:
            best = max(elig)
            raised = base_rv is not None and base_rv > best
            best = max(best, base_rv) if base_rv is not None else best
            kept = [mv for mv, (rv, ok) in zip(cands_list, rvs)
                    if not ok or rv >= best - gate_rel_floor]
        else:
            best = min(elig)
            raised = base_rv is not None and base_rv < best
            best = min(best, base_rv) if base_rv is not None else best
            kept = [mv for mv, (rv, ok) in zip(cands_list, rvs)
                    if not ok or rv <= best + gate_rel_floor]
        if len(kept) < len(cands_list):
            rel_gated_nodes.add(ph)
            if raised:
                rel_own_nodes.add(ph)   # the own-eval raise was (part of) the cut
        return kept

    def cover_eff(ch):
        # Coverage efficiency of the subtree entered by playing into child `ch`: covered
        # opponent-decision DEPTH (reach-weighted) per memorized BRANCH. High = forcing /
        # consolidating (much of the opponent's mass kept on rails with few lines to learn);
        # low = fan-out (many branches, mass leaks to the rare tail). A bare leaf / out-of-book
        # child has depth 0 -> eff 0, so it can't be gamed by exiting book early.
        raw = cover_depth.get(ch, 0.0) / (1.0 + mem_nodes.get(ch, 0.0))
        if cover_prior <= 0.0:
            return raw
        # THIN-DATA SHRINKAGE (--cover-prior), the same Bayesian pattern forcingness
        # uses. The raw ratio is biased HIGH at sparse nodes for exactly the reason
        # raw Simpson is: alternatives that fragmented below the pool's per-edge
        # min_games never appear, so the survivors renormalise to 100% and the node
        # reads as a narrow, efficient rail. Measured: a node with one surviving
        # 55-game reply scores 0.500 while a well-sampled 90/10 node scores 0.333 --
        # inverted. Shrinking toward the reach-weighted mean with pseudocount k
        # restores the ordering, as the identical correction already does for
        # forcingness.
        n = cover_games.get(ch, 0.0)
        if n <= 0.0 or mem_nodes.get(ch, 0.0) <= 0.0:
            # Leaf / truncated / nothing prepared below: raw is 0 and must STAY 0,
            # or shrinking toward a positive baseline would hand a bonus to exiting
            # book early -- the one property this metric is built to deny.
            return raw
        return (cover_prior * cover_baseline + raw * n) / (cover_prior + n)

    def augmented_candidates(ph):
        """Engine-move rescue for a FORCED-LOSING node (used only when every recorded
        move fails the gate). Offers legal — possibly never-played — moves whose
        resulting position the FULL eval DB scores acceptably: a 1-ply lookahead over
        the eval DB. Fixes the missing-improvement blind spot where the sole recorded
        continuation is objectively lost but a better move exists unplayed (e.g. the
        6...c6 rescue in a deep Englund line). Returns only gate-passing moves as
        eval-only move-val dicts (no empirical stats). Memoized per node."""
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
            t_aug = time.perf_counter()
            keys = np.asarray(child_hashes, dtype=np.int64)
            idx = np.clip(np.searchsorted(full_eval_hashes, keys), 0,
                          len(full_eval_hashes) - 1)
            hit = full_eval_hashes[idx] == keys
            ess = [float(full_eval_es[i]) if ok else None for i, ok in zip(idx, hit)]
            AUG_STATS["calls"] += 1
            AUG_STATS["keys"] += len(keys)
            AUG_STATS["secs"] += time.perf_counter() - t_aug
            for san, ch, es, ok in zip(sans, child_hashes, ess, hit):
                if not ok:
                    continue
                # eval-only dict mirroring build_move_vals' shape. worst/robust=es so
                # passes_gate (which falls back to mv["worst"] when the child isn't in
                # the prefiltered eval_lookup) gates on the engine eval directly.
                cand = {"san": san, "val": es, "robust": es, "worst": es,
                        "total": 0, "frc": 0.0, "opp_err": 0.0, "dec": 0.0,
                        "pref": es, "covered": True, "child": ch, "aug": True}
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
                # relative gate applies among the rescues too (sibling baseline
                # only — see apply_rel_gate's own-eval note)
                gated = apply_rel_gate(ph, aug, own_eval=False)
        cands = gated or base
        # Memorization penalty (Lagrangian of a memo budget): forgetting cost of this
        # move (shrunk deviation vs the natural move) + the reach-weighted downstream
        # cost already propagated into the child. memo_weight=0 → term vanishes.
        n_p = sum(mv["total"] for mv in mvs) or 0
        nat_val = max(mvs, key=lambda mv: mv["total"])["val"]
        keyf = lambda mv: (sign * mv["val"]
                           + decisiveness_weight * mv["dec"]
                           + error_weight * mv["opp_err"] + forcing_weight * mv["frc"]
                           + cover_weight * cover_eff(mv["child"])
                           # Self-error is a property of the POSITION, so the local
                           # term at ph is identical for every candidate and cannot
                           # affect this argmax; what discriminates is the error cost
                           # the move leads INTO. Same shape as memo_pot above.
                           - self_error_weight * self_err_pot.get(mv["child"], 0.0)
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

    def local_self_error(ph):
        # OUR expected eval loss at this position: the SAME computation as
        # opponent_error, applied to our own move list with us as the side to
        # move. The repertoire has always rewarded positions where the opponent
        # errs; this is the missing symmetric term for where WE do.
        return opponent_error(children[ph], eval_lookup,
                              opp_is_white=(our_color == chess.WHITE), k_e=error_prior)

    def value_node(ph):
        mvs = build_move_vals(ph)
        if not mvs:
            values[ph] = values_robust[ph] = value_worst[ph] = slice_prior
            best_moves[ph] = best_forcing[ph] = best_error[ph] = None
            best_decis[ph] = None
            if _self_err:
                self_err_pot[ph] = 0.0
            # A childless node at OUR turn = we've left book (no prepared continuation)
            # → leaving-book penalty; at the opponent's turn it's a terminal (game over).
            memo_pot[ph] = memo_leave if position_side[ph] == our_color else 0.0
            # Coverage. The parent credits this reply with share*(1 + cover_depth[ph]),
            # where the 1 asserts "we covered the opponent's decision there". At OUR
            # turn a childless node means we left book — we have NO answer, so that
            # credit is not earned. Returning -cover_leave_cost withdraws it: at 1.0 an
            # unanswerable reply contributes exactly 0 covered depth. At the OPPONENT's
            # turn a childless node is just the game ending, not a preparation failure,
            # so the credit stands. Mirrors memo_leave above.
            cover_depth[ph] = (-cover_leave_cost if position_side[ph] == our_color else 0.0)
            mem_nodes[ph] = 0.0
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
                best_decis[ph] = None
                if _self_err:
                    self_err_pot[ph] = 0.0
                # We are out of book here — assign the leaving-book penalty.
                memo_pot[ph] = memo_leave
                # Past the eval-DB frontier: we are out of book, so withdraw the
                # parent's coverage credit for this reply (see the childless case).
                cover_depth[ph] = -cover_leave_cost
                mem_nodes[ph] = 0.0
                return
            values[ph]        = b["val"]
            # ── imperfect recall ────────────────────────────────────────────
            # We play the booked move only with probability r; otherwise we wing
            # it and play the population's move here. So the node is worth
            #     r * (prescriptive) + (1-r) * (what the population scores here)
            # and the second term is the SAME mixture an opponent node computes,
            # because "we play like the population" and "they play like the
            # population" are the same arithmetic over the same edge list.
            #
            # Like the collapse blend below, r depends on the POSITION and not on
            # the candidate, so this cannot change which move we pick AT this node
            # -- the fallback term is constant in the candidate and drops out of
            # the argmax. It changes the node's VALUE, hence the PARENT's choice.
            # That is the whole mechanism: a deep trap stops paying for itself at
            # the ancestor that would have to walk into it.
            #
            # Frozen-reach approximation: r comes from pass 1's policy, so pass 2
            # cannot discover that a DIFFERENT move would make a node frequent
            # enough to remember. Same approximation the learnability tiebreak
            # already makes off the same export; stated, not hidden.
            if _recall:
                r = recall_weight(_recall_reach.get(ph, recall_default_reach),
                                  recall_midpoint, recall_power)
                if r < 1.0:
                    fb_den = sum(mv["total"] for mv in mvs)
                    fb_num = sum(mv["val"] * mv["total"] for mv in mvs)
                    if _aux:
                        i_fb = idx[ph]
                        a_num, a_den = aux_opp_mix(
                            aux_term_tot[i_fb], aux_term_sum[i_fb],
                            aux_oth_tot[i_fb], aux_oth_sum[i_fb],
                            aux_oth_eval[i_fb], aux_oth_cov[i_fb],
                            aux_hor_tot[i_fb], aux_hor_sum[i_fb],
                            (eval_lookup.get(ph) if eval_lookup else None),
                            eval_weight, eval_weight_min, eval_weight_k,
                            aux_horizon)
                        fb_num += a_num; fb_den += a_den
                    fb = fb_num / fb_den if fb_den else slice_prior
                    values[ph] = r * values[ph] + (1.0 - r) * fb
            # Blend in the games where the opponent resigned BEFORE we moved. The
            # node's value is otherwise purely prescriptive — "what our book gets
            # from here" — and that is right for the games that continued, but
            # some fraction of arrivals never gave us a move to play at all. Those
            # are ours, at score 1.0 (0.0 in white-score units for a Black book).
            #
            # Like self-error, this is a property of the POSITION and identical for
            # every candidate here, so it cannot change which move we pick AT this
            # node — it changes the node's value, and therefore the PARENT's
            # choice. Same mechanism as terminations at an opponent node.
            #
            # value_worst is deliberately untouched: an opponent who resigns is not
            # playing best defence, so worst-case must not improve because of it.
            if _aux:
                i_ph = idx[ph]
                crack = aux_crack[i_ph]
                if crack:
                    # THE WHOLE CONTINUING POPULATION, not just the surviving
                    # edges. Games that continued through a below-floor OTHER
                    # move, or that the extract's horizon cut off, still arrived
                    # here and would still have got our book move. Omitting them
                    # shrank the denominator and over-weighted the collapses,
                    # worst at thin nodes. See aux_our_blend.
                    cont = (sum(mv["total"] for mv in mvs)
                            + float(aux_oth_tot[i_ph]) + float(aux_hor_tot[i_ph]))
                    win = 1.0 if our_color == chess.WHITE else 0.0
                    values[ph] = aux_our_blend(values[ph], cont, crack, win)
            values_robust[ph] = b["robust"]
            best_moves[ph]    = b["san"]
            best_aug[ph]      = b.get("aug", False)
            best_forcing[ph]  = b["frc"]
            best_error[ph]    = b["opp_err"]
            best_decis[ph]    = b["dec"]
            if _self_err:
                # Additive along the chosen chain, exactly like memo_pot.
                self_err_pot[ph] = local_self_error(ph) + self_err_pot.get(b["child"], 0.0)
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
            # A move into an unmaterialised leaf inherits the leaving-book default.
            cover_depth[ph] = cover_depth.get(b["child"], -cover_leave_cost)
            mem_nodes[ph]   = mem_nodes.get(b["child"], 0.0)
        else:
            # Opponent: mean over their empirical distribution (value);
            # critical-line follow for value_robust.
            total = sum(mv["total"] for mv in mvs)
            values[ph] = (sum(mv["val"] * mv["total"] for mv in mvs) / total
                          if total else slice_prior)
            # ── aux: put back the mass the outgoing edges never had ───────────
            # THE defect this rebuild exists to fix. The mean above divides by
            # the sum of OUTGOING edges, so games that ended here, games whose
            # reply fell below the pool floor, and games cut off by the ply cap
            # all contribute nothing — and the first of those is not noise:
            # measured, the side to move scores 0.0953 at a terminal node, so
            # excluding them deletes precisely the opponent's collapses.
            # The bucket-by-bucket rules live in aux_opp_mix, which budget_core
            # imports so both engines value this mass identically. They had
            # drifted apart (the budget builder read the OTHER bucket raw, with
            # no other_eval_mean blend), which is a difference in the OBJECTIVE
            # and not in budget allocation.
            aux_num = aux_den = 0.0
            if _aux:
                i_ph = idx[ph]
                aux_num, aux_den = aux_opp_mix(
                    aux_term_tot[i_ph], aux_term_sum[i_ph],
                    aux_oth_tot[i_ph], aux_oth_sum[i_ph],
                    aux_oth_eval[i_ph], aux_oth_cov[i_ph],
                    aux_hor_tot[i_ph], aux_hor_sum[i_ph],
                    (eval_lookup.get(ph) if eval_lookup else None),
                    eval_weight, eval_weight_min, eval_weight_k,
                    aux_horizon)
                if aux_den:
                    base = values[ph] * total if total else slice_prior * 0.0
                    denom = total + aux_den
                    values[ph] = ((base + aux_num) / denom if denom
                                  else slice_prior)
            # --reply-shrink: the mean above renormalises over the replies that
            # SURVIVED min_games, so a node whose alternatives fragmented below the
            # floor asserts its one survivor with probability 1. Measured case:
            # 1.e4 c5 2.Nf3 d6 3.c3 Nf6 4.Ng5 h6 5.Nf3 — 124 games reached it, only
            # ...Nxe4 (50) survived, so a 40% blunder was modelled at 100% and its
            # +294cp eval propagated three plies undiluted (value 0.7470 == the leaf
            # eval to 6 dp), beating 4.Be2 despite Ng5 being 60cp worse.
            #
            # Coverage c = surviving mass / mass that reached the node. Blend the
            # mean toward the node's OWN engine eval by the missing fraction: the
            # unobserved replies are assumed to lead to what the engine says about
            # the position, rather than to whatever the survivors happened to do.
            # strength 0 -> w == 1.0 exactly -> bit-identical to legacy.
            if _reply_shrink and total:
                es = eval_lookup.get(ph)
                if es is not None:
                    reached = float(reached_mass[idx[ph]])
                    if reached > 0.0:
                        # With the aux table the accounted mass is explicit, so c
                        # stops conflating "replies were pruned" with "the game
                        # ended" and "the extract stopped". That is most of what
                        # --reply-shrink was correcting; running both at full
                        # strength double-counts the same missing games, which is
                        # why the driver zeroes reply_shrink when aux is supplied.
                        c = (total + aux_den) / reached
                        if c > 1.0:      # transpositions: several in-edges, or a
                            c = 1.0      # child counted once per parent
                        w = 1.0 - reply_shrink * (1.0 - c)
                        values[ph] = w * values[ph] + (1.0 - w) * es
            values_robust[ph] = opp_robust(mvs)
            best_moves[ph] = best_forcing[ph] = best_error[ph] = None
            if _self_err:
                # Reach-weighted expectation over their replies (mirrors memo_pot).
                self_err_pot[ph] = (sum(self_err_pot.get(mv["child"], 0.0) * mv["total"]
                                        for mv in mvs) / total if total else 0.0)
            best_decis[ph] = None
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
            # -cover_leave_cost is the DEFAULT, not just the stored value: a reply whose
            # child is a bare leaf is never materialised as a node, so the explicit
            # assignment in the childless branch would never be reached for it. Same
            # convention as memo_pot.get(child, memo_leave).
            cd = (sum((mv["total"] / total)
                      * (1.0 + cover_depth.get(mv["child"], -cover_leave_cost))
                      for mv in prep) if total else 0.0)
            # PROPAGATING coverage correction (--cover-mass-shrink). The shares above
            # renormalise over RECORDED replies, so mass that vanished at the pool's
            # per-edge floor (or because games simply ended) is invisible and the node
            # reads as fully covered. reached_mass is the honest denominator — the same
            # quantity --reply-shrink uses. Applying it HERE rather than at the point of
            # use is the difference that matters: cover_depth is built bottom-up, so an
            # uncorrected deep node carries its inflation into every ancestor.
            if _cover_mass and total:
                reached = float(reached_mass[idx[ph]])
                if reached > 0.0:
                    c = total / reached
                    if c > 1.0:          # transpositions: a child counted once per parent
                        c = 1.0
                    cd *= 1.0 - cover_mass_shrink * (1.0 - c)
            # Unanswerable replies contribute negatively (see cover_leave_cost); a node
            # whose replies we mostly cannot answer floors at zero coverage rather than
            # going negative and inverting the ratio's sign further up.
            cover_depth[ph] = cd if cd > 0.0 else 0.0
            mem_nodes[ph] = (sum(1.0 + (mv["total"] / total) * mem_nodes.get(mv["child"], 0.0)
                                 for mv in prep) if total else 0.0)
            # Sample size behind those ratios, for --cover-prior shrinkage.
            cover_games[ph] = float(total)

    while queue:
        ph = queue.popleft()
        value_node(ph)
        cid = idx[ph]
        for pid in par_idx[par_off[cid]:par_off[cid + 1]]:
            pending_count[pid] -= 1
            if pending_count[pid] == 0 and values_arr[pid] != values_arr[pid]:
                queue.append(int(node_hash[pid]))

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
    leftover = {int(node_hash[i]) for i in np.nonzero(np.isnan(values_arr))[0]}
    if leftover:
        print(f"  cycles: {len(leftover):,}/{N:,} positions "
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
        # below 1e-7 is settled for every purpose. SWEEP_CAP is sized so even the
        # slowest observed contracting cycle (effective discount ~0.98 under damping)
        # reaches EPS. SCCs that plateau instead of contracting are parked limit
        # cycles — the discrete gate/argmax feedback has no fixpoint there and no
        # amount of sweeping (or heavier damping) resolves it; they are detected and
        # abandoned early, values left mid-band by the damping. If a parked flip-flop
        # ever involves a line that matters, the escalation path is policy iteration
        # with a sticky argmax (freeze move choices, solve the affine value system
        # exactly, improve with hysteresis) — deliberately not built for the ~80
        # affected SCCs out of 13M positions.
        EPS, SWEEP_CAP, ALPHA = 1e-7, 1500, 0.5
        # self_err_pot deliberately is NOT gated: like memo_pot it accumulates
        # ADDITIVELY along the chosen chain, so a converged best-move chain that
        # stays inside an SCC diverges there by construction — gating on it would
        # report false non-convergence forever.
        gate_dicts = (values, values_robust, value_worst)
        gate_names = ("value", "value_robust", "value_worst")

        def _sweep(members: list[int], alpha: float = 1.0):
            """One Gauss-Seidel sweep. Returns (residual, worst_node, worst_dict).
            The residual is the UNDAMPED step |f(V)-V|: measuring after the alpha
            blend would scale the reported disagreement by alpha — convergence by
            measurement suppression — and hide parked limit cycles."""
            resid, w_ph, w_dict = 0.0, None, ""
            for ph in members:
                before = [d.get(ph) for d in gate_dicts]
                value_node(ph)
                for old, d, name in zip(before, gate_dicts, gate_names):
                    new = d.get(ph)
                    if old is None or new is None:
                        if old is not new:
                            resid, w_ph, w_dict = float("inf"), ph, name
                    else:
                        if abs(new - old) > resid:
                            resid, w_ph, w_dict = abs(new - old), ph, name
                        if alpha < 1.0:
                            d[ph] = old + alpha * (new - old)
            return resid, w_ph, w_dict

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
            resid, w_ph, w_dict = float("inf"), None, ""
            parked = False
            ckpt = float("inf")            # residual at the last plateau checkpoint
            for i in range(1, SWEEP_CAP + 1):
                resid, w_ph, w_dict = _sweep(members, ALPHA)
                total_sweeps += 1
                if resid < EPS:
                    break
                if i % 100 == 0:
                    # Plateau detection: a contracting SCC at damped rate ~0.99/sweep
                    # improves ~2.7x per 100 sweeps; a parked limit cycle doesn't
                    # improve at all. Bail instead of burning the rest of the cap.
                    if resid > ckpt / 2.0:
                        parked = True
                        break
                    ckpt = resid
            if resid >= EPS:
                n_noncvg += 1
                print(f"  WARNING: SCC of {len(members)} "
                      f"{'parked in a limit cycle' if parked else 'still converging at the cap'} "
                      f"(residual {resid:.2e} in {w_dict} near "
                      f"{position_epd.get(w_ph, '?')!r})", flush=True)
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
        if _rel_own_eval:
            print(f"  own-eval baseline (margin {gate_rel_own_margin}) raised the "
                  f"cut at {len(rel_own_nodes):,} of them", flush=True)
    if _learn:
        print(f"  learnability tiebreak (δ {learn_delta_main}/{learn_delta_rare}, "
              f"pivot {learn_reach_pivot}) overrode the pick at "
              f"{len(learn_override_nodes):,} nodes", flush=True)
    # Per-position coverage efficiency = covered opponent-decision depth per memorized
    # branch (the quantity the selection key rewards). Surfaced for output/inspection.
    cover_effs = _ArrMap(sp, np.nan_to_num(cover_depth.arr, nan=0.0)
                         / (1.0 + np.nan_to_num(mem_nodes.arr, nan=0.0)))
    return (values, best_moves, best_forcing, best_error,
            best_decis, memo_pot, cover_effs, value_worst,
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
                        help="Eval source: an eval-arrays directory from python/eval_arrays.py "
                             "(verified, then memory-mapped; canonical: D:/chess/eval_arrays_full, "
                             "the arrays of D:/chess/eval_full), or a legacy (position_hash, "
                             "eval_cp) parquet such as the retired unified_eval_db. When provided, Stockfish evals are "
                             "used in move selection and written to the output's eval_score column.")
    parser.add_argument("--eval-mate-cp", type=int, default=3000,
                        help="Drop eval_db entries with |eval_cp| >= this at load. RETIRED "
                             "safety: the old eval DB stamped +-10000 mate sentinels on quiet "
                             "positions (1.e4 read +10000), which this dropped. Every eval DB "
                             "since caps decisive evals at +-2000, so nothing "
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
    parser.add_argument("--gate-anchor", choices=["slice-prior", "even"], default="slice-prior",
                        help="Reference point the ABSOLUTE gate is measured from. "
                             "'slice-prior' (DEFAULT, legacy): the slice's empirical white "
                             "score. Measured 0.5183 on the 2019-25 pool, which makes the "
                             "gate ASYMMETRIC BY COLOUR — at floor 0.1 White is held to "
                             ">= -89.5cp while Black is allowed up to +131.0cp, so Black may "
                             "book positions 41.5cp worse than White may. 'even': anchor both "
                             "colours at 0.5 (symmetric +-110.1cp at floor 0.1). Needs an A/B "
                             "before adopting — the asymmetry may be doing useful work for "
                             "Black, who really does start worse.")
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
    parser.add_argument("--gate-rel-baseline", choices=["candidates", "own-eval"],
                        default="candidates",
                        help="What the relative gate measures concession AGAINST. "
                             "'candidates' (default): the best candidate in the set — can "
                             "never empty it. 'own-eval': additionally raised to the node's "
                             "OWN engine eval minus --gate-rel-own-margin — with us to move "
                             "that eval already prices the best LEGAL move, so a sole "
                             "recorded move conceding vs an unplayed engine move gets gated "
                             "and the engine-augmentation rescue supplies the improvement "
                             "(fixes e.g. keeping a mediocre recorded move while Stockfish's "
                             "choice was simply never played). Needs --augment-engine to "
                             "realize the rescue.")
    parser.add_argument("--gate-rel-own-margin", type=float, default=0.02,
                        help="Slack subtracted from the node's own eval before it raises "
                             "the relative-gate baseline (own-eval mode only). Absorbs "
                             "parent/child eval-depth inconsistency in the eval DB. "
                             "Default 0.02.")
    parser.add_argument("--robust-eval-weight", type=float, default=1.0,
                        help="Eval weight used ONLY for the robust (critical-line) value "
                             "that drives the refutation gate. Decoupled from --eval-weight "
                             "so the mean objective can stay empirical (trap value) while the "
                             "gate uses objective eval. 1.0 = pure eval where covered (default), "
                             "empirical where not. Requires --eval-db for effect.")
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
                             "tune (lives on a different scale than value).")
    parser.add_argument("--self-error-weight", type=float, default=0.0,
                        help="Penalty on OUR propagated expected eval loss down the line a move "
                             "enters — the symmetric counterpart to --error-weight, which "
                             "rewards positions where the OPPONENT errs. Without it a +2.0 line "
                             "needing six only-moves outranks a +0.8 line that plays itself. "
                             "Propagates additively along the chosen chain (like memo_cost) and "
                             "is deliberately excluded from the SCC convergence gate for the "
                             "same reason. Requires --eval-db. 0.0 = disabled (default).")
    parser.add_argument("--aux-stats", default=None,
                        help="position_stats_aux_*.parquet sidecar from "
                             "build_pooled_stats.py. Adds the mass an opponent "
                             "node's OUTGOING edges cannot see: games that ENDED "
                             "there (measured, the side to move scores 0.0953, so "
                             "excluding them deletes precisely the opponent's "
                             "collapses), replies below the pool's per-edge floor "
                             "(the other-moves bucket, with its games-weighted "
                             "engine eval), and games cut off by the ply cap. "
                             "Also makes --reply-shrink's coverage c honest, which "
                             "is why supplying this zeroes reply-shrink unless you "
                             "override it. Omitted (DEFAULT) = exact no-op.")
    parser.add_argument("--aux-term-flags", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Count TIME FORFEIT terminations in the aux mean "
                             "(DEFAULT ON). A flag is a fact about the clock, not "
                             "the position, and carries the same sign as a "
                             "resignation — but the edge-level white_score_avg it "
                             "is averaged against already counts time forfeits, so "
                             "excluding them from one half of a mean and not the "
                             "other is a worse distortion. Measured 13.6% of "
                             "in-tree terminations. --no-aux-term-flags to drop them.")
    parser.add_argument("--aux-parts", default="term,other,horizon",
                        help="Which aux populations to restore, comma-separated "
                             "from term,other,horizon (default all). They differ by "
                             "orders of magnitude — on 2018-01 the other-moves "
                             "bucket carried 1,253,751 of edge mass against 4,224 "
                             "for terminations — so isolating a part is the only "
                             "way to attribute an effect to it.")
    parser.add_argument("--aux-horizon", choices=["empirical", "eval"],
                        default="empirical",
                        help="How to value games the ply cap cut off. 'empirical' "
                             "(DEFAULT) uses their actual results: unlike a pruned "
                             "reply, a horizon game's OUTCOME is observed — only "
                             "its path is missing — so it is evidence about the "
                             "node exactly as a leaf edge's score is. 'eval' "
                             "substitutes the node's engine eval (the "
                             "--reply-shrink analogy, kept for A/B).")
    parser.add_argument("--reply-shrink", type=float, default=0.0,
                        help="Shrink an opponent node's mean value toward that "
                             "position's OWN engine eval, in proportion to how much "
                             "of its real reply distribution the pool actually "
                             "contains. The mean normally divides by the SURVIVING "
                             "replies, so alternatives that fragmented below the "
                             "pool's min_games get renormalised onto the survivors — "
                             "a node where one 50-game reply lived and 74 games' "
                             "worth of others died asserts that reply at 100%%. "
                             "Coverage c = surviving mass / mass that reached the "
                             "node; weight w = 1 - strength*(1-c). 0.0 (DEFAULT) is "
                             "w==1.0, bit-identical to legacy. 1.0 blends by the full "
                             "missing fraction. Needs --eval-db; nodes with no eval "
                             "are left alone. NOTE c also dips where games simply "
                             "ENDED (decisive result, or the ply-30 extract cap), not "
                             "only where replies were pruned, so deep nodes shrink "
                             "somewhat more than the pruning alone justifies.")
    parser.add_argument("--cover-min-games", type=int, default=0,
                        help="Min games for an opponent reply to count as a PREPARED branch in "
                             "the coverage metric (a 'line you must learn'). Replies below it are "
                             "treated as un-prepared: their mass is not covered (lowering "
                             "cover_depth) and they add no branch. NOTE the pool already applies "
                             "a per-edge floor at merge — the canonical 2013-2025 pool used "
                             "min_games=50 and its minimum edge total is exactly 50, so ANY value "
                             "<= 50 is an exact no-op there. Measured on that pool, the flag "
                             "starts biting at 75 (excludes 33.7% of edges), 100 (50.4%), "
                             "150 (67.1%), 500 (90.2%). Default 0.")
    parser.add_argument("--cover-prior", type=float, default=0.0,
                        help="Pseudocount for Bayesian shrinkage of cover_eff toward "
                             "--cover-baseline, the same correction --forcing-prior applies to "
                             "Simpson concentration and for the same reason: the raw ratio is "
                             "biased HIGH at sparse nodes, because replies that fragmented below "
                             "the pool's per-edge floor never appear and the survivors "
                             "renormalise to 100%%, so a thin node reads as a narrow efficient "
                             "rail. Measured: one surviving 55-game reply scores 0.500 while a "
                             "well-sampled 90/10 node scores 0.333 — inverted. Leaves and nodes "
                             "with nothing prepared keep raw 0 (shrinking those toward a positive "
                             "baseline would pay for leaving book early). 0.0 = disabled "
                             "(DEFAULT, exact no-op); 200 matches --forcing-prior.")
    parser.add_argument("--cover-leave-cost", type=float, default=0.0,
                        help="Withdraw the coverage credit for an opponent reply we cannot "
                             "answer. cover_depth credits each reply share*(1 + depth below), "
                             "where the 1 asserts we covered that decision — but where OUR book "
                             "ends (childless at our turn, or past the eval-DB frontier) we have "
                             "no answer and never earned it. This subtracts the given amount, so "
                             "1.0 makes an unanswerable reply contribute exactly 0 covered "
                             "depth. WHY IT MATTERS: leaving book was free in BOTH terms, so a "
                             "subtree that simply stops sooner got a better depth-per-branch "
                             "ratio. Measured on 1.d4 d5 2.Bf4: ...h5 scored 0.1672 (depth 5.24 "
                             "/ 30.4 branches, ending mean ply 9.9) versus ...c5 at 0.1298 "
                             "(8.82 / 66.9, ply 17.1) — cover_eff tracked shallowness across all "
                             "six replies, and the 'a bare leaf scores 0' guard reaches only ONE "
                             "ply. NB charging BRANCHES instead does not work: every line "
                             "eventually truncates, so the charge lands on all candidates about "
                             "equally and never reorders them (verified at 1.0 and 3.0). Not "
                             "applied at the opponent's turn — a childless node there is the "
                             "game ending, not a preparation failure. Intended range 0-1. "
                             "0.0 = disabled (DEFAULT, exact no-op).")
    parser.add_argument("--cover-mass-shrink", type=float, default=0.0,
                        help="PROPAGATING coverage correction, strength 0-1. cover_depth's "
                             "shares renormalise over RECORDED replies, so mass that vanished "
                             "at the pool's per-edge floor (or where games simply ended) is "
                             "invisible and a node reads as fully covered. Scales cover_depth "
                             "by 1 - strength*(1-c) with c = recorded reply mass / mass that "
                             "REACHED the node (the same reached_mass --reply-shrink uses, "
                             "clamped at 1.0 for transpositions). Applied inside the recursion, "
                             "not at the point of use, so a thin deep node cannot carry its "
                             "inflation up into every ancestor — which is precisely what "
                             "--cover-prior fails to prevent. Needs no prior or pseudocount. "
                             "0.0 = disabled (DEFAULT, exact no-op).")
    parser.add_argument("--cover-baseline", type=float, default=0.22,
                        help="Baseline cover_eff that thin nodes shrink toward (--cover-prior "
                             "only). Default 0.22 = the reach-weighted mean measured over the "
                             "canonical White pass-1 book (median 0.418; the mean is lower "
                             "because out-of-book nodes score 0).")
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
    parser.add_argument("--recall-midpoint", type=float, default=0.0,
                        help="IMPERFECT RECALL. Per-game reach at which we remember a "
                             "booked move half the time. 0 (default) disables the model "
                             "entirely and is an exact no-op. Requires --plan-reach. "
                             "Typical: 0.001 (a position seen once per 1000 games is a "
                             "coin flip). UNCALIBRATED -- sweep it, do not trust one value.")
    parser.add_argument("--recall-power", type=float, default=2.0,
                        help="Steepness of the recall curve. Higher = sharper cutoff "
                             "around --recall-midpoint. Default 2.0.")
    parser.add_argument("--recall-default-reach", type=float, default=0.0,
                        help="Reach assumed for nodes ABSENT from --plan-reach (beyond "
                             "its --max-our-moves / --epsilon bounds). Default 0.0, i.e. "
                             "no recall -- consistent with that export pruning at 1e-6, "
                             "three decades below a 0.001 midpoint.")
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
                             "(e.g. 'e4' or 'd4'), letting the rest of the tree choose the "
                             "continuations. Only meaningful for --perspective white. "
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
    # Load the sidecar and resolve the reply-shrink interaction up front, so the
    # header prints what will ACTUALLY run rather than what was asked for.
    aux_df = None
    reply_shrink_eff = args.reply_shrink
    if args.aux_stats:
        aux_path = Path(args.aux_stats)
        if not aux_path.exists():
            sys.exit(f"--aux-stats not found: {aux_path}")
        aux_df = pl.read_parquet(aux_path)
        print(f"Aux stats:         {aux_path.name} ({aux_df.height:,} positions, "
              f"flags={'in' if args.aux_term_flags else 'out'}, "
              f"horizon={args.aux_horizon})")
        if reply_shrink_eff:
            # Both correct the same missing mass; the aux table measures it
            # exactly where reply-shrink only approximated it from coverage.
            # Stacking them shrinks twice. Explicit --reply-shrink still wins,
            # but say so rather than silently honouring it.
            print(f"  NOTE: --reply-shrink {reply_shrink_eff} kept alongside "
                  f"--aux-stats; these correct overlapping mass and stacking "
                  f"them double-shrinks. 0.0 is the intended pairing.")
    print(f"Reply shrink:      {reply_shrink_eff}")
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
        print(f"Cover shrinkage:   "
              + (f"prior={args.cover_prior} toward baseline={args.cover_baseline}"
                 if args.cover_prior > 0 else "OFF (raw ratio; biased high at thin nodes)")
              + f" | leave-cost={args.cover_leave_cost}"
              + (" (leaving book is FREE — shallow books score high)"
                 if args.cover_leave_cost <= 0 else "")
              + f" | mass-shrink={args.cover_mass_shrink}")
    print(f"Error weight:      {args.error_weight}")
    if args.error_weight > 0:
        print(f"Error prior:       {args.error_prior}")
    print(f"Decisiveness wt:   {args.decisiveness_weight}")
    print(f"Robustness floor:  {args.robustness_floor}"
          f"{'  (gate disabled)' if args.robustness_floor >= 1.0 else ''}")
    if args.robustness_floor < 1.0:
        print(f"Gate anchor:       {args.gate_anchor}"
              f"{'  (symmetric across colours)' if args.gate_anchor == 'even' else ''}")
    if args.self_error_weight > 0:
        print(f"Self-error weight: {args.self_error_weight}  (our propagated expected "
              f"eval loss down the line)")
    if args.reply_shrink > 0:
        print(f"Reply shrink:      {args.reply_shrink}  (opponent-node mean blended "
              f"toward the position's own eval by its missing reply mass)")
    if args.robustness_floor < 1.0:
        print(f"Robust eval wt:    {args.robust_eval_weight}")
    print(f"Rel gate floor:    {args.gate_rel_floor}"
          f"{'  (rel gate disabled)' if args.gate_rel_floor >= 1.0 else ''}")
    if args.gate_rel_floor < 1.0 and args.gate_rel_baseline == "own-eval":
        print(f"Rel gate baseline: own-eval (margin {args.gate_rel_own_margin})")

    # ── Load learnability plan prior (ctx/token game frequencies + node reach) ──
    learn_prior = learn_ctx = learn_reach = learn_ctx_share = learn_depth = None
    if args.recall_midpoint > 0 and not args.plan_reach:
        sys.exit("FATAL: --recall-midpoint needs --plan-reach: the recall model is a "
                 "function of per-node reach, which only the pass-1 plan export "
                 "carries. Run plan_consistency_report.py --export-prefix first.")
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
    full_eval_hashes = full_eval_es = None   # full DB, sorted (engine augmentation)
    if args.eval_db:
        eval_lookup, full_eval_hashes, full_eval_es = load_evals(
            args.eval_db, input_path, eval_mate_cp=args.eval_mate_cp,
            augment=args.augment_engine, eval_weight=args.eval_weight)

    if args.augment_engine and full_eval_hashes is None:
        print("WARNING: --augment-engine set but no usable --eval-db — augmentation "
              "will no-op (needs the full eval DB).")

    stats = pl.read_parquet(input_path)
    stats = stats.filter(pl.col("elo_band").is_not_null())
    _memlog("post stats-load")

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

    _memlog("pre-slice")
    slices   = stats.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
    frames: list = []
    t_total  = time.time()

    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        mask   = (pl.col("event") == ev) & (pl.col("elo_band") == eb)
        edges  = stats.filter(mask)
        _memlog(f"post edge-slice ({ev}/{eb}, {edges.height:,} edges)")

        t1 = time.time()
        (values, best_moves, best_forcing, best_err, best_decis,
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
            gate_anchor=args.gate_anchor,
            gate_rel_floor=args.gate_rel_floor,
            gate_rel_baseline=args.gate_rel_baseline,
            gate_rel_own_margin=args.gate_rel_own_margin,
            robust_eval_weight=args.robust_eval_weight,
            force_root_move=args.force_root_move,
            require_eval=args.require_eval,
            memo_weight=args.memo_weight,
            memo_prior=args.memo_prior,
            memo_baseline=args.memo_baseline,
            memo_leave=args.memo_leave_cost,
            cover_weight=args.cover_weight,
            cover_min_games=args.cover_min_games,
            cover_prior=args.cover_prior,
            cover_baseline=args.cover_baseline,
            cover_leave_cost=args.cover_leave_cost,
            cover_mass_shrink=args.cover_mass_shrink,
            self_error_weight=args.self_error_weight,
            reply_shrink=reply_shrink_eff,
            aux=aux_df,
            aux_term_flags=args.aux_term_flags,
            aux_horizon=args.aux_horizon,
            aux_parts=args.aux_parts,
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
            recall_midpoint=args.recall_midpoint,
            recall_power=args.recall_power,
            recall_default_reach=args.recall_default_reach,
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
            print(f"    augmentation lookups so far: {AUG_STATS['calls']:,} calls, "
                  f"{AUG_STATS['keys']:,} keys, {AUG_STATS['secs']:.1f}s")

        frames.append(_slice_frame(
            ev, eb, values, best_moves, best_forcing, best_err, best_decis,
            memopot, covereff, worstvals, vals_robust,
            pos_epd, pos_side, bestaug, eval_lookup))

    _memlog(f"pre-output-build ({len(frames)} slice frame(s))")
    result = pl.concat(frames).sort(["event", "elo_band", "position_hash"])
    _memlog("post-output-build")
    # Atomic publish. Every driver skip-gates on the output EXISTING, so a rep
    # half-written when the process dies (power cut, OOM kill) would be silently
    # accepted as complete on the next run and poison whatever consumes it.
    # Writing to a sibling temp and renaming means the final path only ever
    # appears fully written — os.replace is atomic on NTFS and POSIX alike.
    tmp_out = output_path.with_name(output_path.name + ".partial")
    result.write_parquet(str(tmp_out), compression="zstd")
    os.replace(tmp_out, output_path)

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
