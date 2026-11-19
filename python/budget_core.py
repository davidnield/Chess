"""Budget-constrained repertoire selection: the library behind build_budget_books.py.

THE PROBLEM
-----------
Stage 3's book is unbounded: every gated our-turn node gets a move. All its
learning costs (memo_cost, learnability deltas) are REACH-WEIGHTED —
they price expected recall, so a rare deep trap is nearly free. A hard budget of
moves-to-learn is a STORAGE cost: each learned move counts once regardless of
reach. The two diverge exactly on rare deep lines, which is why a 400-move book
should differ in POLICY (choosing early-payoff variations) from a 40,000-move
book (which can afford the trap), not merely truncate the same tree earlier.

THE METHOD
----------
Per-node VALUE-vs-BUDGET CURVES, built bottom-up over the reach-filtered
subgraph, in the SAME topological style as Stage 3:

  our node u:      V_b(u) = max( L_node(u),  max_m  V_{child_m}(b-1) )
  opponent node v: V_b(v) = max over allocations sum(b_i)=b of sum_i p_i V_{b_i}
                            + constant terms (below-floor replies, aux buckets)

A curve is stored as (base, atoms) where atoms are (cost, gain) increments
sorted by density (gain/cost) non-increasing — a concave step hull. The
our-node max CONVEXIFIES adjacent increasing-density increments into single
atoms: that merge IS chain compression — a trap prefix (tiny gains) fuses with
its payoff step into one cost-k atom whose density is the trap's true amortized
rate. The opponent-node allocation is then a greedy density merge.

THE GREEDY MERGE IS NOT EXACT ONCE ANYTHING HAS FUSED (corrected 2026-09-04).
The exchange argument this module used to cite holds for DIVISIBLE concave
resources, and for discrete concavity with UNIT marginal increments — not for
the indivisible cost-k packages convexification produces. Counterexample, two
opponent children at p=0.5 carrying one atom each:

    A: cost 2, gain 0.6  ->  weighted density 0.30/2 = 0.150
    B: cost 3, gain 0.8  ->  weighted density 0.40/3 = 0.133
    at budget 3:  greedy buys A, then cannot afford B  -> 0.30
                  the exact allocation buys B alone    -> 0.40

The earlier claim ("gap localized to at most one straddling atom, measured not
assumed") was therefore wrong twice: the gap is not confined to a straddle, and
it was never measured — `--exact-alloc` is named here but no parser exposes it.
`exact_opp_curve` does implement the exact max-plus convolution, reachable from
tests only, and it is exact ONLY over the child curves handed to it: it cannot
recover a policy an earlier hull already lost.

Consequence for the measured fusion gap (7.2% of random-DAG instances carrying a
fused atom are suboptimal, worst +0.114, against 0.0% of instances with none):
fusion is the TRIGGER, but two mechanisms fire together — the hull under-states
value AND the packages stop being divisible. Retaining the pre-fusion point set
addresses only the first, so it is not on its own a fix.

PERSPECTIVE CONVENTION
----------------------
Internally EVERYTHING is our-perspective expected score in [0, 1], higher =
better for the side whose book is being built. White-perspective inputs
(white_score_avg, evals) are flipped once at load for black; outputs are
flipped back. This removes every sign branch from the curve algebra.

KNOWN v1 APPROXIMATIONS (documented, measured — see the plan)
-------------------------------------------------------------
- Curves carry pure VALUE, and the extraction argmax selects on value too (a
  frozen crush bonus used to enter it; crush was removed 2026-10-02).
  Curve-vs-realized value is reported per book.
- Transposition budget cost is charged PER PATH (conservative overcount);
  realized distinct decisions are reported alongside.
- Cycle-leftover nodes get a flat L curve (counted, logged).

This module deliberately has no CLI; build_budget_books.py wraps it. Tests
(_test_budget_*.py) import it directly, following the repo convention of
importing production logic rather than copying it.
"""
from __future__ import annotations

import heapq
import math
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

from stage3_backwards_induction import (  # noqa: E402  (path insert above)
    aux_opp_mix,
    aux_our_blend,
    LICHESS_CP_SCALE,
    aux_term_reasons,
    check_aux_term_flags,
    cp_to_expected_score,
    effective_eval_weight,
    forcingness,
    smoothed_score,
)

# Stage-3 blessed-recipe constants mirrored here (see build_sharp_reps.common_flags
# and stage3's argparse defaults). Overridable per call; never change silently.
PRIOR_STRENGTH = 500.0
EVAL_WEIGHT = 0.5
EVAL_WEIGHT_MIN = 0.0
EVAL_WEIGHT_K = 0.0
ROBUSTNESS_FLOOR = 0.1
GATE_REL_FLOOR = 0.1
GATE_REL_OWN_MARGIN = 0.02
SCORE_ABSOLUTE_TOLERANCE = {"white": 0.110, "black": 0.075}
MAX_SCORE_LOSS = 0.095
SCORE_EPS = 1e-12


def engine_loss(best_cp, move_cp, score_units):
    """Both inputs are centipawns from the repertoire side's perspective."""
    if score_units:
        return cp_to_expected_score(best_cp) - cp_to_expected_score(move_cp)
    return best_cp - move_cp


def fails_engine_gate(best_cp, move_cp, limit, score_units, score_floor=None):
    return ((limit is not None and engine_loss(best_cp, move_cp, score_units) >
             limit + (SCORE_EPS if score_units else 0.0))
            or (score_floor is not None and cp_to_expected_score(move_cp) + SCORE_EPS < score_floor))


def score_floor_from_meta(meta, our_white):
    if meta.get("max_score_loss") is None:
        return None
    prior = meta["slice_prior"] if our_white else 1.0 - meta["slice_prior"]
    return prior - meta["robustness_floor"]


# ── curves ──────────────────────────────────────────────────────────────────
# A Curve is (base, costs[int64], gains[float64]) with atoms sorted by density
# gains/costs non-increasing and every gain > 0. V(b) = base + sum of gains of
# the maximal atom prefix whose cumulative cost <= b.

@dataclass
class Curve:
    base: float
    costs: np.ndarray  # int64, len K
    gains: np.ndarray  # float64, len K

    def __post_init__(self):
        self.cum_costs = np.cumsum(self.costs) if len(self.costs) else \
            np.zeros(0, dtype=np.int64)
        self.cum_gains = np.cumsum(self.gains) if len(self.gains) else \
            np.zeros(0, dtype=np.float64)

    def eval(self, b: int) -> float:
        """Value at budget b (largest hull vertex <= b; between vertices the
        step curve is flat, so this is a safe under-estimate off-vertex)."""
        if b <= 0 or not len(self.costs):
            return self.base
        k = int(np.searchsorted(self.cum_costs, b, side="right"))
        return self.base + (self.cum_gains[k - 1] if k else 0.0)

    @property
    def capacity(self) -> int:
        return int(self.cum_costs[-1]) if len(self.costs) else 0


def flat_curve(v: float) -> Curve:
    return Curve(v, np.zeros(0, dtype=np.int64), np.zeros(0, dtype=np.float64))


def curve_from_points(points: list[tuple[int, float]]) -> Curve:
    """Concave majorant of achievable (budget, value) points.

    Enforces monotonicity first (any booking feasible at b is feasible at b+1,
    so the best achievable value is non-decreasing), then convexifies: adjacent
    increments are merged while density increases — the chain-compression step.
    """
    if not points:
        raise ValueError("curve_from_points: no points")
    pts: dict[int, float] = {}
    for b, v in points:
        if b < 0:
            raise ValueError(f"negative budget point {b}")
        if b not in pts or v > pts[b]:
            pts[b] = v
    bs = sorted(pts)
    if bs[0] != 0:
        raise ValueError("curve must define V(0)")
    # monotone running max
    vals = []
    run = -np.inf
    for b in bs:
        run = max(run, pts[b])
        vals.append(run)
    base = vals[0]
    # increments -> concave hull via stack merge
    stack: list[list[float]] = []  # [cost, gain]
    prev_b, prev_v = bs[0], vals[0]
    for b, v in zip(bs[1:], vals[1:]):
        dc, dg = b - prev_b, v - prev_v
        prev_b, prev_v = b, v
        if dg <= 1e-15:
            # zero-gain stretch: absorb the cost into the NEXT atom by keeping
            # a zero-gain placeholder that a later merge will swallow, or drop
            # if nothing follows (trailing flat costs buy nothing).
            stack.append([dc, 0.0])
            continue
        stack.append([dc, dg])
        while len(stack) >= 2 and (
                stack[-1][1] * stack[-2][0] > stack[-2][1] * stack[-1][0]):
            c2, g2 = stack.pop()
            stack[-1][0] += c2
            stack[-1][1] += g2
    # drop trailing zero-gain atoms
    while stack and stack[-1][1] <= 1e-15:
        stack.pop()
    if not stack:
        return flat_curve(base)
    costs = np.array([c for c, _ in stack], dtype=np.int64)
    gains = np.array([g for _, g in stack], dtype=np.float64)
    return Curve(base, costs, gains)


def cap_curve(c: Curve, bmax: int, k_atoms: int) -> Curve:
    """Trim atoms beyond bmax cumulative cost; coarsen to <= k_atoms by merging
    the adjacent pair with the smallest density gap (tail-preserving-head)."""
    if len(c.costs) and c.cum_costs[-1] > bmax:
        keep = int(np.searchsorted(c.cum_costs, bmax, side="right"))
        c = Curve(c.base, c.costs[:keep].copy(), c.gains[:keep].copy())
    while len(c.costs) > k_atoms:
        dens = c.gains / c.costs
        gaps = dens[:-1] - dens[1:]
        i = int(np.argmin(gaps))
        costs = np.concatenate([c.costs[:i], [c.costs[i] + c.costs[i + 1]],
                                c.costs[i + 2:]])
        gains = np.concatenate([c.gains[:i], [c.gains[i] + c.gains[i + 1]],
                                c.gains[i + 2:]])
        c = Curve(c.base, costs, gains)
    return c


def merge_weighted(children: list[tuple[float, Curve]], const_base: float,
                   bmax: int, k_atoms: int) -> Curve:
    """Opponent-node sup-convolution: base = const + sum p_i*base_i; atoms =
    density-sorted union of child atoms with gains scaled by p_i. Exact at hull
    vertices for concave children (greedy exchange argument); each child's own
    atoms stay in relative order under a stable sort, preserving the prefix
    property the extraction relies on."""
    base = const_base + sum(p * ch.base for p, ch in children)
    costs_l, gains_l = [], []
    for p, ch in children:
        if len(ch.costs):
            costs_l.append(ch.costs)
            gains_l.append(ch.gains * p)
    if not costs_l:
        return flat_curve(base)
    costs = np.concatenate(costs_l)
    gains = np.concatenate(gains_l)
    dens = gains / costs
    order = np.argsort(-dens, kind="stable")
    return cap_curve(Curve(base, costs[order], gains[order]), bmax, k_atoms)


# ── leaf values (Stage-3 blends, imported not copied) ───────────────────────

def leaf_edge_value(ws_avg: float, total: int, eval_ws: float | None,
                    slice_prior: float, our_white: bool,
                    prior_strength: float = PRIOR_STRENGTH,
                    eval_weight: float = EVAL_WEIGHT,
                    eval_weight_min: float = EVAL_WEIGHT_MIN,
                    eval_weight_k: float = EVAL_WEIGHT_K) -> float:
    """Stage 3's out-of-book child blend (stage3 L1085-1101), our-perspective.

    ws_avg / eval_ws arrive WHITE-perspective (as stored); flipped here."""
    emp = smoothed_score(ws_avg, total, slice_prior, prior_strength)
    if eval_ws is not None and eval_weight > 0:
        ew = effective_eval_weight(eval_weight, eval_weight_min,
                                   eval_weight_k, total)
        v = (1.0 - ew) * emp + ew * eval_ws
    else:
        v = emp
    return v if our_white else 1.0 - v


def leaf_node_value(edges_ws_total: list[tuple[float, int]],
                    own_eval_ws: float | None, slice_prior: float,
                    our_white: bool,
                    prior_strength: float = PRIOR_STRENGTH,
                    eval_weight: float = EVAL_WEIGHT,
                    eval_weight_min: float = EVAL_WEIGHT_MIN,
                    eval_weight_k: float = EVAL_WEIGHT_K) -> float:
    """L_node(u): value of STOPPING at our-turn node u — unprepared play.

    Model: with no book move we play like the population, so the empirical
    anchor is the games-weighted score over u's outgoing edges, smoothed toward
    the slice prior and blended with u's own eval exactly as a leaf edge would
    be. One-step by design (v1); the tier-3 sensitivity check swaps in
    own-eval-only."""
    tot = sum(t for _, t in edges_ws_total)
    ws = (sum(w * t for w, t in edges_ws_total) / tot) if tot else slice_prior
    return leaf_edge_value(ws, tot, own_eval_ws, slice_prior, our_white,
                           prior_strength, eval_weight, eval_weight_min,
                           eval_weight_k)


# Key load_aux_rows stamps on every row it returns: the sidecar's term_reasons
# ('split' or 'pooled'), so the guard travels with the data into aux_row_buckets.
TERM_REASONS_KEY = "_term_reasons"


def load_aux_rows(aux_path, positions=None, term_flags: bool = True
                  ) -> dict[int, dict]:
    """The aux sidecar as position_hash -> row dict (optionally only `positions`),
    each row stamped with the sidecar's term_reasons. Raises up front when
    term_flags=False meets a pooled-reason sidecar (check_aux_term_flags)."""
    import polars as pl
    check_aux_term_flags(aux_path, term_flags)
    reasons = aux_term_reasons(aux_path)
    lf = pl.scan_parquet(aux_path)
    if positions is not None:
        lf = lf.filter(pl.col("position_hash").is_in(list(positions)))
    return {r["position_hash"]: {**r, TERM_REASONS_KEY: reasons}
            for r in lf.collect().iter_rows(named=True)}


def aux_row_buckets(aux: dict | None, our_white: bool,
                    term_flags: bool = True
                    ) -> tuple[float, float, float, float, float,
                               float, float, float, float]:
    """Adapt ONE aux sidecar row into the aggregates the shared valuation takes.

    Stage 3 scatters these into parallel numpy arrays at load; the budget builder
    keeps the raw parquet row. Only the SHAPE differs, so only the shape is
    adapted here -- the rules themselves are aux_opp_mix / aux_our_blend, both
    imported from stage3, so the two engines cannot drift apart again.

    Returns, in WHITE-SCORE units:
        (term_tot, term_sum, crack, oth_tot, oth_sum, oth_eval, oth_cov,
         hor_tot, hor_sum)
    `crack` is a MASS, not a score: the games at an our-turn node where OUR side
    won, i.e. the opponent resigned or was mated before we got to move. Draws are
    excluded deliberately -- a draw with us to move is usually one we agreed to,
    which is a deviation, and the data cannot separate that from stalemate or
    repetition (stage3's loader carries the same note).

    A missing `other_eval_mean` becomes NaN, the sentinel aux_opp_mix tests for;
    reading it as 0.0 would assert "dead lost" on exactly the thin lines the
    bucket exists to describe.
    """
    if not aux:
        return (0.0, 0.0, 0.0, 0.0, 0.0, float("nan"), 0.0, 0.0, 0.0)
    if not term_flags and aux.get(TERM_REASONS_KEY) == "pooled":
        raise ValueError("aux row comes from a sidecar with term_reasons='pooled': "
                         "term_flag_* is 0 and the flags sit in term_other_*, so "
                         "term_flags=False would drop nothing")

    def g(name: str) -> float:
        return float(aux.get(name) or 0.0)

    groups = ["term_normal", "term_other"] + (["term_flag"] if term_flags else [])
    term_tot = sum(g(f"{x}_total") for x in groups)
    term_sum = sum(g(f"{x}_white_wins") + 0.5 * g(f"{x}_draws") for x in groups)
    crack_col = "white_wins" if our_white else "black_wins"
    crack = sum(g(f"{x}_{crack_col}") for x in groups)
    oe = aux.get("other_eval_mean")
    return (term_tot, term_sum, crack,
            g("other_total"),
            g("other_white_wins") + 0.5 * g("other_draws"),
            float("nan") if oe is None else float(oe),
            g("other_eval_cov"),
            g("horizon_total"),
            g("horizon_white_wins") + 0.5 * g("horizon_draws"))


# ── gates (reimplemented; production-parity pinned by _test_budget_oracle) ──

def passes_abs_gate(child_eval_ws: float | None, slice_prior: float,
                    our_white: bool,
                    robustness_floor: float = ROBUSTNESS_FLOOR) -> bool:
    """Stage 3's absolute refutation gate, gate-metric=eval semantics
    (stage3 L1200-1206). Candidates without an eval never reach here in the
    blessed recipe (require_eval drops them first)."""
    if child_eval_ws is None:
        return False
    if our_white:
        return child_eval_ws >= slice_prior - robustness_floor
    return child_eval_ws <= slice_prior + robustness_floor


def rel_gate_keep(cand_evals_ws: list[float | None], own_eval_ws: float | None,
                  our_white: bool,
                  gate_rel_floor: float = GATE_REL_FLOOR,
                  gate_rel_own_margin: float = GATE_REL_OWN_MARGIN,
                  binding: list[bool] | None = None
                  ) -> list[bool]:
    """Stage 3's relative refutation gate (L1208-1263), own-eval baseline.
    Returns a keep-mask; uncovered candidates (None) are exempt (kept).
    binding (rare-move verification, 2026-11-10): which candidates may set the
    reference; None = all (the old rule). See gate_candidates."""
    if gate_rel_floor >= 1.0 or not cand_evals_ws:
        return [True] * len(cand_evals_ws)
    elig = [e for e in cand_evals_ws if e is not None]
    if not elig:
        return [True] * len(cand_evals_ws)
    if binding is not None:
        bound = [e for e, b in zip(cand_evals_ws, binding) if e is not None and b]
        # with no trusted sibling and no own eval there is nothing better to measure from
        elig = bound if (bound or own_eval_ws is not None) else elig
    base_rv = None
    if own_eval_ws is not None:
        base_rv = (own_eval_ws - gate_rel_own_margin if our_white
                   else own_eval_ws + gate_rel_own_margin)
    if base_rv is None and len(cand_evals_ws) < 2:
        return [True] * len(cand_evals_ws)
    if our_white:
        best = max(elig) if elig else base_rv
        if base_rv is not None:
            best = max(best, base_rv)
        return [e is None or e >= best - gate_rel_floor
                for e in cand_evals_ws]
    best = min(elig) if elig else base_rv
    if base_rv is not None:
        best = min(best, base_rv)
    return [e is None or e <= best + gate_rel_floor for e in cand_evals_ws]


def ws_to_cp(ws: float, our_white: bool) -> float:
    """Inverse of cp_to_expected_score, from our side's view (centipawns)."""
    ws = min(max(ws, 1e-9), 1.0 - 1e-9)
    cp = math.log(ws / (1.0 - ws)) / LICHESS_CP_SCALE
    return cp if our_white else -cp


# Evals are whole centipawns, but a gate sees them after cp -> expected score -> cp, which is off
# by ~1e-14: a loss of exactly 30 read 30.00000000000003 for one colour and 29.99999999999997 for
# the other. With rare-move verification the limit is inclusive to within this tolerance, the same
# as the verifier's integer `verified_loss_cp > limit`. The old rule is left bit for bit.
CP_EPS = 1e-6


def eval_loss_keep(cand_es: list[tuple[dict, float | None]], own_eval_ws: float | None,
                   our_white: bool, max_loss_cp: float,
                   binding: list[bool] | None = None, dispute: dict | None = None
                   ) -> list[tuple[dict, float | None]]:
    """--max-eval-loss (2026-10-22): keep only candidates whose engine eval is within
    `max_loss_cp` centipawns of the best play there -- the best evaluated candidate,
    or the position's own eval if that is higher. Applies to the source book's move
    too (the Stage-3 gates admit a gambit worth ~-0.6 at the root). Candidates
    without an eval are dropped once any has one; if none is close enough, the
    single best-evaluated candidate stays, so the node never loses its choice set."""
    scored = [(pair, ws_to_cp(pair[1], our_white)) for pair in cand_es if pair[1] is not None]
    if not scored:
        return cand_es
    if binding is None:
        best = max(c for _, c in scored)
        if own_eval_ws is not None:
            best = max(best, ws_to_cp(own_eval_ws, our_white))
        keep = [pair for pair, c in scored if c >= best - max_loss_cp]
        return keep or [max(scored, key=lambda t: t[1])[0]]
    # Rare-move verification: an unverified rare move's stored eval may not set the
    # reference. It stays eligible when it is within the limit of the reference, or above it.
    binds = {id(pair) for pair, b in zip(cand_es, binding) if b}
    refs = [c for pair, c in scored if id(pair) in binds]
    if own_eval_ws is not None:
        refs.append(ws_to_cp(own_eval_ws, our_white))
    best = max(refs) if refs else max(c for _, c in scored)
    cut = max_loss_cp + CP_EPS
    keep = [pair for pair, c in scored if c >= best - cut]
    if dispute is not None and refs:
        threats = [(pair[0]["move_san"], c) for pair, c in scored
                   if id(pair) not in binds and c > best + CP_EPS]
        if threats:
            top = max(c for _, c in threats)
            risk = [pair[0]["move_san"] for pair, c in scored
                    if best - cut <= c < top - cut]
            dispute.update(reference_cp=best, threats=threats, at_risk=risk)
    # The position's own stored eval can itself sit above every trusted move (it often agrees
    # with the inflated rare child). On the old menu the least-losing trusted move then stayed
    # as the fallback; an unverified rare move inside the limit must not take that away, or
    # the sound line is pruned before anything is searched. Keep it, and record the dispute.
    trusted = [(pair, c) for pair, c in scored if id(pair) in binds]
    if trusted and not any(id(pair) in binds for pair in keep):
        fallback, fc = max(trusted, key=lambda t: t[1])
        # nothing inside the limit at all: the rare moves stored above the fallback stay too
        rare = keep or [pair for pair, c in scored if id(pair) not in binds and c > fc + CP_EPS]
        if dispute is not None and rare:
            cps = {id(pair): c for pair, c in scored}
            dispute.update(reference_cp=best, fallback=True, at_risk=[fallback[0]["move_san"]],
                           threats=[(pair[0]["move_san"], cps[id(pair)]) for pair in rare])
        keep = rare + [fallback]
    return keep or [max(scored, key=lambda t: t[1])[0]]


def score_candidates(h, edges, eval_ws, our_white, slice_prior, absolute, limit,
                     rare_floor, verified, disputes):
    """Reference implementation of the Rust builder's hard expected-score gates."""
    def side(ws):
        return ws if our_white else 1.0 - ws
    floor = side(slice_prior) - absolute
    candidates = [(e, eval_ws[e["child_hash"]]) for e in edges
                  if e.get("child_hash") in eval_ws and side(eval_ws[e["child_hash"]]) + SCORE_EPS >= floor]
    checked = (verified or {}).get(h, ())
    def trusted(e):
        return (rare_floor is None or e.get("engine") or e.get("total", 0) >= rare_floor
                or e["move_san"].rstrip("+#") in checked)
    refs = [side(v) for e, v in candidates if trusted(e)]
    if h in eval_ws:
        refs.append(side(eval_ws[h]))
    best = max(refs, default=None)
    kept = [(e, v) for e, v in candidates if best is None or best - side(v) <= limit + SCORE_EPS]
    if disputes is not None and rare_floor is not None:
        threats = sorted(((e["move_san"], side(v)) for e, v in kept if not trusted(e)
                          and (best is None or side(v) > best + SCORE_EPS)), key=lambda x: (-x[1], x[0]))
        if threats:
            risk = [e["move_san"] for e, v in kept if threats[0][1] - side(v) > limit + SCORE_EPS]
            if risk:
                disputes[h] = dict(reference_cp=None if best is None else ws_to_cp(best, True),
                                   threats=[(m, ws_to_cp(v, True)) for m, v in threats], at_risk=risk)
                if best is None:
                    disputes[h]["unresolved"] = True
    return kept


def gate_candidates(h: int, es: list[dict], src: str | None,
                    eval_ws: dict[int, float], our_white: bool,
                    slice_prior: float,
                    robustness_floor: float = ROBUSTNESS_FLOOR,
                    gate_rel_floor: float = GATE_REL_FLOOR,
                    gate_rel_own_margin: float = GATE_REL_OWN_MARGIN,
                    require_eval: bool = True, gates: bool = True,
                    max_eval_loss: float | None = None,
                    rare_floor: int | None = None,
                    verified: dict | None = None, disputes: dict | None = None, max_score_loss=None
                    ) -> list[tuple[dict, float | None]]:
    """The recorded moves at our node `h` that may be booked: the source book's
    move always, then those passing require-eval and both refutation gates. One
    definition for build_graph (the choice set) and for the subgraph collector's
    --gated-reach expansion, so the two can never disagree on what is reachable.
    max_eval_loss (cp, default None = off) then filters all of them, the source
    move included: see eval_loss_keep.

    rare_floor (rare-move verification, 2026-11-10; None = off, the exact old rule): with a
    complete own-move menu, a move observed fewer than rare_floor times whose score has not
    been verified (verified[h] holds the bare SANs searched afresh at this parent) cannot set
    a gate reference. Its stored eval is a selected maximum over many thin candidates, and
    read about 88 cp high where it displaced a commoner move. It stays a candidate. Where it
    would have excluded a rival had it bound, disputes[h] records the threat, so a caller can
    verify the node once the book depends on it.

    With no trusted reference at all (no eval for the position itself and no trusted or verified
    scored move) both relative filters are deferred: every candidate that passed the other gates
    stays, and disputes[h] is marked 'unresolved' with the moves the old rule would have cut."""
    if max_score_loss is not None:
        return score_candidates(h, es, eval_ws, our_white, slice_prior, robustness_floor,
                                max_score_loss, rare_floor, verified, disputes)
    binds = None
    if rare_floor is not None:
        ok = verified.get(h, ()) if verified else ()

        def binds(e):
            return bool(e.get("engine") or e.get("total", 0) >= rare_floor
                        or e["move_san"].rstrip("+#") in ok)
    cand_es = []
    for e in es:
        ch = e.get("child_hash")
        cev = eval_ws.get(ch) if ch is not None else None
        if e["move_san"] == src:
            cand_es.append((e, cev))
            continue
        if not gates:
            cand_es.append((e, cev))
            continue
        if require_eval and cev is None:
            continue
        if not passes_abs_gate(cev, slice_prior, our_white, robustness_floor):
            continue
        cand_es.append((e, cev))
    if binds is not None and cand_es and eval_ws.get(h) is None             and not any(cv is not None and binds(e) for e, cv in cand_es):
        # Nothing trusted to measure from: the best stored score here is itself an unverified
        # rare move's. Decide nothing on it.
        naive = cand_es
        if gates:
            keep = rel_gate_keep([cv for _, cv in naive], None, our_white,
                                 gate_rel_floor, gate_rel_own_margin)
            naive = [(e, cv) for (e, cv), k in zip(naive, keep) if k or e["move_san"] == src]
        if max_eval_loss is not None:
            naive = eval_loss_keep(naive, None, our_white, max_eval_loss)
        stay = {id(e) for e, _ in naive}
        risk = [e["move_san"] for e, cv in cand_es if cv is not None and id(e) not in stay]
        if risk and disputes is not None:
            disputes[h] = dict(
                reference_cp=None, unresolved=True, at_risk=risk,
                threats=sorted(((e["move_san"], ws_to_cp(cv, our_white)) for e, cv in naive
                                if cv is not None), key=lambda t: (-t[1], t[0])))
        return cand_es
    if gates and cand_es:
        keep = rel_gate_keep([cv for _, cv in cand_es], eval_ws.get(h), our_white,
                             gate_rel_floor, gate_rel_own_margin,
                             None if binds is None else [binds(e) for e, _ in cand_es])
        cand_es = [(e, cv) for (e, cv), k in zip(cand_es, keep)
                   if k or e["move_san"] == src]
    if max_eval_loss is not None and cand_es:
        d = {} if (binds is not None and disputes is not None) else None
        cand_es = eval_loss_keep(cand_es, eval_ws.get(h), our_white, max_eval_loss,
                                 None if binds is None else [binds(e) for e, _ in cand_es], d)
        if d:
            disputes[h] = d
    return cand_es


# ── graph ───────────────────────────────────────────────────────────────────

@dataclass
class OurNode:
    l_node: float                      # our-perspective stop value
    # candidates: (san, child_hash or None, in_subgraph, leaf_val)
    # leaf_val is the our-perspective flat value used when the child is not a
    # subgraph node (dead end / beyond ply cap).
    cands: list[tuple[str, int | None, bool, float]] = field(
        default_factory=list)
    # (continuing mass, crack mass) when the aux sidecar folded opponent collapses
    # into l_node; apply_stop_values re-applies the same blend to a new stop value.
    crack: tuple[float, float] | None = None


@dataclass
class OppNode:
    const_base: float                  # below-floor replies + aux buckets
    # kids: (p, child_hash, in_subgraph, leaf_val)
    kids: list[tuple[float, int | None, bool, float]] = field(
        default_factory=list)
    # replies under the share floor, already summed into const_base:
    # (p, child_hash, leaf_val), kept so a stop model can revalue them
    floor: list[tuple[float, int, float]] = field(default_factory=list)


@dataclass
class Graph:
    root: int
    our: dict[int, OurNode]
    opp: dict[int, OppNode]
    epd: dict[int, str]
    ply: dict[int, int]
    reach: dict[int, float]
    meta: dict = field(default_factory=dict)
    # build_graph(track_route_ply=True): position -> the ply of its highest-reach route (see
    # route_plies). ply above is what the depth cut and turn parity read; this is for values.
    route_ply: dict[int, int] = field(default_factory=dict)
    # build_graph(track_other=True): opponent node -> (the other bucket's share of the node, its
    # value and the node's engine value, our view, engine None if unknown). See sampled_other.py.
    other: dict[int, tuple[float, float, float | None]] = field(default_factory=dict)

    def side(self, h: int) -> str:
        return "our" if h in self.our else "opp"


def incumbent_move(g: "Graph", h: int) -> tuple[str | None, float]:
    """Rebuild stickiness (--incumbent, 2026-10-24): g.meta["stick"] = (moves, margin), where
    moves maps an our-node to the PREVIOUS book's move there (check marks stripped). Every
    move-choosing step adds `margin` to that move's selection key, so it is kept unless
    another candidate is better by more than the margin -- the same rule as Stage 3's
    --stick-margin. Only the choice is biased: values propagate unbiased, so whether to book
    a position at all (vs stopping) is decided on true values. Absent -> (None, 0.0), and
    every key equals its value: an exact no-op."""
    st = g.meta.get("stick") if g.meta else None
    if not st:
        return None, 0.0
    moves, margin = st
    return moves.get(h), margin


def _bonus(san: str, im: str | None, margin: float) -> float:
    return margin if im is not None and san.rstrip("+#") == im else 0.0


def reply_share(edge, recorded_total):
    share = edge.get("reach_share")
    if share is not None:
        return share
    return edge["total"] / recorded_total if recorded_total else 0.0


def optimistic_reach(edges_by_parent: dict[int, list[dict]], root: int,
                     root_our: bool, eps: float, max_ply: int,
                     share_floor: float,
                     admitted: dict[int, set] | None = None
                     ) -> tuple[dict[int, float], dict[int, int]]:
    """Policy-free reach upper bound: max over paths of the product of opponent
    reply shares; our moves carry probability 1. Pop route labels by descending
    reach, expanding each shorter arrival that still fits under the ply cap.

    admitted (--gated-reach): our node -> the child hashes of the moves the gates
    admit. Reach then flows only through moves the book could actually play; a
    position reachable only through a refuted move gets no reach. None = every
    recorded move (the policy-free bound, the default)."""
    import heapq
    reach: dict[int, float] = {}
    ply: dict[int, int] = {}
    heap = [(-1.0, 0, root, 0)]
    tie = 1
    while heap:
        nr, _, h, p_ply = heapq.heappop(heap)
        r = -nr
        if h in ply and ply[h] <= p_ply:
            continue
        reach.setdefault(h, r)
        ply[h] = p_ply
        if p_ply >= max_ply:
            continue
        es = edges_by_parent.get(h)
        if not es:
            continue
        our_turn = (p_ply % 2 == 0) == root_our
        tot = sum(e["total"] for e in es)
        for e in es:
            ch = e.get("child_hash")
            if ch is None:
                continue
            if our_turn:
                if admitted is not None and ch not in admitted.get(h, ()):
                    continue
                cr = r
            else:
                share = reply_share(e,tot)
                if share < share_floor:
                    continue
                cr = r * share
            if cr < eps or (ch in ply and ply[ch] <= p_ply + 1):
                continue
            heapq.heappush(heap, (-cr, tie, ch, p_ply + 1))
            tie += 1
    return reach, ply


def final_candidates(h: int, es: list[dict], src: str | None, eval_ws: dict[int, float],
                     our_white: bool, slice_prior: float, *,
                     robustness_floor: float = ROBUSTNESS_FLOOR,
                     gate_rel_floor: float = GATE_REL_FLOOR,
                     gate_rel_own_margin: float = GATE_REL_OWN_MARGIN,
                     require_eval: bool = True, gates: bool = True,
                     max_eval_loss: float | None = None, max_cands: int = 0, max_score_loss=None,
                     strict_source: bool = False,
                     engine_moves: dict[int, list[tuple[str, int]]] | None = None,
                     edges_by_parent: dict | None = None, epd: str | None = None,
                     excluded: dict | None = None, rare_floor: int | None = None,
                     verified: dict | None = None, disputes: dict | None = None):
    """The moves an our-node may book: one definition for build_graph and validate_book, so the
    book and its checker cannot disagree. Engine proposals join the recorded moves before the
    gates; the gates and --max-eval-loss run over that set; then the candidate cap; then the
    source move, if the set lacks it (see strict_source in build_graph). Returns (cand_es = the
    (edge, child eval) pairs, restored = (source move, child hash) or None, rejected source
    moves, engine moves added)."""
    edges_by_parent = edges_by_parent if edges_by_parent is not None else {}
    veto = excluded.get(h, ()) if excluded else ()
    if veto:
        es = [e for e in es if e["move_san"].rstrip("+#") not in veto]
        if src and src.rstrip("+#") in veto:
            src = None
    es_g, n_eng = es, 0
    if engine_moves and h in engine_moves:
        have = {e["move_san"].rstrip("+#") for e in es}
        extra = [{"move_san": san, "child_hash": ch, "total": 0,
                  "white_score_avg": slice_prior, "engine": True}
                 for san, ch in engine_moves[h] if san.rstrip("+#") not in have and san.rstrip("+#") not in veto]
        if extra:
            n_eng = len(extra)
            es_g = es + extra
    missing_source = None
    if (max_score_loss is not None and src and epd
            and all(e["move_san"].rstrip("+#") != src.rstrip("+#") for e in es_g)):
        from build_baseline_books import child_of
        missing_source = {"move_san": src, "child_hash": child_of(epd, src), "total": 0, "engine": True}
        es_g = [*es_g, missing_source]
    cand_es = gate_candidates(h, es_g, src, eval_ws, our_white,
                              slice_prior, robustness_floor,
                              gate_rel_floor, gate_rel_own_margin,
                              require_eval, gates, max_eval_loss,
                              rare_floor, verified, disputes, max_score_loss)
    # Candidate cap.
    #
    # WHAT IT IS NOT (corrected 2026-08-29): budget_sizing_probe.py
    # measured 4.26M nodes uncapped vs 410K at top-3 and the cap was
    # adopted as "the lever that makes the DP tractable" — but the probe
    # applied the cap DURING level expansion, and this code runs after
    # build_budget_books.collect_subgraph_edges has already expanded
    # every gate-passing child. So in production the cap does not shrink
    # the subgraph at all: mc3 and mc5 builds of the same book both
    # report 53,060 our / 97,889 opponent nodes. It is a CHOICE-SET
    # knob, and its cost is paid in value, not saved in memory.
    # scratch/python/budget_gate_fanout.py has the fanout distribution:
    # gates alone leave a mean of 2.66 candidates/node, 62% of nodes
    # have exactly one, and a top-3 cap binds on 19.5% of them.
    # Making the cap a real memory lever means capping inside
    # collect_subgraph_edges, which needs evals at expansion time.
    #
    # Ranking is deliberately NOT eval alone: eval-ranking biases the
    # cap toward the unconstrained book's own preference, and the whole
    # point of a budget is that a lower-eval move with a CHEAPER subtree
    # can win. So we keep, in order: the source book's move (always —
    # it passed the real gates, incl. engine augmentation), the top-K by
    # our-perspective child eval, and the single most FORCING candidate
    # (highest reply concentration = fewest opponent branches to
    # prepare), which is precisely the budget-relevant axis eval misses.
    if max_cands and len(cand_es) > max_cands:
        def _ev(pair):
            _e, cv = pair
            if cv is None:
                return -1e9
            return cv if our_white else 1.0 - cv

        def _forcing(pair):
            e, _cv = pair
            ch = e.get("child_hash")
            kids = edges_by_parent.get(ch) if ch is not None else None
            return forcingness(kids) if kids else 0.0

        srcs = [c for c in cand_es if c[0]["move_san"] == src]
        rest = [c for c in cand_es if c[0]["move_san"] != src]
        rest.sort(key=_ev, reverse=True)
        kept = rest[:max(0, max_cands - len(srcs))]
        if rest and not any(c is max(rest, key=_forcing) for c in kept):
            kept = kept[:-1] + [max(rest, key=_forcing)] if kept \
                else [max(rest, key=_forcing)]
        cand_es = srcs + kept
    if max_score_loss is not None:
        restored = next(((src, e["child_hash"]) for e, _ in cand_es if e is missing_source), None)
        rejected = bool(src and all(e["move_san"].rstrip("+#") != src.rstrip("+#") for e, _ in cand_es))
        return [(e, cv) for e, cv in cand_es if e is not missing_source], restored, int(rejected), n_eng
    seen_src = any(e["move_san"] == src for e, _cv in cand_es)
    recorded_src = src is not None and any(e["move_san"] == src for e in es)
    if src and not seen_src and strict_source and recorded_src:
        return cand_es, None, 1, n_eng
    if src and not seen_src:
        # engine-augmented source move: no stats edge. Child hash via
        # python-chess where an EPD is available (CLI path); tests pass
        # explicit edges so this stays cold there.
        ch = None
        if epd is not None:
            from build_baseline_books import child_of
            ch = child_of(epd, src)
        if strict_source and max_eval_loss is not None:
            cev = eval_ws.get(ch) if ch is not None else None
            ok = verified.get(h, ()) if verified else ()
            pairs = cand_es + [({"move_san": src}, cev)]
            kept = None if cev is None else eval_loss_keep(
                pairs, eval_ws.get(h), our_white, max_eval_loss,
                None if rare_floor is None else [
                    bool(e.get("engine") or e.get("total", rare_floor) >= rare_floor
                         or e["move_san"].rstrip("+#") in ok) for e, _ in pairs])
            if kept is None or not any(e["move_san"] == src for e, _ in kept):
                return cand_es, None, 1, n_eng
            # One loss rule over the COMPLETE set (follow-up review R1): a
            # source move better than every recorded one raises the baseline,
            # so recorded moves it puts out of range leave too.
            keep_sans = {e["move_san"] for e, _ in kept}
            cand_es = [c for c in cand_es if c[0]["move_san"] in keep_sans]
        return cand_es, (src, ch), 0, n_eng
    return cand_es, None, 0, n_eng


def loss_status(children, h: int, eval_ws: dict[int, float], our_white: bool,
                max_loss_cp: float, eps: float = 0.0) -> str | None:
    """Could --max-eval-loss bind on this final candidate set? 'fallback': no candidate is within
    the limit of the position's own eval, so the least-losing one stayed (the owner's rule,
    2026-10-08). 'unknown': no candidate has an eval. None: every candidate is within it."""
    cps = [ws_to_cp(eval_ws[ch], our_white) for ch in children
           if ch is not None and ch in eval_ws]
    if not cps:
        return "unknown"
    if h in eval_ws and ws_to_cp(eval_ws[h], our_white) - max(cps) > max_loss_cp + eps:
        return "fallback"
    return None


def verified_cp_overrides(frame):
    """Local positional overrides. Keep each edge's verified loss separate."""
    import polars as pl
    direct = {}
    if "transposition_cp_white" in frame.columns:
        rows = frame.select("child_hash", "transposition_cp_white").drop_nulls().unique()
        direct = dict(rows.iter_rows())
        if len(direct) != rows.height:
            raise ValueError("Conflicting direct searches for a transposition")
    if "parent_cp_white" not in frame.columns:
        return direct
    verified = frame.filter(pl.col("parent_cp_white").is_not_null())
    parents = verified.select("position_hash", "parent_cp_white").unique()
    if parents["position_hash"].n_unique() != parents.height:
        raise ValueError("Conflicting reference scores for a single verified root")
    child = verified.group_by("child_hash").agg(pl.col("child_cp_white").cast(pl.Int32).median().round().cast(pl.Int32))
    return dict(child.drop_nulls().iter_rows()) | direct | dict(parents.iter_rows())


def proposal_cp(frame):
    """Resolve incoming PV scores once per position, independent of parquet row order."""
    import polars as pl
    from hybrid_candidates import CLOUD
    # Old nomination files may have only the four graph columns. Equal-quality
    # estimates use their median; a missing field ranks below a known one.
    best = frame.filter(pl.col("child_cp_white").is_not_null())
    if "engine" in best.columns:
        best = best.with_columns((pl.col("engine") != CLOUD).fill_null(False).alias("local"))
    for key in ("local", "depth", "nodes"):
        if key in best.columns:
            quality = pl.col(key).cast(pl.Int64).fill_null(-1)
            best = best.filter(quality == quality.max().over("child_hash"))
    values = best.group_by("child_hash").agg(pl.col("child_cp_white").cast(pl.Int32).median().round().cast(pl.Int32))
    return dict(values.iter_rows()) | verified_cp_overrides(frame)


def load_engine_candidates(path: str | bytes, eval_ws: dict[int, float], conflict_cp: float = 25.0):
    """Load candidate edges and overlay the resolved White-view position scores.

    Report each child whose new score differs materially from the original cache.
    """
    import polars as pl
    from stage3_backwards_induction import cp_to_expected_score
    moves: dict[int, list[tuple[str, int]]] = {}
    conflicts = []
    frame = pl.read_parquet(path)
    resolved = proposal_cp(frame)
    edges = frame.select("position_hash", "move_san", "child_hash").unique().sort(
        "position_hash", pl.col("move_san").str.to_lowercase(), "move_san", "child_hash")
    reported = set()
    for h, san, ch in edges.iter_rows():
        moves.setdefault(h, []).append((san, ch))
        old = eval_ws.get(ch)
        cp = resolved.get(ch)
        if old is not None and cp is not None and ch not in reported:
            old_cp = ws_to_cp(old, True)
            if abs(old_cp - cp) > conflict_cp:
                conflicts.append({"position_hash": h, "move": san, "child_hash": ch,
                                  "cached_cp_white": old_cp, "fresh_cp_white": cp})
                reported.add(ch)
    eval_ws.update({h: cp_to_expected_score(cp) for h, cp in resolved.items()})
    return moves, conflicts


def verified_moves(frame) -> dict:
    """{parent hash: bare SANs searched afresh at that parent} from a proposal overlay."""
    import polars as pl
    if frame is None or "verification_nodes" not in frame.columns:
        return {}
    out: dict = {}
    for h, san in frame.filter(pl.col("verification_nodes").is_not_null()).select(
            "position_hash", "move_san").iter_rows():
        out.setdefault(h, set()).add(san.rstrip("+#"))
    return out


def verified_exclusions(proposals, limit, *, max_score_loss=None, our_white=True, score_floor=None):
    """Edge-local vetoes also apply to recorded moves, restored source moves and fallbacks."""
    import polars as pl
    if proposals is None or (limit is None and max_score_loss is None):
        return {}
    frame = pl.read_parquet(proposals)
    excluded = {}
    if "verified_loss_cp" in frame.columns:
        if max_score_loss is None:
            rejected = pl.col("verified_loss_cp") > limit
        else:
            from stage3_backwards_induction import _es_expr
            # Use the actual verified edge even when a transposition overrides its board score.
            sign = 1 if our_white else -1
            parent = _es_expr("parent_cp_white")
            child = _es_expr("child_cp_white")
            fails = sign * (parent - child) > max_score_loss + SCORE_EPS
            if score_floor is not None:
                fails |= (child if our_white else 1.0 - child) + SCORE_EPS < score_floor
            rejected = pl.col("verified_loss_cp").is_not_null() & fails.fill_null(True)
        for h, san in frame.filter(rejected).select("position_hash", "move_san").iter_rows():
            excluded.setdefault(h, set()).add(san.rstrip("+#"))
    return excluded


def route_plies(edges_by_parent: dict[int, list[dict]], root: int, root_our: bool,
                reach: dict[int, float], ply: dict[int, int], max_ply: int,
                share_floor: float, admitted: dict[int, set] | None = None) -> dict[int, int]:
    """The ply of each position's highest-reach route AMONG THE ROUTES THAT ARRIVE IN TIME.
    optimistic_reach records the earliest qualifying arrival. The stop model instead
    uses the ply of the most likely qualifying route.

    Best reach per (position, ply), level by level up to the cap (2026-11-04, review finding 1).
    Keeping only each position's overall best route ran past the horizon: a high-reach long arrival
    hid the short, less likely arrival that was the only way to reach a later child in time. Every
    label is at most max_ply; a stop just outside is then at most max_ply + 1, the horizon. Ties
    (within rounding, as products of the same shares can differ in the last bit) go to the shortest
    route, independently of edge order. Only edges the graph can hold are walked: a node whose
    earliest qualifying arrival is at the cap has no children in it."""
    best: dict[int, tuple[float, int]] = {root: (1.0, 0)}
    level = {root: 1.0}
    t = 0
    while level and t < max_ply:
        nxt: dict[int, float] = {}
        our_turn = (t % 2 == 0) == root_our
        for h, r in level.items():
            if ply[h] >= max_ply:
                continue
            es = edges_by_parent.get(h)
            if not es:
                continue
            tot = sum(e["total"] for e in es)
            for e in es:
                ch = e.get("child_hash")
                if ch is None or ch not in reach:
                    continue
                if our_turn:
                    if admitted is not None and ch not in admitted.get(h, ()):
                        continue
                    cr = r
                else:
                    share = reply_share(e,tot)
                    if share < share_floor:
                        continue
                    cr = r * share
                if cr > nxt.get(ch, 0.0):
                    nxt[ch] = cr
        t += 1
        for ch, cr in nxt.items():
            b = best.get(ch)
            if b is None or cr > b[0] * (1.0 + 1e-12):
                best[ch] = (cr, t)
        level = nxt
    return {h: p for h, (_r, p) in best.items()}


def build_graph(edges_by_parent: dict[int, list[dict]], root: int,
                our_white: bool, *,
                rep_moves: dict[int, str] | None = None,
                eval_ws: dict[int, float] | None = None,
                aux_rows: dict[int, dict] | None = None,
                slice_prior: float = 0.5,
                eps: float = 1e-3, max_ply: int = 40,
                share_floor: float = 0.002,
                prior_strength: float = PRIOR_STRENGTH,
                eval_weight: float = EVAL_WEIGHT,
                eval_weight_min: float = EVAL_WEIGHT_MIN,
                eval_weight_k: float = EVAL_WEIGHT_K,
                robustness_floor: float = ROBUSTNESS_FLOOR,
                gate_rel_floor: float = GATE_REL_FLOOR,
                gate_rel_own_margin: float = GATE_REL_OWN_MARGIN,
                require_eval: bool = True,
                max_cands: int = 0,
                gates: bool = True,
                other_cov_blend: bool = False,
                gated_reach: bool = False,
                max_eval_loss: float | None = None, max_score_loss=None,
                strict_source: bool = False,
                crack_at_parents: bool = False,
                engine_moves: dict[int, list[tuple[str, int]]] | None = None,
                track_route_ply: bool = False, track_other: bool = False,
                excluded: dict | None = None, rare_floor: int | None = None,
                verified: dict | None = None) -> Graph:
    """Assemble the budget DP's subgraph from in-memory edges.

    edges_by_parent: parent_hash -> list of dicts with keys move_san,
    child_hash, total, white_score_avg (white-perspective). The CLI builds this
    from a DuckDB frontier expansion; tests build it synthetically.

    eval_ws: position_hash -> WHITE-perspective expected score (already through
    cp_to_expected_score). rep_moves: source book's best_move per our node —
    always admitted as a candidate (it passed the real Stage-3 gates, including
    engine augmentation). aux_rows: per-position aux sidecar dicts (term/other/
    horizon buckets) mirrored into opponent-node constants so V_infinity
    reconciles with the source rep.

    Both flags below default off HERE (the legacy graph, which the oracle and parity
    tests pin) and ON in build_budget_books since 2026-10-31.

    strict_source (--strict-source, 2026-10-31): the source book's move
    used to be restored as an "engine-augmented" candidate whenever it was missing
    from the gated set -- including when it WAS recorded and the gates (notably
    --max-eval-loss) had rejected it, so the limit never bound the source move (26 White
    / 37 Black saved choices broke it). Strict: a recorded source move the gates
    reject stays out; an unrecorded one (truly engine-augmented) is restored only if
    it passes --max-eval-loss against the gated candidates and the position's own
    eval, and never without an eval when the limit is on.

    engine_moves (--engine-candidates, 2026-10-31, default None = off): our-node ->
    [(san, child_hash)] proposed by an engine search (engine_candidates.py: Stockfish's
    principal-variation move where the recorded moves leave the loss limit unenforced). Each
    joins the node's moves as a zero-game edge BEFORE the gates and --max-eval-loss, so it is
    judged in the complete set like any recorded move (follow-up review R1/R5); its value is
    its eval through leaf_edge_value (eval_ws must hold the child's eval). Unless an engine
    move leads to an expanded pool node it is valued as a leaf.

    crack_at_parents (--crack-at-parents, 2026-10-31): games that end at
    an our-turn arrival because the OPPONENT resigned or was mated (the `crack` mass)
    are ours whatever we would have played. The legacy blend folded them into the stop
    value only, so booking a move compared an unblended child value against a blended
    stop and lost the crack credit (undervaluing preparation by crack x (1 - v)). Here
    they are taken out at the parent instead: an opponent node's edge into such an our
    node carries probability p * cont/(cont + crack) and the rest, p * crack/(cont +
    crack), is a win in const_base; the our node itself holds unblended values for
    both stopping and booking. Exact and engine-agnostic (every sweep reads kids and
    const_base). The root's own crack, if any, is not applied (no parent).
    """
    eval_ws = eval_ws or {}
    rep_moves = rep_moves or {}
    aux_rows = aux_rows or {}

    # Whose turn it is at even ply depends on the perspective: for a Black book
    # the root (ply 0, White to move) is an OPPONENT node, so reach must decay
    # by White's reply shares there.
    admitted = None
    restored_children = {}
    if gated_reach:
        # Our-turn parity is unknown until reach assigns plies, but the gate is a
        # property of the position: admit per parent, and optimistic_reach only
        # consults it at our-turn nodes.
        admitted = {}
        for h, es in edges_by_parent.items():
            if es and "reply_mass" in es[0]:
                continue  # the overlay contains only opponent-to-move positions
            if strict_source or engine_moves:
                # reach flows only through what the node can finally book
                cs, restored, _, _ = final_candidates(
                    h, es, rep_moves.get(h), eval_ws, our_white, slice_prior,
                    robustness_floor=robustness_floor, gate_rel_floor=gate_rel_floor,
                    gate_rel_own_margin=gate_rel_own_margin, require_eval=require_eval,
                    gates=gates, max_eval_loss=max_eval_loss, max_score_loss=max_score_loss, strict_source=strict_source,
                    engine_moves=engine_moves, edges_by_parent=edges_by_parent,
                    epd=es[0].get("parent_epd"), excluded=excluded,
                    rare_floor=rare_floor, verified=verified)
                if restored is not None and restored[1] is not None:
                    restored_children[h] = restored[1]
            else:
                cs = gate_candidates(
                    h, es, rep_moves.get(h), eval_ws, our_white, slice_prior,
                    robustness_floor, gate_rel_floor, gate_rel_own_margin,
                    require_eval, gates, max_eval_loss, rare_floor, verified, max_score_loss=max_score_loss)
            admitted[h] = {e["child_hash"] for e, _ in cs if e.get("child_hash") is not None}
            if h in restored_children:
                admitted[h].add(restored_children[h])
    if strict_source and not gated_reach:
        for h, src in rep_moves.items():
            es = edges_by_parent.get(h)
            if es and src and not any(e['move_san'] == src for e in es) and es[0].get('parent_epd'):
                from build_baseline_books import child_of
                child = child_of(es[0]['parent_epd'], src)
                if child is not None and src.rstrip('+#') not in (excluded or {}).get(h, ()):
                    restored_children[h] = child
    # Search below an unplayed nominated move when its child has pool replies.
    # Zero observations keep it out of empirical frequencies and stop estimates.
    reach_edges = edges_by_parent
    if engine_moves:
        reach_edges = dict(edges_by_parent)
        for h, moves in engine_moves.items():
            if h in edges_by_parent:
                existing = {e.get("child_hash") for e in edges_by_parent[h]}
                reach_edges[h] = edges_by_parent[h] + [
                    {"child_hash": ch, "total": 0} for _, ch in moves if ch not in existing]
    if restored_children:
        reach_edges = dict(reach_edges)
        for h, ch in restored_children.items():
            if not any(e.get('child_hash') == ch for e in reach_edges[h]):
                reach_edges[h] = reach_edges[h] + [{"child_hash": ch, "total": 0}]
    reach, ply = optimistic_reach(reach_edges, root, our_white, eps,
                                  max_ply, share_floor, admitted)

    def is_our(h: int) -> bool:
        return (ply[h] % 2 == 0) == our_white

    def our_eval(h: int) -> float | None:
        e = eval_ws.get(h)
        if e is None:
            return None
        return e if our_white else 1.0 - e

    route = (route_plies(reach_edges, root, our_white, reach, ply, max_ply, share_floor,
                         admitted) if track_route_ply else {})
    g = Graph(root=root, our={}, opp={}, epd={}, ply=dict(ply), route_ply=route,
              reach=dict(reach), meta={
                  "eps": eps, "max_ply": max_ply, "share_floor": share_floor,
                  "slice_prior": slice_prior, "our_white": our_white})

    for h, r in reach.items():
        es = edges_by_parent.get(h)
        if not es:
            continue                      # reachable but no recorded edges: leaf
        p_ply = ply[h]
        if "parent_epd" in es[0]:
            g.epd[h] = es[0]["parent_epd"]
        if is_our(h):
            lst = [(e["white_score_avg"], e["total"]) for e in es]
            ln = leaf_node_value(lst, eval_ws.get(h), slice_prior, our_white,
                                 prior_strength, eval_weight,
                                 eval_weight_min, eval_weight_k)
            # Opponent collapses BEFORE our move land on OUR nodes, so the aux
            # row here is not empty -- and it used to go entirely unread, valuing
            # "stop and wing it" purely on the games that gave us a move. Stage 3
            # has always blended them; the two engines therefore optimised
            # different objectives, which a four-arm comparison would read as a
            # budget effect. Same rule, same function, both sides.
            if aux_rows:
                (_tt, _ts, crack, _ot, _os, _oe, _oc,
                 _ht, _hs) = aux_row_buckets(aux_rows.get(h), our_white)
                if crack:
                    cont = (sum(e["total"] for e in es) + _ot + _ht)
                    if not crack_at_parents:
                        ln = aux_our_blend(ln, cont, crack, 1.0)
            node = OurNode(l_node=ln)
            if aux_rows and crack:
                node.crack = (cont, crack)
            # gate the recorded candidates; always admit the source move
            src = rep_moves.get(h)
            cand_es, restored, n_rej, n_eng = final_candidates(
                h, es, src, eval_ws, our_white, slice_prior,
                robustness_floor=robustness_floor, gate_rel_floor=gate_rel_floor,
                gate_rel_own_margin=gate_rel_own_margin, require_eval=require_eval,
                gates=gates, max_eval_loss=max_eval_loss, max_score_loss=max_score_loss, max_cands=max_cands,
                strict_source=strict_source, engine_moves=engine_moves,
                edges_by_parent=edges_by_parent, epd=es[0].get("parent_epd"), excluded=excluded,
                rare_floor=rare_floor, verified=verified,
                disputes=g.meta.setdefault("rare_disputes", {}) if rare_floor is not None else None)
            if n_eng:
                g.meta["engine_cands"] = g.meta.get("engine_cands", 0) + n_eng
            if n_rej:
                g.meta["src_rejected"] = g.meta.get("src_rejected", 0) + n_rej
            for e, _cv in cand_es:
                ch = e.get("child_hash")
                in_sub = ch is not None and ch in reach and \
                    edges_by_parent.get(ch) is not None and p_ply + 1 <= max_ply
                lv = leaf_edge_value(e["white_score_avg"], e["total"],
                                     eval_ws.get(ch) if ch is not None else None,
                                     slice_prior, our_white,
                                     prior_strength, eval_weight,
                                     eval_weight_min, eval_weight_k)
                node.cands.append((e["move_san"], ch, in_sub, lv))
            if restored is not None:
                src, ch = restored
                in_sub = ch is not None and ch in reach and \
                    edges_by_parent.get(ch) is not None and p_ply + 1 <= max_ply
                lv = leaf_edge_value(slice_prior, 0,
                                     eval_ws.get(ch) if ch is not None else None,
                                     slice_prior, our_white,
                                     prior_strength, eval_weight,
                                     eval_weight_min, eval_weight_k)
                node.cands.append((src, ch, in_sub, lv))
            g.our[h] = node
        else:
            named = "reply_mass" in es[0]
            tot = sum(e.get("reply_mass",e["total"]) for e in es)
            if named:
                metrics = g.meta.setdefault("named_opponent_replies", {
                    "positions":0, "additional_edges":0, "additional_edges_without_cached_eval":0})
                metrics["positions"] += 1
                metrics["additional_edges"] += sum(e.get("additional_reply",e["total"]==0) for e in es)
                metrics["additional_edges_without_cached_eval"] += sum(
                    e.get("additional_reply",e["total"]==0) and e.get("child_hash") not in eval_ws for e in es)
            # The OTHER bucket used to enter raw here while Stage 3 blended
            # other_eval_mean into it at a coverage-weighted eval weight -- the
            # second half of the objective drift. aux_opp_mix owns all three
            # bucket rules now.
            #
            # horizon_mode is left at stage3's default ('empirical': a horizon
            # game's OUTCOME is observed, only its path is missing). The locked
            # recipe does not override it; a source build that passed
            # --aux-horizon eval would need it mirrored from meta here.
            aux_num = aux_den = 0.0
            if aux_rows:
                (t_tot, t_sum, _crack, o_tot, o_sum, o_eval, o_cov,
                 hr_tot, hr_sum) = aux_row_buckets(aux_rows.get(h), our_white)
                if named:
                    # Every observed rare reply is now explicit. Terminal and
                    # horizon outcomes still retain their original weight.
                    o_tot, o_sum, o_eval, o_cov = 0.0, 0.0, float("nan"), 0.0
                w_num, aux_den = aux_opp_mix(
                    t_tot, t_sum, o_tot, o_sum, o_eval, o_cov,
                    hr_tot, hr_sum, eval_ws.get(h),
                    eval_weight, eval_weight_min, eval_weight_k,
                    cov_blend=other_cov_blend)   # mirror the source rep's Stage-3 flag
                # One flip to our-perspective. Exact because the mix is linear in
                # the bucket sums, so the our-score numerator is den - num.
                aux_num = w_num if our_white else aux_den - w_num
            denom = tot + aux_den
            if track_other and aux_rows and o_tot:
                # the same mix with only this bucket in it: it is linear, so this is the
                # bucket's own part of aux_num
                o_num, o_den = aux_opp_mix(
                    0.0, 0.0, o_tot, o_sum, o_eval, o_cov, 0.0, 0.0, None,
                    eval_weight, eval_weight_min, eval_weight_k,
                    cov_blend=other_cov_blend)
                v_ws, e_ws = o_num / o_den, eval_ws.get(h)
                g.other[h] = (o_den / denom, v_ws if our_white else 1.0 - v_ws,
                              None if e_ws is None else (e_ws if our_white else 1.0 - e_ws))
            node = OppNode(const_base=(aux_num / denom) if denom else 0.0)
            for e in es:
                ch = e.get("child_hash")
                p = (e.get("reply_mass",e["total"]) / denom) if denom else 0.0
                share = reply_share(e,tot)
                lv = leaf_edge_value(e["white_score_avg"], e["total"],
                                     eval_ws.get(ch) if ch is not None else None,
                                     slice_prior, our_white,
                                     prior_strength, eval_weight,
                                     eval_weight_min, eval_weight_k)
                if share < share_floor or ch is None:
                    node.const_base += p * lv
                    if ch is not None:
                        node.floor.append((p, ch, lv))
                    continue
                in_sub = ch in reach and edges_by_parent.get(ch) is not None \
                    and p_ply + 1 <= max_ply
                node.kids.append((p, ch, in_sub, lv))
            g.opp[h] = node
    g.meta["strict_source"] = strict_source
    g.meta["crack_at_parents"] = crack_at_parents
    if crack_at_parents:
        moved = 0
        for n in g.opp.values():
            kids = []
            for p, ch, in_sub, lv in n.kids:
                o = g.our.get(ch) if in_sub and ch is not None else None
                if o is not None and o.crack:
                    cont, crack = o.crack
                    a = cont / (cont + crack)
                    n.const_base += p * (1.0 - a)      # a win, our perspective
                    p *= a
                    moved += 1
                kids.append((p, ch, in_sub, lv))
            n.kids = kids
        g.meta["crack_edges"] = moved
        ro = g.our.get(root)
        if ro is not None and ro.crack:
            g.meta["root_crack"] = ro.crack      # no parent edge: build_budget_books reports it
    if max_eval_loss is not None:
        g.meta["loss_exceptions"] = loss_exceptions(g, eval_ws, our_white, max_eval_loss,
                                                    0.0 if rare_floor is None else CP_EPS)
    return g


def loss_exceptions(g: Graph, eval_ws: dict[int, float], our_white: bool,
                    max_loss_cp: float, eps: float = 0.0) -> dict[int, str]:
    """Our-nodes whose FINAL candidate set the --max-eval-loss rule could not enforce
    (follow-up review R1/R5): 'fallback' -- no candidate is within the limit of the
    position's own eval, so eval_loss_keep kept the least-losing one (the owner's rule,
    2026-10-08); 'unknown' -- no candidate has an eval, so the rule could not judge any.
    With strict_source every other our-node's candidates lie within the limit (the
    legacy restore can add a source move the limit rejected, which this does not see)."""
    out = {}
    for h, n in g.our.items():
        if not n.cands:
            continue
        st = loss_status([c[1] for c in n.cands], h, eval_ws, our_white, max_loss_cp, eps)
        if st:
            out[h] = st
    return out


def stop_model_positions(g: Graph) -> tuple[set[int], set[int]]:
    """Where a stop model must value the position: (our-turn x, opponent-turn y).

    x: every our node (its stop value), every opponent reply leading out of the
       subgraph, every reply under the share floor.
    y: the child of every candidate move leading out of the subgraph (the book
       ends right after our move)."""
    xs, ys = set(g.our), set()
    for n in g.our.values():
        for _san, ch, in_sub, _lv in n.cands:
            if ch is not None and not in_sub:
                ys.add(ch)
    for n in g.opp.values():
        for _p, ch, in_sub, _lv in n.kids:
            if ch is not None and not in_sub:
                xs.add(ch)
        for _p, ch, _lv in n.floor:
            xs.add(ch)
    return xs, ys


def apply_stop_values(g: Graph, W: dict[int, float], R: dict[int, float]) -> dict:
    """Revalue every out-of-book position with a stop model (weak_stop.py): W for
    our-turn positions, R for the opponent-turn position after our book's last move.
    Positions the model has no value for keep the default. Returns counts."""
    c = {"our_l": 0, "cand_leaf": 0, "opp_leaf": 0, "opp_floor": 0}
    for h, n in g.our.items():
        w = W.get(h)
        if w is not None:
            n.l_node = (aux_our_blend(w, n.crack[0], n.crack[1], 1.0)
                        if n.crack and not g.meta.get("crack_at_parents") else w)
            c["our_l"] += 1
        new = []
        for san, ch, in_sub, lv in n.cands:
            if ch is not None and not in_sub and ch in R:
                lv = R[ch]
                c["cand_leaf"] += 1
            new.append((san, ch, in_sub, lv))
        n.cands = new
    for n in g.opp.values():
        new = []
        for p, ch, in_sub, lv in n.kids:
            if ch is not None and not in_sub and ch in W:
                lv = W[ch]
                c["opp_leaf"] += 1
            new.append((p, ch, in_sub, lv))
        n.kids = new
        for p, ch, lv in n.floor:
            if ch in W:
                n.const_base += p * (W[ch] - lv)
                c["opp_floor"] += 1
    return c


# ── the DP ──────────────────────────────────────────────────────────────────

def _adjacency(g: Graph) -> tuple[dict[int, list[int]], dict[int, list[int]]]:
    """(children, parents) restricted to in-subgraph nodes. Duplicate edges
    (a transposition reached twice from one opponent node) are kept — the
    per-path budget accounting depends on them."""
    children: dict[int, list[int]] = {}
    for h, n in g.our.items():
        children[h] = [ch for _, ch, in_sub, _ in n.cands
                       if in_sub and ch is not None]
    for h, n in g.opp.items():
        children[h] = [ch for _, ch, in_sub, _ in n.kids
                       if in_sub and ch is not None]
    all_nodes = set(g.our) | set(g.opp)
    parents: dict[int, list[int]] = {h: [] for h in all_nodes}
    for h in all_nodes:
        for c in children.get(h, []):
            if c in all_nodes:
                parents[c].append(h)
    return children, parents


def topo_down(g: Graph) -> list[int]:
    """Top-down topological order (parents before children) over the subgraph.

    CYCLES ARE NOT MERELY SKIPPED. A node in a transposition cycle never reaches
    in-degree 0 — and neither does anything BELOW it, so a plain Kahn pass drops
    the cycle plus its whole downstream cone. Measured 2026-08-29 on the <=2024
    white pool, eps 0.02, --max-cands 0: the old version returned 50,015 of
    150,949 nodes, the ROOT among the 100,934 omitted, and extract_book (which
    iterates this order and skips what it never sees) booked NOTHING at every
    budget while the curves were healthy — root capacity 20, eight root
    candidates beating the stop value. A top-3 cap hid it by pruning the edges
    that closed the cycles, which is why it only surfaced when the cap came off.

    So when the queue empties with nodes remaining, release the highest-reach
    survivor and continue — the mirror of build_curves' flatten-one-and-resume,
    and highest rather than lowest because here the point is to re-enter the
    stranded region at its most important node. The order may then place a
    released node before one of its parents; that is the same per-path
    conservatism _test_budget_transpo pins, and strictly better than a node the
    extraction never visits.
    """
    children, parents = _adjacency(g)
    all_nodes = set(g.our) | set(g.opp)
    indeg = {h: len(parents[h]) for h in all_nodes}
    q = deque(h for h, d in indeg.items() if d == 0)
    if g.root in all_nodes and indeg.get(g.root, 0) != 0:
        q.appendleft(g.root)          # the root is always an entry point
    order: list[int] = []
    placed: set[int] = set()
    # `remaining` is maintained incrementally and survivors are picked off a
    # heap, for the same reason build_curves keeps its pending counters: the
    # previous `all_nodes - placed` + `max(...)` pair was O(N) per released
    # node, and these graphs strand over a thousand.
    remaining = set(all_nodes)
    heap = [(-g.reach.get(h, 0.0), h) for h in all_nodes]
    heapq.heapify(heap)
    while True:
        while q:
            h = q.popleft()
            if h in placed:
                continue
            placed.add(h)
            remaining.discard(h)
            order.append(h)
            for c in children.get(h, []):
                if c in all_nodes and c not in placed:
                    indeg[c] -= 1
                    if indeg[c] <= 0:
                        q.append(c)
        if not remaining:
            break
        victim = None
        while heap:
            _nr, cand = heapq.heappop(heap)
            if cand in remaining:
                victim = cand
                break
        if victim is None:
            break
        q.append(victim)
    return order


def build_curves(g: Graph, bmax: int, k_atoms: int = 256
                 ) -> tuple[dict[int, Curve], dict]:
    """Bottom-up value-vs-budget curves over the subgraph.

    Kahn order on subgraph edges; nodes stranded by cycles get a flat L curve
    (their count is returned in diagnostics — measure, don't guess)."""
    children, parents = _adjacency(g)
    all_nodes = set(g.our) | set(g.opp)
    pending: dict[int, int] = {}
    for h in all_nodes:
        pending[h] = sum(1 for c in children.get(h, []) if c in all_nodes)

    curves: dict[int, Curve] = {}
    q = deque(h for h, p in pending.items() if p == 0)
    processed = 0
    while q:
        h = q.popleft()
        curves[h] = _node_curve(g, h, curves, bmax, k_atoms)
        processed += 1
        for par in parents[h]:
            pending[par] -= 1
            if pending[par] == 0:
                q.append(par)

    # Cycle handling. A node with an uncomputed child never has its pending
    # count reach zero, so strandedness PROPAGATES UP: left alone, one
    # transposition cycle anywhere gives the ROOT a flat curve and an empty
    # book. (Observed for real: max_ply 14 on the <=2024 pool stranded 185
    # nodes and reported root capacity 0.) So flatten cycle nodes one at a
    # time — cheapest by reach, i.e. least consequential — and resume draining
    # after each, which recomputes every ancestor properly.
    def stop_value(h: int) -> float:
        if h in g.our:
            return g.our[h].l_node
        n = g.opp[h]
        return n.const_base + sum(
            p * (g.our[k].l_node if (in_s and k in g.our) else lv)
            for p, k, in_s, lv in n.kids)

    # The `pending` counters from the drain above are still exactly "children
    # without curves" (duplicate edges are counted symmetrically in pending and
    # in parents, see _adjacency), so KEEP them rather than rescanning. The old
    # version rebuilt `remaining` and `ready` from all_nodes on every single
    # flatten, i.e. O(N + E) per stranded node: measured 2026-08-30 on the
    # b=1000 build, 1,759 stranded over 2.1M nodes spent ~1 h of an 86 min run
    # in this loop. Draining incrementally is the same sequence of operations
    # (right after the drain no node can be ready, so both versions flatten
    # first, then compute whatever that unblocks) at O((N + E) log N) total.
    # Tie-break is now the node hash instead of set-iteration order, which the
    # old `min` left arbitrary -- ties are common because an our-node inherits
    # its parent opponent node's reach exactly.
    remaining = all_nodes - curves.keys()
    heap = [(g.reach.get(h, 0.0), h) for h in remaining]
    heapq.heapify(heap)
    flattened = 0
    while remaining:
        victim = None
        while heap:
            _r, cand = heapq.heappop(heap)
            if cand in remaining:
                victim = cand
                break
        if victim is None:
            break
        curves[victim] = flat_curve(stop_value(victim))
        remaining.discard(victim)
        flattened += 1
        for par in parents[victim]:
            pending[par] -= 1
            if pending[par] == 0 and par in remaining:
                q.append(par)
        while q:
            h = q.popleft()
            if h not in remaining:
                continue
            curves[h] = _node_curve(g, h, curves, bmax, k_atoms)
            remaining.discard(h)
            for par in parents[h]:
                pending[par] -= 1
                if pending[par] == 0 and par in remaining:
                    q.append(par)

    diag = {"nodes": len(all_nodes), "stranded_cycle_nodes": flattened}
    return curves, diag


def _node_curve(g: Graph, h: int, curves: dict[int, Curve], bmax: int,
                k_atoms: int) -> Curve:
    if h in g.our:
        n = g.our[h]
        cands = n.cands
        pts: dict[int, float] = {0: n.l_node}
        keys: dict[int, float] = {0: n.l_node}     # selection keys (== pts unless sticky)
        im, margin = incumbent_move(g, h)
        for san, ch, in_sub, leaf_val in cands:
            bonus = _bonus(san, im, margin)
            if in_sub and ch is not None and ch in curves:
                c = curves[ch]
                vs = [(1, c.base)] + [
                    (1 + int(cc), c.base + float(cg))
                    for cc, cg in zip(c.cum_costs, c.cum_gains)]
            else:
                vs = [(1, leaf_val)]
            for b, v in vs:
                if b > bmax:
                    break
                k = v + bonus
                if b not in keys or k > keys[b]:
                    keys[b] = k
                    pts[b] = v
        return cap_curve(curve_from_points(sorted(pts.items())), bmax, k_atoms)
    n = g.opp[h]
    const = n.const_base
    ch_curves: list[tuple[float, Curve]] = []
    for p, ch, in_sub, leaf_val in n.kids:
        if in_sub and ch is not None and ch in curves:
            ch_curves.append((p, curves[ch]))
        else:
            const += p * leaf_val
    return merge_weighted(ch_curves, const, bmax, k_atoms)


def exact_opp_curve(g: Graph, h: int, curves: dict[int, Curve],
                    bmax: int) -> np.ndarray:
    """Debug-mode exact sup-convolution at one opponent node over a dense
    integer grid 0..bmax (numpy max-plus). Used to MEASURE the hull gap."""
    n = g.opp[h]
    grid = np.full(bmax + 1, n.const_base, dtype=np.float64)
    for p, ch, in_sub, leaf_val in n.kids:
        if in_sub and ch is not None and ch in curves:
            c = curves[ch]
            child_vals = np.array([c.eval(b) for b in range(bmax + 1)]) * p
        else:
            child_vals = np.full(bmax + 1, leaf_val * p)
        new = np.full(bmax + 1, -np.inf)
        for b in range(bmax + 1):
            best = grid[:b + 1] + child_vals[b::-1]
            new[b] = best.max()
        grid = new
    return grid


# ── extraction ──────────────────────────────────────────────────────────────

def match_distinct(g: Graph, curves: dict[int, Curve], target: int,
                   bmax: int,
                   probe_curves: dict[int, Curve] | None = None,
                   scan_cap: int = 64, plain_bmax: int | None = None
                   ) -> tuple[dict, int, bool, int]:
    """Extract a book holding EXACTLY `target` DISTINCT decisions where one is
    reachable, and at least `target` otherwise.

    A book asked for N decisions delivers fewer. Measured on the <=2024 white
    pool: 17 of 20, 305 of 400, 817 of 1000 (15%, 24%, 18%). The truncation
    baseline has no such gap and always books its full budget, so scoring
    "b=400 dp" against "b=400 trunc" compares 305 memorised moves against 400 --
    size rather than method, and biased AGAINST the DP.

    WHY THE GAP EXISTS -- corrected 2026-09-08, this docstring had it wrong.
    It used to say the curves charge a transposed position once per PATH while
    extraction books it once, so the spend inflates past the footprint. That is
    not what happens: across all 104 books ever written to _budget (the two
    that produced the numbers above included) `spent_paths == spent_distinct`
    in EVERY one, so no booked set has ever contained a node reached by two
    booked paths and per-path charging has never inflated one. The gap is
    UNSPENT budget, not double-charged budget. Atoms are indivisible and chain
    compression fuses a prefix and its payoff into a single cost-k atom, so a
    budget that cannot afford the next whole chain leaves the remainder on the
    table -- fixdpC_white_b20 books 13 of 20 with the curve's own value at
    charged equal to the realised value, i.e. the curve agrees there is nothing
    affordable left to buy.

    The distinction matters because it points at a different lever. Per-path
    charging would be fixed by a true DAG-cost DP (the plan's out-of-scope
    item); unspent budget from lumpy atoms is about atom granularity, and this
    flag -- paying a larger charged budget until the next chain becomes
    affordable -- is the direct remedy for it. It also means the flag makes the
    DP buy chains it had priced as not-worth-it at the nominal budget, which is
    correct for an equal-footprint comparison (trunc books its full N whether
    or not the last moves earn their place) but is a real handicap to state
    when the arms are ranked.

    So search for the smallest path-charged budget whose extraction books
    `target` distinct decisions: grow geometrically to bracket it, then bisect.
    Each probe is an extract_book over curves that are already built, so this
    costs extractions, not a rebuild.

    TWO CURVE SETS, AND WHY. `curves` must be built to the caller's PLAIN bmax
    and is what the target extraction uses, so a book that already meets its
    target comes back bit-identical to the same build without this flag -- the
    no-op contract. `probe_curves` (built to a larger bmax) is consulted only
    where the plain set cannot reach. Before 2026-08-31 a single inflated set
    served both and the flag silently changed books it never probed: a hull is
    global, so admitting far points fuses the cheap early atoms away (see the
    module note on curve_from_points and
    scratch/python/match_distinct_repro.py). Passing no probe_curves confines
    the search to the plain set's range.

    `plain_bmax` says how far the plain set reaches, and every probe at or
    below it reads the plain curves rather than the inflated ones -- see
    curves_at(). Without it (before 2026-09-08) EVERY probe above the target
    went to the coarse set even when the fine one covered that budget, which
    cost exactness outright: dp/white b=6 came back charged 12 for nine
    distinct off the coarse hull. Pass it whenever probe_curves is passed.

    Returns (result, charged_budget, hit_target, spent_distinct). hit_target is
    `spent >= target`; compare `spent == target` for an EQUAL-FOOTPRINT claim,
    which is why the realised count is returned rather than left to be re-derived
    -- fixdp_white_b20 booked 21 for a target of 20 and reported success.

    Distinct spend is NOT monotone in the charged budget. It usually steps by
    one (20->17, 21->18, 22->19, 23->20 on the white pool) but the allocator can
    reshuffle: the same b20 run bracketed 30 and 32 while 31 booked FEWER than
    20. Bisection therefore returns a smallest-KNOWN-good budget, not a proven
    minimum.

    THE BRACKET IS WHY THAT USED TO OVERSHOOT, IN TWO WAYS. Probes grow by
    1.5x, so a target of 20 jumps straight to 30 and the bisection then only
    narrows inside [30, 32] -- budgets 21..29 are never extracted. And because
    the count is not monotone, an exact hit sits ABOVE the smallest known-good
    budget as readily as below it, so stopping the search at the bracket misses
    those too. A capped ascending LINEAR scan over the whole of
    [target+1, bmax] now runs whenever the bracket overshoots (it cannot be a
    bisection -- non-monotone), and the smallest charged budget landing exactly
    on the target wins. Past `scan_cap` extra extractions the previous
    smallest-known-good answer stands and the caller still sees
    `spent > target`, so an inexact book stays visible rather than becoming a
    silent unequal-footprint comparison.

    MEASURED, with --scan-distinct on the <=2024 white pool 2026-09-08 -- do
    not re-derive this by intuition, two plausible-sounding guesses about it
    were wrong before it was measured:

      dp/white     charged 12 -> 9 distinct, 14 -> 6, 19 -> 16, 20 -> 17
      fixdp/white  charged  8 -> 5 distinct,  9 -> 7, 19 -> 16, 20 -> 13

    Three things follow. (1) The count FALLS as budget rises -- fixdp/white 19
    books 16 and 20 books 13 -- while root value stays monotone (0.540650 ->
    0.541724), because the DP maximises value and buys one expensive deep chain
    over several cheap shallow ones. Non-monotone footprint is a consequence of
    correct optimisation, not a defect. (2) An exact hit can therefore need a
    LARGER charged budget than the bracket: dp/white target 6 is unreachable
    below 12 but sits at 14, with a higher root value than the 9-move book.
    (3) Some footprints do not exist at all. fixdp/white reaches
    {1,2,3,4,5,7..17,19,21,22} over charged 1..36 -- 6, 18 and 20 are absent,
    so no search can deliver a 6-move or 20-move fixdp/white book and an
    equal-footprint comparison at those rungs is simply not available for that
    arm. Callers must read `spent_distinct` and decide; this function will not
    fabricate a book by trimming one the DP never chose.

    scan_cap 64 covers the {1,2,3,6,10,20} ladder completely -- the widest
    skipped range there is 21..31. It does NOT cover the 400/4000/40000 rung,
    where growth jumps 400 -> 600 and leaves 199 budgets unprobed: the scan
    would reach 464 and stop. Raise the cap or tighten the 1.5x growth when
    that rung is actually run, and size it from a measured distinct-vs-charged
    curve rather than by guessing which end the exact hit sits at.
    """
    # The graph is fixed while the budget and curve set change between probes.
    order = topo_down(g)
    res = extract_book(g, curves, target, order=order)
    if res["spent_distinct"] >= target or target >= bmax:
        return (res, target, res["spent_distinct"] >= target,
                res["spent_distinct"])

    # Probes only, and only above the target -- see the docstring.
    pc = probe_curves if probe_curves is not None else curves

    seen: dict[int, dict] = {}

    def curves_at(b: int) -> dict[int, Curve]:
        """The FINEST curve set that can express budget b.

        Inflating a hull can only coarsen it (the module note on
        curve_from_points), so the plain set is strictly better wherever it
        reaches -- smaller atoms, finer distinct counts, and a better-valued
        book at the same charged budget. Probing everything on the inflated
        set was costing exactness: on the 2026-09-08 white rebuild dp/white
        b=6 came back charged 12 for NINE distinct because the coarse hull's
        counts jumped 3 -> 9, while the plain curves step 6 -> 3 and 20 -> 17
        with room in between for a 6 to exist.

        It also makes `charged` mean the same thing in both book sets: a probe
        at budget b now reads the same curves the flag-OFF build at budget b
        reads, so the matched-CHARGED set and a flag-on book charged the same
        amount are answering the same question.
        """
        if plain_bmax is not None and b <= plain_bmax:
            return curves
        return pc

    def probe_at(b: int) -> dict:
        if b not in seen:
            seen[b] = extract_book(g, curves_at(b), b, order=order)
        return seen[b]

    lo, best_lo = target, res           # books < target
    hi, best_hi = -1, None              # books >= target
    probe = target
    while probe < bmax:
        probe = min(bmax, max(probe + 1, int(probe * 1.5)))
        r = probe_at(probe)
        if r["spent_distinct"] >= target:
            hi, best_hi = probe, r
            break
        lo, best_lo = probe, r
    if best_hi is None:                 # capacity cannot reach the target
        return best_lo, lo, False, best_lo["spent_distinct"]

    while hi - lo > 1:
        mid = (lo + hi) // 2
        r = probe_at(mid)
        if r["spent_distinct"] >= target:
            hi, best_hi = mid, r
        else:
            lo = mid

    # Exactness scan (see docstring). Ascending, so the smallest charged budget
    # that lands exactly wins; a linear walk rather than a bisection because the
    # count is not monotone; capped so the big-budget rung cannot turn into
    # thousands of extractions.
    #
    # The walk runs to BMAX, not to `hi`. Stopping at the bracket was the first
    # version of this and it still missed the case it was written for: because
    # the count is not monotone, an exact hit lives ABOVE the smallest
    # known-good budget as readily as below it. Measured 2026-09-08 on dp/white
    # with --scan-distinct, target 6: the bracket settles at charged 12 booking
    # NINE, budgets 7..11 book 4/3/3/3/3, and charged 14 books exactly 6 -- at
    # a HIGHER root value (0.545478) than the 9-move book it was going to
    # return (0.543017). Paying a larger charged budget to land on the target
    # is the whole point of the flag; the charged number is a knob, the
    # footprint is the deliverable.
    if best_hi["spent_distinct"] > target:
        spent_probes = 0
        for b in range(target + 1, bmax + 1):
            if b not in seen:
                if spent_probes >= scan_cap:
                    break
                spent_probes += 1
            r = probe_at(b)
            if r["spent_distinct"] == target:
                return r, b, True, target

    return best_hi, hi, True, best_hi["spent_distinct"]


def extract_book(g: Graph, curves: dict[int, Curve], budget: int, *,
                 order: list[int] | None = None) -> dict:
    """Top-down allocation of `budget` over the curves; returns the booked
    tree, per-node allocations, realized bottom-up values, and spend stats.

    Our node: book iff the curve strictly beats stopping; move chosen by
    value (mirrors Stage 3). Opponent node: greedy density prefix over child atoms with skip-fit
    (a child whose atom is skipped is closed — atoms are prefixes).
    """
    booked: dict[int, str] = {}
    alloc: dict[int, int] = {0: 0}
    reached: set[int] = {g.root}
    visited: list[int] = []
    spent_paths = 0
    alloc = {g.root: budget}
    # Top-down in topological order so a transposed node's allocation is FINAL
    # (max over every in-path — funding one path funds the position) before its
    # booking decision runs. The naive stack version processed a node at
    # whichever allocation happened to pop first; _test_budget_transpo caught
    # it booking nothing on the zero-alloc path.
    for h in topo_down(g) if order is None else order:
        if h not in reached:
            continue
        b = alloc.get(h, 0)
        visited.append(h)
        if h in g.our:
            n = g.our[h]
            if b < 1:
                continue
            cands = n.cands
            best = None
            im, margin = incumbent_move(g, h)
            for san, ch, in_sub, leaf_val in cands:
                if in_sub and ch is not None and ch in curves:
                    v = curves[ch].eval(max(b - 1, 0))
                else:
                    v = leaf_val
                key = v + _bonus(san, im, margin)
                if best is None or key > best[0]:
                    best = (key, v, san, ch, in_sub)
            if best is None:
                continue
            _key, v, san, ch, in_sub = best
            if v <= n.l_node + 1e-12:
                continue                       # stopping is at least as good
            booked[h] = san
            spent_paths += 1
            if in_sub and ch is not None:
                reached.add(ch)
                alloc[ch] = max(alloc.get(ch, 0), max(b - 1, 0))
        else:
            n = g.opp[h]
            entries = []                       # (density, child idx, atom j, cost)
            for idx, (p, ch, in_sub, _lv) in enumerate(n.kids):
                if not (in_sub and ch is not None and ch in curves):
                    continue
                c = curves[ch]
                for j in range(len(c.costs)):
                    entries.append((float(c.gains[j] * p) / float(c.costs[j]),
                                    idx, j, int(c.costs[j])))
            # density desc; within a child, atom order ascends (prefix property)
            entries.sort(key=lambda t: (-t[0], t[1], t[2]))
            remaining = b
            child_alloc = dict.fromkeys(range(len(n.kids)), 0)
            closed: set[int] = set()
            for dens, idx, _j, cost in entries:
                if idx in closed:
                    continue
                if cost <= remaining:
                    child_alloc[idx] += cost
                    remaining -= cost
                else:
                    closed.add(idx)            # prefix property: no later atoms
            for idx, (p, ch, in_sub, _lv) in enumerate(n.kids):
                if in_sub and ch is not None and ch in curves:
                    reached.add(ch)
                    alloc[ch] = max(alloc.get(ch, 0), child_alloc[idx])

    # realized bottom-up values over the visited set
    realized: dict[int, float] = {}
    for h in reversed(visited):
        if h in g.our:
            n = g.our[h]
            mv = booked.get(h)
            if mv is None:
                realized[h] = n.l_node
                continue
            for san, ch, in_sub, leaf_val in n.cands:
                if san == mv:
                    if in_sub and ch is not None and ch in realized:
                        realized[h] = realized[ch]
                    elif in_sub and ch is not None and ch in curves:
                        realized[h] = curves[ch].eval(alloc.get(ch, 0))
                    else:
                        realized[h] = leaf_val
                    break
        else:
            n = g.opp[h]
            v = n.const_base
            for p, ch, in_sub, lv in n.kids:
                if in_sub and ch is not None and ch in realized:
                    v += p * realized[ch]
                elif in_sub and ch is not None and ch in curves:
                    v += p * curves[ch].eval(0)
                else:
                    v += p * lv
            realized[h] = v

    return {
        "booked": booked, "alloc": alloc, "visited": visited,
        "realized": realized,
        "spent_paths": spent_paths, "spent_distinct": len(booked),
        "root_value_curve": curves[g.root].eval(budget)
        if g.root in curves else None,
        "root_value_realized": realized.get(g.root),
    }


# ── price per move (Lagrangian) ─────────────────────────────────────────────
#
# THE METHOD (2026-10-19). The curve engine above packs each node's options
# into concave value-vs-budget curves and buys their atoms by density. That is
# exact only for diminishing returns, and opening preparation is lumpy: a main
# line's first moves gain almost nothing over the stop value and the payoff sits
# deep, so the curves fuse it into one big atom and the density pass can starve
# it (the b2000 White books left 1...e5 and 1...c5 unprepared). Every patch
# (grid cap, skip-fit, the retired --leftover, match-inflate) fixes a symptom of that.
#
# Price per move replaces the budget with a price `lam` per learned position,
# in root expected-score units, and maximises  value - lam * positions:
#
#   our node u:  V(u) = max( L(u),  max_m V(child_m) - lam / r(u) )
#   opp node v:  V(v) = const + sum_i p_i V(child_i)
#
# r(u) converts the root-unit price into u's local units. On a tree r(u) is the
# product of reply shares on u's path and the recursion is EXACT: the book it
# returns is the best book of its own size (if a book S' with |S'| <= |S| scored
# more, S' would beat S on value - lam * size). Lowering lam grows the book;
# price_search bisects lam onto a target size. Not every size is reachable
# (sizes jump where a whole chain flips in at once), and any size B is bounded
# by  min over the lams tried of  (V_pen(lam) + lam * B),  reported as the gap.
#
# Transpositions: the recipe prices with max(optimistic reach, the source book's
# total reach) (seed_reach), so a shared position the source book takes is charged
# once rather than once per path, and a line it does not take is priced as if it
# were. On a DAG that is an approximation, not a proven optimum; polish_book (a
# coordinate ascent on the reached region, the "DAG polish" section at the end of
# this module) then closes nearly all of the gap to the exact oracle in
# _test_budget_dag.py. The re-pricing rounds and reach fixed points tried before
# polish (plan item 11) were retired on 2026-11-12.

def topo_up(g: Graph) -> tuple[list[int], set[int]]:
    """Bottom-up order (children before parents) for valuation, and the nodes
    placed early to break cycles.

    Strongly connected components come out of Tarjan children-first. A single
    node goes straight in; inside a real cycle, the members are drained in
    dependency order and, when every remaining member waits on the cycle, the
    lowest-reach member is placed early. A node placed early is still valued from
    every child already computed; only its edges INTO the unresolved cycle read
    the child's static value (price_book's back-edge rule), so a repetition move
    (Nf3-g1 back to the start) is cut, not the position and its subtree.

    The cut must fall INSIDE the cycle. Every chess cycle's positions share one
    reach (our moves do not decay it), so a global "lowest reach first" rule ties
    and can pick an ancestor merely waiting on the cycle (in the test, the root:
    nothing was booked). topo_down is not a valuation order at all: it releases
    the HIGHEST-reach survivor first, so in reverse its parents are valued before
    it, and the first real-data price books lost whole main-line subtrees that
    way (0.018 below the curve engine at 400 positions).

    This is _topo_up_dict's algorithm on integer ids (2026-10-29, ~2x faster): the
    same Tarjan, roots taken in the same set-iteration order and children in the
    same list order, so the order and the cut set are identical (pinned by
    _test_budget_speed.py)."""
    all_nodes = set(g.our) | set(g.opp)
    nodes = list(all_nodes)              # Tarjan's roots, in the reference's order
    id_of = {h: i for i, h in enumerate(nodes)}
    get = id_of.get
    our, opp = g.our, g.opp
    succ: list[list[int]] = []
    for h in nodes:
        n = opp.get(h)                   # _adjacency writes opp nodes last
        es = n.kids if n is not None else our[h].cands
        succ.append([c for c in (get(ch) for _a, ch, in_sub, _b in es
                                 if in_sub and ch is not None) if c is not None])
    N = len(nodes)
    index = [-1] * N
    low = [0] * N
    onst = bytearray(N)
    stack: list[int] = []
    order: list[int] = []
    early: set[int] = set()
    counter = 0
    for root in range(N):
        if index[root] >= 0:
            continue
        wn, wc = [root], [0]
        while wn:
            node, ci = wn[-1], wc[-1]
            if ci == 0:
                index[node] = low[node] = counter
                counter += 1
                stack.append(node)
                onst[node] = 1
            kids = succ[node]
            nk = len(kids)
            advanced = False
            while ci < nk:
                ch = kids[ci]
                ci += 1
                if index[ch] < 0:
                    wc[-1] = ci
                    wn.append(ch)
                    wc.append(0)
                    advanced = True
                    break
                if onst[ch] and index[ch] < low[node]:
                    low[node] = index[ch]
            if advanced:
                continue
            wn.pop()
            wc.pop()
            if low[node] == index[node]:
                w = stack.pop()
                onst[w] = 0
                if w == node:
                    order.append(nodes[w])
                else:
                    scc = [w]
                    while w != node:
                        w = stack.pop()
                        onst[w] = 0
                        scc.append(w)
                    _drain_cycle(g, [nodes[x] for x in scc],
                                 {nodes[x]: [nodes[c] for c in succ[x]] for x in scc},
                                 order, early)
            if wn:
                parent = wn[-1]
                if low[node] < low[parent]:
                    low[parent] = low[node]
    return order, early


def _drain_cycle(g: Graph, scc: list[int], succ: dict[int, list[int]], order: list[int],
                 early: set[int]) -> None:
    """topo_up's handling of one multi-node SCC (see its docstring), appending to order."""
    members = set(scc)
    pending = {h: sum(1 for c in succ[h] if c in members) for h in scc}
    rev: dict[int, list[int]] = {h: [] for h in scc}
    for h in scc:
        for c in succ[h]:
            if c in members:
                rev[c].append(h)
    done: set[int] = set()
    q = deque(h for h in scc if pending[h] == 0)
    by_reach = sorted(scc, key=lambda h: (g.reach.get(h, 0.0), h))
    k = 0
    while len(done) < len(scc):
        if not q:
            while by_reach[k] in done:
                k += 1
            early.add(by_reach[k])
            q.append(by_reach[k])
        h = q.popleft()
        if h in done:
            continue
        done.add(h)
        order.append(h)
        for par in rev[h]:
            if par not in done:
                pending[par] -= 1
                if pending[par] <= 0:
                    q.append(par)


def _topo_up_dict(g: Graph) -> tuple[list[int], set[int]]:
    """topo_up's reference implementation: dict adjacency and stage3's _tarjan_sccs, as
    topo_up was before 2026-10-29. Kept for price_search(engine='legacy') and for
    _test_budget_speed.py, which holds topo_up's order and cut set to it."""
    from stage3_backwards_induction import _tarjan_sccs
    children, _parents = _adjacency(g)
    all_nodes = set(g.our) | set(g.opp)
    succ = {h: [c for c in children.get(h, []) if c in all_nodes] for h in all_nodes}
    order: list[int] = []
    early: set[int] = set()
    for scc in _tarjan_sccs(all_nodes, succ):
        if len(scc) == 1:
            order.append(scc[0])
            continue
        members = set(scc)
        pending = {h: sum(1 for c in succ[h] if c in members) for h in scc}
        rev: dict[int, list[int]] = {h: [] for h in scc}
        for h in scc:
            for c in succ[h]:
                if c in members:
                    rev[c].append(h)
        done: set[int] = set()
        q = deque(h for h in scc if pending[h] == 0)
        by_reach = sorted(scc, key=lambda h: (g.reach.get(h, 0.0), h))
        k = 0
        while len(done) < len(scc):
            if not q:
                while by_reach[k] in done:
                    k += 1
                early.add(by_reach[k])
                q.append(by_reach[k])
            h = q.popleft()
            if h in done:
                continue
            done.add(h)
            order.append(h)
            for par in rev[h]:
                if par not in done:
                    pending[par] -= 1
                    if pending[par] <= 0:
                        q.append(par)
    return order, early


def policy_value(g: Graph, moves: dict[int, str], order: list[int],
                 detail: bool = False, *, compiled=None) -> dict:
    """In-model value of FOLLOWING a given book on this graph: at our nodes play the
    book's move when it is one of the node's candidates (else stop, value L), at
    opponent nodes the graph's reply mix. One evaluator for every method's books, so
    their values compare on identical terms (the curve engine's own realized values
    read its curves and overstate its books by 0.01-0.045, measured 2026-10-19).
    `order` = topo_up(g)[0]. Returns {'value', 'played', 'missing'}: the root value,
    how many book positions the walk plays, and book moves that are not candidates
    at their node (gated out, or the position is not a node of this graph).
    compiled: a budget_compiled.CompiledGraph of g (same order) -> the same dict from
    array sweeps."""
    if compiled is not None and compiled.ok:
        import budget_compiled
        return budget_compiled.policy_value(compiled, moves, detail)
    T: dict[int, float] = {}
    missing = 0

    def static(h: int) -> float:
        if h in g.our:
            return g.our[h].l_node
        n = g.opp[h]
        return n.const_base + sum(
            p * (g.our[k].l_node if (in_s and k in g.our) else lv)
            for p, k, in_s, lv in n.kids)

    for h in order:
        if h in g.our:
            n = g.our[h]
            want = moves.get(h)
            t = n.l_node
            if want is not None:
                c = next((c for c in n.cands if c[0] == want), None)
                if c is None:
                    missing += 1
                else:
                    _san, ch, in_sub, lv = c
                    t = (T[ch] if ch in T else static(ch)) if (in_sub and ch is not None) else lv
            T[h] = t
        else:
            n = g.opp[h]
            t = n.const_base
            for p, ch, in_sub, lv in n.kids:
                if in_sub and ch is not None:
                    t += p * (T[ch] if ch in T else static(ch))
                else:
                    t += p * lv
            T[h] = t
    # positions the walk actually plays
    played, stack, seen = 0, [g.root], set()
    while stack:
        h = stack.pop()
        if h in seen:
            continue
        seen.add(h)
        if h in g.our:
            want = moves.get(h)
            c = next((c for c in g.our[h].cands if c[0] == want), None) if want else None
            if c is not None:
                played += 1
                if c[2] and c[1] is not None:
                    stack.append(c[1])
        elif h in g.opp:
            stack.extend(ch for _p, ch, in_sub, _lv in g.opp[h].kids if in_sub and ch is not None)
    out = {"value": T.get(g.root), "played": played, "missing": missing}
    if detail:
        # how often the walk reaches each node (summed over paths), for breakdowns
        out["T"] = T
        out["W"] = policy_reach(g, moves, order)
    return out


def price_book(g: Graph, lam: float, order: list[int],
               reach: dict[int, float] | None = None, *, compiled=None) -> dict:
    """One price-per-move solve at price `lam`. `order` = topo_up(g)[0]
    (children first). Returns extract_book's keys plus root_value_penalized, lam,
    reach_policy. compiled: a budget_compiled.CompiledGraph of g (same order) -> the
    same dict, bit for bit, from array sweeps (this dict walk is the reference)."""
    reach = g.reach if reach is None else reach
    if compiled is not None and compiled.ok:
        import budget_compiled
        return budget_compiled.price_book(compiled, lam, reach)
    V: dict[int, float] = {}     # penalised value (local units)
    T: dict[int, float] = {}     # true value under the chosen policy
    choice: dict[int, str] = {}

    def static(h: int) -> float:
        """A back edge into a cycle: the node's value with nothing booked."""
        if h in g.our:
            return g.our[h].l_node
        n = g.opp[h]
        return n.const_base + sum(
            p * (g.our[k].l_node if (in_s and k in g.our) else lv)
            for p, k, in_s, lv in n.kids)

    for h in order:
        if h in g.our:
            n = g.our[h]
            cands = n.cands
            best = None
            im, margin = incumbent_move(g, h)
            for san, ch, in_sub, lv in cands:
                if in_sub and ch is not None:
                    if ch in V:
                        v, t = V[ch], T[ch]
                    else:
                        v = t = static(ch)
                else:
                    v = t = lv
                key = v + _bonus(san, im, margin)
                if best is None or key > best[0]:
                    best = (key, v, t, san)
            r = reach.get(h, 0.0)
            if best is not None and r > 0 and best[1] - lam / r > n.l_node + 1e-15:
                V[h] = best[1] - lam / r
                T[h] = best[2]
                choice[h] = best[3]
            else:
                V[h] = T[h] = n.l_node
        else:
            n = g.opp[h]
            v = t = n.const_base
            for p, ch, in_sub, lv in n.kids:
                if in_sub and ch is not None:
                    if ch in V:
                        v += p * V[ch]
                        t += p * T[ch]
                    else:
                        s = static(ch)
                        v += p * s
                        t += p * s
                else:
                    v += p * lv
                    t += p * lv
            V[h], T[h] = v, t

    # top-down: what the book reaches, and how often (summed over its paths)
    R: dict[int, float] = {g.root: 1.0}
    visited: list[int] = []
    booked: dict[int, str] = {}
    for h in reversed(order):           # parents first
        r = R.get(h, 0.0)
        if r <= 0:
            continue
        visited.append(h)
        if h in g.our:
            mv = choice.get(h)
            if mv is None:
                continue
            booked[h] = mv
            for san, ch, in_sub, _lv in g.our[h].cands:
                if san == mv:
                    if in_sub and ch is not None:
                        R[ch] = R.get(ch, 0.0) + r
                    break
        else:
            for p, ch, in_sub, _lv in g.opp[h].kids:
                if in_sub and ch is not None:
                    R[ch] = R.get(ch, 0.0) + r * p
    return {"booked": booked, "alloc": {}, "visited": visited,
            "realized": {h: T[h] for h in visited},
            "spent_paths": len(booked), "spent_distinct": len(booked),
            "root_value_curve": None, "root_value_realized": T.get(g.root),
            "root_value_penalized": V.get(g.root), "lam": lam, "reach_policy": R}


def policy_reach(g: Graph, moves: dict[int, str], order: list[int], *,
                 compiled=None) -> dict[int, float]:
    """Total reach of every node (summed over paths) when our side always plays
    `moves` where it is an in-subgraph candidate, and stops elsewhere. `order` =
    topo_up(g)[0]; a cut back edge carries no reach. compiled: as price_book."""
    if compiled is not None and compiled.ok:
        import budget_compiled
        return budget_compiled.policy_reach(compiled, moves)
    R: dict[int, float] = {g.root: 1.0}
    for h in reversed(order):           # parents first
        r = R.get(h, 0.0)
        if r <= 0:
            continue
        if h in g.our:
            want = moves.get(h)
            if want is None:
                continue
            for san, ch, in_sub, _lv in g.our[h].cands:
                if san == want:
                    if in_sub and ch is not None:
                        R[ch] = R.get(ch, 0.0) + r
                    break
        else:
            for p, ch, in_sub, _lv in g.opp[h].kids:
                if in_sub and ch is not None:
                    R[ch] = R.get(ch, 0.0) + r * p
    return R


# price_search's engine='auto' compiles graphs of at least this many nodes. Measured
# 2026-10-29 on big_dag graphs: compiling costs about four dict solves, and a compiled
# solve breaks even with a dict one near 3k nodes (0.4 ms of numpy overhead per solve on
# a 17-node graph against 0.01 ms; 3 ms against 12 ms at 10k nodes; 0.2 s against 1.6 s
# at 1M). Both engines return the same floats, so this is purely a speed choice.
PRICE_COMPILED_MIN_NODES = 5000


def _gc_paused(fn):
    """Run fn with the cyclic garbage collector paused (budget_compiled.gc_paused) unless
    engine='legacy': every full collection rescans the whole graph, and the search
    allocates enough to trigger several. Results cannot depend on collection timing, and
    the search builds no cycles worth collecting (polish_book clears its memos)."""
    import functools

    @functools.wraps(fn)
    def run(*args, **kwargs):
        if kwargs.get("engine") == "legacy":
            return fn(*args, **kwargs)
        import budget_compiled
        with budget_compiled.gc_paused():
            return fn(*args, **kwargs)
    return run


@_gc_paused
def price_search(g: Graph, target: int,
                 steps: int = 40, log=print, *,
                 seed_reach: dict[int, float] | None = None, select_tol: float = 0.01,
                 polish: bool = True, line_k: int = 8, engine: str = "auto",
                 compiled=None) -> tuple[dict, dict]:
    """Bisect the price onto `target` booked positions, keep the best-valued solve whose
    size is within select_tol of the target (else the solve nearest the target), then
    polish it. Returns (result, info): info holds the lam sweep, the chosen lam, the
    nearest sizes either side of the target, and the duality bound on how much more any
    book of `target` positions could score.

    One pricing round. The price recursion is exact only on a tree: on a DAG a position's
    true reach depends on the whole book. Re-pricing rounds, reach fixed points and extra
    polish starts were measured against an exact oracle (plan item 11, 2026-10-28) and
    retired on 2026-11-12: one round plus polish matched four rounds (tag
    archive-2026-11-11 keeps them).
      seed_reach  the base reach to price with instead of the optimistic g.reach (the
                  recipe passes max(optimistic, the source book's policy_reach), so a
                  transposition the book does not take yet is still priced at the reach
                  a full book would give it)
      polish      True (the recipe): polish_book from the selected book, the best-valued
                  solve that fits and the smallest solve over the target; the best
                  polished book is returned (it never exceeds the target). Under
                  g.meta["stick"] polish maximises polish_book's sticky objective J, and
                  the best J is returned. line_k bounds polish_book's line moves.
                  False: the selected solve itself (tests of the price recursion).
      engine      'compiled': every solve, reach walk and polish evaluation runs on a
                  budget_compiled.CompiledGraph (array sweeps; `compiled` passes one already
                  built from this graph, e.g. shared across budgets); 'legacy': the dict walks
                  and _topo_up_dict, kept as the reference; 'auto' (default): compiled from
                  PRICE_COMPILED_MIN_NODES nodes (or when `compiled` is given), else the dict
                  walks. All return the same book, value and sweep bit for bit
                  (_test_budget_speed.py); info['timing'] says where the time went."""
    import time as _time
    if engine not in ("auto", "compiled", "legacy"):
        raise ValueError(f"engine {engine!r}")
    if engine == "auto":
        engine = ("compiled" if compiled is not None
                  or len(g.our) + len(g.opp) >= PRICE_COMPILED_MIN_NODES else "dict")
    t_start = _time.time()
    pool: list[tuple[int, float, dict]] = []   # (size, value, booked) of every solve
    cg = pos = None
    if engine == "legacy":
        order, cut = _topo_up_dict(g)
    elif engine == "dict":
        order, cut = topo_up(g)
    else:
        if compiled is None:
            import budget_compiled
            compiled = budget_compiled.compile_graph(g)
        elif compiled.g is not g:
            raise ValueError("compiled was built from another graph")
        order, cut = compiled.order, compiled.cut
        if compiled.ok:
            cg, pos = compiled, compiled.pos
    t_order = _time.time() - t_start
    t_solves = [0.0]
    if cut:
        log(f"  price search: {len(cut):,} back edges cut in repetition cycles")
    tried: list[tuple[float, int, float, float]] = []  # lam, n, T, Vpen
    near: list[dict] = []               # solves within select_tol of the target
    reach = g.reach if seed_reach is None else seed_reach

    def solve(lam):
        t0 = _time.time()
        res = price_book(g, lam, order, reach, compiled=cg)
        t_solves[0] += _time.time() - t0
        pool.append((res["spent_distinct"], res["root_value_realized"], res["booked"]))
        tried.append((lam, res["spent_distinct"], res["root_value_realized"],
                      res["root_value_penalized"]))
        if abs(res["spent_distinct"] - target) <= select_tol * target:
            near.append(res)
        return res

    def bisect():
        lo, hi = math.log(1e-10), math.log(1.0)   # log lam: N(lo) large, N(hi) ~ 0
        r_hi = solve(math.exp(hi))
        if r_hi["spent_distinct"] >= target:
            return None, r_hi
        r_lo = solve(math.exp(lo))
        if r_lo["spent_distinct"] <= target:
            return r_lo, None
        best_under, best_over = r_hi, r_lo
        for _ in range(steps):
            mid = (lo + hi) / 2
            r = solve(math.exp(mid))
            n = r["spent_distinct"]
            if n == target:
                return r, r
            if n < target:
                hi, best_under = mid, r
            else:
                lo, best_over = mid, r
            if hi - lo < 1e-7:
                break
        return best_under, best_over

    u, o = bisect()
    if near:
        res = max(near, key=lambda r: r["root_value_realized"])
    elif o is None:
        res = u
    elif u is None:
        res = o
    else:
        res = u if target - u["spent_distinct"] <= o["spent_distinct"] - target else o
    polish_info = None
    t_polish = _time.time()
    if polish:
        fits = [e for e in pool if e[0] <= target]
        best_fit = max(fits, key=lambda e: e[1]) if fits else None
        over = [e for e in pool if e[0] > target]
        starts = [res["booked"]]
        if best_fit is not None:
            starts.append(best_fit[2])
        if over:
            starts.append(min(over, key=lambda e: (e[0], -e[1]))[2])
        seen_s: set = set()
        best_p = None
        polish_info = []
        free_lines: dict = {}
        for st in starts:
            key = frozenset(st.items())
            if key in seen_s:
                continue
            seen_s.add(key)
            moves, pst = polish_book(g, st, target, order,
                                     line_k=line_k, compiled=cg, free_lines=free_lines)
            polish_info.append(pst)
            # under stickiness, the best polish OBJECTIVE (value without one)
            if best_p is None or (pst.get("objective", pst["value"])
                                  > best_p[1].get("objective", best_p[1]["value"]) + 1e-15):
                best_p = (moves, pst)
        free_lines.clear()
        res = book_result(g, best_p[0], order, lam=res["lam"], pos=pos,
                          compiled=cg)
    t_polish = _time.time() - t_polish
    ub = min(vp + lam * target for lam, _n, _t, vp in tried)
    info = {"lam": res["lam"], "booked": res["spent_distinct"],
            "under": None if u is None else u["spent_distinct"],
            "over": None if o is None else o["spent_distinct"],
            "value": res["root_value_realized"], "upper_bound": ub,
            "gap": ub - res["root_value_realized"],
            "sweep": [{"lam": lam, "booked": n, "value": t, "round": 1}
                      for lam, n, t, _vp in sorted(tried)],
            "timing": {"engine": ("compiled" if cg is not None
                                  else "legacy" if engine == "legacy" else "dict"),
                       "order_s": round(t_order, 3), "solves_s": round(t_solves[0], 3),
                       "dag_s": round(t_polish, 3),
                       "total_s": round(_time.time() - t_start, 3)}}
    log(f"  price search: lam {res['lam']:.3e} -> {res['spent_distinct']:,} positions "
        f"(target {target:,}; nearest sizes {info['under']} / {info['over']}), "
        f"value {res['root_value_realized']:.5f}, bound at {target:,}: {ub:.5f} "
        f"(gap {info['gap']:.5f}), {len(tried)} solves")
    tm = info["timing"]
    log(f"  price search time ({tm['engine']}): order {tm['order_s']:.1f}s, solves "
        f"{tm['solves_s']:.1f}s, dag mode {tm['dag_s']:.1f}s, total {tm['total_s']:.1f}s")
    if polish:
        info["dag_mode"] = "polish"
        info["polish"] = polish_info
        if polish_info:
            log("  dag polish: " + "; ".join(
                f"{p['start_size']:,} -> {p['size']:,} positions, {p['start_value']:.5f} -> "
                f"{p['value']:.5f} ({p['accepted']} moves, {p['seconds']:.1f}s)"
                for p in polish_info))
    return res, info


# ── DAG polish (plan item 11) ───────────────────────────────────────────────
#
# On a DAG the price recursion misprices transpositions, and on any graph it only
# reaches the sizes on its Lagrangian hull (a chain that flips in whole skips the sizes
# in between). Measured against the exhaustive oracle in _test_budget_dag.py, a local
# search after the price search closes nearly all of both gaps, where re-pricing to a
# reach fixed point alone does not. It works on the REACHED REGION only -- the
# positions the book reaches and its frontier -- so a step costs O(book x branching),
# not O(graph): a book of 2,000 positions on a 5M-node graph evaluates in milliseconds.
#
# Every step's effect is measured exactly (the region evaluator is policy_value
# restricted to what the book reaches, same order, same sums), so the value never goes
# down and the size never exceeds the target; the proposals that pick which steps to
# try are estimates.

def _static_value(g: Graph, h: int) -> float:
    """A node's value with nothing booked below it (price_book's back-edge rule)."""
    if h in g.our:
        return g.our[h].l_node
    n = g.opp[h]
    return n.const_base + sum(
        p * (g.our[k].l_node if (in_s and k in g.our) else lv)
        for p, k, in_s, lv in n.kids)


@dataclass
class _Region:
    """A book on the reached region: W = total reach (policy_reach), T = value
    (policy_value), both only on reached nodes; booked = the reached book moves."""
    W: dict[int, float]
    T: dict[int, float]
    booked: dict[int, str]
    child: dict[int, int | None]       # booked node -> the in-subgraph child its move enters
    value: float
    aux: object = None                 # budget_compiled.RegionAux (compiled evaluator only)

    @property
    def size(self) -> int:
        return len(self.booked)


def _book_region(g: Graph, moves: dict[int, str], pos: dict[int, int]) -> _Region:
    """policy_reach and policy_value of `moves`, computed on the reached nodes only and
    in the same order (pos = index in topo_up's order), so the floats are identical. A
    book move that is not an (allowed) candidate is dropped, as are moves at positions
    the book does not reach: what remains is exactly what the book costs."""
    root = g.root
    W: dict[int, float] = {root: 1.0}
    heap = [(-pos[root], root)]
    booked: dict[int, str] = {}
    child: dict[int, int | None] = {}
    seen = {root}
    while heap:                       # parents first: descending order position
        _np, h = heapq.heappop(heap)
        r = W[h]
        if h in g.our:
            want = moves.get(h)
            if want is None:
                continue
            c = next((c for c in g.our[h].cands if c[0] == want), None)
            if c is None:
                continue
            booked[h] = want
            _san, ch, in_sub, _lv = c
            child[h] = ch if (in_sub and ch is not None) else None
            edges = [(1.0, ch)] if child[h] is not None else []
        else:
            edges = [(p, ch) for p, ch, in_sub, _lv in g.opp[h].kids
                     if in_sub and ch is not None]
        ph = pos[h]
        for p, ch in edges:
            pc = pos.get(ch)
            if pc is None or pc > ph:      # a cut back edge carries no reach
                continue
            W[ch] = W.get(ch, 0.0) + r * p
            if ch not in seen:
                seen.add(ch)
                heapq.heappush(heap, (-pc, ch))
    T: dict[int, float] = {}
    for h in sorted(W, key=pos.__getitem__):     # children first
        if h in g.our:
            if h in booked:
                ch = child[h]
                if ch is None:
                    lv = next(c[3] for c in g.our[h].cands if c[0] == booked[h])
                    T[h] = lv
                else:
                    T[h] = T[ch] if ch in T and pos[ch] < pos[h] else _static_value(g, ch)
            else:
                T[h] = g.our[h].l_node
        else:
            n = g.opp[h]
            t = n.const_base
            for p, ch, in_sub, lv in n.kids:
                if in_sub and ch is not None:
                    t += p * (T[ch] if ch in T and pos[ch] < pos[h] else _static_value(g, ch))
                else:
                    t += p * lv
            T[h] = t
    return _Region(W=W, T=T, booked=booked, child=child, value=T[root])


def _region_compiled(cg, moves: dict[int, str]) -> _Region:
    """_book_region on a CompiledGraph (budget_compiled.region): the same _Region, plus
    the index-keyed view polish's incremental verification reads."""
    import budget_compiled
    W, T, booked, child, value, aux = budget_compiled.region(cg, moves)
    return _Region(W=W, T=T, booked=booked, child=child, value=value, aux=aux)


def book_result(g: Graph, moves: dict[int, str], order: list[int],
                lam: float | None = None,
                pos: dict[int, int] | None = None, compiled=None) -> dict:
    """price_book's result dict for an arbitrary book (what the DAG modes return).
    compiled: a budget_compiled.CompiledGraph of g -> its region evaluator (same floats)."""
    if compiled is not None and compiled.ok:
        reg = _region_compiled(compiled, moves)
        pos = compiled.pos
    else:
        pos = {h: i for i, h in enumerate(order)} if pos is None else pos
        reg = _book_region(g, moves, pos)
    visited = sorted(reg.W, key=lambda h: -pos[h])
    return {"booked": reg.booked, "alloc": {}, "visited": visited,
            "realized": {h: reg.T[h] for h in visited},
            "spent_paths": reg.size, "spent_distinct": reg.size,
            "root_value_curve": None, "root_value_realized": reg.value,
            "root_value_penalized": None, "lam": lam, "reach_policy": reg.W}


def polish_book(g: Graph, moves: dict[int, str], target: int, order: list[int], *,
                line_k: int = 8,
                max_verify: int = 24, max_steps: int = 100_000,
                fill_width: int = 256, max_exchange: int = 16,
                verify_after_hit: int = 4, compiled=None,
                free_lines: dict | None = None) -> tuple[dict, dict]:
    """Candidate (c) of plan item 11: coordinate ascent on a book under a hard size.

    1. TRIM. While the book is over `target`, drop the leaf (a booked position with no
       booked position directly below it) whose drop loses least.
    2. ASCEND. Propose, estimate, then verify the best proposals exactly (at most
       max_verify, and verify_after_hit more once one improves); take the best verified
       improvement within the target; stop when none improves.
         line    at a reached unbooked position, book a LINE of 1..k moves (a move, then
                 at most one continuation per reply) -- a lumpy chain pays only when
                 complete, so single moves cannot find it;
         rebook  at a leaf, replace its move by a line (a switch, or a longer chain);
         switch  at any booked position, another candidate (its old subtree is freed);
         fill    the best combination of lines at distinct reached positions (max-plus
                 over their line tables, the fill_width most promising), up to slack
                 moves -- or slack + d after dropping the d cheapest leaves;
         swap    drop the d cheapest leaves and book a line of up to slack + d moves;
         exchange  drop one of the max_exchange cheapest NON-leaf positions with what it
                 alone reaches, and fill the budget that frees.
    Proposals read a line table built from the current book's values (gains are exact
    for a single move on a DAG: W(u) * (new local value - old)); verification re-runs
    the region evaluator. Returns (moves, stats).

    `moves` may be any book: the price search's own (price_search with polish), or
    a caller's, e.g. the curve engine's (build_budget_books --method dp --polish). A start
    that fits is never made worse; one over the target is trimmed first.

    Stickiness (g.meta["stick"] = (incumbent moves, margin), --incumbent). The objective
    is then
        J(book) = value(book) - margin * sum of W(u) over the CHANGED positions u,
    a changed position being one the book plays where the incumbent's move is a
    candidate (the one incumbent_move would credit) and the book plays another. In u's
    own units a switch away from the incumbent pays only when it is better by more
    than the margin, the rule incumbent_move gives every move choice. A position the
    book drops, or one the incumbent never booked, costs nothing, so whether to book
    a position at all is still decided on true values. (One band differs from
    price_book: there a rival that wins the key is booked when its TRUE value beats
    stopping; here a changed position must beat stopping by the margin too.) Every
    step, the trim and the choice between starts compare J; a margin of 0, or no
    stick, is J = value and the same code path bit for bit.

    compiled: a budget_compiled.CompiledGraph of g (same order). Used only without
    stickiness (a margin > 0 runs the region evaluator below: the incremental verifier,
    _extend_fast and the shared free-line tables carry no churn term). The same steps, the
    same book and the same floats, faster (2026-10-29): regions are evaluated on node
    indices; a proposal is verified incrementally (budget_compiled.quick: only what the
    change can touch is re-evaluated, with the same expressions), and the accepted one's
    region is rebuilt the same way (budget_compiled.advance); static values are
    memoised; a line table is reused from an earlier step when everything it reads (its
    children's values, which replies are booked, the free-line memo entries) is bitwise
    unchanged; and free_lines (a dict shared by the polishes of one search, same graph)
    carries book-free line tables between them (see line_free)."""
    import time as _time
    t0 = _time.time()
    st = g.meta.get("stick") if g.meta else None
    margin = st[1] if st and st[1] else 0.0
    # position -> its incumbent move, where that move is a candidate here
    inc: dict[int, str] = {}
    if margin:
        for h, im in st[0].items():
            if h in g.our and any(c[0].rstrip("+#") == im for c in g.our[h].cands):
                inc[h] = im
    fast = compiled is not None and compiled.ok and not inc
    if fast:
        import budget_compiled
        pos = compiled.pos

        def evaluate(m: dict[int, str]) -> _Region:
            return _region_compiled(compiled, m)

        def static_h(h: int) -> float:
            return compiled.static(pos[h])
    else:
        pos = {h: i for i, h in enumerate(order)}

        def evaluate(m: dict[int, str]) -> _Region:
            return _book_region(g, m, pos)

        def static_h(h: int) -> float:
            return _static_value(g, h)
    eps = 1e-12

    def churn(h: int, san: str) -> float:
        """J's charge, in h's own units, for playing san at h"""
        im = inc.get(h)
        return margin if im is not None and san.rstrip("+#") != im else 0.0

    def obj(rg: _Region) -> float:
        if not inc:
            return rg.value
        return rg.value - sum(rg.W[u] * churn(u, m) for u, m in rg.booked.items() if u in inc)

    reg = evaluate(moves)
    start_size, start_value, start_obj = reg.size, reg.value, obj(reg)
    trims = 0
    accepted: dict[str, int] = {}

    def leaves(rg: _Region) -> list[int]:
        out = []
        for u, ch in rg.child.items():
            if ch is None or ch in g.our:
                out.append(u)
                continue
            if not any(in_s and k in rg.booked for _p, k, in_s, _lv in g.opp[ch].kids):
                out.append(u)
        return out

    def _freed(rg: _Region, u: int) -> int:
        """booked positions reached only through u (u included): walk u's cone carrying
        the reach that enters through u; a node is freed when that is all of its reach"""
        through = {u: rg.W[u]}
        heap = [(-pos[u], u)]
        n = 0
        while heap:
            _np, h = heapq.heappop(heap)
            r = through[h]
            if h in g.our:
                if h not in rg.booked:
                    continue
                if r >= rg.W[h] * (1 - 1e-9):
                    n += 1
                edges = [(1.0, rg.child[h])] if rg.child[h] is not None else []
            else:
                edges = [(p, ch) for p, ch, in_s, _lv in g.opp[h].kids if in_s and ch is not None]
            for p, ch in edges:
                if ch in rg.W and pos[ch] < pos[h]:
                    if ch not in through:
                        heapq.heappush(heap, (-pos[ch], ch))
                    through[ch] = through.get(ch, 0.0) + r * p
        return n

    def _cheapest(drops: list, adds: tuple, need: int) -> list | None:
        """the `need` cheapest leaves not among the positions `adds` books"""
        added = {h for h, _m in adds}
        out = []
        for lo, d in drops:
            if d not in added:
                out.append((lo, d))
                if len(out) == need:
                    return out
        return None

    def loss(rg: _Region, u: int) -> float:
        """what dropping leaf u costs in J"""
        return rg.W[u] * (rg.T[u] - churn(u, rg.booked[u]) - g.our[u].l_node)

    while reg.size > target:
        lv = leaves(reg)
        if not lv:                    # only in a cycle of booked positions
            lv = list(reg.booked)
        u = min(lv, key=lambda x: (loss(reg, x), pos[x]))
        m = dict(reg.booked)
        del m[u]
        reg = evaluate(m)
        trims += 1

    def val(rg: _Region, h: int) -> float:
        """the value of entering h under book rg (estimate on a cut back edge)"""
        if h in rg.T:
            return rg.T[h]
        if h in g.our:
            return g.our[h].l_node
        n = g.opp[h]
        t = n.const_base
        for p, k, in_s, lv in n.kids:
            if in_s and k is not None:
                t += p * (rg.T[k] if k in rg.T else static_h(k))
            else:
                t += p * lv
        return t

    # Line tables. Only the TOP level reads the current book (the move's own child and
    # which replies are already booked); below that, values are book-free (stop values,
    # _static_value) so one memo serves every step of the run -- rebuilding the whole
    # depth-k cone each step was O(graph) per step on big graphs. A reply whose
    # optimistic reach is under `floor` (half the cheapest booked leaf's worth) is not
    # explored: a move there cannot pay for the position it costs.
    floor = 0.0
    if reg.booked:
        lv0 = [loss(reg, u) for u in leaves(reg)]
        floor = 0.5 * min((x for x in lv0 if x > 0), default=0.0)
    free_memo: dict[int, tuple] = free_lines if (fast and free_lines is not None) else {}

    def _extend(k: int, K: int, depth: int, base_of, skip, sub_of) -> list:
        """best[j] = (local value, moves) with at most j new moves starting at our node
        k (treated as unbooked); best[0] is stopping. Max-plus at opponent nodes over
        the cone unfolded as a tree (a transposition inside it is charged per path --
        an estimate; verification is exact)."""
        best: list = [(g.our[k].l_node, ())] + [None] * K
        for san, ch, in_sub, lv in g.our[k].cands:
            cs = churn(k, san)
            if not (in_sub and ch is not None):
                acc = [None, (lv - cs, ((k, san),))]
            elif ch in g.our:
                acc = [None, (base_of(ch) - cs, ((k, san),))]
            else:
                acc = [None, (base_of(ch) - cs, ((k, san),))] + [None] * (K - 1)
                if K > 1 and depth < 2 * line_k:
                    share: dict[int, float] = {}
                    for p, kk, in_s, _lv in g.opp[ch].kids:
                        if (in_s and kk in g.our and kk != k and not skip(kk)
                                and g.reach.get(kk, 0.0) >= floor):
                            share[kk] = share.get(kk, 0.0) + p
                    for kk, sh in share.items():
                        sub = sub_of(kk, K - 1, depth + 1)
                        stop = g.our[kk].l_node
                        gains = [(j, sh * (sub[j][0] - stop), sub[j][1])
                                 for j in range(1, K) if sub[j] is not None
                                 and sub[j][0] > stop + eps and sub[j] is not sub[j - 1]]
                        if not gains:
                            continue
                        nxt = list(acc)
                        for j1 in range(1, K + 1):
                            if acc[j1] is None:
                                continue
                            for j2, gn, mv in gains:
                                j = j1 + j2
                                if j <= K and (nxt[j] is None
                                               or acc[j1][0] + gn > nxt[j][0]):
                                    nxt[j] = (acc[j1][0] + gn, acc[j1][1] + mv)
                        acc = nxt
            for j in range(1, min(len(acc), K + 1)):
                if acc[j] is not None and (best[j] is None or acc[j][0] > best[j][0]):
                    best[j] = acc[j]
        for j in range(1, K + 1):             # at most j moves
            if best[j] is None or (best[j - 1] is not None and best[j - 1][0] > best[j][0]):
                best[j] = best[j - 1]
        return best

    our_d, opp_d, reach_get = g.our, g.opp, g.reach.get

    def _extend_fast(k: int, K: int, depth: int, base_of, skip, sub_of, iv=None) -> list:
        """_extend with the iterations that cannot update anything skipped (gains come in
        ascending length, so once j1 + j2 > K the rest of the row is dead; j1 = K never
        fits) and skip=None for "nothing booked". Same tables, same objects. iv (free
        tables): [lo, hi] narrowed to the floors that make the same reach decisions over
        the explored cone -- the largest reach left out, the smallest let in."""
        best: list = [(our_d[k].l_node, ())] + [None] * K
        for san, ch, in_sub, lv in g.our[k].cands:
            if not (in_sub and ch is not None):
                acc = [None, (lv, ((k, san),))]
            elif ch in our_d:
                acc = [None, (base_of(ch), ((k, san),))]
            else:
                acc = [None, (base_of(ch), ((k, san),))] + [None] * (K - 1)
                if K > 1 and depth < 2 * line_k:
                    share: dict[int, float] = {}
                    for p, kk, in_s, _lv in opp_d[ch].kids:
                        if (in_s and kk in our_d and kk != k
                                and (skip is None or not skip(kk))):
                            r = reach_get(kk, 0.0)
                            if r >= floor:
                                share[kk] = share.get(kk, 0.0) + p
                                if iv is not None and r < iv[1]:
                                    iv[1] = r
                            elif iv is not None and r > iv[0]:
                                iv[0] = r
                    for kk, sh in share.items():
                        sub = sub_of(kk, K - 1, depth + 1)
                        if iv is not None:
                            e = free_memo[kk]
                            if e[2] > iv[0]:
                                iv[0] = e[2]
                            if e[3] < iv[1]:
                                iv[1] = e[3]
                        stop = our_d[kk].l_node
                        gains = [(j, sh * (sub[j][0] - stop), sub[j][1])
                                 for j in range(1, K) if sub[j] is not None
                                 and sub[j][0] > stop + eps and sub[j] is not sub[j - 1]]
                        if not gains:
                            continue
                        nxt = list(acc)
                        for j1 in range(1, K):
                            a = acc[j1]
                            if a is None:
                                continue
                            for j2, gn, mv in gains:
                                j = j1 + j2
                                if j > K:
                                    break
                                v = a[0] + gn
                                c = nxt[j]
                                if c is None or v > c[0]:
                                    nxt[j] = (v, a[1] + mv)
                        acc = nxt
            for j in range(1, min(len(acc), K + 1)):
                if acc[j] is not None and (best[j] is None or acc[j][0] > best[j][0]):
                    best[j] = acc[j]
        for j in range(1, K + 1):             # at most j moves
            if best[j] is None or (best[j - 1] is not None and best[j - 1][0] > best[j][0]):
                best[j] = best[j - 1]
        return best

    def line_free(k: int, K: int, depth: int) -> list:
        """Book-free line table of k for at most K moves, memoised. A table built for
        more moves serves fewer: entries j <= K of a K'-table (K' > K) equal the K-table's,
        values, moves and aliasing, because the max-plus fills entry j only from pairs
        j1 + j2 = j visited in the same order, and the "at most j" pass reads entries <= j
        (by induction over the cone; the depth cap never binds, K falls by one per level
        from line_k). Compiled: the memo may come from another polish of the same search,
        with another floor, so an entry also carries the floors (lo, hi] that make the
        same reach decisions over its cone, and serves only a floor inside them."""
        got = free_memo.get(k)
        if fast:
            if got is not None and got[0] >= K and got[2] < floor <= got[3]:
                return got[1]
            iv = [-math.inf, math.inf]
            best = _extend_fast(k, K, depth, static_h, None, line_free, iv)
            free_memo[k] = (K, best, iv[0], iv[1])
            return best
        if got is not None and got[0] >= K:
            return got[1]
        best = _extend(k, K, depth, static_h, lambda h: False, line_free)
        free_memo[k] = (K, best)
        return best

    # Compiled only: line tables kept across steps, keyed (k, K). The top level of
    # _extend(k, K, 0, val(rg, .), rg.booked, line_free) reads, besides the fixed graph,
    # floor: val(rg, ch) of each in-subgraph candidate child (rg.T[ch], or
    # for a child outside the region its replies' rg.T), whether each reply it would
    # explore is booked, and the free-line memo entry of each unbooked one. After every
    # step, positions whose region value/membership (tchg) or booked status (bchg) changed
    # are stamped with the step that first sees the change; a kept table is reused while
    # nothing it read has a later stamp and its memo entries are the same objects -- its
    # inputs are then bitwise those it was built from, so _extend would rebuild it equal.
    # (The changed positions come from budget_compiled.advance's delta.)
    line_keep: dict[tuple[int, int], tuple] = {}
    tchg: dict[int, int] = {}
    bchg: dict[int, int] = {}
    step_now = [0]

    def line_deps(rg: _Region, k: int, K: int) -> tuple[list, list, list]:
        vdeps, rdeps, mdeps = [], [], []
        for _san, ch, in_sub, _lv in g.our[k].cands:
            if not (in_sub and ch is not None):
                continue
            vdeps.append(ch)
            if ch in our_d:
                continue
            kids = opp_d[ch].kids
            if ch not in rg.T:
                vdeps.extend(kk for _p, kk, in_s, _lv2 in kids if in_s and kk is not None)
            if K <= 1:
                continue
            for _p, kk, in_s, _lv2 in kids:
                if in_s and kk in our_d and kk != k and reach_get(kk, 0.0) >= floor:
                    rdeps.append(kk)
                    if kk not in rg.booked:
                        mdeps.append((kk, free_memo.get(kk)))
        return vdeps, rdeps, mdeps

    def kept_ok(kept: tuple) -> bool:
        s0, vdeps, rdeps, mdeps, _best = kept
        tget, bget, fget = tchg.get, bchg.get, free_memo.get
        for x in vdeps:
            if tget(x, -1) > s0:
                return False
        for x in rdeps:
            if bget(x, -1) > s0:
                return False
        for kk, e in mdeps:
            if fget(kk) is not e:
                return False
        return True

    def changes(old: _Region, new: _Region) -> tuple[set, set]:
        """(value or membership changed, booked status changed) by a full diff: what
        budget_compiled.advance reports from its delta (checked under CHECK_ADVANCE)"""
        same = budget_compiled._same
        oT, nT, ob, nb = old.T, new.T, old.booked, new.booked
        tc = {h for h, t in nT.items() if h not in oT or not same(oT[h], t)}
        tc |= {h for h in oT if h not in nT}
        return tc, {h for h in nb if h not in ob} | {h for h in ob if h not in nb}

    def line_table(rg: _Region):
        memo: dict[int, tuple[int, list]] = {}

        def line(k: int, K: int, depth: int) -> list:
            got = memo.get(k)
            if got is not None and got[0] >= K:
                return got[1]
            if fast and depth == 0:
                kept = line_keep.get((k, K))
                if kept is not None and kept_ok(kept):
                    memo[k] = (K, kept[4])
                    return kept[4]
            best = (_extend_fast if fast else _extend)(
                k, K, depth, lambda h: val(rg, h), rg.booked.__contains__, line_free)
            if fast and depth == 0:
                line_keep[(k, K)] = (step_now[0], *line_deps(rg, k, K), best)
            memo[k] = (K, best)
            return best
        return line

    steps = 0
    while steps < max_steps:
        steps += 1
        step_now[0] = steps
        slack = target - reg.size
        line = line_table(reg)
        # (estimated gain, kind, moves to add, positions to drop); built only if verified
        props: list[tuple[float, str, tuple, tuple]] = []
        kmax = line_k
        lvs = leaves(reg)
        leafset = set(lvs)
        drops = sorted(((loss(reg, u), u) for u in lvs), key=lambda t: (t[0], pos[t[1]]))
        # lines at the frontier
        lines: list[tuple[float, int, tuple]] = []      # (gain, length, moves)
        ftab: dict[int, list] = {}                       # frontier -> [(length, gain, moves)]
        for u, w in reg.W.items():
            if u not in g.our or u in reg.booked or w <= 0:
                continue
            best = line(u, kmax, 0)
            stop = g.our[u].l_node
            prev = None
            for j in range(1, kmax + 1):
                b = best[j]
                if b is None or not b[1] or b is prev:
                    continue
                prev = b
                gain = w * (b[0] - stop)
                if gain > eps:
                    lines.append((gain, len(b[1]), b[1]))
                    ftab.setdefault(u, []).append((len(b[1]), gain, b[1]))
        fill_from = sorted(ftab, key=lambda u: -max(t[1] for t in ftab[u]))[:fill_width]
        lines.sort(key=lambda t: -t[0])
        # fill: the best COMBINATION of lines at distinct frontier positions (max-plus
        # over the frontier's tables) -- two independent single moves can beat one
        # two-move line, which best-first over single proposals never sees
        comb: list = [(0.0, ())] + [None] * kmax
        for u in fill_from:
            tab = ftab[u]
            nxt = list(comb)
            for j1 in range(kmax):
                if comb[j1] is None:
                    continue
                for j2, gn, mv in tab:
                    j = j1 + j2
                    if j <= kmax and (nxt[j] is None or comb[j1][0] + gn > nxt[j][0]):
                        nxt[j] = (comb[j1][0] + gn, comb[j1][1] + mv)
            comb = nxt
        if fast:
            # _cheapest(drops, adds, need) is the plain prefix drops[:need] whenever no
            # position of adds is a leaf, so its sum and tuple are memoised per need (the
            # same expressions over the same elements)
            drop_set = {d for _lo, d in drops}
            prefix: dict[int, tuple] = {}

            def cheapest(adds: tuple, need: int) -> tuple | None:
                """(sum of the losses, dropped positions) of _cheapest(drops, adds, need)"""
                for h, _m in adds:
                    if h in drop_set:
                        break
                else:
                    got = prefix.get(need)
                    if got is None:
                        avail = drops[:need]
                        got = prefix[need] = (
                            (None,) if len(avail) < need else
                            (sum(lo for lo, _d in avail), tuple(d for _lo, d in avail)))
                    return None if got[0] is None else got
                avail = _cheapest(drops, adds, need)
                return None if avail is None else (sum(lo for lo, _d in avail),
                                                   tuple(d for _lo, d in avail))
        for j in range(1, kmax + 1):
            c = comb[j]
            if c is None or c[0] <= eps or (comb[j - 1] is not None and c is comb[j - 1]):
                continue
            n_new = len(dict(c[1]))
            if n_new <= slack:
                props.append((c[0], "fill", c[1], ()))
            elif fast:
                ch_ = cheapest(c[1], n_new - slack)
                if ch_ is not None and c[0] - ch_[0] > eps:
                    props.append((c[0] - ch_[0], "fill", c[1], ch_[1]))
            else:
                avail = _cheapest(drops, c[1], n_new - slack)
                if avail is not None and c[0] - sum(lo for lo, _d in avail) > eps:
                    props.append((c[0] - sum(lo for lo, _d in avail), "fill", c[1],
                                  tuple(d for _lo, d in avail)))
        # exchange: drop a NON-leaf (its exclusively reached subtree goes with it) and
        # fill the freed budget; moves below it stay in the dict, so whatever the fill
        # re-reaches keeps them (the evaluator prunes the rest and counts the true size)
        nonleaf = sorted((loss(reg, u), u) for u in reg.booked if u not in leafset)
        for lo, u in nonleaf[:max_exchange]:
            freed = _freed(reg, u)
            for j in range(1, min(kmax, slack + freed) + 1):
                c = comb[j]
                if c is None or (comb[j - 1] is not None and c is comb[j - 1]):
                    continue
                props.append((c[0] - lo, "exchange", c[1], (u,)))
        for gain, n_new, mv in lines:
            if n_new <= slack:
                props.append((gain, "line", mv, ()))
            elif fast:
                ch_ = cheapest(mv, n_new - slack)
                if ch_ is not None and gain - ch_[0] > eps:
                    props.append((gain - ch_[0], "swap", mv, ch_[1]))
            else:                                          # swap: free n_new - slack leaves
                avail = _cheapest(drops, mv, n_new - slack)
                if avail is not None:
                    lost = sum(lo for lo, _d in avail)
                    if gain - lost > eps:
                        props.append((gain - lost, "swap", mv, tuple(d for _lo, d in avail)))
        # rebook a leaf with a line (including a plain switch of its move)
        for lo, u in drops:
            best = line(u, min(line_k, slack + 1), 0)
            for j in range(1, len(best)):
                b = best[j]
                if b is None or not b[1]:
                    continue
                gain = reg.W[u] * (b[0] - (reg.T[u] - churn(u, reg.booked[u])))
                if gain > eps and len(b[1]) <= slack + 1:
                    props.append((gain, "rebook", b[1], ()))
        # switch a non-leaf (frees its subtree, books one move)
        for u, w in reg.booked.items():
            if u in leafset:
                continue
            for san, ch, in_sub, lv in g.our[u].cands:
                if san == w:
                    continue
                nv = val(reg, ch) if (in_sub and ch is not None) else lv
                gain = reg.W[u] * ((nv - churn(u, san)) - (reg.T[u] - churn(u, w)))
                if gain > eps:
                    props.append((gain, "switch", ((u, san),), ()))
        if inc:
            # drop: a changed leaf whose move beats stopping by less than the margin
            # raises J when dropped (no stick, no drop proposals: the moves are as before)
            for lo, u in drops:
                if lo < -eps:
                    props.append((-lo, "drop", (), (u,)))
        props.sort(key=lambda t: -t[0])
        took = None
        left = max_verify
        cur = obj(reg)
        for _gain, kind, adds, dels in props:
            if left <= 0:
                break
            left -= 1
            m = {**reg.booked, **dict(adds)}
            for d in dels:
                m.pop(d, None)
            if fast:              # no stickiness here, so J is the value
                changed = [h for h, _mv in adds] + list(dels)
                size, value = budget_compiled.quick(
                    compiled, reg.aux, reg.booked, reg.size, reg.value, m, changed)
                new, j = (m, size, value, changed), value
            else:
                rg = _book_region(g, m, pos)
                new, j = (rg, rg.size, rg.value), obj(rg)
            if new[1] <= target and j > (took[2] if took else cur) + eps:
                if took is None:
                    left = min(left, verify_after_hit)
                took = (new, kind, j)
        if took is None:
            break
        if fast:
            m, size, value, changed = took[0]
            prev = reg
            W2, T2, b2, c2, v2, aux2 = budget_compiled.advance(
                compiled, reg.W, reg.T, reg.booked, reg.child, reg.value, reg.aux, m, changed)
            reg = _Region(W=W2, T=T2, booked=b2, child=c2, value=v2, aux=aux2)
            for h in aux2.tchg:
                tchg[h] = steps + 1
            for h in aux2.bchg:
                bchg[h] = steps + 1
            if reg.size != size or not budget_compiled._same(reg.value, value):
                raise AssertionError(
                    f"polish_book: incremental verification {size} / {value!r} != the "
                    f"rebuilt region's {reg.size} / {reg.value!r}; rerun with "
                    f"--price-engine legacy and report the graph")
            if budget_compiled.CHECK_ADVANCE:
                bad = budget_compiled.region_diff(evaluate(m), reg)
                tc, bcg = changes(prev, reg)
                if tc != set(aux2.tchg) or bcg != set(aux2.bchg):
                    bad.append("change stamps")
                if bad:
                    raise AssertionError(f"polish_book: incremental region != full: {bad}")
        else:
            reg = took[0][0]
        accepted[took[1]] = accepted.get(took[1], 0) + 1
    stats = {"start_size": start_size, "start_value": start_value, "size": reg.size,
             "value": reg.value, "trims": trims, "accepted": sum(accepted.values()),
             "by_kind": accepted, "steps": steps, "seconds": _time.time() - t0}
    if inc:
        ch = [u for u, m in reg.booked.items() if churn(u, m)]
        stats.update(start_objective=start_obj, objective=obj(reg), margin=margin,
                     changed=len(ch), reach_changed=float(sum(reg.W[u] for u in ch)))
    if fast:              # line_free's self-reference would keep them until a GC pass
        if free_lines is None:
            free_memo.clear()
        line_keep.clear()
    return dict(reg.booked), stats
