"""Budget-constrained repertoire selection: the library behind build_budget_books.py.

THE PROBLEM
-----------
Stage 3's book is unbounded: every gated our-turn node gets a move. All its
learning costs (memo_cost, crush gamma, learnability deltas) are REACH-WEIGHTED —
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
rate. The opponent-node allocation is then a greedy density merge, which is the
exact sup-convolution for concave inputs (classic exchange argument); the
duality gap is localized to at most one straddling atom per node and is
measured, not assumed (--exact-alloc in the CLI, T3/T7 tests).

PERSPECTIVE CONVENTION
----------------------
Internally EVERYTHING is our-perspective expected score in [0, 1], higher =
better for the side whose book is being built. White-perspective inputs
(white_score_avg, evals) are flipped once at load for black; outputs are
flipped back. This removes every sign branch from the curve algebra.

KNOWN v1 APPROXIMATIONS (documented, measured — see the plan)
-------------------------------------------------------------
- Curves carry pure VALUE; the frozen unconstrained crush term enters only the
  extraction argmax (mirroring Stage 3, which selects by key but propagates
  value). Curve-vs-realized value is reported per book.
- Transposition budget cost is charged PER PATH (conservative overcount);
  realized distinct decisions are reported alongside.
- Cycle-leftover nodes get a flat L curve (counted, logged).

This module deliberately has no CLI; build_budget_books.py wraps it. Tests
(_test_budget_*.py) import it directly, following the repo convention of
importing production logic rather than copying it.
"""
from __future__ import annotations

import heapq
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

from stage3_backwards_induction import (  # noqa: E402  (path insert above)
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
                  gate_rel_own_margin: float = GATE_REL_OWN_MARGIN
                  ) -> list[bool]:
    """Stage 3's relative refutation gate (L1208-1263), own-eval baseline.
    Returns a keep-mask; uncovered candidates (None) are exempt (kept)."""
    if gate_rel_floor >= 1.0 or not cand_evals_ws:
        return [True] * len(cand_evals_ws)
    elig = [e for e in cand_evals_ws if e is not None]
    if not elig:
        return [True] * len(cand_evals_ws)
    base_rv = None
    if own_eval_ws is not None:
        base_rv = (own_eval_ws - gate_rel_own_margin if our_white
                   else own_eval_ws + gate_rel_own_margin)
    if base_rv is None and len(cand_evals_ws) < 2:
        return [True] * len(cand_evals_ws)
    if our_white:
        best = max(elig)
        if base_rv is not None:
            best = max(best, base_rv)
        return [e is None or e >= best - gate_rel_floor
                for e in cand_evals_ws]
    best = min(elig)
    if base_rv is not None:
        best = min(best, base_rv)
    return [e is None or e <= best + gate_rel_floor for e in cand_evals_ws]


# ── graph ───────────────────────────────────────────────────────────────────

@dataclass
class OurNode:
    l_node: float                      # our-perspective stop value
    # candidates: (san, child_hash or None, in_subgraph, leaf_val, crush_bonus)
    # leaf_val is the our-perspective flat value used when the child is not a
    # subgraph node (dead end / beyond ply cap); crush_bonus is the frozen
    # cw*line_crush term, our-perspective, used ONLY in the extraction argmax.
    cands: list[tuple[str, int | None, bool, float, float]] = field(
        default_factory=list)


@dataclass
class OppNode:
    const_base: float                  # below-floor replies + aux buckets
    # kids: (p, child_hash, in_subgraph, leaf_val)
    kids: list[tuple[float, int | None, bool, float]] = field(
        default_factory=list)


@dataclass
class Graph:
    root: int
    our: dict[int, OurNode]
    opp: dict[int, OppNode]
    epd: dict[int, str]
    ply: dict[int, int]
    reach: dict[int, float]
    meta: dict = field(default_factory=dict)

    def side(self, h: int) -> str:
        return "our" if h in self.our else "opp"


def optimistic_reach(edges_by_parent: dict[int, list[dict]], root: int,
                     root_our: bool, eps: float, max_ply: int,
                     share_floor: float) -> tuple[dict[int, float],
                                                  dict[int, int]]:
    """Policy-free reach upper bound: max over paths of the product of opponent
    reply shares; our moves carry probability 1. Max-product Dijkstra (all
    factors <= 1, so first finalization is maximal); lazy-deletion heap."""
    import heapq
    reach: dict[int, float] = {}
    ply: dict[int, int] = {}
    heap = [(-1.0, 0, root)]
    ply[root] = 0
    tie = 1
    while heap:
        nr, _, h = heapq.heappop(heap)
        r = -nr
        if h in reach:
            continue
        reach[h] = r
        p_ply = ply[h]
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
                cr = r
            else:
                share = e["total"] / tot if tot else 0.0
                if share < share_floor:
                    continue
                cr = r * share
            if cr < eps or ch in reach:
                continue
            if ch not in ply or ply[ch] > p_ply + 1:
                ply[ch] = p_ply + 1
            heapq.heappush(heap, (-cr, tie, ch))
            tie += 1
    return reach, ply


def build_graph(edges_by_parent: dict[int, list[dict]], root: int,
                our_white: bool, *,
                rep_moves: dict[int, str] | None = None,
                rep_crush: dict[int, float] | None = None,
                eval_ws: dict[int, float] | None = None,
                aux_rows: dict[int, dict] | None = None,
                slice_prior: float = 0.5,
                eps: float = 1e-3, max_ply: int = 40,
                share_floor: float = 0.002,
                crush_weight: float = 0.0, crush_gamma: float = 0.99,
                edge_imm: dict[tuple[int, str], float] | None = None,
                prior_strength: float = PRIOR_STRENGTH,
                eval_weight: float = EVAL_WEIGHT,
                eval_weight_min: float = EVAL_WEIGHT_MIN,
                eval_weight_k: float = EVAL_WEIGHT_K,
                robustness_floor: float = ROBUSTNESS_FLOOR,
                gate_rel_floor: float = GATE_REL_FLOOR,
                gate_rel_own_margin: float = GATE_REL_OWN_MARGIN,
                require_eval: bool = True,
                max_cands: int = 0,
                gates: bool = True) -> Graph:
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
    """
    eval_ws = eval_ws or {}
    rep_moves = rep_moves or {}
    rep_crush = rep_crush or {}
    aux_rows = aux_rows or {}
    edge_imm = edge_imm or {}

    # Whose turn it is at even ply depends on the perspective: for a Black book
    # the root (ply 0, White to move) is an OPPONENT node, so reach must decay
    # by White's reply shares there.
    reach, ply = optimistic_reach(edges_by_parent, root, our_white, eps,
                                  max_ply, share_floor)

    def is_our(h: int) -> bool:
        return (ply[h] % 2 == 0) == our_white

    def our_eval(h: int) -> float | None:
        e = eval_ws.get(h)
        if e is None:
            return None
        return e if our_white else 1.0 - e

    g = Graph(root=root, our={}, opp={}, epd={}, ply=dict(ply),
              reach=dict(reach), meta={
                  "eps": eps, "max_ply": max_ply, "share_floor": share_floor,
                  "slice_prior": slice_prior, "our_white": our_white,
                  "crush_weight": crush_weight, "crush_gamma": crush_gamma})

    gamma_hop = crush_gamma ** 0.5

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
            node = OurNode(l_node=ln)
            # gate the recorded candidates; always admit the source move
            src = rep_moves.get(h)
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
                if not passes_abs_gate(cev, slice_prior if our_white
                                       else slice_prior, our_white,
                                       robustness_floor):
                    continue
                cand_es.append((e, cev))
            if gates and cand_es:
                keep = rel_gate_keep([cv for _, cv in cand_es],
                                     eval_ws.get(h), our_white,
                                     gate_rel_floor, gate_rel_own_margin)
                cand_es = [(e, cv) for (e, cv), k in zip(cand_es, keep)
                           if k or e["move_san"] == src]
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
            seen_src = False
            for e, _cv in cand_es:
                ch = e.get("child_hash")
                in_sub = ch is not None and ch in reach and \
                    edges_by_parent.get(ch) is not None and p_ply + 1 <= max_ply
                lv = leaf_edge_value(e["white_score_avg"], e["total"],
                                     eval_ws.get(ch) if ch is not None else None,
                                     slice_prior, our_white,
                                     prior_strength, eval_weight,
                                     eval_weight_min, eval_weight_k)
                imm = edge_imm.get((h, e["move_san"]), 0.0)
                cp_child = rep_crush.get(ch, 0.0) if ch is not None else 0.0
                bonus = crush_weight * (imm + (1.0 - imm) * gamma_hop * cp_child)
                node.cands.append((e["move_san"], ch, in_sub, lv, bonus))
                if e["move_san"] == src:
                    seen_src = True
            if src and not seen_src:
                # engine-augmented source move: no stats edge. Child hash via
                # python-chess where an EPD is available (CLI path); tests pass
                # explicit edges so this stays cold there.
                ch = None
                if h in g.epd:
                    from build_baseline_books import child_of
                    ch = child_of(g.epd[h], src)
                in_sub = ch is not None and ch in reach and \
                    edges_by_parent.get(ch) is not None
                lv = leaf_edge_value(slice_prior, 0,
                                     eval_ws.get(ch) if ch is not None else None,
                                     slice_prior, our_white,
                                     prior_strength, eval_weight,
                                     eval_weight_min, eval_weight_k)
                cp_child = rep_crush.get(ch, 0.0) if ch is not None else 0.0
                bonus = crush_weight * gamma_hop * cp_child
                node.cands.append((src, ch, in_sub, lv, bonus))
            g.our[h] = node
        else:
            tot = sum(e["total"] for e in es)
            aux = aux_rows.get(h)
            aux_num = aux_den = 0.0
            if aux:
                for grp in ("term_normal", "term_other", "term_flag"):
                    t = aux.get(f"{grp}_total", 0) or 0
                    if t:
                        s = (aux.get(f"{grp}_white_wins", 0) or 0) + \
                            0.5 * (aux.get(f"{grp}_draws", 0) or 0)
                        aux_num += s if our_white else t - s
                        aux_den += t
                for grp in ("other", "horizon"):
                    t = aux.get(f"{grp}_total", 0) or 0
                    if t:
                        s = (aux.get(f"{grp}_white_wins", 0) or 0) + \
                            0.5 * (aux.get(f"{grp}_draws", 0) or 0)
                        aux_num += s if our_white else t - s
                        aux_den += t
            denom = tot + aux_den
            node = OppNode(const_base=(aux_num / denom) if denom else 0.0)
            for e in es:
                ch = e.get("child_hash")
                p = (e["total"] / denom) if denom else 0.0
                share = (e["total"] / tot) if tot else 0.0
                lv = leaf_edge_value(e["white_score_avg"], e["total"],
                                     eval_ws.get(ch) if ch is not None else None,
                                     slice_prior, our_white,
                                     prior_strength, eval_weight,
                                     eval_weight_min, eval_weight_k)
                if share < share_floor or ch is None:
                    node.const_base += p * lv
                    continue
                in_sub = ch in reach and edges_by_parent.get(ch) is not None \
                    and p_ply + 1 <= max_ply
                node.kids.append((p, ch, in_sub, lv))
            g.opp[h] = node
    return g


# ── the DP ──────────────────────────────────────────────────────────────────

def _adjacency(g: Graph) -> tuple[dict[int, list[int]], dict[int, list[int]]]:
    """(children, parents) restricted to in-subgraph nodes. Duplicate edges
    (a transposition reached twice from one opponent node) are kept — the
    per-path budget accounting depends on them."""
    children: dict[int, list[int]] = {}
    for h, n in g.our.items():
        children[h] = [ch for _, ch, in_sub, _, _ in n.cands
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


def build_curves(g: Graph, bmax: int, k_atoms: int = 256,
                 fixed_policy: dict[int, str] | None = None
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
        curves[h] = _node_curve(g, h, curves, bmax, k_atoms, fixed_policy)
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
            curves[h] = _node_curve(g, h, curves, bmax, k_atoms, fixed_policy)
            remaining.discard(h)
            for par in parents[h]:
                pending[par] -= 1
                if pending[par] == 0 and par in remaining:
                    q.append(par)

    diag = {"nodes": len(all_nodes), "stranded_cycle_nodes": flattened}
    return curves, diag


def _node_curve(g: Graph, h: int, curves: dict[int, Curve], bmax: int,
                k_atoms: int, fixed_policy: dict[int, str] | None) -> Curve:
    if h in g.our:
        n = g.our[h]
        cands = n.cands
        if fixed_policy is not None:
            want = fixed_policy.get(h)
            cands = [c for c in cands if c[0] == want]
        pts: dict[int, float] = {0: n.l_node}
        for _san, ch, in_sub, leaf_val, _bonus in cands:
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
                if b not in pts or v > pts[b]:
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


# ── greedy stopping (Phase-B comparison method, deliberately myopic) ────────

def greedy_stopping(g: Graph, policy: dict[int, str], budget: int
                    ) -> tuple[set[int], float]:
    """One-step gain-greedy over the FIXED policy: repeatedly book the frontier
    our-node with the largest reach * (one_step_value - L_node). No chain
    atoms — this is the strawman whose trap-blindness the DP fixes; it exists
    so Phase D can attribute wins. Returns (booked our-node hashes, spent)."""
    import heapq

    def one_step_gain(h: int) -> float:
        n = g.our[h]
        want = policy.get(h)
        for san, ch, in_sub, leaf_val, _ in n.cands:
            if san != want:
                continue
            if in_sub and ch is not None and ch in g.opp:
                opp = g.opp[ch]
                v = opp.const_base + sum(
                    p * (g.our[k].l_node if in_s and k in g.our else lv)
                    for p, k, in_s, lv in opp.kids)
            else:
                v = leaf_val
            return g.reach.get(h, 0.0) * (v - n.l_node)
        return -np.inf

    booked: set[int] = set()
    heap = [(-one_step_gain(g.root), 0, g.root)] if g.root in g.our else []
    tie = 1
    if g.root in g.opp:
        for p, ch, in_sub, _ in g.opp[g.root].kids:
            if in_sub and ch in g.our:
                heap.append((-one_step_gain(ch), tie, ch))
                tie += 1
        heapq.heapify(heap)
    spent = 0
    while heap and spent < budget:
        ng, _, h = heapq.heappop(heap)
        if h in booked or -ng == -np.inf:
            continue
        booked.add(h)
        spent += 1
        want = policy.get(h)
        for san, ch, in_sub, _, _ in g.our[h].cands:
            if san != want or not in_sub or ch not in g.opp:
                continue
            for p, k, in_s, _ in g.opp[ch].kids:
                if in_s and k in g.our and k not in booked:
                    heapq.heappush(heap, (-one_step_gain(k), tie, k))
                    tie += 1
    return booked, spent


# ── extraction ──────────────────────────────────────────────────────────────

def extract_book(g: Graph, curves: dict[int, Curve], budget: int,
                 fixed_policy: dict[int, str] | None = None,
                 force_booked: set[int] | None = None
                 ) -> dict:
    """Top-down allocation of `budget` over the curves; returns the booked
    tree, per-node allocations, realized bottom-up values, and spend stats.

    Our node: book iff the curve strictly beats stopping; move chosen by
    KEY = value + frozen crush bonus (mirrors Stage 3: select by key, propagate
    value). Opponent node: greedy density prefix over child atoms with skip-fit
    (a child whose atom is skipped is closed — atoms are prefixes).

    force_booked: book these our-nodes unconditionally, skipping both the
    allocation test and the beats-stopping test, and still walk the values
    bottom-up over the result. This exists for the greedy arm, whose SET is
    chosen by greedy_stopping and must not then be re-decided by the density
    allocator. Measured 2026-08-29 without it: greedy selected 6 nodes (all
    with positive one-step gain) and only 3 were booked, every miss caused by
    receiving alloc 0 and NONE by failing a value test — so the arm silently
    reported a 14-move book for a 20-move budget, which makes it useless as an
    equal-footprint comparator. Callers pass the greedy set here and keep the
    same set as fixed_policy so the values stay consistent with the moves.
    """
    booked: dict[int, str] = {}
    alloc: dict[int, int] = {0: 0}
    reached: set[int] = {g.root}
    visited: list[int] = []
    spent_paths = 0
    alloc = {g.root: budget}
    if force_booked:
        # Forced nodes must be REACHED as well as exempt from the value test.
        # The topo walk below skips anything not in `reached` BEFORE it looks at
        # `forced`, so exempting the value test alone changes nothing for a pick
        # the allocator never walks to. Measured 2026-08-30: with only the value
        # exemption, greedy_white still booked 3/6, 4/10 and 12/20 at eps 0.005,
        # because essentially every missing pick was never-reached rather than
        # merely unfunded. Seeding here is what actually lets greedy keep its
        # own selection.
        reached |= set(force_booked)

    # Top-down in topological order so a transposed node's allocation is FINAL
    # (max over every in-path — funding one path funds the position) before its
    # booking decision runs. The naive stack version processed a node at
    # whichever allocation happened to pop first; _test_budget_transpo caught
    # it booking nothing on the zero-alloc path.
    for h in topo_down(g):
        if h not in reached:
            continue
        b = alloc.get(h, 0)
        visited.append(h)
        if h in g.our:
            n = g.our[h]
            forced = force_booked is not None and h in force_booked
            if b < 1 and not forced:
                continue
            cands = n.cands
            if fixed_policy is not None:
                want = fixed_policy.get(h)
                cands = [c for c in cands if c[0] == want]
            best = None
            for san, ch, in_sub, leaf_val, bonus in cands:
                if in_sub and ch is not None and ch in curves:
                    v = curves[ch].eval(max(b - 1, 0))
                else:
                    v = leaf_val
                key = v + bonus
                if best is None or key > best[0]:
                    best = (key, v, san, ch, in_sub)
            if best is None:
                continue
            _key, v, san, ch, in_sub = best
            if not forced and v <= n.l_node + 1e-12:
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
            for san, ch, in_sub, leaf_val, _ in n.cands:
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
