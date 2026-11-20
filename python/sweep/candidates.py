"""Consequential missing/boundary evidence on the own-move frontier of a chosen book.

Requests are independent of historical move counts, except where a thin move's stored score
is the reference that cuts a rival (``unverified-rare-reference``). A missing child may open a
previously absent branch; solving again must permit expansion.

Scope (review of 19 November 2026, finding 1). ``reach`` is the chosen book's POLICY reach:
the positions it books and the positions where it stops. The whole graph's optimistic reach
would request millions of parents (three in four own positions have some move without a
stored eval). ``select`` keeps parents at or above ``min_reach``, the most reached first, up to
``max_parents`` per round, and accounts for everything it leaves: deferred beyond the cap
(re-requested next round), below the reach threshold (not examined), and positions off the
book's policy.

Reasons, per parent:
  missing-candidate-evidence   a move without a stored or verified score
  candidate-score-boundary     a move within ``band`` of the relative or absolute gate
  inconsistent-parent-child    the best child's score exceeds the parent's by more than band
  parent-above-children        the parent's score exceeds every child's by more than band and
                               the parent has no 5M+ verification. The relative gate measures
                               from the parent too, so this can empty the menu; the search
                               nominates the engine's best move, which joins the menu as an
                               engine candidate in the next round (finding 2).
  unverified-rare-reference    the best child has fewer than ``rare_trust`` games and no 5M+
                               verification, and the reference (it, or the parent's own score,
                               which often agrees with it) cuts a move that would pass against
                               the best trusted child (finding 7).
"""
import math
import numpy as np
import polars as pl
import budget_core as bc
from eval_arrays import MISSING
from .quality import score

SHALLOW = 5_000_000
DEEP = 20_000_000


def requests(edges, reach, lookup, *, white, floor, limit, verified_frame=None, overrides=None,
             band=.01, rare_trust=None, min_reach=0.):
    """Request rows for parents with ``reach[h] >= min_reach`` (and > 0). ``edges`` has
    parent_hash, parent_epd, move_san, child_hash and, for the rare-reference rule, total."""
    sign = 1 if white else -1
    keep = [h for h, r in reach.items() if r > 0 and r >= min_reach]
    if not keep:
        return []
    edges = edges.filter(pl.col("parent_hash").is_in(pl.Series(keep, dtype=edges.schema["parent_hash"]).implode()))
    ids = sorted(set(edges["parent_hash"]) | set(edges["child_hash"].drop_nulls()))
    cps = {h: int(c) for h, c in zip(ids, lookup(np.asarray(ids, np.int64))) if c != MISSING}
    cps.update(overrides or {})
    done = {}
    if verified_frame is not None:
        for row in verified_frame.to_dicts():
            if row.get("verification_nodes"):
                k = (row["position_hash"], row["move_san"].rstrip("+#"))
                done[k] = max(done.get(k, 0), row["verification_nodes"])
    has_total = "total" in edges.columns
    out = []
    for (h,), group in edges.group_by("parent_hash", maintain_order=True):
        items = group.to_dicts()
        sans = [r["move_san"].rstrip("+#") for r in items]
        value = {s: sign * cps[r["child_hash"]] for s, r in zip(sans, items) if r["child_hash"] in cps}
        child = list(value.values())
        parent = sign * cps[h] if h in cps else None
        reference = max(child + ([parent] if parent is not None else []), default=None)
        parent_checked = max((done.get((h, s), 0) for s in sans), default=0)
        moves, reasons = [], set()
        for s, r in zip(sans, items):
            budget = done.get((h, s), 0)
            reason = None
            if s not in value or reference is None:
                if budget < SHALLOW:
                    reason = "missing-candidate-evidence"
            elif budget < DEEP:
                v = score(value[s]); loss = score(reference) - v
                if (abs(loss - limit) <= band + bc.SCORE_EPS and v >= floor - band - bc.SCORE_EPS
                        or abs(v - floor) <= band + bc.SCORE_EPS and loss <= limit + band + bc.SCORE_EPS):
                    reason = "candidate-score-boundary"
            if reason:
                moves.append(s); reasons.add(reason)
        if parent is not None and child:
            gap = score(max(child)) - score(parent)
            if gap > band and any(done.get((h, s), 0) < DEEP for s in value):
                moves += list(value); reasons.add("inconsistent-parent-child")
            if -gap > band and parent_checked < SHALLOW:
                moves += sans; reasons.add("parent-above-children")
        if rare_trust is not None and has_total and value:
            totals = dict(zip(sans, (r.get("total") or 0 for r in items)))
            top = max(value, key=lambda s: (value[s], s))
            trusted = [value[s] for s in value if totals[s] >= rare_trust or done.get((h, s), 0) >= SHALLOW]
            if totals[top] < rare_trust and done.get((h, top), 0) < SHALLOW:
                # The parent's stored score often agrees with an inflated thin child (10 Nov),
                # so the trusted reference is the trusted children alone.
                alt = max(trusted, default=None)
                ref = score(max(value[top], parent) if parent is not None else value[top])
                cut = [s for s in value if s != top and ref - score(value[s]) > limit + bc.SCORE_EPS
                       and (alt is None or score(alt) - score(value[s]) <= limit + bc.SCORE_EPS)]
                if cut:
                    moves += [top] + cut; reasons.add("unverified-rare-reference")
        if moves:
            out.append(dict(position_hash=h, epd=items[0]["parent_epd"], moves=sorted(set(moves)),
                            reason="; ".join(sorted(reasons)), reach=reach[h]))
    return out


def select(rows, reach, *, min_reach, max_parents, graph_parents=None):
    """Rank request rows by reach (most reached first; ties by key) and cap them. Returns
    (kept rows, summary). ``reach`` is the policy reach the rows were computed from;
    ``graph_parents`` the number of own-turn parents in the collected graph."""
    if max_parents is not None and max_parents < 1:
        raise ValueError("max_parents must be positive")
    if not 0 <= min_reach <= 1:
        raise ValueError("min_reach must be a probability")
    ranked = sorted(rows, key=lambda r: (-r["reach"], str(r["position_hash"])))
    kept = ranked if max_parents is None else ranked[:max_parents]
    deferred = ranked[len(kept):]
    on_policy = [r for r in reach.values() if r > 0]
    below = [r for r in on_policy if r < min_reach]
    reasons = {}
    for r in ranked:
        for x in r["reason"].split("; "):
            reasons[x] = reasons.get(x, 0) + 1
    summary = dict(min_reach=min_reach, max_parents=max_parents,
                   candidates=len(ranked), requested=len(kept),
                   requested_reach=float(sum(r["reach"] for r in kept)),
                   deferred=len(deferred), deferred_reach=float(sum(r["reach"] for r in deferred)),
                   below_threshold_positions=len(below), below_threshold_reach=float(sum(below)),
                   on_policy_positions=len(on_policy), reasons=reasons)
    if graph_parents is not None:
        summary["off_policy_graph_parents"] = int(graph_parents) - len(on_policy)
    if any(not math.isfinite(summary[k]) for k in ("requested_reach", "deferred_reach", "below_threshold_reach")):
        raise ValueError("invalid reach")
    return kept, summary
