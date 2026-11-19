"""Auditable extension accounting on supplied disjoint continuation routes.

This is the selection/accounting kernel, not the engine expansion scheduler.
An unresolved route cannot be called complete merely because a local gap is small.
"""
from __future__ import annotations
from dataclasses import dataclass
import math

def severity(scores, legal_count):
    """Own-side expected scores, descending, of the top min(3, legal_count) moves."""
    if legal_count<1 or len(scores)!=min(3,legal_count):
        raise ValueError("missing engine analysis; not a zero gap")
    if any(not math.isfinite(s) or not 0<=s<=1 for s in scores) or list(scores)!=sorted(scores,reverse=True):
        raise ValueError("invalid ranked expected scores")
    gap2=scores[0]-scores[1] if legal_count>=2 else 0.
    gap3=scores[0]-scores[2] if legal_count>=3 else 0.
    return dict(gap2=gap2,gap3=gap3,severity=max(gap2,.60*gap3))

@dataclass(frozen=True)
class Route:
    probability: float
    positions: tuple  # full position keys along this route; identical keys cost once
    complete: bool = True

def validate(routes, risks):
    mass=sum(r.probability for r in routes)
    if mass>1+1e-10 or any(not math.isfinite(r.probability) or r.probability<0 for r in routes):
        raise ValueError("routes must be disjoint probability mass, not overlapping reach totals")
    for r in routes:
        for k in r.positions:
            if k not in risks or not math.isfinite(risks[k]) or not 0<=risks[k]<=1:
                raise ValueError("unknown tactical severity requires analysis")

def exposure(routes, risks, added, base=()):
    """Only the continuously covered prefix can realize the proposed continuation."""
    validate(routes,risks)
    selected=set(added); cover=selected|set(base)
    result=0.
    for route in routes:
        peak=0.
        for k in route.positions:
            if k not in cover: break
            if k in selected: peak=max(peak,risks[k])
        result+=route.probability*peak
    return result

def package_value(routes, risks, package, selected=(), base=()):
    new=set(package)-set(base)-set(selected)
    before=exposure(routes,risks,selected,base)
    after=exposure(routes,risks,set(selected)|new,base)
    return dict(new_positions=len(new),marginal_exposure=after-before,
                density=(after-before)/len(new) if new else 0.)

def unresolved_bound(routes, risks, selected=(), base=()):
    """Conservative total residual exposure bound: severity <= 1, at least one new move.
    The caller must also check every known candidate package. This bound alone does
    not certify optimal package search or adequate engine/policy provenance.
    """
    validate(routes,risks)
    result=0.
    for r in routes:
        if r.complete: continue
        # Missing connectors may allow a later risk even if current local risk is zero.
        peak=0.
        for k in r.positions:
            if k not in set(selected)|set(base): break
            if k in selected: peak=max(peak,risks[k])
        result+=r.probability*(1-peak)
    return result

def require_closed_frontier(routes,risks,threshold,selected=(),base=()):
    remaining=unresolved_bound(routes,risks,selected,base)
    if remaining>=threshold:
        raise ValueError(f"unresolved continuation exposure {remaining:g} can justify more analysis")
    return remaining

def checked_terminal(board):
    """History-bearing boards are mandatory: transposed FEN alone cannot prove repetition."""
    # Automatic outcomes are exact. Claimable repetition/draw is a separate player decision.
    outcome=board.outcome(claim_draw=False)
    return None if outcome is None else outcome.result()
