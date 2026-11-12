"""Optimize a two-decision repertoire slice with one budget for distinct boards."""
from collections import defaultdict
from dataclasses import dataclass

import numpy as np
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix


@dataclass
class Action:
    root: int
    after: int
    base: float  # conditional score when none of the replies are prepared
    probs: dict   # conditional probability of named replies


def optimize(weights, stops, actions, gains, budget, cross_ply=None):
    """A purchased follow-up earns its gain on every incoming chosen action.

    Follow-ups private to one root can be bought fractionally in the relaxation.
    Once the root choices and shared purchases are integral, the residual problem
    picks the largest equal-cost gains and always has an integral optimum.
    """
    costs, integers, rows, cols, vals, upper = [], [], [], [], [], []

    def var(profit=0., integer=False):
        costs.append(-1e5 * profit)
        integers.append(int(integer))
        return len(costs)-1

    def limit(terms, rhs):
        for i, a in terms:
            rows.append(len(upper))
            cols.append(i)
            vals.append(a)
        upper.append(rhs)

    cross_ply = cross_ply or {}
    if not actions or not budget:
        base = sum(w*stops[h] for h,w in weights.items())
        return dict(value=base,upper=base,roots=[],leaves=[],solver_status=0)
    roots, incoming, linked = defaultdict(list), defaultdict(list), defaultdict(list)
    cards = []
    for i, action in enumerate(actions):
        weight = weights[action.root]
        assert i == var(weight * (action.base-stops[action.root]), True)
        cards.append(i)
        roots[action.root].append(i)
        for h, p in action.probs.items():
            if p > 0 and h in cross_ply:
                linked[h].append((i,weight*p))
            elif p > 0 and gains.get(h, 0.) > 0:
                incoming[h].append((i, weight*p*gains[h]))
    for indices in roots.values():
        limit(((i, 1.) for i in indices), 1.)
    # If a board can occur at both book depths, the same paid SAN is compulsory
    # on both visits, even when that action has a negative deeper-ply gain.
    for h,edges in linked.items():
        for j in roots[h]:
            gain = cross_ply[h][actions[j].after]
            for i,mass in edges:
                used = var(mass*gain)
                limit([(used,1.),(i,-1.)],0.)
                limit([(used,1.),(j,-1.)],0.)
                limit([(i,1.),(j,1.),(used,-1.)],1.)
    for edges in incoming.values():
        shared = len({actions[i].root for i, _ in edges}) > 1
        if shared:
            buy = var(integer=True)
            cards.append(buy)
        by_root = defaultdict(list)
        for i, profit in edges:
            used = var(profit)
            limit([(used, 1.), (i, -1.)], 0.)
            if shared:
                by_root[actions[i].root].append(used)
            else:
                cards.append(used)
        if shared:
            for uses in by_root.values():
                limit([(u, 1.) for u in uses] + [(buy, -1.)], 0.)
    limit(((i, 1.) for i in cards), budget)
    mat = coo_matrix((vals, (rows, cols)), shape=(len(upper), len(costs))).tocsc()
    result = milp(costs, integrality=integers, bounds=Bounds(0., 1.),
                  constraints=LinearConstraint(mat, -np.inf, upper),
                  options={"mip_rel_gap": 1e-7, "time_limit": 90})
    if result.x is None or result.mip_dual_bound is None:
        raise RuntimeError(result.message)
    chosen = [i for i in range(len(actions)) if result.x[i] > .5]
    if len({actions[i].root for i in chosen}) != len(chosen) or len(chosen)>budget:
        raise ValueError("Infeasible root assignment")
    benefits = defaultdict(float)
    base = sum(w*stops[h] for h,w in weights.items())
    value = base
    root_moves = {actions[i].root:actions[i].after for i in chosen}
    for i in chosen:
        a = actions[i]
        value += weights[a.root]*(a.base-stops[a.root])
        for h,p in a.probs.items():
            mass = weights[a.root]*p
            if h in cross_ply:
                if h in root_moves:
                    value += mass*cross_ply[h][root_moves[h]]
            else:
                benefits[h] += mass*gains.get(h,0.)
    leaves = sorted((h for h,v in benefits.items() if v>0), key=lambda h:(-benefits[h],h))
    leaves = leaves[:budget-len(chosen)]
    value += sum(benefits[h] for h in leaves)
    dual = base-result.mip_dual_bound/1e5
    if dual+1e-7 < value:
        raise ArithmeticError("Feasible book exceeds the MIP bound")
    return dict(value=value, upper=max(value,dual), roots=chosen, leaves=leaves,
                solver_status=int(result.status))
