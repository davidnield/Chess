"""Build exact and conservatively truncated chance distributions on the same panel."""
from collections import Counter, defaultdict
from dataclasses import dataclass

import numpy as np
from scipy.sparse import csr_matrix

from root_search_pilot import KS


@dataclass
class Level:
    ids: list
    stop: np.ndarray
    owners: np.ndarray
    moves: list
    base: np.ndarray
    edges: csr_matrix


def pack(xs,stop,owners,moves,base,edges,next_count):
    rows,cols,probs = zip(*edges) if edges else ([],[],[])
    return Level(xs,np.array([stop[h] for h in xs]),np.array(owners,dtype=int),moves,np.array(base,dtype=float),
                 csr_matrix((probs,(rows,cols)),shape=(len(owners),next_count)))


def order_tail(cost,allow):
    """Skip the least expensive items whose total bound fits the allowance."""
    spent = 0.
    selected = set()
    for h,v in sorted(cost.items(),key=lambda item:(item[1],item[0])):
        if spent+v<=allow:
            spent += v
        else:
            selected.add(h)
    return selected


def distribution(row,exact,k):
    n,edges = row
    if exact is not None:
        n = sum(exact.values())
    if not n:
        return {},float(n is None),n==0
    probs = {h:((exact.get(h,0) if exact is not None else c)+k*q)/(n+k) for h,q,c in edges}
    remainder = (n-sum(c for _,_,c in edges))/(n+k) if exact is None else 0.
    if abs(sum(probs.values())+remainder-1)>1e-8 or remainder<0:
        raise ValueError("Invalid chance distribution")
    return probs,remainder,False


def build(panel,inputs,anchor,budget,tol,full,limit):
    def epds(hs):
        return {h:panel.boards[h] for h in hs}
    layers = {k:[] for k in KS}
    reach = panel.weights
    raw_bounds = Counter()
    exit_bounds = Counter()
    nlevels = len(panel.layers)
    phase = []
    for depth,xs in enumerate(panel.layers[:-1]):
        ply = anchor+depth*2
        mids = {m for h in xs for m in panel.menu[h]}
        next_ids = {h:i for i,h in enumerate(panel.layers[depth+1])}
        raw_cost = {k:Counter() for k in KS}
        for h in xs:
            for m in panel.menu[h]:
                n,edges = panel.white[m]
                r = n-sum(c for _,_,c in edges) if n is not None else None
                if r==0:
                    continue
                for k in KS:
                    raw_cost[k][m] += reach[k].get(h,0.)*(1. if r is None else r/(n+k))
        allow = .4*tol/(nlevels-1)
        needed = set().union(*(cost if full else order_tail(cost,allow) for cost in raw_cost.values()))
        print(f"Ply {ply}: resolving {len(needed)} archive parents of {len(mids)} opponent boards",flush=True)
        exact = inputs.exact_counts(epds(needed))
        for k in KS:
            raw_bounds[k] += sum(v for m,v in raw_cost[k].items() if m not in needed)
        dist = {k:{} for k in KS}
        exits,nohistory = set(),set()
        for m in sorted(mids):
            for k in KS:
                p,_,no_moves = distribution(panel.white[m],exact.get(m),k)
                if no_moves:
                    nohistory.add(m)
                if not full:
                    off = {h:v for h,v in p.items() if v and h not in next_ids}
                    keep = order_tail(off,.2*tol/(nlevels-1))
                    p = {h:v for h,v in p.items() if h in next_ids or h in keep}
                dist[k][m] = {h:v for h,v in p.items() if v>0}
                exits.update(h for h in dist[k][m] if h not in next_ids)
        w = inputs.values(epds(exits),ply+2)
        r = inputs.values(epds(nohistory),ply+1)
        stop = inputs.values(epds(xs),ply)
        following = {k:Counter() for k in KS}
        for k in KS:
            owners,moves,base,edges = [],[],[],[]
            for i,h in enumerate(xs):
                best = Counter()
                for m in panel.menu[h]:
                    val = r.get(m,0.)
                    for child,p in dist[k][m].items():
                        if child in next_ids:
                            edges.append((len(owners),next_ids[child],p))
                            best[child] = max(best[child],p)
                        else:
                            val += p*w[child]
                    owners.append(i)
                    moves.append(m)
                    base.append(val)
                for ch,prob in best.items():
                    following[k][ch] += reach[k].get(h,0.)*prob
            following[k] = {h:min(1.,p) for h,p in following[k].items()}
            layers[k].append(pack(xs,stop,owners,moves,base,edges,len(next_ids)))
            if not full:
                exit_bounds[k] += .2*tol/(nlevels-1)
        phase.append(dict(ply=ply,raw=len(needed),white=len(mids),exit_values=len(exits)))
        reach = following
    xs = panel.layers[-1]
    ply = anchor+2*(nlevels-1)
    stop = inputs.values(epds(xs),ply)
    mandatory = set(xs).intersection(set().union(*panel.layers[:-1]))
    needed = set(xs) if full else mandatory.copy()
    ranked = {}
    for k in KS:
        ranked[k] = sorted(((h,reach[k].get(h,0.)*(1-stop[h])) for h in xs if h not in mandatory),
                           key=lambda pair:(-pair[1],pair[0]))
        if not full:
            # At most B optional cards can yield a terminal-depth improvement.
            bounds = np.r_[[v for _,v in ranked[k]],np.zeros(budget)]
            prefix = np.r_[0.,np.cumsum(bounds)]
            sums = prefix[budget:]-prefix[:-budget]
            n = int(np.flatnonzero(sums<=.399*tol)[0])
            needed.update(h for h,_ in ranked[k][:n])
    print(f"Ply {ply}: evaluating {len(needed)} of {len(xs)} final Black decisions",flush=True)
    choices = inputs.candidates(epds(needed))
    menu = {h:list(cs)[:limit] for h,cs in choices.items()}
    final_boards = {m:choices[h][m][1] for h in menu for m in menu[h]}
    vals = inputs.values(final_boards,ply+1)
    owners,moves,base = [],[],[]
    for i,h in enumerate(xs):
        for m in menu.get(h,[]):
            owners.append(i)
            moves.append(m)
            base.append(vals[m])
    last = pack(xs,stop,owners,moves,base,[],0)
    errors = {}
    for k in KS:
        layers[k].append(last)
        tail = [v for h,v in ranked[k] if h not in needed][:budget]
        errors[k] = raw_bounds[k]+exit_bounds[k]+sum(tail)
        if errors[k]>tol+1e-9:
            raise ValueError("Input approximation exceeds requested tolerance")
    phase.append(dict(ply=ply,expanded=len(needed),eligible=len(xs),exit_values=len(vals)))
    return layers,errors,phase
