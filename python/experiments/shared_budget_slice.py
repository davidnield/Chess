"""Compare selective expansion with full expansion on a connected Black opening slice."""
import argparse
from collections import Counter, defaultdict
from pathlib import Path
import time

import chess

from root_pilot_data import Data, Maia, zobrist_int64
from root_search_pilot import KS, chance_inputs
from run_practical_rebuild import publish
from shared_slice_solver import Action, optimize
from shared_slice_stops import Stops


def exact_policy(counts, q, k):
    total = sum(counts.values())
    if not total:
        return {}
    return {h:(counts.get(h,0)+k*p)/(total+k) for h,p in q.items()}


class Slice:
    def __init__(self, data, maia, stops, nroots, anchor_ply=3):
        self.data, self.stops = data,stops
        self.ply = anchor_ply
        known = {}
        board = chess.Board()
        start = zobrist_int64(board)
        chance_inputs(data,maia,{start:board.epd()},known)
        raw = data.exact_counts({start:board.epd()})[start]
        first = known[start]
        reaches = {k:exact_policy(raw,first.q,k) for k in KS}
        boards = {h:e for h,(_,e) in first.legal.items()}
        histories = {h:[(reaches[30][h],[san])] for h,(san,_) in first.legal.items()}
        for _ in range(1,anchor_ply,2):
            black = data.candidates(boards)
            fixed = {h:min(cs,key=lambda ch:(data.cp[ch],cs[ch][0])) for h,cs in black.items() if cs}
            prefix = {ch:black[h][ch][1] for h,ch in fixed.items()}
            chance_inputs(data,maia,prefix,known)
            exact = data.exact_counts(prefix)
            following = {k:Counter() for k in KS}
            paths,children = defaultdict(list),{}
            for h,mid in fixed.items():
                for k in KS:
                    for child,p in exact_policy(exact[mid],known[mid].q,k).items():
                        if not p:
                            continue
                        san,epd = known[mid].legal[child]
                        children[child] = epd
                        following[k][child] += reaches[k][h]*p
                        if k==30:
                            paths[child].extend((mass*p,line+[black[h][mid][0],san])
                                                for mass,line in histories[h])
            keys = sorted(children,key=lambda h:(-max(following[k][h] for k in KS),h))[:nroots]
            boards = {h:children[h] for h in keys}
            reaches,histories = following,paths
        keys = sorted(boards,key=lambda h:(-max(reaches[k][h] for k in KS),h))[:nroots]
        self.coverage = {k:sum(reaches[k][h] for h in keys) for k in KS}
        self.weights = {k:{h:reaches[k][h]/self.coverage[k] for h in keys} for k in KS}
        self.histories = {h:sorted(histories[h],reverse=True) for h in keys}
        roots = {h:boards[h] for h in keys}
        self.root_stop = stops.get(roots,self.ply)
        self.options = data.candidates(roots)
        self.after = {ch:e for cs in self.options.values() for ch,(_,e) in cs.items()}
        chance_inputs(data,maia,self.after,known)
        self.nodes = {h:known[h] for h in self.after}
        self.black = {ch:e for n in self.nodes.values() for ch,(_,e) in n.legal.items()}
        self.stop = stops.get(self.black,self.ply+2)
        self.after_stop = stops.get(self.after,self.ply+1)
        overlap = self.options.keys() & self.black.keys()
        linked = {ch:e for h in overlap for ch,(_,e) in self.options[h].items()}
        values = stops.get(linked,self.ply+3)
        self.cross_ply = {h:{ch:values[ch]-self.stop[h] for ch in self.options[h]} for h in overlap}

    def actions(self,k,exact,upper):
        actions = []
        for h,cs in self.options.items():
            for mid in cs:
                node = self.nodes[mid]
                coarse,n = self.data.coarse[mid]
                if mid in exact:
                    coarse = exact[mid]
                    n = sum(coarse.values())
                if n:
                    p = {ch:(coarse.get(ch,0)+k*q)/(n+k) for ch,q in node.q.items()}
                    resid = (n-sum(coarse.values()))/(n+k)
                    if not 0<=resid<=1 or abs(sum(p.values())+resid-1)>1e-8:
                        raise ValueError("Reply mass is not conserved")
                    base = sum(prob*self.stop[ch] for ch,prob in p.items()) + upper*resid
                else:
                    p = {}
                    base = float(upper) if n is None else self.after_stop[mid]
                actions.append(Action(h,mid,base,p))
        return actions


class Search:
    def __init__(self, graph, budget, tol):
        self.g,self.budget,self.tol = graph,budget,tol
        self.exact,self.gains,self.answers = {},{},{}
        self.answer_hash = {}
        self.stats = Counter()

    def expand(self,raw,leaves):
        data,g = self.g.data,self.g
        t = time.perf_counter()
        raw = set(raw)-self.exact.keys()
        self.exact.update(data.exact_counts({h:g.after[h] for h in raw}))
        self.stats["archive_seconds"] += time.perf_counter()-t
        t = time.perf_counter()
        leaves = set(leaves)-self.gains.keys()-g.cross_ply.keys()
        cs = data.candidates({h:g.black[h] for h in sorted(leaves)})
        boards = {ch:e for moves in cs.values() for ch,(_,e) in moves.items()}
        vals = g.stops.get(boards,g.ply+3)
        for h,moves in cs.items():
            self.gains[h] = 0.
            if moves:
                best = min(moves,key=lambda ch:(-vals[ch],moves[ch][0]))
                self.gains[h] = max(0.,vals[best]-g.stop[h])
                self.answers[h] = moves[best][0]
                self.answer_hash[h] = best
        self.stats["followup_actions"] += sum(map(len,cs.values()))
        self.stats["leaf_seconds"] += time.perf_counter()-t

    def solve(self,k,upper):
        g = self.g
        t = time.perf_counter()
        actions = g.actions(k,self.exact,upper)
        gains = ({h:self.gains.get(h,1-stop) for h,stop in g.stop.items()}
                 if upper else self.gains)
        sol = optimize(g.weights[k],g.root_stop,actions,gains,self.budget,g.cross_ply)
        sol["roots"] = [(actions[i].root,actions[i].after) for i in sol["roots"]]
        self.stats["optimization_seconds"] += time.perf_counter()-t
        return sol

    def batched(self):
        """Bound the total missing value of any feasible book before optimizing.

        Within one root only one action can be chosen. Across roots all arrivals
        can matter. At most B distinct follow-up gains can enter the same book.
        """
        g = self.g
        raw = set()
        for h,options in g.options.items():
            for mid in options:
                c,n = g.data.coarse[mid]
                tail = 1. if n is None else (n-sum(c.values()))/n if n else 0.
                if max(w[h] for w in g.weights.values())*tail > self.tol*.25/len(g.options):
                    raw.add(mid)
        self.expand(raw,())
        wanted = set()
        envelope = {}
        for k in KS:
            by_root = defaultdict(Counter)
            tail = Counter()
            for a in g.actions(k,self.exact,False):
                w = g.weights[k][a.root]
                c,n = g.data.coarse[a.after]
                if a.after not in self.exact:
                    remaining = 1. if n is None else (n-sum(c.values()))/(n+k) if n else 0.
                    tail[a.root] = max(tail[a.root],w*remaining)
                row = by_root[a.root]
                for ch,p in a.probs.items():
                    if ch not in g.cross_ply:
                        row[ch] = max(row[ch],w*p*(1-g.stop[ch]))
            possible = Counter()
            for row in by_root.values():
                possible.update(row)
            ranked = possible.most_common()
            # Prefix removal makes the sum of the largest B remaining gains monotone.
            lo,hi = 0,len(ranked)
            reserve = self.tol*.99-sum(tail.values())
            if reserve<0:
                raise ArithmeticError("Archive envelope exhausted the error allowance")
            while lo<hi:
                mid = (lo+hi)//2
                if sum(v for _,v in ranked[mid:mid+self.budget])>reserve:
                    lo = mid+1
                else:
                    hi = mid
            wanted.update(h for h,_ in ranked[:lo])
            envelope[k] = (sum(tail.values()),ranked)
        self.expand((),wanted)
        books,gaps = {},{}
        for k,(residual,ranked) in envelope.items():
            sol = self.solve(k,False)
            remaining = [v for h,v in ranked if h not in wanted][:self.budget]
            bound = sol["upper"]+residual+sum(remaining)
            gaps[k] = bound-sol["value"]
            if gaps[k]>self.tol:
                raise RuntimeError(f"Shared-budget MIP certificate exceeds tolerance: {gaps[k]}")
            books[k] = dict(roots=sol["roots"],leaves={h:self.answers[h] for h in sol["leaves"]},
                            lower=sol["value"],upper=bound)
        return books,[gaps]

    def run(self,full):
        g = self.g
        needed = {h for h in g.nodes if g.data.coarse[h][1] is None or
                  g.data.coarse[h][1]!=sum(g.data.coarse[h][0].values())}
        if full:
            self.expand(needed,g.black)
        history = []
        while True:
            lo = {k:self.solve(k,False) for k in KS}
            hi = lo if full else {k:self.solve(k,True) for k in KS}
            gaps = {k:hi[k]["upper"]-lo[k]["value"] for k in KS}
            if full and max(gaps.values())>1e-6:
                raise RuntimeError(f"Full reference failed to establish optimality within 1e-6: {gaps}")
            history.append(gaps)
            print(f"  bounds {gaps} after {len(self.gains)} follow-ups, {len(self.exact)} archive parents",flush=True)
            if full or max(gaps.values()) <= self.tol:
                for k in KS:
                    if hi[k]["upper"]+1e-7<lo[k]["value"]:
                        raise ValueError("Inverted bounds")
                return {k:dict(roots=lo[k]["roots"], leaves={h:self.answers[h] for h in lo[k]["leaves"]},
                               lower=lo[k]["value"],upper=hi[k]["upper"]) for k in KS},history
            raw,leaves = set(),set()
            importance = Counter()
            # Batch likely refinements from the optimistic policy and its reached replies.
            for k in KS:
                for h,mid in hi[k]["roots"]:
                    if mid in needed:
                        raw.add(mid)
                    c,n = g.data.coarse[mid]
                    if mid in self.exact:
                        c = self.exact[mid]
                        n = sum(c.values())
                    for ch,p in g.nodes[mid].q.items():
                        if n and ch not in self.gains and ch not in g.cross_ply:
                            importance[ch] += g.weights[k][h]*(c.get(ch,0)+k*p)/(n+k)*(1-g.stop[ch])
                leaves.update(set(hi[k]["leaves"])-self.gains.keys())
            leaves.update(ch for ch,_ in importance.most_common(3*self.budget))
            raw -= self.exact.keys()
            if not raw and not leaves:
                raise RuntimeError(f"Solver dual gap is too large to certify {gaps}")
            self.expand(raw,leaves)


def replay(g,search,book,k):
    """Independent forward replay using exact probabilities and summed arrivals."""
    roots,leaves = dict(book["roots"]),book["leaves"]
    value,arrivals,parents = 0.,Counter(),defaultdict(list)
    prior_mass = 0.
    for h,w in g.weights[k].items():
        if h not in roots:
            value += w*g.root_stop[h]
            if (g.ply,h) in g.stops.unsupported:
                prior_mass += w
            continue
        mid = roots[h]
        counts = search.exact.get(mid,g.data.coarse[mid][0])
        probs = exact_policy(counts,g.nodes[mid].q,k)
        if not probs:
            value += w*g.after_stop[mid]
            if (g.ply+1,mid) in g.stops.unsupported:
                prior_mass += w
        for ch,p in probs.items():
            if p:
                arrivals[ch] += w*p
                parents[ch].append((h,mid,w*p))
    for h,w in arrivals.items():
        if h in leaves and leaves[h]!=search.answers[h]:
            raise ValueError("Prepared response differs from exhaustive local valuation")
        gain = search.gains[h] if h in leaves else 0.
        if h in roots:
            gain = g.cross_ply[h][roots[h]]
        value += w*(g.stop[h]+gain)
        exit_board = ((g.ply+3,roots[h]) if h in roots else
                      (g.ply+3,search.answer_hash[h]) if h in leaves else (g.ply+2,h))
        if exit_board in g.stops.unsupported:
            prior_mass += w
    shared = sorted((h for h in leaves if len(parents[h])>1),key=lambda h:-arrivals[h])
    examples = []
    for ch in shared[:3]:
        lines = []
        for h,mid,p in parents[ch]:
            prefix = g.histories[h][0][1]
            lines.append(dict(san=prefix+[g.options[h][mid][0],g.nodes[mid].legal[ch][0],leaves[ch]],reach=p))
        examples.append(dict(hash=ch,lines=lines))
    return dict(value=value,prior_exit_mass=prior_mass,cards=len(roots.keys()|leaves.keys()),shared_followups=len(shared),
                cross_ply_cards_revisited=sum(h in arrivals for h in roots),
                extra_incoming_routes=sum(len(parents[h])-1 for h in shared),examples=examples)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    base = Path("outputs/rebuild-2026-11-06")
    ap.add_argument("--inputs",type=Path,default=base/"black/depth-policy/round-00")
    ap.add_argument("--out",type=Path,required=True)
    ap.add_argument("--policy-cache",type=Path,required=True)
    ap.add_argument("--roots",type=int,default=64)
    ap.add_argument("--anchor-ply",type=int,choices=range(3,27,2),default=3)
    ap.add_argument("--budget",type=int,default=128)
    ap.add_argument("--tolerance",type=float,default=.001)
    ap.add_argument("--schedule",choices=["batched","iterative"],default="batched")
    ap.add_argument("--full-first",action="store_true")
    a = ap.parse_args()
    if min(a.roots,a.budget)<1 or not 0<a.tolerance<1:
        ap.error("Expected positive roots and budget, and a score tolerance between zero and one")
    a.out.mkdir(parents=True,exist_ok=True)
    t = time.perf_counter()
    data = Data()
    stops = Stops(a.inputs,data)
    maia = Maia(a.policy_cache)
    try:
        g = Slice(data,maia,stops,a.roots,a.anchor_ply)
    finally:
        maia.close()
    setup = time.perf_counter()-t
    print(f"Prepared {len(g.options)} roots, {len(g.nodes)} White boards, {len(g.stop)} follow-ups in {setup:.1f}s",flush=True)
    base_values,base_cp = stops.values.copy(),data.cp.copy()
    results = dict(roots=len(g.options),anchor_ply=a.anchor_ply,budget=a.budget,tolerance=a.tolerance,
                   coverage=g.coverage,white_boards=len(g.nodes),black_followups=len(g.stop),
                   maia_inferred=maia.inferred,setup_seconds=setup,setup_stops=dict(stops.counts),
                   inputs=dict(model_dir=str(a.inputs.resolve()),maia=maia.identity,
                               archive_metadata_sha256=data.pool_meta["book_meta_sha256"],
                               production_stop_recipe=stops.recipe),
                   cross_ply_positions=len(g.cross_ply),
                   empty_book={k:sum(w*g.root_stop[h] for h,w in g.weights[k].items()) for k in KS},
                   root_routes=g.histories,schedule=a.schedule,full_first=a.full_first,methods={})
    searches,books = {},{}
    for method in (("full","selective") if a.full_first else ("selective","full")):
        t = time.perf_counter()
        stops.values,data.cp = base_values.copy(),base_cp.copy()
        stops.counts.clear()
        search = Search(g,a.budget,a.tolerance)
        books[method],history = (search.batched() if method=="selective" and a.schedule=="batched"
                                 else search.run(method=="full"))
        searches[method] = search
        results["methods"][method] = dict(seconds=time.perf_counter()-t,statistics=dict(search.stats),
                                          stops=dict(stops.counts),archive_parents=len(search.exact),
                                          followups=len(search.gains),gaps=history)
        publish(a.out/f"{method}-books.json",books[method])
    results["comparison"] = {}
    for k in KS:
        scores = {}
        for method in books:
            b = books[method][k]
            scores[method] = replay(g,searches["full"],b,k)
            v = scores[method]["value"]
            if not b["lower"]-1e-7<=v<=b["upper"]+1e-7 or scores[method]["cards"]>a.budget:
                raise ValueError("Full replay outside certificate or study budget")
        sel,ref = books["selective"][k],books["full"][k]
        r1,r2 = dict(sel["roots"]),dict(ref["roots"])
        overlap = set(r1)&set(r2)
        cards1 = set(r1.items())|set(sel["leaves"].items())
        cards2 = set(r2.items())|set(ref["leaves"].items())
        if (scores["full"]["value"] > sel["upper"]+1e-7 or
                scores["selective"]["value"] > ref["upper"]+1e-7 or
                scores["full"]["value"]-scores["selective"]["value"] > a.tolerance+1e-7):
            raise ValueError("Exhaustive replay contradicts the selective bound")
        results["comparison"][k] = dict(scores=scores,shared_root_choices=len(overlap),
            matching_root_choices=sum(r1[h]==r2[h] for h in overlap),
            root_policy_differences=sum(r1.get(h)!=r2.get(h) for h in set(r1)|set(r2)),
            matching_cards=len(cards1&cards2), regret=scores["full"]["value"]-scores["selective"]["value"],
            regret_upper=ref["upper"]-scores["selective"]["value"])
    publish(a.out/"result.json",results)
    print(f"Verified results in {a.out}",flush=True)


if __name__=="__main__":
    main()
