"""Root-led Black search with a bounded, independently exhaustible study budget."""
import argparse
from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
import time

import chess
import numpy as np

from root_pilot_data import Data, Maia, successors, zobrist_int64, POOL, WEAK, ARCHIVE
from weak_stop import uniform_score
from run_practical_rebuild import publish
from run_opponent_prior_challenger import lock

KS = (0,30)
BUDGETS = (1,2,4)
TOL = .001
OPENING_RANKS = (0,1,2,4,6,9)
SECOND_PANEL = (3,5,7,8,10,12)
AUDIT_PLIES = (1,5,9,11)
METHOD = {"perspective":"Black","candidate_source":"all legal Black moves with cached evaluations passing the gates",
          "plies":AUDIT_PLIES,"opening_frequency_ranks_zero_based":OPENING_RANKS,"budgets":BUDGETS,"K":KS,
          "tolerance_score":TOL,"speedup_target":2,"material_margin":TOL,
          "scope":"One booked Black move, one White reply, optional prepared Black response. Budgets per subtree.",
          "conditioning":"Given a White reply. With no historical reply both arms stop at the parent evaluation.",
          "leaf":"One empirical weak Black move, then cached engine score. Static fallback below 20 weak games.",
          "timing":"Common discovery and cheap-input staging shared. Independent selective then exhaustive runs."}


@dataclass
class Chance:
    epd: str
    legal: dict
    q: dict


def chance_inputs(data,maia,boards,known):
    fresh = {h:e for h,e in boards.items() if h not in known}
    if not fresh:
        return
    data.populate(fresh)
    policies = maia.get(fresh)
    for h,e in fresh.items():
        legal = successors(e)
        if data.coarse[h][0].keys()-legal.keys():
            raise ValueError(f"Invalid historical successor at {h}")
        probs = {ch:policies[h][san.rstrip("+#")] for ch,(san,_) in legal.items()}
        mass = sum(probs.values())
        if legal and (mass<=0 or len(probs)!=len(policies[h])):
            raise ValueError(f"Invalid legal policy at {h}")
        known[h] = Chance(e,legal,{ch:p/mass for ch,p in probs.items()})


def discovery_policy(coarse,node,k):
    """Five-game estimate for ordering routes only. The solver bounds residual mass."""
    counts,n = coarse
    if not n:
        return node.q
    residual = n-sum(counts.values())
    missing = {ch:p for ch,p in node.q.items() if ch not in counts}
    support = missing if sum(missing.values()) else node.q
    mass = sum(support.values())
    return {ch:(counts.get(ch,0)+k*p+(residual*support.get(ch,0)/mass if mass else 0))/(n+k)
            for ch,p in node.q.items()}


@dataclass
class Route:
    h: int
    epd: str
    moves: tuple
    opening: int
    arm: int
    reach: float


def discover(data,maia,opening_ranks):
    root = chess.Board()
    start = zobrist_int64(root)
    nodes = {}
    chance_inputs(data,maia,{start:root.epd()},nodes)
    ranked = sorted(nodes[start].legal,key=lambda ch:(-data.coarse[start][0].get(ch,0),ch))
    routes = []
    for group,rank in enumerate(opening_ranks):
        ch = ranked[rank]
        san,e = nodes[start].legal[ch]
        for k in KS:
            p = discovery_policy(data.coarse[start],nodes[start],k)[ch]
            routes.append(Route(ch,e,(san,),group,k,p))
    audit = {}
    total_routes = 0
    for ply in range(1,max(AUDIT_PLIES)+1,2):
        choices = data.candidates({r.h:r.epd for r in routes})
        options = {ch:e for cs in choices.values() for ch,(_,e) in cs.items()}
        chance_inputs(data,maia,options,nodes)
        following = []
        for r in routes:
            cs = choices[r.h]
            if not cs:
                continue
            if ply in AUDIT_PLIES:
                audit.setdefault(r.h,(r,cs))
            total_routes += 1
            if ply==max(AUDIT_PLIES):
                continue
            # Vary the eligible Black move and White response rank by opening.
            # Popular, middling and lower-ranked branches all reach the oracle.
            candidates = sorted(cs,key=lambda ch:(data.cp[ch],cs[ch][0]))
            offset = (r.opening+ply//2)%3 if r.opening>=2 else 0
            for mid in candidates[offset:]+candidates[:offset]:
                policy = discovery_policy(data.coarse[mid],nodes[mid],r.arm)
                replies = sorted((ch for ch in policy if policy[ch]>0),
                                 key=lambda ch:(-policy[ch],nodes[mid].legal[ch][0]))
                if not replies:
                    continue
                idx = min((0,0,1,2,3,4)[r.opening],len(replies)-1)
                child = replies[idx]
                san,e = nodes[mid].legal[child]
                following.append(Route(child,e,(*r.moves,cs[mid][0],san),r.opening,r.arm,r.reach*policy[child]))
                break
        routes = following
    return nodes,audit,total_routes


def engine_score(cp):
    return float(uniform_score(-cp)) if cp is not None else .5


@dataclass
class Leaf:
    stop: float
    answer: str
    gain: float


@dataclass
class Bounds:
    low: float
    high: float
    cards: tuple
    requests: list


class Search:
    def __init__(self,data,nodes,roots,archive_cost=20):
        self.data,self.nodes,self.roots = data,nodes,roots
        self.leaves,self.exact = {},{}
        self.stops = {}
        self.archive_cost = archive_cost
        self.stats = Counter()

    def prepare_stops(self,keys):
        todo = set(keys)-self.stops.keys()
        self.data.evals(todo.union(*(set(self.data.weak[h]) for h in todo)))
        for h in todo:
            base = engine_score(self.data.cp[h])
            end = chess.Board(self.epds[h]+" 0 1").outcome()
            if end:
                base = .5 if end.winner is None else float(end.winner==chess.BLACK)
            else:
                weak = self.data.weak[h]
                if sum(weak.values())>=20:
                    self.stats["weak_empirical"] += 1
                    base = sum(n*(engine_score(self.data.cp[ch]) if self.data.cp[ch] is not None else base)
                               for ch,n in weak.items())/sum(weak.values())
                else:
                    self.stats["weak_static"] += 1
                    self.stats["weak_neutral"] += int(self.data.cp[h] is None)
            self.stops[h] = base

    def no_reply(self,h):
        end = chess.Board(self.nodes[h].epd+" 0 1").outcome()
        if end:
            return .5 if end.winner is None else float(end.winner==chess.BLACK)
        return engine_score(self.data.cp[h])

    def expand(self,requests):
        raw = {h:self.nodes[h].epd for typ,h in requests if typ=="archive" and h not in self.exact}
        if raw:
            t = time.perf_counter()
            self.exact.update(self.data.exact_counts(raw))
            self.stats["archive_seconds"] += time.perf_counter()-t
        todo = {h:self.epds[h] for typ,h in requests if typ=="leaf" and h not in self.leaves}
        if not todo:
            return
        t = time.perf_counter()
        self.prepare_stops(todo)
        candidates = self.data.candidates(todo)
        for h,cs in candidates.items():
            base = self.stops[h]
            best = sorted(cs,key=lambda ch:(self.data.cp[ch],cs[ch][0]))
            score = engine_score(self.data.cp[best[0]]) if best else base
            self.leaves[h] = Leaf(base,cs[best[0]][0] if best else "",max(0,score-base))
        self.stats["leaf_seconds"] += time.perf_counter()-t

    def distribution(self,h,k):
        coarse,n = self.data.coarse[h]
        if h in self.exact:
            coarse = self.exact[h]
            n = sum(coarse.values())
        if n is None or n==0:
            return {},None if n is None else 0
        weights = {ch:(coarse.get(ch,0)+k*p)/(n+k) for ch,p in self.nodes[h].q.items()}
        unknown = (n-sum(coarse.values()))/(n+k)
        if abs(sum(weights.values())+unknown-1)>1e-8:
            raise ValueError("Incomplete reply distribution")
        return weights,unknown

    def bound(self,h,k,budget):
        weights,residual = self.distribution(h,k)
        if residual is None:
            return Bounds(0,1,(),[(1,"archive",h)])
        if not weights:
            v = self.no_reply(h)
            return Bounds(v,v,(),[])
        base = sum(p*self.stops[ch] for ch,p in weights.items() if p)
        guaranteed,possible = [],[]
        for ch,p in weights.items():
            if p<=0:
                continue
            leaf = self.leaves.get(ch)
            if leaf is None:
                possible.append((p*(1-self.stops[ch]),ch))
            elif leaf.gain>0:
                guaranteed.append((p*leaf.gain,ch))
                possible.append((p*leaf.gain,ch))
        key = lambda item:(-item[0],item[1])
        bought = sorted(guaranteed,key=key)[:budget-1]
        optimistic = sorted(possible,key=key)[:budget-1]
        cards = tuple(ch for _,ch in bought)
        low = base+sum(g for g,_ in bought)
        high = base+sum(g for g,_ in optimistic)
        requests = [(g,"leaf",ch) for g,ch in optimistic if ch not in self.leaves and g>0]
        # An unexpanded answer can improve the stop value by at most 1-stop.
        # Only B-1 replies can be prepared. The residual historical mass may
        # also belong to a retained child (different SAN spellings in the archive).
        if residual:
            floor = min(self.stops[ch]+(self.leaves[ch].gain if ch in cards else 0) for ch in weights)
            ceiling = max((self.stops[ch] if budget==1 else
                           self.stops[ch]+self.leaves[ch].gain if ch in self.leaves else 1.) for ch in weights)
            low += residual*floor
            high += residual*ceiling
            if ceiling>floor:
                requests.append((residual*(ceiling-floor),"archive",h))
        return Bounds(low,high,cards,requests)

    def solve(self,full):
        self.epds = {ch:e for _,cs in self.roots.values() for h in cs
                     for ch,(_,e) in self.nodes[h].legal.items()}
        if full:
            raw = {("archive",h) for _,cs in self.roots.values() for h in cs
                   if self.data.coarse[h][1]!=sum(self.data.coarse[h][0].values())}
            self.expand(raw)
            active = {("leaf",ch) for _,cs in self.roots.values() for h in cs
                      for ch,p in self.distribution(h,30)[0].items() if p>0}
            self.expand(active)
        else:
            self.prepare_stops(self.epds)
        for step in range(500):
            result,work = {},set()
            for rh,(_,cs) in self.roots.items():
                for k in KS:
                    for budget in BUDGETS:
                        bounds = {h:self.bound(h,k,budget) for h in cs}
                        chosen = min(cs,key=lambda h:(-bounds[h].low,cs[h][0]))
                        choice = bounds[chosen]
                        high = max(row.high for row in bounds.values())
                        result[rh,k,budget] = (chosen,choice.cards,choice.low,high)
                        if high-choice.low <= (1e-10 if full else TOL):
                            continue
                        if full:
                            raise ValueError("Exhaustive solve left unresolved probability")
                        # Union of all competitive alternatives at both K values
                        # and all budgets. No best-route reach multiplier is needed.
                        for row in bounds.values():
                            if row.high>choice.low+TOL:
                                # Reading nine archive slices costs far more than looking
                                # up one response in the engine cache. Avoid issuing a
                                # tiny archive request just to fill the fourth batch slot.
                                ranked = sorted(((width/(self.archive_cost if kind=="archive" else 1),kind,h)
                                                for width,kind,h in row.requests),reverse=True)
                                for priority,kind,h in ranked[:4]:
                                    if priority >= ranked[0][0]/4:
                                        work.add((kind,h))
            if not work:
                self.stats.update(rounds=step,raw_parents=len(self.exact),evaluated_leaves=len(self.leaves))
                return result
            self.expand(work)
        raise RuntimeError("Refinement did not converge")

    def score_policy(self,h,k,cards):
        w,rem = self.distribution(h,k)
        if rem:
            raise ValueError("Oracle distribution incomplete")
        if not w:
            return self.no_reply(h)
        return sum(p*(self.leaves[ch].stop+(self.leaves[ch].gain if ch in cards else 0))
                   for ch,p in w.items() if p>0)


def run(out,panel="main",full_first=False,policy_cache=None):
    start = time.perf_counter()
    ranks = OPENING_RANKS if panel=="main" else SECOND_PANEL
    order = ("full","selective") if full_first else ("selective","full")
    design = {**METHOD,"panel":panel,"opening_frequency_ranks_zero_based":ranks,
              "bound":"Fixed unaided values with a budget-constrained upper bound on prepared replies.",
              "schedule":"Archive work costs 20 response lookups for ranking. Batch priorities within 4x of the highest.",
              "timing":f"Shared root discovery and input staging. Independent solve order: {order}."}
    publish(out/"design.json",design)
    data = Data()
    maia = Maia(policy_cache or out)
    try:
        nodes,roots,route_count = discover(data,maia,ranks)
        print(f"Discovered {len(roots)} study roots and {len(nodes)} White positions",flush=True)
    finally:
        maia.close()
    if len(roots)<8 or not any(r.opening>=3 for r,_ in roots.values()):
        raise RuntimeError("Insufficient independent root coverage")
    mids = {h for _,cs in roots.values() for h in cs}
    leaves = {ch for h in mids for ch in nodes[h].legal}
    data.weak_counts(leaves)
    cp_seed = data.cp.copy()
    shared = time.perf_counter()-start
    answers,engines,times = {},{},{}
    for mode in order:
        data.cp = cp_seed.copy()
        engine = Search(data,nodes,roots)
        print(f"Starting {mode} expansion for {len(mids)} candidate positions",flush=True)
        t = time.perf_counter()
        answers[mode] = engine.solve(mode=="full")
        times[mode] = time.perf_counter()-t
        engines[mode] = engine
        print(mode,round(times[mode],2),dict(engine.stats),flush=True)
    oracle = engines["full"]
    cases = []
    for (rh,k,budget),(move,cards,lo,hi) in answers["selective"].items():
        best,exact_cards,optimal,_ = answers["full"][rh,k,budget]
        route,cs = roots[rh]
        if any(engines["selective"].leaves[ch].answer != oracle.leaves[ch].answer for ch in cards):
            raise ValueError("Prepared response differs between independent leaf searches")
        realized = oracle.score_policy(move,k,cards)
        if not lo-1e-9<=realized<=optimal+1e-9 or optimal>hi+1e-9 or optimal-realized>TOL+1e-9:
            raise ValueError(f"Invalid pruning certificate at {rh,k,budget}")
        alternatives = [oracle.bound(h,k,budget).low for h in cs if h!=best]
        gap = optimal-max(alternatives) if alternatives else None
        def book(h,paid):
            return {str(ch):oracle.leaves[ch].answer for ch in paid}
        cases.append(dict(root=rh,line=" ".join(route.moves),opening_rank=ranks[route.opening]+1,
                          ply=len(route.moves),k=k,budget=budget,candidates=len(cs),selective=cs[move][0],
                          full=cs[best][0],selective_cards=book(move,cards),full_cards=book(best,exact_cards),
                          best_gap=gap,realized=realized,optimal=optimal,regret=max(0,optimal-realized),
                          lower=lo,upper=hi,match=move==best,book_match=move==best and set(cards)==set(exact_cards)))
    publish(out/"cases.json",cases)
    material = [c for c in cases if c["best_gap"] is not None and c["best_gap"]>TOL]
    result = dict(design=design,subtrees=len(roots),cases=len(cases),ranked_routes=route_count,
                  candidate_white_positions=len(mids),legal_reply_positions=len(leaves),
                  maia_positions=len(maia.used),new_maia_positions=maia.inferred,model=maia.identity,
                  shared_setup_seconds=shared,solve_seconds=times,
                  inclusive_speedup=(shared+times["full"])/(shared+times["selective"]),
                  refinement_speedup=times["full"]/times["selective"],
                  work={m:dict(e.stats) for m,e in engines.items()},root_matches=sum(c["match"] for c in cases),
                  entire_book_matches=sum(c["book_match"] for c in cases),
                  material_choices=len(material),material_matches=sum(c["match"] for c in material),
                  max_regret=max(c["regret"] for c in cases),mean_regret=np.mean([c["regret"] for c in cases]),
                  max_interval_width=max(c["upper"]-c["lower"] for c in cases),pool=str(POOL),weak_pool=str(WEAK),
                  archive=str(ARCHIVE))
    publish(out/"result.json",result)
    print(json.dumps(result),flush=True)


if __name__=="__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out",type=Path,required=True)
    ap.add_argument("--panel",choices=("main","second"),default="main")
    ap.add_argument("--full-first",action="store_true")
    ap.add_argument("--policy-cache",type=Path,help="Share a directory of model-fingerprinted Maia predictions across pilots")
    args = ap.parse_args()
    args.out.mkdir(parents=True,exist_ok=True)
    with lock(args.out/"run.lock"):
        run(args.out,args.panel,args.full_first,args.policy_cache)
