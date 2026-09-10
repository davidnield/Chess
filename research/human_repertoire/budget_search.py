"""Bounded package search with candidate-dependent contextual recall.

The real search is heuristic; tiny instances may be enumerated independently.
Frozen historical experiments are not imported by this active workflow.
"""
from collections import defaultdict
from dataclasses import replace
from math import fsum
import time
from .context_recall import ContextCurve, solve
from .exposure_recall import topological
from .model import eligible, evaluate


def signature(book): return tuple(sorted(book.items()))


class DagScorer:
    """Compiled layered-DAG scorer, with exact generic fallback for other DAGs.

Search needs known first-advantage mass and unresolved mass, not placeholder
W/D/L. Only clean no-hit mass propagates for this objective; all arrival mass
propagates separately for recall. Final books use the full common evaluator.
"""
    def __init__(self, graph, cfg, entry, anchors=None, root_ply=0, curve=ContextCurve()):
        graph.validate(); cfg.validate(graph)
        if not cfg.reentry or cfg.fallback or cfg.opponent: raise ValueError('requires graph policies and reentry')
        if not 0<=entry<=1: raise ValueError('invalid entry')
        self.g=graph; self.cfg=cfg; self.entry=entry; self.anchors=dict(anchors or {}); self.root_ply=root_ply; self.curve=curve
        for k,v in self.anchors.items():
            if k not in graph.nodes or graph.nodes[k].side not in ('us','opp') or not 0<=v<=1: raise ValueError('invalid anchor')
        order=topological(graph)
        if order is None: raise ValueError('requires DAG')
        self.order=order; self.index={k:i for i,k in enumerate(order)}; self.depth={graph.root:0}; self.layered=True
        for k in order:
            for e in graph.nodes[k].edges:
                depth=self.depth[k]+1
                if e.child in self.depth and self.depth[e.child]!=depth: self.layered=False
                self.depth.setdefault(e.child,depth)
        self.allowed={k:set(eligible(graph,cfg,k)) for k in order if graph.nodes[k].side=='us'}
        self.compiled=[(k,graph.nodes[k],self.depth[k],[(self.index[e.child],e.move,e.probability) for e in graph.nodes[k].edges]) for k in order]

    def score(self, book, perfect=False):
        if any(move not in self.allowed.get(k,set()) for k,move in book.items()): raise ValueError('uncertified candidate')
        if not self.layered:
            metrics=(evaluate(self.g,book,replace(self.cfg,recall=1,recall_by_position={ })).report() if perfect else
                solve(self.g,book,self.cfg,self.entry,anchors=self.anchors,root_ply=self.root_ply,curve=self.curve)['conditional_metrics'])
            return {'score':metrics['certain_early_advantage'],'unresolved':metrics['unresolved_first_hit_mass']}
        n=len(self.order); arrivals=[None]*n; clean=[0.]*n
        i=self.index[self.g.root]; arrivals[i]={(None,0):self.entry}; clean[i]=1.
        strengths={}; advantage=0.; bad=0.; unresolved=0.; state_count=0
        for i,(k,node,ply,edges) in enumerate(self.compiled):
            routes=arrivals[i] or {}; frequency=min(1.,fsum(routes.values())); probability=clean[i]
            state_count+=len(routes)
            if state_count>self.cfg.max_states: raise ValueError('context state cap')
            if k in self.anchors:
                strengths[k]=max(self.anchors[k],self.curve.frequency(frequency)); routes={(k,ply):frequency}
            recall=0.
            if k in book:
                if perfect: recall=1.
                else:
                    support=fsum(mass*self.curve.support(strengths[a],ply-origin,self.root_ply+ply)
                                 for (a,origin),mass in routes.items() if a is not None)/frequency if frequency else 0.
                    recall=self.curve.recall(frequency,min(1.,support))
            if node.cp is None: unresolved+=probability; probability=0.
            elif node.cp>=self.cfg.threshold_cp: advantage+=probability*self.cfg.discount**ply; probability=0.
            elif node.cp<=-self.cfg.threshold_cp: bad+=probability; probability=0.
            if node.side=='frontier' and ply<self.cfg.horizon: unresolved+=probability; probability=0.
            if node.side not in ('us','opp') or ply>=self.cfg.horizon: continue
            for child,move,fallback in edges:
                p=((1-recall)*fallback+recall*(move==book.get(k)))*(1-node.end_probability)
                if p==0: continue
                clean[child]+=probability*p
                if frequency:
                    if arrivals[child] is None: arrivals[child]=defaultdict(float)
                    for context,mass in routes.items(): arrivals[child][context]+=mass*p
        return {'score':advantage,'unresolved':unresolved}


class CommonScorer:
    def __init__(self, scorers): self.scorers=scorers
    def score(self,book,perfect=False):
        rows=[s.score(book,perfect) for s in self.scorers]
        return {k:fsum(r[k] for r in rows)/len(rows) for k in rows[0]}


def rank(result, book, costs):
    # A common lower-bound objective; uncertainty is reported, never rewarded.
    return (round(result['score'],12),-sum(costs[k] for k in book))


def search(scorer, candidates, packets, budgets, reference=None, costs=None,
           perfect=False, max_evaluations=1800, max_seconds=180, proposal_width=48):
    costs=costs or {k:1 for k in candidates}; reference=reference or {}; cache={}; total_calls=0
    if any(type(b) is not int or b<0 for b in budgets) or max_evaluations<1 or max_seconds<=0 or proposal_width<1:
        raise ValueError('invalid search limits')
    def cost(book): return sum(costs[k] for k in book)
    def valid(book): return all(m in candidates.get(k,()) for k,m in book.items())
    if not valid(reference): raise ValueError('reference outside candidates')
    operations={signature({k:m}) for k,moves in candidates.items() for m in moves}
    operations.update(signature(p) for p in packets if valid(p))
    operations=sorted(operations)
    def score(book):
        nonlocal total_calls
        s=signature(book)
        if s not in cache: cache[s]=scorer.score(book,perfect); total_calls+=1
        return cache[s]
    results={}; previous={}; priorities={}; empty=score({})
    for budget in sorted(set(budgets)):
        started=time.monotonic(); initial_calls=total_calls; trace=[]; stopped='local_plateau'
        seed={}
        for k,m in reference.items():
            if cost(seed)+costs[k]<=budget: seed[k]=m
        choices=[{},previous,seed]; book=max(choices,key=lambda x:rank(score(x),x,costs)).copy()
        best=score(book); trace.append({'cost':cost(book),'score':best['score'],'kind':'seed'})
        # Outcome estimates for packet screening are cached, and included in the cap.
        for operation in operations:
            if operation in priorities or cost(dict(operation))>budget: continue
            if total_calls-initial_calls>=max_evaluations or time.monotonic()-started>=max_seconds: break
            packet=dict(operation); value=score(packet)
            priorities[operation]=(value['score']-empty['score'])/max(1,cost(packet))
        while True:
            if total_calls-initial_calls>=max_evaluations: stopped='evaluation_cap'; break
            if time.monotonic()-started>=max_seconds: stopped='time_cap'; break
            proposals={}; remaining=budget-cost(book)
            for operation in operations:
                packet=dict(operation)
                if cost(packet)>budget: continue
                candidate={**book,**packet}
                if cost(candidate)>budget:
                    # Deterministic bounded replacement: remove low-priority incumbent
                    # cards outside the packet until the package fits.
                    removable=sorted((k for k in book if k not in packet),
                        key=lambda k:(priorities.get(((k,book[k]),),0),k))
                    for k in removable:
                        if cost(candidate)<=budget: break
                        del candidate[k]
                if candidate==book or cost(candidate)>budget: continue
                s=signature(candidate)
                proposals[s]=max(proposals.get(s,float('-inf')),priorities.get(operation,0))
            for k in book:
                removed={x:m for x,m in book.items() if x!=k}; proposals.setdefault(signature(removed),0.)
            ordered=sorted(proposals,key=lambda s:(-proposals[s],s))
            evaluated=[]
            # Explore a rotating slice beyond the static top proposals to avoid starving branches.
            offset=(len(trace)-1)*max(1,proposal_width//2)
            chosen=list(dict.fromkeys(ordered[:proposal_width//2]+ordered[offset:offset+proposal_width]))
            for s in chosen:
                if total_calls-initial_calls>=max_evaluations or time.monotonic()-started>=max_seconds: break
                candidate=dict(s); value=score(candidate); evaluated.append((rank(value,candidate,costs),candidate,value))
            if not evaluated: stopped='search_cap_or_empty'; break
            _,next_book,value=max(evaluated,key=lambda x:x[0])
            if rank(value,next_book,costs)<=rank(best,book,costs):
                # The first plateau triggers a full remaining-neighborhood check within the cap.
                improved=False
                for s in ordered:
                    if s in chosen: continue
                    if total_calls-initial_calls>=max_evaluations or time.monotonic()-started>=max_seconds: break
                    candidate=dict(s); v=score(candidate)
                    if rank(v,candidate,costs)>rank(best,book,costs): next_book,value=candidate,v; improved=True; break
                if not improved:
                    stopped='evaluation_cap' if total_calls-initial_calls>=max_evaluations else 'time_cap' if time.monotonic()-started>=max_seconds else 'local_plateau'
                    break
            book=next_book; best=value; trace.append({'cost':cost(book),'score':best['score'],'kind':'package_update'})
        previous=book.copy()
        results[str(budget)]={'book':book,'cost':cost(book),'selection_metrics':best,'trace':trace,
                              'evaluations':total_calls-initial_calls,'seconds':time.monotonic()-started,'stopped':stopped,
                              'global_optimum_proven':False}
    return results
