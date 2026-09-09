"""Encounter-based recall with exact DAG propagation and checked cyclic solves."""
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from math import fsum, isfinite
from .model import evaluate


@dataclass(frozen=True)
class RecallCurve:
    midpoint: float = .001
    power: float = 2.

    def __post_init__(self):
        if not isfinite(self.midpoint) or not 0 < self.midpoint <= 1 or not isfinite(self.power) or self.power <= 0:
            raise ValueError('invalid recall curve')

    def __call__(self, frequency):
        if not isfinite(frequency) or not 0 <= frequency <= 1: raise ValueError('frequency must be per-game probability')
        if frequency == 0: return 0.
        # Stable at very small frequencies; all inputs here are probabilities.
        ratio = frequency/self.midpoint
        if ratio <= 1:
            value = ratio**self.power; return value/(1+value)
        return 1/(1+(1/ratio)**self.power)


def action_probabilities(node, key, book, recall):
    r = recall.get(key, 0.) if node.side == 'us' and key in book else 0.
    return [(e, (1-r)*e.probability+r*(e.move == book.get(key))) for e in node.edges]


def validate(graph, book, cfg, entry):
    graph.validate(); cfg.validate(graph)
    if not isfinite(entry) or not 0 <= entry <= 1: raise ValueError('invalid entry probability')
    if not cfg.reentry or cfg.fallback or cfg.opponent: raise ValueError('requires re-entry and graph policies')
    for key,move in book.items():
        if key not in graph.nodes or graph.nodes[key].side != 'us' or move not in {e.move for e in graph.nodes[key].edges}:
            raise ValueError('invalid book prescription')


def topological(graph):
    reachable=set(); pending=[graph.root]
    while pending:
        k=pending.pop()
        if k in reachable: continue
        reachable.add(k); pending.extend(e.child for e in graph.nodes[k].edges if e.probability >= 0)
    incoming={k:0 for k in reachable}
    for k in reachable:
        for e in graph.nodes[k].edges: incoming[e.child]+=1
    ready=deque(sorted(k for k,n in incoming.items() if n==0)); order=[]
    while ready:
        k=ready.popleft(); order.append(k)
        for e in graph.nodes[k].edges:
            incoming[e.child]-=1
            if incoming[e.child]==0: ready.append(e.child)
    return order if len(order)==len(reachable) else None


def fixed_recall_exposure(graph, book, cfg, entry, recall):
    """Exact per-game first encounter probability, including transpositions/cycles."""
    validate(graph,book,cfg,entry)
    frequency={k:0. for k in book}; states={(graph.root,(),frozenset()):entry}; visited_states=0
    for ply in range(cfg.horizon+1):
        following=defaultdict(float)
        for (key,history,seen),mass in states.items():
            visited_states+=1
            if visited_states>cfg.max_states: raise ValueError('exposure state cap; no mass dropped')
            if not mass: continue
            if key in book and key not in seen: frequency[key]+=mass; seen=seen|{key}
            node=graph.nodes[key]; visits=dict(history); visits[key]=visits.get(key,0)+1
            if node.side not in ('us','opp') or ply==cfg.horizon or (cfg.repetition_visits is not None and visits[key]>=cfg.repetition_visits): continue
            for edge,probability in action_probabilities(node,key,book,recall):
                following[edge.child,tuple(sorted(visits.items())),seen]+=mass*(1-node.end_probability)*probability
        states=following
    return frequency


def solve(graph, book, cfg, entry_probability=1., curve=RecallCurve(), tolerance=1e-10, iterations=1000):
    validate(graph,book,cfg,entry_probability)
    order=topological(graph)
    if order is not None:
        mass=defaultdict(lambda:[0.]*(cfg.horizon+1)); mass[graph.root][0]=entry_probability
        frequencies={k:0. for k in book}; recall={k:0. for k in book}
        for key in order:
            if key in book:
                frequencies[key]=min(1.,fsum(mass[key])); recall[key]=curve(frequencies[key])
            node=graph.nodes[key]
            if node.side not in ('us','opp'): continue
            for edge,p in action_probabilities(node,key,book,recall):
                for ply in range(cfg.horizon): mass[edge.child][ply+1]+=mass[key][ply]*(1-node.end_probability)*p
        method='exact_dag'; residual=0.; steps=1
    else:
        solutions=[]
        for start in (0.,1.):
            recall={k:start for k in book}
            for steps in range(1,iterations+1):
                frequencies=fixed_recall_exposure(graph,book,cfg,entry_probability,recall)
                target={k:curve(f) for k,f in frequencies.items()}
                residual=max((abs(target[k]-recall[k]) for k in book),default=0.)
                if residual<=tolerance: break
                recall={k:(recall[k]+target[k])/2 for k in book}
            else: raise ValueError('cyclic recall did not converge; no result accepted')
            solutions.append((recall,frequencies,residual,steps))
        if max((abs(solutions[0][0][k]-solutions[1][0][k]) for k in book),default=0.)>100*tolerance:
            raise ValueError('multiple cyclic recall solutions detected')
        recall,frequencies,residual,steps=solutions[0]; method='cyclic_two_start_checked_not_uniqueness_proof'
    conditional=evaluate(graph,book,replace(cfg,recall_by_position=recall)).report()
    return {'entry_probability':entry_probability,'frequencies':frequencies,'recall':recall,'method':method,
        'residual':residual,'iterations':steps,'conditional_metrics':conditional,
        'per_game_early_advantage_contribution':entry_probability*conditional['early_advantage']}
