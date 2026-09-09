"""First-hit bounds with exact cancellation of common continuation flow.

All paths reaching a state have known scores below both first-hit thresholds;
unknown scores absorb flow immediately, so incompatible unknown histories cannot
be merged. Keys include ply. This bounds a fixed continuation policy, not recall
recalibration or an unknown opponent policy outside the graph.
"""
from collections import defaultdict
from math import fsum, isfinite


def flow_bounds(graph, root_weights, horizon=4, discount=.99, threshold=300):
    graph.validate()
    root=graph.nodes[graph.root]
    if root.cp is None or abs(root.cp)>=threshold or root.side not in ('us','opp'):
        raise ValueError('requires a known no-hit decision boundary')
    if set(root_weights)!={e.move for e in root.edges} or any(not isfinite(v) for v in root_weights.values()):
        raise ValueError('root weights must cover all moves and be finite')
    if type(horizon) is not int or horizon<1 or not 0<discount<=1 or threshold<=0:
        raise ValueError('invalid objective')
    states=defaultdict(float)
    for edge in root.edges:
        states[edge.child]+=(1-root.end_probability)*root_weights[edge.move]
    known=0.; lower_unknown=0.; upper_unknown=0.; uncertain=defaultdict(float); visited=0
    for ply in range(1,horizon+1):
        following=defaultdict(float)
        for key,mass in states.items():
            visited+=1
            if not mass: continue
            node=graph.nodes[key]; cap=discount**ply
            if node.cp is None:
                lower_unknown+=min(0,mass)*cap; upper_unknown+=max(0,mass)*cap
                uncertain['missing_score']+=abs(mass); continue
            if node.cp>=threshold:
                known+=mass*cap; continue
            if node.cp<=-threshold: continue
            if node.side=='frontier' and ply<horizon:
                lower_unknown+=min(0,mass)*cap; upper_unknown+=max(0,mass)*cap
                uncertain['cutoff']+=abs(mass); continue
            if node.side not in ('us','opp') or ply==horizon: continue
            for edge in node.edges:
                following[edge.child]+=mass*(1-node.end_probability)*edge.probability
        states=following
    return {'interval':[known+lower_unknown,known+upper_unknown], 'known_contribution':known,
            'uncertainty_width':upper_unknown-lower_unknown,'absolute_unresolved_mass':dict(uncertain),
            'visited_states':visited}


def intervention(graph, move='Be3', **kwargs):
    probabilities={e.move:e.probability for e in graph.nodes[graph.root].edges}
    if move not in probabilities: raise ValueError('prepared move absent')
    return flow_bounds(graph,{m:float(m==move)-p for m,p in probabilities.items()},**kwargs)
