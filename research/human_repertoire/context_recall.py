"""Position-level recall from exposure and route-weighted familiar context.

Only acyclic graphs are supported. Explicit anchors are personal profile inputs,
not automatically inferred from population popularity. Original frozen solvers
are unchanged. See context_recall_plan.md for the calibration assumptions.
"""
from collections import defaultdict
from dataclasses import dataclass, replace
from math import fsum, isfinite

from .exposure_recall import RecallCurve, action_probabilities, topological, validate
from .model import evaluate


@dataclass(frozen=True)
class ContextCurve:
    frequency: RecallCurve = RecallCurve()
    ceiling: float = .98
    local_grace_plies: int = 2
    local_half_life_plies: float = 4.
    shallow_through_ply: int = 8
    depth_half_life_plies: float = 8.

    def __post_init__(self):
        if not isinstance(self.frequency, RecallCurve):
            raise ValueError('frequency must be a RecallCurve')
        if not isfinite(self.ceiling) or not 0 <= self.ceiling <= 1:
            raise ValueError('invalid contextual ceiling')
        for value in (self.local_grace_plies, self.shallow_through_ply):
            if type(value) is not int or value < 0:
                raise ValueError('invalid ply boundary')
        for value in (self.local_half_life_plies, self.depth_half_life_plies):
            if not isfinite(value) or value <= 0:
                raise ValueError('invalid half life')

    def support(self, strength, distance, absolute_ply):
        if not isfinite(strength) or not 0 <= strength <= 1:
            raise ValueError('invalid familiarity strength')
        if any(type(v) is not int or v < 0 for v in (distance, absolute_ply)):
            raise ValueError('invalid distance or depth')
        local = 2 ** (-max(0, distance-self.local_grace_plies)/self.local_half_life_plies)
        depth = 2 ** (-max(0, absolute_ply-self.shallow_through_ply)/self.depth_half_life_plies)
        return strength * local * depth

    def recall(self, frequency, mean_support):
        if not isfinite(mean_support) or not 0 <= mean_support <= 1:
            raise ValueError('invalid contextual support')
        base = self.frequency(frequency)
        return base + (1-base)*self.ceiling*mean_support


def solve(graph, book, cfg, entry_probability=1., *, anchors=None,
          root_ply=0, curve=ContextCurve()):
    """Forward solution, pooling context across routes before choosing actions.

States retain arrival ply and most recent anchor/arrival ply. All incoming
mass is available before evaluating a position, so its frequency and recall
are determined before they affect descendants. DAG visits are automatically
first visits. No local conditional frequencies are substituted for per-game
exposure; anchor strength is a separate, explicit personal-familiarity prior.
"""
    validate(graph, book, cfg, entry_probability)
    if type(root_ply) is not int or root_ply < 0:
        raise ValueError('root_ply must be a nonnegative integer')
    anchors = dict(anchors or {})
    for key, strength in anchors.items():
        if key not in graph.nodes or graph.nodes[key].side not in ('us', 'opp'):
            raise ValueError('anchor must be a decision position in the graph')
        if not isfinite(strength) or not 0 <= strength <= 1:
            raise ValueError('anchor strength must be a probability')
    order = topological(graph)
    if order is None:
        raise ValueError('contextual recall requires an acyclic graph')
    incoming = defaultdict(lambda: defaultdict(float))
    incoming[graph.root][0, None, 0] = entry_probability
    frequencies = {key: 0. for key in book}
    recall = {key: 0. for key in book}
    components = {key: {'frequency_recall': 0., 'mean_support': 0., 'boost': 0.}
                  for key in book}
    anchor_frequencies = {key: 0. for key in anchors}
    strengths = {}
    processed = 0
    for key in order:
        arrivals = incoming.pop(key, {})
        processed += len(arrivals)
        if processed > cfg.max_states:
            raise ValueError('context state cap; no mass dropped')
        frequency = min(1., fsum(arrivals.values()))
        if key in anchors:
            anchor_frequencies[key] = frequency
            strengths[key] = max(anchors[key], curve.frequency(frequency))
            # Reaching a declared tabiya resets the context for this route.
            reset = defaultdict(float)
            for (ply, _, _), mass in arrivals.items():
                reset[ply, key, ply] += mass
            arrivals = reset
        if key in book:
            support_mass = fsum(
                mass*curve.support(strengths[anchor], ply-anchor_ply, root_ply+ply)
                for (ply, anchor, anchor_ply), mass in arrivals.items() if anchor is not None)
            support = min(1., support_mass/frequency) if frequency else 0.
            frequencies[key] = frequency
            recall[key] = curve.recall(frequency, support)
            components[key] = {'frequency_recall': curve.frequency(frequency),
                               'mean_support': support, 'boost': curve.ceiling*support}
        node = graph.nodes[key]
        if node.side not in ('us', 'opp'):
            continue
        for (ply, anchor, anchor_ply), mass in arrivals.items():
            if ply >= cfg.horizon or mass == 0:
                continue
            for edge, probability in action_probabilities(node, key, book, recall):
                flow = mass*(1-node.end_probability)*probability
                if flow:
                    incoming[edge.child][ply+1, anchor, anchor_ply] += flow
    conditional = evaluate(graph, book, replace(cfg, recall_by_position=recall)).report()
    return {'entry_probability': entry_probability, 'frequencies': frequencies,
            'recall': recall, 'components': components, 'anchor_frequencies': anchor_frequencies,
            'anchor_strengths': strengths, 'method': 'exact_dag_pooled_context',
            'processed_states': processed, 'conditional_metrics': conditional,
            'per_game_early_advantage_contribution': entry_probability*conditional['early_advantage']}
