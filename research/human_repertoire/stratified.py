"""Exact prefix enumeration and weighted continuation sampling.

Preserves original visit history, horizon and first-hit times. The initial
implementation requires re-entry: active-book history is then irrelevant.
"""
from math import log, sqrt
from .trajectory import categorical, uniform, mean_se


def stopped(view, keys, ply, cfg):
    node = view.node
    reason = ('terminal' if node.side == 'terminal' else
        'repetition' if cfg.repetition_visits is not None and keys.count(view.key)+1 >= cfg.repetition_visits else
        'human_end' if node.side == 'human_terminal' else 'frontier' if node.side == 'frontier' else
        'horizon' if ply == cfg.horizon else None)
    return reason, (0, 1, 0) if reason == 'repetition' else node.wdl


def enumerate_prefixes(backend, model, book, cfg, depth=2, cap=2000):
    if not cfg.reentry or cfg.fallback or cfg.opponent:
        raise ValueError('prefix sampling requires re-entry and backend policies')
    if depth < 0: raise ValueError('negative prefix depth')
    result = []
    def walk(state, keys, moves, mass):
        if not mass: return
        view = backend.view(state, model); ply = len(moves)
        reason, ending = stopped(view, keys, ply, cfg)
        entry = {'keys': keys+[view.key], 'moves': moves, 'probability': mass,
            'reason': reason, 'wdl': list(ending), 'exact_cp': view.exact_cp}
        if reason or ply == depth:
            if len(result) >= cap: raise ValueError('prefix cap; no mass dropped')
            result.append(entry); return
        hazard = view.node.end_probability
        if hazard:
            if len(result) >= cap: raise ValueError('prefix cap; no mass dropped')
            result.append({**entry, 'probability': mass*hazard, 'reason': 'human_end', 'wdl': list(view.node.end_wdl)})
        recall = cfg.recall_by_position.get(view.key, cfg.recall) if view.node.side == 'us' and view.key in book else 0
        if recall and book[view.key] not in {e.move for e in view.node.edges}: raise ValueError('illegal prescribed move')
        for edge in sorted(view.node.edges, key=lambda e:e.move):
            probability = (1-recall)*edge.probability + recall*(book.get(view.key) == edge.move)
            walk(backend.advance(state, edge), entry['keys'], moves+[edge.move], mass*(1-hazard)*probability)
    walk(backend.initial, [], [], 1.0)
    if abs(sum(p['probability'] for p in result)-1) > 1e-10: raise ValueError('prefix mass not conserved')
    return result


def conditional_prefix(prefixes, u):
    if not 0 <= u < 1: raise ValueError('invalid prefix draw')
    total = sum(p['probability'] for p in prefixes)
    if total <= 0: raise ValueError('empty stratum')
    cumulative = 0
    for prefix in prefixes:
        cumulative += prefix['probability']/total
        if u < cumulative: return prefix
    return prefixes[-1]


def continue_trace(backend, model, book, cfg, seed, index, prefix):
    if not cfg.reentry: raise ValueError('prefix sampling requires re-entry')
    state = backend.initial; keys = []; moves = list(prefix['moves'])
    for ply, move in enumerate(moves):
        view = backend.view(state, model)
        if view.key != prefix['keys'][ply]: raise ValueError('prefix state drift')
        keys.append(view.key)
        edge = next(e for e in view.node.edges if e.move == move)
        state = backend.advance(state, edge)
    if backend.view(state, model).key != prefix['keys'][-1]: raise ValueError('prefix endpoint drift')
    if prefix['reason']:
        return {'index': index, **{k:prefix[k] for k in ('keys', 'moves', 'reason', 'wdl', 'exact_cp')}}
    for ply in range(len(moves), cfg.horizon+1):
        view = backend.view(state, model); node = view.node
        reason, ending = stopped(view, keys, ply, cfg); keys.append(view.key)
        if not reason and uniform(seed, index, ply, 'end') < node.end_probability:
            reason, ending = 'human_end', node.end_wdl
        if reason:
            return {'index': index, 'keys': keys, 'moves': moves, 'reason': reason,
                'wdl': list(ending), 'exact_cp': view.exact_cp}
        recall = cfg.recall_by_position.get(view.key, cfg.recall) if node.side == 'us' and view.key in book else 0
        if uniform(seed, index, ply, 'recall') < recall:
            edge = next(e for e in node.edges if e.move == book[view.key])
        else: edge = categorical(sorted(node.edges, key=lambda e:e.move), uniform(seed,index,ply,'move'))
        moves.append(edge.move); state = backend.advance(state, edge)
    raise AssertionError('continuation exceeded horizon')


def weighted_summary(values, weights, alpha=.05, difference=False):
    if set(values) != set(weights) or abs(sum(weights.values())-1) > 1e-10 or any(w < 0 for w in weights.values()):
        raise ValueError('stratum weights must partition unit mass')
    if not 0 < alpha < 1: raise ValueError('invalid alpha')
    limits = (-1,1) if difference else (0,1)
    if any(not limits[0] <= x <= limits[1] for xs in values.values() for x in xs): raise ValueError('value outside declared range')
    parts = {s:mean_se(xs) for s,xs in values.items()}
    mean = sum(weights[s]*v['mean'] for s,v in parts.items())
    se = sqrt(sum((weights[s]*v['mc_se'])**2 for s,v in parts.items()))
    width = 2 if difference else 1
    margin = width*sqrt(log(2/alpha)/2*sum(weights[s]**2/len(xs) for s,xs in values.items()))
    return {'mean': mean, 'mc_se': se, 'hoeffding_margin': margin,
        'mc_outer': [max(limits[0],mean-margin), min(limits[1],mean+margin)], 'strata': parts}
