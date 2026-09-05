"""Independent subset/product book enumeration and explicit complete-path sums.

Does not call the production evaluator, policy mixture, gate, cost or objective.
It shares only input/output dataclasses. Intended exclusively for tiny fixtures.
"""
from itertools import combinations, product
from .model import Metrics


def path_evaluate(g, book, c):
    totals = {k: 0.0 for k in Metrics.__dataclass_fields__}
    def record(mass, wdl, reason, hit, unknown, unknown_before_hit):
        for name, outcome in zip(("win", "draw", "loss"), wdl):
            totals[name] += mass * outcome
        totals[reason] += mass
        totals["unknown_eval_mass"] += mass * unknown
        totals[hit[0] if hit else "no_hit"] += mass
        totals["unresolved_first_hit_mass"] += mass * unknown_before_hit
        if hit and hit[0] == "advantage":
            totals["early_advantage"] += mass * c.discount ** hit[1]
            totals["win_after_advantage"] += mass * wdl[0]
            if not unknown_before_hit:
                totals["certain_advantage_mass"] += mass
                totals["certain_early_advantage"] += mass * c.discount ** hit[1]
    stack = [(g.root, 1.0, [], None, True, False, False)]
    while stack:
        key, mass, path, hit, enabled, unknown, unknown_before_hit = stack.pop()
        n = g.nodes[key]
        depth = len(path)
        unknown |= n.cp is None
        unknown_before_hit |= hit is None and n.cp is None
        if hit is None and n.cp is not None:
            if n.cp >= c.threshold_cp:
                hit = ("advantage", depth)
            elif n.cp <= -c.threshold_cp:
                hit = ("disadvantage", depth)
        counts = sum(previous == key for previous in path) + 1
        if n.side == "terminal":
            reason, wdl = "terminal_mass", n.wdl
        elif c.repetition_visits is not None and counts >= c.repetition_visits:
            reason, wdl = "repetition_mass", (0, 1, 0)
        elif n.side == "human_terminal":
            reason, wdl = "human_end_mass", n.wdl
        elif n.side == "frontier":
            reason, wdl = "frontier_mass", n.wdl
        elif depth >= c.horizon:
            reason, wdl = "horizon_mass", n.wdl
        else:
            if n.end_probability:
                record(mass * n.end_probability, n.end_wdl, "human_end_mass", hit, unknown, unknown_before_hit)
                mass *= 1-n.end_probability
            if n.side == "opp":
                moves = [(e, c.opponent.get(key, {}).get(e.move, e.probability), enabled) for e in n.edges]
            else:
                probability_remembered = 0
                if key in book and (c.reentry or enabled):
                    probability_remembered = c.recall_by_position.get(key, c.recall)
                moves = []
                for e in n.edges:
                    fallback = c.fallback.get(key, {}).get(e.move, e.probability)
                    moves.append((e, (1 - probability_remembered) * fallback, c.reentry))
                    if key in book and e.move == book[key]:
                        moves.append((e, probability_remembered, True))
            for e, probability, next_enabled in moves:
                if probability > 0:
                    stack.append((e.child, mass * probability, path + [key], hit, next_enabled, unknown, unknown_before_hit))
            continue
        record(mass, wdl, reason, hit, unknown, unknown_before_hit)
        if reason == "frontier_mass" and depth < c.horizon:
            totals["frontier_before_horizon_mass"] += mass
            if hit is None:
                totals["unresolved_no_hit_mass"] += mass
                if not unknown_before_hit:
                    totals["unresolved_first_hit_mass"] += mass
    return Metrics(**totals)


def enumerate_books(g, c, budget, allowed=None):
    choices = {}
    for key, n in g.nodes.items():
        if n.side != "us" or n.cp is None:
            continue
        moves = [e.move for e in n.edges if e.sound_cp is not None
                 and e.sound_cp >= c.soundness_floor_cp and n.cp - e.sound_cp <= c.max_engine_loss_cp]
        if allowed is not None:
            moves = [m for m in moves if m in allowed.get(key, ())]
        if moves:
            choices[key] = moves
    keys = sorted(choices)
    for size in range(len(keys) + 1):
        for subset in combinations(keys, size):
            if sum(g.nodes[k].cost for k in subset) > budget:
                continue
            for moves in product(*(choices[k] for k in subset)):
                yield dict(zip(subset, moves))


def oracle_best(g, c, budget, allowed=None):
    best = None
    for book in enumerate_books(g, c, budget, allowed):
        m = path_evaluate(g, book, c)
        cost = sum(g.nodes[k].cost for k in book)
        if m.points + 1e-12 < c.min_points or m.disadvantage - 1e-12 > c.max_disadvantage:
            continue
        score = (round(m.early_advantage, 12), round(m.points, 12), -cost)
        if best is None or score > best[0]:
            best = score, book, m
    return best
