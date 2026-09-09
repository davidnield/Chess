"""Independent complete-path reconstruction of contextual recall and exposure.

Does not use the contextual solver, support function, or policy mixture helper.
Checks the saved position-level policy for self-consistency; original graph
legality and empirical mass checks remain bound by the source manifest.
"""
from collections import defaultdict
from dataclasses import replace
from math import isclose

from . import exposure_real as previous
from .context_comparison import PREFIX, read_results
from .io import output_path, sha256, write_json
from .model import Config, graph_from_dict
from .oracle import path_evaluate


def path_context(graph, book, cfg, entry, recall, anchors, root_ply, parameters):
    arrivals = defaultdict(list)
    stack = [(graph.root, 0, entry, None, 0)]
    processed = 0
    while stack:
        key, ply, mass, anchor, anchor_ply = stack.pop()
        if mass == 0:
            continue
        processed += 1
        if processed > cfg.max_states:
            raise ValueError('independent path cap')
        if key in anchors:
            anchor, anchor_ply = key, ply
        arrivals[key].append((mass, ply, anchor, anchor_ply))
        node = graph.nodes[key]
        if ply == cfg.horizon or node.side not in ('us', 'opp'):
            continue
        for edge in node.edges:
            probability = edge.probability
            if node.side == 'us' and key in book:
                probability = (1-recall[key])*probability + recall[key]*(edge.move == book[key])
            stack.append((edge.child, ply+1, mass*(1-node.end_probability)*probability, anchor, anchor_ply))
    def base(f):
        return f**parameters['frequency']['power'] / (
            f**parameters['frequency']['power']+parameters['frequency']['midpoint']**parameters['frequency']['power'])
    frequencies = {key: sum(row[0] for row in arrivals[key]) for key in book}
    anchor_frequencies = {key: sum(row[0] for row in arrivals[key]) for key in anchors}
    strengths = {key: max(value, base(anchor_frequencies[key])) for key, value in anchors.items()}
    support = {}
    expected = {}
    for key, frequency in frequencies.items():
        weighted = 0.
        for mass, ply, anchor, anchor_ply in arrivals[key]:
            if anchor is None:
                continue
            distance_decay = .5**(max(0, ply-anchor_ply-parameters['local_grace_plies'])/parameters['local_half_life_plies'])
            depth_decay = .5**(max(0, root_ply+ply-parameters['shallow_through_ply'])/parameters['depth_half_life_plies'])
            weighted += mass*strengths[anchor]*distance_decay*depth_decay
        support[key] = weighted/frequency if frequency else 0.
        expected[key] = base(frequency)+(1-base(frequency))*parameters['ceiling']*support[key]
    return frequencies, anchor_frequencies, support, expected


def main():
    result = read_results(); profile = result['identity']; cfg = Config(horizon=3, recall=.85)
    graphs = previous.read('graphs'); checks = 0; cases = 0
    for event, raw in graphs.items():
        graph = graph_from_dict(raw)
        cases_to_check = [(book, float(entry), row['contextual'], profile['curve'])
                          for entry, books in result['results'][event].items()
                          for name, row in books.items() for book in [result['books'][name]]]
        cases_to_check.extend((result['books']['Nf6_Nc3'], .01, row,
                               {**profile['curve'], 'ceiling': float(ceiling)})
                              for ceiling, row in result['sensitivity'][event].items())
        for book, entry, row, curve in cases_to_check:
            frequencies, anchor_frequencies, support, expected = path_context(
                graph, book, cfg, entry, row['recall'], profile['anchors'], profile['root_ply'], curve)
            for key in book:
                for actual, target in [(row['frequencies'][key], frequencies[key]),
                                       (row['recall'][key], expected[key]),
                                       (row['components'][key]['mean_support'], support[key])]:
                    assert isclose(actual, target, rel_tol=1e-10, abs_tol=1e-12)
                    checks += 1
            for key in profile['anchors']:
                assert isclose(row['anchor_frequencies'][key], anchor_frequencies[key], abs_tol=1e-12)
                checks += 1
            metrics = path_evaluate(graph, book, replace(cfg, recall_by_position=row['recall'])).report()
            for key, value in metrics.items():
                target = row['conditional_metrics'][key]
                assert value is None and target is None or value is not None and isclose(value, target, abs_tol=1e-10)
                checks += 1
            assert isclose(metrics['advantage']+metrics['disadvantage']+metrics['no_hit'], 1, abs_tol=1e-10)
            assert isclose(metrics['win']+metrics['draw']+metrics['loss'], 1, abs_tol=1e-10)
            assert isclose(row['per_game_early_advantage_contribution'], entry*metrics['early_advantage'], abs_tol=1e-12)
            cases += 1
    report = {'status': 'passed', 'cases': cases, 'scalar_checks': checks,
              'results_sha256': sha256(output_path(PREFIX+'results.json')),
              'verifier_sha256': sha256(__file__)}
    write_json(PREFIX+'verification.json', report)
    print(report)


if __name__ == '__main__':
    main()
