"""Compare contextual recall on frozen empirical graphs; no inference or scans."""
import argparse
from dataclasses import asdict, replace
import json

from . import exposure_real as previous
from .context_recall import ContextCurve, solve
from .io import ROOT, output_path, sha256, write_json
from .model import Config, graph_from_dict


PREFIX = 'context_recall/'
ROOT_KEY = '8704797333742910878'
CARD_KEY = '-1867097936539676342'


def identity():
    return {
        'code': {name: sha256(ROOT/'research/human_repertoire'/name)
                 for name in ('context_recall.py', 'context_comparison.py')},
        'plan_sha256': sha256(ROOT/'docs/human_repertoire/context_recall_plan.md'),
        'source_manifest_sha256': sha256(output_path(previous.PREFIX+'manifest.json')),
        'source_results_sha256': sha256(output_path(previous.PREFIX+'results.json')),
        'curve': asdict(ContextCurve()), 'anchors': {ROOT_KEY: .99}, 'root_ply': 4,
    }


def compare():
    manifest = previous.verified()
    source = previous.read('results')
    if source['identity'] != manifest['identity']:
        raise ValueError('original result identity drift')
    if source['manifest_sha256'] != sha256(output_path(previous.PREFIX+'manifest.json')):
        raise ValueError('original result manifest drift')
    if sha256(output_path(previous.PREFIX+'results.json')) != previous.read('verification')['results_sha256']:
        raise ValueError('original result is not the independently verified result')
    resolved = identity()
    graphs = previous.read('graphs'); fens = previous.read('fens')
    results = {}; sensitivity = {}
    cfg = Config(horizon=manifest['horizon'], recall=.85)
    for event, data in graphs.items():
        graph = graph_from_dict(data)
        fields = fens[graph.root].split()
        actual_ply = 2*(int(fields[5])-1)+(fields[1] == 'b')
        if graph.root != ROOT_KEY or actual_ply != resolved['root_ply']:
            raise ValueError('wrong opening boundary for this personal profile')
        results[event] = {}
        for entry in manifest['entries']:
            results[event][str(entry)] = {}
            for name, book in manifest['books'].items():
                new = solve(graph, book, cfg, entry, anchors=resolved['anchors'], root_ply=resolved['root_ply'])
                metrics = new['conditional_metrics']
                metrics['early_interval'] = [metrics['certain_early_advantage'],
                    min(1, metrics['certain_early_advantage']+metrics['unresolved_first_hit_mass'])]
                results[event][str(entry)][name] = {
                    'contextual': new, 'frequency_only': source['results'][event][str(entry)][name]['exposure']}
            print(event, entry, 'complete', flush=True)
        sensitivity[event] = {}
        for ceiling in (.90, .95, .98):
            new = solve(graph, manifest['books']['Nf6_Nc3'], cfg, .01,
                        anchors=resolved['anchors'], root_ply=resolved['root_ply'],
                        curve=replace(ContextCurve(), ceiling=ceiling))
            sensitivity[event][str(ceiling)] = new
        if results[event]['0.01']['Nf6_Nc3']['contextual']['recall'][CARD_KEY] < .95:
            raise ValueError('failed user calibration constraint')
    if resolved != identity():
        raise ValueError('inputs changed during comparison')
    write_json(PREFIX+'results.json', {'identity': resolved, 'books': manifest['books'],
                                      'results': results, 'sensitivity': sensitivity})


def read_results():
    previous.verified()
    result = json.loads(output_path(PREFIX+'results.json').read_text())
    if result['identity'] != identity():
        raise ValueError('context comparison identity drift')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['compare'])
    parser.parse_args()
    compare()
