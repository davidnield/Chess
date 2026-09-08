"""Frozen Maia-1400 marginal study-card comparison using the existing evaluator."""
import argparse
from dataclasses import replace
import json
import time

import chess

from . import paired_panel as panel
from .board_benchmark import isolated
from .io import ROOT, output_path, sha256, write_json
from .maia_policy import CONFIG as MAIA_CONFIG, MaiaPolicy, MaiaFallbackKernels
from .model import Config
from .trajectory import ChessBackend, sample_trace, score_trace, mean_se

CONFIG = ROOT/'docs/human_repertoire/maia_comparison_config.json'
PREFIX = 'maia_compare/'


def read(name):
    return json.loads(output_path(PREFIX+name).read_text())


def save(name, value):
    return write_json(PREFIX+name, value)


def identity(c):
    return {'config': c, 'plan_sha256': sha256(ROOT/'docs/human_repertoire/maia_comparison_plan.md'),
        'code': {name: sha256(ROOT/'research/human_repertoire'/name) for name in
            ('maia_comparison.py', 'maia_policy.py', 'trajectory.py', 'paired_panel.py', 'model.py')},
        'maia_config_sha256': sha256(MAIA_CONFIG), 'maia_verification_sha256': sha256(output_path('maia2/integration_verification.json')),
        'm6_manifest_sha256': sha256(output_path('m6/panel_manifest.json')), 'm6_engine_sha256': sha256(output_path('m6/engine_audit.json'))}


def source(c):
    c6 = json.loads(panel.CONFIG.read_text()); p6 = panel.verified_panel(c6)
    base, _, records = panel.inherited(c6)
    audit = json.loads(output_path('m6/engine_audit.json').read_text())
    if audit['identity'] != panel.audit_identity(p6):
        raise ValueError('M6 engine identity drift')
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        records[phase].update(panel.checked_records(audit, phase, nodes, seconds))
    return c6, base, records


def select_targets(paths, fens, gate_keys, records, c, cfg):
    contexts = sorted({arm.rsplit('/', 1)[0] for arm in paths})
    combined = {phase: {'required': [], 'reused': [], 'targets': [], 'exact_terminals': []} for phase in ('base', 'deeper')}
    for context in contexts:
        subset = {name: paths[context+'/'+name] for name in c['plans']}
        selection = panel.target_selection(subset, fens, gate_keys, records, {
            'panel_trajectories_per_arm': c['pairs'], 'deeper_pairs': c['deeper_pairs'],
            'max_base_positions': c['max_positions'], 'max_deeper_positions': c['max_deeper_positions'], 'config': cfg})
        for phase in combined:
            for field in combined[phase]:
                combined[phase][field].extend(selection[phase][field])
    for phase in combined:
        combined[phase] = {k: list(dict.fromkeys(v)) for k, v in combined[phase].items()}
        cap = c['max_positions'] if phase == 'base' else c['max_deeper_positions']
        if len(combined[phase]['required'])+len(combined[phase]['exact_terminals']) > cap:
            raise ValueError('union position cap; no paths dropped')
    return combined


def verified(c):
    p = read('case_manifest.json')
    if p['identity'] != identity(c) or any(sha256(output_path(PREFIX+k)) != h for k, h in p['snapshots'].items()):
        raise ValueError('Maia comparison identity/snapshot drift')
    return p


def prepare(c):
    if output_path(PREFIX+'case_manifest.json').exists():
        verified(c); return
    c6, base, records = source(c)
    c4, prep, train, fitted, specs, _, _ = base
    root = next(iter(prep['books']['source_one_card']))
    extra = next(k for k in prep['books']['two_cards'] if k != root)
    policy = MaiaPolicy(); paths, tables, local, fens = {}, {}, {}, {}
    started = time.monotonic()
    for name in c['policies']:
        backend = ChessBackend(train['fens'][root], fitted, specs, train, c4, c['max_positions'])
        if name != 'shared_position':
            backend.kernels = MaiaFallbackKernels(policy, 'Blitz' if name == 'maia_blitz' else 'Rapid', chess.WHITE,
                c['opponent_rating_input'], backend.kernels, 'shared_position', cap=c['max_positions'])
        model = 'shared_position' if name == 'shared_position' else 'maia1400'
        _, ps = backend.kernels.get(extra, train['fens'][extra])
        local[name] = {'extra_position': extra, 'fen': train['fens'][extra], 'move': prep['books']['two_cards'][extra],
            'fallback_probability': ps[model][prep['books']['two_cards'][extra]]}
        for recall in c['recalls']:
            cfg = replace(Config(**c6['config']), recall=recall)
            for book in c['plans']:
                arm = name+'/r'+str(recall)+'/'+book
                values = []
                for index in range(c['pairs']):
                    if time.monotonic()-started > c['sampling_seconds']:
                        raise TimeoutError('Maia comparison sampling guard')
                    values.append(sample_trace(backend, model, prep['books'][book], cfg, c['seed'], index))
                paths[arm] = values
            print(name, recall, 'complete;', len(backend.fens), 'positions', flush=True)
        tables[name] = backend.kernels.cache
        for key, fen in backend.fens.items():
            if key in fens and fens[key].split()[:4] != fen.split()[:4]:
                raise ValueError('cross-policy FEN drift')
            fens.setdefault(key, fen)
    gates, gate_fens = panel.gate_definitions(base)
    fens = {**gate_fens, **fens}
    selection = select_targets(paths, fens, gate_fens, records, c, c6['config'])
    kept = {phase: {k: records[phase][k] for k in selection[phase]['reused']} for phase in records}
    snapshots = {name: sha256(save(name, value)) for name, value in {
        'traces.json': paths, 'policy_tables.json': tables, 'local_probabilities.json': local, 'fens.json': fens,
        'engine_selection.json': selection, 'inherited_records.json': kept}.items()}
    p = {'identity': identity(c), 'snapshots': snapshots, 'gates': gates, 'books': prep['books'], 'evaluator_config': c6['config'],
        'sampling_seconds': time.monotonic()-started,
        'engine_identity': prep['engine_identity'], 'engine_path': c4['engine_path'],
        'engine_options': {**c4['engine_options'], 'SyzygyPath': ''}}
    save('case_manifest.json', p)
    print(json.dumps({'local': local, 'targets': {k: len(v['targets']) for k, v in selection.items()}}, indent=2), flush=True)


def engine(c):
    module = isolated('calibration_experiment.py', 'm7/', PREFIX)
    module.verified_cases = lambda unused: verified(c)
    module.engine(c)


def compare(c):
    p = verified(c); audit = read('engine_audit.json')
    if audit['identity'] != {'case_manifest_sha256': sha256(output_path(PREFIX+'case_manifest.json')), 'engine': p['engine_identity']}:
        raise ValueError('Maia comparison engine identity mismatch')
    paths, prior, selection = read('traces.json'), read('inherited_records.json'), read('engine_selection.json')
    maps = {}
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        fresh = panel.checked_records(audit, phase, nodes, seconds)
        if set(fresh) != set(selection[phase]['targets']):
            raise ValueError('Maia comparison incomplete engine coverage')
        maps[phase] = {k: r['cp'] for k, r in {**prior[phase], **fresh}.items()}
    results = {}; scored = {}
    for name in c['policies']:
        for recall in c['recalls']:
            context = name+'/r'+str(recall); cfg = replace(Config(**p['evaluator_config']), recall=recall)
            subset = {book: paths[context+'/'+book] for book in c['plans']}
            comparison = panel.score_panel(subset, maps['base'], cfg, c['alpha'], True)
            rows = {book: [score_trace(t, maps['base'], cfg) for t in traces] for book, traces in subset.items()}
            comparison['two_minus_one_disadvantage'] = mean_se([a['disadvantage']-b['disadvantage'] for a, b in zip(rows['two_cards'], rows['source_one_card'])])
            q = read('local_probabilities.json')[name]['fallback_probability']
            comparison['local_extra_card'] = {'fallback_probability': q, 'with_card_probability': recall+(1-recall)*q,
                'action_total_variation_at_encounter': recall*(1-q),
                'paths_revisiting_extra_position': sum(t['keys'].count(read('local_probabilities.json')[name]['extra_position']) > 1 for traces in subset.values() for t in traces)}
            nested = {book: traces[:c['deeper_pairs']] for book, traces in subset.items()}
            comparison['deeper_subset'] = {phase: panel.score_panel(nested, scores, cfg, c['alpha'], True) for phase, scores in maps.items()}
            results[context] = comparison; scored[context] = rows
    gates = {phase: panel.check_gates(p['gates'], scores, Config(**p['evaluator_config'])) for phase, scores in maps.items()}
    if any(not g['passed'] for values in gates.values() for g in values):
        raise ValueError('book soundness gate failed')
    result = {'identity': identity(c), 'engine_sha256': sha256(output_path(PREFIX+'engine_audit.json')),
        'contexts': results, 'gates': gates, 'scored_sha256': sha256(save('scored.json', scored))}
    save('results.json', result)
    print(json.dumps({k: {'early': v['two_minus_one']['early_advantage'], 'downside': v['two_minus_one_disadvantage'], 'local': v['local_extra_card']} for k, v in results.items()}, indent=2), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('phase', choices=('prepare', 'engine', 'compare'))
    args = parser.parse_args(); globals()[args.phase](json.loads(CONFIG.read_text()))
