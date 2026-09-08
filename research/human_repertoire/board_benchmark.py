"""Frozen M8 development, fresh holdout and paired tactical benchmark."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json

import chess
import numpy as np
from python.zobrist import zobrist_int64

from .board_prior import FEATURES, BoardPrior, fit_weights, panel, score, training_arrays
from .io import ROOT, output_path, sha256, write_json
from .shared_fallback import inputs, paired_bootstrap, identity as reference_identity
from . import date_cohort
from .loss_calibration import loss_rows, propose, select_cases, summaries, targets
from .trajectory import uniform

CONFIG = ROOT/'docs/human_repertoire/milestone8_config.json'


def read(name):
    return json.loads(output_path(name).read_text())


def now():
    return datetime.now(timezone.utc).isoformat()


def identity(c):
    return {'config': c, 'plan_sha256': sha256(ROOT/'docs/human_repertoire/milestone8_plan.md'),
        'code': {n: sha256(ROOT/'research/human_repertoire'/n) for n in
            ('board_prior.py', 'board_benchmark.py', 'date_cohort.py', 'cohort.py', 'shared_fallback.py',
             'loss_calibration.py', 'calibration_experiment.py', 'paired_panel.py', 'io.py')},
        'reference_model_sha256': sha256(output_path('m3/frozen_model.json')),
        'cohort_manifest_sha256': sha256(output_path('m3/cohort_manifest.json')),
        'chess_version': chess.__version__, 'numpy_version': np.__version__}


def reference():
    c3 = json.loads(date_cohort.CONFIG.read_text())
    model = read('m3/frozen_model.json')
    if model['identity'] != reference_identity(c3) or sha256(output_path('m3/fitted_counts.json')) != model['fitted_counts_sha256']:
        raise ValueError('reference drift')
    return read('m3/fitted_counts.json'), model['selected_specs']['shared_position']


def verified_model(c):
    model = read('m8/frozen_model.json')
    if model['identity'] != identity(c):
        raise ValueError('M8 frozen model drift')
    for path, digest in model['snapshots'].items():
        if sha256(output_path(path)) != digest:
            raise ValueError('M8 development snapshot drift')
    reference()
    return model


def develop(c):
    if output_path('m8/frozen_model.json').exists():
        verified_model(c)
        return
    fitted, setting = reference()
    data = inputs(json.loads(date_cohort.CONFIG.read_text()), ('train', 'tune'))
    train, dev = [panel(data[k], c, k) for k in ('train', 'tune')]
    kernel = BoardPrior(fitted, setting, c)
    arrays = training_arrays(train, kernel, c)
    print('Training', len(train), 'games;', len(arrays[2]), 'decisions;', len(arrays[0]), 'legal rows', flush=True)
    weights, history = fit_weights(arrays, c)
    strengths = {str(beta): beta for beta in c['strengths']}
    summary, rows = score(dev, BoardPrior(fitted, setting, c), weights, strengths, c)
    beta = min(c['strengths'], key=lambda b: (summary[str(b)]['pooled']['log_loss'], b))
    snapshots = {name: sha256(write_json(name, value)) for name, value in {
        'm8/development_scores.json': {'summary': summary, 'rows': rows},
        'm8/development_ids.json': {'train': [g['game_id'] for g in train], 'tune': [g['game_id'] for g in dev]}}.items()}
    np.savez_compressed(output_path('m8/training_arrays.npz'), x=arrays[0], logq=arrays[1], starts=arrays[2], chosen=arrays[3])
    snapshots['m8/training_arrays.npz'] = sha256(output_path('m8/training_arrays.npz'))
    model = {'identity': identity(c), 'weights': weights.tolist(), 'features': FEATURES, 'beta': beta,
        'training_loss_history': history, 'snapshots': snapshots, 'frozen_utc': now()}
    write_json('m8/frozen_model.json', model)
    print(json.dumps({'beta': beta, 'development': {k: v['pooled'] for k, v in summary.items()}, 'weights': model['weights']}, indent=2), flush=True)


def isolated(filename, old_prefix, new_prefix):
    spec = importlib.util.spec_from_file_location('research.human_repertoire._m8_'+filename.removesuffix('.py'), ROOT/'research/human_repertoire'/filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    def translated(name):
        if not name.startswith(old_prefix) or '..' in name.split('/'):
            raise ValueError('adapter output escaped phase')
        return new_prefix+name[len(old_prefix):]
    module.output_path = lambda name: output_path(translated(name))
    module.write_json = lambda name, value: write_json(translated(name), value)
    return module


def holdout(c):
    model = verified_model(c)  # Must happen before discovery/extraction.
    c3 = json.loads(date_cohort.CONFIG.read_text())
    c3.update(months={'test': c['test_month']}, games_per_frame=c['test_games_per_frame'],
              rank_seed=c['test_rank_seed'], max_decoded_rows=c['max_decoded_rows'])
    module = isolated('date_cohort.py', 'm3/', 'm8/cohort/')
    module.discover(c3)
    module.extract(c3)
    manifest = read('m8/cohort/cohort_manifest.json')
    if model['frozen_utc'] >= manifest['created_utc']:
        raise ValueError('model not frozen before test extraction')
    test_ids = {g['game_id'] for g in read('m8/cohort/test_games.json')}
    old = inputs(json.loads(date_cohort.CONFIG.read_text()), ('train', 'tune', 'test'))
    overlaps = {k: len(test_ids & {g['game_id'] for g in games}) for k, games in old.items()}
    if any(overlaps.values()):
        raise ValueError('holdout game leakage')
    write_json('m8/holdout_manifest.json', {'model_sha256': sha256(output_path('m8/frozen_model.json')),
        'cohort_sha256': sha256(output_path('m8/cohort/cohort_manifest.json')),
        'test_sha256': sha256(output_path('m8/cohort/test_games.json')), 'overlaps': overlaps})


def test_games(c):
    verified_model(c)
    p = read('m8/holdout_manifest.json')
    for field, name in [('model', 'm8/frozen_model.json'), ('cohort', 'm8/cohort/cohort_manifest.json'), ('test', 'm8/cohort/test_games.json')]:
        if p[field+'_sha256'] != sha256(output_path(name)):
            raise ValueError('holdout identity drift')
    return read('m8/cohort/test_games.json')


def final_test(c):
    model, games = verified_model(c), test_games(c)
    ident = {'model_sha256': sha256(output_path('m8/frozen_model.json')), 'holdout_sha256': sha256(output_path('m8/holdout_manifest.json'))}
    if output_path('m8/test_results.json').exists():
        previous = read('m8/test_results.json')
        if previous['identity'] != ident or previous['rows_sha256'] != sha256(output_path('m8/test_game_scores.json')):
            raise ValueError('test result drift')
        return
    fitted, setting = reference()
    summary, rows = score(games, BoardPrior(fitted, setting, c), np.array(model['weights']), {'reference': 0, 'candidate': model['beta']}, c)
    paired = paired_bootstrap(rows, 'candidate', 'reference', c['bootstrap_draws'], c['bootstrap_seed'])
    result = {'identity': ident, 'summary': summary, 'paired_bootstrap': paired,
        'rows_sha256': sha256(write_json('m8/test_game_scores.json', rows)), 'completed_utc': now()}
    write_json('m8/test_results.json', result)
    print(json.dumps(result, indent=2), flush=True)


def calibration_config(c):
    c7 = json.loads((ROOT/'docs/human_repertoire/milestone7_config.json').read_text())
    c7.update(test_month='2024-04', max_games=900, max_base_positions=384, max_deeper_positions=48,
        cases_per_stratum=c['calibration_cases_per_stratum'], deeper_cases_per_stratum=c['deeper_cases_per_stratum'],
        selection_seed=c['calibration_selection_seed'], within_game_seed=c['calibration_within_seed'], draw_seed=c['calibration_draw_seed'])
    for k in ('bootstrap_draws', 'bootstrap_seed', 'engine_nodes', 'engine_time_seconds', 'deeper_nodes', 'deeper_time_seconds', 'engine_runtime_seconds'):
        c7[k] = c[k]
    return c7


def calibration_identity(c):
    verified_model(c)
    return {'model_sha256': sha256(output_path('m8/frozen_model.json')), 'holdout_sha256': sha256(output_path('m8/holdout_manifest.json')),
        'config': calibration_config(c), 'm7_manifest_sha256': sha256(output_path('m7/case_manifest.json')),
        'm7_engine_sha256': sha256(output_path('m7/engine_audit.json'))}


def verified_calibration(c):
    p = read('m8/calibration/case_manifest.json')
    if p['identity'] != calibration_identity(c):
        raise ValueError('calibration identity drift')
    if any(sha256(output_path(k)) != h for k, h in p['snapshots'].items()):
        raise ValueError('calibration snapshot drift')
    return p


def prepare(c):
    from .paired_panel import CONFIG as M6_CONFIG, inherited, verified_panel, checked_records
    from .calibration_experiment import CONFIG as M7_CONFIG, verified_cases
    if output_path('m8/calibration/case_manifest.json').exists():
        verified_calibration(c)
        return
    model, games = verified_model(c), test_games(c)
    fitted, setting = reference()
    c7 = calibration_config(c)
    selected, frame = select_cases(games, fitted, c7)
    ref_cases, fens = propose(selected, fitted, {'shared_position': setting}, c7)
    candidate = []
    kernel = BoardPrior(fitted, setting, c)
    for case in ref_cases:
        sans, probs = kernel.probabilities(case['parent'], case['fen'], np.array(model['weights']), model['beta'])
        u = uniform(c7['draw_seed'], case['case_id'], 0, 'move')
        index = min(int(np.searchsorted(np.cumsum(probs), u, side='right')), len(sans)-1)
        board = chess.Board(case['fen']); board.push_san(sans[index])
        key = str(zobrist_int64(board))
        if key in fens and fens[key].split()[:4] != board.fen().split()[:4]:
            raise ValueError('candidate child FEN drift')
        fens.setdefault(key, board.fen())
        candidate.append({**case, 'model_san': sans[index], 'model_child': key,
            'human_move_probability': float(probs[sans.index(case['human_san'])]), 'model_move_probability': float(probs[index])})
    c6 = json.loads(M6_CONFIG.read_text()); p6 = verified_panel(c6)
    _, _, records = inherited(c6)
    p7 = verified_cases(json.loads(M7_CONFIG.read_text()))
    if p6['engine_identity'] != p7['engine_identity']:
        raise ValueError('inherited engine identity mismatch')
    for number, manifest, field in ((6, p6, 'panel_manifest_sha256'), (7, p7, 'case_manifest_sha256')):
        audit = read(f'm{number}/engine_audit.json')
        manifest_name = 'panel_manifest.json' if number == 6 else 'case_manifest.json'
        if audit['identity'] != {field: sha256(output_path(f'm{number}/'+manifest_name)), 'engine': manifest['engine_identity']}:
            raise ValueError('inherited engine audit drift')
        for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
            records[phase].update(checked_records(audit, phase, nodes, seconds))
    selection = targets(ref_cases+candidate, fens, records, c7)
    for phase in selection:
        selection[phase]['cases'] //= 2  # Two draws per same actual case.
    kept = {phase: {k: records[phase][k] for k in selection[phase]['reused']} for phase in records}
    snapshots = {name: sha256(write_json(name, value)) for name, value in {
        'm8/calibration/cases.json': {'reference': ref_cases, 'candidate': candidate},
        'm8/calibration/selection_frame.json': frame, 'm8/calibration/fens.json': fens,
        'm8/calibration/engine_selection.json': selection, 'm8/calibration/inherited_records.json': kept}.items()}
    p = {'identity': calibration_identity(c), 'snapshots': snapshots, 'frozen_utc': now(),
        **{k: p6[k] for k in ('engine_identity', 'engine_path', 'engine_options')}}
    write_json('m8/calibration/case_manifest.json', p)
    print(json.dumps({phase: {k: len(selection[phase][k]) for k in ('required', 'reused', 'targets')} for phase in selection}), flush=True)


def engine(c):
    module = isolated('calibration_experiment.py', 'm7/', 'm8/calibration/')
    module.verified_cases = lambda unused: verified_calibration(c)
    module.engine(calibration_config(c))


def compare(c):
    from .paired_panel import checked_records
    p = verified_calibration(c)
    audit = read('m8/calibration/engine_audit.json')
    if audit['identity'] != {'case_manifest_sha256': sha256(output_path('m8/calibration/case_manifest.json')), 'engine': p['engine_identity']}:
        raise ValueError('engine audit identity drift')
    selection, prior, cases = [read('m8/calibration/'+n+'.json') for n in ('engine_selection', 'inherited_records', 'cases')]
    c7 = calibration_config(c); result = {}; saved_rows = {}
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        fresh = checked_records(audit, phase, nodes, seconds)
        if set(fresh) != set(selection[phase]['targets']):
            raise ValueError('engine target coverage incomplete')
        scores = {**{k: r['cp'] for k, r in {**prior[phase], **fresh}.items()}, **selection[phase]['exact']}
        for label, subset_only in ([(phase, False), ('base_nested', True)] if phase == 'base' else [(phase, True)]):
            rows = {name: loss_rows([r for r in values if not subset_only or r['deeper']], scores, c7) for name, values in cases.items()}
            paired_cases = [{**b, 'human_san': a['model_san'], 'human_child': a['model_child']} for a, b in zip(cases['reference'], cases['candidate']) if not subset_only or b['deeper']]
            rows['candidate_vs_reference'] = loss_rows(paired_cases, scores, c7)
            result[label] = {name: summaries(values, c7, bootstrap=label == 'base') for name, values in rows.items()}
            saved_rows[label] = rows
    write_json('m8/calibration/scored_cases.json', saved_rows)
    write_json('m8/calibration/results.json', {'identity': calibration_identity(c), 'engine_sha256': sha256(output_path('m8/calibration/engine_audit.json')),
        'rows_sha256': sha256(output_path('m8/calibration/scored_cases.json')), 'comparisons': result,
        'paired_labels': {'human': 'reference draw', 'model': 'candidate draw'}, 'completed_utc': now()})
    print(json.dumps({k: v['groups']['balanced']['metrics']['loss_primary'] for k, v in result['base'].items()}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['develop', 'holdout', 'test', 'prepare', 'engine', 'compare'])
    args = parser.parse_args()
    globals()['final_test' if args.phase == 'test' else args.phase](json.loads(CONFIG.read_text()))


if __name__ == '__main__':
    main()
