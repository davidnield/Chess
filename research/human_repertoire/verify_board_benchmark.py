"""Independent scalar reconstruction and replay of frozen M8 evidence."""
from collections import Counter
from datetime import datetime
import json
import math
import statistics

import chess
import numpy as np
from python.zobrist import zobrist_int64

from .board_benchmark import CONFIG, calibration_config, read, reference, test_games, verified_calibration, verified_model
from .board_prior import features, fit_weights
from .io import output_path, sha256, write_json
from .shared_fallback import distribution, move_token, paired_bootstrap
from .loss_calibration import METRICS, select_cases, summaries
from .trajectory import uniform
from .paired_panel import CONFIG as M6_CONFIG, inherited


def main():
    c = json.loads(CONFIG.read_text()); model = verified_model(c)
    games = test_games(c); by_id = {g['game_id']: g for g in games}
    fitted, setting = reference(); p = verified_calibration(c); c7 = calibration_config(c)
    arrays = np.load(output_path('m8/training_arrays.npz'))
    w, history = fit_weights(tuple(arrays[k] for k in ('x', 'logq', 'starts', 'chosen')), c)
    np.testing.assert_allclose(w, model['weights'], atol=1e-12, rtol=0)
    np.testing.assert_allclose(history, model['training_loss_history'], atol=1e-12, rtol=0)
    dev = read('m8/development_scores.json')['summary']
    assert model['beta'] == min(c['strengths'], key=lambda b: (dev[str(b)]['pooled']['log_loss'], b))
    cohort = read('m8/cohort/cohort_manifest.json')
    assert datetime.fromisoformat(model['frozen_utc']) < datetime.fromisoformat(cohort['created_utc'])
    rows, test = read('m8/test_game_scores.json'), read('m8/test_results.json')
    assert test['rows_sha256'] == sha256(output_path('m8/test_game_scores.json'))
    assert len(rows) == len(games) == 900
    assert {r['game_id'] for r in rows} == set(by_id)
    for name in ('reference', 'candidate'):
        for group in ('pooled', 'event:Blitz', 'event:Rapid', 'event:Classical'):
            subset = [r for r in rows if group == 'pooled' or 'event:'+r['event'] == group]
            n = sum(r['models'][name]['decisions'] for r in subset)
            assert n == test['summary'][name][group]['decisions']
            for metric in ('log_loss', 'brier'):
                value = math.fsum(r['models'][name][metric] for r in subset)/n
                assert abs(value-test['summary'][name][group][metric]) < 1e-12
    assert paired_bootstrap(rows, 'candidate', 'reference', c['bootstrap_draws'], c['bootstrap_seed']) == test['paired_bootstrap']
    cases, fens, selection, prior, audit, scored, result = [read('m8/calibration/'+n+'.json') for n in
        ('cases', 'fens', 'engine_selection', 'inherited_records', 'engine_audit', 'scored_cases', 'results')]
    assert audit['complete']
    assert result['rows_sha256'] == sha256(output_path('m8/calibration/scored_cases.json'))
    assert result['engine_sha256'] == sha256(output_path('m8/calibration/engine_audit.json'))
    assert datetime.fromisoformat(p['frozen_utc']) < datetime.fromisoformat(audit['frozen_utc']) < datetime.fromisoformat(result['completed_utc'])
    selected, frame = select_cases(games, fitted, c7)
    assert frame == read('m8/calibration/selection_frame.json')
    assert len(selected) == 96 and all(len(values) == 96 for values in cases.values())
    _, _, inherited_records = inherited(json.loads(M6_CONFIG.read_text()))
    for number in (6, 7):
        saved_audit = read(f'm{number}/engine_audit.json')
        for phase in ('base', 'deeper'):
            inherited_records[phase].update(saved_audit[phase])
    visited = set(); probability_checks = 0
    for name, values in cases.items():
        for raw, case in zip(selected, values):
            assert all(case[k] == v for k, v in raw.items())
            game = by_id[case['game_id']]
            b = chess.Board()
            for san in ['e4', 'e5', 'Nf3', 'Nc6']+game['moves'][:case['ply']]:
                b.push_san(san)
            assert b.fen().split()[:4] == case['fen'].split()[:4]
            assert str(zobrist_int64(b)) == case['parent']
            assert game['moves'][case['ply']] == case['human_san']
            moves = sorted(b.legal_moves, key=lambda m: b.san(m)); sans = [b.san(m) for m in moves]
            counts = fitted['positions'].get(case['parent'], {})
            q = distribution([m.uci() for m in moves], [move_token(b, m) for m in moves], counts,
                             fitted['tokens'], {**setting, 'only_prior': True})
            beta = model['beta'] if name == 'candidate' else 0
            unnormalised = [a*math.exp(beta*math.fsum(v*t for v, t in zip(model['weights'], features(b, m)))) for a, m in zip(q, moves)]
            z = math.fsum(unnormalised); n = sum(counts.values()); alpha = setting['alpha']
            probs = [(counts.get(m.uci(), 0)+alpha*a/z)/(n+alpha) for m, a in zip(moves, unnormalised)]
            u = uniform(c7['draw_seed'], case['case_id'], 0, 'move'); cumulative = 0; chosen = None
            for san, probability in zip(sans, probs):
                cumulative += probability
                if u < cumulative:
                    chosen = san
                    break
            assert chosen == case['model_san']
            for role in ('human', 'model'):
                assert abs(probs[sans.index(case[role+'_san'])]-case[role+'_move_probability']) < 1e-12
                child = b.copy(); child.push_san(case[role+'_san'])
                key = str(zobrist_int64(child))
                assert key == case[role+'_child'] and child.fen().split()[:4] == fens[key].split()[:4]
                visited.add(key); probability_checks += 1
            visited.add(case['parent'])
            assert case['human_child'] == game['positions'][case['ply']+1]['key']
    assert visited == set(fens)
    maps, work = {}, {}
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        assert set(audit[phase]) == set(selection[phase]['targets'])
        assert set(prior[phase]) == set(selection[phase]['reused'])
        assert all(r == inherited_records[phase][k] for k, r in prior[phase].items())
        records = {**prior[phase], **audit[phase]}
        assert set(records) == set(selection[phase]['required'])
        assert not set(prior[phase]) & set(audit[phase])
        expected = {r[k] for values in cases.values() for r in values if phase == 'base' or r['deeper'] for k in ('parent', 'human_child', 'model_child')}
        assert expected == set(records) | set(selection[phase]['exact'])
        for key, record in records.items():
            b = chess.Board(record['fen'])
            assert str(zobrist_int64(b)) == key and b.fen().split()[:4] == fens[key].split()[:4]
            assert record['limits'] == {'nodes': nodes, 'seconds': seconds}
            assert 0 < record['score_nodes'] <= record['nodes']
            for uci in record['pv']:
                move = chess.Move.from_uci(uci)
                assert move in b.legal_moves
                b.push(move)
        maps[phase] = {**{k: r['cp'] for k, r in records.items()}, **selection[phase]['exact']}
        new = list(audit[phase].values())
        work[phase] = {'required': len(records), 'reused': len(prior[phase]), 'new': len(new),
            'new_engine_seconds': sum(r['seconds'] for r in new), 'nodes': sum(r['nodes'] for r in new),
            'time_at_least_limit': sum(r['seconds'] >= seconds for r in new),
            'depth_range': [min(r['depth'] for r in new), max(r['depth'] for r in new)] if new else None}
    scalar_checks = 0
    for phase, comparisons in scored.items():
        scores = maps['deeper' if phase == 'deeper' else 'base']
        for name, values in comparisons.items():
            ca = cases['candidate' if name == 'candidate_vs_reference' else name]
            for row in values:
                index = next(i for i, r in enumerate(ca) if r['case_id'] == row['case_id']); case = ca[index]
                human_child = cases['reference'][index]['model_child'] if name == 'candidate_vs_reference' else case['human_child']
                for role, key in [('human', human_child), ('model', case['model_child'])]:
                    signed = (scores[case['parent']]-scores[key])*(1 if chess.Board(case['fen']).turn else -1)
                    expected = {'signed_loss_cp': signed, 'loss_cp': max(0, signed), 'loss_primary': int(signed >= 150),
                        'loss_secondary': int(signed >= 300), 'capped_loss_cp': min(1000, max(0, signed))}
                    for metric, value in expected.items():
                        assert row[role+'_'+metric] == value
                        scalar_checks += 1
            summary = result['comparisons'][phase][name]
            assert summaries(values, c7, bootstrap=phase == 'base') == summary
            for group in summary['groups'].values():
                for metric in METRICS:
                    for role in ('human', 'model'):
                        means = [statistics.mean(r[role+'_'+metric] for r in values if r['stratum'] == s) for s in group['strata']]
                        assert abs(statistics.mean(means)-group['metrics'][metric][role]) < 1e-12
    changes = {name: {role: {metric: sum(a[role+'_'+metric] != b[role+'_'+metric] for a, b in zip(scored['base_nested'][name], scored['deeper'][name]))
        for metric in ('loss_primary', 'loss_secondary')} for role in ('human', 'model')} for name in ('reference', 'candidate')}
    output = {'status': 'passed', 'verifier_sha256': sha256(__file__), 'games': len(games), 'calibration_cases': len(selected),
        'probability_checks': probability_checks, 'scalar_checks': scalar_checks, 'engine_work': work,
        'deeper_category_changes': changes, 'test_results_sha256': sha256(output_path('m8/test_results.json')),
        'calibration_results_sha256': sha256(output_path('m8/calibration/results.json'))}
    write_json('m8/verification.json', output)
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
