"""Frozen-policy trace replay and independent scalar audit of the study decision."""
from dataclasses import replace
import json
import math

import chess
from python.zobrist import zobrist_int64

from .maia_comparison import CONFIG, PREFIX, read, save, source, verified, select_targets
from .io import output_path, sha256
from .model import Config
from .trajectory import ChessBackend, uniform
from . import paired_panel as panel


def main():
    c = json.loads(CONFIG.read_text()); p = verified(c)
    c6, base, old = source(c); c4, _, train, fitted, specs, _, _ = base
    paths, tables, fens, selected, audit, prior, result = [read(n+'.json') for n in
        ('traces', 'policy_tables', 'fens', 'engine_selection', 'engine_audit', 'inherited_records', 'results')]
    assert audit['complete'] and result['engine_sha256'] == sha256(output_path(PREFIX+'engine_audit.json'))
    assert result['scored_sha256'] == sha256(output_path(PREFIX+'scored.json'))
    root = next(iter(p['books']['source_one_card'])); replayed = 0; legal_moves = 0
    for arm, traces in paths.items():
        name, recall_text, book_name = arm.split('/'); recall = float(recall_text[1:])
        cfg = replace(Config(**c6['config']), recall=recall); book = p['books'][book_name]
        model = 'shared_position' if name == 'shared_position' else 'maia1400'
        class FrozenKernel:
            def get(self, key, fen):
                moves, ps, core = tables[name][key]
                assert (' '.join(core) if isinstance(core, list) else core) == ' '.join(fen.split()[:4])
                assert set(moves.values()) == {m.uci() for m in chess.Board(fen).legal_moves}
                assert abs(sum(ps[model].values())-1) < 1e-10
                return moves, ps
        backend = ChessBackend(fens[root], fitted, specs, train, c4, c['max_positions']); backend.kernels = FrozenKernel()
        assert [t['index'] for t in traces] == list(range(c['pairs']))
        for trace in traces:
            board = chess.Board(fens[root]); visited = []
            for ply, key in enumerate(trace['keys']):
                assert str(zobrist_int64(board)) == key
                assert board.fen().split()[:4] == fens[key].split()[:4]
                view = backend.view(board, model); node = view.node
                repeated = visited.count(key)+1 >= cfg.repetition_visits
                visited.append(key)
                reason = ('terminal' if node.side == 'terminal' else 'repetition' if repeated else
                    'human_end' if node.side == 'human_terminal' else 'horizon' if ply == cfg.horizon else None)
                ending = (0, 1, 0) if reason == 'repetition' else node.wdl
                if not reason and uniform(c['seed'], trace['index'], ply, 'end') < node.end_probability:
                    reason, ending = 'human_end', node.end_wdl
                if reason:
                    assert ply == len(trace['moves']) and reason == trace['reason']
                    assert list(ending) == trace['wdl'] and trace['exact_cp'] == view.exact_cp
                    break
                r = recall if node.side == 'us' and key in book else 0
                if uniform(c['seed'], trace['index'], ply, 'recall') < r:
                    move = book[key]
                else:
                    u = uniform(c['seed'], trace['index'], ply, 'move'); cumulative = 0; move = None
                    for edge in sorted(node.edges, key=lambda e: e.move):
                        cumulative += edge.probability
                        if u < cumulative:
                            move = edge.move; break
                    assert move is not None
                assert move == trace['moves'][ply]
                board.push_san(move); legal_moves += 1
            replayed += 1
    assert replayed == len(c['policies'])*len(c['recalls'])*len(c['plans'])*c['pairs']
    gate_keys = {g[k] for g in p['gates'] for k in ('position', 'child')}
    assert select_targets(paths, fens, gate_keys, old, c, c6['config']) == selected
    maps = {}; work = {}
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        assert set(prior[phase]) == set(selected[phase]['reused'])
        assert set(audit[phase]) == set(selected[phase]['targets'])
        assert all(v == old[phase][k] for k, v in prior[phase].items())
        records = {**prior[phase], **audit[phase]}
        assert set(records) == set(selected[phase]['required'])
        for key, record in records.items():
            assert record['limits'] == {'nodes': nodes, 'seconds': seconds}
            b = chess.Board(record['fen'])
            assert str(zobrist_int64(b)) == key and b.fen().split()[:4] == fens[key].split()[:4]
            for uci in record['pv']:
                move = chess.Move.from_uci(uci); assert move in b.legal_moves; b.push(move)
        maps[phase] = {k: v['cp'] for k, v in records.items()}
        work[phase] = {'required': len(records), 'reused': len(prior[phase]), 'new': len(audit[phase]),
            'engine_seconds': sum(v['seconds'] for v in audit[phase].values())}
    scored = read('scored.json'); checks = 0
    for context, rows in scored.items():
        for book, values in rows.items():
            for row, trace in zip(values, paths[context+'/'+book]):
                hit = 0; hit_ply = None
                for ply, key in enumerate(trace['keys']):
                    cp = trace['exact_cp'] if ply == len(trace['keys'])-1 and trace['exact_cp'] is not None else maps['base'][key]
                    if abs(cp) >= 300:
                        hit, hit_ply = (1 if cp > 0 else -1), ply; break
                assert row['advantage'] == (hit == 1) and row['disadvantage'] == (hit == -1)
                assert row['early_advantage'] == (.99**hit_ply if hit == 1 else 0)
                assert not row['unknown_eval_mass'] and not row['frontier_mass']; checks += 5
        differences = [a['early_advantage']-b['early_advantage'] for a, b in zip(rows['two_cards'], rows['source_one_card'])]
        observed = result['contexts'][context]['two_minus_one']['early_advantage']
        mean = sum(differences)/len(differences)
        se = (sum((d-mean)**2 for d in differences)/(len(differences)-1)/len(differences))**.5
        assert abs(observed['mean']-mean) < 1e-12 and abs(observed['mc_se']-se) < 1e-12
        bounds = result['contexts'][context]['two_minus_one']['discounted_first_advantage']
        margin = math.sqrt(2*math.log(2/c['alpha'])/c['pairs'])
        assert abs(bounds['hoeffding_margin']-margin) < 1e-12
        assert bounds['mc_outer_95'] == [max(-1, mean-margin), min(1, mean+margin)]
        cfg = replace(Config(**p['evaluator_config']), recall=float(context.split('/r')[1]))
        nested = {book: paths[context+'/'+book][:c['deeper_pairs']] for book in c['plans']}
        for phase in maps:
            assert result['contexts'][context]['deeper_subset'][phase] == panel.score_panel(nested, maps[phase], cfg, c['alpha'], True)
    for phase, gates in result['gates'].items():
        for gate in gates:
            parent, child = maps[phase][gate['position']], maps[phase][gate['child']]
            assert (parent, child) == (gate['parent_cp'], gate['child_cp'])
            assert gate['passed'] == (child >= cfg.soundness_floor_cp and child >= parent-cfg.max_engine_loss_cp)
    out = {'status': 'passed', 'paths_replayed': replayed, 'moves_replayed': legal_moves, 'scalar_checks': checks,
        'engine_work': work, 'results_sha256': sha256(output_path(PREFIX+'results.json')), 'verifier_sha256': sha256(__file__)}
    save('verification.json', out); print(json.dumps(out, indent=2))


if __name__ == '__main__':
    main()
