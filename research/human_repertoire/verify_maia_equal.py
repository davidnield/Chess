"""Replay fixed equal-budget packages and independently audit scalar outcomes."""
import json
import math
import chess
from python.zobrist import zobrist_int64
from .maia_equal import CONFIG, PREFIX, read, save, source, verified, packages, selection
from .io import output_path, sha256
from .model import Config
from .trajectory import ChessBackend, sample_trace


def main():
    c = json.loads(CONFIG.read_text()); p = verified(c); _, _, base, old = source(c)
    c4, _, train, fitted, specs, _, _ = base
    paths, tables, fens, targets, audit, prior, result, scored = [read(n) for n in
        ['traces', 'policy_tables', 'fens', 'engine_selection', 'engine_audit', 'inherited_records', 'results', 'scored']]
    root, books, gates, _ = packages(c['cards']); assert books == p['books'] and gates == p['gates']
    cfg = Config(**p['evaluator_config']); count = moves_count = 0
    assert result['identity'] == p['identity']
    assert result['scored_sha256'] == sha256(output_path(PREFIX+'scored.json'))
    assert result['engine_sha256'] == sha256(output_path(PREFIX+'engine_audit.json'))
    assert audit['complete'] and audit['identity'] == {'case_manifest_sha256': sha256(output_path(PREFIX+'case_manifest.json')), 'engine': p['engine_identity']}
    for policy in c['policies']:
        class FrozenKernel:
            def get(self, key, fen):
                moves, ps, core = tables[policy][key]; board = chess.Board(fen)
                assert (' '.join(core) if isinstance(core, list) else core) == ' '.join(fen.split()[:4])
                assert set(moves.values()) == {m.uci() for m in board.legal_moves}
                assert all(board.parse_san(s).uci() == u for s,u in moves.items())
                assert set(ps['maia1400']) == set(moves) and all(v > 0 for v in ps['maia1400'].values())
                assert abs(sum(ps['maia1400'].values())-1) < 1e-10
                return moves, ps
        backend = ChessBackend(root.fen(), fitted, specs, train, c4, c['max_positions']); backend.kernels = FrozenKernel()
        for book in c['cards']:
            traces = paths[policy+'/'+book]
            assert [t['index'] for t in traces] == list(range(c['pairs']))
            for trace in traces:
                assert sample_trace(backend, 'maia1400', books[book], cfg, c['seed'], trace['index']) == trace
                board = root.copy()
                for i,key in enumerate(trace['keys']):
                    assert str(zobrist_int64(board)) == key and board.fen().split()[:4] == fens[key].split()[:4]
                    if i < len(trace['moves']): board.push_san(trace['moves'][i]); moves_count += 1
                count += 1
    assert count == len(c['policies'])*len(c['cards'])*c['pairs']
    gate_keys = {g[k] for g in gates for k in ('position', 'child')}
    assert selection(paths, fens, gate_keys, old, c, p['evaluator_config']) == targets
    maps = {}; work = {}
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        assert set(prior[phase]) == set(targets[phase]['reused']) and set(audit[phase]) == set(targets[phase]['targets'])
        assert all(v == old[phase][k] for k,v in prior[phase].items())
        records = {**prior[phase], **audit[phase]}; assert set(records) == set(targets[phase]['required'])
        for key, r in records.items():
            assert r['limits'] == {'nodes': nodes, 'seconds': seconds}
            board = chess.Board(r['fen']); assert str(zobrist_int64(board)) == key
            assert board.fen().split()[:4] == fens[key].split()[:4]
            for u in r['pv']:
                m = chess.Move.from_uci(u); assert m in board.legal_moves; board.push(m)
        maps[phase] = {k:r['cp'] for k,r in records.items()}
        work[phase] = {'required': len(records), 'reused': len(prior[phase]), 'new': len(audit[phase]), 'seconds': sum(r['seconds'] for r in audit[phase].values())}
    checks = 0; deeper_changes = {}
    for context, rows in scored.items():
        phase, policy = context.split('/')
        n = c['pairs'] if phase == 'base' else c['deeper_pairs']
        for book, values in rows.items():
            assert len(values) == n
            for row, trace in zip(values, paths[policy+'/'+book]):
                hit = 0; at = None
                for ply,key in enumerate(trace['keys']):
                    cp = trace['exact_cp'] if ply == len(trace['keys'])-1 and trace['exact_cp'] is not None else maps[phase][key]
                    if abs(cp) >= cfg.threshold_cp: hit,at = (1 if cp > 0 else -1),ply; break
                assert row['advantage'] == (hit == 1) and row['disadvantage'] == (hit == -1)
                assert row['early_advantage'] == (cfg.discount**at if hit == 1 else 0)
                assert row['unknown_eval_mass'] == row['frontier_mass'] == 0; checks += 5
            assert result['contexts'][context]['arms'][book]['metrics']['early_advantage']['mean'] == sum(r['early_advantage'] for r in values)/n
        for book, contrast in result['contexts'][context]['contrasts'].items():
            for metric in ['early_advantage', 'disadvantage']:
                ds = [a[metric]-b[metric] for a,b in zip(rows[book], rows[c['reference']])]
                mean = sum(ds)/n; se = (sum((x-mean)**2 for x in ds)/(n-1)/n)**.5
                assert abs(mean-contrast[metric]['mean']) < 1e-12 and abs(se-contrast[metric]['mc_se']) < 1e-12
            bounds = contrast['discounted_first_advantage']; margin = math.sqrt(2*math.log(2/result['contrast_alpha'])/n)
            mean = contrast['early_advantage']['mean']
            assert bounds['mc_outer_95'] == [max(-1, mean-margin), min(1, mean+margin)]
        if phase == 'deeper':
            deeper_changes[policy] = {b: sum(a['early_advantage'] != z['early_advantage'] for a,z in zip(values, scored['base/'+policy][b])) for b,values in rows.items()}
    for phase, gs in result['gates'].items():
        for g in gs:
            parent,child = maps[phase][g['position']],maps[phase][g['child']]
            assert (parent,child) == (g['parent_cp'],g['child_cp'])
            assert g['passed'] == (child >= cfg.soundness_floor_cp and child >= parent-cfg.max_engine_loss_cp)
    for b, book in books.items():
        assert result['eligible'][b] == all(g['passed'] for gs in result['gates'].values() for g in gs if book.get(g['position']) == g['move'])
    out = {'status': 'passed', 'paths_replayed': count, 'legal_moves': moves_count, 'scalar_checks': checks,
        'engine_work': work, 'deeper_changed_early_utility_rows': deeper_changes,
        'results_sha256': sha256(output_path(PREFIX+'results.json')), 'verifier_sha256': sha256(__file__)}
    save('verification', out); print(json.dumps(out, indent=2))


if __name__ == '__main__': main()
