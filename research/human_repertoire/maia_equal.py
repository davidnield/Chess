"""Fixed equal-cost follow-up packages under Maia-1400 fallback."""
import argparse
from dataclasses import replace
import json
import time
import chess
from python.zobrist import zobrist_int64
from . import maia_comparison as previous, paired_panel as panel
from .board_benchmark import isolated
from .io import ROOT, output_path, sha256, write_json
from .maia_policy import MaiaPolicy, MaiaFallbackKernels
from .model import Config
from .trajectory import ChessBackend, sample_trace, score_trace, arm_summary, paired_summary, mean_se

CONFIG = ROOT/'docs/human_repertoire/maia_equal_config.json'
PREFIX = 'maia_equal/'


def read(name):
    return json.loads(output_path(PREFIX+name+'.json').read_text())


def save(name, value):
    return write_json(PREFIX+name+'.json', value)


def source(c):
    old_c = json.loads(previous.CONFIG.read_text()); p = previous.verified(old_c)
    c6, base, records = previous.source(old_c); audit = previous.read('engine_audit.json')
    if not audit['complete'] or audit['identity'] != {'case_manifest_sha256': sha256(output_path(previous.PREFIX+'case_manifest.json')), 'engine': p['engine_identity']}:
        raise ValueError('previous comparison engine drift')
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        records[phase].update(panel.checked_records(audit, phase, nodes, seconds))
    return p, c6, base, records


def identity(c):
    return {'config': c, 'code_sha256': sha256(__file__),
        'plan_sha256': sha256(ROOT/'docs/human_repertoire/maia_equal_plan.md'),
        'previous_identity': previous.identity(json.loads(previous.CONFIG.read_text())),
        'previous_manifest_sha256': sha256(output_path(previous.PREFIX+'case_manifest.json')),
        'previous_engine_sha256': sha256(output_path(previous.PREFIX+'engine_audit.json'))}


def verified(c):
    p = read('case_manifest')
    if p['identity'] != identity(c) or any(sha256(output_path(PREFIX+k+'.json')) != h for k, h in p['snapshots'].items()):
        raise ValueError('equal-budget frozen input drift')
    return p


def packages(cards):
    root = chess.Board()
    for move in ['e4', 'e5', 'Nf3', 'Nc6']: root.push_san(move)
    root_key = str(zobrist_int64(root)); fens = {root_key: root.fen()}; books, gates = {}, {}
    for name, (reply, move) in cards.items():
        b = root.copy(); b.push_san('d4'); b.push_san(reply)
        key = str(zobrist_int64(b)); fens[key] = b.fen()
        book = {root_key: 'd4', key: move}
        if len(book) != 2: raise ValueError('two distinct positions required')
        books[name] = book
        for k, m in book.items():
            b = chess.Board(fens[k]); b.push_san(m); child = str(zobrist_int64(b)); fens[child] = b.fen()
            gates[k, m] = {'position': k, 'move': m, 'child': child}
    return root, books, list(gates.values()), fens


def selection(paths, fens, gates, records, c, cfg):
    paired = {}; reference = c['reference']
    for policy in c['policies']:
        for book in c['cards']:
            if book == reference: continue
            for label, name in [('source_one_card', reference), ('two_cards', book)]:
                paired[policy+'_'+book+'/'+label] = paths[policy+'/'+name]
    return previous.select_targets(paired, fens, gates, records,
        {**c, 'plans': ['source_one_card', 'two_cards']}, cfg)


def prepare(c):
    if output_path(PREFIX+'case_manifest.json').exists(): verified(c); return
    old, c6, base, records = source(c); c4, _, train, fitted, specs, _, _ = base
    root, books, gates, fens = packages(c['cards']); paths, tables, local = {}, {}, {}
    cfg = replace(Config(**c6['config']), recall=c['recall']); policy = MaiaPolicy(); start = time.monotonic()
    for name in c['policies']:
        backend = ChessBackend(root.fen(), fitted, specs, train, c4, c['max_positions'])
        backend.kernels = MaiaFallbackKernels(policy, 'Blitz' if name == 'maia_blitz' else 'Rapid', chess.WHITE,
            c['opponent_rating_input'], backend.kernels, 'shared_position', cap=c['max_positions'])
        branch = root.copy(); branch.push_san('d4')
        _, branch_ps = backend.kernels.get(str(zobrist_int64(branch)), branch.fen())
        local[name] = {}
        for book, moves in books.items():
            extra = next(k for k in moves if k != str(zobrist_int64(root)))
            _, ps = backend.kernels.get(extra, fens[extra])
            q = ps['maia1400'][moves[extra]]
            local[name][book] = {'position': extra, 'fallback_probability': q,
                'branch_probability_after_d4': branch_ps['maia1400'][c['cards'][book][0]],
                'with_card_probability': c['recall']+(1-c['recall'])*q}
            traces = []
            for index in range(c['pairs']):
                if time.monotonic()-start > c['sampling_seconds']: raise TimeoutError('sampling guard')
                traces.append(sample_trace(backend, 'maia1400', moves, cfg, c['seed'], index))
            paths[name+'/'+book] = traces
        tables[name] = backend.kernels.cache
        for key, fen in backend.fens.items():
            if key in fens and fens[key].split()[:4] != fen.split()[:4]: raise ValueError('FEN drift')
            fens.setdefault(key, fen)
        print(name, 'complete', len(backend.fens), flush=True)
    gate_keys = {g[k] for g in gates for k in ('position', 'child')}
    targets = selection(paths, fens, gate_keys, records, c, c6['config'])
    prior = {s: {k: records[s][k] for k in targets[s]['reused']} for s in records}
    snapshots = {n: sha256(save(n, v)) for n, v in {'traces': paths, 'policy_tables': tables, 'local': local,
        'fens': fens, 'engine_selection': targets, 'inherited_records': prior}.items()}
    save('case_manifest', {'identity': identity(c), 'snapshots': snapshots, 'books': books, 'gates': gates,
        'evaluator_config': {**c6['config'], 'recall': c['recall']}, 'sampling_seconds': time.monotonic()-start,
        **{k: old[k] for k in ('engine_identity', 'engine_path', 'engine_options')}})
    print(json.dumps({'local': local, 'targets': {s: len(t['targets']) for s, t in targets.items()}}, indent=2))


def engine(c):
    module = isolated('calibration_experiment.py', 'm7/', PREFIX)
    module.verified_cases = lambda unused: verified(c)
    module.engine(c)


def compare(c):
    p = verified(c); audit = read('engine_audit'); prior = read('inherited_records'); targets = read('engine_selection')
    if not audit['complete'] or audit['identity'] != {'case_manifest_sha256': sha256(output_path(PREFIX+'case_manifest.json')), 'engine': p['engine_identity']}:
        raise ValueError('incomplete or stale engine audit')
    maps = {}
    for phase, nodes, seconds in [('base', c['engine_nodes'], c['engine_time_seconds']), ('deeper', c['deeper_nodes'], c['deeper_time_seconds'])]:
        fresh = panel.checked_records(audit, phase, nodes, seconds)
        if set(fresh) != set(targets[phase]['targets']): raise ValueError('incomplete coverage')
        maps[phase] = {k: r['cp'] for k, r in {**prior[phase], **fresh}.items()}
    cfg = Config(**p['evaluator_config']); paths = read('traces'); scored, results = {}, {}
    alpha = c['alpha']/(len(c['policies'])*(len(c['cards'])-1))
    for phase in maps:
        n = c['pairs'] if phase == 'base' else c['deeper_pairs']
        for policy in c['policies']:
            context = phase+'/'+policy; traces = {b: paths[policy+'/'+b][:n] for b in c['cards']}
            rows = {b: [score_trace(t, maps[phase], cfg) for t in ts] for b, ts in traces.items()}
            if any(r['unknown_eval_mass'] or r['frontier_mass'] for rs in rows.values() for r in rs): raise ValueError('missing scores')
            contrasts = {}; ref = c['reference']
            for b in c['cards']:
                if b == ref: continue
                contrasts[b] = paired_summary(rows[b], rows[ref], traces[b], traces[ref], alpha)
                contrasts[b]['disadvantage'] = mean_se([a['disadvantage']-z['disadvantage'] for a,z in zip(rows[b], rows[ref])])
            results[context] = {'arms': {b: arm_summary(rs, c['alpha']) for b,rs in rows.items()}, 'contrasts': contrasts}
            scored[context] = rows
    gates = {s: panel.check_gates(p['gates'], m, cfg) for s,m in maps.items()}
    eligibility = {b: all(g['passed'] for gs in gates.values() for g in gs if p['books'][b].get(g['position']) == g['move']) for b in c['cards']}
    out = {'identity': identity(c), 'engine_sha256': sha256(output_path(PREFIX+'engine_audit.json')),
        'scored_sha256': sha256(save('scored', scored)), 'contexts': results, 'gates': gates,
        'eligible': eligibility, 'contrast_alpha': alpha, 'local': read('local')}
    save('results', out)
    print(json.dumps({'eligible': eligibility, 'contrasts': {k:v['contrasts'] for k,v in results.items() if k.startswith('base/')}}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('phase', choices=('prepare', 'engine', 'compare'))
    args = parser.parse_args(); globals()[args.phase](json.loads(CONFIG.read_text()))
