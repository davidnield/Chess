"""Targeted follow-up of frozen ChessDB source policies; never changes active lookups.

Stockfish performs the searches natively. This small orchestration/report layer reuses
the existing panel and receipt conventions; it is not a repertoire optimizer.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import time

import numpy as np
import pandas as pd
from month_archive import file_lock, read_json, sha, write_json

HERE = Path(__file__).resolve().parent
SEED = 20261125


def digest(x):
    return hashlib.sha256(json.dumps(x, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def score(cp):
    return 1 / (1 + np.exp(-.00368208 * np.asarray(cp, dtype=float)))


def admitted(cp, color, gate, baselines):
    sign = 1 if color == 'white' else -1
    base = baselines[color] if sign == 1 else 1 - baselines[color]
    return score(sign * np.asarray(cp)) >= base - gate[color]


def selection(d, diagnostics, gates, baselines):
    reasons = {r.id: [] for r in d.itertuples()}
    menu = set()
    def add(mask, reason):
        for ident in d.loc[mask, 'id']:
            reasons[ident].append(reason)
    for name, gate in gates.items():
        for color in ('white', 'black'):
            add(admitted(d.step1, color, gate, baselines) !=
                admitted(d.proposal, color, gate, baselines), f'absolute-flip:{name}:{color}')
    add(abs(score(d.step1) - score(d.proposal)) >= .02, 'source-disagreement-2pp')
    for label, frame, column, threshold in diagnostics:
        for row in frame.itertuples(index=False):
            if float(getattr(row, column)) >= threshold:
                ident = f'{int(row.lo)}:{int(row.hi)}'
                if ident not in reasons:
                    raise ValueError('diagnostic key absent from panel')
                reasons[ident].append(label)
                menu.add(ident)
    for group, part in d.groupby('grp'):
        ids = sorted(part.id, key=lambda k: digest([SEED, group, k]))[:10]
        for ident in ids:
            reasons[ident].append('control')
    return {k: sorted(set(v)) for k, v in reasons.items() if v}, menu


def code_identity():
    return {name: sha(HERE / name) for name in
            ('chessdb_policy_validation.py', 'month_archive.py')}


def prepare(panel, prior, diagnostic, engine, out):
    import chess
    from sweep.spec import GATES, BASELINES
    panel, prior, diagnostic, engine, out = map(Path, (panel, prior, diagnostic, engine, out))
    if out.exists():
        raise ValueError('prepare requires a new experiment directory')
    definition = read_json(prior / 'definition.json')
    if sha(panel / 'panel_scored_sf18_d24.parquet') != definition['panel_sha256']:
        raise ValueError('original panel identity changed')
    if definition['gates'] != GATES or definition['baselines'] != BASELINES or definition['adopted']:
        raise ValueError('source proposal/gate recipe changed')
    engine_sha = sha(engine)
    if not engine_sha.startswith('c86215fa'):
        raise ValueError('not the pinned SF18 binary')
    d = pd.read_parquet(panel / 'panel_scored_sf18_d24.parquet')
    paired = pd.read_parquet(prior / 'paired-inputs.parquet')
    d = d.merge(paired[['lo', 'hi', 'step1', 'proposal']], on=['lo', 'hi'], validate='one_to_one')
    if len(d) != 1500 or d[['sf', 'step1', 'proposal']].isna().any().any() or d.id.duplicated().any():
        raise ValueError('incomplete/duplicate source panel')
    diagnostic_specs = [('cdb-regret-1pp', 'candidate-regret.csv', 'score_points', 1.),
                        ('cdb-gap-error-3pp', 'candidate-gaps.csv', 'gap2_error_points', 3.),
                        ('cloud-gap-error-3pp', 'cloud-shared-move-gaps.csv', 'gap2_error_points', 3.)]
    frames = [(label, pd.read_csv(diagnostic / file, dtype={'lo': 'int64', 'hi': 'int64'}), col, threshold)
              for label, file, col, threshold in diagnostic_specs]
    reasons, menu = selection(d, frames, GATES, BASELINES)
    raw = [json.loads(line) for line in (panel / 'search_sf18_d24.jsonl').read_text().splitlines() if line.strip()]
    refs = {r['id']: r for r in raw}
    if len(refs) != len(raw) or any(r.get('engine') != 'Stockfish 18' for r in raw if not r.get('terminal')):
        raise ValueError('duplicate reference or wrong reference engine')
    moves = pd.read_parquet(panel / 'panel_moves.parquet')
    parents, tasks = [], []
    def task(ident, parent, kind, fen, depth, **extra):
        ref = refs[ident]
        if chess.Board(ref['fen']).fen() != chess.Board(fen).fen():
            raise ValueError('reference FEN mismatch')
        return dict(id=ident, parent=parent, kind=kind, fen=fen, target_depth=depth,
                    minimum_depth=int(ref['depth']) + 2, old_reference=ref, **extra)
    for r in d.itertuples():
        if r.id not in reasons:
            continue
        p = dict(id=r.id, lo=int(r.lo), hi=int(r.hi), grp=r.grp, epd=r.epd,
                 white_to_move=bool(r.white_to_move), reasons=reasons[r.id],
                 step1=float(r.step1), proposal=float(r.proposal), old_sf=float(r.sf),
                 chessdb=float(r.chessdb), menu_test=r.id in menu)
        parents.append(p)
        tasks.append(task(r.id, r.id, 'parent', r.epd + ' 0 1', 28))
        if r.id in menu:
            for m in moves.loc[(moves.lo == r.lo) & (moves.hi == r.hi)].sort_values('rank').itertuples():
                b = chess.Board(r.epd + ' 0 1')
                move = chess.Move.from_uci(m.uci)
                if move not in b.legal_moves:
                    raise ValueError('illegal source move')
                b.push(move)
                cp = int(m.score) * (1 if r.white_to_move else -1)
                cp = max(-2000, min(2000, cp))
                tasks.append(task(f'{r.id}:{m.rank}', r.id, 'child', b.fen(), 27,
                                  rank=int(m.rank), uci=m.uci, source_cp_white=cp))
    if len({t['id'] for t in tasks}) != len(tasks):
        raise ValueError('duplicate search task')
    # Each source group is represented early for resource measurement. Then gate flips first.
    rank = {p['id']: (not any(x.startswith('absolute-flip') for x in p['reasons']),
                       digest([SEED, p['id']])) for p in parents}
    tasks.sort(key=lambda t: (*rank[t['parent']], t['kind'] != 'parent', t['id']))
    pilot = []
    for grp in sorted({p['grp'] for p in parents}):
        wanted = {p['id'] for p in parents if p['grp'] == grp}
        pilot.extend([t for t in tasks if t['parent'] in wanted and t['kind'] == 'parent'][:2])
    pilot_ids = {t['id'] for t in pilot}
    tasks = pilot + [t for t in tasks if t['id'] not in pilot_ids]
    inputs = [panel / f for f in ('panel_scored_sf18_d24.parquet', 'panel_moves.parquet', 'search_sf18_d24.jsonl')]
    inputs += [prior / f for f in ('definition.json', 'paired-inputs.parquet')]
    inputs += [diagnostic / x[1] for x in diagnostic_specs]
    manifest = dict(schema=1, created_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
        code=code_identity(), sources={str(p.resolve()): sha(p) for p in inputs},
        engine=str(engine.resolve()), engine_sha256=engine_sha,
        settings=dict(threads=1, hash_mb=128, multipv=1, time_seconds=60., workers=4,
                      seed=SEED, clip_cp=2000, gate_margin=.002),
        gates=GATES, baselines=BASELINES, parents=parents, tasks=tasks,
        interpretation='targeted exposed-panel diagnostic; no adoption; no reach weighting')
    out.mkdir(parents=True)
    write_json(out / 'manifest.json', manifest)
    print(json.dumps(dict(parents=len(parents), menu_parents=len(menu), tasks=len(tasks),
                          source_groups={g: sum(p['grp'] == g for p in parents) for g in sorted(d.grp.unique())})))
    return manifest


def qualified(task, result):
    return bool(result.get('terminal') or
                (result.get('depth', 0) >= task['minimum_depth'] and not result.get('bound')))


def completed_reference(updates):
    """Use one complete UCI score record, never python-chess's merged info dict.

    The aggregate retains an earlier aspiration upper/lowerbound when a later
    exact score omits that flag. An unfinished deeper bound also must not lend
    its depth to a shallower exact score.
    """
    exact = last = None
    bound_updates = max_depth = 0
    for info in updates:
        if 'score' not in info or 'depth' not in info or info.get('multipv', 1) != 1:
            continue
        last = dict(info)
        max_depth = max(max_depth, info['depth'])
        bounded = bool(info.get('upperbound') or info.get('lowerbound'))
        bound_updates += int(bounded)
        if not bounded and info.get('pv') and (exact is None or info['depth'] >= exact['depth']):
            exact = dict(info)
    if last is None:
        raise ValueError('engine returned no scored iteration')
    chosen = exact if exact is not None else last
    if exact is None and not (chosen.get('upperbound') or chosen.get('lowerbound')):
        raise ValueError('engine returned no complete scored PV')
    return chosen, dict(bound_updates=bound_updates, maximum_reported_depth=max_depth,
                        reference_kind='complete-exact-iteration' if exact else 'bound-only')


def search_one(task, manifest):
    import chess
    import chess.engine
    start = time.monotonic()
    b = chess.Board(task['fen'])
    if b.is_game_over():
        winner = b.outcome().winner
        return dict(cp_white=0 if winner is None else 2000 if winner else -2000,
                    terminal=True, nodes=0, depth=0, seconds=0., pv=[], bound=False,
                    engine='Stockfish 18', engine_sha256=manifest['engine_sha256'])
    engine = chess.engine.SimpleEngine.popen_uci(manifest['engine'], timeout=15,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    try:
        if engine.id.get('name') != 'Stockfish 18':
            raise ValueError('wrong running reference engine')
        engine.configure({'Threads': 1, 'Hash': 128})
        with engine.analysis(b, chess.engine.Limit(depth=task['target_depth'],
                             time=manifest['settings']['time_seconds']), game=object()) as analysis:
            info, iteration_details = completed_reference(analysis)
            analysis.wait()
        value = info['score'].white()
        return dict(cp_white=max(-2000, min(2000, value.score(mate_score=100000))),
                    mate=value.mate(), terminal=False, nodes=info.get('nodes', 0),
                    depth=info.get('depth', 0), seldepth=info.get('seldepth'),
                    seconds=time.monotonic()-start, pv=[m.uci() for m in info.get('pv', [])],
                    bound=bool(info.get('upperbound') or info.get('lowerbound')),
                    engine=engine.id['name'], engine_sha256=manifest['engine_sha256'],
                    **iteration_details)
    finally:
        try:
            engine.quit()
        finally:
            engine.close()


def load_results(out, manifest):
    identity = digest(manifest)
    tasks = {digest(t['id']): t for t in manifest['tasks']}
    results = {}
    for path in sorted((out / 'results').glob('*.json')):
        saved = read_json(path)
        if path.stem not in tasks:
            raise ValueError('unexpected checkpoint task')
        task = tasks[path.stem]
        r = saved['payload']
        if (saved['identity'] != identity or saved['sha256'] != digest(r)
                or r['task'] != task or r['engine_sha256'] != manifest['engine_sha256']
                or not math.isfinite(r['cp_white']) or not -2000 <= r['cp_white'] <= 2000
                or r['qualified'] != qualified(task, r)):
            raise ValueError('result checkpoint identity/value mismatch')
        results[task['id']] = r
    return results


def summarize(values):
    a = np.asarray(values, dtype=float)
    return dict(n=len(a), mean=float(a.mean()), p95=float(np.quantile(a, .95)),
                p99=float(np.quantile(a, .99)), maximum=float(a.max())) if len(a) else dict(n=0)


def report(out, manifest, results):
    rows, menu_rows = [], []
    for p in manifest['parents']:
        r = results.get(p['id'])
        if not r:
            continue
        rows.append({**{k: v for k, v in p.items() if k != 'reasons'},
                     'reasons': '|'.join(p['reasons']), 'control': 'control' in p['reasons'],
                     'new_sf': r['cp_white'], 'qualified': r['qualified'], 'depth': r['depth'],
                     'seconds': r['seconds'], 'nodes': r['nodes']})
        kids = [t for t in manifest['tasks'] if t['parent'] == p['id'] and t['kind'] == 'child']
        if not kids or not r['qualified'] or any(t['id'] not in results or not results[t['id']]['qualified'] for t in kids):
            continue
        sign = 1 if p['white_to_move'] else -1
        kids.sort(key=lambda t: t['rank'])
        cs = score([sign*t['source_cp_white'] for t in kids])
        rs = score([sign*results[t['id']]['cp_white'] for t in kids])
        color = 'white' if sign == 1 else 'black'
        for name, gate in manifest['gates'].items():
            base = manifest['baselines'][color] if sign == 1 else 1-manifest['baselines'][color]
            floor = base-gate[color]
            cbest = max(float(max(cs)), float(score(sign*p['chessdb'])))
            rbest = max(float(max(rs)), float(score(sign*r['cp_white'])))
            for t, c, ref in zip(kids, cs, rs):
                menu_rows.append(dict(parent=p['id'], uci=t['uci'], gate=name, color=color,
                    cdb_admitted=bool(c >= floor and cbest-c <= gate['relative']),
                    sf_admitted=bool(ref >= floor and rbest-ref <= gate['relative']),
                    cdb_top1_regret_points=100*max(0., float(max(rs)-rs[0])),
                    scope='searched ChessDB menu, not blended child-lookup policy'))
    d = pd.DataFrame(rows)
    d.to_csv(out/'parent-results.csv', index=False)
    pd.DataFrame(menu_rows).to_csv(out/'menu-results.csv', index=False)
    errors, gates = [], []
    if len(d):
        q = d[d.qualified]
        groups = [('targeted_all', q), ('controls', q[q.control])]
        groups += list(q.groupby('grp'))
        for name, x in groups:
            for policy in ('step1', 'proposal'):
                errors.append(dict(group=name, policy=policy,
                    **summarize(100*abs(score(x[policy])-score(x.new_sf)))))
        for name, gate in manifest['gates'].items():
            for color in ('white', 'black'):
                ref = admitted(q.new_sf, color, gate, manifest['baselines'])
                old = admitted(q.old_sf, color, gate, manifest['baselines'])
                sign = 1 if color == 'white' else -1
                base = manifest['baselines'][color] if sign == 1 else 1-manifest['baselines'][color]
                near = abs(score(sign*q.new_sf)-(base-gate[color])) <= .002
                for policy in ('step1', 'proposal'):
                    a = admitted(q[policy], color, gate, manifest['baselines'])
                    gates.append(dict(gate=name, color=color, policy=policy, n=len(q),
                        false_admissions=int((a & ~ref).sum()), false_exclusions=int((~a & ref).sum()),
                        clear_false_admissions=int((a & ~ref & ~near).sum()),
                        near_floor=int(near.sum()), changed_reference=int((ref != old).sum())))
    pd.DataFrame(errors).to_csv(out/'source-errors.csv', index=False)
    pd.DataFrame(gates).to_csv(out/'absolute-gates.csv', index=False)
    summary = dict(identity=digest(manifest), completed=len(results), total=len(manifest['tasks']),
        qualified=sum(r['qualified'] for r in results.values()),
        unresolved=[k for k, r in results.items() if not r['qualified']],
        source_errors=errors, absolute_gates=gates, adopted=False,
        limitations=manifest['interpretation']+'; engine uncertainty remains; full relative gates require pilot')
    write_json(out/'summary.json', summary)
    return summary


def run(out, workers=4, max_new=None, search=search_one, check_inputs=True):
    out = Path(out)
    with file_lock(out/'run.lock'):
        manifest = read_json(out/'manifest.json')
        if manifest['schema'] != 1 or not 1 <= workers <= manifest['settings']['workers']:
            raise ValueError('invalid schema/workers')
        if max_new is not None and max_new < 1:
            raise ValueError('positive bounded run limit required')
        if check_inputs:
            if code_identity() != manifest['code'] or sha(Path(manifest['engine'])) != manifest['engine_sha256']:
                raise ValueError('code/engine identity drift')
            if any(sha(Path(p)) != h for p, h in manifest['sources'].items()):
                raise ValueError('source identity drift')
        (out/'results').mkdir(exist_ok=True)
        identity = digest(manifest)
        results = load_results(out, manifest)
        todo = [t for t in manifest['tasks'] if t['id'] not in results]
        if max_new is not None:
            todo = todo[:max_new]
        begin, submitted = time.monotonic(), 0
        def state(label, **kw):
            obj = dict(state=label, utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                pid=os.getpid(), completed=len(results), total=len(manifest['tasks']),
                qualified=sum(r['qualified'] for r in results.values()),
                invocation_seconds=time.monotonic()-begin, identity=identity, **kw)
            write_json(out/'status.json', obj)
            print(json.dumps(obj), flush=True)
        try:
            state('running', active=0)
            with cf.ThreadPoolExecutor(workers) as pool:
                live = {}
                while live or submitted < len(todo):
                    while len(live) < workers and submitted < len(todo) and not (out/'STOP').exists():
                        task = todo[submitted]; submitted += 1
                        live[pool.submit(search, task, manifest)] = task
                    if not live:
                        break
                    finished, _ = cf.wait(live, timeout=10, return_when=cf.FIRST_COMPLETED)
                    for future in finished:
                        task = live.pop(future)
                        r = dict(future.result(), task=task)
                        r['qualified'] = qualified(task, r)
                        path = out/'results'/f"{digest(task['id'])}.json"
                        write_json(path, dict(identity=identity, sha256=digest(r), payload=r))
                        results[task['id']] = r
                    state('running', active=len(live))
            # Re-read durable records before reporting, just as a resumed run does.
            results = load_results(out, manifest)
            report(out, manifest, results)
            state('complete' if len(results) == len(manifest['tasks']) else 'stopped-or-bounded', active=0)
        except BaseException as exc:
            state('error', error=repr(exc))
            raise


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest='command', required=True)
    p = sub.add_parser('prepare')
    p.add_argument('--panel', type=Path, required=True)
    p.add_argument('--prior', type=Path, required=True)
    p.add_argument('--diagnostic', type=Path, required=True)
    p.add_argument('--engine', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p = sub.add_parser('run')
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--max-new', type=int)
    args = vars(ap.parse_args()); command = args.pop('command')
    (prepare if command == 'prepare' else run)(**args)


if __name__ == '__main__':
    main()
