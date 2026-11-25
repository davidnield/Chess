"""Checkpointed SF18 upgrades for the frozen, selection-only White gate review.

This is a targeted diagnostic, not another weight-selection sample. Existing
references are never overwritten; a new reference is used only when qualified.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import datetime as dt
from pathlib import Path
import sys

import chess
import pandas as pd
import chessdb_policy_validation as ref
from month_archive import file_lock, read_json, sha, write_json
from zobrist128 import zobrist128


def code_identity():
    here = Path(__file__).parent
    return {n: sha(here/n) for n in ('eval_white_audit.py', 'chessdb_policy_validation.py',
                                    'month_archive.py', 'zobrist128.py', 'zobrist.py')}


def prepare(cases, engine, out):
    cases, engine, out = (Path(p).resolve() for p in (cases, engine, out))
    rows = pd.read_csv(cases, dtype={'id': str})
    if rows.id.duplicated().any():
        raise ValueError('review must have one row per full position key')
    tasks = []
    for r in rows.itertuples():
        b = chess.Board(r.epd)
        if not b.is_valid() or ':'.join(map(str, zobrist128(b))) != r.id:
            raise ValueError('review position/key mismatch')
        if r.needs_stronger_reference:
            tasks.append(dict(id=r.id, parent=r.id, kind='parent', fen=b.fen(),
                              target_depth=30, minimum_depth=28,
                              old_reference=float(r.reference_cp)))
    manifest = dict(schema='white-gate-reference-audit-v1', code=code_identity(),
        sources={str(cases): sha(cases)}, cases=str(cases), engine=str(engine),
        engine_sha256=sha(engine), settings=dict(workers=4, time_seconds=180),
        tasks=tasks, selection_only=True, production_binding_changed=False)
    out.mkdir(parents=True, exist_ok=True)
    with file_lock(out/'run.lock'):
        path = out/'manifest.json'
        if path.exists() and read_json(path) != manifest:
            raise ValueError('different audit already occupies output')
        write_json(path, manifest)
    return manifest


def checked(out):
    m = read_json(out/'manifest.json')
    if m['schema'] != 'white-gate-reference-audit-v1' or m['code'] != code_identity():
        raise ValueError('audit code identity changed')
    if sha(Path(m['engine'])) != m['engine_sha256']:
        raise ValueError('reference engine changed')
    if any(sha(Path(p)) != h for p, h in m['sources'].items()):
        raise ValueError('review cases changed')
    return m


def run(out, search=ref.search_one):
    out = Path(out)
    with file_lock(out/'run.lock'):
        m = checked(out)
        (out/'results').mkdir(exist_ok=True)
        results = ref.load_results(out, m)
        def status(state):
            s = dict(state=state, completed=len(results), total=len(m['tasks']),
                     qualified=sum(r['qualified'] for r in results.values()),
                     utc=dt.datetime.now(dt.timezone.utc).isoformat(), identity=ref.digest(m))
            write_json(out/'status.json', s)
            print(s, flush=True)
        status('running')
        try:
            todo = iter(t for t in m['tasks'] if t['id'] not in results)
            with cf.ThreadPoolExecutor(m['settings']['workers']) as pool:
                live = {}
                while True:
                    while len(live) < m['settings']['workers'] and not (out/'STOP').exists():
                        task = next(todo, None)
                        if task is None:
                            break
                        live[pool.submit(search, task, m)] = task
                    if not live:
                        break
                    done, _ = cf.wait(live, timeout=10, return_when=cf.FIRST_COMPLETED)
                    for future in done:
                        task = live.pop(future)
                        r = dict(future.result(), task=task)
                        r['qualified'] = ref.qualified(task, r)
                        write_json(out/'results'/f"{ref.digest(task['id'])}.json",
                                   dict(identity=ref.digest(m), sha256=ref.digest(r), payload=r))
                        results[task['id']] = r
                    status('running')
            results = ref.load_results(out, m)
            status('complete' if len(results) == len(m['tasks']) else 'stopped')
        except BaseException:
            status('error')
            raise
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('command', choices=('prepare', 'run'))
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--cases', type=Path)
    ap.add_argument('--engine', type=Path)
    a = ap.parse_args()
    if a.command == 'prepare':
        if not a.cases or not a.engine:
            ap.error('prepare requires --cases and --engine')
        m = prepare(a.cases, a.engine, a.out)
        print(dict(tasks=len(m['tasks']), identity=ref.digest(m)))
    else:
        run(a.out)


if __name__ == '__main__':
    main()
