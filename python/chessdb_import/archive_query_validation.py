"""Freeze a reproducible archive/native parity panel; no production mutation."""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import subprocess
import time
from pathlib import Path


def mirror(fen: str) -> str:
    board, turn, castle, ep = fen.split()[:4]
    castle = ''.join(c.upper() for c in castle if not c.isupper()) + ''.join(c.lower() for c in castle if c.isupper())
    return f"{'/'.join(r.swapcase() for r in board.split('/')[::-1])} {'b' if turn == 'w' else 'w'} {castle} {ep if ep == '-' else ep[0] + str(9-int(ep[1]))}"


def create_panel(root: Path, out: Path) -> None:
    import chess
    import pyarrow.parquet as pq
    out.mkdir(parents=True, exist_ok=False)
    marker = json.loads((root / 'ARCHIVE.COMPLETE.json').read_text())
    receipts = [json.loads((root / p).read_text()) for p in marker['receipts']]
    receipts.sort(key=lambda r: r['source']['first_key_hex'])
    requests, expected, seen = [], [], set()

    def add(fen, group, row=None, mirrored=False):
        if fen in seen:
            return
        seen.add(fen)
        i = len(requests)
        requests.append(dict(id=i, fen=fen, group=group))
        if row is not None:
            moves = [dict(uci=(m['uci'][0]+str(9-int(m['uci'][1]))+m['uci'][2]+str(9-int(m['uci'][3]))+m['uci'][4:] if mirrored else m['uci']), score=m['score']) for m in row['moves']]
            expected.append(dict(id=i, source_key=row['source_key'].hex(), moves=moves,
                                 ply=row['cdb_mirror_ply' if mirrored else 'cdb_ply'], record_present=True))

    for si in sorted({round(i*(len(receipts)-1)/31) for i in range(32)}):
        path = receipts[si]['file']
        p = pq.ParquetFile(root / path)
        for rg in sorted({0, p.num_row_groups//2, p.num_row_groups-1}):
            rows = p.read_row_group(rg).to_pylist()
            selected = {0, len(rows)-1}
            for predicate in [lambda r: any(len(m['uci']) == 5 for m in r['moves']),
                              lambda r: any(abs(m['score']) >= 15000 for m in r['moves']),
                              lambda r: r['cdb_ply'] == -2,
                              lambda r: r['fen'].split()[3] != '-']:
                hit = next((j for j, r in enumerate(rows) if predicate(r)), None)
                if hit is not None:
                    selected.add(hit)
            for j in sorted(selected):
                r = rows[j]
                # This adapter explicitly supports standard chess, not variant castling.
                if not set(r['fen'].split()[2]) <= set('KQkq-'):
                    continue
                tag = dict(kind='archive', file=path, row_group=rg, row=j)
                add(r['fen'], tag, r)
                add(mirror(r['fen']), tag, r, True)
        print(f"sampled {si}: {len(requests)} requests", flush=True)

    rng = random.Random(20261125)
    for game in range(16):
        b = chess.Board()
        for ply in range(120):
            if ply % 16 == 0:
                add(' '.join(b.fen(en_passant='fen').split()[:4]), dict(kind='legal-random', game=game, ply=ply))
                add(' '.join(b.fen(en_passant='legal').split()[:4]), dict(kind='legal-random-legal-ep', game=game, ply=ply))
            if b.is_game_over():
                break
            b.push(rng.choice(list(b.legal_moves)))
    for fen in ['4k3/8/8/3pP3/8/8/8/4K3 w - d6', '4r1k1/8/8/3pP3/8/8/8/4K3 w - d6',
                '4k3/P7/8/8/8/8/8/4K3 w - -', '7k/6Q1/5K2/8/8/8/8/8 b - -']:
        add(fen, dict(kind='edge-case'))
        add(mirror(fen), dict(kind='edge-case-mirror'))
    (out/'requests.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in requests), encoding='utf8')
    (out/'requests.tsv').write_text(''.join(f"{r['id']}\t0\t{r['fen']}\n" for r in requests), encoding='utf8')
    (out/'fens.txt').write_text(''.join(r['fen']+'\n' for r in requests), encoding='utf8')
    (out/'archive-expected.json').write_text(json.dumps(expected), encoding='utf8')
    (out/'panel.json').write_text(json.dumps(dict(requests=len(requests), archived=len(expected),
        inputs_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir()},
        purpose='Exact functional parity, not statistical accuracy study; frozen before querying either reader.'), indent=2), encoding='utf8')


def verify(run: Path, executable: Path) -> None:
    panel = json.loads((run/'panel/panel.json').read_text())
    for name, digest in panel['inputs_sha256'].items():
        assert hashlib.sha256((run/'panel'/name).read_bytes()).hexdigest() == digest, name
    keys = json.loads((run/'rust-keys.json').read_text())
    native_keys = (run/'native-keys.tsv').read_text().splitlines()
    requests = [json.loads(l) for l in (run/'panel/requests.jsonl').read_text().splitlines()]
    assert len(keys) == len(native_keys) == len(requests)
    for a, b in zip(keys, native_keys):
        k, mirror_bit = b.split('\t')
        assert a['source_key'] == k and a['mirrored'] == bool(int(mirror_bit)), (a, b)
    first = json.loads((run/'rust-first.json').read_text())
    rows = first['results']
    native = (run/'native-results.tsv').read_text().splitlines()
    assert len(rows) == len(native) == len(requests)
    for r, line in zip(rows, native):
        lo, hi, ply, moves = line.split('\t')
        parsed = [dict(uci=x.split(':')[0], score=int(x.split(':')[1])) for x in moves.split(';') if x]
        assert r['id'] == int(lo) and hi == '0' and r['ply'] == int(ply) and r['moves'] == parsed and r['found'] == (int(ply) != -2), (r, line)
    expected = json.loads((run/'panel/archive-expected.json').read_text())
    for e in expected:
        actual = rows[e['id']]
        assert all(actual[k] == v for k, v in e.items()), (actual, e)
    times = dict(first_process_seconds=first['total_seconds'], first_metrics=first['metrics'])
    for i in range(2):
        dest = run/f'rust-warm-{i}.json'
        subprocess.run([str(executable), 'query', str(run/'index.json'), str(run/'panel/requests.jsonl'), str(dest)], check=True)
        warm = json.loads(dest.read_text())
        assert warm['results'] == rows
        times[f'warm_process_{i}_seconds'] = warm['total_seconds']
    native_warm = run/'native-warm.tsv'
    def wsl(p):
        p = p.as_posix()
        return '/mnt/'+p[0].lower()+p[2:]
    t = time.perf_counter()
    subprocess.run(['wsl', '-d', 'Ubuntu', '--', '/opt/chessdb-pilot/import/cdb_lookup', wsl(run/'panel/requests.tsv'), wsl(native_warm), '2'], check=True)
    times['native_warm_process_seconds'] = time.perf_counter()-t
    assert native_warm.read_bytes() == (run/'native-results.tsv').read_bytes()
    # Persistent reader: same small opening batch twice, separate from whole-panel cache.
    with subprocess.Popen([str(executable), 'serve', str(run/'index.json')], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding='utf8') as proc:
        for i in range(2):
            t = time.perf_counter()
            proc.stdin.write(json.dumps(requests[-8:])+'\n')
            proc.stdin.flush()
            result = json.loads(proc.stdout.readline())
            assert result['results'] == rows[-8:]
            times[f'persistent_8_positions_{i}_seconds'] = time.perf_counter()-t
        proc.stdin.close()
        assert proc.wait() == 0
    report = dict(verified=True, queries=len(rows), exact_keys=len(keys), exact_native_rows=len(rows), archive_rows=len(expected),
                  found=sum(r['found'] for r in rows), missing=sum(not r['record_present'] for r in rows),
                  empty_records=sum(r['record_present'] and not r['found'] for r in rows),
                  moves=sum(len(r['moves']) for r in rows), promotions=sum(len(m['uci'])==5 for r in rows for m in r['moves']),
                  mate_or_tb_moves=sum(abs(m['score'])>=15000 for r in rows for m in r['moves']),
                  timings=times, caveat='No OS cache flush; panel sampling preceded queries; concurrent pool work. Timings are not cold-drive benchmarks.',
                  executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                  index_sha256=hashlib.sha256((run/'index.json').read_bytes()).hexdigest(),
                  marker_sha256=first['marker_sha256'], panel=panel)
    with (run/'verification.json').open('x', encoding='utf8') as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('archive', type=Path)
    p.add_argument('output', type=Path)
    p.add_argument('--verify-executable', type=Path)
    a = p.parse_args()
    if a.verify_executable:
        verify(a.output, a.verify_executable)
    else:
        create_panel(a.archive, a.output)
