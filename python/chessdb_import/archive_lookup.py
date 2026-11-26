"""Bounded TSV adapter for the Rust ChessDB archive reader.

The binding pins the executable, complete archive generation and sorted shard
index. An existing result is reused only with a matching input/output receipt.
Historical WSL-only frozen import releases are deliberately left unchanged.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import subprocess
from pathlib import Path


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(4 << 20), b''):
            h.update(b)
    return h.hexdigest()


def binding(path: Path) -> dict:
    b = json.loads(path.read_text(encoding='utf8'))
    if b.get('schema') != 'chessdb-archive-query-binding-v1':
        raise ValueError('unsupported binding')
    for name in ('executable', 'index'):
        if sha(Path(b[name])) != b[name+'_sha256']:
            raise ValueError(f'{name} identity changed')
    index = json.loads(Path(b['index']).read_text(encoding='utf8'))
    if sha(Path(index['archive'])/'ARCHIVE.COMPLETE.json') != b['marker_sha256'] or index['marker_sha256'] != b['marker_sha256']:
        raise ValueError('archive identity changed')
    return b


def lookup(bind_path: Path, source: Path, output: Path, batch_size: int = 512) -> dict:
    if not 1 <= batch_size <= 4096:
        raise ValueError('batch size must be 1..4096')
    b = binding(bind_path)
    identity = dict(binding_sha256=sha(bind_path), input_sha256=sha(source), adapter_sha256=sha(Path(__file__)))
    receipt = output.with_suffix(output.suffix+'.receipt.json')
    if output.exists() or receipt.exists():
        if not output.exists() or not receipt.exists():
            raise ValueError('unreceipted/incomplete result; preserve it and choose a new output')
        r = json.loads(receipt.read_text(encoding='utf8'))
        if any(r.get(k) != v for k, v in identity.items()) or r.get('output_sha256') != sha(output):
            raise ValueError('resume identity/output differs')
        # Reusing a fully receipted result does not require reading archive shards.
        return r
    output.parent.mkdir(parents=True, exist_ok=True)
    lock = output.with_suffix(output.suffix+'.lock')
    tmp = output.with_suffix(output.suffix+'.partial')
    proc = None
    with lock.open('x') as lock_handle:
        try:
            count = found = 0
            with tmp.open('x', encoding='utf8', newline='\n') as out, source.open(encoding='utf8') as inp, output.with_suffix(output.suffix+'.stderr.log').open('x', encoding='utf8') as err:
                proc = subprocess.Popen([b['executable'], 'serve', b['index']], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, text=True, encoding='utf8')
                while lines := list(itertools.islice(inp, batch_size)):
                    ids, requests = [], []
                    for line in lines:
                        lo, hi, fen = line.rstrip('\r\n').split('\t', 2)
                        if not all(-(1<<63) <= int(x) < (1<<63) for x in (lo, hi)):
                            raise ValueError('TSV key exceeds signed 64-bit range')
                        ids.append(f'{lo}\t{hi}')
                        requests.append(dict(id=count+len(requests), fen=fen))
                    proc.stdin.write(json.dumps(requests)+'\n')
                    proc.stdin.flush()
                    line = proc.stdout.readline()
                    if not line:
                        raise RuntimeError('reader failed; inspect stderr log')
                    response = json.loads(line)
                    rows = response['results']
                    if response['marker_sha256'] != b['marker_sha256'] or len(rows) != len(requests):
                        raise ValueError('reader generation or row count differs')
                    for ident, request, row in zip(ids, requests, rows):
                        if row['id'] != request['id']:
                            raise ValueError('reader reordered rows')
                        moves = ';'.join(f"{m['uci']}:{m['score']}" for m in row['moves'])
                        out.write(f"{ident}\t{row['ply']}\t{moves}\n")
                        found += row['found']
                    count += len(rows)
                proc.stdin.close()
                if proc.wait() != 0:
                    raise RuntimeError('reader exited unsuccessfully')
                out.flush()
                os.fsync(out.fileno())
            if sha(source) != identity['input_sha256'] or sha(bind_path) != identity['binding_sha256']:
                raise ValueError('input or binding changed during lookup')
            binding(bind_path)
            result = dict(schema='chessdb-archive-query-receipt-v1', **identity, output_sha256=sha(tmp), rows=count, found=found,
                          marker_sha256=b['marker_sha256'])
            # A crash between publication and receipt fails closed on next run.
            tmp.replace(output)
            rt = receipt.with_suffix(receipt.suffix+'.partial')
            with rt.open('x', encoding='utf8') as f:
                json.dump(result, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            rt.replace(receipt)
            return result
        finally:
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait()
            lock_handle.close()
            lock.unlink()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binding', type=Path, required=True)
    p.add_argument('--input', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--batch-size', type=int, default=512)
    a = p.parse_args()
    print(json.dumps(lookup(a.binding, a.input, a.output, a.batch_size)))


if __name__ == '__main__':
    main()
