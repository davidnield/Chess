"""Bounded, resumable read-only empirical projections for continuation layers."""
import time
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from python.zobrist import zobrist_int64
from .extract_empirical_positions import SOURCE, TAG, footer_hash
from .hashed_board import HashedBoard
from .io import fingerprint, output_path, sha256, write_json
import json

PREFIX = 'followthrough/'
PATHS = {kind: SOURCE/(prefix+TAG+'.parquet') for kind, prefix in
         [('stats', 'position_stats_pooled_'), ('aux', 'position_stats_aux_pooled_')]}
STATS = ['parent_hash', 'parent_epd', 'child_hash', 'move_san', 'total',
         'white_wins', 'draws', 'black_wins', 'event', 'elo_band']
AUX = ['position_hash']+[prefix+'_'+suffix
    for prefix in ['other', 'horizon', 'term_normal', 'term_flag', 'term_other']
    for suffix in ['total', 'white_wins', 'draws', 'black_wins']]


def sources():
    return {kind: {**fingerprint(path), 'footer_sha256': footer_hash(path)} for kind, path in PATHS.items()}


def read(name):
    return json.loads(output_path(PREFIX+name+'.json').read_text())


def save(name, value):
    return write_json(PREFIX+name+'.json', value)


def layer_snapshot(layer, boards, source_identity):
    if len(boards) > 64:
        raise ValueError('empirical query cap')
    identity = {'sources': source_identity, 'boards': boards, 'code_sha256': sha256(__file__)}
    path = output_path(PREFIX+f'layer_{layer}_empirical.json')
    if path.exists():
        result = json.loads(path.read_text())
        if result['identity'] != identity:
            raise ValueError('layer snapshot drift')
        return result
    started = time.monotonic(); result = {'identity': identity, 'work': {}}
    for kind, path in PATHS.items():
        pf = pq.ParquetFile(path); column = 'parent_hash' if kind == 'stats' else 'position_hash'
        columns = STATS if kind == 'stats' else AUX
        keys = pa.array([int(k) for k in boards], type=pa.int64())
        rows = []; decoded = 0; payload = 0
        if pf.metadata.num_rows > 40000000 or pf.num_row_groups > 400:
            raise ValueError('source size cap')
        for index in range(pf.num_row_groups):
            if time.monotonic()-started > 240:
                raise TimeoutError('layer extraction time cap')
            if pf.metadata.row_group(index).total_byte_size > 32*1024**2:
                raise ValueError('row group size cap')
            hashes = pf.read_row_group(index, columns=[column], use_threads=False)
            mask = pc.is_in(hashes[column], value_set=keys); decoded += hashes.nbytes
            if pc.any(mask).as_py():
                table = pf.read_row_group(index, columns=columns, use_threads=False)
                decoded += table.nbytes; payload += 1
                rows.extend(table.filter(mask).to_pylist()); del table
            if len(rows) > 5000 or decoded > 4*1024**3:
                raise ValueError('layer projection/output cap')
        result['work'][kind] = {'rows': len(rows), 'decoded_bytes': decoded,
                               'hash_groups': pf.num_row_groups, 'payload_groups': payload}
        if kind == 'stats':
            result[kind] = {k: [] for k in boards}
            for row in rows:
                key = str(row['parent_hash']); board = HashedBoard(row['parent_epd'])
                if str(zobrist_int64(board)) != key or board.fen().split()[:4] != boards[key].split()[:4]:
                    raise ValueError('empirical parent drift')
                if row['event'] != 'Pooled' or row['elo_band'] != 0 or row['total'] <= 0:
                    raise ValueError('invalid cohort/count')
                if sum(row[k] for k in ['white_wins', 'draws', 'black_wins']) != row['total']:
                    raise ValueError('WDL count mismatch')
                board.push_san(row['move_san'])
                if str(zobrist_int64(board)) != str(row['child_hash']):
                    raise ValueError('empirical child drift')
                result[kind][key].append(row)
            for rows in result[kind].values():
                if len(rows) != len({r['move_san'] for r in rows}):
                    raise ValueError('duplicate empirical move')
        else:
            result[kind] = {str(row['position_hash']): row for row in rows}
            if len(result[kind]) != len(rows):
                raise ValueError('duplicate aux position')
            for row in rows:
                for prefix in ['other', 'horizon', 'term_normal', 'term_flag', 'term_other']:
                    if row[prefix+'_total'] < 0 or sum(row[prefix+'_'+k] for k in ['white_wins', 'draws', 'black_wins']) != row[prefix+'_total']:
                        raise ValueError('aux WDL count mismatch')
    if sources() != source_identity:
        raise ValueError('canonical source changed during lookup')
    result['seconds'] = time.monotonic()-started
    save(f'layer_{layer}_empirical', result)
    return result
