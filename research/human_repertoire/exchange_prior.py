"""Thirteen-feature candidate and January-only, fixed-order cost pilot."""
import ctypes
from ctypes import wintypes
import json
import time

import chess
import numpy as np

from .board_benchmark import CONFIG as M8_CONFIG, read, verified_model
from .board_prior import FEATURES as BASE_FEATURES, features as base_features, panel
from .date_cohort import CONFIG as M3_CONFIG
from .io import ROOT, output_path, sha256, write_json
from .residual_audit import exposure
from .shared_fallback import inputs

FEATURES = BASE_FEATURES + ('opponent_exchange_gain', 'mover_exchange_gain', 'opponent_check_count')


def memory():
    """Windows process working set and private commit; bytes, not Python-only heap."""
    class Counters(ctypes.Structure):
        _fields_ = [('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in ('PeakWorkingSetSize', 'WorkingSetSize',
            'QuotaPeakPagedPoolUsage', 'QuotaPagedPoolUsage', 'QuotaPeakNonPagedPoolUsage',
            'QuotaNonPagedPoolUsage', 'PagefileUsage', 'PeakPagefileUsage', 'PrivateUsage')]
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    psapi = ctypes.WinDLL('psapi', use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    info = Counters(); info.cb = ctypes.sizeof(info)
    if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(info), info.cb):
        raise ctypes.WinError(ctypes.get_last_error())
    return {'working_set_bytes': info.WorkingSetSize, 'peak_working_set_bytes': info.PeakWorkingSetSize,
            'private_bytes': info.PrivateUsage, 'peak_pagefile_bytes': info.PeakPagefileUsage}


def features(board, move):
    base = base_features(board, move)
    destination = move.to_square
    board.push(move)
    try:
        ex = exposure(board)
        mover_gains = [0]
        for row in ex['captures']:
            reply = board.parse_san(row['san'])
            victim = reply.to_square
            if board.is_en_passant(reply):
                victim += -8 if board.turn else 8
            if victim == destination:
                mover_gains.append(row['exchange_gain'])
        checks = sum(board.gives_check(m) for m in board.legal_moves)
        extra = [min(9, ex['best_gain'])/9, min(9, max(mover_gains))/9, min(8, checks)/8]
        return np.r_[base, extra], ex['nodes']
    finally:
        board.pop()


def alternatives(games):
    for game in games:
        for ply, position in enumerate(game['positions'][:-1]):
            board = chess.Board(position['fen'])
            for move in sorted(board.legal_moves, key=lambda m: board.san(m)):
                yield game['game_id'], ply, board, move


def pilot():
    c8 = json.loads(M8_CONFIG.read_text()); verified_model(c8)
    games = panel(inputs(json.loads(M3_CONFIG.read_text()), ('train',))['train'], c8, 'train')
    if [g['game_id'] for g in games] != read('m8/development_ids.json')['train']:
        raise ValueError('January panel drift')
    ident = {'model_sha256': sha256(output_path('m8/frozen_model.json')), 'panel_sha256': sha256(output_path('m8/development_ids.json')),
        'code_sha256': sha256(__file__), 'exchange_code_sha256': sha256(ROOT/'research/human_repertoire/residual_audit.py'),
        'alternatives': 1000, 'seconds_cap': 30, 'node_cap_per_child': 20000, 'private_memory_cap_bytes': 268435456}
    rows = []; started = time.perf_counter(); before = memory(); peak_private = before['private_bytes']
    status, error = 'passed', None
    try:
        for game_id, ply, board, move in alternatives(games):
            if time.perf_counter()-started > 30:
                raise TimeoutError('pilot runtime cap')
            vector, nodes = features(board, move)
            rows.append({'game_id': game_id, 'ply': ply, 'fen': board.fen(), 'uci': move.uci(),
                         'features': vector.tolist(), 'exchange_nodes': nodes})
            if len(rows) % 50 == 0:
                current = memory(); peak_private = max(peak_private, current['private_bytes'])
                if peak_private > ident['private_memory_cap_bytes']:
                    raise MemoryError('pilot private-memory cap')
            if len(rows) == 1000:
                break
        if len(rows) != 1000:
            raise ValueError('short pilot')
        if time.perf_counter()-started > 30:
            raise TimeoutError('pilot runtime cap at completion')
    except (TimeoutError, ValueError, MemoryError) as exc:
        status, error = 'failed', str(exc)
    elapsed = time.perf_counter()-started
    result = {'identity': ident, 'status': status, 'error': error, 'completed_alternatives': len(rows),
        'seconds': elapsed, 'exchange_nodes': sum(r['exchange_nodes'] for r in rows),
        'max_child_nodes': max([0]+[r['exchange_nodes'] for r in rows]), 'memory_before': before,
        'memory_after': memory(), 'max_sampled_private_bytes': peak_private,
        'distinct_positions': len({r['fen'] for r in rows}), 'distinct_games': len({r['game_id'] for r in rows}),
        'rows_sha256': sha256(write_json('m10/pilot_rows.json', rows))}
    write_json('m10/pilot.json', result)
    print(json.dumps(result, indent=2))
    if status != 'passed':
        raise RuntimeError(error)


if __name__ == '__main__':
    pilot()
