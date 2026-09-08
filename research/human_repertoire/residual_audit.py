"""Retrospective M8 failure audit; no new engine requests or model fitting."""
from collections import Counter
import json
import math

import chess

from .board_benchmark import CONFIG, read, reference, verified_calibration, verified_model
from .board_prior import FEATURES, features
from .io import output_path, sha256, write_json

VALUES = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5, chess.QUEEN: 9, chess.KING: 0}


def capture_value(board, move):
    victim = 1 if board.is_en_passant(move) else VALUES.get(board.piece_type_at(move.to_square), 0)
    return victim + (VALUES[move.promotion]-1 if move.promotion else 0)


def exchange(board, square, depth, budget):
    """Optional-stop material game confined to legal captures on one square.

    This is a feature, not a forced tactical proof. Stopping is allowed even in
    check, noncapture evasions are excluded, and the horizon truncates exchanges.
    """
    budget[0] += 1
    if budget[0] > 20000:
        raise ValueError('exchange node cap')
    if depth == 0:
        return 0
    best = 0
    for move in list(board.legal_moves):
        if move.to_square != square or not board.is_capture(move):
            continue
        value = capture_value(board, move)
        board.push(move)
        try:
            value -= exchange(board, square, depth-1, budget)
        finally:
            board.pop()
        best = max(best, value)
    return best


def exposure(board):
    """Best immediate legal capture followed by at most five same-square replies."""
    result = []; budget = [0]
    for move in list(board.legal_moves):
        if not board.is_capture(move):
            continue
        san = board.san(move); victim = capture_value(board, move)
        target = chess.square_name(move.to_square)
        board.push(move)
        try:
            recaptures = [m for m in board.legal_moves if m.to_square == move.to_square and board.is_capture(m)]
            gain = victim-exchange(board, move.to_square, 5, budget)
        finally:
            board.pop()
        result.append({'san': san, 'target': target, 'victim_value': victim, 'exchange_gain': gain,
                       'immediate_recaptures': len(recaptures)})
    return {'best_gain': max([0]+[r['exchange_gain'] for r in result]),
        'free_nonpawn_capture': any(r['victim_value'] >= 3 and r['immediate_recaptures'] == 0 for r in result),
        'captures': sorted(result, key=lambda r: (-r['exchange_gain'], r['san'])), 'nodes': budget[0]}


def pv_description(board, pv, mover):
    out = []
    for uci in pv[:8]:
        move = chess.Move.from_uci(uci)
        if move not in board.legal_moves:
            raise ValueError('illegal frozen PV')
        out.append({'san': board.san(move), 'capture': board.is_capture(move),
            'captured_value': capture_value(board, move), 'mover_side': board.turn == mover,
            'check': board.gives_check(move)})
        board.push(move)
    return out


def run():
    c = json.loads(CONFIG.read_text()); model = verified_model(c); verified_calibration(c)
    if read('m8/verification.json')['calibration_results_sha256'] != sha256(output_path('m8/calibration/results.json')):
        raise ValueError('unverified input result')
    cases = read('m8/calibration/cases.json'); scored = read('m8/calibration/scored_cases.json')['base']
    records = {**read('m8/calibration/inherited_records.json')['base'], **read('m8/calibration/engine_audit.json')['base']}
    rows = []
    for case, reference_case, loss, reference_loss in zip(cases['candidate'], cases['reference'], scored['candidate'], scored['reference']):
        if not (case['case_id'] == reference_case['case_id'] == loss['case_id'] == reference_loss['case_id']):
            raise ValueError('case order mismatch')
        board = chess.Board(case['fen']); mover = board.turn
        details = {}
        for role, san in [('human', case['human_san']), ('candidate', case['model_san'])]:
            move = board.parse_san(san)
            vector = features(board, move)
            child = board.copy(); child.push(move)
            key = case['human_child' if role == 'human' else 'model_child']
            details[role] = {'san': san, 'features': dict(zip(FEATURES, vector.tolist())),
                'correction_log_weight': model['beta']*math.fsum(a*b for a, b in zip(model['weights'], vector)),
                'exposure': exposure(child), 'pv': pv_description(child.copy(), records[key]['pv'], mover)}
        first = details['candidate']['pv'][0] if details['candidate']['pv'] else None
        row = {'case_id': case['case_id'], 'event': case['event'], 'support': case['support'], 'fen': case['fen'],
            'candidate_loss_cp': loss['model_loss_cp'], 'reference_loss_cp': reference_loss['model_loss_cp'],
            'human_loss_cp': loss['human_loss_cp'], 'candidate_probability': case['model_move_probability'],
            'human_probability': case['human_move_probability'], 'reference_san': reference_case['model_san'],
            'candidate_severe': bool(loss['model_loss_primary']), 'reference_severe': bool(reference_loss['model_loss_primary']),
            'first_pv_reply_capture': bool(first and first['capture']), 'first_pv_reply_check': bool(first and first['check']),
            'details': details}
        rows.append(row)
    if len(rows) != 96:
        raise ValueError('fixed diagnostic requires all 96 cases')
    summary = {}
    for label, subset in [('all', rows), ('candidate_severe', [r for r in rows if r['candidate_severe']]),
                          ('candidate_nonsevere', [r for r in rows if not r['candidate_severe']])]:
        summary[label] = {'cases': len(subset), 'unseen': sum(r['support'] == 'unseen' for r in subset),
            'first_pv_reply_capture': sum(r['first_pv_reply_capture'] for r in subset),
            'first_pv_reply_check': sum(r['first_pv_reply_check'] for r in subset),
            'roles': {role: {'free_nonpawn_capture': sum(r['details'][role]['exposure']['free_nonpawn_capture'] for r in subset),
                'exchange_gain_at_least_1': sum(r['details'][role]['exposure']['best_gain'] >= 1 for r in subset),
                'exchange_gain_at_least_3': sum(r['details'][role]['exposure']['best_gain'] >= 3 for r in subset)} for role in ('human', 'candidate')},
            'candidate_geometric_hanging_mover': sum(r['details']['candidate']['features']['mover_hanging'] > 0 for r in subset),
            'candidate_geometric_hanging_any': sum(r['details']['candidate']['features']['max_friendly_hanging'] > 0 for r in subset),
            'candidate_higher_correction_than_human': sum(r['details']['candidate']['correction_log_weight'] > r['details']['human']['correction_log_weight'] for r in subset)}
    transitions = Counter(('both' if r['reference_severe'] and r['candidate_severe'] else
        'resolved' if r['reference_severe'] else 'introduced' if r['candidate_severe'] else 'neither') for r in rows)
    identity = {'m8_model_sha256': sha256(output_path('m8/frozen_model.json')),
        'm8_calibration_results_sha256': sha256(output_path('m8/calibration/results.json')), 'code_sha256': sha256(__file__),
        'diagnostic_only': True, 'exchange_capture_plies': 6, 'per_child_node_cap': 20000}
    write_json('m9/residual_cases.json', rows)
    result = {'identity': identity, 'rows_sha256': sha256(output_path('m9/residual_cases.json')),
        'summary': summary, 'transitions': dict(transitions), 'exchange_nodes': sum(r['details'][role]['exposure']['nodes'] for r in rows for role in ('human', 'candidate'))}
    write_json('m9/results.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    run()
