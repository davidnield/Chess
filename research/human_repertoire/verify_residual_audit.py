"""Verify the retrospective ledger and export all severe cases as annotated PGN."""
from collections import Counter
import json

import chess
import chess.pgn

from .board_benchmark import read
from .io import ROOT, output_path, sha256, write_json
from .residual_audit import capture_value, exposure


def main():
    rows, result = read('m9/residual_cases.json'), read('m9/results.json')
    assert result['rows_sha256'] == sha256(output_path('m9/residual_cases.json'))
    assert result['identity']['code_sha256'] == sha256(ROOT/'research/human_repertoire/residual_audit.py')
    source = {r['case_id']: r for r in read('m8/calibration/cases.json')['candidate']}
    scored = {r['case_id']: r for r in read('m8/calibration/scored_cases.json')['base']['candidate']}
    assert len(rows) == len(source) == 96
    nodes = 0; pgns = []
    for row in rows:
        case = source[row['case_id']]; original = chess.Board(case['fen'])
        assert row['fen'] == case['fen']
        assert row['candidate_loss_cp'] == scored[row['case_id']]['model_loss_cp']
        assert row['candidate_severe'] == (row['candidate_loss_cp'] >= 150)
        game = chess.pgn.Game(); game.setup(original)
        game.headers.update(Event='M9 retrospective residual audit', Site='Frozen April convenience sample',
            White='Actual parent: White', Black='Actual parent: Black', Result='*')
        game.comment = f"Case {row['case_id']}; {row['event']}; {row['support']}; candidate loss {row['candidate_loss_cp']} cp. Model draw, not an observed human move."
        for role in ('candidate', 'human'):
            detail = row['details'][role]
            board = original.copy(); move = board.parse_san(detail['san']); board.push(move)
            assert exposure(board) == detail['exposure']
            if row['candidate_severe']:
                node = game.add_variation(move)
                node.comment = role+' move; existing base-budget engine continuation follows.'
            for item in detail['pv']:
                move = board.parse_san(item['san'])
                assert board.is_capture(move) == item['capture']
                assert capture_value(board, move) == item['captured_value']
                assert board.gives_check(move) == item['check']
                board.push(move); nodes += 1
                if row['candidate_severe']:
                    node = node.add_variation(move)
        if row['candidate_severe']:
            pgns.append(str(game))
    assert len(pgns) == 22
    transitions = Counter(('both' if r['reference_severe'] and r['candidate_severe'] else 'resolved' if r['reference_severe'] else 'introduced' if r['candidate_severe'] else 'neither') for r in rows)
    assert dict(transitions) == result['transitions']
    for label, summary in result['summary'].items():
        subset = [r for r in rows if label == 'all' or r['candidate_severe'] == (label == 'candidate_severe')]
        assert len(subset) == summary['cases']
        for role in ('human', 'candidate'):
            ex = [r['details'][role]['exposure'] for r in subset]
            assert summary['roles'][role] == {'free_nonpawn_capture': sum(e['free_nonpawn_capture'] for e in ex),
                'exchange_gain_at_least_1': sum(e['best_gain'] >= 1 for e in ex), 'exchange_gain_at_least_3': sum(e['best_gain'] >= 3 for e in ex)}
    path = output_path('m9/severe_cases.pgn'); path.write_text('\n\n'.join(pgns)+'\n', encoding='utf-8')
    with path.open() as stream:
        count = 0
        while (game := chess.pgn.read_game(stream)) is not None:
            assert not game.errors and len(game.variations) == 2
            count += 1
    assert count == 22
    verification = {'status': 'passed', 'cases': len(rows), 'legal_pv_plies': nodes, 'pgn_cases': count,
        'pgn_sha256': sha256(path), 'results_sha256': sha256(output_path('m9/results.json')), 'verifier_sha256': sha256(__file__)}
    write_json('m9/verification.json', verification)
    print(json.dumps(verification, indent=2))


if __name__ == '__main__':
    main()
