import unittest

import chess

from .residual_audit import capture_value, exchange, exposure, pv_description


def clone_oracle(board, square, depth):
    if depth == 0:
        return 0
    scores = [0]
    for move in board.legal_moves:
        if board.is_capture(move) and move.to_square == square:
            child = board.copy(); value = capture_value(board, move); child.push(move)
            scores.append(value-clone_oracle(child, square, depth-1))
    return max(scores)


class ResidualAuditTests(unittest.TestCase):
    def test_defended_queen_still_loses_exchange(self):
        b = chess.Board('7k/8/3p4/4Q3/3P4/8/8/K7 b - - 0 1')
        old = b.fen(); out = exposure(b)
        self.assertEqual(out['best_gain'], 8)
        self.assertFalse(out['free_nonpawn_capture'])
        self.assertEqual(b.fen(), old)

    def test_pinned_attacker_not_legal_capture(self):
        b = chess.Board('4k3/4n3/8/3Q4/8/8/8/K3R3 b - - 0 1')
        self.assertTrue(b.is_attacked_by(chess.BLACK, chess.D5))
        self.assertFalse(any(r['target'] == 'd5' for r in exposure(b)['captures']))

    def test_en_passant_and_capture_promotion(self):
        b = chess.Board('7k/8/8/3pP3/8/8/8/K7 w - d6 0 1')
        self.assertEqual(exposure(b)['best_gain'], 1)
        b = chess.Board('r6k/1P6/8/8/8/8/8/K7 w - - 0 1')
        self.assertEqual(exposure(b)['best_gain'], 13)

    def test_exchange_matches_independent_copy_tree(self):
        b = chess.Board('3r3k/8/3p4/4Q3/3P4/8/8/K2R4 b - - 0 1')
        for square in (chess.D4, chess.E5):
            for depth in range(7):
                self.assertEqual(exchange(b, square, depth, [0]), clone_oracle(b, square, depth))

    def test_caps_restore_board_and_illegal_pv_rejected(self):
        b = chess.Board('7k/8/3p4/4Q3/3P4/8/8/K7 b - - 0 1')
        old = b.fen()
        with self.assertRaises(ValueError):
            exchange(b, chess.E5, 6, [19999])
        self.assertEqual(b.fen(), old)
        with self.assertRaises(ValueError):
            pv_description(chess.Board(), ['e2e5'], chess.WHITE)


if __name__ == '__main__':
    unittest.main()
