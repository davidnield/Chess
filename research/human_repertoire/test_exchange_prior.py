import unittest

import chess
import numpy as np

from .board_prior import features as base_features
from .exchange_prior import features


class ExchangePriorTests(unittest.TestCase):
    def test_mirror_base_parity_and_restoration(self):
        board = chess.Board('r3k2r/1P6/8/3pP3/8/8/8/R3K2R w KQkq d6 0 1')
        fen = board.fen()
        for move in list(board.legal_moves):
            vector, nodes = features(board, move)
            mirror = chess.Move(chess.square_mirror(move.from_square), chess.square_mirror(move.to_square), promotion=move.promotion)
            reflected, _ = features(board.mirror(), mirror)
            np.testing.assert_array_equal(vector, reflected)
            np.testing.assert_array_equal(vector[:10], base_features(board, move))
            self.assertTrue(np.all((vector[10:] >= 0) & (vector[10:] <= 1)))
            self.assertEqual(board.fen(), fen)
            self.assertLessEqual(nodes, 20000)

    def test_en_passant_identifies_moved_victim(self):
        b = chess.Board('7k/8/8/8/3p4/8/4P3/K7 w - - 0 1')
        vector, _ = features(b, b.parse_san('e4'))
        self.assertEqual(vector[10], 1/9)
        self.assertEqual(vector[11], 1/9)

    def test_defended_queen_and_check_replies(self):
        b = chess.Board('7k/8/3p4/8/3P4/8/4Q3/K7 w - - 0 1')
        vector, _ = features(b, b.parse_san('Qe5'))
        self.assertEqual(vector[10], 8/9)
        self.assertEqual(vector[11], 8/9)
        b = chess.Board('7k/8/8/8/8/7r/8/K7 w - - 0 1')
        move = b.parse_san('Kb1'); vector, _ = features(b, move)
        b.push(move)
        self.assertEqual(vector[12], min(8, sum(b.gives_check(m) for m in b.legal_moves))/8)


if __name__ == '__main__':
    unittest.main()
