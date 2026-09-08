import unittest
from unittest.mock import patch
import chess

from .maia_policy import MaiaPolicy, MaiaFallbackKernels, model_type, validate_probabilities


class MaiaPolicyContractTests(unittest.TestCase):
    def test_time_controls(self):
        self.assertEqual(model_type('Blitz'), 'blitz')
        self.assertEqual(model_type('Rapid'), 'rapid')
        self.assertEqual(model_type('Classical'), 'rapid')
        with self.assertRaises(ValueError): model_type('Bullet')

    def test_mass_support_and_rounded_zero_rejected(self):
        board = chess.Board(); p = {m.uci(): 1/20 for m in board.legal_moves}
        validate_probabilities(board, p)
        for bad in ({**p, 'a1a8': .1}, {**p, 'e2e4': 0}, {**p, 'e2e4': float('nan')}, {**p, 'e2e4': .2}):
            with self.assertRaises(ValueError): validate_probabilities(board, bad)

    def test_terminal_skips_model(self):
        policy = object.__new__(MaiaPolicy)
        for fen in ('7k/5Q2/6K1/8/8/8/8/8 b - - 0 1', '7k/6Q1/6K1/8/8/8/8/8 b - - 0 1', '7k/8/8/8/8/8/8/K7 w - - 0 1'):
            with patch.object(policy, 'load') as load:
                self.assertEqual(policy.probabilities(chess.Board(fen), 'Rapid', 1800), {})
                load.assert_not_called()

    def test_sampling_retains_nonargmax_and_passes_ratings(self):
        policy = object.__new__(MaiaPolicy); board = chess.Board()
        with patch.object(policy, 'probabilities', return_value={'d2d4': .2, 'e2e4': .8}) as method:
            self.assertEqual(policy.sample(board, 'Rapid', 2100, .1), 'd2d4')
            method.assert_called_with(board, 'Rapid', 2100, None)
            self.assertEqual(policy.sample(board, 'Rapid', 2100, .9), 'e2e4')

    def test_evaluator_adapter_does_not_weaken_opponent(self):
        from unittest.mock import Mock
        from python.zobrist import zobrist_int64
        policy, opponent = Mock(), Mock()
        b = chess.Board(); ps = {m.uci(): 1/20 for m in b.legal_moves}
        policy.probabilities.return_value = ps
        kernel = MaiaFallbackKernels(policy, 'Blitz', chess.WHITE, 2000, opponent, 'existing', cap=2)
        key = str(zobrist_int64(b))
        moves, probs = kernel.get(key, b.fen())
        self.assertEqual(set(moves.values()), set(ps))
        self.assertAlmostEqual(sum(probs['maia1400'].values()), 1)
        policy.probabilities.assert_called_once_with(b, 'Blitz', 2000)
        b.push_san('e4'); other_key = str(zobrist_int64(b))
        opponent.get.return_value = ({'e5': 'e7e5'}, {'existing': {'e5': 1.0}})
        self.assertEqual(kernel.get(other_key, b.fen())[1], {'maia1400': {'e5': 1.0}})
        self.assertEqual(policy.probabilities.call_count, 1)

    def test_evaluator_adapter_hash_and_cap(self):
        from unittest.mock import Mock
        from python.zobrist import zobrist_int64
        b = chess.Board(); kernel = MaiaFallbackKernels(Mock(), 'Rapid', chess.WHITE, 1900, Mock(), 'existing', cap=0)
        with self.assertRaises(ValueError): kernel.get('wrong', b.fen())
        with self.assertRaises(ValueError): kernel.get(str(zobrist_int64(b)), b.fen())
