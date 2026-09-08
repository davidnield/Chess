import unittest
from unittest.mock import patch

import chess
import numpy as np

from .board_prior import BoardPrior, FEATURES, features, fit_weights, objective_gradient, panel
from .shared_fallback import distribution, move_token, spec
from . import board_benchmark, date_cohort


class BoardPriorTests(unittest.TestCase):
    def test_mirror_and_board_restoration(self):
        boards = [chess.Board(), chess.Board('r3k2r/1P6/8/3pP3/8/8/8/R3K2R w KQkq d6 0 1')]
        for board in boards:
            original = board.fen()
            mirror = board.mirror()
            for move in list(board.legal_moves):
                reflected = chess.Move(chess.square_mirror(move.from_square), chess.square_mirror(move.to_square), promotion=move.promotion)
                np.testing.assert_array_equal(features(board, move), features(mirror, reflected))
                self.assertEqual(board.fen(), original)

    def test_special_moves(self):
        b = chess.Board('r3k2r/1P6/8/3pP3/8/8/8/R3K2R w KQkq d6 0 1')
        self.assertEqual(features(b, b.parse_san('exd6'))[2], 1/9)
        self.assertEqual(features(b, b.parse_san('O-O'))[1], 1)
        self.assertEqual(features(b, b.parse_san('bxa8=Q'))[7], 1)
        self.assertEqual(features(b, b.parse_san('bxa8=Q'))[2], 5/9)

    def test_hanging_queen(self):
        b = chess.Board('7k/8/3p4/8/8/8/4Q3/K7 w - - 0 1')
        x = features(b, b.parse_san('Qe5'))
        self.assertEqual(tuple(x[3:6]), (1, 1, 1))
        self.assertEqual(x[9], 1)

    def test_gradient_finite_difference(self):
        rng = np.random.default_rng(42)
        x = rng.normal(size=(7, len(FEATURES)))
        logq = np.log([.2, .8, .1, .3, .6, .7, .3])
        starts, chosen = np.array([0, 2, 5]), np.array([1, 4, 5])
        w = rng.normal(size=len(FEATURES))*.2
        _, gradient = objective_gradient(w, x, logq, starts, chosen, .01)
        for j in range(len(w)):
            delta = np.zeros(len(w)); delta[j] = 1e-6
            plus = objective_gradient(w+delta, x, logq, starts, chosen, .01)[0]
            minus = objective_gradient(w-delta, x, logq, starts, chosen, .01)[0]
            self.assertAlmostEqual(gradient[j], (plus-minus)/2e-6, places=8)
        fitted, losses = fit_weights((x, logq, starts, chosen), {'phase_seconds': 10, 'gradient_steps': 20, 'ridge': .01, 'learning_rate': .5})
        self.assertLess(losses[-1], losses[0])

    def test_baseline_parity_and_mass(self):
        board = chess.Board(); setting = spec('token', 1, .5)
        tokens = {move_token(board, m): i+1 for i, m in enumerate(board.legal_moves)}
        for counts in ({}, {'e2e4': 8, 'd2d4': 3}):
            fitted = {'positions': {'root': counts}, 'tokens': tokens}
            kernel = BoardPrior(fitted, setting, {'feature_position_cap': 1})
            row = kernel.get('root', board.fen())
            expected = distribution(row['ucis'], [move_token(board, chess.Move.from_uci(m)) for m in row['ucis']], counts, tokens, setting)
            _, p = kernel.probabilities('root', board.fen(), np.ones(10), 0)
            np.testing.assert_allclose(p, expected, rtol=0, atol=1e-14)
            _, corrected = kernel.probabilities('root', board.fen(), np.arange(10)-5, 2)
            self.assertAlmostEqual(sum(corrected), 1)
            self.assertTrue(np.all(corrected > 0))
            with self.assertRaises(ValueError):
                kernel.get('root', board.mirror().fen())

    def test_panel_outcome_independent(self):
        games = [{'game_id': e+str(i), 'event': e, 'result': '1-0'} for e in ('Blitz', 'Rapid', 'Classical') for i in range(4)]
        c = {'development_seed': 'test', 'development_games_per_event': 2}
        a = [g['game_id'] for g in panel(games, c, 'train')]
        b = [g['game_id'] for g in panel([{**g, 'result': '0-1'} for g in reversed(games)], c, 'train')]
        self.assertEqual(a, b)

    def test_adapter_confines_outputs(self):
        original = date_cohort.output_path
        module = board_benchmark.isolated('date_cohort.py', 'm3/', 'm8/cohort/')
        self.assertIs(date_cohort.output_path, original)
        self.assertEqual(module.output_path('m3/probe.json'), board_benchmark.output_path('m8/cohort/probe.json'))
        for name in ('m7/elsewhere.json', 'm3/../elsewhere.json'):
            with self.assertRaises(ValueError):
                module.output_path(name)

    def test_holdout_requires_frozen_model(self):
        with patch.object(board_benchmark, 'verified_model', side_effect=ValueError('unfrozen')), patch.object(board_benchmark, 'isolated') as adapter:
            with self.assertRaises(ValueError):
                board_benchmark.holdout({})
            adapter.assert_not_called()


if __name__ == '__main__':
    unittest.main()
