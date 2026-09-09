import unittest
from unittest.mock import Mock
import chess
from python.zobrist import zobrist_int64
from .maia_policy import MaiaFallbackKernels
from .maia_opponent import MaiaOpponentKernels


class OpponentTests(unittest.TestCase):
    def test_ratings_are_separate_in_both_colors(self):
        for our_color in (chess.WHITE,chess.BLACK):
            policy=Mock()
            policy.probabilities.side_effect=lambda b,*args,**kwargs:{m.uci():1/b.legal_moves.count() for m in b.legal_moves}
            opponent=MaiaOpponentKernels(policy,'Blitz',our_color,2100,1900)
            both=MaiaFallbackKernels(policy,'Blitz',our_color,2100,opponent,opponent.name)
            b=chess.Board()
            for i in range(2):
                both.get(str(zobrist_int64(b)),b.fen())
                if b.turn==our_color: policy.probabilities.assert_called_with(b,'Blitz',2100)
                else: policy.probabilities.assert_called_with(b,'Blitz',1900,self_rating=2100)
                b.push_san('e4' if i==0 else 'e5')

    def test_wrong_side_hash_cap_and_invalid_policy_fail(self):
        policy=Mock(); b=chess.Board(); kernel=MaiaOpponentKernels(policy,'Rapid',chess.WHITE,2000,2000,cap=0)
        with self.assertRaises(ValueError): kernel.get(str(zobrist_int64(b)),b.fen())
        b.push_san('e4')
        with self.assertRaises(ValueError): kernel.get('bad',b.fen())
        with self.assertRaises(ValueError): kernel.get(str(zobrist_int64(b)),b.fen())
        kernel.cap=1; policy.probabilities.return_value={'e7e5':1.0}
        with self.assertRaises(ValueError): kernel.get(str(zobrist_int64(b)),b.fen())

    def test_scenario_ratings_must_be_explicit(self):
        with self.assertRaises(ValueError): MaiaOpponentKernels(Mock(),'Rapid',chess.WHITE,None,2000)
