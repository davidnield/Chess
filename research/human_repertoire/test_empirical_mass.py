import unittest
from .exposure_real import empirical_edges


class EmpiricalMassTests(unittest.TestCase):
    def test_tail_horizon_and_endings_are_not_silently_renormalized(self):
        a=empirical_edges([{'move_san':'e5','total':80}],{'other_total':10,'horizon_total':5,
            'term_flag_total':5,'term_flag_white_wins':2,'term_flag_draws':1,'term_flag_black_wins':2})
        self.assertEqual(a['denominator'],100)
        self.assertAlmostEqual((1-a['end_probability'])*a['probabilities']['e5'],.8)
        self.assertAlmostEqual((1-a['end_probability'])*a['unresolved_probability'],.15)
        self.assertAlmostEqual(a['end_probability'],.05)
        self.assertAlmostEqual(sum(a['end_wdl']),1)
