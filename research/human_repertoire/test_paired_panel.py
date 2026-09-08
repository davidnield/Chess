import json
import unittest
from unittest.mock import patch

import chess
from python.zobrist import zobrist_int64

from .model import Config
from .paired_panel import (CONFIG, check_gates, checked_records, engine,
                           score_panel, target_selection)
from .verify_rollout import reconstruct


class PairedPanelTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads(CONFIG.read_text()) | {"panel_trajectories_per_arm": 2, "deeper_pairs": 1}
        self.fens = {}
        self.keys = []
        board = chess.Board()
        for move in (None, "e4", "e5", "Nf3"):
            if move:
                board.push_san(move)
            key = str(zobrist_int64(board))
            self.keys.append(key)
            self.fens[key] = board.fen()
        self.paths = {arm: [{"index": i, "keys": self.keys[:3+i]} for i in range(2)]
                      for arm in ("x/source_one_card", "x/two_cards")}
        self.records = {"base": {}, "deeper": {}}

    def test_target_completeness_order_and_shared_position_deduplication(self):
        r = target_selection(self.paths, self.fens, [], self.records, self.c)
        self.assertEqual(r["base"]["targets"], self.keys)
        self.assertEqual(r["deeper"]["targets"], self.keys[:3])
        self.assertEqual(r["base"]["pairs"], 2)
        # Key insertion order and arm order cannot change the frozen ranking.
        self.assertEqual(r, target_selection(dict(reversed(list(self.paths.items()))), dict(reversed(list(self.fens.items()))), [], self.records, self.c))

    def test_reuse_never_substitutes_deeper_scores_into_base_budget(self):
        key = self.keys[0]
        self.records["deeper"][key] = {"fen": self.fens[key], "cp": 350}
        r = target_selection(self.paths, self.fens, [key], self.records, self.c)
        self.assertIn(key, r["base"]["targets"])
        self.assertIn(key, r["deeper"]["reused"])
        audit = {"complete": True, "deeper": {key: {**self.records["deeper"][key], "limits": {"nodes": 400000, "seconds": 2.0}}}}
        self.assertEqual(checked_records(audit, "deeper", 400000, 2), audit["deeper"])
        with self.assertRaisesRegex(ValueError, "budget mismatch"):
            checked_records(audit, "deeper", 100000, 1)

    def test_terminal_exclusion_and_off_path_gate_coverage(self):
        board = chess.Board("7k/6Q1/6K1/8/8/8/8/8 b - - 0 1")
        key = str(zobrist_int64(board))
        self.fens[key] = board.fen()
        self.paths["x/two_cards"][0]["keys"] = [key]
        gates = [self.keys[-1]]
        r = target_selection(self.paths, self.fens, gates, self.records, self.c)
        self.assertIn(key, r["base"]["exact_terminals"])
        self.assertNotIn(key, r["base"]["targets"])
        self.assertEqual(r["deeper"]["targets"][0], gates[0])

    def test_caps_and_missing_indices_fail_without_discarding_paths(self):
        with self.assertRaisesRegex(ValueError, "no paths dropped"):
            target_selection(self.paths, self.fens, [], self.records, self.c | {"max_base_positions": 1})
        self.paths["x/two_cards"].pop()
        with self.assertRaisesRegex(ValueError, "every paired index"):
            target_selection(self.paths, self.fens, [], self.records, self.c)

    def test_cached_counter_sensitivity_is_rejected(self):
        key = self.keys[0]
        board = chess.Board(self.fens[key])
        board.halfmove_clock = 95
        self.records["base"][key] = {"fen": board.fen(), "cp": 0}
        with self.assertRaisesRegex(ValueError, "counter mismatch"):
            target_selection(self.paths, self.fens, [], self.records, self.c)

    def test_complete_panel_requires_scores_even_after_a_known_hit(self):
        paths = {arm: [{"index": i, "keys": ["r", "a"], "reason": "horizon", "wdl": [0, 1, 0], "exact_cp": None} for i in range(2)]
                 for arm in ("x/source_one_card", "x/two_cards")}
        cfg = Config(horizon=1)
        partial = score_panel(paths, {"r": 350}, cfg, .05)
        self.assertEqual(partial["two_minus_one"]["first_advantage"]["sample_difference_interval"], [0, 0])
        with self.assertRaisesRegex(ValueError, "missing positions"):
            score_panel(paths, {"r": 350}, cfg, .05, True)
        complete = score_panel(paths, {"r": 350, "a": 0}, cfg, .05, True)
        for arm in complete["arms"].values():
            self.assertEqual(arm["metrics"]["unresolved_first_hit_mass"]["mean"], 0)
            self.assertEqual(arm["metrics"]["first_advantage"]["sample_observation_interval"], [1, 1])
        independent = reconstruct(paths["x/two_cards"][0], {"r": 350, "a": 0}, cfg)
        self.assertEqual(independent["certain_early_advantage"], 1)

    def test_gate_failure_is_explicit_under_the_selected_budget(self):
        gates = [{"position": "r", "child": "a", "move": "x"}]
        self.assertTrue(check_gates(gates, {"r": 0, "a": 0}, Config())[0]["passed"])
        self.assertFalse(check_gates(gates, {"r": 200, "a": 0}, Config())[0]["passed"])
        self.assertFalse(check_gates(gates, {"r": 0}, Config())[0]["passed"])

    def test_engine_drift_fails_before_launch(self):
        p = {"engine_path": "unused", "engine_identity": {"binary": "expected"}}
        with patch("research.human_repertoire.paired_panel.verified_panel", return_value=p), patch("research.human_repertoire.paired_panel.fingerprint", return_value="changed"), patch("chess.engine.SimpleEngine.popen_uci") as launch:
            with self.assertRaisesRegex(ValueError, "executable drift"):
                engine(self.c)
            launch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
