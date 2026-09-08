import copy
import json
import unittest

import chess
from python.zobrist import zobrist_int64

from .calibration_experiment import CONFIG
from .loss_calibration import components, exact_terminal_cp, loss_rows, propose, select_cases, summaries, targets
from .shared_fallback import spec


class LossCalibrationTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads(CONFIG.read_text()) | {"events": ["Blitz"], "cases_per_stratum": 2, "deeper_cases_per_stratum": 1, "bootstrap_draws": 100}
        board = chess.Board()
        self.positions = [{"key": str(zobrist_int64(board)), "fen": board.fen()}]
        for san in ("e4", "e5", "Nf3"):
            board.push_san(san)
            self.positions.append({"key": str(zobrist_int64(board)), "fen": board.fen()})
        self.games = [{"game_id": str(i), "event": "Blitz", "utc_date": "2024-03-10", "time_control": "180+0",
            "white": "w"+str(i), "black": "b"+str(i), "white_elo": 1800, "black_elo": 2000,
            "positions": self.positions, "moves": ["e4", "e5", "Nf3"], "result": "1-0"} for i in range(4)]
        self.fitted = {"positions": {self.positions[0]["key"]: {"e2e4": 1}}, "tokens": {}}

    def test_deterministic_selection_per_game_quota_and_no_outcome_dependence(self):
        a, frame = select_cases(self.games, self.fitted, self.c)
        changed = [{**g, "result": "0-1", "wdl": [0, 0, 1], "engine_cp": 900} for g in reversed(self.games)]
        self.assertEqual((a, frame), select_cases(changed, self.fitted, self.c))
        self.assertEqual(len(a), 4)
        self.assertEqual(len({(r["game_id"], r["support"]) for r in a}), 4)
        self.assertEqual(frame["strata"]["Blitz/unseen"], {"eligible_games": 4, "eligible_decisions": 8})
        for r in a:
            self.assertEqual(r["case_inclusion_probability"], .5/(2 if r["support"] == "unseen" else 1))

    def test_short_frame_rating_period_and_scan_caps_fail_without_replacement(self):
        with self.assertRaisesRegex(ValueError, "short stratum"):
            select_cases(self.games[:1], self.fitted, self.c)
        for changes in ({"white_elo": 1799}, {"black_elo": 2001}, {"utc_date": "2024-02-01"}):
            with self.assertRaises(ValueError):
                select_cases([{**self.games[0], **changes}], self.fitted, self.c)
        with self.assertRaisesRegex(ValueError, "decision cap"):
            select_cases(self.games, self.fitted, self.c | {"max_decisions": 1})

    def test_model_draw_is_legal_reproducible_and_keeps_full_targets(self):
        selected, _ = select_cases(self.games, self.fitted, self.c)
        cases, fens = propose(selected, self.fitted, {"shared_position": spec("token", 1, .5)}, self.c)
        self.assertEqual((cases, fens), propose(selected, self.fitted, {"shared_position": spec("token", 1, .5)}, self.c))
        for r in cases:
            for role in ("human", "model"):
                board = chess.Board(r["fen"])
                board.push_san(r[role+"_san"])
                self.assertEqual(str(zobrist_int64(board)), r[role+"_child"])
        old = {"base": {}, "deeper": {cases[0]["parent"]: {"fen": cases[0]["fen"], "cp": 300}}}
        selection = targets(cases, fens, old, self.c)
        self.assertEqual(set(selection["base"]["targets"]), set(fens))
        self.assertEqual(len(selection["deeper"]["reused"]), 1)
        with self.assertRaisesRegex(ValueError, "position cap"):
            targets(cases, fens, old, self.c | {"max_base_positions": 1})

    def case(self, white=True):
        return {"case_id": "a", "game_id": "a", "white": "one", "black": "two", "stratum": "Blitz/unseen",
            "event": "Blitz", "support": "unseen", "parent": "p", "human_child": "h", "model_child": "m",
            "human_san": "a", "model_san": "b", "white_to_move": white}

    def test_mover_perspective_loss_clipping_caps_and_threshold_equality(self):
        a = loss_rows([self.case()], {"p": 50, "h": -100, "m": 90}, self.c)[0]
        b = loss_rows([self.case(False)], {"p": -50, "h": 100, "m": -90}, self.c)[0]
        for key in ("human_loss_cp", "model_loss_cp", "human_loss_primary", "model_signed_loss_cp"):
            self.assertEqual(a[key], b[key])
        self.assertEqual((a["human_loss_primary"], a["model_loss_cp"], a["model_signed_loss_cp"]), (1, 0, -40))
        mate = loss_rows([self.case()], {"p": 0, "h": -100000, "m": -300}, self.c)[0]
        self.assertEqual((mate["human_capped_loss_cp"], mate["model_loss_secondary"], mate["mate_involved"]), (1000, 1, True))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            loss_rows([self.case()], {"p": 0, "h": 0}, self.c)

    def test_equal_moves_cancel_and_rule_terminal_orientation(self):
        case = self.case() | {"model_san": "a", "model_child": "h"}
        row = loss_rows([case], {"p": 0, "h": -300}, self.c)[0]
        self.assertTrue(row["same_move"])
        self.assertEqual(row["human_loss_cp"], row["model_loss_cp"])
        self.assertEqual(exact_terminal_cp("7k/6Q1/6K1/8/8/8/8/8 b - - 0 1"), 100000)
        self.assertEqual(exact_terminal_cp("8/8/8/8/8/6k1/6q1/7K w - - 0 1"), -100000)
        self.assertEqual(exact_terminal_cp("7k/5Q2/6K1/8/8/8/8/8 b - - 0 1"), 0)
        self.assertIsNone(exact_terminal_cp(chess.STARTING_FEN))

    def test_components_preserve_cross_stratum_player_connections(self):
        rows = [{"white": "A", "black": "B"}, {"white": "b", "black": "C"}, {"white": "X", "black": "Y"}]
        self.assertEqual(sorted(map(len, components(rows))), [1, 2])

    def test_balancing_retains_stratum_weights_and_reports_empty_bootstrap_draws(self):
        base = loss_rows([self.case()], {"p": 0, "h": 0, "m": -200}, self.c)[0]
        second = copy.deepcopy(base)
        second.update(case_id="b", game_id="b", white="other", black="another", stratum="Blitz/seen", support="seen", model_loss_primary=0)
        rows = [base, {**base, "case_id": "c"}, second]
        result = summaries(rows, self.c)
        self.assertEqual(result["groups"]["balanced"]["metrics"]["loss_primary"]["difference"], .5)
        self.assertGreater(result["groups"]["balanced"]["empty_stratum_draws"], 0)
        self.assertIsNone(result["groups"]["balanced"]["metrics"]["loss_primary"]["component_percentile_95"])
        self.assertEqual(result["bootstrap"]["components"], 2)


if __name__ == "__main__":
    unittest.main()
