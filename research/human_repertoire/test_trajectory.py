from dataclasses import replace
import json
from math import sqrt
import unittest
from unittest.mock import patch

import chess
from python.zobrist import zobrist_int64

from .continuation_audit import CONFIG
from .empirical import stats
from .fixtures import demanding, shared_reentry
from .model import Config, Edge, Graph, Node, evaluate
from .oracle import path_evaluate
from .rollout_experiment import final, select_missing_targets
from .shared_fallback import fit, spec
from .trajectory import (ChessBackend, GraphBackend, arm_summary, categorical,
    observation_endpoints, paired_summary, sample_trace, score_trace, uniform)


class TrajectoryTests(unittest.TestCase):
    def test_sampling_matches_independent_tiny_path_oracle(self):
        cases = [(demanding(), {"root": "demanding", "d1": "remember1", "d2": "remember2"}),
                 (shared_reentry(), {"shared": "find"})]
        for graph, book in cases:
            backend = GraphBackend(graph)
            evals = {k: n.cp for k, n in graph.nodes.items()}
            for reentry in (True, False):
                cfg = Config(recall=.65, reentry=reentry, recall_by_position={"d2": .3})
                exact = path_evaluate(graph, book, cfg).report()
                rows = [score_trace(sample_trace(backend, "x", book, cfg, "tiny", i), evals, cfg) for i in range(8192)]
                for name, value in arm_summary(rows)["metrics"].items():
                    if "mean" in value:
                        self.assertLessEqual(abs(value["mean"]-exact[name]), 6*value["mc_se"]+2/len(rows), name)
                for r in rows:
                    self.assertAlmostEqual(r["win"]+r["draw"]+r["loss"], 1)
                    self.assertEqual(r["advantage"]+r["disadvantage"]+r["no_hit"], 1)

    def test_failed_recall_disables_preparation_even_when_fallback_finds_move(self):
        g = demanding()
        book = {"root": "demanding", "d1": "remember1", "d2": "remember2"}
        def draw(seed, index, ply, channel):
            if channel == "recall":
                return .9 if ply == 0 else .1
            return .05 if ply == 0 else .95
        with patch("research.human_repertoire.trajectory.uniform", side_effect=draw):
            a = sample_trace(GraphBackend(g), "x", book, Config(recall=.5, reentry=False), "x", 0)
            b = sample_trace(GraphBackend(g), "x", book, Config(recall=.5, reentry=True), "x", 0)
        self.assertEqual(a["moves"][:3], ["demanding", "forced", "slip1"])
        self.assertEqual(b["moves"][:3], ["demanding", "forced", "remember1"])

    def test_ending_horizon_and_repetition_arrival_priority(self):
        graph = Graph("r", {"r": Node("opp", (Edge("next", "a", 1),), cp=0),
            "a": Node("us", (Edge("back", "r", 1),), cp=350, end_probability=.4, end_wdl=(0, 0, 1))})
        cfg = Config(horizon=8)
        rows = [score_trace(sample_trace(GraphBackend(graph), "x", {}, cfg, "ending", i), {"r": 0, "a": 350}, cfg) for i in range(8192)]
        exact = evaluate(graph, {}, cfg).report()
        for name, value in arm_summary(rows)["metrics"].items():
            if "mean" in value:
                self.assertLessEqual(abs(value["mean"]-exact[name]), 6*value["mc_se"]+2/len(rows), name)
        with patch("research.human_repertoire.trajectory.uniform", return_value=.1):
            t = sample_trace(GraphBackend(graph), "x", {}, replace(cfg, horizon=1), "x", 0)
        self.assertEqual(t["reason"], "horizon")  # Hazard cannot supersede horizon.
        self.assertEqual(score_trace(t, {"r": 0, "a": 350}, cfg)["advantage"], 1)
        with patch("research.human_repertoire.trajectory.uniform", return_value=.9):
            t = sample_trace(GraphBackend(graph), "x", {}, cfg, "x", 0)
        self.assertEqual((t["reason"], t["keys"], t["wdl"]), ("repetition", ["r", "a", "r", "a", "r"], [0, 1, 0]))
        terminal = Graph("r", {"r": Node("terminal", cp=350, wdl=(1, 0, 0))})
        self.assertEqual(sample_trace(GraphBackend(terminal), "x", {}, Config(horizon=0), "x", 0)["reason"], "terminal")

    def test_unknown_before_hit_and_final_exact_mate(self):
        trace = {"index": 0, "keys": ["a", "b", "c"], "moves": ["x", "y"], "reason": "horizon", "wdl": [1, 0, 0], "exact_cp": None}
        cfg = Config(horizon=2)
        early_gap = score_trace(trace, {"a": 0, "c": 350}, cfg)
        late_gap = score_trace(trace, {"a": 350, "b": 0}, cfg)
        self.assertEqual(observation_endpoints(early_gap), (0, 1))
        self.assertEqual(observation_endpoints(late_gap), (1, 1))
        self.assertEqual(late_gap["unknown_eval_mass"], 1)
        mate = score_trace({**trace, "reason": "terminal", "exact_cp": -100000}, {"a": 0, "b": 0, "c": 350}, cfg)
        self.assertEqual((mate["disadvantage"], mate["certain_advantage_mass"], mate["unresolved_first_hit_mass"]), (1, 0, 0))

    def test_paired_bounds_cancel_identical_unknown_paths_and_keep_changed_paths(self):
        cfg = Config(horizon=1)
        trace = {"index": 0, "keys": ["r", "a"], "moves": ["x"], "reason": "horizon", "wdl": [0, 1, 0], "exact_cp": None}
        ta = [trace, {**trace, "index": 1}]
        ra = [score_trace(t, {"r": 0}, cfg) for t in ta]
        self.assertEqual(paired_summary(ra, ra, ta, ta)["first_advantage"]["sample_difference_interval"], [0, 0])
        tb = [trace, {**trace, "index": 1, "keys": ["r", "b"]}]
        rb = [score_trace(t, {"r": 0, "b": 350}, cfg) for t in tb]
        p = paired_summary(ra, rb, ta, tb)
        self.assertEqual(p["first_advantage"]["sample_difference_interval"], [-.5, 0])
        self.assertEqual(p["identical_position_sequences"], 1)
        self.assertAlmostEqual(p["early_advantage"]["mean"], -.5*cfg.discount)
        with self.assertRaisesRegex(ValueError, "indices"):
            paired_summary(ra, rb, ta, list(reversed(tb)))

    def test_mc_mean_error_and_observation_envelope_are_separate(self):
        cfg = Config(horizon=0)
        rows = [score_trace({"keys": ["r"], "reason": "horizon", "wdl": [x, 1-x, 0], "exact_cp": None}, {"r": 350 if x else 0}, cfg) for x in (0, 1, 0, 1)]
        summary = arm_summary(rows)["metrics"]
        self.assertEqual(summary["advantage"]["mean"], .5)
        self.assertAlmostEqual(summary["advantage"]["mc_se"], sqrt(1/12))
        self.assertEqual(summary["first_advantage"]["sample_observation_interval"], [.5, .5])
        self.assertEqual(summary["first_advantage"]["mc_outer_95"], [0, 1])

    def test_legal_backend_registers_visited_children_only_and_replays(self):
        c = json.loads(CONFIG.read_text())
        backend = ChessBackend(chess.STARTING_FEN, fit([]), {"x": spec("uniform")}, stats([]), c, 30)
        root = backend.view(backend.initial, "x")
        self.assertEqual(len(root.node.edges), 20)
        self.assertEqual(len(backend.fens), 1)
        self.assertAlmostEqual(sum(e.probability for e in root.node.edges), 1)
        trace = sample_trace(backend, "x", {}, Config(horizon=12), "legal", 0)
        board = chess.Board()
        for i, key in enumerate(trace["keys"]):
            self.assertEqual(key, str(zobrist_int64(board)))
            self.assertEqual(backend.fens[key].split()[:4], board.fen().split()[:4])
            if i < len(trace["moves"]):
                board.push_san(trace["moves"][i])
        self.assertEqual(trace["reason"], "horizon")
        self.assertLessEqual(len(backend.fens), 13)
        backend.max_positions = len(backend.fens)
        board = chess.Board()
        board.push_san("a4")
        if str(zobrist_int64(board)) in backend.fens:
            board.push_san("a5")
        with self.assertRaisesRegex(ValueError, "cap exceeded"):
            backend.view(board, "x")

    def test_human_ending_hazard_excludes_censored_arrivals(self):
        c = json.loads(CONFIG.read_text())
        key = str(zobrist_int64(chess.Board()))
        train = stats([])
        train["outcomes"][key] = [500, 100, 400]  # Arrivals are not hazard exposures.
        train["endings"][key] = [1, 0, 1]
        train["moves"][key] = {"e4": 3}
        backend = ChessBackend(chess.STARTING_FEN, fit([]), {"x": spec("uniform")}, train, c, 5)
        view = backend.view(backend.initial, "x")
        self.assertEqual(view.node.end_probability, .4)
        train["moves"][key] = {}
        view = backend.view(backend.initial, "x")
        self.assertEqual(view.node.side, "human_terminal")
        self.assertEqual(view.node.wdl, (.5, 0, .5))

    def test_pilot_target_priority_is_bounded_and_stops_at_known_hit(self):
        traces = {"arm": [{"keys": ["r", "a", "b"], "exact_cp": None}, {"keys": ["r", "c", "b"], "exact_cp": None}]}
        fens = {k: chess.STARTING_FEN for k in "rabc"}
        source = {"r": 0, "a": None, "b": None, "c": 350}
        selected = select_missing_targets(traces, source, {}, fens, Config(), 1)
        self.assertEqual(selected["targets"], ["a"])
        self.assertEqual(selected["ranking"][0]["total_first_gap_count"], 1)
        self.assertEqual(select_missing_targets(traces, source, {"a": 0}, fens, Config(), 1)["targets"], ["b"])

    def test_final_requires_frozen_engine_before_generation(self):
        with patch("research.human_repertoire.rollout_experiment.verified_pilot", return_value={}), patch("pathlib.Path.read_text", return_value=json.dumps({"complete": False})), patch("research.human_repertoire.rollout_experiment.generate") as generate:
            with self.assertRaisesRegex(ValueError, "completed frozen"):
                final({})
            generate.assert_not_called()

    def test_random_channels_and_categorical_boundaries(self):
        values = [uniform("s", 0, 0, channel) for channel in ("end", "recall", "move")]
        self.assertEqual(len(set(values)), 3)
        self.assertTrue(all(0 <= v < 1 for v in values))
        self.assertNotEqual(uniform("pilot", 0, 0, "move"), uniform("final", 0, 0, "move"))
        edges = [Edge("zero", "a", 0), Edge("first", "b", .25), Edge("second", "c", .75)]
        self.assertEqual(categorical(edges, 0).move, "first")
        self.assertEqual(categorical(edges, .25).move, "second")
        with self.assertRaises(ValueError):
            categorical(edges, 1)


if __name__ == "__main__":
    unittest.main()
