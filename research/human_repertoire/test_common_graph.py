from dataclasses import asdict, replace
import json
import unittest
from unittest.mock import patch

import chess
import chess.engine

from .cohort import replay
from .common_graph import (Kernels, add_edge, apply_evals, base_template, expand_template,
    forward_visits, materialize, missing_edge_ranking, select_engine_targets)
from .continuation_audit import CONFIG, analyse_position, engine_audit
from .empirical import audit_metrics, build_graph, stats
from .model import Config, Edge, Graph, Node, evaluate
from .oracle import path_evaluate
from .shared_fallback import fit, spec
from .test_cohort import row


class CommonGraphTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads(CONFIG.read_text())
        replay_config = {"root_san": ["e4", "e5", "Nf3", "Nc6"], "horizon": 12}
        self.games = [replay(row("a", "1.e4 e5 2.Nf3 Nc6 3.Bc4 Nf6 4.d3 Bc5 5.O-O d6 6.a3 a6 7.h3 h6 8.Be3 Be6 9.Nbd2"), replay_config),
                      replay(row("b", "1.e4 e5 2.Nf3 Nc6 3.Bb5 a6 4.Ba4 Nf6 5.O-O Be7 6.d3 d6 7.c3 O-O 8.Nbd2 Re8 9.h3"), replay_config)]
        self.train = stats(self.games)
        self.root = self.games[0]["positions"][0]["key"]
        self.books = {"empty": {}, "one": {self.root: "Bc4"}}
        self.kernels = Kernels(fit(self.games), {"uniform": spec("uniform"), "shared": spec("token", 1, .5)})
        self.base = base_template(self.train, self.root, self.books)

    def test_baseline_parity_with_old_builder(self):
        evals = {k: 0 for k in self.base["fens"]}
        graph = materialize(self.base, self.kernels, self.train, evals, "uniform", self.c)
        old, _ = build_graph(self.train, self.train, evals, {self.root: ["Bc4"]}, self.root, {**self.c, "move_prior_strength": 1})
        self.assertEqual(graph.nodes.keys(), old.nodes.keys())
        for key in graph.nodes:
            a, b = graph.nodes[key], old.nodes[key]
            self.assertEqual(replace(a, edges=()), replace(b, edges=()))
            for x, y in zip(a.edges, b.edges):
                self.assertEqual(replace(x, probability=0), replace(y, probability=0))
                self.assertAlmostEqual(x.probability, y.probability)

    def test_shared_support_omission_mass_and_eval_only_change(self):
        graphs = [materialize(self.base, self.kernels, self.train, {}, name, self.c) for name in self.kernels.specs]
        a, b = graphs
        self.assertEqual(a.nodes.keys(), b.nodes.keys())
        for key in a.nodes:
            self.assertEqual([(e.move, e.child) for e in a.nodes[key].edges], [(e.move, e.child) for e in b.nodes[key].edges])
        root = b.nodes[self.root]
        _, probabilities = self.kernels.get(self.root, self.base["fens"][self.root])
        omitted = set(probabilities["shared"])-set(self.base["explicit"][self.root])
        self.assertAlmostEqual(root.edges[-1].probability, sum(probabilities["shared"][m] for m in omitted))
        changed = apply_evals(b, self.base, {k: 75 for k in self.base["fens"]})
        for key in b.nodes:
            self.assertEqual([(e.move, e.child, e.probability) for e in b.nodes[key].edges], [(e.move, e.child, e.probability) for e in changed.nodes[key].edges])

    def test_exact_transposition_reuses_one_position(self):
        known = add_edge(self.base, self.games[0]["positions"][2]["key"], "a3")
        a = add_edge(self.base, self.root, "a3")
        b = add_edge(self.base, a, "Nf6")
        before = len(self.base["fens"])
        child = add_edge(self.base, b, "Bc4")
        self.assertEqual(child, known)
        self.assertEqual(len(self.base["fens"]), before)

    def test_unseen_expansion_keeps_probability_and_matches_path_oracle(self):
        expanded = expand_template(self.base, [{"position": self.root, "move": "a3", "priority": 1}], self.kernels, self.train, self.c)
        self.assertGreater(len(expanded["new_expanded"]), 0)
        new_key = expanded["explicit"][self.root]["a3"]
        self.assertNotIn(new_key, self.train["moves"])
        evals = {k: 0 for k in expanded["fens"]}
        evals[new_key] = 350
        graph = materialize(expanded, self.kernels, self.train, evals, "shared", self.c)
        self.assertEqual(graph.nodes[new_key].end_probability, 0)
        self.assertAlmostEqual(sum(e.probability for e in graph.nodes[new_key].edges), 1)
        cfg = Config(horizon=3, recall=.85)
        a, b = evaluate(graph, self.books["one"], cfg), path_evaluate(graph, self.books["one"], cfg)
        audit_metrics(a)
        for name in asdict(a):
            self.assertAlmostEqual(getattr(a, name), getattr(b, name))
        self.assertGreater(a.certain_advantage_mass, 0)
        self.assertGreater(a.unresolved_first_hit_mass, 0)
        with self.assertRaisesRegex(ValueError, "expansion cap"):
            expand_template(self.base, [{"position": self.root, "move": "a3"}], self.kernels, self.train, {**self.c, "max_new_expanded_positions": 0})

    def test_reach_accounts_for_recall_and_human_ending(self):
        graph = Graph("r", {"r": Node("us", (Edge("a", "a", .4), Edge("b", "b", .6)), end_probability=.2),
                            "a": Node("frontier"), "b": Node("frontier")})
        arrivals, departures = forward_visits(graph, {"r": "a"}, 12, .5)
        self.assertAlmostEqual(arrivals["a"], .56)
        self.assertAlmostEqual(arrivals["b"], .24)
        self.assertAlmostEqual(departures["r"], .8)

    def test_priority_ignores_eval_changes(self):
        maps = [{k: 0 for k in self.base["fens"]}, {k: 500 for k in self.base["fens"]}]
        ranks = []
        for evals in maps:
            graphs = {n: materialize(self.base, self.kernels, self.train, evals, n, self.c) for n in self.kernels.specs}
            ranks.append(missing_edge_ranking(self.base, self.kernels, graphs, self.books, self.c))
        self.assertEqual(ranks[0], ranks[1])

    def test_engine_targets_cover_gates_and_missing_quota(self):
        keys = list(self.base["fens"])[:6]
        fens = {k: self.base["fens"][k] for k in keys}
        priorities = {k: 6-i for i, k in enumerate(keys)}
        source = {k: 0 if i % 2 else None for i, k in enumerate(keys)}
        targets = select_engine_targets(fens, priorities, source, {keys[-1]}, 4, 2, 2)
        self.assertIn(keys[-1], targets["targets"])
        self.assertIn(keys[-1], targets["deeper"])
        self.assertEqual(targets["missing_targets"], 2)
        self.assertEqual(len(set(targets["targets"])), 4)
        source = {k: 999 if v is not None else None for k, v in source.items()}
        self.assertEqual(targets, select_engine_targets(fens, priorities, source, {keys[-1]}, 4, 2, 2))

    def test_engine_score_perspective_limits_and_bound_rejection(self):
        class FakeEngine:
            def __init__(self):
                self.calls = []
                self.bound = False
            def configure(self, values):
                self.calls.append(values)
            def analyse(self, board, limit, game):
                self.calls.append((limit.nodes, limit.time, game))
                return {"score": chess.engine.PovScore(chess.engine.Cp(-45), chess.BLACK),
                        "nodes": 123, "depth": 8, "lowerbound": self.bound}
        engine = FakeEngine()
        result = analyse_position(engine, chess.STARTING_FEN, 1000, .1)
        self.assertEqual(result["cp"], 45)
        self.assertEqual(engine.calls[0], {"Clear Hash": None})
        self.assertEqual(engine.calls[1][:2], (1000, .1))
        engine.bound = True
        with self.assertRaisesRegex(ValueError, "root score"):
            analyse_position(engine, chess.STARTING_FEN, 1000, .1)

    def test_engine_drift_stops_before_launch(self):
        with patch("research.human_repertoire.continuation_audit.verified_preparation", return_value={"engine_identity": {"binary": "old"}}), patch("research.human_repertoire.continuation_audit.fingerprint", return_value="changed"), patch("chess.engine.SimpleEngine.popen_uci") as launch:
            with self.assertRaisesRegex(ValueError, "executable drift"):
                engine_audit(self.c)
            launch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
