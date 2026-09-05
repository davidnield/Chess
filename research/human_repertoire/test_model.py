import json
import random
import unittest
from dataclasses import asdict, replace

from .fixtures import demanding, fallback_skill, shared_reentry
from .model import Config, Edge, Graph, Node, book_cost, evaluate, graph_dict, graph_from_dict, objective, optimise
from .oracle import enumerate_books, oracle_best, path_evaluate


class ModelTests(unittest.TestCase):
    def assertMetrics(self, a, b):
        for k in asdict(a):
            self.assertAlmostEqual(getattr(a, k), getattr(b, k), places=11, msg=k)

    def assertMass(self, m):
        self.assertAlmostEqual(m.win + m.draw + m.loss, 1)
        self.assertAlmostEqual(m.advantage + m.disadvantage + m.no_hit, 1)
        self.assertAlmostEqual(m.terminal_mass + m.frontier_mass + m.horizon_mass + m.repetition_mass, 1)

    def assertObjective(self, actual, oracle):
        # Different summation orders may straddle a decimal rounding boundary.
        self.assertAlmostEqual(actual[0], oracle[0], places=11)
        self.assertAlmostEqual(actual[1], oracle[1], places=11)
        self.assertEqual(actual[2], oracle[2])

    def test_budget_and_recall_switch(self):
        g = demanding()
        c = Config(min_points=.48, max_disadvantage=.4)
        self.assertEqual(optimise(g, c, 1).book, {"root": "easy"})
        self.assertEqual(optimise(g, c, 3).book["root"], "demanding")
        low = replace(c, recall=.65)
        self.assertEqual(optimise(g, low, 3).book["root"], "easy")
        high_skill = replace(low, fallback=fallback_skill(g, .8))
        self.assertEqual(optimise(g, high_skill, 3).book["root"], "demanding")

    def test_analytic_mixture_and_conversion(self):
        g = demanding()
        book = {"root": "demanding", "d1": "remember1", "d2": "remember2"}
        c = Config(recall=.65)
        m = evaluate(g, book, c)
        q = .65 + .35 * .1
        expected = q ** 3 * .9 + .35 * .1 * .5
        self.assertAlmostEqual(m.advantage, expected)
        self.assertAlmostEqual(m.win_after_advantage / m.advantage, .8)
        self.assertMass(m)

    def test_shared_cost_and_reentry(self):
        g = shared_reentry()
        book = {"shared": "find"}
        c = Config(discount=1)
        self.assertEqual(book_cost(g, book), 1)
        self.assertAlmostEqual(evaluate(g, book, c).advantage, 1)
        self.assertAlmostEqual(evaluate(g, book, replace(c, reentry=False)).advantage, .4)
        s = optimise(g, c, 1)
        self.assertEqual(s.book, book)
        self.assertEqual(s.metrics, evaluate(g, s.book, c))

    def test_failed_recall_can_disable_later_preparation(self):
        g = demanding()
        b = {"root": "demanding", "d1": "remember1", "d2": "remember2"}
        a = evaluate(g, b, Config(recall=.5))
        z = evaluate(g, b, Config(recall=.5, reentry=False))
        self.assertGreater(a.advantage, z.advantage)
        self.assertMetrics(z, path_evaluate(g, b, Config(recall=.5, reentry=False)))

    def test_history_and_horizon(self):
        g = Graph("a", {"a": Node("us", (Edge("loop", "b", 1, 0),), 0),
                        "b": Node("opp", (Edge("return", "a", 1),), 0)})
        c = Config(horizon=8, repetition_visits=3)
        self.assertEqual(evaluate(g, {}, c).repetition_mass, 1)
        self.assertEqual(evaluate(g, {}, c).draw, 1)
        for c in [replace(c, horizon=3), replace(c, repetition_visits=None)]:
            m = evaluate(g, {}, c)
            self.assertEqual(m.horizon_mass, 1)
            self.assertMetrics(m, path_evaluate(g, {}, c))

    def test_first_hit_not_recounted_and_outcome_continues(self):
        g = Graph("a", {"a": Node("opp", (Edge("next", "b", 1),), 400),
                        "b": Node("terminal", cp=-400, wdl=(0, 0, 1))})
        m = evaluate(g, {}, Config())
        self.assertEqual((m.advantage, m.disadvantage, m.early_advantage, m.loss), (1, 0, 1, 1))
        self.assertEqual(m.win_after_advantage, 0)

    def test_missing_eval_is_not_certified(self):
        g = Graph("a", {"a": Node("us", (Edge("x", "b", 1),), 0),
                        "b": Node("frontier", cp=None)})
        self.assertEqual(optimise(g, Config(), 1).book, {})
        self.assertEqual(evaluate(g, {}, Config()).unknown_eval_mass, 1)
        with self.assertRaises(ValueError):
            evaluate(g, {"a": "x"}, Config())

    def test_constraints_and_no_silent_gate_relaxation(self):
        g = demanding()
        c = Config(min_points=1)
        self.assertIsNone(optimise(g, c, 3))
        self.assertEqual(optimise(g, Config(soundness_floor_cp=500), 3).book, {})
        with self.assertRaises(RuntimeError):
            optimise(g, Config(max_books=1), 1)

    def test_validation(self):
        g = demanding()
        for c in [Config(recall=-.1), Config(horizon=-1), Config(repetition_visits=1),
                  Config(fallback={"root": {"easy": 1}}), Config(discount=float("nan"))]:
            with self.assertRaises(ValueError):
                evaluate(g, {}, c)
        broken = Graph("a", {"a": Node("opp", (Edge("x", "a", .5),), 0)})
        with self.assertRaises(ValueError):
            evaluate(broken, {}, Config())

    def test_serialized_book_reconstructs_metrics(self):
        g = demanding()
        c = Config(recall=.9)
        s = optimise(g, c, 3)
        reconstructed = graph_from_dict(json.loads(json.dumps(graph_dict(g))))
        book = json.loads(json.dumps(s.book))
        self.assertEqual(evaluate(reconstructed, book, c), s.metrics)

    def test_exhaustive_fixture_oracle(self):
        for g in [demanding(), shared_reentry()]:
            for recall in [0, .65, 1]:
                for reentry in [True, False]:
                    c = Config(recall=recall, reentry=reentry)
                    for budget in range(4):
                        for b in enumerate_books(g, c, budget):
                            m = evaluate(g, b, c)
                            self.assertMass(m)
                            self.assertMetrics(m, path_evaluate(g, b, c))
                        s = optimise(g, c, budget)
                        oracle = oracle_best(g, c, budget)
                        self.assertObjective(objective(s.metrics, s.cost), oracle[0])
                        self.assertLessEqual(s.cost, budget)

    def test_seeded_shared_cyclic_graphs_against_oracle(self):
        for seed in range(20):
            rng = random.Random(seed)
            nodes = {"end": Node("terminal", cp=400 if seed % 2 else -400, wdl=(.6, .2, .2))}
            keys = ["a", "b", "c", "d", "end"]
            for i, k in enumerate(keys[:-1]):
                p = rng.uniform(.1, .9)
                nodes[k] = Node("us" if i % 2 == 0 else "opp",
                                tuple(Edge(str(j), rng.choice(keys), prob, 0) for j, prob in enumerate([p, 1-p])),
                                0, cost=1 + (i == 2))
            g = Graph("a", nodes)
            c = Config(horizon=5, recall=.7, reentry=bool(seed % 2))
            for budget in range(4):
                for b in enumerate_books(g, c, budget):
                    m = evaluate(g, b, c)
                    self.assertMass(m)
                    self.assertMetrics(m, path_evaluate(g, b, c))
                s = optimise(g, c, budget)
                self.assertObjective(objective(s.metrics, s.cost), oracle_best(g, c, budget)[0])


if __name__ == "__main__":
    unittest.main()
