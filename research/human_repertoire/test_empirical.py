from dataclasses import asdict, replace
import json
import unittest

from .cohort import CONFIG, replay
from .empirical import (audit_metrics, build_graph, candidates, combine, first_hit_bounds,
                        move_diagnostics, outcome_diagnostics, player_component_bootstrap, stats)
from .fixtures import demanding
from .model import Config, Edge, Graph, Node, evaluate
from .oracle import path_evaluate
from .test_cohort import row


class EmpiricalTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads(CONFIG.read_text())

    def game(self, **kwargs):
        return {**replay(row(**kwargs), self.c), "event": "Rapid"}

    def test_human_endings_conserve_arrival_mass_without_a_ply(self):
        g = Graph("root", {"root": Node("us", (Edge("play", "adv", 1, 0),), 0,
                                            end_probability=.25, end_wdl=(0, 0, 1)),
                           "adv": Node("terminal", cp=400, wdl=(1, 0, 0))})
        c = Config(horizon=1, discount=.9)
        m = evaluate(g, {"root": "play"}, c)
        self.assertAlmostEqual(m.win, .75)
        self.assertAlmostEqual(m.human_end_mass, .25)
        self.assertAlmostEqual(m.early_advantage, .75*.9)
        audit_metrics(m)
        oracle = path_evaluate(g, {"root": "play"}, c)
        for k in asdict(m):
            self.assertAlmostEqual(getattr(m, k), getattr(oracle, k))

    def test_endings_with_certainty_do_not_expand_zero_mass(self):
        g = Graph("root", {"root": Node("us", (Edge("never", "root", 1, 0),), 0,
                                            end_probability=1, end_wdl=(1, 0, 0))})
        m = evaluate(g, {}, Config(max_states=1))
        self.assertEqual(m.human_end_mass, 1)
        self.assertEqual(m.points, 1)
        with self.assertRaisesRegex(RuntimeError, "state cap"):
            evaluate(demanding(), {}, Config(max_states=1))

    def test_censor_bounds_distinguish_before_and_at_horizon(self):
        g = Graph("root", {"root": Node("opp", (Edge("a", "adv", .3), Edge("b", "cut", .7)), 0),
                           "adv": Node("terminal", cp=400, wdl=(1, 0, 0)),
                           "cut": Node("frontier", cp=0)})
        m = evaluate(g, {}, Config(horizon=3))
        bounds = first_hit_bounds(m)
        self.assertAlmostEqual(bounds["advantage_lower"], .3)
        self.assertAlmostEqual(bounds["advantage_upper"], 1)
        m = evaluate(g, {}, Config(horizon=1))
        self.assertAlmostEqual(first_hit_bounds(m)["advantage_upper"], .3)
        self.assertEqual(m.unresolved_no_hit_mass, 0)

    def test_correlated_recall_same_mean_different_joint_execution(self):
        g = demanding()
        b = {"root": "demanding", "d1": "remember1", "d2": "remember2"}
        c = Config(recall=.85)
        independent = evaluate(g, b, c)
        correlated = combine([(.85, evaluate(g, b, replace(c, recall=1))), (.15, evaluate(g, b, replace(c, recall=0)))])
        self.assertGreater(correlated.advantage, independent.advantage)
        audit_metrics(correlated)
        self.assertEqual(evaluate(g, b, replace(c, recall=0)), evaluate(g, {}, replace(c, recall=0)))

    def test_unknown_after_a_certain_hit_does_not_erase_it(self):
        g = Graph("a", {"a": Node("opp", (Edge("go", "b", 1),), 400),
                        "b": Node("frontier", cp=None)})
        m = evaluate(g, {}, Config(horizon=3))
        self.assertEqual(m.unknown_eval_mass, 1)
        self.assertEqual(first_hit_bounds(m)["advantage_lower"], 1)
        self.assertEqual(m.unresolved_first_hit_mass, 0)
        # An unknown observation before the first known hit is unresolved.
        g = Graph("a", {"a": Node("opp", (Edge("go", "b", 1),), None),
                        "b": Node("frontier", cp=400)})
        m = evaluate(g, {}, Config(horizon=3))
        self.assertEqual(first_hit_bounds(m)["advantage_lower"], 0)
        self.assertEqual(first_hit_bounds(m)["advantage_upper"], 1)
        oracle = path_evaluate(g, {}, Config(horizon=3))
        self.assertEqual(m.unresolved_first_hit_mass, oracle.unresolved_first_hit_mass)

    def test_empirical_kernel_keeps_early_end_and_unseen_move_mass(self):
        a = self.game()
        b = {**self.game(game_id="b", moves="1.e4 e5 2.Nf3 Nc6 0-1"), "wdl": (0, 0, 1)}
        s = stats([a, b])
        root = a["positions"][0]["key"]
        evals = {k: 0 for k in s["fens"]}
        g, _ = build_graph(s, s, evals, {root: ["Bb5"]}, root, self.c)
        n = g.nodes[root]
        self.assertEqual(n.end_probability, .5)
        self.assertAlmostEqual(sum(e.probability for e in n.edges), 1)
        self.assertGreater(next(e.probability for e in n.edges if e.move.startswith("<")), 0)
        m = evaluate(g, {}, Config())
        audit_metrics(m)
        oracle = path_evaluate(g, {}, Config())
        for k in asdict(m):
            self.assertAlmostEqual(getattr(m, k), getattr(oracle, k))

    def test_validation_unseen_moves_get_positive_probability(self):
        train = self.game()
        validation = self.game(game_id="b", moves="1.e4 e5 2.Nf3 Nc6 3.Bc4 Bc5")
        diagnostics, _ = move_diagnostics(stats([train]), [validation], self.c)
        self.assertEqual(diagnostics["pooled"]["unseen_move"], 1)
        self.assertGreater(diagnostics["pooled"]["log_loss"], 0)
        scores, _ = outcome_diagnostics([train], [validation], self.c)
        self.assertEqual(scores["pooled"]["games"], 1)

    def test_player_components_not_independent_games(self):
        rows = [{"white": "a", "black": "b", "error": 0},
                {"white": "b", "black": "c", "error": 1},
                {"white": "d", "black": "e", "error": 0}]
        result = player_component_bootstrap(rows, "error", draws=100)
        self.assertEqual(result["components"], 2)
        self.assertEqual(result["largest_component_games"], 2)

    def test_table_selection_matches_independent_depth_first_search(self):
        from .model import book_cost, objective, optimise
        from .oracle import enumerate_books
        from .validate_cohort import choose
        g = demanding()
        for recall in (.65, 1):
            c = Config(recall=recall, min_points=.48, max_disadvantage=.4)
            books = list(enumerate_books(g, c, 3))
            values = {tuple(sorted(b.items())): evaluate(g, b, c) for b in books}
            for budget in range(4):
                selected = choose(books, values, g, c, budget)
                dfs = optimise(g, c, budget)
                self.assertEqual(objective(selected[1], book_cost(g, selected[0])), objective(dfs.metrics, dfs.cost))
                self.assertEqual(selected[1], evaluate(g, selected[0], c))


if __name__ == "__main__":
    unittest.main()
