from dataclasses import replace
import unittest

from .context_recall import ContextCurve, solve
from .exposure_recall import solve as frequency_solve
from .model import Config, Edge, Graph, Node, book_cost
from .oracle import path_evaluate


def branch_fixture():
    return Graph('tabiya', {
        'tabiya': Node('us', (Edge('prepared', 'reply', .2, 0),
                              Edge('deviate', 'end', .8, 0)), cp=0),
        'reply': Node('opp', (Edge('rare', 'card', .02), Edge('usual', 'end', .98)), cp=0),
        'card': Node('us', (Edge('remember', 'win', .1, 400),
                            Edge('other', 'end', .9, 0)), cp=0),
        'win': Node('terminal', cp=400, wdl=(1, 0, 0)),
        'end': Node('terminal', cp=0, wdl=(0, 1, 0)),
    })


class ContextRecallTests(unittest.TestCase):
    def test_user_calibration_and_upstream_effect(self):
        graph = branch_fixture(); book = {'tabiya': 'prepared', 'card': 'remember'}
        cfg = Config(horizon=3)
        old = frequency_solve(graph, book, cfg, .01)
        new = solve(graph, book, cfg, .01, anchors={'tabiya': .99}, root_ply=4)
        self.assertGreater(new['recall']['card'], .95)
        self.assertLess(old['recall']['card'], .04)
        self.assertGreater(new['frequencies']['card'], old['frequencies']['card'])
        expected = .01*(.2+.8*new['recall']['tabiya'])*.02
        self.assertAlmostEqual(new['frequencies']['card'], expected)
        oracle = path_evaluate(graph, book, replace(cfg, recall_by_position=new['recall']))
        self.assertAlmostEqual(oracle.early_advantage, new['conditional_metrics']['early_advantage'])
        self.assertEqual(book_cost(graph, book), 2)

    def test_no_anchor_or_zero_boost_preserves_frequency_model(self):
        graph = branch_fixture(); book = {'tabiya': 'prepared', 'card': 'remember'}
        cfg = Config(horizon=3)
        for entry in (0, .001, .01, 1):
            old = frequency_solve(graph, book, cfg, entry)
            for args in ({}, {'anchors': {'tabiya': .99}, 'curve': ContextCurve(ceiling=0)}):
                new = solve(graph, book, cfg, entry, **args)
                for key in book:
                    self.assertAlmostEqual(new['recall'][key], old['recall'][key])
                    self.assertAlmostEqual(new['frequencies'][key], old['frequencies'][key])

    def test_depth_distance_and_unfamiliar_early_branch(self):
        curve = ContextCurve(); f = .0002
        shallow = curve.recall(f, curve.support(.99, 2, 6))
        distant = curve.recall(f, curve.support(.99, 10, 14))
        deep_tabiya = curve.recall(f, curve.support(.99, 2, 22))
        unfamiliar = curve.recall(f, 0)
        self.assertGreater(shallow, .95)
        self.assertGreater(shallow, distant)
        self.assertGreater(shallow, deep_tabiya)
        self.assertLess(unfamiliar, .04)
        for distance in range(2, 30):
            self.assertGreaterEqual(curve.support(.99, distance, 4+distance),
                                    curve.support(.99, distance+1, 5+distance))

    def test_transposition_weights_routes_and_absolute_depth(self):
        graph = Graph('r', {
            'r': Node('opp', (Edge('familiar', 'a', .25), Edge('unfamiliar', 'b', .75)), cp=0),
            'a': Node('opp', (Edge('join', 's', 1),), cp=0),
            'b': Node('opp', (Edge('go', 'c', 1),), cp=0),
            'c': Node('opp', (Edge('join', 's', 1),), cp=0),
            's': Node('us', (Edge('win', 'w', 0, 400), Edge('miss', 't', 1, 0)), cp=0),
            'w': Node('terminal', cp=400, wdl=(1, 0, 0)),
            't': Node('terminal', cp=0, wdl=(0, 1, 0))})
        cfg = Config(horizon=4)
        result = solve(graph, {'s': 'win'}, cfg, .001, anchors={'a': .99}, root_ply=8)
        support = .25*.99*2**(-2/8)
        self.assertAlmostEqual(result['frequencies']['s'], .001)
        self.assertAlmostEqual(result['components']['s']['mean_support'], support)
        self.assertAlmostEqual(result['recall']['s'], .5+.5*.98*support)
        # A shortened horizon includes only the short route and uses its own denominator.
        short = solve(graph, {'s': 'win'}, replace(cfg, horizon=2), .001,
                      anchors={'a': .99}, root_ply=8)
        self.assertAlmostEqual(short['frequencies']['s'], .00025)
        self.assertAlmostEqual(short['components']['s']['mean_support'], .99*2**(-2/8))

    def test_ending_hazard_zero_entry_and_familiar_anchor_reset(self):
        graph = branch_fixture()
        graph = Graph(graph.root, {**graph.nodes, 'reply': replace(graph.nodes['reply'], end_probability=.5)})
        book = {'card': 'remember'}
        cfg = Config(horizon=3)
        result = solve(graph, book, cfg, .01, anchors={'tabiya': .99, 'card': .6})
        self.assertAlmostEqual(result['frequencies']['card'], .01*.2*.5*.02)
        self.assertAlmostEqual(result['components']['card']['mean_support'], .6)
        zero = solve(graph, book, cfg, 0, anchors={'tabiya': .99})
        self.assertEqual(zero['recall']['card'], 0)

    def test_invalid_profiles_cycles_and_state_cap(self):
        graph = branch_fixture(); cfg = Config(horizon=3)
        for args in ({'anchors': {'absent': .9}}, {'anchors': {'tabiya': 2}},
                     {'root_ply': -1}, {'anchors': {'end': .9}}):
            with self.assertRaises(ValueError): solve(graph, {}, cfg, **args)
        for args in ({'ceiling': 2}, {'local_half_life_plies': 0}, {'shallow_through_ply': -1}):
            with self.assertRaises(ValueError): ContextCurve(**args)
        cycle = Graph('r', {'r': Node('opp', (Edge('loop', 'r', 1),), cp=0)})
        with self.assertRaisesRegex(ValueError, 'acyclic'): solve(cycle, {}, cfg)
        with self.assertRaisesRegex(ValueError, 'state cap'):
            solve(graph, {}, replace(cfg, max_states=1))
