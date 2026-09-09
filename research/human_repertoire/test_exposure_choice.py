"""Synthetic model prediction: encounter frequency changes one-card allocation."""
from dataclasses import replace
import unittest

from .exposure_recall import solve
from .model import Config, Edge, Graph, Node
from .oracle import enumerate_books, path_evaluate


class ExposureChoiceTests(unittest.TestCase):
    def test_frequency_changes_best_one_card_book(self):
        graph = Graph('root', {
            'root': Node('opp', (Edge('common', 'common', .01),
                                 Edge('rare', 'rare', .001),
                                 Edge('other', 'end', .989)), cp=0),
            'common': Node('us', (Edge('prepare', 'reply', 0, 0),
                                  Edge('miss', 'end', 1, 0)), cp=0),
            'rare': Node('us', (Edge('prepare', 'win', 0, 400),
                                Edge('miss', 'end', 1, 0)), cp=0),
            'reply': Node('opp', (Edge('slip', 'win', .06),
                                  Edge('safe', 'end', .94)), cp=0),
            'win': Node('terminal', cp=400, wdl=(1, 0, 0)),
            'end': Node('terminal', cp=0, wdl=(0, 1, 0)),
        })
        config = Config(horizon=3, recall=.85, min_points=0)
        books = list(enumerate_books(graph, config, 1,
                                     {'common': ['prepare'], 'rare': ['prepare']}))
        self.assertEqual(len(books), 3)
        flat, frequency = [], []
        for book in books:
            flat.append(path_evaluate(graph, book, config).early_advantage)
            result = solve(graph, book, config)
            independent = path_evaluate(graph, book,
                                        replace(config, recall_by_position=result['recall']))
            self.assertAlmostEqual(result['conditional_metrics']['early_advantage'],
                                   independent.early_advantage)
            frequency.append(independent.early_advantage)
        self.assertEqual(books[max(range(3), key=flat.__getitem__)], {'rare': 'prepare'})
        self.assertEqual(books[max(range(3), key=frequency.__getitem__)], {'common': 'prepare'})
