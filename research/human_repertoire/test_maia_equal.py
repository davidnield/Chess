import unittest
from .maia_equal import packages, selection


class EqualBudgetTests(unittest.TestCase):
    def test_packages_have_same_cost_and_legal_root_dependency(self):
        root, books, gates, fens = packages({'a': ['exd4', 'Nxd4'], 'b': ['Nf6', 'Nc3'], 'c': ['d6', 'Nc3']})
        self.assertEqual([len(b) for b in books.values()], [2, 2, 2])
        self.assertEqual(len(set.intersection(*(set(b) for b in books.values()))), 1)
        self.assertEqual(len(gates), 4)
        import chess
        from python.zobrist import zobrist_int64
        for gate in gates:
            board = chess.Board(fens[gate['position']]); board.push_san(gate['move'])
            self.assertEqual(str(zobrist_int64(board)), gate['child'])

    def test_union_keeps_each_alternative_and_rejects_missing_pairs(self):
        root, books, gates, fens = packages({'a': ['exd4', 'Nxd4'], 'b': ['Nf6', 'Nc3'], 'c': ['d6', 'Nc3']})
        c = {'policies': ['p'], 'cards': books, 'reference': 'a', 'pairs': 2, 'deeper_pairs': 1, 'max_positions': 20, 'max_deeper_positions': 20}
        paths = {'p/'+b: [{'index': i, 'keys': list(book)} for i in range(2)] for b,book in books.items()}
        result = selection(paths, fens, [], {'base': {}, 'deeper': {}}, c, {'horizon': 12})
        self.assertEqual(len(result['base']['required']), 4)
        paths['p/c'].pop()
        with self.assertRaises(ValueError): selection(paths, fens, [], {'base': {}, 'deeper': {}}, c, {'horizon': 12})
