import unittest
import chess
from python.zobrist import zobrist_int64
from .maia_comparison import select_targets


class MaiaComparisonTests(unittest.TestCase):
    def fixture(self):
        a = chess.Board(); b = a.copy(); b.push_san('e4')
        keys = [str(zobrist_int64(x)) for x in (a, b)]
        fens = dict(zip(keys, [a.fen(), b.fen()]))
        paths = {context+'/'+book: [{'index': i, 'keys': [keys[i]]} for i in range(2)]
            for context in ('shared/r0.5', 'maia/r1.0') for book in ('source_one_card', 'two_cards')}
        c = {'plans': ['source_one_card', 'two_cards'], 'pairs': 2, 'deeper_pairs': 1, 'max_positions': 2, 'max_deeper_positions': 1}
        return paths, fens, {'base': {}, 'deeper': {}}, c

    def test_union_covers_every_context_and_nested_index(self):
        paths, fens, records, c = self.fixture()
        selected = select_targets(paths, fens, [], records, c, {'horizon': 12})
        self.assertEqual(len(selected['base']['targets']), 2)
        self.assertEqual(len(selected['deeper']['targets']), 1)

    def test_no_dropping_over_cap_or_unpaired_index(self):
        paths, fens, records, c = self.fixture()
        with self.assertRaises(ValueError): select_targets(paths, fens, [], records, {**c, 'max_positions': 1}, {'horizon': 12})
        paths['shared/r0.5/two_cards'][0]['index'] = 7
        with self.assertRaises(ValueError): select_targets(paths, fens, [], records, c, {'horizon': 12})
