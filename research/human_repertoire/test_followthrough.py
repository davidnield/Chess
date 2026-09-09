import unittest
from .followthrough import packages, gate_checks
from .hashed_board import HashedBoard


class FollowthroughTests(unittest.TestCase):
    def test_pgn_roundtrip_preserves_every_prescription(self):
        from .followthrough_pgn import export, verify
        _,books,_,_=packages()
        text=export(); verify(text,books)
        self.assertIn('6. c3',text)

    def test_equal_budget_and_legal_reconstruction(self):
        root, books, forced, fens = packages()
        self.assertEqual(set(books), {'d4_only','exd4_Nxd4','Nf6_Nc3','d6_Nc3',
                                      'breadth4','sequence3','sequence4'})
        self.assertEqual(len(books['sequence4']),len(books['breadth4']))
        self.assertEqual(len(books['sequence4']),4)
        self.assertEqual(len(books['sequence3']),3)
        self.assertTrue(set(books['sequence3'].items()) < set(books['sequence4'].items()))
        for book in books.values():
            for key,move in book.items():
                board=HashedBoard(fens[key]); self.assertTrue(board.turn)
                self.assertIn(board.parse_san(move),board.legal_moves)
        self.assertTrue(forced[7])
        self.assertTrue(all(len(keys)<=64 for keys in forced.values()))

    def test_deeper_gate_failure_keeps_package_ineligible(self):
        p={'books':{'b':{'p':'move'}},'gates':[{'parent':'p','child':'c','move':'move'}]}
        audit={'base':{'p':{'cp':0},'c':{'cp':0}},'deeper':{'p':{'cp':70},'c':{'cp':-90}}}
        self.assertFalse(gate_checks(p,audit)['b']['passed'])
        audit['deeper']['c']['cp']=-80
        self.assertTrue(gate_checks(p,audit)['b']['passed'])
