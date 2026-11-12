"""Compare the shared-card optimizer with literal enumeration on small graphs."""
from itertools import combinations, product
from pathlib import Path
import random
import sys
import unittest
from types import SimpleNamespace

sys.path.insert(0,str(Path(__file__).parent/"experiments"))
from shared_slice_solver import Action, optimize
from shared_budget_slice import Search, Slice


def enumerate_books(weights,stops,actions,gains,budget):
    options = [[None]+[a for a in actions if a.root==h] for h in weights]
    best = 0.
    for assignment in product(*options):
        used = sum(a is not None for a in assignment)
        for n in range(min(len(gains),budget-used)+1):
            for bought in combinations(gains,n):
                score = 0.
                for (h,w),a in zip(weights.items(),assignment):
                    val = stops[h] if a is None else a.base+sum(a.probs.get(ch,0.)*gains[ch] for ch in bought)
                    score += w*val
                best = max(best,score)
    return best


class SharedBudgetTest(unittest.TestCase):
    def test_enumeration(self):
        rng = random.Random(923)
        weights,stops = {1:.55,2:.3,3:.15}, {1:.42,2:.46,3:.45}
        gains = {i:rng.random()*.2 for i in range(10,15)}
        actions = []
        for h in weights:
            for i in range(2):
                p = {ch:rng.random() for ch in [10,11,h+11]}
                total = sum(p.values())
                actions.append(Action(h,i,.4+rng.random()*.1,{ch:v/total for ch,v in p.items()}))
        for b in range(9):
            sol = optimize(weights,stops,actions,gains,b)
            exact = enumerate_books(weights,stops,actions,gains,b)
            self.assertAlmostEqual(sol["value"],exact,places=8)
            self.assertAlmostEqual(sol["upper"],exact,places=8)

    def test_transposition_changes_which_card_wins(self):
        actions = [Action(1,100,.4,{90:.4,91:.6}),Action(2,101,.4,{90:.4,92:.6})]
        sol = optimize({1:.5,2:.5},{1:.1,2:.1},actions,{90:.2,91:.2,92:.2},3)
        self.assertEqual(sol["leaves"],[90])
        self.assertAlmostEqual(sol["value"],.48)

    def test_cross_ply_board_forces_same_move_even_with_negative_gain(self):
        weights,stops = {1:.6,2:.4},{1:.1,2:.2}
        actions = [Action(1,10,.4,{2:1.}),Action(2,20,.5,{}),Action(2,21,.48,{})]
        linked = {2:{20:-.15,21:.1}}
        sol = optimize(weights,stops,actions,{2:.1},2,linked)
        self.assertEqual(sol["roots"],[0,2])
        self.assertEqual(sol["leaves"],[])
        self.assertAlmostEqual(sol["value"],.6*(.4+.1)+.4*.48)
        linked[2][21] = -.2
        sol = optimize(weights,stops,actions,{2:.1},2,linked)
        self.assertAlmostEqual(sol["value"],.6*(.4-.15)+.4*.5)

    def test_batched_envelope_with_unnamed_and_shared_replies(self):
        g = Slice.__new__(Slice)
        g.weights = {0:{1:.6,2:.4},30:{1:.6,2:.4}}
        g.root_stop = {1:.2,2:.23}
        g.options = {1:{11:None,12:None},2:{13:None}}
        g.cross_ply = {}
        g.stop = {5:.36,6:.52,7:.64,8:.4,9:.3}
        q = {5:.4,6:.3,7:.299999,8:.0000005,9:.0000005}
        g.nodes = {i:SimpleNamespace(q=q) for i in [11,12,13]}
        counts = {11:{5:40,6:10,7:3},12:{5:9500,6:500,8:1},13:{5:40,7:60,9:2}}
        all_gains = {5:.3,6:.01,7:.06,8:.2,9:.4}
        g.data = SimpleNamespace(coarse={i:({h:n for h,n in c.items() if n>=5},sum(c.values()))
                                             for i,c in counts.items()})

        class Toy(Search):
            def expand(self,raw,leaves):
                self.exact.update({i:counts[i] for i in raw})
                self.gains.update({h:all_gains[h] for h in leaves})
                self.answers.update(dict.fromkeys(leaves,"reply"))

        run = Toy(g,3,.01)
        books,_ = run.batched()
        self.assertNotIn(12,run.exact)
        for k,book in books.items():
            exact = optimize(g.weights[k],g.root_stop,g.actions(k,counts,False),all_gains,3)
            self.assertLessEqual(book["lower"],exact["value"]+1e-9)
            self.assertGreaterEqual(book["upper"]+1e-9,exact["value"])
            self.assertLessEqual(book["upper"]-book["lower"],.01)


if __name__=="__main__":
    unittest.main()
