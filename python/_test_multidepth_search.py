"""Check bounds against explicit policies with a recurring board and shared arrivals."""
from itertools import product
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).parent/"experiments"))
from multidepth_allocation import Allocator
from multidepth_graph import build, pack
from multidepth_panel import Panel
from repertoire_input_cache import Cache


class MultiDepth(unittest.TestCase):
    def test_global_policy_and_bounds(self):
        levels = [
            pack([1,2],{1:.31,2:.32},[0,0,1],[101,102,201],[.1,.2,.05],
                 [(0,0,.7),(1,0,.2),(2,0,.4),(2,1,.5)],2),
            pack([3,4],{3:.4,4:.42},[0,0,1],[301,302,401],[.07,.2,.2],
                 [(0,0,.7),(0,1,.1),(1,1,.5),(2,1,.2)],2),
            pack([1,5],{1:.3,5:.25},[0,0,1],[101,102,501],[.15,.55,.65],[],0)]
        a = Allocator(levels,{1:.55,2:.45})
        for budget in (1,2,3,4,5):
            best = 0.
            for picks in product(*[[None,*menu] for menu in a.menus.values()]):
                b = {h:m for h,m in zip(a.menus,picks) if m is not None}
                if len(b)>budget:
                    continue
                # Direct recursion follows the same SAN for every visit to board 1.
                def value(d,i):
                    l = levels[d]
                    h = l.ids[i]
                    if h not in b:
                        return l.stop[i]
                    row = l.moves.index(b[h])
                    return l.base[row]+sum(p*value(d+1,j) for j,p in zip(l.edges[row].indices,l.edges[row].data))
                best = max(best,.55*value(0,0)+.45*value(0,1))
            book,upper = a.optimize(budget)
            replay = a.replay(book)
            self.assertLessEqual(replay["cards"],budget)
            self.assertLessEqual(replay["value"],best+1e-9)
            self.assertGreaterEqual(min(upper,a.lp_bound(budget))+1e-9,best)
            if budget==5:
                self.assertAlmostEqual(best,replay["value"])

    def test_cache_keys_rollback_and_empty_hit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"cache.sqlite"
            s = Cache(path)
            s.batch("move","a",0,{1:"board"},lambda b:{1:[]})
            s.db.close()
            s = Cache(path)
            self.assertEqual(s.batch("move","a",0,{1:"board"},lambda _:self.fail()),{1:[]})
            s.batch("move","b",0,{1:"board"},lambda b:{1:[1]})
            s.batch("move","b",1,{1:"board"},lambda b:{1:[2]})
            with self.assertRaises(ValueError):
                s.batch("move","a",0,{1:"different"},lambda _:self.fail())
            with self.assertRaises(ValueError):
                s.batch("move","a",0,{2:"x",3:"y"},lambda _:{2:[],3:float("nan")})
            self.assertEqual(s.batch("move","a",0,{2:"x"},lambda _:{2:[9]}),{2:[9]})
            s.db.close()

    def test_full_and_truncated_input_bound(self):
        p = Panel()
        p.layers = [[1],[2,3],[1,4,5]]
        p.weights = {0:{1:1.},30:{1:1.}}
        p.menu = {1:[11],2:[12],3:[13]}
        p.boards = {i:str(i) for i in range(1,30)}
        p.white = {11:(10001,[[2,.7,7000],[3,.3,3000],[21,0.,0]]),
                   12:(None,[[1,.8,0],[4,.2,0]]),13:(10001,[[4,.99,10000],[5,.01,0]])}
        class Inputs:
            def values(self,boards,ply):
                return {h:.2 if ply%2 else .6 for h in boards}
            def candidates(self,boards):
                return {h:{h+10:("a",str(h+10))} for h in boards}
            def exact_counts(self,boards):
                c = {11:{2:7000,3:3000,21:1},12:{1:1,4:4},13:{4:10000,5:1}}
                return {h:c[h] for h in boards}
        exact,_,_ = build(p,Inputs(),5,3,.01,True,1)
        cut,gap,_ = build(p,Inputs(),5,3,.01,False,1)
        for k in (0,30):
            ref = Allocator(exact[k],p.weights[k])
            search = Allocator(cut[k],p.weights[k])
            b,ub = search.optimize(3)
            self.assertGreaterEqual(ref.replay(b)["value"]+1e-9,search.replay(b)["value"])
            self.assertLessEqual(ref.replay(b)["value"]-search.replay(b)["value"],gap[k]+1e-8)
            self.assertLessEqual(ref.replay(ref.optimize(3)[0])["value"],ub+gap[k]+1e-8)


if __name__=="__main__":
    unittest.main()
