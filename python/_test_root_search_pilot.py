"""Independent brute-force check of the bounded pilot's card allocation and pruning."""
import itertools
from pathlib import Path
import random
import sys
from types import SimpleNamespace

sys.path.insert(0,str(Path(__file__).parent/"experiments"))
from root_search_pilot import BUDGETS, KS, TOL, Chance, Leaf, Search


def check():
    rng = random.Random(69)
    for _ in range(20):
        leaves = {i:Leaf(rng.random()*.7,"x",rng.random()*.25) for i in range(10,18)}
        counts = {1:{10:7,11:4,12:1},2:{10:1,13:6,15:3},3:{16:1,17:1}}
        coarse = {1:({10:6},12),2:({13:6},10),3:({},None)}
        mids = {}
        for h in counts:
            raw = {i:rng.uniform(.001,1) for i in leaves}
            mids[h] = Chance("",{i:("","") for i in leaves},{i:p/sum(raw.values()) for i,p in raw.items()})
        class Fixture(Search):
            def prepare_stops(self,keys):
                self.stops.update({h:leaves[h].stop for h in keys})
            def expand(self,requests):
                for kind,h in requests:
                    if kind=="archive":
                        self.exact[h] = counts[h]
                    else:
                        self.leaves[h] = leaves[h]
        roots = {0:(None,{h:(str(h),"") for h in mids})}
        engine = Fixture(SimpleNamespace(coarse=coarse),mids,roots)
        engine.prepare_stops(leaves)
        engine.expand({("leaf",10),("leaf",14)})
        # Check the interval with deliberately unresolved leaves AND unassigned
        # counts, before the adaptive search can reveal them.
        partial = {(h,k,b):engine.bound(h,k,b) for h,k,b in itertools.product(mids,KS,BUDGETS)}
        result = engine.solve(False)
        for k,budget in itertools.product(KS,BUDGETS):
            def value(h,cards):
                n = sum(counts[h].values())
                return sum((counts[h].get(i,0)+k*q)/(n+k)*(leaves[i].stop+(leaves[i].gain if i in cards else 0))
                           for i,q in mids[h].q.items())
            true = max(value(h,subset) for h in mids for subset in itertools.combinations(leaves,budget-1))
            for h in mids:
                row = partial[h,k,budget]
                optimum = max(value(h,subset) for subset in itertools.combinations(leaves,budget-1))
                assert row.low-1e-12 <= value(h,row.cards) <= optimum+1e-12 and optimum <= row.high+1e-12
            move,cards,lo,hi = result[0,k,budget]
            realized = value(move,cards)
            assert lo-1e-12<=realized<=true+1e-12 and true<=hi+1e-12 and true-realized<=TOL+1e-12
            assert len(cards)<=budget-1 and len(cards)==len(set(cards))
    print("PASS exhaustive policy enumeration, shared children, zero floor-five support and below-floor aliases")


if __name__=="__main__":
    check()
