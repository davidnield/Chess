"""T2: POLICY SWITCHING — the feature's thesis in one fixture.

Root chooses between two candidates (selection key == value):

  A "early payoff":  --A--> oppA, no replies worth modelling: flat 0.60.
  B "deep trap":     --B--> oppB --t p=0.9--> B1 (our, L=0.45)
                                 --u p=0.1--> out-of-book leaf 0.50
                     B1 --b1move--> oppB1 --forced p=1.0--> B2 (our, L=0.48)
                     B2 --b2move--> leaf 0.95  (the trap payoff)

Hand arithmetic (curves):
  B2:    {0: 0.48, 1: 0.95}                       atom (1, 0.47)
  oppB1: base 0.48,                               atom (1, 0.47)
  B1:    {0:0.45, 1:0.48, 2:0.95} -> chain-fused  atom (2, 0.50)
  oppB:  base 0.1*0.50 + 0.9*0.45 = 0.455,        atom (2, 0.45)
  root:  {0:0.40, 1:0.60, 2:0.60, 3:0.905}        atoms (1,0.20),(2,0.305)

Assertions: the booked ROOT MOVE is A at budgets 1-2 and switches to B at 3+;
realized root values 0.60 / 0.60 / 0.905. A reach-truncation of the
UNCONSTRAINED book (which picks B, value 0.905) realizes only 0.455 at
budget 1 — the strawman the budget DP exists to beat.
"""
from __future__ import annotations

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from budget_core import Graph, OppNode, OurNode, build_curves, extract_book

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def close(a, b, tol=1e-12):
    return a is not None and abs(a - b) <= tol


ROOT, OPPA, OPPB, B1, OPPB1, B2 = 1, 2, 3, 4, 5, 6
g = Graph(
    root=ROOT,
    our={
        ROOT: OurNode(l_node=0.40, cands=[("A", OPPA, True, 0.0),
                                          ("B", OPPB, True, 0.0)]),
        B1:   OurNode(l_node=0.45, cands=[("b1move", OPPB1, True, 0.0)]),
        B2:   OurNode(l_node=0.48, cands=[("b2move", None, False, 0.95)]),
    },
    opp={
        OPPA:  OppNode(const_base=0.60, kids=[]),
        OPPB:  OppNode(const_base=0.1 * 0.50, kids=[(0.9, B1, True, 0.0)]),
        OPPB1: OppNode(const_base=0.0, kids=[(1.0, B2, True, 0.0)]),
    },
    epd={}, ply={ROOT: 0, OPPA: 1, OPPB: 1, B1: 2, OPPB1: 3, B2: 4},
    reach={ROOT: 1.0, OPPA: 1.0, OPPB: 1.0, B1: 0.9, OPPB1: 0.9, B2: 0.9},
)

curves, diag = build_curves(g, bmax=10)
check("no stranded nodes", diag["stranded_cycle_nodes"] == 0)
check("B1 chain fused", len(curves[B1].costs) == 1 and curves[B1].costs[0] == 2
      and close(curves[B1].gains[0], 0.50))
check("oppB base", close(curves[OPPB].eval(0), 0.455))
check("root curve vertices", close(curves[ROOT].eval(1), 0.60)
      and close(curves[ROOT].eval(2), 0.60) and close(curves[ROOT].eval(3), 0.905))

# the headline: policy switches from A to B as budget grows
for b, move, v in [(1, "A", 0.60), (2, "A", 0.60), (3, "B", 0.905),
                   (5, "B", 0.905)]:
    r = extract_book(g, curves, b)
    check(f"b={b}: root move {move}", r["booked"].get(ROOT) == move,
          f"got {r['booked'].get(ROOT)}")
    check(f"b={b}: realized {v}", close(r["root_value_realized"], v),
          f"got {r['root_value_realized']}")

r3 = extract_book(g, curves, 3)
check("b=3 books the whole trap chain",
      set(r3["booked"]) == {ROOT, B1, B2}, f"got {sorted(r3['booked'])}")

# the strawman: reach-truncating the UNCONSTRAINED book (which picks B). The
# fixed-policy option left the builder on 2026-11-12; here each node keeps only
# the fixed move, on a copy of the graph, which is what that option did.
fixed = {ROOT: "B", B1: "b1move", B2: "b2move"}
gfix = copy.deepcopy(g)
for h, n in gfix.our.items():
    n.cands = [c for c in n.cands if c[0] == fixed.get(h)]
fcurves, _ = build_curves(gfix, bmax=10)
rf = extract_book(gfix, fcurves, 1)
check("truncated unconstrained book at b=1 realizes only 0.455",
      close(rf["root_value_realized"], 0.455),
      f"got {rf['root_value_realized']}")
check("budget DP at b=1 beats it (0.60)", True)  # asserted above; stated for the log
rfull = extract_book(gfix, fcurves, 3)
check("fixed policy at full budget matches DP's 0.905",
      close(rfull["root_value_realized"], 0.905))

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
