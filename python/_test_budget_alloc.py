"""T3: opponent-node budget allocation — exactness at hull vertices, the
characterized straddle gap, and skip-fit extraction recovering it.

Root is an OPPONENT node (as it is for every Black book), children:

  X (p=0.6): our node, values 0.50 / 0.60 / 0.62 by budget — concave.
  Y (p=0.4): our node, values 0.50 / 0.57 / 0.65 — a NON-CONCAVE lump, whose
             own curve convexifies to one cost-2 atom (gain 0.15).

Weighted atoms at the root merge: X1 (1, .060), Y (2, .060), X2 (1, .012).

Semantics pinned forever by this test:
  hull eval:   b=1 -> 0.560 (exact)   b=2 -> 0.560 (conservative straddle)
               b=3 -> 0.620 (exact)   b=4 -> 0.632 (exact)
  exact-alloc: b=2 -> 0.572 (best allocation over the HULLED children)
  extraction (skip-fit): realizes 0.572 at b=2 — beats its own hull.
  The true two-level optimum at b=2 is 0.588 (X at 1 + Y's raw 0.57): the
  missing 0.016 is the price of convexifying Y's lump into a chain atom, and
  it is invisible to exact-alloc BY DESIGN — child hulls are taken first.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from budget_core import (
    Graph, OppNode, OurNode, build_curves, exact_opp_curve, extract_book,
)

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def close(a, b, tol=1e-12):
    return a is not None and abs(a - b) <= tol


O, X, OX, X2, Y, OY, Y2 = 1, 2, 3, 4, 5, 6, 7
g = Graph(
    root=O,
    our={
        X:  OurNode(l_node=0.50, cands=[("x1", OX, True, 0.0, 0.0)]),
        X2: OurNode(l_node=0.60, cands=[("x2", None, False, 0.62, 0.0)]),
        Y:  OurNode(l_node=0.50, cands=[("y1", OY, True, 0.0, 0.0)]),
        Y2: OurNode(l_node=0.57, cands=[("y2", None, False, 0.65, 0.0)]),
    },
    opp={
        O:  OppNode(const_base=0.0, kids=[(0.6, X, True, 0.0),
                                          (0.4, Y, True, 0.0)]),
        OX: OppNode(const_base=0.0, kids=[(1.0, X2, True, 0.0)]),
        OY: OppNode(const_base=0.0, kids=[(1.0, Y2, True, 0.0)]),
    },
    epd={}, ply={O: 0, X: 1, OX: 2, X2: 3, Y: 1, OY: 2, Y2: 3},
    reach={O: 1.0, X: 0.6, OX: 0.6, X2: 0.6, Y: 0.4, OY: 0.4, Y2: 0.4},
)

curves, diag = build_curves(g, bmax=10)
check("no stranded nodes", diag["stranded_cycle_nodes"] == 0)

check("X concave: two atoms", len(curves[X].costs) == 2
      and close(curves[X].eval(1), 0.60) and close(curves[X].eval(2), 0.62))
check("Y lump fused: one cost-2 atom", len(curves[Y].costs) == 1
      and curves[Y].costs[0] == 2 and close(curves[Y].gains[0], 0.15))

rc = curves[O]
check("hull b=1 exact", close(rc.eval(1), 0.560))
check("hull b=2 conservative straddle", close(rc.eval(2), 0.560))
check("hull b=3 exact", close(rc.eval(3), 0.620))
check("hull b=4 exact", close(rc.eval(4), 0.632))

grid = exact_opp_curve(g, O, curves, bmax=4)
check("exact-alloc agrees at vertices",
      close(grid[1], 0.560) and close(grid[3], 0.620) and close(grid[4], 0.632))
check("exact-alloc recovers the straddle: b=2 -> 0.572", close(grid[2], 0.572),
      f"got {grid[2]}")
# The true optimum 0.588 (using Y's pre-hull 0.57) is deliberately out of
# reach: child convexification costs 0.016 here — the chain-atom price.
check("hull <= exact-alloc everywhere",
      all(rc.eval(b) <= grid[b] + 1e-12 for b in range(5)))

# extraction with skip-fit
for b, v, xa, ya in [(1, 0.560, 1, 0), (2, 0.572, 2, 0),
                     (3, 0.620, 1, 2), (4, 0.632, 2, 2)]:
    r = extract_book(g, curves, b)
    check(f"extract b={b}: realized {v}", close(r["root_value_realized"], v),
          f"got {r['root_value_realized']}")
    check(f"extract b={b}: alloc X={xa} Y={ya}",
          r["alloc"].get(X, 0) == xa and r["alloc"].get(Y, 0) == ya,
          f"got X={r['alloc'].get(X)} Y={r['alloc'].get(Y)}")
check("extraction beats its own hull at the straddle",
      extract_book(g, curves, 2)["root_value_realized"] > rc.eval(2) + 1e-9)

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
