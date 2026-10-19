"""extract_book(leftover=True) (2026-10-19): budget an opponent node cannot spend
goes to the closed child with the densest blocking atom.

FIXTURE. Root O is an opponent node:

  X (p 0.6): our node, stop 0.50, one move to a leaf worth 0.60
             -> one atom (cost 1, gain 0.10); weighted density 0.06
  Y (p 0.4): our node, stop 0.50, move y1 -> OY, an opponent node with two
             replies Z1, Z2 (p 0.5 each), each an our node (stop 0.50) with one
             move to a leaf worth 0.70.
             Y's values by budget: 1 -> 0.50 (y1 alone gains nothing),
             2 -> 0.60, 3 -> 0.70. The hull fuses that into ONE atom
             (cost 3, gain 0.20), although 2 moves already earn 0.10.

At budget 3 the density pass buys X (cost 1), then cannot fit Y's cost-3 atom:
Y closes with 2 left over.
  A  default: 1 move booked, root value 0.6*0.6 + 0.4*0.5 = 0.56
  B  leftover: Y gets the 2, books y1 and one reply's move: 3 moves,
     root value 0.6*0.6 + 0.4*0.6 = 0.60
  C  at budget 4 (everything fits) the two agree: leftover never changes a book
     the density pass could fully pay for
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from budget_core import Graph, OppNode, OurNode, build_curves, extract_book  # noqa: E402

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


O, X, Y, OY, Z1, Z2 = 1, 2, 3, 4, 5, 6
g = Graph(
    root=O,
    our={X: OurNode(l_node=0.50, cands=[("x1", None, False, 0.60)]),
         Y: OurNode(l_node=0.50, cands=[("y1", OY, True, 0.0)]),
         Z1: OurNode(l_node=0.50, cands=[("z1", None, False, 0.70)]),
         Z2: OurNode(l_node=0.50, cands=[("z2", None, False, 0.70)])},
    opp={O: OppNode(const_base=0.0, kids=[(0.6, X, True, 0.0), (0.4, Y, True, 0.0)]),
         OY: OppNode(const_base=0.0, kids=[(0.5, Z1, True, 0.0), (0.5, Z2, True, 0.0)])},
    epd={}, ply={O: 0, X: 1, Y: 1, OY: 2, Z1: 3, Z2: 3},
    reach={O: 1.0, X: 0.6, Y: 0.4, OY: 0.4, Z1: 0.2, Z2: 0.2})

curves, _ = build_curves(g, bmax=10)
check("Y's curve is one fused atom (cost 3, gain 0.2)",
      list(curves[Y].costs) == [3] and abs(curves[Y].gains[0] - 0.2) < 1e-12,
      f"{list(curves[Y].costs)}, {list(curves[Y].gains)}")

a = extract_book(g, curves, 3)
b = extract_book(g, curves, 3, leftover=True)
check("A: default books 1 move, value 0.56",
      a["spent_paths"] == 1 and abs(a["root_value_realized"] - 0.56) < 1e-12,
      f"{a['spent_paths']}, {a['root_value_realized']}")
check("B: leftover books 3 moves, value 0.60",
      b["spent_paths"] == 3 and abs(b["root_value_realized"] - 0.60) < 1e-12,
      f"{b['spent_paths']}, {b['root_value_realized']}")
c0 = extract_book(g, curves, 4)
c1 = extract_book(g, curves, 4, leftover=True)
check("C: when everything fits, leftover changes nothing",
      c0["booked"] == c1["booked"] and abs(c0["root_value_realized"] - 0.70 * 0.4 - 0.36) < 1e-12
      and c0["root_value_realized"] == c1["root_value_realized"],
      f"{c0['booked']} vs {c1['booked']}, {c0['root_value_realized']}")

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
