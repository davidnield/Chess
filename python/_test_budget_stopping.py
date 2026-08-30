"""T1: optimal stopping under a hard budget — the smallest end-to-end fixture.

Tree (our perspective values; crush off so key == value):

  R (our, L=0.50) --rmove--> O (opp) --x p=0.8--> U1 (our, L=0.55, book->0.70)
                                      --y p=0.2--> U2 (our, L=0.45, book->0.85)

Hand arithmetic:
  O(0)   = 0.8*0.55 + 0.2*0.45          = 0.53
  book U1: gain 0.8*(0.70-0.55)         = 0.12   (density 0.12)
  book U2: gain 0.2*(0.85-0.45)         = 0.08   (density 0.08)
  Realized root values at b=0..3:  0.50, 0.53, 0.65, 0.73

The ROOT HULL fuses everything into one cost-3 atom (0.23): the entry move's
gain (0.03) is below the downstream densities, so the chain-compression rule
treats R+U1+U2 as a package. eval(1)/eval(2) therefore under-report at 0.50 —
the documented conservative straddle — while EXTRACTION still recovers the
exact realized values at every budget. Both layers are asserted.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from budget_core import Graph, OppNode, OurNode, build_curves, extract_book, greedy_stopping

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def close(a, b, tol=1e-12):
    return a is not None and abs(a - b) <= tol


R, O, U1, U2 = 1, 2, 3, 4
g = Graph(
    root=R,
    our={
        R:  OurNode(l_node=0.50, cands=[("rmove", O, True, 0.0, 0.0)]),
        U1: OurNode(l_node=0.55, cands=[("u1move", None, False, 0.70, 0.0)]),
        U2: OurNode(l_node=0.45, cands=[("u2move", None, False, 0.85, 0.0)]),
    },
    opp={O: OppNode(const_base=0.0,
                    kids=[(0.8, U1, True, 0.0), (0.2, U2, True, 0.0)])},
    epd={}, ply={R: 0, O: 1, U1: 2, U2: 2},
    reach={R: 1.0, O: 1.0, U1: 0.8, U2: 0.2},
)

curves, diag = build_curves(g, bmax=10)
check("no stranded nodes", diag["stranded_cycle_nodes"] == 0)

# leaf curves
check("U1 curve", close(curves[U1].eval(0), 0.55) and close(curves[U1].eval(1), 0.70))
check("U2 curve", close(curves[U2].eval(0), 0.45) and close(curves[U2].eval(1), 0.85))
check("O base = weighted stop values", close(curves[O].eval(0), 0.53))
check("O eval(1) books U1 first", close(curves[O].eval(1), 0.65))
check("O eval(2) books both", close(curves[O].eval(2), 0.73))

rc = curves[R]
check("root hull fused to one cost-3 atom",
      len(rc.costs) == 1 and rc.costs[0] == 3 and close(rc.gains[0], 0.23),
      f"atoms={list(zip(rc.costs, rc.gains))}")
check("root hull conservative mid-chain", close(rc.eval(1), 0.50)
      and close(rc.eval(2), 0.50) and close(rc.eval(3), 0.73))

# extraction: exact realized values and booking order at every budget
want = {0: (0.50, set()), 1: (0.53, {R}), 2: (0.65, {R, U1}),
        3: (0.73, {R, U1, U2})}
for b, (v, booked) in want.items():
    r = extract_book(g, curves, b)
    check(f"extract b={b}: realized {v}", close(r["root_value_realized"], v),
          f"got {r['root_value_realized']}")
    check(f"extract b={b}: books {sorted(booked)}",
          set(r["booked"]) == booked, f"got {sorted(r['booked'])}")
    check(f"extract b={b}: distinct spend", r["spent_distinct"] == len(booked))

# monotone realized values
vals = [extract_book(g, curves, b)["root_value_realized"] for b in range(5)]
check("realized monotone in budget", all(x <= y + 1e-12 for x, y in zip(vals, vals[1:])))
check("budget beyond capacity saturates", close(vals[4], 0.73))

# greedy (myopic) agrees on this concave-ish fixture: order R, U1, U2
policy = {R: "rmove", U1: "u1move", U2: "u2move"}
for b, booked in [(1, {R}), (2, {R, U1}), (3, {R, U1, U2})]:
    got, spent = greedy_stopping(g, policy, b)
    check(f"greedy b={b}", got == booked and spent == len(booked),
          f"got {sorted(got)}")

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
