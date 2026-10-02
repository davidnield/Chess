"""T2: POLICY SWITCHING — the feature's thesis in one fixture.

Root chooses between two candidates (crush off, key == value):

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
        ROOT: OurNode(l_node=0.40, cands=[("A", OPPA, True, 0.0, 0.0),
                                          ("B", OPPB, True, 0.0, 0.0)]),
        B1:   OurNode(l_node=0.45, cands=[("b1move", OPPB1, True, 0.0, 0.0)]),
        B2:   OurNode(l_node=0.48, cands=[("b2move", None, False, 0.95, 0.0)]),
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

# the strawman: reach-truncating the UNCONSTRAINED book (which picks B).
fixed = {ROOT: "B", B1: "b1move", B2: "b2move"}
fcurves, _ = build_curves(g, bmax=10, fixed_policy=fixed)
rf = extract_book(g, fcurves, 1, fixed_policy=fixed)
check("truncated unconstrained book at b=1 realizes only 0.455",
      close(rf["root_value_realized"], 0.455),
      f"got {rf['root_value_realized']}")
check("budget DP at b=1 beats it (0.60)", True)  # asserted above; stated for the log
rfull = extract_book(g, fcurves, 3, fixed_policy=fixed)
check("fixed policy at full budget matches DP's 0.905",
      close(rfull["root_value_realized"], 0.905))

# The GREEDY comparator must be genuinely trap-blind, or Phase D cannot
# attribute the DP's win to policy switching. Greedy follows the unconstrained
# policy (which picks B) and books by one-step gain, so it pays for the trap
# prefix it cannot finish: 0.455 / 0.482 against the DP's 0.600 / 0.600, with
# both converging at b=3. If this ever ties, the comparator is worthless and
# the experiment needs redesigning -- hence an assertion, not a note.
from budget_core import greedy_stopping                        # noqa: E402

fixed_pol = {ROOT: "B", B1: "b1move", B2: "b2move"}
greedy_vals = {}
greedy_spend = {}
for b in (1, 2, 3):
    got, _spent = greedy_stopping(g, fixed_pol, b)
    mask = {h: fixed_pol[h] for h in got}
    gc, _ = build_curves(g, bmax=b, fixed_policy=mask)
    # force_booked mirrors the production call in build_budget_books: greedy's
    # SET is greedy's to choose, and the density allocator must not re-decide
    # it. See the extract_book docstring for the measurement that forced this.
    r = extract_book(g, gc, b, fixed_policy=mask, force_booked=got)
    greedy_vals[b] = r["root_value_realized"]
    greedy_spend[b] = (len(got), r["spent_distinct"])

# EQUAL FOOTPRINT is the precondition for the whole comparison: an arm that
# books fewer moves than its budget is being measured on size, not method.
for b in (1, 2, 3):
    sel, bk = greedy_spend[b]
    check(f"greedy books its whole selection at b={b} ({sel})", sel == bk,
          f"selected {sel}, booked {bk}")
check("greedy is trap-blind at b=1 (0.455)", close(greedy_vals[1], 0.455),
      f"got {greedy_vals[1]}")
check("greedy is trap-blind at b=2 (0.482)", close(greedy_vals[2], 0.482),
      f"got {greedy_vals[2]}")
check("greedy converges to the DP at b=3", close(greedy_vals[3], 0.905))
dp_vals = {b: extract_book(g, curves, b)["root_value_realized"] for b in (1, 2, 3)}
check("DP strictly beats greedy at small budgets",
      dp_vals[1] > greedy_vals[1] + 1e-9 and dp_vals[2] > greedy_vals[2] + 1e-9,
      f"dp={dp_vals[1]:.3f}/{dp_vals[2]:.3f} greedy={greedy_vals[1]:.3f}/{greedy_vals[2]:.3f}")


# FORCE_BOOKED must cover the NEVER-REACHED case, not just the unfunded one.
# extract_book's topo walk skips any node not in `reached` BEFORE it consults
# `forced`, so exempting the value test alone is not enough: a forced node
# behind an ancestor that declines to book is never walked to at all. The first
# version of this fix did only the value exemption, greedy_white rebuilt to the
# identical 3/6, 4/10, 12/20 it had before, and the test below is what was
# missing to catch it.
#
# Chain: ROOT -> Oa -> M -> Ob -> D.  M's stop value (0.90) beats anything its
# subtree can return (0.82), so unforced M never books and D is unreachable.
FROOT, FOA, FM, FOB, FD = 31, 32, 33, 34, 35
gforce = Graph(
    root=FROOT,
    our={
        FROOT: OurNode(l_node=0.40, cands=[("r", FOA, True, 0.0, 0.0)]),
        FM:    OurNode(l_node=0.90, cands=[("m", FOB, True, 0.0, 0.0)]),
        FD:    OurNode(l_node=0.10, cands=[("d", None, False, 0.82, 0.0)]),
    },
    opp={
        FOA: OppNode(const_base=0.0, kids=[(1.0, FM, True, 0.0)]),
        FOB: OppNode(const_base=0.0, kids=[(1.0, FD, True, 0.0)]),
    },
    epd={}, ply={FROOT: 0, FOA: 1, FM: 2, FOB: 3, FD: 4},
    reach={FROOT: 1.0, FOA: 1.0, FM: 1.0, FOB: 1.0, FD: 1.0},
)
fcur, _fd = build_curves(gforce, bmax=6)
plain = extract_book(gforce, fcur, 6)
check("unforced: M declines and D is never reached",
      FM not in plain["booked"] and FD not in plain["booked"],
      f"booked={sorted(plain['booked'])}")
want = {FROOT, FM, FD}
forced_res = extract_book(gforce, fcur, 6, force_booked=want)
check("force_booked books every node in the set, reached or not",
      want <= set(forced_res["booked"]),
      f"missing {sorted(want - set(forced_res['booked']))}")
check("forced spend equals the forced set size",
      forced_res["spent_distinct"] == len(want),
      f"got {forced_res['spent_distinct']} of {len(want)}")

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
