"""T8: transposition characterization — the v1 per-path budget overcount.

Diamond: root (our) --m--> O (opp) whose two replies (p=0.5 each) both lead to
the SAME our node T (a transposition). T books one move to a 0.9 leaf, L=0.5.

Contract pinned here (v1, documented in budget_core's header):
  - the CURVE charges T once PER PATH: root capacity = 3 (1 root + 2 for T);
  - EXTRACTION books T once: funding one path funds the position, so at b=2
    the realized value is the full 0.9-both-paths answer, better than the
    conservative hull claims;
  - spent_distinct counts T once (2 decisions), spent_paths likewise counts
    booked nodes (the overcount lives in the curve, not the spend report).
A future v2 that charges distinct cost must change these assertions
DELIBERATELY — that is what a characterization test is for.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from budget_core import (Graph, OppNode, OurNode, build_curves, extract_book,
                         topo_down)

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def close(a, b, tol=1e-12):
    return a is not None and abs(a - b) <= tol


ROOT, O, T = 1, 2, 3
g = Graph(
    root=ROOT,
    our={
        ROOT: OurNode(l_node=0.40, cands=[("m", O, True, 0.0, 0.0)]),
        T:    OurNode(l_node=0.50, cands=[("t", None, False, 0.90, 0.0)]),
    },
    opp={O: OppNode(const_base=0.0,
                    kids=[(0.5, T, True, 0.0), (0.5, T, True, 0.0)])},
    epd={}, ply={ROOT: 0, O: 1, T: 2},
    reach={ROOT: 1.0, O: 1.0, T: 0.5},
)

curves, diag = build_curves(g, bmax=10)
check("no stranded nodes", diag["stranded_cycle_nodes"] == 0)

# per-path double charge in the curve
check("root curve capacity 3 (T charged per path)", curves[ROOT].capacity == 3,
      f"capacity={curves[ROOT].capacity}")
check("O(0) both paths stop", close(curves[O].eval(0), 0.50))
check("O(2) both paths funded", close(curves[O].eval(2), 0.90))

r2 = extract_book(g, curves, 2)
check("b=2 books root and T once", set(r2["booked"]) == {ROOT, T}
      and r2["spent_distinct"] == 2, f"got {sorted(r2['booked'])}")
check("b=2 realized full transposed value 0.90",
      close(r2["root_value_realized"], 0.90),
      f"got {r2['root_value_realized']}")
check("realized beats the per-path hull (documented conservatism)",
      r2["root_value_realized"] > curves[ROOT].eval(2) + 1e-9,
      f"hull={curves[ROOT].eval(2)}")

r1 = extract_book(g, curves, 1)
check("b=1 books only root", set(r1["booked"]) == {ROOT})
check("b=1 realized 0.50", close(r1["root_value_realized"], 0.50))

# CYCLE POISONING regression. A transposition cycle leaves nodes uncomputable
# by Kahn order; if those are merely flattened in place, every ANCESTOR still
# has a non-zero pending count and gets flattened too — so one cycle anywhere
# gives the ROOT capacity 0 and an empty book. Observed for real on the <=2024
# pool at max_ply 14 (185 stranded nodes -> root capacity 0). build_curves now
# flattens cycle nodes one at a time and resumes the drain, so ancestors are
# computed properly.
C1, C2, COPP = 11, 12, 13
gcyc = Graph(
    root=ROOT,
    our={
        ROOT: OurNode(l_node=0.40, cands=[("m", O, True, 0.0, 0.0)]),
        T:    OurNode(l_node=0.50, cands=[("t", None, False, 0.90, 0.0)]),
        C1:   OurNode(l_node=0.50, cands=[("c1", COPP, True, 0.0, 0.0)]),
        C2:   OurNode(l_node=0.50, cands=[("c2", None, False, 0.55, 0.0)]),
    },
    opp={
        O:    OppNode(const_base=0.0, kids=[(0.5, T, True, 0.0),
                                            (0.5, C1, True, 0.0)]),
        COPP: OppNode(const_base=0.0, kids=[(1.0, C1, True, 0.0)]),  # C1 -> C1
    },
    epd={}, ply={ROOT: 0, O: 1, T: 2, C1: 2, COPP: 3, C2: 4},
    reach={ROOT: 1.0, O: 1.0, T: 0.5, C1: 0.5, COPP: 0.5, C2: 0.5},
)
ccur, cdiag = build_curves(gcyc, bmax=10)
check("cycle detected and flattened", cdiag["stranded_cycle_nodes"] >= 1,
      f"flattened={cdiag['stranded_cycle_nodes']}")
check("every node still gets a curve", len(ccur) == len(gcyc.our) + len(gcyc.opp),
      f"{len(ccur)} of {len(gcyc.our)+len(gcyc.opp)}")
check("ROOT is NOT poisoned by the cycle (capacity > 0)",
      ccur[ROOT].capacity > 0, f"capacity={ccur[ROOT].capacity}")
rcyc = extract_book(gcyc, ccur, 3)
check("cycle graph still books the reachable T line", ROOT in rcyc["booked"]
      and T in rcyc["booked"], f"booked={sorted(rcyc['booked'])}")

check("topo_down covers every node despite the cycle",
      len(set(topo_down(gcyc))) == len(gcyc.our) + len(gcyc.opp),
      f"{len(set(topo_down(gcyc)))} of {len(gcyc.our)+len(gcyc.opp)}")

# TOPO-DOWN CONE-DROP regression (2026-08-29). build_curves survives cycles by
# flattening a victim and resuming, but topo_down used to just let Kahn stall:
# a cycle node never hits in-degree 0, and NEITHER DOES ANYTHING BELOW IT, so
# the returned order lost the cycle plus its whole downstream cone. extract_book
# iterates that order and skips what it never sees, so an empty book came back
# with perfectly healthy curves. Worst case is a cycle that reaches the ROOT:
# then nothing at all is booked. Measured on the <=2024 white pool at
# --max-cands 0: 50,015 of 150,949 nodes ordered, root omitted, 0 booked at
# every budget. A candidate cap masked it by pruning the cycle-closing edges.
#
# Fixture: the root itself sits on a cycle (RC -> ROPP -> RC), with a booking
# opportunity hanging off it. A Kahn-only order returns neither.
RC, ROPP, RLEAF = 21, 22, 23
groot = Graph(
    root=RC,
    our={
        RC:    OurNode(l_node=0.40, cands=[("r", ROPP, True, 0.0, 0.0)]),
        RLEAF: OurNode(l_node=0.50, cands=[("x", None, False, 0.95, 0.0)]),
    },
    opp={ROPP: OppNode(const_base=0.0, kids=[(0.5, RC, True, 0.0),
                                             (0.5, RLEAF, True, 0.0)])},
    epd={}, ply={RC: 0, ROPP: 1, RLEAF: 2},
    reach={RC: 1.0, ROPP: 1.0, RLEAF: 0.5},
)
gcur, _gd = build_curves(groot, bmax=10)
ordr = topo_down(groot)
check("root on a cycle is still ordered", RC in ordr, f"order={ordr}")
check("every node ordered when the root is on a cycle",
      len(set(ordr)) == 3, f"{len(set(ordr))} of 3")
rroot = extract_book(groot, gcur, 2)
check("a cycle through the root does not empty the book",
      len(rroot["booked"]) > 0 and rroot["root_value_realized"] is not None,
      f"booked={sorted(rroot['booked'])}, "
      f"realized={rroot['root_value_realized']}")

# MATCH-DISTINCT. The per-path overcount above means a book asked for N
# decisions delivers fewer; the truncation baseline has no such gap, so a
# like-for-like Phase D comparison needs the DP to actually reach N. Measured
# on the <=2024 white pool: 17 of 20, 305 of 400, 817 of 1000.
#
# The diamond at the top of this file is the minimal case: 2 path-charged units
# buy 2 distinct decisions (root + T), and T is charged twice, so asking for 3
# distinct must charge more than 3.
from budget_core import match_distinct                          # noqa: E402

r2, ch2, ok2 = match_distinct(g, curves, 2, 10)
check("match_distinct is a no-op when the plain budget already suffices",
      ok2 and ch2 == 2 and r2["spent_distinct"] == 2,
      f"charged {ch2}, booked {r2['spent_distinct']}, ok={ok2}")

# a target the graph cannot reach: only 2 our-nodes exist at all
r9, ch9, ok9 = match_distinct(g, curves, 9, 10)
check("match_distinct reports failure rather than looping when capacity is short",
      ok9 is False and r9["spent_distinct"] < 9,
      f"charged {ch9}, booked {r9['spent_distinct']}, ok={ok9}")
check("failed match still returns a usable book", r9["spent_distinct"] >= 1,
      f"booked {r9['spent_distinct']}")

# on the cycle fixture the search must still terminate and stay consistent
rc, chc, okc = match_distinct(gcyc, ccur, 2, 10)
check("match_distinct terminates on a graph with cycles",
      rc["spent_distinct"] >= 1 and chc >= 2,
      f"charged {chc}, booked {rc['spent_distinct']}, ok={okc}")

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
