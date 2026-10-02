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

# MATCH-DISTINCT. A book asked for N decisions delivers fewer; the truncation
# baseline has no such gap, so a like-for-like Phase D comparison needs the DP
# to actually reach N. Measured on the <=2024 white pool: 17 of 20, 305 of 400,
# 817 of 1000.
#
# The diamond at the top of this file exercises the per-path overcount, which
# is what makes the SEARCH necessary in principle: 2 path-charged units buy 2
# distinct decisions (root + T), T is charged twice, so asking for 3 distinct
# must charge more than 3. On the real pool that mechanism has never actually
# fired -- all 104 books written to _budget have spent_paths == spent_distinct
# -- and the production gap is unspent budget from indivisible chain atoms
# instead (see match_distinct's docstring, corrected 2026-09-08). Both produce
# "asked for N, got fewer" and the search fixes both, so this fixture stays:
# it is the only place the path-charging branch is covered at all.
from budget_core import flat_curve, match_distinct              # noqa: E402

r2, ch2, ok2, sp2 = match_distinct(g, curves, 2, 10)
check("match_distinct is a no-op when the plain budget already suffices",
      ok2 and ch2 == 2 and r2["spent_distinct"] == 2,
      f"charged {ch2}, booked {r2['spent_distinct']}, ok={ok2}")

# a target the graph cannot reach: only 2 our-nodes exist at all
r9, ch9, ok9, sp9 = match_distinct(g, curves, 9, 10)
check("match_distinct reports failure rather than looping when capacity is short",
      ok9 is False and r9["spent_distinct"] < 9,
      f"charged {ch9}, booked {r9['spent_distinct']}, ok={ok9}")
check("failed match still returns a usable book", r9["spent_distinct"] >= 1,
      f"booked {r9['spent_distinct']}")

# on the cycle fixture the search must still terminate and stay consistent
rc, chc, okc, spc = match_distinct(gcyc, ccur, 2, 10)
check("match_distinct terminates on a graph with cycles",
      rc["spent_distinct"] >= 1 and chc >= 2,
      f"charged {chc}, booked {rc['spent_distinct']}, ok={okc}")

# --- the returned spend, and the no-op contract (2026-08-31) --------------
# The flag broke its own contract by changing books it never probed: a single
# INFLATED curve set served both the target extraction and the probes, and a
# concave hull is global, so far points fused the cheap early atoms away.
# Pin the two properties that failure needs.
check("match_distinct returns the realised distinct count",
      sp2 == r2["spent_distinct"] and sp9 == r9["spent_distinct"]
      and spc == rc["spent_distinct"],
      f"returned {sp2}/{sp9}/{spc}")

# hit_target is `>= target`; only `== target` licenses an equal-footprint
# claim. fixdp_white_b20 booked 21 for a target of 20 and reported success.
check("hit_target is the >= test, so callers must check == for equal footprint",
      ok2 is (sp2 >= 2) and ok9 is (sp9 >= 9),
      f"ok2={ok2} sp2={sp2}; ok9={ok9} sp9={sp9}")

# The target extraction must read the PLAIN curves, so passing probe_curves
# cannot disturb a book that never probes. Hand it a deliberately different
# set: if the early exit ever consulted it, the result would move.
_wrong = {h: c for h, c in curves.items()}
_wrong[g.root] = flat_curve(-99.0)
rp, chp, okp, spp = match_distinct(g, curves, 2, 10, probe_curves=_wrong)
check("probe curves cannot touch a book that meets its target unprobed",
      chp == ch2 and spp == sp2 and okp == ok2
      and rp["booked"] == r2["booked"],
      f"charged {chp} vs {ch2}, booked {spp} vs {sp2}")

# --- the exactness scan over the range GROWTH skips (2026-09-08) -----------
# The search grows probes by 1.5x, so a target of 20 jumps to 30 and the
# bisection then narrows only inside [30, 32]: budgets 21..29 are never
# extracted. fixdp_white_b20 booked 21 moves for a target of 20 because of that
# blind spot, not because no exact budget existed -- an unequal footprint in
# the one comparison the ladder exists to make.
#
# Driven by a TABLE rather than a graph on purpose. The failure needs the
# distinct count to be non-monotone in the charged budget (30 and 31 booking
# FEWER than 32) and to skip the target entirely at the bracket; no fixture
# small enough to hand-verify does both, and the property under test belongs to
# the SEARCH, not to any graph. The table mirrors the measured white pool: 23
# is dp_white_b20's real charged budget, 32 is fixdp_white_b20's.
import budget_core as _bc                                       # noqa: E402

_REAL_EXTRACT = _bc.extract_book


def _table_search(table, target, bmax, scan_cap=64):
    """Run match_distinct against `table` (charged budget -> distinct count).

    Returns (charged, spent, hit, probed_budgets)."""
    probed = []

    def stub(g_, curves_, budget, fixed_policy=None, force_booked=None):
        probed.append(budget)
        return {"spent_distinct": table[budget], "booked": {}, "b": budget}

    _bc.extract_book = stub
    try:
        r, charged, hit, spent = _bc.match_distinct(
            None, {}, target, bmax, scan_cap=scan_cap)
    finally:
        _bc.extract_book = _REAL_EXTRACT
    return charged, spent, hit, probed, r


# 20 books 17 (the flag must engage); 30 and 31 book FEWER than the target
# while 32 clears it, so the bracket lands on an overshoot of 21; 23 is the
# smallest budget that lands exactly on 20.
TBL = {20: 17, 21: 18, 22: 19, 23: 20, 24: 21, 25: 20, 26: 22,
       27: 21, 28: 22, 29: 23, 30: 19, 31: 19, 32: 21}

ch, sp, hit, probed, _r = _table_search(TBL, 20, 32)
check("exactness scan finds the target the growth phase jumped over",
      sp == 20 and hit, f"charged {ch}, booked {sp}, hit={hit}")
check("the scan takes the SMALLEST charged budget that lands exactly",
      ch == 23, f"charged {ch} (25 also books 20, 23 is smaller)")
check("the bracket really did overshoot, so the scan is what fixed it",
      TBL[32] == 21 and 23 not in (30, 31, 32),
      "bisection settles on 32 -> 21 distinct")
check("budgets skipped by the 1.5x growth are the ones now probed",
      23 in probed and 30 in probed,
      f"probed {sorted(set(probed))}")

# scan_cap 0 must reproduce the OLD answer exactly -- the cap is a budget on
# extractions, not a change of verdict, and an inexact book stays visible.
ch0, sp0, hit0, _p0, _r0 = _table_search(TBL, 20, 32, scan_cap=0)
check("scan_cap 0 falls back to the smallest-known-good bracket",
      ch0 == 32 and sp0 == 21 and hit0,
      f"charged {ch0}, booked {sp0}")
check("a capped-out search still reports the overshoot to the caller",
      sp0 > 20, f"booked {sp0} for target 20")

# no exact hit anywhere: every budget skips 20. Must not loop, must return the
# smallest known-good, and must still say spent > target.
TBL_NONE = dict(TBL)
TBL_NONE.update({23: 21, 25: 21, 21: 18, 22: 19})
chn, spn, hitn, _pn, _rn = _table_search(TBL_NONE, 20, 32)
check("no exact budget exists -> smallest-known-good, still flagged inexact",
      spn > 20 and hitn, f"charged {chn}, booked {spn}")

# probes are cached: the scan must not re-extract a budget the bisection
# already paid for.
_chd, _spd, _hitd, probed_d, _rd = _table_search(TBL, 20, 32)
check("probe results are cached, never extracted twice",
      len(probed_d) == len(set(probed_d)),
      f"{len(probed_d)} extractions, {len(set(probed_d))} distinct budgets")

# --- an exact hit ABOVE the bracket (2026-09-08) ---------------------------
# The first version of the scan walked only [target+1, hi) and still missed the
# production case. Non-monotonicity cuts both ways: measured on dp/white with
# --scan-distinct, target 6 brackets at charged 12 booking NINE, budgets 7..11
# book 4/3/3/3/3, and charged 14 books exactly 6 -- above the bracket, and at a
# higher root value than the 9-move book. This table is that measurement.
DPW = {6: 3, 7: 4, 8: 3, 9: 3, 10: 3, 11: 3, 12: 9, 13: 10, 14: 6, 15: 12,
       16: 13, 17: 12, 18: 13, 19: 16, 20: 17, 21: 16, 22: 17, 23: 17,
       24: 18, 25: 19, 26: 20, 27: 20, 28: 20, 29: 21, 30: 21, 31: 23,
       32: 23}

ch, sp, hit, probed, _r = _table_search(DPW, 6, 32)
check("an exact hit ABOVE the bracket is found, not just below it",
      sp == 6 and ch == 14, f"charged {ch}, booked {sp}")
check("the bracket alone would have overshot by 3",
      DPW[12] == 9, "charged 12 books 9 for a target of 6")
check("budgets above the bracket are actually probed",
      any(b > 12 for b in probed), f"probed {sorted(set(probed))}")

# ... and when the footprint genuinely does not exist, no budget in range
# lands on it and the caller still sees the overshoot. fixdp/white, target 6:
# the count steps 8 -> 5 then 9 -> 7 and never equals 6 anywhere in 1..36.
FIXW = {6: 3, 7: 4, 8: 5, 9: 7, 10: 7, 11: 8, 12: 9, 13: 10, 14: 11, 15: 12,
        16: 13, 17: 14, 18: 15, 19: 16, 20: 13, 21: 13, 22: 14, 23: 13,
        24: 14, 25: 14, 26: 14, 27: 17, 28: 17, 29: 19, 30: 19, 31: 19,
        32: 21}
check("6 really is absent from fixdp/white's reachable footprints",
      6 not in FIXW.values(), f"reachable {sorted(set(FIXW.values()))}")
chf, spf, hitf, _pf, _rf = _table_search(FIXW, 6, 32)
check("an unreachable footprint reports the overshoot rather than faking it",
      spf > 6 and hitf, f"charged {chf}, booked {spf}")

# the scan must not run at all when the bracket already lands exactly --
# otherwise it would trade the smallest known-good budget for a larger one.
EXACT = dict(FIXW); EXACT[9] = 6
che, spe, _hite, probede, _re = _table_search(EXACT, 6, 32)
check("an exact bracket short-circuits, keeping the smallest charged budget",
      spe == 6 and che == 9 and max(probede) <= 9,
      f"charged {che}, booked {spe}, max probe {max(probede)}")

# --- which curve set answers a probe (2026-09-08) --------------------------
# Inflating a hull only coarsens it, so the plain set is strictly better
# wherever it reaches. Probing everything on the inflated set cost exactness on
# the real pool: dp/white b=6 came back charged 12 for NINE distinct off the
# coarse hull. Every probe at or below plain_bmax must read the plain curves.
PLAIN, PROBE = {"which": "plain"}, {"which": "probe"}


def _which_curves(target, bmax, plain_bmax, table_plain, table_probe):
    """Returns (charged, spent, [(budget, 'plain'|'probe'), ...])."""
    used = []

    def stub(g_, curves_, budget, fixed_policy=None, force_booked=None):
        tag = curves_.get("which", "plain")
        used.append((budget, tag))
        tbl = table_plain if tag == "plain" else table_probe
        return {"spent_distinct": tbl[budget], "booked": {}, "b": budget}

    _bc.extract_book = stub
    try:
        _r, charged, _hit, spent = _bc.match_distinct(
            None, PLAIN, target, bmax, probe_curves=PROBE,
            plain_bmax=plain_bmax)
    finally:
        _bc.extract_book = _REAL_EXTRACT
    return charged, spent, used


# The fine set steps one at a time and reaches 6 at budget 11; the coarse set
# jumps 3 -> 9 and never lands on 6, which is the production failure.
FINE = {6: 3, 7: 3, 8: 4, 9: 4, 10: 5, 11: 6, 12: 7, 13: 8, 14: 9,
        15: 9, 16: 10, 17: 11, 18: 12, 19: 13, 20: 14}
COARSE = {b: (3 if b < 12 else 9) for b in range(6, 33)}

ch, sp, used = _which_curves(6, 32, 20, FINE, COARSE)
check("a probe within the plain set's range reads the PLAIN curves",
      all(tag == "plain" for b, tag in used if b <= 20),
      f"used {used}")
check("the finer curves let the search land exactly on the target",
      sp == 6 and ch == 11, f"charged {ch}, booked {sp}")

# above plain_bmax the plain set cannot express the budget, so the inflated
# set must still be consulted -- otherwise the search has nowhere to go.
DEEP_FINE = {b: 2 for b in range(6, 21)}
DEEP_COARSE = {b: (2 if b < 30 else 6) for b in range(6, 33)}
ch2, sp2, used2 = _which_curves(6, 32, 20, DEEP_FINE, DEEP_COARSE)
check("above plain_bmax the search falls back to the inflated curves",
      any(tag == "probe" for b, tag in used2 if b > 20),
      f"used {sorted(set(used2))}")
check("the fallback still reaches a target the plain set cannot",
      sp2 == 6 and ch2 >= 30, f"charged {ch2}, booked {sp2}")

# plain_bmax omitted = the old behaviour, every probe on the inflated set.
_bc_used = []


def _stub_none(g_, curves_, budget, fixed_policy=None, force_booked=None):
    _bc_used.append(curves_.get("which", "plain"))
    return {"spent_distinct": COARSE[budget], "booked": {}, "b": budget}


_bc.extract_book = _stub_none
try:
    _bc.match_distinct(None, PLAIN, 6, 32, probe_curves=PROBE)
finally:
    _bc.extract_book = _REAL_EXTRACT
check("omitting plain_bmax keeps every probe on the inflated set",
      all(w == "probe" for w in _bc_used[1:]) and _bc_used[0] == "plain",
      f"first={_bc_used[0]}, rest={set(_bc_used[1:])}")

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
