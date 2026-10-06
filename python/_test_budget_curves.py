"""T4: curve primitives — convexify/chain-compression, merge order, cap, eval.

Synthetic, hand-computed. PASS/FAIL, exit 0/1 (run_tests.py convention).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

from budget_core import Curve, cap_curve, curve_from_points, flat_curve, merge_weighted

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def close(a, b, tol=1e-12):
    return abs(a - b) <= tol


# ── chain compression ───────────────────────────────────────────────────────
# Increments 0.005, 0.005, 0.20 (cost 1 each): densities rise, so the hull
# must fuse all three into ONE cost-3 atom of gain 0.21 (density 0.07) — the
# trap-chain compression the design rests on.
c = curve_from_points([(0, 0.0), (1, 0.005), (2, 0.010), (3, 0.210)])
check("chain fuses to one atom", len(c.costs) == 1 and c.costs[0] == 3
      and close(c.gains[0], 0.21), f"atoms={list(zip(c.costs, c.gains))}")
check("chain eval(2) under-reports mid-chain (documented)", close(c.eval(2), 0.0))
check("chain eval(3) exact at vertex", close(c.eval(3), 0.21))

# Decreasing densities stay separate atoms.
c2 = curve_from_points([(0, 0.5), (1, 0.7), (2, 0.75), (3, 0.76)])
check("concave input keeps 3 atoms", len(c2.costs) == 3,
      f"atoms={list(zip(c2.costs, c2.gains))}")
check("concave eval each vertex",
      close(c2.eval(1), 0.7) and close(c2.eval(2), 0.75) and close(c2.eval(3), 0.76))

# Monotone enforcement: a dipping point set is lifted to its running max.
c3 = curve_from_points([(0, 0.5), (1, 0.6), (2, 0.55), (3, 0.65)])
check("monotone running max", close(c3.eval(2), 0.6) and close(c3.eval(3), 0.65))

# Zero-gain stretch absorbed into the following atom's cost.
c4 = curve_from_points([(0, 0.0), (1, 0.0), (2, 0.3)])
check("flat stretch folds into next atom", len(c4.costs) == 1
      and c4.costs[0] == 2 and close(c4.gains[0], 0.3))
check("trailing flat dropped",
      len(curve_from_points([(0, 0.1), (1, 0.1)]).costs) == 0)

# ── weighted merge (opponent node) ─────────────────────────────────────────
# Sibling atoms 0.09 and 0.05 around the fused 0.07 chain atom: density order
# must interleave 0.09 > 0.07 > 0.05.
sib = Curve(0.0, np.array([1, 1], dtype=np.int64),
            np.array([0.09, 0.05], dtype=np.float64))
m = merge_weighted([(1.0, sib), (1.0, c)], 0.0, bmax=100, k_atoms=16)
dens = list(m.gains / m.costs)
check("merge density order", np.all(np.diff(dens) <= 1e-15)
      and close(dens[0], 0.09) and close(dens[1], 0.07) and close(dens[2], 0.05),
      f"densities={dens}")
check("merge base sums weighted bases", close(m.base, 0.0))
mw = merge_weighted([(0.5, sib)], 0.25, bmax=100, k_atoms=16)
check("merge scales gains by p and adds const",
      close(mw.base, 0.25) and close(mw.gains[0], 0.045))

# ── cap ─────────────────────────────────────────────────────────────────────
big = Curve(0.0, np.array([1, 1, 1, 1], dtype=np.int64),
            np.array([0.4, 0.3, 0.2, 0.1], dtype=np.float64))
t = cap_curve(big, bmax=2, k_atoms=16)
check("bmax trims atoms", t.capacity == 2 and close(t.eval(2), 0.7))
k = cap_curve(big, bmax=100, k_atoms=2)
check("k_atoms coarsens, value at capacity preserved",
      len(k.costs) == 2 and close(k.eval(4), 1.0))
dens_k = list(k.gains / k.costs)
check("coarsened curve still density-sorted", np.all(np.diff(dens_k) <= 1e-15))

check("flat curve capacity 0 evals base", close(flat_curve(0.42).eval(7), 0.42))

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
