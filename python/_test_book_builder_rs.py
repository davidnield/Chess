"""The Rust builder core (book_builder_rs, rust/book_builder) is budget_core, bit for bit.

Skips without the extension (build it with rust/book_builder/build.ps1). Every exported
function is compared with the Python function it ports, on fixed edge cases and on
random inputs, at tolerance 0: floats by their bits, so a NaN, a -0.0 or the last ulp
all count.

  V  cp_to_expected_score on every int16 centipawn and on the eval-curve range, against
     stage3's math.exp version AND the polars expression the eval loaders use;
     ws_to_cp against budget_core.ws_to_cp, both colours, clamps included.
  L  leaf_edge_value and leaf_node_value, both colours, with and without evals, zero
     and huge counts, fixed and dynamic eval weights.
  A  aux_row_buckets + aux_opp_mix: missing rows, missing other_eval_mean, zero buckets,
     both colours, both coverage rules, dynamic eval weights.
"""
from __future__ import annotations

import math
import random
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

import budget_core as bc                                             # noqa: E402
from stage3_backwards_induction import (_es_expr, aux_opp_mix,       # noqa: E402
                                        cp_to_expected_score)

try:
    import book_builder_rs as rs
except ImportError:
    rs = None

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def bits(x) -> np.ndarray:
    return np.ascontiguousarray(np.asarray(x, dtype=np.float64)).view(np.int64)


def same(a, b) -> int:
    """How many elements differ by bits."""
    a, b = bits(a), bits(b)
    return int(a.size != b.size) or int((a != b).sum())


def check_values() -> None:
    cps = np.arange(-32768, 32768, dtype=np.int64)
    py = np.array([cp_to_expected_score(int(c)) for c in cps])
    pol = pl.DataFrame({"eval_cp": cps.astype(np.int32)}).select(_es_expr()).to_series().to_numpy()
    got = rs.cp_to_expected_score(cps.astype(np.float64))
    check("V: cp_to_expected_score = stage3's (math.exp) on every int16", same(got, py) == 0,
          f"{same(got, py)} of {cps.size} differ")
    check("V: ... and = the polars expression the eval loaders use", same(got, pol) == 0,
          f"{same(got, pol)} differ")
    rng = random.Random(1)
    ws = [0.0, 1.0, 1e-12, 1 - 1e-12, 0.5, 1e-9, 1 - 1e-9] + [rng.random() for _ in range(20000)]
    for ow in (True, False):
        py = np.array([bc.ws_to_cp(w, ow) for w in ws])
        got = rs.ws_to_cp(np.array(ws), ow)
        check(f"V: ws_to_cp = budget_core's (our_white={ow})", same(got, py) == 0,
              f"{same(got, py)} of {len(ws)} differ")


BLENDS = [  # (slice_prior, prior_strength, eval_weight, eval_weight_min, eval_weight_k)
    (0.5196, 500.0, 0.75, 0.0, 0.0),         # the recipe
    (0.5, 500.0, 0.5, 0.0, 0.0),
    (0.48, 0.0, 1.0, 0.0, 0.0),               # the oracle fixture's knobs
    (0.52, 200.0, 0.0, 0.0, 0.0),             # no engine
    (0.51, 500.0, 0.9, 0.2, 300.0),           # dynamic eval weight
]


def check_leaves() -> None:
    rng = random.Random(2)
    n = 20000
    ws = np.array([rng.choice([0.0, 1.0, 0.5, rng.random()]) for _ in range(n)])
    tot = np.array([rng.choice([0, 1, 5, 50, 10**6, 3 * 10**9, rng.randrange(1, 5000)])
                    for _ in range(n)], dtype=np.int64)
    ev = np.array([math.nan if rng.random() < 0.3 else rng.random() for _ in range(n)])
    bad_e = bad_n = 0
    for sp, ps, ew, ewm, ewk in BLENDS:
        # prior strength 0 with a zero-game edge is 0/0, which Python raises on; the port's
        # graph assembly refuses that combination instead (no recipe has prior strength 0)
        tot = np.where(tot == 0, 1, tot) if ps == 0 else tot
        for ow in (True, False):
            py = [bc.leaf_edge_value(float(w), int(t), None if math.isnan(e) else float(e),
                                     sp, ow, ps, ew, ewm, ewk) for w, t, e in zip(ws, tot, ev)]
            got = rs.leaf_edge_values(ws, tot, ev, sp, ow, ps, ew, ewm, ewk)
            bad_e += same(got, py)
            # nodes: consecutive runs of 0-7 edges (empty nodes included)
            ptr, i = [0], 0
            while i < n:
                i = min(n, i + rng.randrange(1 if ps == 0 else 0, 8))
                ptr.append(i)
            own = np.array([math.nan if rng.random() < 0.3 else rng.random()
                            for _ in range(len(ptr) - 1)])
            py = [bc.leaf_node_value(list(zip(ws[a:z].tolist(), tot[a:z].tolist())),
                                     None if math.isnan(o) else float(o), sp, ow, ps, ew, ewm, ewk)
                  for a, z, o in zip(ptr, ptr[1:], own)]
            got = rs.leaf_node_values(np.array(ptr, dtype=np.int64), ws, tot, own,
                                      sp, ow, ps, ew, ewm, ewk)
            bad_n += same(got, py)
    check("L: leaf_edge_value, every blend and colour", bad_e == 0, f"{bad_e} differ")
    check("L: leaf_node_value, every blend and colour (empty nodes included)", bad_n == 0,
          f"{bad_n} differ")


GROUPS = ("term_normal", "term_other", "term_flag")


def check_aux() -> None:
    rng = random.Random(3)
    rows = []
    for _ in range(6000):
        if rng.random() < 0.1:
            rows.append(None)
            continue
        r = {}
        for gname in GROUPS:
            for c in ("total", "white_wins", "black_wins", "draws"):
                r[f"{gname}_{c}"] = rng.choice([None, 0, rng.randrange(0, 50), rng.random() * 9])
        for c in ("other_total", "other_white_wins", "other_draws", "horizon_total",
                  "horizon_white_wins", "horizon_draws", "other_eval_cov"):
            r[c] = rng.choice([None, 0, rng.randrange(0, 3000), rng.random() * 100])
        r["other_eval_cov"] = rng.choice([None, 0.0, 1.0, rng.random()])
        r["other_eval_mean"] = rng.choice([None, rng.random(), 0.0])
        rows.append(r)

    def f(r, k):
        return float(r.get(k) or 0.0) if r else 0.0

    tt = np.array([[f(r, f"{g}_total") for g in GROUPS] for r in rows])
    tw = np.array([[f(r, f"{g}_white_wins") for g in GROUPS] for r in rows])
    tb = np.array([[f(r, f"{g}_black_wins") for g in GROUPS] for r in rows])
    td = np.array([[f(r, f"{g}_draws") for g in GROUPS] for r in rows])
    other = np.array([[f(r, "other_total"), f(r, "other_white_wins"), f(r, "other_draws"),
                       (math.nan if not r or r.get("other_eval_mean") is None
                        else float(r["other_eval_mean"])),
                       f(r, "other_eval_cov"), f(r, "horizon_total"),
                       f(r, "horizon_white_wins"), f(r, "horizon_draws"),
                       0.0 if r is None else 1.0] for r in rows])
    bad_b = bad_m = 0
    for ow in (True, False):
        for ew, ewm, ewk in ((0.75, 0.0, 0.0), (0.0, 0.0, 0.0), (0.9, 0.1, 200.0)):
            for cov in (False, True):
                buckets, mix = rs.aux_mix(tt, tw, tb, td, other, ow, ew, ewm, ewk, cov)
                for i, r in enumerate(rows):
                    pyb = bc.aux_row_buckets(r, ow)
                    bad_b += same(buckets[i], pyb)
                    # (term_tot, term_sum, oth_*, hor_*): crack is not part of the mix
                    pym = aux_opp_mix(pyb[0], pyb[1], pyb[3], pyb[4], pyb[5], pyb[6],
                                      pyb[7], pyb[8], None, ew, ewm, ewk, cov_blend=cov)
                    bad_m += same(mix[i], pym)
    check("A: aux_row_buckets, both colours (missing rows and evals included)", bad_b == 0,
          f"{bad_b} differ")
    check("A: aux_opp_mix, both colours, both coverage rules, dynamic weights", bad_m == 0,
          f"{bad_m} differ")


def main() -> int:
    if rs is None:
        print("  SKIP  no book_builder_rs extension (build it: rust/book_builder/build.ps1)")
        return 0
    check_values()
    check_leaves()
    check_aux()
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
