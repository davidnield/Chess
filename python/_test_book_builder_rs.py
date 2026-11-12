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
  G  final_candidates over random our-nodes whose evals sit on every gate threshold (the
     absolute and relative floors, a loss of exactly 30 cp through cp -> score -> cp):
     kept moves in order, restored and rejected source moves, engine proposals and the
     rare-verification disputes, for every combination of the gates, require-eval, the
     loss limit, rare verification and strict source, both colours.
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


# ── G: the gates (final_candidates) ─────────────────────────────────────────

SANS = ["Nf3", "Nf3+", "e4", "e5", "d4", "d5", "Bb5", "Bb5+", "Qh5#", "O-O", "c4", "a3",
        "h6", "Nxe5", "Rxf7+", "g4"]


def bare(san: str) -> str:
    return san.rstrip("+#")


def gate_nodes(rng: random.Random, n: int, our_white: bool):
    """Random our-nodes whose evals sit on and around every gate threshold."""
    sp, rf, loss = 0.52, 0.1, 30.0
    side = 1 if our_white else -1
    # cps on an integer grid: through cp -> ws -> cp a loss of exactly 30 is the edge case
    def ws_of(cp):
        return cp_to_expected_score(cp)
    nodes = []
    for i in range(n):
        k = rng.randrange(0, 7)
        sans = rng.sample(SANS, k)
        # at most one of each bare move per node (a position has one Nf3)
        seen, es = set(), []
        for s in sans:
            if bare(s) in seen:
                continue
            seen.add(bare(s))
            base = rng.choice([0, 10, 29, 30, 31, -30, 60, 100, -200, 300,
                               rng.randrange(-400, 400)])
            cp = side * base
            ev = rng.choice([ws_of(cp), ws_of(cp), None,
                             sp - side * rf, sp + side * rf,            # abs-gate edges
                             rng.random()])
            tot = rng.choice([0, 1, 4, 5, 6, 20, 1000, rng.randrange(0, 50)])
            es.append({"move_san": s, "child_hash": 1000 * i + len(es),
                       "total": tot, "white_score_avg": rng.random(), "ev": ev})
        own = rng.choice([None, ws_of(side * rng.choice([0, 30, 50, -20, 300])), rng.random()])
        src = rng.choice([None, None] + [e["move_san"] for e in es]
                         + [s for s in SANS if bare(s) not in seen])
        eng = []
        for s in rng.sample(SANS, rng.randrange(0, 3)):
            if any(bare(s) == bare(e2) for e2, _ in eng):
                continue
            eng.append((s, rng.choice([ws_of(side * rng.choice([0, 30, 60, 200])), None,
                                       rng.random()])))
        verified = {bare(s) for s in SANS if rng.random() < 0.15}
        veto = {bare(s) for s in SANS if rng.random() < 0.08}
        src_eval = rng.choice([None, ws_of(side * rng.choice([0, 25, 30, 31, 90])), rng.random()])
        nodes.append(dict(h=i, es=es, own=own, src=src, eng=eng, verified=verified, veto=veto,
                          src_eval=src_eval))
    return nodes


def run_python(nodes, our_white, params, want_disputes):
    """budget_core.final_candidates per node, with child_of stubbed to a known child."""
    import build_baseline_books as bbb
    src_child = {}

    def fake_child_of(epd, san):
        return src_child[epd]
    real = bbb.child_of
    bbb.child_of = fake_child_of
    out = []
    try:
        for nd in nodes:
            eval_ws = {e["child_hash"]: e["ev"] for e in nd["es"] if e["ev"] is not None}
            if nd["own"] is not None:
                eval_ws[-1 - nd["h"]] = nd["own"]
            engine_moves = {}
            if nd["eng"]:
                engine_moves[-1 - nd["h"]] = [(s, 10 ** 9 + 100 * nd["h"] + j)
                                              for j, (s, _ev) in enumerate(nd["eng"])]
                for j, (s, ev) in enumerate(nd["eng"]):
                    if ev is not None:
                        eval_ws[10 ** 9 + 100 * nd["h"] + j] = ev
            sc = 2 * 10 ** 9 + nd["h"]
            src_child[f"epd{nd['h']}"] = sc
            if nd["src_eval"] is not None:
                eval_ws[sc] = nd["src_eval"]
            es = [{k: v for k, v in e.items() if k != "ev"} for e in nd["es"]]
            disputes = {} if want_disputes else None
            cand, restored, n_rej, n_eng = bc.final_candidates(
                -1 - nd["h"], es, nd["src"], eval_ws, our_white, params["slice_prior"],
                robustness_floor=params["robustness_floor"], gate_rel_floor=params["gate_rel_floor"],
                gate_rel_own_margin=params["gate_rel_own_margin"],
                require_eval=params["require_eval"], gates=params["gates"],
                max_eval_loss=params["max_eval_loss"], strict_source=params["strict_source"],
                engine_moves=engine_moves or None, epd=f"epd{nd['h']}",
                excluded={-1 - nd["h"]: nd["veto"]} if nd["veto"] else None,
                rare_floor=params["rare_floor"],
                verified={-1 - nd["h"]: nd["verified"]} if nd["verified"] else None,
                disputes=disputes)
            kept = [e["move_san"] for e, _cv in cand]
            out.append((kept, restored is not None, n_rej, n_eng,
                        (disputes or {}).get(-1 - nd["h"])))
    finally:
        bbb.child_of = real
    return out


def run_rust(nodes, our_white, params, want_disputes):
    sans = sorted(set(SANS))
    code = {s: i for i, s in enumerate(sans)}
    bcode = {b: i for i, b in enumerate(sorted({bare(s) for s in SANS}))}
    nan = math.nan
    own = [nan if nd["own"] is None else nd["own"] for nd in nodes]
    ss = [-1 if nd["src"] is None else code[nd["src"]] for nd in nodes]
    sb = [-1 if nd["src"] is None else bcode[bare(nd["src"])] for nd in nodes]
    se = [nan if nd["src_eval"] is None else nd["src_eval"] for nd in nodes]
    ep, esn, ebr, eev, etot = [0], [], [], [], []
    mp, msn, mbr, mev = [0], [], [], []
    vp, vb, xp, xb = [0], [], [0], []
    for nd in nodes:
        for e in nd["es"]:
            esn.append(code[e["move_san"]]); ebr.append(bcode[bare(e["move_san"])])
            eev.append(nan if e["ev"] is None else e["ev"]); etot.append(e["total"])
        ep.append(len(esn))
        for s, ev in nd["eng"]:
            msn.append(code[s]); mbr.append(bcode[bare(s)]); mev.append(nan if ev is None else ev)
        mp.append(len(msn))
        vb += sorted(bcode[b] for b in nd["verified"]); vp.append(len(vb))
        xb += sorted(bcode[b] for b in nd["veto"]); xp.append(len(xb))
    a = lambda x, t=np.float64: np.array(x, dtype=t)   # noqa: E731
    i64 = np.int64
    k_ptr, k_ref, restored, n_rej, n_eng, disputes = rs.final_candidates_batch(
        a(own), a(ss, i64), a(sb, i64), a(se),
        a(ep, i64), a(esn, i64), a(ebr, i64), a(eev), a(etot, i64),
        a(mp, i64), a(msn, i64), a(mbr, i64), a(mev),
        a(vp, i64), a(vb, i64), a(xp, i64), a(xb, i64),
        our_white, params["slice_prior"], params["robustness_floor"], params["gate_rel_floor"],
        params["gate_rel_own_margin"], params["require_eval"], params["gates"],
        nan if params["max_eval_loss"] is None else params["max_eval_loss"],
        -1 if params["rare_floor"] is None else params["rare_floor"],
        params["strict_source"], want_disputes)
    dmap = {d[0]: d for d in disputes}
    out = []
    for i in range(len(nodes)):
        kept = []
        for r in k_ref[k_ptr[i]:k_ptr[i + 1]]:
            kept.append(sans[esn[r]] if r >= 0 else sans[msn[-1 - r]])
        d = dmap.get(i)
        if d is not None:
            _i, ref, threats, risk, fb, unres = d
            dd = {"reference_cp": ref,
                  "threats": [(sans[c], cp) for c, cp in threats],
                  "at_risk": [sans[c] for c in risk]}
            if fb:
                dd["fallback"] = True
            if unres:
                dd["unresolved"] = True
            d = dd
        out.append((kept, bool(restored[i]), int(n_rej[i]), int(n_eng[i]), d))
    return out


def same_dispute(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if set(a) != set(b):
        return False
    for k in a:
        x, y = a[k], b[k]
        if k == "reference_cp":
            if (x is None) != (y is None) or (x is not None and same([x], [y])):
                return False
        elif k == "threats":
            if [s for s, _ in x] != [s for s, _ in y] or same([c for _, c in x], [c for _, c in y]):
                return False
        elif x != y:
            return False
    return True


def check_gates() -> None:
    rng = random.Random(4)
    n_nodes = bad = n_disp = n_rest = n_rej = 0
    first = None
    for our_white in (True, False):
        nodes = gate_nodes(rng, 3000, our_white)
        for gates_on, require_eval in ((True, True), (True, False), (False, True)):
            for loss in (None, 30.0):
                for rare in (None, 5):
                    for strict in (True, False):
                        for want in ((True, False) if rare is not None else (False,)):
                            params = dict(slice_prior=0.52, robustness_floor=0.1,
                                          gate_rel_floor=0.1, gate_rel_own_margin=0.02,
                                          require_eval=require_eval, gates=gates_on,
                                          max_eval_loss=loss, rare_floor=rare,
                                          strict_source=strict)
                            py = run_python(nodes, our_white, params, want)
                            got = run_rust(nodes, our_white, params, want)
                            for i, (a, b) in enumerate(zip(py, got)):
                                n_nodes += 1
                                n_disp += a[4] is not None
                                n_rest += a[1]
                                n_rej += a[2]
                                ok = a[:4] == b[:4] and same_dispute(a[4], b[4])
                                if not ok:
                                    bad += 1
                                    if first is None:
                                        first = (our_white, params, want, nodes[i], a, b)
    check("G: final_candidates (gates, loss limit, rare verification, strict source, "
          "engine moves, vetoes) = budget_core's", bad == 0,
          f"{n_nodes:,} node solves ({n_disp:,} disputes, {n_rest:,} restored, "
          f"{n_rej:,} rejected sources), {bad} differ")
    if first is not None:
        print("    first difference:", first)


def main() -> int:
    if rs is None:
        print("  SKIP  no book_builder_rs extension (build it: rust/book_builder/build.ps1)")
        return 0
    check_values()
    check_leaves()
    check_aux()
    check_gates()
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
