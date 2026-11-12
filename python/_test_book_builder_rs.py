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
  R  optimistic_reach and route_plies on random graphs with transpositions, repetition
     cycles, childless edges, overlay reach shares and gated admission: both dicts in
     insertion order (it fixes the graph's node order), reach by bits.
  B  build_graph assembled in Rust and read back as a Graph (budget_rs.build_graph_rs), on
     random DAGs with every input kind (aux rows, overlay rows, source moves, engine
     proposals, vetoes, verified moves) and random flags: every node, candidate, kid and
     floor entry by bits and in order, the reach/ply/route dicts, the meta (disputes, crack
     edges, loss exceptions, overlay counts) and the 'other' bucket table.
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


# ── R: reach and route plies ────────────────────────────────────────────────

def random_edges(rng: random.Random, n: int):
    """edges_by_parent over positions 1..n (root 1): transpositions, back edges (repetition
    cycles), edges with no child, overlay rows carrying reach_share, tied shares."""
    ebp = {}
    for h in range(1, n + 1):
        if rng.random() < 0.15:
            continue                                   # a leaf: no recorded edges
        k = rng.randrange(0, 6)
        es = []
        overlay = rng.random() < 0.1
        for _ in range(k):
            r = rng.random()
            if r < 0.05:
                ch = None
            elif r < 0.15:
                ch = rng.randrange(1, h + 1)            # back edge: a cycle
            elif r < 0.2:
                ch = rng.randrange(1, n + 1) + n        # outside the parents (a leaf child)
            else:
                ch = rng.randrange(h + 1, min(n, h + 12) + 1) if h < n else None
            tot = rng.choice([1, 1, 2, 5, 10, 100, rng.randrange(0, 1000)])
            e = {"child_hash": ch, "total": tot, "move_san": f"m{len(es)}"}
            if overlay:
                e["reach_share"] = rng.choice([0.5, 0.25, rng.random()])
            es.append(e)
        ebp[h] = es
    return ebp


def reach_case(ebp, root_our, eps, max_ply, share_floor, admitted, track):
    parents = list(ebp)
    ptr, ch, has, tot, share = [0], [], [], [], []
    for h in parents:
        for e in ebp[h]:
            ch.append(0 if e["child_hash"] is None else e["child_hash"])
            has.append(e["child_hash"] is not None)
            tot.append(e["total"])
            share.append(e.get("reach_share", math.nan))
        ptr.append(len(ch))
    ap, ac = [], []
    if admitted is not None:
        ap = [0]
        for h in parents:
            ac += sorted(admitted.get(h, ()))
            ap.append(len(ac))
    i64 = np.int64
    got = rs.reach_batch(np.array(parents, i64), np.array(ptr, i64), np.array(ch, i64),
                         np.array(has, bool), np.array(tot, i64), np.array(share, np.float64),
                         np.array(ap, i64), np.array(ac, i64), 1, root_our, eps, max_ply,
                         share_floor, track)
    reach, ply = bc.optimistic_reach(ebp, 1, root_our, eps, max_ply, share_floor, admitted)
    route = bc.route_plies(ebp, 1, root_our, reach, ply, max_ply, share_floor,
                           admitted) if track else {}
    rk, rv, pk, pv, qk, qv = got
    ok = (list(reach) == rk.tolist() and same(list(reach.values()), rv) == 0
          and list(ply) == pk.tolist() and list(ply.values()) == pv.tolist()
          and list(route) == qk.tolist() and list(route.values()) == qv.tolist())
    return ok, len(reach), sum(1 for h in route if route[h] != ply[h])


def check_reach() -> None:
    rng = random.Random(5)
    bad = n = nodes = moved = 0
    for t in range(400):
        ebp = random_edges(rng, rng.choice([20, 60, 200, 1000]))
        adm = None
        if t % 2:
            adm = {h: {e["child_hash"] for e in es if e["child_hash"] is not None
                       and rng.random() < 0.7} for h, es in ebp.items() if rng.random() < 0.9}
        for root_our in (True, False):
            eps = rng.choice([1e-9, 1e-4, 0.01])
            max_ply = rng.choice([3, 8, 29, 60])
            sf = rng.choice([0.0, 0.002, 0.05])
            ok, k, m = reach_case(ebp, root_our, eps, max_ply, sf, adm, True)
            n += 1
            nodes += k
            moved += m
            bad += not ok
    check("R: optimistic_reach and route_plies = budget_core's (orders, reach bits, plies)",
          bad == 0, f"{n} graphs, {nodes:,} reached positions ({moved:,} with a route ply "
          f"other than the legacy ply), {bad} differ")


# ── B: the whole graph (build_graph) ────────────────────────────────────────

def fbits(x) -> int:
    return int(np.float64(x).view(np.int64))


def deep_same(a, b) -> bool:
    """Equal structure, floats by bits, dicts in the same insertion order."""
    if isinstance(a, float) or isinstance(b, float):
        return (isinstance(a, (float, int)) and isinstance(b, (float, int))
                and not isinstance(a, bool) and not isinstance(b, bool)
                and fbits(a) == fbits(b))
    if isinstance(a, dict):
        return (isinstance(b, dict) and list(a) == list(b)
                and all(deep_same(a[k], b[k]) for k in a))
    if isinstance(a, (list, tuple)):
        return (isinstance(b, (list, tuple)) and len(a) == len(b)
                and all(deep_same(x, y) for x, y in zip(a, b)))
    return a == b and type(a) is type(b)


def graph_diff(a, b) -> str | None:
    for name in ("reach", "ply", "route_ply", "epd", "other"):
        if not deep_same(getattr(a, name), getattr(b, name)):
            return name
    if list(a.our) != list(b.our):
        return "our order"
    for h in a.our:
        x, y = a.our[h], b.our[h]
        if not deep_same((x.l_node, x.cands, x.crack), (y.l_node, y.cands, y.crack)):
            return f"our {h}: {(x.l_node, x.cands, x.crack)} vs {(y.l_node, y.cands, y.crack)}"
    if list(a.opp) != list(b.opp):
        return "opp order"
    for h in a.opp:
        x, y = a.opp[h], b.opp[h]
        if not deep_same((x.const_base, x.kids, x.floor), (y.const_base, y.kids, y.floor)):
            return f"opp {h}: {(x.const_base, x.kids, x.floor)} vs {(y.const_base, y.kids, y.floor)}"
    ma = dict(a.meta)
    mb = dict(b.meta)
    da, db = ma.pop("rare_disputes", None), mb.pop("rare_disputes", None)
    if (da is None) != (db is None) or (da is not None and (
            list(da) != list(db) or any(not deep_same(dict(sorted(da[h].items())),
                                                      dict(sorted(db[h].items()))) for h in da))):
        return f"disputes: {da} vs {db}"
    if not deep_same(dict(sorted(ma.items())), dict(sorted(mb.items()))):
        return f"meta: {ma} vs {mb}"
    return None


def random_build_inputs(rng: random.Random, n: int, our_white: bool):
    """build_graph's inputs on a random DAG with transpositions and cycles."""
    keys = [rng.randrange(-2 ** 63, 2 ** 63) for _ in range(3 * n)]
    pos = keys[:n]
    root = pos[0]
    ebp = {}
    for i, h in enumerate(pos):
        if rng.random() < 0.1:
            continue
        overlay = rng.random() < 0.06
        es, seen = [], set()
        for s in rng.sample(SANS, rng.randrange(1, 6)):
            if bare(s) in seen:
                continue
            seen.add(bare(s))
            r = rng.random()
            if r < 0.04:
                ch = None
            elif r < 0.12:
                ch = pos[rng.randrange(0, i + 1)]                    # cycle / transposition
            elif r < 0.2:
                ch = keys[n + rng.randrange(0, 2 * n)]               # a leaf position
            else:
                ch = pos[rng.randrange(i + 1, min(n, i + 11))] if i + 1 < n else None
            e = {"move_san": s, "child_hash": ch,
                 "total": rng.choice([1, 2, 3, 5, 8, 20, 50, 400, rng.randrange(1, 3000)]),
                 "white_score_avg": rng.choice([0.5, 1.0, 0.0, rng.random()])}
            if not es:
                e["parent_epd"] = f"epd{i}"
            if overlay:
                e.update(reply_mass=rng.choice([float(e["total"]), rng.random() * 30]),
                         reach_share=rng.choice([None, rng.random()]),
                         additional_reply=rng.choice([True, False]))
                if e["reach_share"] is None:
                    del e["reach_share"]
            es.append(e)
        ebp[h] = es
    side = 1 if our_white else -1
    eval_ws = {}
    for k in keys:
        if rng.random() < 0.8:
            cp = side * rng.choice([0, 30, -30, 29, 31, 60, 100, -150, 300, rng.randrange(-500, 500)])
            eval_ws[k] = rng.choice([cp_to_expected_score(cp), rng.random()])
    aux = {}
    for h in ebp:
        if rng.random() < 0.6:
            r = {}
            for gname in GROUPS:
                for c in ("total", "white_wins", "black_wins", "draws"):
                    r[f"{gname}_{c}"] = rng.choice([None, 0, rng.randrange(0, 40)])
            for c in ("other_total", "other_white_wins", "other_draws", "horizon_total",
                      "horizon_white_wins", "horizon_draws"):
                r[c] = rng.choice([None, 0, rng.randrange(0, 500)])
            r["other_eval_cov"] = rng.choice([0.0, 1.0, rng.random()])
            r["other_eval_mean"] = rng.choice([None, rng.random()])
            aux[h] = r
    rep = {h: rng.choice([es[0]["move_san"], rng.choice(SANS)]) for h, es in ebp.items()
           if rng.random() < 0.5}
    eng = {}
    for h in ebp:
        if rng.random() < 0.1:
            eng[h] = [(s, rng.choice(keys)) for s in rng.sample(SANS, rng.randrange(1, 3))]
    verified = {h: {bare(s) for s in SANS if rng.random() < 0.2} for h in ebp if rng.random() < 0.2}
    excluded = {h: {bare(rng.choice(SANS))} for h in ebp if rng.random() < 0.05}
    child = {(f"epd{i}", s): rng.choice(keys + [None]) for i in range(n) for s in SANS}
    return ebp, root, eval_ws, aux, rep, eng, verified, excluded, child


def check_graph() -> None:
    import budget_rs
    import build_baseline_books as bbb
    rng = random.Random(6)
    bad = n = nodes = 0
    seen = dict(disputes=0, crack_edges=0, named=0, engine=0, rejected=0, other=0,
                loss_exceptions=0, floors=0, restored=0)
    first = None
    real = bbb.child_of
    try:
        for t in range(1000):
            our_white = t % 2 == 0
            ebp, root, eval_ws, aux, rep, eng, verified, excluded, child = random_build_inputs(
                rng, rng.choice([15, 40, 120, 400]), our_white)
            bbb.child_of = lambda epd, san: child[(epd, san)]
            kw = dict(rep_moves=rep, eval_ws=eval_ws,
                      aux_rows=aux if t % 5 else {},
                      slice_prior=rng.choice([0.5196, 0.48]),
                      eps=rng.choice([1e-6, 1e-4, 1e-3]), max_ply=rng.choice([4, 9, 29]),
                      share_floor=rng.choice([0.0, 0.002, 0.03]),
                      eval_weight=rng.choice([0.75, 0.5, 0.0]),
                      prior_strength=rng.choice([500.0, 100.0]),
                      other_cov_blend=rng.random() < 0.5, gated_reach=rng.random() < 0.7,
                      max_eval_loss=rng.choice([None, 30.0, 60.0]),
                      strict_source=rng.random() < 0.7, crack_at_parents=rng.random() < 0.7,
                      engine_moves=eng if rng.random() < 0.5 else None,
                      excluded=excluded if rng.random() < 0.5 else None,
                      rare_floor=rng.choice([None, 5]),
                      verified=verified if rng.random() < 0.7 else None,
                      track_route_ply=rng.random() < 0.5, track_other=rng.random() < 0.5)
            a = bc.build_graph(ebp, root, our_white, **kw)
            b = budget_rs.build_graph_rs(ebp, root, our_white, **kw)
            d = graph_diff(a, b)
            n += 1
            nodes += len(a.our) + len(a.opp)
            m = a.meta
            seen["disputes"] += len(m.get("rare_disputes", {}))
            seen["crack_edges"] += m.get("crack_edges", 0)
            seen["named"] += m.get("named_opponent_replies", {}).get("positions", 0)
            seen["engine"] += m.get("engine_cands", 0)
            seen["rejected"] += m.get("src_rejected", 0)
            seen["other"] += len(a.other)
            seen["loss_exceptions"] += len(m.get("loss_exceptions", {}))
            seen["floors"] += sum(len(x.floor) for x in a.opp.values())
            seen["restored"] += sum(1 for h, x in a.our.items() for c in x.cands
                                    if rep.get(h) == c[0] and not any(
                                        e["move_san"] == c[0] for e in ebp[h]))
            if d is not None:
                bad += 1
                if first is None:
                    first = (t, d[:600])
    finally:
        bbb.child_of = real
    check("B: build_graph (Rust CSR, back as a Graph) = budget_core's: nodes, candidates, "
          "kids, floors, reach/ply/route orders, meta, disputes, other",
          bad == 0, f"{n} graphs, {nodes:,} nodes, {bad} differ; exercised: "
          + ", ".join(f"{k} {v:,}" for k, v in seen.items()))
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
    check_reach()
    check_graph()
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
