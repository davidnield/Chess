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
  D  the graph owned by Rust (RsGraph) on the DP and price tests' graphs, cyclic graphs,
     big DAGs and build_graph's own graphs, with and without stickiness: Python's set
     order rebuilt from key lists, topo_up (order and cut set), topo_down, the stop
     positions, apply_stop_values, every node's curve at two caps, extract_book at
     several budgets (every dict in insertion order) and match_distinct with and
     without probe curves.
  P  book_result and polish_book on the same graphs, from the DP's book and from random
     over-budget books (trims), with and without stickiness: the book in insertion
     order and every statistic, against the dict path and (no stickiness) the
     compiled one.
  S  price_search and policy_reach on the price and DP tests' graphs: optimistic and
     seeded reach, polish on and off, stickiness; the book, its reach and values, lam,
     the sweep, the nearest sizes, the duality bound and every polish's stats.
  C  the columnar path on random build inputs: build_graph_csr's input arrays made from
     the collector's frame equal the dict path's, and the graph view and the RsGraph made
     straight from the Rust output behave as the Graph and its RsGraph do.
  T  the sampled tables on the Rust graph: the stop dump, the other dump, the other table's
     application and the finished-game outcomes equal sampled_stop's / sampled_other's.
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


# ── D: the graph owned by Rust: orders, stop values, the curve engine ───────────────────────

def dp_graphs(rng: random.Random):
    """(label, Graph, budgets): the DP and price tests' graphs, cyclic graphs, big DAGs, and
    graphs build_graph assembles from random_build_inputs (real candidate shapes)."""
    import build_baseline_books as bbb
    import _test_budget_dag as tdag
    import _test_budget_speed as tspeed
    out = [(f"report{t}", g, b) for t, (g, b) in enumerate(tdag.report_instances(n_graphs=120))]
    out += [(f"E{t}", g, [2, 5]) for t, (g, _b) in enumerate(tdag.e_instances())]
    out += [(f"NP{t}", g, [2, 5]) for t, g in enumerate(tdag.np_graphs())]
    out += [("D", tdag.case_d(), [1, 2, 3]), ("F50", tdag.case_f(50), [1, 2, 3]),
            ("F5", tdag.case_f(5), [1, 2, 3]), ("T8", tdag.case_t8(), [1, 2])]
    out += [(f"cyc{t}", tspeed.cyclic_graph(rng, rng.randint(5, 120)), [1, 3, 8])
            for t in range(150)]
    out += [(f"big{t}", tdag.big_dag(rng, n), [10, 60, 300]) for t, n in enumerate([800, 3000])]
    real = bbb.child_of
    try:
        for t in range(60):
            our_white = t % 2 == 0
            ebp, root, eval_ws, aux, rep, eng, verified, excluded, child = random_build_inputs(
                rng, rng.choice([40, 120, 400]), our_white)
            bbb.child_of = lambda epd, san: child[(epd, san)]
            g = bc.build_graph(ebp, root, our_white, rep_moves=rep, eval_ws=eval_ws,
                               aux_rows=aux, slice_prior=0.5196, eps=1e-6, max_ply=29,
                               share_floor=rng.choice([0.0, 0.03]), gated_reach=True,
                               max_eval_loss=30.0, strict_source=True,
                               crack_at_parents=t % 3 == 0, engine_moves=eng)
            out.append((f"built{t}", g, [1, 5, 20, 80]))
    finally:
        bbb.child_of = real
    return out


def stick(g, rng: random.Random) -> None:
    """a random incumbent: moves at a third of the our nodes (some unknown), a margin"""
    moves = {}
    for h, n in g.our.items():
        if n.cands and rng.random() < 0.33:
            moves[h] = rng.choice([bare(rng.choice(n.cands)[0]), "Zz9"])
    g.meta["stick"] = (moves, rng.choice([0.0, 0.01, 0.05]))


def curve_same(c, r) -> bool:
    return (r is not None and fbits(c.base) == fbits(r[0])
            and c.costs.tolist() == r[1].tolist()
            and bits(c.gains).tolist() == bits(r[2]).tolist())


def book_same(a: dict, b: dict) -> str | None:
    for k in ("booked", "alloc", "realized"):
        if not deep_same(list(a[k].items()), list(b[k].items())):
            return k
    for k in ("visited", "spent_paths", "spent_distinct", "root_value_curve",
              "root_value_realized"):
        if not deep_same(a[k], b[k]):
            return k
    return None


def check_dp() -> None:
    import copy
    import budget_rs
    rng = random.Random(11)
    bad = {k: 0 for k in ("set order", "topo_up", "topo_down", "stop positions",
                          "stop values", "curves", "extract", "match")}
    first: dict = {}
    n_graphs = n_nodes = n_books = n_cut = n_flat = n_stick = n_probe = 0

    def fail(k, what):
        bad[k] += 1
        first.setdefault(k, what)

    for label, g, budgets in dp_graphs(rng):
        g.meta = dict(g.meta or {})
        if rng.random() < 0.4:
            stick(g, rng)
            n_stick += 1
        n_graphs += 1
        n_nodes += len(g.our) + len(g.opp)
        so = budget_rs.set_order(list(g.our), list(g.opp))
        if so.tolist() != list(set(g.our) | set(g.opp)):
            fail("set order", label)
        r, sans = budget_rs.rs_graph(g)
        o, e = r.topo_up()
        po, pe = bc.topo_up(g)
        if o.tolist() != po or set(e.tolist()) != pe or len(e) != len(pe):
            fail("topo_up", label)
        n_cut += bool(pe)
        if r.topo_down().tolist() != bc.topo_down(g):
            fail("topo_down", label)
        xs, ys = bc.stop_model_positions(g)
        rx, ry = r.stop_positions()
        if rx.tolist() != sorted(xs) or ry.tolist() != sorted(ys):
            fail("stop positions", label)
        # stop values on most stop positions, some missing
        W = {h: rng.random() for h in xs if rng.random() < 0.8}
        R = {h: rng.random() for h in ys if rng.random() < 0.8}
        g2 = copy.deepcopy(g)
        pc = bc.apply_stop_values(g2, W, R)
        rc = r.apply_stop_values(np.array(list(W), np.int64), np.array(list(W.values())),
                                 np.array(list(R), np.int64), np.array(list(R.values())))
        want, _s, _b = budget_rs.graph_to_csr(g2)
        got = r.values()
        if pc != dict(rc) or any(bits(want[k]).tolist() != bits(got[k]).tolist()
                                 for k in ("l_node", "c_lv", "k_lv", "const_base")):
            fail("stop values", (label, pc, dict(rc)))
        g = g2
        for bmax, k_atoms in ((max(budgets) + 1, 256), (3 * max(budgets) + 5, 4)):
            curves, diag = bc.build_curves(g, bmax=bmax, k_atoms=k_atoms)
            sid, flat = r.build_curves(bmax, k_atoms)
            n_flat += flat
            if flat != diag["stranded_cycle_nodes"] or any(
                    not curve_same(c, r.curve(sid, h)) for h, c in curves.items()):
                fail("curves", (label, bmax, k_atoms))
                continue
            for b in budgets + [0, bmax]:
                n_books += 1
                d = book_same(bc.extract_book(g, curves, b),
                              budget_rs.book_from_rs(r.extract_book(sid, b), sans))
                if d is not None:
                    fail("extract", (label, b, d))
            pbmax = 2 * bmax
            probe, _ = bc.build_curves(g, bmax=pbmax, k_atoms=k_atoms)
            pid, _ = r.build_curves(pbmax, k_atoms)
            for target in budgets:
                for kw, rkw in (({}, {}),
                                (dict(probe_curves=probe, plain_bmax=bmax),
                                 dict(probe_set=pid, plain_bmax=bmax)),
                                (dict(probe_curves=probe, scan_cap=2),
                                 dict(probe_set=pid, scan_cap=2))):
                    bm = pbmax if kw else bmax
                    pa = bc.match_distinct(g, curves, target, bm, **kw)
                    rb = r.match_distinct(sid, target, bm, **rkw)
                    n_probe += pa[1] != target
                    if (tuple(pa[1:]) != tuple(rb[1:]) or
                            book_same(pa[0], budget_rs.book_from_rs(rb[0], sans)) is not None):
                        fail("match", (label, target, list(kw), pa[1:], rb[1:]))
    check("D: RsGraph = budget_core: set order, topo_up (order, cuts), topo_down, stop "
          "positions and values, every curve, extract_book, match_distinct (probe curves, "
          "plain bmax, scan cap), stickiness",
          not any(bad.values()),
          f"{n_graphs} graphs, {n_nodes:,} nodes, {n_books} extractions; exercised: {n_cut} "
          f"with cycles cut, {n_flat} flattened curves, {n_stick} sticky, {n_probe} matches "
          f"charged above the target; differ: {bad}")
    for k, v in first.items():
        print(f"    first {k} difference: {v}")


# ── P: the region evaluator and polish_book ───────────────────────────────────────────────

def rs_moves(moves: dict, sans: list[str]):
    code = {s: i for i, s in enumerate(sans)}
    return (np.array(list(moves), np.int64),
            np.array([code.get(s, -1) for s in moves.values()], np.int64))


def check_polish() -> None:
    import budget_compiled
    import budget_rs
    rng = random.Random(23)
    bad = {"book_result": 0, "polish": 0, "compiled": 0}
    first: dict = {}
    n = n_polish = n_trim = n_sticky = n_moved = n_compiled = n_open = 0
    kinds: dict = {}

    def fail(k, what):
        bad[k] += 1
        first.setdefault(k, what)

    for label, g, budgets in dp_graphs(rng):
        g.meta = dict(g.meta or {})
        nodes = set(g.our) | set(g.opp)
        if g.root not in nodes or any(
                c[2] and c[1] is not None and c[1] not in nodes
                for x in g.our.values() for c in x.cands) or any(
                k[2] and k[1] is not None and k[1] not in nodes
                for x in g.opp.values() for k in x.kids):
            n_open += 1                  # the region walk needs a closed graph (Python too)
            continue
        if rng.random() < 0.3:
            stick(g, rng)
        n += 1
        r, sans = budget_rs.rs_graph(g)
        order = bc.topo_up(g)[0]
        pos = {h: i for i, h in enumerate(order)}
        curves, _ = bc.build_curves(g, bmax=max(budgets) + 1, k_atoms=256)
        st = g.meta.get("stick")
        sticky = bool(st and st[1] and any(
            h in g.our and any(c[0].rstrip("+#") == im for c in g.our[h].cands)
            for h, im in st[0].items()))
        cg = budget_compiled.compile_graph(g) if not sticky else None
        for target in budgets:
            starts = [bc.extract_book(g, curves, target)["booked"]]
            # an over-budget random book: trims first
            rnd = {h: rng.choice(x.cands)[0] for h, x in g.our.items()
                   if x.cands and rng.random() < 0.6}
            starts.append(rnd)
            for moves in starts:
                a = bc.book_result(g, moves, order)
                b = r.book_result(*rs_moves(moves, sans))
                got = {"booked": dict(zip(b["booked_keys"].tolist(),
                                          (sans[c] for c in b["booked_san"].tolist()))),
                       "visited": b["visited"].tolist(),
                       "realized": dict(zip(b["visited"].tolist(), b["realized_vals"].tolist())),
                       "reach_policy": dict(zip(b["w_keys"].tolist(), b["w_vals"].tolist())),
                       "root_value_realized": b["value"]}
                if any(not deep_same(list(a[k].items()) if isinstance(a[k], dict) else a[k],
                                     list(got[k].items()) if isinstance(got[k], dict) else got[k])
                       for k in got):
                    fail("book_result", (label, target))
                n_polish += 1
                pm, ps = bc.polish_book(g, moves, target, order)
                rk, rsn, rst = r.polish(*rs_moves(moves, sans), target)
                rm = dict(zip(rk.tolist(), (sans[c] for c in rsn.tolist())))
                ps = {k: v for k, v in ps.items() if k != "seconds"}
                rst = {k: v for k, v in dict(rst).items() if k != "seconds"}
                n_trim += ps["trims"] > 0
                n_sticky += "objective" in ps
                n_moved += ps["accepted"] > 0
                for k, v in ps["by_kind"].items():
                    kinds[k] = kinds.get(k, 0) + v
                if (list(pm.items()) != list(rm.items()) or not deep_same(
                        {k: ps[k] for k in sorted(ps)}, {k: rst.get(k) for k in sorted(ps)})
                        or list(ps["by_kind"]) != list(rst["by_kind"]) or set(rst) != set(ps)):
                    fail("polish", (label, target, len(moves), ps, rst))
                if cg is not None and cg.ok:
                    n_compiled += 1
                    cm, cs_ = bc.polish_book(g, moves, target, cg.order, compiled=cg)
                    if list(cm.items()) != list(rm.items()):
                        fail("compiled", (label, target))
    check("P: RsGraph book_result and polish_book = budget_core's (dict path and compiled "
          "path): the book in insertion order, value, size, trims, steps, moves by kind, the "
          "sticky objective",
          not any(bad.values()),
          f"{n} graphs ({n_open} not closed, skipped), {n_polish} polishes ({n_trim} trimmed, {n_moved} improved, "
          f"{n_sticky} sticky, {n_compiled} also on the compiled path); moves by kind {kinds}; "
          f"differ: {bad}")
    for k, v in first.items():
        print(f"    first {k} difference: {str(v)[:1500]}")


# ── S: the price search ────────────────────────────────────────────────────────────────────

def closed(g) -> bool:
    nodes = set(g.our) | set(g.opp)
    return g.root in nodes and not any(
        c[2] and c[1] is not None and c[1] not in nodes
        for x in g.our.values() for c in x.cands) and not any(
        k[2] and k[1] is not None and k[1] not in nodes
        for x in g.opp.values() for k in x.kids)


def no_seconds(info: dict) -> dict:
    out = {k: v for k, v in info.items() if k != "timing"}
    if "polish" in out:
        out["polish"] = [{k: v for k, v in p.items() if k != "seconds"} for p in out["polish"]]
    return out


def check_price() -> None:
    import _test_budget_dag as tdag
    import _test_budget_speed as tspeed
    import budget_rs
    rng = random.Random(31)
    graphs = [(n, g, b) for n, g, b in tspeed.price_graphs()] + dp_graphs(rng)
    bad = {"policy_reach": 0, "search": 0}
    first: dict = {}
    n = n_search = n_sticky = n_open = 0
    sizes_hit = 0

    def fail(k, what):
        bad[k] += 1
        first.setdefault(k, what)

    quiet = lambda *_a, **_k: None  # noqa: E731
    for label, g, budgets in graphs:
        g.meta = dict(g.meta or {})
        if not closed(g):
            n_open += 1
            continue
        if rng.random() < 0.3:
            stick(g, rng)
            n_sticky += 1
        n += 1
        r, sans = budget_rs.rs_graph(g)
        order = bc.topo_up(g)[0]
        book = {h: rng.choice(x.cands)[0] for h, x in g.our.items()
                if x.cands and rng.random() < 0.7}
        if not deep_same(list(bc.policy_reach(g, book, order).items()),
                         list(budget_rs.policy_reach_rs(r, sans, book).items())):
            fail("policy_reach", label)
        seed = tdag.source_seed(g, order)
        for target in budgets:
            for kw in (dict(seed_reach=seed), {}, dict(seed_reach=seed, polish=False)):
                n_search += 1
                a, ai = bc.price_search(g, target, log=quiet, **kw)
                b, bi = budget_rs.price_search_rs(r, sans, target, log=quiet, **kw)
                sizes_hit += a["spent_distinct"] == target
                keys = ("booked", "visited", "realized", "reach_policy", "root_value_realized",
                        "root_value_penalized", "lam", "spent_distinct")
                if any(not deep_same(list(a[k].items()) if isinstance(a[k], dict) else a[k],
                                     list(b[k].items()) if isinstance(b[k], dict) else b[k])
                       for k in keys) or not deep_same(
                        {k: v for k, v in sorted(no_seconds(ai).items())},
                        {k: v for k, v in sorted(no_seconds(bi).items())}):
                    fail("search", (label, target, list(kw), no_seconds(ai), no_seconds(bi)))
    check("S: RsGraph price_search and policy_reach = budget_core's: the book, its reach "
          "and values, lam, the sweep, the nearest sizes, the bound, every polish's stats; "
          "optimistic and seeded reach, polish on and off, stickiness",
          not any(bad.values()),
          f"{n} graphs ({n_open} not closed, skipped; {n_sticky} sticky), {n_search} searches "
          f"({sizes_hit} landing exactly on the target); differ: {bad}")
    for k, v in first.items():
        print(f"    first {k} difference: {str(v)[:2000]}")


# ── C: the columnar path (the collector's frame straight to Rust) ──────────────────────────

def frame_of(ebp: dict):
    """edges_by_parent as the collector's frame (no opponent-prior columns)"""
    rows = []
    for h, es in ebp.items():
        for i, e in enumerate(es):
            rows.append({"parent_hash": h, "move_san": e["move_san"],
                         "child_hash": e.get("child_hash"),
                         "parent_epd": e.get("parent_epd") if i == 0 else None,
                         "total": e["total"], "white_score_avg": e.get("white_score_avg")})
    return pl.DataFrame(rows, schema={"parent_hash": pl.Int64, "move_san": pl.String,
                                      "child_hash": pl.Int64, "parent_epd": pl.String,
                                      "total": pl.Int64, "white_score_avg": pl.Float64})


def arrays_same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if set(a) != set(b):
        return False
    for k in a:
        x, y = np.asarray(a[k]), np.asarray(b[k])
        if x.shape != y.shape or x.dtype.kind != y.dtype.kind:
            return False
        if x.dtype.kind == "f":
            if bits(x.reshape(-1)).tolist() != bits(y.reshape(-1)).tolist():
                return False
        elif x.tolist() != y.tolist():
            return False
    return True


def check_columns() -> None:
    import budget_rs
    import build_baseline_books as bbb
    rng = random.Random(41)
    bad = {"inputs": 0, "view": 0, "rsgraph": 0}
    first: dict = {}
    n = nodes = 0
    real = bbb.child_of
    quiet = lambda *_a, **_k: None  # noqa: E731

    def fail(k, what):
        bad[k] += 1
        first.setdefault(k, what)
    budget_rs.KEEP_INPUTS = True
    try:
        for t in range(300):
            our_white = t % 2 == 0
            ebp, root, eval_ws, aux, rep, eng, verified, excluded, child = random_build_inputs(
                rng, rng.choice([15, 40, 120, 400]), our_white)
            for es in ebp.values():               # no opponent-prior rows on this path
                for e in es:
                    for k in ("reply_mass", "reach_share", "additional_reply"):
                        e.pop(k, None)
            ebp = {h: es for h, es in ebp.items() if es}
            if root not in ebp:
                continue
            bbb.child_of = lambda epd, san: child[(epd, san)]
            kw = dict(rep_moves=rep, eval_ws=eval_ws, aux_rows=aux if t % 5 else {},
                      slice_prior=0.5196, eps=rng.choice([1e-6, 1e-4]), max_ply=29,
                      share_floor=rng.choice([0.0, 0.03]), gated_reach=rng.random() < 0.7,
                      max_eval_loss=rng.choice([None, 30.0]), strict_source=rng.random() < 0.7,
                      crack_at_parents=rng.random() < 0.7,
                      engine_moves=eng if rng.random() < 0.5 else None,
                      excluded=excluded if rng.random() < 0.5 else None,
                      rare_floor=rng.choice([None, 5]),
                      verified=verified if rng.random() < 0.7 else None)
            g = budget_rs.build_graph_rs(ebp, root, our_white, **kw)
            want = dict(budget_rs.LAST_INPUTS)
            af = None
            if kw["aux_rows"]:
                af = pl.DataFrame([{"position_hash": h, **r} for h, r in kw["aux_rows"].items()],
                                  infer_schema_length=None)
            got = budget_rs.frame_inputs(frame_of(ebp), rep_moves=rep, engine_moves=kw["engine_moves"],
                                         excluded=kw["excluded"], verified=kw["verified"],
                                         strict_source=kw["strict_source"], aux_frame=af)
            n += 1
            nodes += len(g.our) + len(g.opp)
            if (got["san_list"] != want["san_list"] or not arrays_same(got["edges"], want["edges"])
                    or not arrays_same(got["rows"], want["rows"])
                    or not arrays_same(got["aux"], want["aux"])):
                fail("inputs", (t, [k for k in ("edges", "rows", "aux") if not arrays_same(got[k], want[k])]))
                continue
            out = budget_rs.run_csr(
                got["edges"], got["rows"], got["aux"], want["ek"], want["ev"], root, our_white,
                slice_prior=0.5196, prior_strength=bc.PRIOR_STRENGTH, eval_weight=bc.EVAL_WEIGHT,
                eval_weight_min=bc.EVAL_WEIGHT_MIN, eval_weight_k=bc.EVAL_WEIGHT_K,
                eps=kw["eps"], max_ply=29, share_floor=kw["share_floor"],
                robustness_floor=bc.ROBUSTNESS_FLOOR, gate_rel_floor=bc.GATE_REL_FLOOR,
                gate_rel_own_margin=bc.GATE_REL_OWN_MARGIN, require_eval=True, gates=True,
                max_eval_loss=kw["max_eval_loss"], rare_floor=kw["rare_floor"],
                strict_source=kw["strict_source"], other_cov_blend=False,
                gated_reach=kw["gated_reach"], crack_at_parents=kw["crack_at_parents"],
                engine_moves_given=bool(kw["engine_moves"]), track_route_ply=False,
                track_other=False)
            v = budget_rs.GraphView(out, got["san_list"], root, got["parents"], got["epd"],
                                    eps=kw["eps"], max_ply=29, share_floor=kw["share_floor"],
                                    slice_prior=0.5196, our_white=our_white,
                                    strict_source=kw["strict_source"],
                                    crack_at_parents=kw["crack_at_parents"])
            if (sorted(v.our) != sorted(g.our) or sorted(v.opp) != sorted(g.opp)
                    or not deep_same(list(v.reach.items()), list(g.reach.items()))
                    or not deep_same(list(v.ply.items()), list(g.ply.items()))
                    or any(v.epd.get(h) != g.epd.get(h) for h in g.epd)
                    or not deep_same(dict(sorted((k, x) for k, x in v.meta.items() if k != "rare_disputes")),
                                     dict(sorted((k, x) for k, x in g.meta.items() if k != "rare_disputes")))
                    or list(v.meta.get("rare_disputes", {})) != list(g.meta.get("rare_disputes", {}))):
                fail("view", t)
            if not g.our:
                continue
            g.meta = dict(g.meta)
            stick_ = None
            if rng.random() < 0.4:
                stick(g, rng)
                stick_ = g.meta["stick"]
            r1, s1 = budget_rs.rs_graph(g)
            r2, s2 = budget_rs.rs_graph_from_out(out, got["san_list"], root,
                                                 crack_at_parents=kw["crack_at_parents"], stick=stick_)
            same = (r1.topo_up()[0].tolist() == r2.topo_up()[0].tolist()
                    and r1.topo_down().tolist() == r2.topo_down().tolist()
                    and all(a.tolist() == b.tolist() for a, b in zip(r1.stop_positions(), r2.stop_positions())))
            if same:
                c1, _ = r1.build_curves(40, 256)
                c2, _ = r2.build_curves(40, 256)
                for b in (1, 5, 20):
                    same &= book_same(budget_rs.book_from_rs(r1.extract_book(c1, b), s1),
                                      budget_rs.book_from_rs(r2.extract_book(c2, b), s2)) is None
                if closed(g):
                    a1, i1 = budget_rs.price_search_rs(r1, s1, 5, log=quiet)
                    a2, i2 = budget_rs.price_search_rs(r2, s2, 5, log=quiet)
                    same &= list(a1["booked"].items()) == list(a2["booked"].items()) and deep_same(
                        no_seconds(i1), no_seconds(i2))
            if not same:
                fail("rsgraph", t)
    finally:
        bbb.child_of = real
        budget_rs.KEEP_INPUTS = False
    check("C: the columnar path = the dict path: build_graph_csr's input arrays from the "
          "collector's frame, the graph view (node sets, reach, ply, EPDs, meta) and the "
          "RsGraph made straight from the Rust output (orders, curves, books, price search)",
          not any(bad.values()), f"{n} graphs, {nodes:,} nodes; differ: {bad}")
    for k, v in first.items():
        print(f"    first {k} difference: {v}")


# ── T: the sampled tables on the Rust graph ───────────────────────────────────────────────

def check_sampled() -> None:
    import copy
    import budget_rs
    import build_baseline_books as bbb
    import sampled_other as so
    import sampled_stop as ss
    rng = random.Random(53)
    bad = {"stop dump": 0, "other dump": 0, "other apply": 0, "terminal": 0}
    first: dict = {}
    n = n_rows = n_other = n_applied = 0
    real = bbb.child_of
    fit0 = so.fit

    def fail(k, what):
        bad[k] += 1
        first.setdefault(k, what)
    try:
        for t in range(150):
            our_white = t % 2 == 0
            ebp, root, eval_ws, aux, rep, eng, verified, excluded, child = random_build_inputs(
                rng, rng.choice([120, 400, 1500]), our_white)
            for es in ebp.values():
                for e in es:
                    for k in ("reply_mass", "reach_share", "additional_reply"):
                        e.pop(k, None)
            ebp = {h: es for h, es in ebp.items() if es}
            if root not in ebp:
                continue
            bbb.child_of = lambda epd, san: child[(epd, san)]
            kw = dict(rep_moves=rep, eval_ws=eval_ws, aux_rows=aux, slice_prior=0.5196,
                      eps=1e-6, max_ply=rng.choice([9, 29]), share_floor=rng.choice([0.0, 0.03]),
                      gated_reach=True, max_eval_loss=30.0, strict_source=True,
                      crack_at_parents=True, engine_moves=eng if t % 3 else None,
                      track_route_ply=True, track_other=True)
            g = bc.build_graph(ebp, root, our_white, **kw)
            if not g.our:
                continue
            af = pl.DataFrame([{"position_hash": h, **r} for h, r in aux.items()],
                              infer_schema_length=None)
            fi = budget_rs.frame_inputs(frame_of(ebp), rep_moves=rep, engine_moves=kw["engine_moves"],
                                        strict_source=True, aux_frame=af)
            ek = np.fromiter(eval_ws.keys(), np.int64, len(eval_ws))
            ev = np.fromiter(eval_ws.values(), np.float64, len(eval_ws))
            out = budget_rs.run_csr(
                fi["edges"], fi["rows"], fi["aux"], ek, ev, root, our_white, slice_prior=0.5196,
                prior_strength=bc.PRIOR_STRENGTH, eval_weight=bc.EVAL_WEIGHT,
                eval_weight_min=bc.EVAL_WEIGHT_MIN, eval_weight_k=bc.EVAL_WEIGHT_K, eps=1e-6,
                max_ply=kw["max_ply"], share_floor=kw["share_floor"],
                robustness_floor=bc.ROBUSTNESS_FLOOR, gate_rel_floor=bc.GATE_REL_FLOOR,
                gate_rel_own_margin=bc.GATE_REL_OWN_MARGIN, require_eval=True, gates=True,
                max_eval_loss=30.0, rare_floor=None, strict_source=True, other_cov_blend=False,
                gated_reach=True, crack_at_parents=True, engine_moves_given=bool(kw["engine_moves"]),
                track_route_ply=True, track_other=True)
            view = budget_rs.GraphView(out, fi["san_list"], root, fi["parents"], fi["epd"],
                                       eps=1e-6, max_ply=kw["max_ply"],
                                       share_floor=kw["share_floor"], slice_prior=0.5196,
                                       our_white=our_white, strict_source=True,
                                       crack_at_parents=True)
            rsg, _sans = budget_rs.rs_graph_from_out(out, fi["san_list"], root, crack_at_parents=True)
            n += 1
            try:
                want = ss.stop_positions(g)
            except ValueError as e:
                want = str(e)
            try:
                got = ss.stop_positions_rs(rsg, view)
            except ValueError as e:
                got = str(e)
            if isinstance(want, str) or isinstance(got, str):
                if not (isinstance(want, str) and isinstance(got, str)):
                    fail("stop dump", (t, want if isinstance(want, str) else "ok",
                                       got if isinstance(got, str) else "ok"))
            elif not want.equals(got):
                fail("stop dump", (t, want.height, got.height))
            else:
                n_rows += want.height
            if g.other:
                n_other += 1
                a_, b_ = so.other_positions(g), so.other_positions_view(view)
                if not a_.equals(b_):
                    fail("other dump", t)
                # a sampled table as sampled_other writes it: the dump's rows, V0 and its SE,
                # a tenth of them sampled at another ply (refused)
                tab = a_.with_columns(
                    pl.Series("v0", [rng.random() for _ in range(a_.height)]),
                    pl.Series("se", [rng.random() * 0.1 for _ in range(a_.height)]),
                    pl.Series("ply", [p + (rng.random() < 0.1) for p in a_["ply"].to_list()],
                              dtype=pl.Int32))
                g2 = copy.deepcopy(g)
                if t % 2:
                    # these graphs carry fewer than the 50 buckets a calibration needs: repeat the
                    # rows (the same least-squares solution) so the application runs too
                    so.fit = lambda tb, _f=fit0: _f(pl.concat([tb] * (50 // max(tb.height, 1) + 1)))
                else:
                    so.fit = fit0
                try:
                    ra = so.apply(g2, tab)
                except ValueError as e:
                    ra = str(e)
                try:
                    rb = so.apply_rs(view, rsg, tab)
                except ValueError as e:
                    rb = str(e)
                if isinstance(ra, str) != isinstance(rb, str) or (
                        not isinstance(ra, str) and (not deep_same(dict(sorted(ra.items())), dict(sorted(rb.items())))
                                                     or bits(budget_rs.graph_to_csr(g2)[0]["const_base"]).tolist()
                                                     != bits(rsg.values()["const_base"]).tolist())):
                    fail("other apply", (t, ra if isinstance(ra, str) else "ok", rb if isinstance(rb, str) else "ok"))
                elif not isinstance(ra, str):
                    n_applied += 1
    finally:
        bbb.child_of = real
        so.fit = fit0
    # terminal outcomes on real positions: a mate with and without its '#', a stalemate,
    # insufficient material, an ordinary move, an illegal move, a child that is a parent
    import chess
    fools = chess.Board()
    for m in ("f3", "e5", "g4"):
        fools.push_san(m)
    stale = chess.Board("7k/5K2/8/6Q1/8/8/8/8 w - - 0 1")    # Qg6 stalemates
    bare = chess.Board("8/8/8/8/8/6k1/1r6/K7 w - - 0 1")
    ebp2 = {101: [{"move_san": "Qh4#", "child_hash": 1, "total": 3, "white_score_avg": 0.0,
                   "parent_epd": fools.epd()},
                  {"move_san": "Nc6", "child_hash": 2, "total": 3, "white_score_avg": 0.5}],
            102: [{"move_san": "Qh4", "child_hash": 3, "total": 3, "white_score_avg": 0.0,
                   "parent_epd": fools.epd()},
                  {"move_san": "Zz9", "child_hash": 4, "total": 1, "white_score_avg": 0.5}],
            103: [{"move_san": "Qg6", "child_hash": 5, "total": 2, "white_score_avg": 0.5,
                   "parent_epd": stale.epd()},
                  {"move_san": "Qd8+", "child_hash": 101, "total": 2, "white_score_avg": 0.5}],
            104: [{"move_san": "Kxb2", "child_hash": 6, "total": 2, "white_score_avg": 0.5,
                   "parent_epd": bare.epd()}]}
    for ow in (True, False):
        fi2 = budget_rs.frame_inputs(frame_of(ebp2))
        if ss.terminal_outcomes(ebp2, ow) != ss.terminal_outcomes_rs(fi2, ow):
            fail("terminal", (ow, ss.terminal_outcomes(ebp2, ow), ss.terminal_outcomes_rs(fi2, ow)))
    check("T: the sampled tables on the Rust graph = sampled_stop / sampled_other on the Graph: "
          "the stop dump (rows, order, plies, refusals past the horizon), the other dump, the "
          "other table's application (report and every const_base), finished-game outcomes",
          not any(bad.values()),
          f"{n} graphs, {n_rows:,} stop rows, {n_other} with other buckets, {n_applied} "
          f"calibrated; differ: {bad}")
    for k, v in first.items():
        print(f"    first {k} difference: {str(v)[:800]}")


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
    check_dp()
    check_polish()
    check_price()
    check_columns()
    check_sampled()
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
