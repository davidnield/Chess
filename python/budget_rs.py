"""The Rust builder core (book_builder_rs, rust/book_builder) behind budget_core's interfaces.

build_graph_rs(...) takes build_graph's arguments, assembles the graph in Rust and returns a
budget_core.Graph equal to build_graph's, float for float and in every dict order
(_test_book_builder_rs.py check B). It exists to hold the port to the reference while the
downstream stages are ported; the production path will hand the collector's columns to Rust
directly instead of building edges_by_parent dicts first.

Not supported: max_cands > 0 (no recipe uses it).
"""
from __future__ import annotations

import math

import numpy as np

import budget_core as bc

try:
    import book_builder_rs as rs
except ImportError:          # the extension is optional; callers check available()
    rs = None


# Seconds spent by the last build_graph_rs: converting the dict inputs, in Rust, and building
# the Graph back (the first and last disappear once the collector feeds Rust directly).
LAST_TIMING: dict[str, float] = {}
# Tests set KEEP_INPUTS: build_graph_rs then keeps the arrays it hands to Rust in LAST_INPUTS
KEEP_INPUTS = False
LAST_INPUTS: dict = {}


def available() -> bool:
    return rs is not None


def _bare(san: str) -> str:
    return san.rstrip("+#")


def build_graph_rs(edges_by_parent: dict[int, list[dict]], root: int, our_white: bool, *,
                   rep_moves=None, eval_ws=None, aux_rows=None, slice_prior=0.5, eps=1e-3,
                   max_ply=40, share_floor=0.002, prior_strength=bc.PRIOR_STRENGTH,
                   eval_weight=bc.EVAL_WEIGHT, eval_weight_min=bc.EVAL_WEIGHT_MIN,
                   eval_weight_k=bc.EVAL_WEIGHT_K, robustness_floor=bc.ROBUSTNESS_FLOOR,
                   gate_rel_floor=bc.GATE_REL_FLOOR,
                   gate_rel_own_margin=bc.GATE_REL_OWN_MARGIN, require_eval=True,
                   max_cands=0, gates=True, other_cov_blend=False, gated_reach=False,
                   max_eval_loss=None, strict_source=False, crack_at_parents=False,
                   engine_moves=None, track_route_ply=False, track_other=False,
                   excluded=None, rare_floor=None, verified=None) -> bc.Graph:
    if max_cands:
        raise ValueError("build_graph_rs: max_cands > 0 is not ported")
    if rs is None:
        raise ImportError("book_builder_rs is not built (rust/book_builder/build.ps1)")
    import time
    t0 = time.time()
    eval_ws = eval_ws or {}
    rep_moves = rep_moves or {}
    aux_rows = aux_rows or {}
    parents = list(edges_by_parent)

    # moves: SAN codes in sorted order (code order = string order), bare codes
    sans = set()
    for es in edges_by_parent.values():
        sans.update(e["move_san"] for e in es)
    for h in parents:
        if rep_moves.get(h):
            sans.add(rep_moves[h])
        for san, _ch in (engine_moves or {}).get(h, ()):
            sans.add(san)
    san_list = sorted(sans)
    code = {s: i for i, s in enumerate(san_list)}
    bares = sorted({_bare(s) for s in san_list}
                   | {b for h in parents for b in (verified or {}).get(h, ())}
                   | {b for h in parents for b in (excluded or {}).get(h, ())})
    bcode = {b: i for i, b in enumerate(bares)}

    e_ptr, e_child, e_has, e_tot, e_share = [0], [], [], [], []
    e_san, e_bare, e_ws, e_mass, e_add = [], [], [], [], []
    for h in parents:
        for e in edges_by_parent[h]:
            ch = e.get("child_hash")
            e_child.append(0 if ch is None else ch)
            e_has.append(ch is not None)
            e_tot.append(e["total"])
            s = e.get("reach_share")
            e_share.append(math.nan if s is None else s)
            e_san.append(code[e["move_san"]])
            e_bare.append(bcode[_bare(e["move_san"])])
            e_ws.append(e.get("white_score_avg", math.nan))
            m = e.get("reply_mass")
            e_mass.append(math.nan if m is None else m)
            a = e.get("additional_reply")
            e_add.append(-1 if a is None else int(a))
        e_ptr.append(len(e_child))
    i64, f64 = np.int64, np.float64
    edges = dict(parents=np.array(parents, i64), ptr=np.array(e_ptr, i64),
                 child=np.array(e_child, i64), has_child=np.array(e_has, bool),
                 total=np.array(e_tot, i64), share=np.array(e_share, f64),
                 san=np.array(e_san, i64), bare=np.array(e_bare, i64), ws=np.array(e_ws, f64),
                 reply_mass=np.array(e_mass, f64), additional=np.array(e_add, i64))

    # per parent row: source move (and its child where Python would resolve one), overlay
    # flag, engine proposals, verified and vetoed moves
    import build_baseline_books
    ss, sb, sc, sh, named = [], [], [], [], []
    m_ptr, m_san, m_bare, m_child, m_has = [0], [], [], [], []
    v_ptr, v_bare, x_ptr, x_bare = [0], [], [0], []
    for h in parents:
        es = edges_by_parent[h]
        src = rep_moves.get(h)
        veto = (excluded or {}).get(h, ())
        if src and _bare(src) in veto:
            src_eff = None
        else:
            src_eff = src
        ss.append(code[src] if src else -1)
        sb.append(bcode[_bare(src)] if src else -1)
        ch = None
        # final_candidates resolves the source move's child only when it is not booked and
        # (strict) not recorded; it reads the EPD of the parent's first row
        epd = es[0].get("parent_epd") if es else None
        recorded = src_eff is not None and any(e["move_san"] == src_eff for e in es
                                               if _bare(e["move_san"]) not in veto)
        if src_eff and epd is not None and not (strict_source and recorded):
            ch = build_baseline_books.child_of(epd, src_eff)
        sc.append(0 if ch is None else ch)
        sh.append(ch is not None)
        named.append(bool(es) and "reply_mass" in es[0])
        for san, mch in (engine_moves or {}).get(h, ()):
            m_san.append(code[san])
            m_bare.append(bcode[_bare(san)])
            m_child.append(0 if mch is None else mch)
            m_has.append(mch is not None)
        m_ptr.append(len(m_san))
        v_bare += [bcode[b] for b in sorted((verified or {}).get(h, ()))]
        v_ptr.append(len(v_bare))
        x_bare += [bcode[b] for b in sorted(veto)]
        x_ptr.append(len(x_bare))
    rows = dict(src_san=np.array(ss, i64), src_bare=np.array(sb, i64),
                src_child=np.array(sc, i64), src_has_child=np.array(sh, bool),
                named=np.array(named, bool), m_ptr=np.array(m_ptr, i64),
                m_san=np.array(m_san, i64), m_bare=np.array(m_bare, i64),
                m_child=np.array(m_child, i64), m_has_child=np.array(m_has, bool),
                v_ptr=np.array(v_ptr, i64), v_bare=np.array(v_bare, i64),
                x_ptr=np.array(x_ptr, i64), x_bare=np.array(x_bare, i64))

    aux = None
    if aux_rows:
        groups = ("term_normal", "term_other", "term_flag")

        def f(r, k):
            return float(r.get(k) or 0.0) if r else 0.0
        rr = [aux_rows.get(h) for h in parents]
        aux = dict(
            term_total=np.array([[f(r, f"{g}_total") for g in groups] for r in rr], f64).reshape(-1, 3),
            term_white_wins=np.array([[f(r, f"{g}_white_wins") for g in groups] for r in rr], f64).reshape(-1, 3),
            term_black_wins=np.array([[f(r, f"{g}_black_wins") for g in groups] for r in rr], f64).reshape(-1, 3),
            term_draws=np.array([[f(r, f"{g}_draws") for g in groups] for r in rr], f64).reshape(-1, 3),
            other=np.array([[f(r, "other_total"), f(r, "other_white_wins"), f(r, "other_draws"),
                             (math.nan if not r or r.get("other_eval_mean") is None
                              else float(r["other_eval_mean"])),
                             f(r, "other_eval_cov"), f(r, "horizon_total"),
                             f(r, "horizon_white_wins"), f(r, "horizon_draws"),
                             1.0 if r else 0.0] for r in rr], f64).reshape(-1, 9))

    ek = np.fromiter(eval_ws.keys(), dtype=i64, count=len(eval_ws))
    ev = np.fromiter(eval_ws.values(), dtype=f64, count=len(eval_ws))
    t1 = time.time()
    if KEEP_INPUTS:
        LAST_INPUTS.update(edges=edges, rows=rows, aux=aux, ek=ek, ev=ev, san_list=san_list)
    out = run_csr(edges, rows, aux, ek, ev, root, our_white, slice_prior=slice_prior,
                  prior_strength=prior_strength, eval_weight=eval_weight,
                  eval_weight_min=eval_weight_min, eval_weight_k=eval_weight_k, eps=eps,
                  max_ply=max_ply, share_floor=share_floor, robustness_floor=robustness_floor,
                  gate_rel_floor=gate_rel_floor, gate_rel_own_margin=gate_rel_own_margin,
                  require_eval=require_eval, gates=gates, max_eval_loss=max_eval_loss,
                  rare_floor=rare_floor, strict_source=strict_source,
                  other_cov_blend=other_cov_blend, gated_reach=gated_reach,
                  crack_at_parents=crack_at_parents, engine_moves_given=bool(engine_moves),
                  track_route_ply=track_route_ply, track_other=track_other)
    t2 = time.time()
    g = graph_from_csr(out, edges_by_parent, root, san_list, eps=eps, max_ply=max_ply,
                       share_floor=share_floor, slice_prior=slice_prior,
                       our_white=our_white, strict_source=strict_source,
                       crack_at_parents=crack_at_parents)
    LAST_TIMING.update(convert_in=t1 - t0, rust=t2 - t1, convert_out=time.time() - t2)
    return g


def run_csr(edges, rows, aux, ek, ev, root, our_white, *, slice_prior, prior_strength,
            eval_weight, eval_weight_min, eval_weight_k, eps, max_ply, share_floor,
            robustness_floor, gate_rel_floor, gate_rel_own_margin, require_eval, gates,
            max_eval_loss, rare_floor, strict_source, other_cov_blend, gated_reach,
            crack_at_parents, engine_moves_given, track_route_ply, track_other) -> dict:
    """book_builder_rs.build_graph_csr on the input arrays"""
    return rs.build_graph_csr(
        edges, rows, aux, ek, ev, root, our_white, slice_prior, prior_strength, eval_weight,
        eval_weight_min, eval_weight_k, eps, max_ply, share_floor, robustness_floor,
        gate_rel_floor, gate_rel_own_margin, require_eval, gates,
        math.nan if max_eval_loss is None else float(max_eval_loss),
        -1 if rare_floor is None else int(rare_floor), strict_source, other_cov_blend,
        gated_reach, crack_at_parents, engine_moves_given, track_route_ply, track_other)


def meta_from_csr(out: dict, san_list, m: dict, *, strict_source, crack_at_parents) -> dict:
    """build_graph's meta entries from the Rust output, into m (in build_graph's order)"""
    if out["disputes"] is not None:
        d = m["rare_disputes"] = {}
        for h, ref, threats, risk, fb, unres in out["disputes"]:
            x = {"reference_cp": ref}
            if unres:
                x["unresolved"] = True
            x["threats"] = [(san_list[c], cp) for c, cp in threats]
            x["at_risk"] = [san_list[c] for c in risk]
            if fb:
                x["fallback"] = True
            d[h] = x
    if out["engine_cands"]:
        m["engine_cands"] = out["engine_cands"]
    if out["src_rejected"]:
        m["src_rejected"] = out["src_rejected"]
    if out["named"] is not None:
        a, b, c = out["named"]
        m["named_opponent_replies"] = {"positions": a, "additional_edges": b,
                                       "additional_edges_without_cached_eval": c}
    m["strict_source"] = strict_source
    m["crack_at_parents"] = crack_at_parents
    if out["crack_edges"] is not None:
        m["crack_edges"] = out["crack_edges"]
    if out["root_crack"] is not None:
        m["root_crack"] = tuple(out["root_crack"])
    if out["loss_exceptions"] is not None:
        m["loss_exceptions"] = dict(out["loss_exceptions"])
    return m


def graph_from_csr(out: dict, edges_by_parent, root, san_list, *, eps, max_ply, share_floor,
                   slice_prior, our_white, strict_source, crack_at_parents) -> bc.Graph:
    """The Rust arrays as a budget_core.Graph, in build_graph's insertion orders."""
    tl = lambda a: a.tolist()          # noqa: E731
    reach = dict(zip(tl(out["reach_keys"]), tl(out["reach_vals"])))
    ply = dict(zip(tl(out["ply_keys"]), tl(out["ply_vals"])))
    route = dict(zip(tl(out["route_keys"]), tl(out["route_vals"])))
    g = bc.Graph(root=root, our={}, opp={}, epd={}, ply=ply, route_ply=route, reach=reach,
                 meta={"eps": eps, "max_ply": max_ply, "share_floor": share_floor,
                       "slice_prior": slice_prior, "our_white": our_white})
    c_ptr, c_san, c_child = tl(out["c_ptr"]), tl(out["c_san"]), tl(out["c_child"])
    c_has, c_in, c_lv = tl(out["c_has_child"]), tl(out["c_in_sub"]), tl(out["c_lv"])
    k_ptr, k_p, k_child = tl(out["k_ptr"]), tl(out["k_p"]), tl(out["k_child"])
    k_has, k_in, k_lv = tl(out["k_has_child"]), tl(out["k_in_sub"]), tl(out["k_lv"])
    f_ptr, f_p, f_child, f_lv = tl(out["f_ptr"]), tl(out["f_p"]), tl(out["f_child"]), tl(out["f_lv"])
    l_node, cc, cm = tl(out["l_node"]), tl(out["crack_cont"]), tl(out["crack_mass"])
    cb = tl(out["const_base"])
    oi = pi = 0
    for h, our in zip(tl(out["node_keys"]), tl(out["node_our"])):
        es = edges_by_parent[h]
        if "parent_epd" in es[0]:
            g.epd[h] = es[0]["parent_epd"]
        if our:
            n = bc.OurNode(l_node=l_node[oi])
            if not math.isnan(cm[oi]):
                n.crack = (cc[oi], cm[oi])
            n.cands = [(san_list[c_san[j]], c_child[j] if c_has[j] else None, c_in[j], c_lv[j])
                       for j in range(c_ptr[oi], c_ptr[oi + 1])]
            g.our[h] = n
            oi += 1
        else:
            n = bc.OppNode(const_base=cb[pi])
            n.kids = [(k_p[j], k_child[j] if k_has[j] else None, k_in[j], k_lv[j])
                      for j in range(k_ptr[pi], k_ptr[pi + 1])]
            n.floor = [(f_p[j], f_child[j], f_lv[j]) for j in range(f_ptr[pi], f_ptr[pi + 1])]
            g.opp[h] = n
            pi += 1
    meta_from_csr(out, san_list, g.meta, strict_source=strict_source,
                  crack_at_parents=crack_at_parents)
    for h, share, v, e in out["other"]:
        g.other[h] = (share, v, None if math.isnan(e) else e)
    return g


# ── a graph owned by Rust (book_builder_rs.RsGraph) ─────────────────────────────────────────

def set_order(our_keys, opp_keys) -> np.ndarray:
    """list(set(g.our) | set(g.opp)) from the two dicts' key orders alone: set() of a dict
    presizes on its length and inserts in key order, so dict.fromkeys(keys) builds the same
    table and the union iterates identically (topo_up's Tarjan roots and topo_down's queue
    start in this order, and it decides which back edges are cut)."""
    s = set(dict.fromkeys(our_keys)) | set(dict.fromkeys(opp_keys))
    return np.fromiter(s, dtype=np.int64, count=len(s))


def graph_to_csr(g: bc.Graph, extra_sans=()) -> tuple[dict, list[str], dict[str, int]]:
    """Any budget_core.Graph as RsGraph's arrays. Returns (csr, san table, bare-code table):
    SAN codes are positions in the sorted SAN list, bare codes in the sorted bare list (which
    also holds extra_sans' bare forms, e.g. incumbent moves)."""
    i64, f64 = np.int64, np.float64
    sans = sorted({c[0] for n in g.our.values() for c in n.cands})
    code = {s: i for i, s in enumerate(sans)}
    bares = sorted({_bare(s) for s in sans} | {_bare(s) for s in extra_sans})
    bcode = {b: i for i, b in enumerate(bares)}
    our, opp = list(g.our), list(g.opp)
    cs = [c for h in our for c in g.our[h].cands]
    ks = [k for h in opp for k in g.opp[h].kids]
    fs = [f for h in opp for f in g.opp[h].floor]
    cum = lambda lens: np.concatenate([[0], np.cumsum(lens, dtype=i64)]).astype(i64)  # noqa: E731
    csr = dict(
        node_keys=np.array(our + opp, i64),
        node_our=np.array([True] * len(our) + [False] * len(opp), bool),
        l_node=np.array([g.our[h].l_node for h in our], f64),
        crack_cont=np.array([g.our[h].crack[0] if g.our[h].crack else math.nan for h in our], f64),
        crack_mass=np.array([g.our[h].crack[1] if g.our[h].crack else math.nan for h in our], f64),
        c_ptr=cum([len(g.our[h].cands) for h in our]),
        c_san=np.array([code[c[0]] for c in cs], i64),
        c_bare=np.array([bcode[_bare(c[0])] for c in cs], i64),
        c_child=np.array([0 if c[1] is None else c[1] for c in cs], i64),
        c_has_child=np.array([c[1] is not None for c in cs], bool),
        c_in_sub=np.array([bool(c[2]) for c in cs], bool),
        c_lv=np.array([c[3] for c in cs], f64),
        const_base=np.array([g.opp[h].const_base for h in opp], f64),
        k_ptr=cum([len(g.opp[h].kids) for h in opp]),
        k_p=np.array([k[0] for k in ks], f64),
        k_child=np.array([0 if k[1] is None else k[1] for k in ks], i64),
        k_has_child=np.array([k[1] is not None for k in ks], bool),
        k_in_sub=np.array([bool(k[2]) for k in ks], bool),
        k_lv=np.array([k[3] for k in ks], f64),
        f_ptr=cum([len(g.opp[h].floor) for h in opp]),
        f_p=np.array([f[0] for f in fs], f64),
        f_child=np.array([f[1] for f in fs], i64),
        f_lv=np.array([f[2] for f in fs], f64),
        reach_keys=np.array(list(g.reach), i64),
        reach_vals=np.array(list(g.reach.values()), f64))
    for k in ("c_lv", "k_lv", "f_lv", "f_p", "k_p", "l_node", "const_base", "reach_vals"):
        csr[k] = csr[k].reshape(-1)
    return csr, sans, bcode


def rs_graph(g: bc.Graph):
    """(RsGraph of g, its SAN table): stickiness from g.meta["stick"] carried over."""
    st = g.meta.get("stick") if g.meta else None
    csr, sans, bcode = graph_to_csr(g, extra_sans=list(st[0].values()) if st else ())
    r = rs.RsGraph(csr, set_order(list(g.our), list(g.opp)), g.root,
                   bool(g.meta.get("crack_at_parents")) if g.meta else False)
    if st:
        moves, margin = st
        r.set_stick(np.array(list(moves), np.int64),
                    np.array([bcode.get(m, -1) for m in moves.values()], np.int64), margin)
    return r, sans


def book_from_rs(d: dict, sans: list[str]) -> dict:
    """An RsGraph book dict as extract_book's result (dicts in Python's insertion orders)."""
    tl = lambda a: a.tolist()  # noqa: E731
    return {"booked": dict(zip(tl(d["booked_keys"]), (sans[c] for c in tl(d["booked_san"])))),
            "alloc": dict(zip(tl(d["alloc_keys"]), tl(d["alloc_vals"]))),
            "visited": tl(d["visited"]),
            "realized": dict(zip(tl(d["realized_keys"]), tl(d["realized_vals"]))),
            "spent_paths": d["spent_paths"], "spent_distinct": d["spent_distinct"],
            "root_value_curve": d["root_value_curve"],
            "root_value_realized": d["root_value_realized"]}


def rs_moves(moves: dict, sans: list[str]):
    """a {position: SAN} book as RsGraph's (keys, SAN codes); an unknown SAN never matches"""
    code = {s: i for i, s in enumerate(sans)}
    return (np.fromiter(moves.keys(), np.int64, len(moves)),
            np.array([code.get(s, -1) for s in moves.values()], np.int64))


def polish_rs(r, sans: list[str], moves: dict, target: int, **kw) -> tuple[dict, dict]:
    """polish_book on an RsGraph: (moves, stats), as budget_core.polish_book returns them"""
    import time
    t0 = time.time()
    k, c, st = r.polish(*rs_moves(moves, sans), target, **kw)
    st = dict(st)
    st["by_kind"] = dict(st["by_kind"])
    st["seconds"] = time.time() - t0
    return dict(zip(k.tolist(), (sans[x] for x in c.tolist()))), st


def book_result_rs(r, sans: list[str], moves: dict, lam=None) -> dict:
    """book_result on an RsGraph (topo_up's order), as budget_core.book_result returns it"""
    d = r.book_result(*rs_moves(moves, sans))
    visited = d["visited"].tolist()
    size = len(d["booked_keys"])
    return {"booked": dict(zip(d["booked_keys"].tolist(), (sans[x] for x in d["booked_san"].tolist()))),
            "alloc": {}, "visited": visited,
            "realized": dict(zip(visited, d["realized_vals"].tolist())),
            "spent_paths": size, "spent_distinct": size,
            "root_value_curve": None, "root_value_realized": d["value"],
            "root_value_penalized": None, "lam": lam,
            "reach_policy": dict(zip(d["w_keys"].tolist(), d["w_vals"].tolist()))}


def policy_reach_rs(r, sans: list[str], moves: dict) -> dict:
    """budget_core.policy_reach on an RsGraph (topo_up's order), in insertion order"""
    k, v = r.policy_reach(*rs_moves(moves, sans))
    return dict(zip(k.tolist(), v.tolist()))


def price_search_rs(r, sans: list[str], target: int, *, seed_reach: dict | None = None,
                    steps: int = 40, select_tol: float = 0.01, polish: bool = True,
                    line_k: int = 8, log=print) -> tuple[dict, dict]:
    """budget_core.price_search on an RsGraph: the same (result, info) and log lines (the
    timing reports engine 'rust')."""
    import time
    t0 = time.time()
    sk = sv = None
    if seed_reach is not None:
        sk = np.fromiter(seed_reach.keys(), np.int64, len(seed_reach))
        sv = np.fromiter(seed_reach.values(), np.float64, len(seed_reach))
    d, inf = r.price_search(target, sk, sv, steps, select_tol, polish, line_k)
    if inf["cut"]:
        log(f"  price search: {inf['cut']:,} back edges cut in repetition cycles")
    visited = d["visited"].tolist()
    size = len(d["booked_keys"])
    res = {"booked": dict(zip(d["booked_keys"].tolist(), (sans[x] for x in d["booked_san"].tolist()))),
           "alloc": {}, "visited": visited,
           "realized": dict(zip(visited, d["realized_vals"].tolist())),
           "spent_paths": size, "spent_distinct": size,
           "root_value_curve": None, "root_value_realized": d["value"],
           "root_value_penalized": d.get("value_penalized"), "lam": inf["lam"],
           "reach_policy": dict(zip(d["w_keys"].tolist(), d["w_vals"].tolist()))}
    ub = inf["upper_bound"]
    info = {"lam": inf["lam"], "booked": size, "under": inf["under"], "over": inf["over"],
            "value": res["root_value_realized"], "upper_bound": ub,
            "gap": ub - res["root_value_realized"],
            "sweep": [{"lam": a, "booked": b, "value": c, "round": 1} for a, b, c in
                      zip(inf["sweep_lam"].tolist(), inf["sweep_booked"].tolist(),
                          inf["sweep_value"].tolist())],
            "timing": {"engine": "rust", "total_s": round(time.time() - t0, 3)}}
    log(f"  price search: lam {res['lam']:.3e} -> {res['spent_distinct']:,} positions "
        f"(target {target:,}; nearest sizes {info['under']} / {info['over']}), "
        f"value {res['root_value_realized']:.5f}, bound at {target:,}: {ub:.5f} "
        f"(gap {info['gap']:.5f}), {len(info['sweep'])} solves")
    log(f"  price search time (rust): total {info['timing']['total_s']:.1f}s")
    if polish:
        info["dag_mode"] = "polish"
        info["polish"] = [dict(p, by_kind=dict(p["by_kind"])) for p in inf["polish"]]
        if info["polish"]:
            log("  dag polish: " + "; ".join(
                f"{p['start_size']:,} -> {p['size']:,} positions, {p['start_value']:.5f} -> "
                f"{p['value']:.5f} ({p['accepted']} moves, {p['seconds']:.1f}s)"
                for p in info["polish"]))
    return res, info


# ── the columnar path: the collector's frame to Rust, no edges_by_parent dicts ─────────────

AUX_GROUPS = ("term_normal", "term_other", "term_flag")
AUX_OTHER = ("other_total", "other_white_wins", "other_draws", "other_eval_mean",
             "other_eval_cov", "horizon_total", "horizon_white_wins", "horizon_draws")


def frame_inputs(edf, *, rep_moves=None, engine_moves=None, excluded=None, verified=None,
                 strict_source=False, aux_frame=None) -> dict:
    """build_graph_rs's input arrays straight from the collector's frame (parent_hash,
    move_san, child_hash, parent_epd, total, white_score_avg; each parent's edges contiguous,
    parents in first-appearance order, as the collector returns them): the same arrays bit for
    bit (_test_book_builder_rs.py check C). aux_frame: the aux sidecar's rows (polars), the
    rows load_aux_rows reads. Returns {edges, rows, aux, san_list, parents, epd}."""
    import polars as pl
    import build_baseline_books
    i64, f64 = np.int64, np.float64
    rep_moves = rep_moves or {}
    engine_moves = engine_moves or {}
    excluded = excluded or {}
    verified = verified or {}
    if "reply_mass" in edf.columns:
        raise ValueError("frame_inputs: opponent-prior rows are not supported")
    par = edf["parent_hash"].cast(pl.Int64).to_numpy()
    m = len(par)
    if m:
        starts = np.flatnonzero(np.concatenate([[True], par[1:] != par[:-1]]))
    else:
        starts = np.zeros(0, i64)
    parents = par[starts]
    if len(np.unique(parents)) != len(parents):
        raise ValueError("frame_inputs: a parent's edges are not contiguous")
    e_ptr = np.concatenate([starts, [m]]).astype(i64)
    pl_ = parents.tolist()
    sans_col = edf["move_san"]
    sans = set(sans_col.unique().to_list())
    srcs = [rep_moves.get(h) for h in pl_]
    sans.update(x for x in srcs if x)
    for h in pl_:
        for san, _ch in engine_moves.get(h, ()):
            sans.add(san)
    san_list = sorted(sans)
    code = {x: i for i, x in enumerate(san_list)}
    bares = sorted({_bare(x) for x in san_list}
                   | {b for h in pl_ for b in verified.get(h, ())}
                   | {b for h in pl_ for b in excluded.get(h, ())})
    bcode = {b: i for i, b in enumerate(bares)}
    chs = edf["child_hash"].cast(pl.Int64)
    ws = edf["white_score_avg"].cast(pl.Float64).fill_null(math.nan)
    edges = dict(parents=parents.astype(i64), ptr=e_ptr,
                 child=chs.fill_null(0).to_numpy().astype(i64),
                 has_child=chs.is_not_null().to_numpy(),
                 total=edf["total"].cast(pl.Int64).to_numpy(),
                 share=np.full(m, math.nan, f64),
                 san=sans_col.replace_strict(code, return_dtype=pl.Int64).to_numpy(),
                 bare=sans_col.str.strip_chars_end("+#").replace_strict(
                     bcode, return_dtype=pl.Int64).to_numpy(),
                 ws=ws.to_numpy(), reply_mass=np.full(m, math.nan, f64),
                 additional=np.full(m, -1, i64))
    epd = edf["parent_epd"].gather(starts).to_list() if m else []
    # the source move, unless vetoed, and whether it is among the recorded (unvetoed) moves
    src_eff = []
    for h, src in zip(pl_, srcs):
        veto = excluded.get(h, ())
        src_eff.append(None if (src and _bare(src) in veto) else src)
    recorded = set()
    if any(x is not None for x in src_eff):
        ms = sans_col.to_list()
        for idx, x in enumerate(src_eff):
            if x is None:
                continue
            veto = excluded.get(pl_[idx], ())
            for j in range(e_ptr[idx], e_ptr[idx + 1]):
                if ms[j] == x and _bare(ms[j]) not in veto:
                    recorded.add(idx)
                    break
        del ms
    ss, sb, sc, sh = [], [], [], []
    m_ptr, m_san, m_bare, m_child, m_has = [0], [], [], [], []
    v_ptr, v_bare, x_ptr, x_bare = [0], [], [0], []
    for idx, h in enumerate(pl_):
        src = srcs[idx]
        ss.append(code[src] if src else -1)
        sb.append(bcode[_bare(src)] if src else -1)
        se = src_eff[idx]
        ch = None
        if se and epd[idx] is not None and not (strict_source and idx in recorded):
            ch = build_baseline_books.child_of(epd[idx], se)
        sc.append(0 if ch is None else ch)
        sh.append(ch is not None)
        for san, mch in engine_moves.get(h, ()):
            m_san.append(code[san])
            m_bare.append(bcode[_bare(san)])
            m_child.append(0 if mch is None else mch)
            m_has.append(mch is not None)
        m_ptr.append(len(m_san))
        v_bare += [bcode[b] for b in sorted(verified.get(h, ()))]
        v_ptr.append(len(v_bare))
        x_bare += [bcode[b] for b in sorted(excluded.get(h, ()))]
        x_ptr.append(len(x_bare))
    n = len(pl_)
    rows = dict(src_san=np.array(ss, i64), src_bare=np.array(sb, i64),
                src_child=np.array(sc, i64), src_has_child=np.array(sh, bool),
                named=np.zeros(n, bool), m_ptr=np.array(m_ptr, i64),
                m_san=np.array(m_san, i64), m_bare=np.array(m_bare, i64),
                m_child=np.array(m_child, i64), m_has_child=np.array(m_has, bool),
                v_ptr=np.array(v_ptr, i64), v_bare=np.array(v_bare, i64),
                x_ptr=np.array(x_ptr, i64), x_bare=np.array(x_bare, i64))
    aux = None
    if aux_frame is not None and aux_frame.height:
        # a dict of rows keeps the last row of a duplicated position
        af = aux_frame.unique("position_hash", keep="last", maintain_order=True)
        jn = (pl.DataFrame({"position_hash": parents.astype(i64)})
              .join(af.with_columns(pl.lit(1.0).alias("__present")), on="position_hash",
                    how="left", maintain_order="left"))

        def col(name):
            if name not in jn.columns:
                return np.zeros(n, f64)
            c = jn[name].cast(pl.Float64)
            if name == "other_eval_mean":
                return c.fill_null(math.nan).to_numpy()
            return c.fill_null(0.0).to_numpy()

        def stack(names):
            return np.stack([col(x) for x in names], axis=1).reshape(-1, len(names))
        aux = dict(term_total=stack([f"{g}_total" for g in AUX_GROUPS]),
                   term_white_wins=stack([f"{g}_white_wins" for g in AUX_GROUPS]),
                   term_black_wins=stack([f"{g}_black_wins" for g in AUX_GROUPS]),
                   term_draws=stack([f"{g}_draws" for g in AUX_GROUPS]),
                   other=stack(list(AUX_OTHER) + ["__present"]))
    return dict(edges=edges, rows=rows, aux=aux, san_list=san_list, parents=parents, epd=epd)


class ArrayMap:
    """A read-only int -> value map on sorted arrays (get, [], in, len, items and keys in the
    original order): a dict's interface without a dict's memory."""

    def __init__(self, keys, vals):
        keys = np.asarray(keys, np.int64)
        self._ok, self._ov = keys, vals
        o = np.argsort(keys, kind="stable")
        self._k = keys[o]
        self._v = [vals[i] for i in o.tolist()] if isinstance(vals, list) else np.asarray(vals)[o]

    def _find(self, h):
        i = int(np.searchsorted(self._k, h))
        return i if i < len(self._k) and self._k[i] == h else -1

    def _val(self, i):
        v = self._v[i]
        return v.item() if hasattr(v, "item") else v

    def get(self, h, default=None):
        i = self._find(h)
        return default if i < 0 else self._val(i)

    def __getitem__(self, h):
        i = self._find(h)
        if i < 0:
            raise KeyError(h)
        return self._val(i)

    def __contains__(self, h):
        return self._find(h) >= 0

    def __len__(self):
        return len(self._k)

    def __iter__(self):
        return iter(self._ok.tolist())

    def items(self):
        v = self._ov
        return zip(self._ok.tolist(), v if isinstance(v, list) else np.asarray(v).tolist())

    def keys(self):
        return self._ok.tolist()


class GraphView:
    """What build_budget_books reads from a Graph once the graph lives in Rust: the root, the
    node sets (len, in), reach, ply, the parents' EPDs, meta and the other-bucket table."""

    def __init__(self, out: dict, san_list, root, parents, epd, *, eps, max_ply, share_floor,
                 slice_prior, our_white, strict_source, crack_at_parents):
        self.root = root
        nk, no = out["node_keys"], out["node_our"]
        self.our = ArrayMap(nk[no], np.zeros(int(no.sum())))
        self.opp = ArrayMap(nk[~no], np.zeros(int((~no).sum())))
        self.reach = ArrayMap(out["reach_keys"], out["reach_vals"])
        self.ply = ArrayMap(out["ply_keys"], out["ply_vals"])
        self.route_ply = ArrayMap(out["route_keys"], out["route_vals"])
        # the EPDs of the graph's nodes (a Graph keeps no other parent's)
        on = np.isin(np.asarray(parents, np.int64), nk)
        self.epd = ArrayMap(np.asarray(parents, np.int64)[on], [e for e, k in zip(epd, on.tolist()) if k])
        # the opponent nodes' children, for sampled_other.other_positions_view
        ok = nk[~no]
        self._opp_row = ArrayMap(ok, np.arange(len(ok)))
        self._k = (out["k_ptr"], out["k_child"], out["k_has_child"])
        self._f = (out["f_ptr"], out["f_child"])
        self._out_plies = (out["ply_keys"], out["ply_vals"], out["route_keys"], out["route_vals"])
        self.meta = {"eps": eps, "max_ply": max_ply, "share_floor": share_floor,
                     "slice_prior": slice_prior, "our_white": our_white}
        meta_from_csr(out, san_list, self.meta, strict_source=strict_source,
                      crack_at_parents=crack_at_parents)
        self.other = {h: (share, v, None if math.isnan(e) else e)
                      for h, share, v, e in out["other"]}


def rs_graph_from_out(out: dict, san_list, root, *, crack_at_parents, stick=None):
    """(RsGraph, SAN table) straight from build_graph_csr's output, as rs_graph(graph_from_csr
    (...)) gives without the Graph: c_bare from the SAN table, stickiness as rs_graph sets it."""
    extra = list(stick[0].values()) if stick else []
    bares = sorted({_bare(x) for x in san_list} | {_bare(x) for x in extra})
    bcode = {b: i for i, b in enumerate(bares)}
    sb = np.array([bcode[_bare(x)] for x in san_list], np.int64)
    keys = ("node_keys", "node_our", "l_node", "crack_cont", "crack_mass", "c_ptr", "c_san",
            "c_child", "c_has_child", "c_in_sub", "c_lv", "const_base", "k_ptr", "k_p",
            "k_child", "k_has_child", "k_in_sub", "k_lv", "f_ptr", "f_p", "f_child", "f_lv",
            "reach_keys", "reach_vals")
    csr = {k: out[k] for k in keys}
    csr["c_bare"] = sb[out["c_san"]] if len(out["c_san"]) else np.zeros(0, np.int64)
    nk, no = out["node_keys"], out["node_our"]
    r = rs.RsGraph(csr, set_order(nk[no].tolist(), nk[~no].tolist()), root, crack_at_parents)
    if stick:
        moves, margin = stick
        r.set_stick(np.fromiter(moves.keys(), np.int64, len(moves)),
                    np.array([bcode.get(x, -1) for x in moves.values()], np.int64), margin)
    return r, san_list


def opp_children(view: "GraphView", h: int) -> list:
    """An opponent node's reply children (kids with a child, then the floor), as
    sampled_other.other_positions lists them"""
    i = view._opp_row[h]
    kp, kc, kh = view._k
    fp, fc = view._f
    kids = [c for c, has in zip(kc[kp[i]:kp[i + 1]].tolist(), kh[kp[i]:kp[i + 1]].tolist()) if has]
    return kids + fc[fp[i]:fp[i + 1]].tolist()
