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
    out = rs.build_graph_csr(
        edges, rows, aux, ek, ev, root, our_white, slice_prior, prior_strength, eval_weight,
        eval_weight_min, eval_weight_k, eps, max_ply, share_floor, robustness_floor,
        gate_rel_floor, gate_rel_own_margin, require_eval, gates,
        math.nan if max_eval_loss is None else float(max_eval_loss),
        -1 if rare_floor is None else int(rare_floor), strict_source, other_cov_blend,
        gated_reach, crack_at_parents, bool(engine_moves), track_route_ply, track_other)
    t2 = time.time()
    g = graph_from_csr(out, edges_by_parent, root, san_list, eps=eps, max_ply=max_ply,
                       share_floor=share_floor, slice_prior=slice_prior,
                       our_white=our_white, strict_source=strict_source,
                       crack_at_parents=crack_at_parents)
    LAST_TIMING.update(convert_in=t1 - t0, rust=t2 - t1, convert_out=time.time() - t2)
    return g


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
    m = g.meta
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
    for h, share, v, e in out["other"]:
        g.other[h] = (share, v, None if math.isnan(e) else e)
    m["strict_source"] = strict_source
    m["crack_at_parents"] = crack_at_parents
    if out["crack_edges"] is not None:
        m["crack_edges"] = out["crack_edges"]
    if out["root_crack"] is not None:
        m["root_crack"] = tuple(out["root_crack"])
    if out["loss_exceptions"] is not None:
        m["loss_exceptions"] = dict(out["loss_exceptions"])
    return g
