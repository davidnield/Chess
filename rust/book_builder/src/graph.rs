//! budget_core.build_graph, assembled into compressed sparse rows.
//!
//! Inputs are columnar: parent rows in edges_by_parent's order, their edges in order, and
//! per-row extras (source move, aux row, engine proposals, vetoes, verified moves). Evals
//! are a sorted key array searched by bisection. The output keeps Python's node order (the
//! reach dict's) and, per node, Python's candidate / kid / floor orders.
//!
//! Not ported (refused by the binding): max_cands > 0 and the legacy (non-strict, no crack
//! at parents) flags are kept, since they are cheap; only max_cands is refused.

use std::collections::HashMap;

use crate::gates::{self, Cand, Dispute, GateParams, NodeIn, Ref};
use crate::reach::{self, Edges, Key, OrderedMap};
use crate::value::{self, AuxRow, Blend};

/// Sorted keys with their White-view evals: eval_ws as a bisection table.
pub struct Evals<'a> {
    pub keys: &'a [Key],
    pub vals: &'a [f64],
}

impl<'a> Evals<'a> {
    /// NaN = not covered (Python's eval_ws.get(h) is None)
    pub fn get(&self, k: Option<Key>) -> f64 {
        match k {
            Some(k) => match self.keys.binary_search(&k) {
                Ok(i) => self.vals[i],
                Err(_) => f64::NAN,
            },
            None => f64::NAN,
        }
    }
}

/// One parent row's edges plus the per-edge fields only build_graph reads.
pub struct EdgeCols<'a> {
    pub san: &'a [i64],
    pub bare: &'a [i64],
    pub ws: &'a [f64],
    pub reply_mass: &'a [f64],     // NaN = absent
    pub additional: &'a [i64],     // additional_reply; -1 = absent
}

pub struct RowExtras<'a> {
    pub src_san: &'a [i64],        // -1 = no source move
    pub src_bare: &'a [i64],
    pub src_child: &'a [Option<Key>],  // the source move's child (python-chess), if resolvable
    pub named: &'a [bool],         // "reply_mass" in es[0]
    pub aux: Option<&'a [Option<AuxRow>]>,  // None = aux_rows empty
    pub m_ptr: &'a [i64],
    pub m: &'a [Cand],             // engine proposals (Ref::Engine(global index))
    pub m_child: &'a [Option<Key>],
    pub v_ptr: &'a [i64],
    pub v_bare: &'a [i64],
    pub x_ptr: &'a [i64],
    pub x_bare: &'a [i64],
}

#[derive(Clone, Copy)]
pub struct Params {
    pub our_white: bool,
    pub blend: Blend,
    pub eps: f64,
    pub max_ply: i64,
    pub share_floor: f64,
    pub gate: GateParams,
    pub other_cov_blend: bool,
    pub gated_reach: bool,
    pub crack_at_parents: bool,
    /// engine_moves was a non-empty dict (Python's truthiness picks the admitted pass's
    /// gate function from it and strict_source)
    pub engine_moves_given: bool,
    pub track_route_ply: bool,
    pub track_other: bool,
}

/// A candidate at an our node: (san code, child, in subgraph, leaf value).
pub type CandOut = (i64, Option<Key>, bool, f64);
/// A reply at an opponent node: (p, child, in subgraph, leaf value).
pub type KidOut = (f64, Option<Key>, bool, f64);

#[derive(Default)]
pub struct Graph {
    pub reach: OrderedMap<f64>,
    pub ply: OrderedMap<i64>,
    pub route: OrderedMap<i64>,
    /// nodes in Python's order: (key, our?, parent row)
    pub nodes: Vec<(Key, bool, usize)>,
    // our nodes (in node order)
    pub l_node: Vec<f64>,
    pub crack: Vec<Option<(f64, f64)>>,
    pub c_ptr: Vec<i64>,
    pub cands: Vec<CandOut>,
    // opponent nodes (in node order)
    pub const_base: Vec<f64>,
    pub k_ptr: Vec<i64>,
    pub kids: Vec<KidOut>,
    pub f_ptr: Vec<i64>,
    pub floor: Vec<(f64, Key, f64)>,
    // meta
    pub engine_cands: i64,
    pub src_rejected: i64,
    pub named: Option<(i64, i64, i64)>,
    pub disputes: Option<Vec<(Key, Dispute)>>,
    pub crack_edges: Option<i64>,
    pub root_crack: Option<(f64, f64)>,
    pub loss_exceptions: Option<Vec<(Key, &'static str)>>,
    pub other: Vec<(Key, f64, f64, f64)>,  // (h, share, value, engine NaN = None)
}

#[allow(clippy::too_many_arguments)]
pub fn build_graph(g_edges: &Edges, e: &EdgeCols, x: &RowExtras, evals: &Evals, root: Key,
                   p: &Params) -> Graph {
    let rows = g_edges.row_of();
    let range = |row: usize| g_edges.ptr[row] as usize..g_edges.ptr[row + 1] as usize;
    let mrange = |row: usize| x.m_ptr[row] as usize..x.m_ptr[row + 1] as usize;
    let vr = |row: usize| &x.v_bare[x.v_ptr[row] as usize..x.v_ptr[row + 1] as usize];
    let xr = |row: usize| &x.x_bare[x.x_ptr[row] as usize..x.x_ptr[row + 1] as usize];

    // final_candidates per parent row, computed once (the admitted pass and the node loop
    // see the same inputs; only the node loop records disputes)
    let recorded: Vec<Cand> = (0..g_edges.child.len()).map(|i| Cand {
        r: Ref::Edge(i), san: e.san[i], bare: e.bare[i], eval: evals.get(g_edges.child[i]),
        total: g_edges.total[i], engine: false,
    }).collect();
    let mut fc_cache: HashMap<usize, gates::NodeOut> = HashMap::new();
    let compute = |row: usize, h: Key| -> gates::NodeOut {
        let node = NodeIn {
            own: evals.get(Some(h)),
            src: if x.src_san[row] < 0 { None } else { Some(x.src_san[row]) },
            src_bare: x.src_bare[row],
            src_eval: evals.get(x.src_child[row]),
            recorded: &recorded[range(row)],
            engine: &x.m[mrange(row)],
            verified: vr(row),
            veto: xr(row),
        };
        gates::final_candidates(&node, &p.gate, p.gate.rare_floor.is_some())
    };

    // --gated-reach: the admitted children of every (non-overlay) parent row
    let mut adm_ptr = vec![0i64];
    let mut adm_keys: Vec<Key> = Vec::new();
    if p.gated_reach {
        for row in 0..g_edges.parents.len() {
            if !range(row).is_empty() && !x.named[row] && !(p.gate.strict_source || p.engine_moves_given) {
                // gate_candidates on the recorded moves as they stand (no vetoes, no proposals)
                let h = g_edges.parents[row];
                let src = if x.src_san[row] < 0 { None } else { Some(x.src_san[row]) };
                let (cs, _d) = gates::gate_candidates(&recorded[range(row)], src,
                                                      evals.get(Some(h)), vr(row), &p.gate, false);
                for c in &cs {
                    if let Ref::Edge(j) = c.r {
                        if let Some(ch) = g_edges.child[j] {
                            adm_keys.push(ch);
                        }
                    }
                }
            } else if !range(row).is_empty() && !x.named[row] {
                let o = compute(row, g_edges.parents[row]);
                for r in &o.kept {
                    if let Ref::Edge(j) = *r {
                        if let Some(ch) = g_edges.child[j] {
                            adm_keys.push(ch);
                        }
                    }
                    if let Ref::Engine(j) = *r {
                        if let Some(ch) = x.m_child[j] {
                            adm_keys.push(ch);
                        }
                    }
                }
                fc_cache.insert(row, o);
            }
            adm_ptr.push(adm_keys.len() as i64);
        }
    }
    let ge = Edges {
        parents: g_edges.parents, ptr: g_edges.ptr, child: g_edges.child, total: g_edges.total,
        share: g_edges.share,
        admitted: if p.gated_reach { Some((&adm_ptr, &adm_keys)) } else { None },
    };
    // Engine proposals join the reach walk as zero-game edges (build_graph's reach_edges):
    // child hashes not already recorded at the parent, total 0. Rebuilt as an extended CSR.
    let has_engine = !x.m.is_empty();
    let (rp, rc, rt, rs);
    let reach_edges = if has_engine {
        let mut ptr = vec![0i64];
        let (mut ch, mut tot, mut sh) = (Vec::new(), Vec::new(), Vec::new());
        for row in 0..g_edges.parents.len() {
            let rg = range(row);
            for i in rg.clone() {
                ch.push(g_edges.child[i]);
                tot.push(g_edges.total[i]);
                sh.push(g_edges.share[i]);
            }
            // edges_by_parent[h] + [{"child_hash": ch, "total": 0} for ...]: only rows that
            // exist in edges_by_parent are extended
            let existing: Vec<Option<Key>> = rg.clone().map(|i| g_edges.child[i]).collect();
            for j in mrange(row) {
                let c = x.m_child[j];
                if !existing.contains(&c) {
                    ch.push(c);
                    tot.push(0);
                    sh.push(f64::NAN);
                }
            }
            ptr.push(ch.len() as i64);
        }
        rp = ptr;
        rc = ch;
        rt = tot;
        rs = sh;
        Edges { parents: g_edges.parents, ptr: &rp, child: &rc, total: &rt, share: &rs,
                admitted: ge.admitted }
    } else {
        Edges { parents: g_edges.parents, ptr: g_edges.ptr, child: g_edges.child,
                total: g_edges.total, share: g_edges.share, admitted: ge.admitted }
    };
    let (reach_m, ply_m) = reach::optimistic_reach(&reach_edges, &rows, root, p.our_white,
                                                   p.eps, p.max_ply, p.share_floor);
    let route = if p.track_route_ply {
        reach::route_plies(&reach_edges, &rows, root, p.our_white, &reach_m, &ply_m, p.max_ply,
                           p.share_floor)
    } else {
        OrderedMap::default()
    };

    let mut g = Graph { c_ptr: vec![0], k_ptr: vec![0], f_ptr: vec![0], ..Default::default() };
    let is_parent = |k: &Key| rows.contains_key(k);
    let ow = p.our_white;
    let b = &p.blend;
    let have_aux = x.aux.is_some();
    let mut disputes: Option<Vec<(Key, Dispute)>> = None;
    let mut our_index: HashMap<Key, usize> = HashMap::new();
    let mut named_counts: Option<(i64, i64, i64)> = None;

    for (ni, &h) in reach_m.keys.iter().enumerate() {
        let _r = reach_m.vals[ni];
        let Some(&row) = rows.get(&h) else { continue };
        let rg = range(row);
        if rg.is_empty() {
            continue;
        }
        let p_ply = ply_m.get(&h).unwrap();
        let our = (p_ply % 2 == 0) == ow;
        let aux_b = if have_aux {
            Some(value::aux_row_buckets(x.aux.unwrap()[row].as_ref(), ow))
        } else {
            None
        };
        if our {
            let ln0 = value::leaf_node_value(&e.ws[rg.clone()], &g_edges.total[rg.clone()],
                                             evals.get(Some(h)), b);
            let mut ln = ln0;
            let mut crack_pair = None;
            if let Some(ab) = aux_b {
                if ab.crack != 0.0 {
                    let tot_i: i64 = g_edges.total[rg.clone()].iter().sum();
                    let cont = tot_i as f64 + ab.oth_tot + ab.hor_tot;
                    if !p.crack_at_parents {
                        // stage3.aux_our_blend(ln, cont, crack, 1.0)
                        let total = cont + ab.crack;
                        if !(ab.crack <= 0.0 || total <= 0.0) {
                            ln = (ln * cont + 1.0 * ab.crack) / total;
                        }
                    }
                    crack_pair = Some((cont, ab.crack));
                }
            }
            let o = fc_cache.remove(&row).unwrap_or_else(|| compute(row, h));
            if p.gate.rare_floor.is_some() {
                let d = disputes.get_or_insert_with(Vec::new);
                if let Some(dd) = o.dispute.clone() {
                    d.push((h, dd));
                }
            }
            g.engine_cands += o.n_eng;
            g.src_rejected += o.n_rej;
            for r in &o.kept {
                let (san, ch, ws, total) = match *r {
                    Ref::Edge(j) => (e.san[j], g_edges.child[j], e.ws[j], g_edges.total[j]),
                    Ref::Engine(j) => (x.m[j].san, x.m_child[j], b.slice_prior, 0),
                };
                let in_sub = match ch {
                    Some(c) => reach_m.contains(&c) && is_parent(&c) && p_ply + 1 <= p.max_ply,
                    None => false,
                };
                let lv = value::leaf_edge_value(ws, total as f64, evals.get(ch), b);
                g.cands.push((san, ch, in_sub, lv));
            }
            if o.restored {
                let ch = x.src_child[row];
                let in_sub = match ch {
                    Some(c) => reach_m.contains(&c) && is_parent(&c),
                    None => false,
                };
                let lv = value::leaf_edge_value(b.slice_prior, 0.0, evals.get(ch), b);
                g.cands.push((x.src_san[row], ch, in_sub, lv));
            }
            g.c_ptr.push(g.cands.len() as i64);
            our_index.insert(h, g.l_node.len());
            g.l_node.push(ln);
            g.crack.push(crack_pair);
            g.nodes.push((h, true, row));
        } else {
            let named = x.named[row];
            let mass = |i: usize| -> f64 {
                let m = e.reply_mass[i];
                if m.is_nan() { g_edges.total[i] as f64 } else { m }
            };
            let mut tot = 0.0f64;
            for i in rg.clone() {
                tot += mass(i);
            }
            if named {
                let c = named_counts.get_or_insert((0, 0, 0));
                c.0 += 1;
                for i in rg.clone() {
                    // sum(e.get("additional_reply", total == 0)), then `x and child not in eval_ws`
                    let add = if e.additional[i] >= 0 { e.additional[i] }
                              else { (g_edges.total[i] == 0) as i64 };
                    c.1 += add;
                    c.2 += (add != 0 && evals.get(g_edges.child[i]).is_nan()) as i64;
                }
            }
            let (mut aux_num, mut aux_den) = (0.0f64, 0.0f64);
            let mut o_parts = None;
            if let Some(mut ab) = aux_b {
                if named {
                    ab.oth_tot = 0.0;
                    ab.oth_sum = 0.0;
                    ab.oth_eval = f64::NAN;
                    ab.oth_cov = 0.0;
                }
                let (w_num, den) = value::aux_opp_mix(&ab, b.eval_weight, b.eval_weight_min,
                                                      b.eval_weight_k, p.other_cov_blend);
                aux_den = den;
                aux_num = if ow { w_num } else { den - w_num };
                o_parts = Some(ab);
            }
            let denom = tot + aux_den;
            if p.track_other {
                if let Some(ab) = o_parts {
                    if ab.oth_tot != 0.0 {
                        let only = value::AuxBuckets {
                            term_tot: 0.0, term_sum: 0.0, crack: 0.0, hor_tot: 0.0, hor_sum: 0.0,
                            ..ab
                        };
                        let (o_num, o_den) = value::aux_opp_mix(&only, b.eval_weight,
                                                                b.eval_weight_min,
                                                                b.eval_weight_k,
                                                                p.other_cov_blend);
                        let v_ws = o_num / o_den;
                        let e_ws = evals.get(Some(h));
                        g.other.push((h, o_den / denom, if ow { v_ws } else { 1.0 - v_ws },
                                      if e_ws.is_nan() { f64::NAN }
                                      else if ow { e_ws } else { 1.0 - e_ws }));
                    }
                }
            }
            let mut const_base = if denom != 0.0 { aux_num / denom } else { 0.0 };
            for i in rg.clone() {
                let ch = g_edges.child[i];
                let pp = if denom != 0.0 { mass(i) / denom } else { 0.0 };
                // reply_share(e, tot)
                let share = if !g_edges.share[i].is_nan() { g_edges.share[i] }
                            else if tot != 0.0 { g_edges.total[i] as f64 / tot } else { 0.0 };
                let lv = value::leaf_edge_value(e.ws[i], g_edges.total[i] as f64,
                                                evals.get(ch), b);
                if share < p.share_floor || ch.is_none() {
                    const_base += pp * lv;
                    if let Some(c) = ch {
                        g.floor.push((pp, c, lv));
                    }
                    continue;
                }
                let c = ch.unwrap();
                let in_sub = reach_m.contains(&c) && is_parent(&c) && p_ply + 1 <= p.max_ply;
                g.kids.push((pp, ch, in_sub, lv));
            }
            g.k_ptr.push(g.kids.len() as i64);
            g.f_ptr.push(g.floor.len() as i64);
            g.const_base.push(const_base);
            g.nodes.push((h, false, row));
        }
    }
    g.named = named_counts;
    g.disputes = disputes;
    if p.crack_at_parents {
        let mut moved = 0i64;
        for oi in 0..g.const_base.len() {
            for k in g.k_ptr[oi] as usize..g.k_ptr[oi + 1] as usize {
                let (pp, ch, in_sub, lv) = g.kids[k];
                let o = if in_sub { ch.and_then(|c| our_index.get(&c)) } else { None };
                if let Some(&oi2) = o {
                    if let Some((cont, crack)) = g.crack[oi2] {
                        let a = cont / (cont + crack);
                        g.const_base[oi] += pp * (1.0 - a);
                        g.kids[k] = (pp * a, ch, in_sub, lv);
                        moved += 1;
                    }
                }
            }
        }
        g.crack_edges = Some(moved);
        if let Some(&ri) = our_index.get(&root) {
            g.root_crack = g.crack[ri];
        }
    }
    if let Some(ml) = p.gate.max_eval_loss {
        let eps = if p.gate.rare_floor.is_none() { 0.0 } else { gates::CP_EPS };
        let mut out = Vec::new();
        for (oi, &(h, our, _row)) in g.nodes.iter().filter(|n| n.1).enumerate() {
            debug_assert!(our);
            let cs = &g.cands[g.c_ptr[oi] as usize..g.c_ptr[oi + 1] as usize];
            if cs.is_empty() {
                continue;
            }
            // budget_core.loss_status
            let cps: Vec<f64> = cs.iter().filter_map(|c| {
                let v = evals.get(c.1);
                if v.is_nan() { None } else { Some(value::ws_to_cp(v, ow)) }
            }).collect();
            if cps.is_empty() {
                out.push((h, "unknown"));
                continue;
            }
            let own = evals.get(Some(h));
            if !own.is_nan() {
                let mut mx = cps[0];
                for &c in &cps[1..] {
                    if c > mx {
                        mx = c;
                    }
                }
                if value::ws_to_cp(own, ow) - mx > ml + eps {
                    out.push((h, "fallback"));
                }
            }
        }
        g.loss_exceptions = Some(out);
    }
    g.reach = reach_m;
    g.ply = ply_m;
    g.route = route;
    g
}
