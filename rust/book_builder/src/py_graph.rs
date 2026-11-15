//! The Python binding of graph::build_graph (budget_rs.build_graph_rs builds its inputs).

use numpy::ndarray::ArrayView2;
use numpy::{PyArray1, PyReadonlyArray1, PyReadonlyArray2};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::gates::{Cand, GateParams, Ref};
use crate::value::AuxRow;
use crate::{blend, csr, graph, key, lo, reach, same_len};

fn opt_keys(k: &[i64], has: &[bool]) -> Vec<Option<reach::Key>> {
    k.iter().zip(has).map(|(&c, &h)| if h { Some(key(c)) } else { None }).collect()
}

fn aux_rows(tt: &ArrayView2<f64>, tw: &ArrayView2<f64>, tb: &ArrayView2<f64>,
            td: &ArrayView2<f64>, o: &ArrayView2<f64>) -> PyResult<Vec<Option<AuxRow>>> {
    let n = o.nrows();
    for a in [tt, tw, tb, td] {
        if a.nrows() != n || a.ncols() != 3 {
            return Err(PyValueError::new_err("term arrays must be (n, 3)"));
        }
    }
    if o.ncols() != 9 {
        return Err(PyValueError::new_err("other must be (n, 9)"));
    }
    Ok((0..n).map(|i| {
        if o[[i, 8]] == 0.0 {
            return None;
        }
        Some(AuxRow {
            term_total: [tt[[i, 0]], tt[[i, 1]], tt[[i, 2]]],
            term_white_wins: [tw[[i, 0]], tw[[i, 1]], tw[[i, 2]]],
            term_black_wins: [tb[[i, 0]], tb[[i, 1]], tb[[i, 2]]],
            term_draws: [td[[i, 0]], td[[i, 1]], td[[i, 2]]],
            other_total: o[[i, 0]], other_white_wins: o[[i, 1]], other_draws: o[[i, 2]],
            other_eval_mean: o[[i, 3]], other_eval_cov: o[[i, 4]], horizon_total: o[[i, 5]],
            horizon_white_wins: o[[i, 6]], horizon_draws: o[[i, 7]],
        })
    }).collect())
}

fn arr<'py, T: numpy::Element>(d: &Bound<'py, PyDict>, k: &str)
                               -> PyResult<PyReadonlyArray1<'py, T>> {
    d.get_item(k)?.ok_or_else(|| PyValueError::new_err(format!("missing {k}")))?
        .extract::<PyReadonlyArray1<T>>().map_err(|e| PyValueError::new_err(format!("{k}: {e}")))
}

fn arr2<'py>(d: &Bound<'py, PyDict>, k: &str) -> PyResult<PyReadonlyArray2<'py, f64>> {
    d.get_item(k)?.ok_or_else(|| PyValueError::new_err(format!("missing aux {k}")))?
        .extract::<PyReadonlyArray2<f64>>().map_err(|e| PyValueError::new_err(format!("{k}: {e}")))
}

/// budget_core.build_graph into CSR arrays (graph.rs). `edges` and `rows` are dicts of
/// arrays (budget_rs.build_graph_rs documents and builds them); `aux` is None (no aux rows)
/// or the aux_mix arrays, one row per parent. Returns a dict of arrays and meta.
#[pyfunction(signature=(edges,rows,aux,eval_keys,eval_vals,root,our_white,slice_prior,
    prior_strength,eval_weight,eval_weight_min,eval_weight_k,eps,max_ply,share_floor,
    robustness_floor,gate_rel_floor,gate_rel_own_margin,require_eval,gates,max_eval_loss,
    rare_floor,strict_source,other_cov_blend,gated_reach,crack_at_parents,engine_moves_given,
    track_route_ply,track_other,max_score_loss=None))]
#[allow(clippy::too_many_arguments)]
pub fn build_graph_csr<'py>(
    py: Python<'py>, edges: &Bound<'py, PyDict>, rows: &Bound<'py, PyDict>,
    aux: Option<&Bound<'py, PyDict>>, eval_keys: PyReadonlyArray1<'py, i64>,
    eval_vals: PyReadonlyArray1<'py, f64>, root: i64, our_white: bool, slice_prior: f64,
    prior_strength: f64, eval_weight: f64, eval_weight_min: f64, eval_weight_k: f64,
    eps: f64, max_ply: i64, share_floor: f64, robustness_floor: f64, gate_rel_floor: f64,
    gate_rel_own_margin: f64, require_eval: bool, gates: bool, max_eval_loss: f64,
    rare_floor: i64, strict_source: bool, other_cov_blend: bool, gated_reach: bool,
    crack_at_parents: bool, engine_moves_given: bool, track_route_ply: bool,
    track_other: bool, max_score_loss: Option<f64>,
) -> PyResult<Bound<'py, PyDict>> {
    let pa = arr::<i64>(edges, "parents")?;
    let par: Vec<reach::Key> = pa.as_slice()?.iter().map(|&h| key(h)).collect();
    let n = par.len();
    let (ep, ec, eh, et, es) = (arr::<i64>(edges, "ptr")?, arr::<i64>(edges, "child")?,
                                arr::<bool>(edges, "has_child")?, arr::<i64>(edges, "total")?,
                                arr::<f64>(edges, "share")?);
    let (esan, ebare, ews, emass, eadd) = (arr::<i64>(edges, "san")?, arr::<i64>(edges, "bare")?,
                                           arr::<f64>(edges, "ws")?,
                                           arr::<f64>(edges, "reply_mass")?,
                                           arr::<i64>(edges, "additional")?);
    let (ep, ec, eh, et, es) = (ep.as_slice()?, ec.as_slice()?, eh.as_slice()?, et.as_slice()?,
                                es.as_slice()?);
    let ne = ec.len();
    same_len(ne, &[eh.len(), et.len(), es.len(), esan.as_slice()?.len(), ebare.as_slice()?.len(), ews.as_slice()?.len(),
                   emass.as_slice()?.len(), eadd.as_slice()?.len()])?;
    csr(ep, n, ne, "edges.ptr")?;
    let child = opt_keys(ec, eh);

    let (ssan, sbare, sch, shas, named) = (arr::<i64>(rows, "src_san")?, arr::<i64>(rows, "src_bare")?,
                                           arr::<i64>(rows, "src_child")?,
                                           arr::<bool>(rows, "src_has_child")?,
                                           arr::<bool>(rows, "named")?);
    same_len(n, &[ssan.as_slice()?.len(), sbare.as_slice()?.len(), sch.as_slice()?.len(), shas.as_slice()?.len(), named.as_slice()?.len()])?;
    let src_child = opt_keys(sch.as_slice()?, shas.as_slice()?);
    let (mp, msan, mbare, mch, mhas) = (arr::<i64>(rows, "m_ptr")?, arr::<i64>(rows, "m_san")?,
                                        arr::<i64>(rows, "m_bare")?, arr::<i64>(rows, "m_child")?,
                                        arr::<bool>(rows, "m_has_child")?);
    let m_child = opt_keys(mch.as_slice()?, mhas.as_slice()?);
    same_len(m_child.len(), &[msan.as_slice()?.len(), mbare.as_slice()?.len()])?;
    csr(mp.as_slice()?, n, m_child.len(), "rows.m_ptr")?;
    let (vp, vb, xp, xb) = (arr::<i64>(rows, "v_ptr")?, arr::<i64>(rows, "v_bare")?,
                            arr::<i64>(rows, "x_ptr")?, arr::<i64>(rows, "x_bare")?);
    csr(vp.as_slice()?, n, vb.as_slice()?.len(), "rows.v_ptr")?;
    csr(xp.as_slice()?, n, xb.as_slice()?.len(), "rows.x_ptr")?;

    // evals, sorted in the crate's key order for bisection
    let (ek, ev) = (eval_keys.as_slice()?, eval_vals.as_slice()?);
    same_len(ek.len(), &[ev.len()])?;
    let mut ord: Vec<usize> = (0..ek.len()).collect();
    ord.sort_unstable_by_key(|&i| key(ek[i]));
    let ekeys: Vec<reach::Key> = ord.iter().map(|&i| key(ek[i])).collect();
    let evals_v: Vec<f64> = ord.iter().map(|&i| ev[i]).collect();
    if ekeys.windows(2).any(|w| w[0] == w[1]) {
        return Err(PyValueError::new_err("eval keys must be distinct"));
    }
    let evals = graph::Evals { keys: &ekeys, vals: &evals_v };
    let (msan, mbare) = (msan.as_slice()?, mbare.as_slice()?);
    let m: Vec<Cand> = (0..m_child.len()).map(|j| Cand {
        r: Ref::Engine(j), san: msan[j], bare: mbare[j], eval: evals.get(m_child[j]),
        total: 0, engine: true,
    }).collect();
    let aux_v = match aux {
        None => None,
        Some(d) => {
            let (tt, tw, tb, td, o) = (arr2(d, "term_total")?, arr2(d, "term_white_wins")?,
                                       arr2(d, "term_black_wins")?, arr2(d, "term_draws")?,
                                       arr2(d, "other")?);
            let v = aux_rows(&tt.as_array(), &tw.as_array(), &tb.as_array(), &td.as_array(),
                             &o.as_array())?;
            if v.len() != n {
                return Err(PyValueError::new_err("aux arrays must have one row per parent"));
            }
            Some(v)
        }
    };
    let ge = reach::Edges { parents: &par, ptr: ep, child: &child, total: et, share: es,
                            admitted: None };
    if ge.row_of().len() != n {
        return Err(PyValueError::new_err("parents must be distinct"));
    }
    if prior_strength == 0.0 && et.contains(&0) {
        return Err(PyValueError::new_err(
            "prior strength 0 with a zero-game edge is 0/0 (Python raises ZeroDivisionError)"));
    }
    let ecols = graph::EdgeCols { san: esan.as_slice()?, bare: ebare.as_slice()?,
                                  ws: ews.as_slice()?, reply_mass: emass.as_slice()?,
                                  additional: eadd.as_slice()? };
    let extras = graph::RowExtras {
        src_san: ssan.as_slice()?, src_bare: sbare.as_slice()?, src_child: &src_child,
        named: named.as_slice()?, aux: aux_v.as_deref(), m_ptr: mp.as_slice()?, m: &m,
        m_child: &m_child, v_ptr: vp.as_slice()?, v_bare: vb.as_slice()?,
        x_ptr: xp.as_slice()?, x_bare: xb.as_slice()?,
    };
    let params = graph::Params {
        our_white,
        blend: blend(slice_prior, our_white, prior_strength, eval_weight, eval_weight_min,
                     eval_weight_k),
        eps, max_ply, share_floor,
        gate: GateParams {
            our_white, slice_prior, robustness_floor, gate_rel_floor, gate_rel_own_margin,
            require_eval, gates,
            max_eval_loss: if max_eval_loss.is_nan() { None } else { Some(max_eval_loss) },
            max_score_loss: crate::score_limit(max_score_loss, max_eval_loss)?,
            rare_floor: if rare_floor < 0 { None } else { Some(rare_floor) },
            strict_source,
        },
        other_cov_blend, gated_reach, crack_at_parents, engine_moves_given, track_route_ply,
        track_other,
    };
    let g = graph::build_graph(&ge, &ecols, &extras, &evals, key(root), &params);

    let out = PyDict::new(py);
    let ks = |m: &[reach::Key]| m.iter().map(|&k| lo(k)).collect::<Vec<i64>>();
    let set_f = |k: &str, v: Vec<f64>| out.set_item(k, PyArray1::from_vec(py, v));
    let set_i = |k: &str, v: Vec<i64>| out.set_item(k, PyArray1::from_vec(py, v));
    let set_b = |k: &str, v: Vec<bool>| out.set_item(k, PyArray1::from_vec(py, v));
    set_i("reach_keys", ks(&g.reach.keys))?;
    set_f("reach_vals", g.reach.vals.clone())?;
    set_i("ply_keys", ks(&g.ply.keys))?;
    set_i("ply_vals", g.ply.vals.clone())?;
    set_i("route_keys", ks(&g.route.keys))?;
    set_i("route_vals", g.route.vals.clone())?;
    set_i("node_keys", g.nodes.iter().map(|n| lo(n.0)).collect())?;
    set_b("node_our", g.nodes.iter().map(|n| n.1).collect())?;
    set_f("l_node", g.l_node.clone())?;
    set_f("crack_cont", g.crack.iter().map(|c| c.map_or(f64::NAN, |x| x.0)).collect())?;
    set_f("crack_mass", g.crack.iter().map(|c| c.map_or(f64::NAN, |x| x.1)).collect())?;
    set_i("c_ptr", g.c_ptr.clone())?;
    set_i("c_san", g.cands.iter().map(|c| c.0).collect())?;
    set_i("c_child", g.cands.iter().map(|c| c.1.map_or(0, lo)).collect())?;
    set_b("c_has_child", g.cands.iter().map(|c| c.1.is_some()).collect())?;
    set_b("c_in_sub", g.cands.iter().map(|c| c.2).collect())?;
    set_f("c_lv", g.cands.iter().map(|c| c.3).collect())?;
    set_f("const_base", g.const_base.clone())?;
    set_i("k_ptr", g.k_ptr.clone())?;
    set_f("k_p", g.kids.iter().map(|k| k.0).collect())?;
    set_i("k_child", g.kids.iter().map(|k| k.1.map_or(0, lo)).collect())?;
    set_b("k_has_child", g.kids.iter().map(|k| k.1.is_some()).collect())?;
    set_b("k_in_sub", g.kids.iter().map(|k| k.2).collect())?;
    set_f("k_lv", g.kids.iter().map(|k| k.3).collect())?;
    set_i("f_ptr", g.f_ptr.clone())?;
    set_f("f_p", g.floor.iter().map(|f| f.0).collect())?;
    set_i("f_child", g.floor.iter().map(|f| lo(f.1)).collect())?;
    set_f("f_lv", g.floor.iter().map(|f| f.2).collect())?;
    out.set_item("engine_cands", g.engine_cands)?;
    out.set_item("src_rejected", g.src_rejected)?;
    out.set_item("named", g.named)?;
    out.set_item("disputes", g.disputes.map(|v| v.into_iter().map(|(h, d)| {
        (lo(h), d.reference_cp, d.threats, d.at_risk, d.fallback, d.unresolved)
    }).collect::<Vec<_>>()))?;
    out.set_item("crack_edges", g.crack_edges)?;
    out.set_item("root_crack", g.root_crack)?;
    out.set_item("loss_exceptions", g.loss_exceptions.map(|v| v.into_iter()
        .map(|(h, s)| (lo(h), s)).collect::<Vec<_>>()))?;
    out.set_item("other", g.other.iter().map(|o| (lo(o.0), o.1, o.2, o.3)).collect::<Vec<_>>())?;
    Ok(out)
}
