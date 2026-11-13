//! The budget-book builder's core in Rust (plan: docs/builder-port-and-128bit-plan-2026-11-12.md,
//! phase 3). Python imports the extension as `book_builder_rs`; budget_core stays the
//! reference, and python/_test_book_builder_rs.py holds every exported function to it at
//! tolerance 0. See README.md for the build and the exactness contract.

pub mod collect;
pub mod core;
pub mod dp;
pub mod gates;
pub mod graph;
pub mod polish;
pub mod price;
mod py_collect;
mod py_core;
mod py_graph;
mod py_rollout;
pub mod reach;
pub mod rollout;
pub mod value;

use numpy::{PyArray1, PyArray2, PyReadonlyArray1, PyReadonlyArray2};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use gates::{Cand, GateParams, NodeIn, Ref};

use value::{AuxRow, Blend};

pub(crate) fn blend(slice_prior: f64, our_white: bool, prior_strength: f64, eval_weight: f64,
         eval_weight_min: f64, eval_weight_k: f64) -> Blend {
    Blend { slice_prior, our_white, prior_strength, eval_weight, eval_weight_min, eval_weight_k }
}

pub(crate) fn same_len(n: usize, others: &[usize]) -> PyResult<()> {
    if others.iter().any(|&m| m != n) {
        return Err(PyValueError::new_err("arrays differ in length"));
    }
    Ok(())
}

/// cp_to_expected_score over an array of centipawns.
#[pyfunction]
fn cp_to_expected_score<'py>(py: Python<'py>, cp: PyReadonlyArray1<'py, f64>)
                             -> PyResult<Bound<'py, PyArray1<f64>>> {
    let v: Vec<f64> = cp.as_slice()?.iter().map(|&c| value::cp_to_expected_score(c)).collect();
    Ok(PyArray1::from_vec(py, v))
}

/// ws_to_cp over an array of expected scores.
#[pyfunction]
fn ws_to_cp<'py>(py: Python<'py>, ws: PyReadonlyArray1<'py, f64>, our_white: bool)
                 -> PyResult<Bound<'py, PyArray1<f64>>> {
    let v: Vec<f64> = ws.as_slice()?.iter().map(|&w| value::ws_to_cp(w, our_white)).collect();
    Ok(PyArray1::from_vec(py, v))
}

/// leaf_edge_value per edge; eval_ws NaN = None.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn leaf_edge_values<'py>(py: Python<'py>, ws_avg: PyReadonlyArray1<'py, f64>,
                         total: PyReadonlyArray1<'py, i64>, eval_ws: PyReadonlyArray1<'py, f64>,
                         slice_prior: f64, our_white: bool, prior_strength: f64,
                         eval_weight: f64, eval_weight_min: f64, eval_weight_k: f64)
                         -> PyResult<Bound<'py, PyArray1<f64>>> {
    let (w, t, e) = (ws_avg.as_slice()?, total.as_slice()?, eval_ws.as_slice()?);
    same_len(w.len(), &[t.len(), e.len()])?;
    let b = blend(slice_prior, our_white, prior_strength, eval_weight, eval_weight_min,
                  eval_weight_k);
    let v: Vec<f64> = (0..w.len())
        .map(|i| value::leaf_edge_value(w[i], t[i] as f64, e[i], &b))
        .collect();
    Ok(PyArray1::from_vec(py, v))
}

/// leaf_node_value per node: node i's edges are ws/totals[ptr[i]..ptr[i+1]].
#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn leaf_node_values<'py>(py: Python<'py>, ptr: PyReadonlyArray1<'py, i64>,
                         ws: PyReadonlyArray1<'py, f64>, totals: PyReadonlyArray1<'py, i64>,
                         own_eval_ws: PyReadonlyArray1<'py, f64>, slice_prior: f64,
                         our_white: bool, prior_strength: f64, eval_weight: f64,
                         eval_weight_min: f64, eval_weight_k: f64)
                         -> PyResult<Bound<'py, PyArray1<f64>>> {
    let (p, w, t, e) = (ptr.as_slice()?, ws.as_slice()?, totals.as_slice()?, own_eval_ws.as_slice()?);
    same_len(w.len(), &[t.len()])?;
    if p.len() != e.len() + 1 || p.first() != Some(&0) || *p.last().unwrap() as usize != w.len() {
        return Err(PyValueError::new_err("ptr must start at 0, end at len(ws), one per node + 1"));
    }
    let b = blend(slice_prior, our_white, prior_strength, eval_weight, eval_weight_min,
                  eval_weight_k);
    let v: Vec<f64> = (0..e.len())
        .map(|i| {
            let (a, z) = (p[i] as usize, p[i + 1] as usize);
            value::leaf_node_value(&w[a..z], &t[a..z], e[i], &b)
        })
        .collect();
    Ok(PyArray1::from_vec(py, v))
}

/// aux_row_buckets then aux_opp_mix per row. Term columns are (n, 3) in the order
/// term_normal, term_other, term_flag; `other` is (n, 9): other_total, other_white_wins,
/// other_draws, other_eval_mean (NaN = None), other_eval_cov, horizon_total,
/// horizon_white_wins, horizon_draws, present (0 = no aux row). Returns (buckets (n, 9),
/// mix (n, 2)).
#[pyfunction]
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn aux_mix<'py>(py: Python<'py>, term_total: PyReadonlyArray2<'py, f64>,
                term_white_wins: PyReadonlyArray2<'py, f64>,
                term_black_wins: PyReadonlyArray2<'py, f64>,
                term_draws: PyReadonlyArray2<'py, f64>, other: PyReadonlyArray2<'py, f64>,
                our_white: bool, eval_weight: f64, eval_weight_min: f64, eval_weight_k: f64,
                cov_blend: bool)
                -> PyResult<(Bound<'py, PyArray2<f64>>, Bound<'py, PyArray2<f64>>)> {
    let (tt, tw, tb, td, o) = (term_total.as_array(), term_white_wins.as_array(),
                               term_black_wins.as_array(), term_draws.as_array(),
                               other.as_array());
    let n = o.nrows();
    for a in [&tt, &tw, &tb, &td] {
        if a.nrows() != n || a.ncols() != 3 {
            return Err(PyValueError::new_err("term arrays must be (n, 3)"));
        }
    }
    if o.ncols() != 9 {
        return Err(PyValueError::new_err("other must be (n, 9)"));
    }
    let mut buckets = Vec::with_capacity(n);
    let mut mix = Vec::with_capacity(n);
    for i in 0..n {
        let row = AuxRow {
            term_total: [tt[[i, 0]], tt[[i, 1]], tt[[i, 2]]],
            term_white_wins: [tw[[i, 0]], tw[[i, 1]], tw[[i, 2]]],
            term_black_wins: [tb[[i, 0]], tb[[i, 1]], tb[[i, 2]]],
            term_draws: [td[[i, 0]], td[[i, 1]], td[[i, 2]]],
            other_total: o[[i, 0]],
            other_white_wins: o[[i, 1]],
            other_draws: o[[i, 2]],
            other_eval_mean: o[[i, 3]],
            other_eval_cov: o[[i, 4]],
            horizon_total: o[[i, 5]],
            horizon_white_wins: o[[i, 6]],
            horizon_draws: o[[i, 7]],
        };
        let x = value::aux_row_buckets(if o[[i, 8]] != 0.0 { Some(&row) } else { None }, our_white);
        buckets.push(vec![x.term_tot, x.term_sum, x.crack, x.oth_tot, x.oth_sum, x.oth_eval,
                          x.oth_cov, x.hor_tot, x.hor_sum]);
        let (nu, de) = value::aux_opp_mix(&x, eval_weight, eval_weight_min, eval_weight_k, cov_blend);
        mix.push(vec![nu, de]);
    }
    Ok((PyArray2::from_vec2(py, &buckets)?, PyArray2::from_vec2(py, &mix)?))
}

pub(crate) fn csr(ptr: &[i64], n: usize, len: usize, what: &str) -> PyResult<()> {
    if ptr.len() != n + 1 || ptr[0] != 0 || ptr[n] as usize != len
        || ptr.windows(2).any(|w| w[1] < w[0]) {
        return Err(PyValueError::new_err(format!("{what}: bad CSR pointer")));
    }
    Ok(())
}

/// final_candidates for many our-nodes (budget_core.final_candidates, max_cands 0).
///
/// Nodes: own eval, source move (san/bare code, -1 = none) and the restored source move's
/// child eval. Recorded edges e_* and engine proposals m_* in CSR (san/bare codes, child
/// eval, total); verified (v_*) and vetoed (x_*) bare codes in CSR. SAN codes must be
/// assigned in sorted SAN order. max_eval_loss NaN = off, rare_floor < 0 = off.
///
/// Returns (k_ptr, k_ref, restored, n_rej, n_eng, disputes): k_ref >= 0 is a global
/// recorded-edge index, -(1 + j) the global engine proposal j; disputes is a list of
/// (node, reference_cp or None, [(san, cp)], [san], fallback, unresolved).
#[pyfunction]
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn final_candidates_batch<'py>(
    py: Python<'py>,
    own: PyReadonlyArray1<'py, f64>, src_san: PyReadonlyArray1<'py, i64>,
    src_bare: PyReadonlyArray1<'py, i64>, src_eval: PyReadonlyArray1<'py, f64>,
    e_ptr: PyReadonlyArray1<'py, i64>, e_san: PyReadonlyArray1<'py, i64>,
    e_bare: PyReadonlyArray1<'py, i64>, e_eval: PyReadonlyArray1<'py, f64>,
    e_total: PyReadonlyArray1<'py, i64>,
    m_ptr: PyReadonlyArray1<'py, i64>, m_san: PyReadonlyArray1<'py, i64>,
    m_bare: PyReadonlyArray1<'py, i64>, m_eval: PyReadonlyArray1<'py, f64>,
    v_ptr: PyReadonlyArray1<'py, i64>, v_bare: PyReadonlyArray1<'py, i64>,
    x_ptr: PyReadonlyArray1<'py, i64>, x_bare: PyReadonlyArray1<'py, i64>,
    our_white: bool, slice_prior: f64, robustness_floor: f64, gate_rel_floor: f64,
    gate_rel_own_margin: f64, require_eval: bool, gates: bool, max_eval_loss: f64,
    rare_floor: i64, strict_source: bool, want_disputes: bool,
) -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<bool>>,
               Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>, Bound<'py, PyList>)> {
    let (own, ss, sb, se) = (own.as_slice()?, src_san.as_slice()?, src_bare.as_slice()?,
                             src_eval.as_slice()?);
    let n = own.len();
    same_len(n, &[ss.len(), sb.len(), se.len()])?;
    let (ep, es, eb, ee, et) = (e_ptr.as_slice()?, e_san.as_slice()?, e_bare.as_slice()?,
                                e_eval.as_slice()?, e_total.as_slice()?);
    same_len(es.len(), &[eb.len(), ee.len(), et.len()])?;
    csr(ep, n, es.len(), "e_ptr")?;
    let (mp, ms, mb, me) = (m_ptr.as_slice()?, m_san.as_slice()?, m_bare.as_slice()?,
                            m_eval.as_slice()?);
    same_len(ms.len(), &[mb.len(), me.len()])?;
    csr(mp, n, ms.len(), "m_ptr")?;
    let (vp, vb, xp, xb) = (v_ptr.as_slice()?, v_bare.as_slice()?, x_ptr.as_slice()?,
                            x_bare.as_slice()?);
    csr(vp, n, vb.len(), "v_ptr")?;
    csr(xp, n, xb.len(), "x_ptr")?;
    let p = GateParams {
        our_white, slice_prior, robustness_floor, gate_rel_floor, gate_rel_own_margin,
        require_eval, gates,
        max_eval_loss: if max_eval_loss.is_nan() { None } else { Some(max_eval_loss) },
        rare_floor: if rare_floor < 0 { None } else { Some(rare_floor) },
        strict_source,
    };
    let edges: Vec<Cand> = (0..es.len()).map(|i| Cand {
        r: Ref::Edge(i), san: es[i], bare: eb[i], eval: ee[i], total: et[i], engine: false,
    }).collect();
    let props: Vec<Cand> = (0..ms.len()).map(|i| Cand {
        r: Ref::Engine(i), san: ms[i], bare: mb[i], eval: me[i], total: 0, engine: true,
    }).collect();
    let (mut k_ptr, mut k_ref) = (vec![0i64], Vec::new());
    let (mut restored, mut n_rej, mut n_eng) = (Vec::with_capacity(n), Vec::with_capacity(n),
                                                Vec::with_capacity(n));
    let disputes = PyList::empty(py);
    for i in 0..n {
        let r = |p: &[i64]| p[i] as usize..p[i + 1] as usize;
        let node = NodeIn {
            own: own[i], src: if ss[i] < 0 { None } else { Some(ss[i]) }, src_bare: sb[i],
            src_eval: se[i], recorded: &edges[r(ep)], engine: &props[r(mp)],
            verified: &vb[r(vp)], veto: &xb[r(xp)],
        };
        let o = gates::final_candidates(&node, &p, want_disputes);
        for k in &o.kept {
            k_ref.push(match *k {
                Ref::Edge(j) => j as i64,
                Ref::Engine(j) => -(1 + j as i64),
            });
        }
        k_ptr.push(k_ref.len() as i64);
        restored.push(o.restored);
        n_rej.push(o.n_rej);
        n_eng.push(o.n_eng);
        if let Some(d) = o.dispute {
            disputes.append((i, d.reference_cp, d.threats, d.at_risk, d.fallback, d.unresolved))?;
        }
    }
    Ok((PyArray1::from_vec(py, k_ptr), PyArray1::from_vec(py, k_ref),
        PyArray1::from_vec(py, restored), PyArray1::from_vec(py, n_rej),
        PyArray1::from_vec(py, n_eng), disputes))
}

/// A 64-bit position key from Python (zobrist_int64) as the crate's Key (high half 0).
pub(crate) fn key(lo: i64) -> reach::Key {
    lo as u64 as reach::Key
}

pub(crate) fn lo(k: reach::Key) -> i64 {
    k as u64 as i64
}

/// optimistic_reach, then (optionally) route_plies. Edges in CSR over `parents`
/// (edges_by_parent's keys and edge orders); e_has_child 0 = child_hash None; e_share NaN =
/// no reach_share. adm_ptr empty = admitted None. Returns (reach keys, reach values, ply
/// keys, ply values, route keys, route plies), each in Python's dict insertion order (route
/// empty unless track_route_ply).
#[pyfunction]
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn reach_batch<'py>(
    py: Python<'py>, parents: PyReadonlyArray1<'py, i64>, e_ptr: PyReadonlyArray1<'py, i64>,
    e_child: PyReadonlyArray1<'py, i64>, e_has_child: PyReadonlyArray1<'py, bool>,
    e_total: PyReadonlyArray1<'py, i64>, e_share: PyReadonlyArray1<'py, f64>,
    adm_ptr: PyReadonlyArray1<'py, i64>, adm_child: PyReadonlyArray1<'py, i64>,
    root: i64, root_our: bool, eps: f64, max_ply: i64, share_floor: f64,
    track_route_ply: bool,
) -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<f64>>, Bound<'py, PyArray1<i64>>,
               Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>)> {
    let par: Vec<reach::Key> = parents.as_slice()?.iter().map(|&h| key(h)).collect();
    let (ep, ec, eh, et, es) = (e_ptr.as_slice()?, e_child.as_slice()?, e_has_child.as_slice()?,
                                e_total.as_slice()?, e_share.as_slice()?);
    same_len(ec.len(), &[eh.len(), et.len(), es.len()])?;
    csr(ep, par.len(), ec.len(), "e_ptr")?;
    let child: Vec<Option<reach::Key>> = ec.iter().zip(eh)
        .map(|(&c, &has)| if has { Some(key(c)) } else { None }).collect();
    let (ap, ac) = (adm_ptr.as_slice()?, adm_child.as_slice()?);
    let adm_keys: Vec<reach::Key> = ac.iter().map(|&c| key(c)).collect();
    let admitted = if ap.is_empty() {
        None
    } else {
        csr(ap, par.len(), ac.len(), "adm_ptr")?;
        Some((ap, adm_keys.as_slice()))
    };
    let g = reach::Edges { parents: &par, ptr: ep, child: &child, total: et, share: es, admitted };
    let rows = g.row_of();
    if rows.len() != par.len() {
        return Err(PyValueError::new_err("parents must be distinct"));
    }
    let (r, p) = reach::optimistic_reach(&g, &rows, key(root), root_our, eps, max_ply, share_floor);
    let route = if track_route_ply {
        reach::route_plies(&g, &rows, key(root), root_our, &r, &p, max_ply, share_floor)
    } else {
        reach::OrderedMap::default()
    };
    let ks = |m: &[reach::Key]| m.iter().map(|&k| lo(k)).collect::<Vec<i64>>();
    Ok((PyArray1::from_vec(py, ks(&r.keys)), PyArray1::from_vec(py, r.vals),
        PyArray1::from_vec(py, ks(&p.keys)), PyArray1::from_vec(py, p.vals),
        PyArray1::from_vec(py, ks(&route.keys)), PyArray1::from_vec(py, route.vals)))
}

#[pymodule]
fn book_builder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cp_to_expected_score, m)?)?;
    m.add_function(wrap_pyfunction!(ws_to_cp, m)?)?;
    m.add_function(wrap_pyfunction!(leaf_edge_values, m)?)?;
    m.add_function(wrap_pyfunction!(leaf_node_values, m)?)?;
    m.add_function(wrap_pyfunction!(aux_mix, m)?)?;
    m.add_function(wrap_pyfunction!(final_candidates_batch, m)?)?;
    m.add_function(wrap_pyfunction!(reach_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_graph::build_graph_csr, m)?)?;
    m.add_class::<py_core::RsGraph>()?;
    m.add_class::<py_collect::RsCollector>()?;
    m.add_class::<py_rollout::RsRollouts>()?;
    m.add_function(wrap_pyfunction!(py_rollout::terminal_outcomes, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
