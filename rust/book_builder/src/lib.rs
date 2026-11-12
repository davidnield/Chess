//! The budget-book builder's core in Rust (plan: docs/builder-port-and-128bit-plan-2026-11-12.md,
//! phase 3). Python imports the extension as `book_builder_rs`; budget_core stays the
//! reference, and python/_test_book_builder_rs.py holds every exported function to it at
//! tolerance 0. See README.md for the build and the exactness contract.

pub mod value;

use numpy::{PyArray1, PyArray2, PyReadonlyArray1, PyReadonlyArray2};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use value::{AuxRow, Blend};

fn blend(slice_prior: f64, our_white: bool, prior_strength: f64, eval_weight: f64,
         eval_weight_min: f64, eval_weight_k: f64) -> Blend {
    Blend { slice_prior, our_white, prior_strength, eval_weight, eval_weight_min, eval_weight_k }
}

fn same_len(n: usize, others: &[usize]) -> PyResult<()> {
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

#[pymodule]
fn book_builder_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cp_to_expected_score, m)?)?;
    m.add_function(wrap_pyfunction!(ws_to_cp, m)?)?;
    m.add_function(wrap_pyfunction!(leaf_edge_values, m)?)?;
    m.add_function(wrap_pyfunction!(leaf_node_values, m)?)?;
    m.add_function(wrap_pyfunction!(aux_mix, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
