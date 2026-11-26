use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use crate::human_policy::{self, Opponent};

#[pyfunction]
#[pyo3(signature=(counts,maia,strength,cap=None))]
pub fn own_policy(counts: Vec<f64>, maia: Vec<f64>, strength: f64, cap: Option<f64>) -> PyResult<Vec<f64>> {
    human_policy::own(&counts,&maia,strength,cap).map_err(PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature=(broad,adult,target,otb,maia,otb_strength,empirical_strength,maia_strength,backup_discount,broad_discount,gate=None,mix=None))]
#[allow(clippy::too_many_arguments)]
pub fn opponent_policy(broad: Vec<f64>, adult: Vec<f64>, target: Vec<f64>, otb: Vec<f64>, maia: Option<Vec<f64>>,
    otb_strength: f64, empirical_strength: f64, maia_strength: f64,
    backup_discount: f64, broad_discount: f64, gate: Option<f64>, mix: Option<f64>) -> PyResult<Vec<f64>> {
    human_policy::opponent(&broad,&adult,&target,&otb,maia.as_deref(),Opponent {
        otb:otb_strength,empirical:empirical_strength,maia:maia_strength,backup_discount,broad_discount,gate,mix,
    }).map_err(PyValueError::new_err)
}

#[pyfunction]
pub fn bounded_expectation(p: Vec<f64>, lower: Vec<f64>, upper: Vec<f64>) -> PyResult<(f64,f64)> {
    human_policy::expectation(&p,&lower,&upper).map_err(PyValueError::new_err)
}

#[pyfunction]
pub fn continuation_utility(cp: Vec<f64>, white: bool) -> Vec<f64> {
    cp.into_iter().map(|v| if v == -32768. || !v.is_finite() { f64::NAN } else { human_policy::utility(v,white) }).collect()
}
