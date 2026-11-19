//! RsCollector: collect.rs for Python (budget_rs.collect_subgraph_edges_rs drives it).

use std::collections::HashMap;

use numpy::{PyArray1, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::collect::{Collector, Gate, Level};
use crate::gates::GateParams;
use crate::{key, lo};

#[pyclass(module = "book_builder_rs")]
pub struct RsCollector {
    c: Collector,
}

fn keys_of(k: &[i64]) -> Vec<crate::reach::Key> {
    k.iter().map(|&x| key(x)).collect()
}

fn opt_keys(k: &[i64], has: &[bool]) -> Vec<Option<crate::reach::Key>> {
    k.iter().zip(has).map(|(&x, &h)| if h { Some(key(x)) } else { None }).collect()
}

fn pairs(k: &[i64], v: &[i64]) -> HashMap<crate::reach::Key, Vec<i64>> {
    let mut m: HashMap<crate::reach::Key, Vec<i64>> = HashMap::new();
    for (&a, &b) in k.iter().zip(v) {
        m.entry(key(a)).or_default().push(b);
    }
    m
}

#[pymethods]
impl RsCollector {
    #[new]
    fn new(root: i64, our_white: bool, eps: f64, max_ply: i64, share_floor: f64) -> Self {
        RsCollector { c: Collector::new(key(root), our_white, eps, max_ply, share_floor) }
    }

    fn set_source_children(&mut self, parents: PyReadonlyArray1<i64>, children: PyReadonlyArray1<i64>) -> PyResult<()> {
        let (p, c) = (parents.as_slice()?, children.as_slice()?);
        crate::same_len(p.len(), &[c.len()])?;
        self.c.source_children = p.iter().zip(c).map(|(&h,&ch)| (key(h), key(ch))).collect();
        Ok(())
    }

    /// GateAdmit: the source moves (first per hash wins, as IncumbentMoves.get), vetoes and
    /// verified moves as (key, bare code) pairs, the seen cache (NaN = no eval) and the gates
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature=(src_keys,src_sans,ex_keys,ex_bare,ver_keys,ver_bare,seen_keys,seen_vals,
        slice_prior,robustness_floor,gate_rel_floor,gate_rel_own_margin,gates,max_eval_loss,
        rare_floor,max_score_loss=None))]
    fn set_gate(&mut self, src_keys: PyReadonlyArray1<i64>, src_sans: PyReadonlyArray1<i64>,
                ex_keys: PyReadonlyArray1<i64>, ex_bare: PyReadonlyArray1<i64>,
                ver_keys: PyReadonlyArray1<i64>, ver_bare: PyReadonlyArray1<i64>,
                seen_keys: PyReadonlyArray1<i64>, seen_vals: PyReadonlyArray1<f64>,
                slice_prior: f64, robustness_floor: f64, gate_rel_floor: f64,
                gate_rel_own_margin: f64, gates: bool, max_eval_loss: f64, rare_floor: i64,
                max_score_loss: Option<f64>)
                -> PyResult<()> {
        let (sk, ss) = (src_keys.as_slice()?, src_sans.as_slice()?);
        let (xk, xb) = (ex_keys.as_slice()?, ex_bare.as_slice()?);
        let (vk, vb) = (ver_keys.as_slice()?, ver_bare.as_slice()?);
        let (ck, cv) = (seen_keys.as_slice()?, seen_vals.as_slice()?);
        crate::same_len(sk.len(), &[ss.len()])?;
        crate::same_len(xk.len(), &[xb.len()])?;
        crate::same_len(vk.len(), &[vb.len()])?;
        crate::same_len(ck.len(), &[cv.len()])?;
        let mut src = HashMap::with_capacity(sk.len());
        for (&k, &s) in sk.iter().zip(ss) {
            src.entry(key(k)).or_insert(s);
        }
        self.c.gate = Some(Gate {
            params: GateParams {
                our_white: self.c.our_white, slice_prior, robustness_floor, gate_rel_floor,
                gate_rel_own_margin, require_eval: gates, gates,
                max_eval_loss: if max_eval_loss.is_nan() { None } else { Some(max_eval_loss) },
                max_score_loss: crate::score_limit(max_score_loss, max_eval_loss)?,
                rare_floor: if rare_floor < 0 { None } else { Some(rare_floor) },
                strict_source: false,
            },
            src, excluded: pairs(xk, xb), verified: pairs(vk, vb),
            cache: ck.iter().zip(cv).map(|(&k, &v)| (key(k), v)).collect(),
        });
        Ok(())
    }

    /// engine_moves: per parent, its proposals in order
    fn set_engine(&mut self, keys: PyReadonlyArray1<i64>, sans: PyReadonlyArray1<i64>,
                  bares: PyReadonlyArray1<i64>, child: PyReadonlyArray1<i64>,
                  has_child: PyReadonlyArray1<bool>) -> PyResult<()> {
        let (k, s, b, c, h) = (keys.as_slice()?, sans.as_slice()?, bares.as_slice()?,
                               child.as_slice()?, has_child.as_slice()?);
        crate::same_len(k.len(), &[s.len(), b.len(), c.len(), h.len()])?;
        for i in 0..k.len() {
            self.c.engine.entry(key(k[i])).or_default()
                .push((s[i], b[i], if h[i] { Some(key(c[i])) } else { None }));
        }
        Ok(())
    }

    fn frontier<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i64>> {
        PyArray1::from_vec(py, self.c.frontier.iter().map(|&k| lo(k)).collect())
    }

    /// the frontier's not-yet-fetched parents; marks the frontier fetched
    fn begin_level<'py>(&mut self, py: Python<'py>) -> Bound<'py, PyArray1<i64>> {
        PyArray1::from_vec(py, self.c.begin_level().into_iter().map(lo).collect())
    }

    /// hashes of this level the gates' cache has not looked up
    fn unseen<'py>(&self, py: Python<'py>, parent: PyReadonlyArray1<i64>, child: PyReadonlyArray1<i64>,
                   has_child: PyReadonlyArray1<bool>) -> PyResult<Bound<'py, PyArray1<i64>>> {
        let p = keys_of(parent.as_slice()?);
        let c = opt_keys(child.as_slice()?, has_child.as_slice()?);
        let lv = Level { parent: &p, child: &c, total: &[], san: &[], bare: &[], share: None };
        Ok(PyArray1::from_vec(py, self.c.unseen(&lv).into_iter().map(lo).collect()))
    }

    fn store(&mut self, keys: PyReadonlyArray1<i64>, cps: PyReadonlyArray1<i16>) -> PyResult<()> {
        let (k, c) = (keys.as_slice()?, cps.as_slice()?);
        crate::same_len(k.len(), &[c.len()])?;
        self.c.store(&keys_of(k), c);
        Ok(())
    }

    /// one level's walk over its sorted rows: the next frontier. share: the opponent
    /// prior's reach_share per row (NaN = none), or None without a prior.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (parent, child, has_child, total, san, bare, share=None))]
    fn walk<'py>(&mut self, py: Python<'py>, parent: PyReadonlyArray1<i64>, child: PyReadonlyArray1<i64>,
                 has_child: PyReadonlyArray1<bool>, total: PyReadonlyArray1<i64>,
                 san: PyReadonlyArray1<i64>, bare: PyReadonlyArray1<i64>,
                 share: Option<PyReadonlyArray1<f64>>)
                 -> PyResult<Bound<'py, PyArray1<i64>>> {
        let p = keys_of(parent.as_slice()?);
        let c = opt_keys(child.as_slice()?, has_child.as_slice()?);
        let (t, s, b) = (total.as_slice()?, san.as_slice()?, bare.as_slice()?);
        crate::same_len(p.len(), &[c.len(), t.len(), s.len(), b.len()])?;
        let sh = match &share {
            Some(x) => Some(x.as_slice()?),
            None => None,
        };
        if let Some(x) = sh {
            crate::same_len(p.len(), &[x.len()])?;
        }
        if p.windows(2).any(|w| crate::core::pk(w[1]) < crate::core::pk(w[0])) {
            return Err(PyValueError::new_err("rows must be sorted by parent"));
        }
        let lv = Level { parent: &p, child: &c, total: t, san: s, bare: b, share: sh };
        let c = &mut self.c;
        let nxt = py.detach(|| c.walk(&lv));
        Ok(PyArray1::from_vec(py, nxt.into_iter().map(lo).collect()))
    }

    #[getter]
    fn reopened(&self) -> i64 {
        self.c.reopened
    }

    #[getter]
    fn fetched(&self) -> usize {
        self.c.fetched.len()
    }
}
