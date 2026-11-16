//! RsRollouts: rollout.rs for Python (convertibility_eval.rollouts_rust drives it).

use std::collections::HashMap;

use numpy::{PyArray1, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::rollout::{Rollouts, Start};

#[pyclass(module = "book_builder_rs", unsendable)]
pub struct RsRollouts {
    r: Rollouts,
}

#[pymethods]
impl RsRollouts {
    /// starts as columns (cp: MISSING = None); book: our moves by position (SAN)
    #[new]
    #[pyo3(signature = (h, epd, pr, ply, cp, ws, f, our_white, k_games, max_ply, seed, recall, book_keys, book_sans, want_exits, want_weak, position_ids=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(h: PyReadonlyArray1<i64>, epd: Vec<String>, pr: PyReadonlyArray1<f64>,
           ply: PyReadonlyArray1<i64>, cp: PyReadonlyArray1<i64>, ws: PyReadonlyArray1<f64>,
           f: PyReadonlyArray1<f64>, our_white: bool, k_games: usize, max_ply: i64, seed: i64,
           recall: f64, book_keys: Vec<i64>, book_sans: Vec<String>, want_exits: bool,
           want_weak: bool, position_ids: Option<PyRef<'_, crate::position_ids::RsPositionIds>>)
           -> PyResult<Self> {
        let (h, pr, ply, cp, ws, f) = (h.as_slice()?, pr.as_slice()?, ply.as_slice()?,
                                       cp.as_slice()?, ws.as_slice()?, f.as_slice()?);
        crate::same_len(h.len(), &[epd.len(), pr.len(), ply.len(), cp.len(), ws.len(), f.len()])?;
        crate::same_len(book_keys.len(), &[book_sans.len()])?;
        let starts: Vec<Start> = (0..h.len()).map(|i| Start {
            h: h[i], epd: &epd[i], pr: pr[i], ply: ply[i], cp: cp[i], ws: ws[i], f: f[i],
        }).collect();
        let book: HashMap<i64, String> = book_keys.into_iter().zip(book_sans).collect();
        let r = Rollouts::new(&starts, our_white, k_games, max_ply, seed, recall, book, want_exits,
                              want_weak, position_ids.map(|keys| keys.inner.clone()))
            .map_err(PyValueError::new_err)?;
        Ok(RsRollouts { r })
    }

    fn alive(&self) -> usize {
        self.r.alive()
    }

    fn hashes<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i64>> {
        PyArray1::from_vec(py, self.r.hashes())
    }

    fn set_cp(&mut self, cps: PyReadonlyArray1<i64>) -> PyResult<()> {
        let c = cps.as_slice()?;
        crate::same_len(c.len(), &[self.r.alive()])?;
        self.r.set_cp(c);
        Ok(())
    }

    /// (our hashes, their EPDs, their hashes, their EPDs) still without a distribution
    fn needed<'py>(&mut self, py: Python<'py>)
                   -> (Bound<'py, PyArray1<i64>>, Vec<String>, Bound<'py, PyArray1<i64>>, Vec<String>) {
        let (o, t) = self.r.needed();
        let (oh, oe): (Vec<i64>, Vec<String>) = o.into_iter().unzip();
        let (th, te): (Vec<i64>, Vec<String>) = t.into_iter().unzip();
        (PyArray1::from_vec(py, oh), oe, PyArray1::from_vec(py, th), te)
    }

    /// distributions for positions of one side: per position its (SAN, p) list, sorted
    fn set_dists(&mut self, ours: bool, hashes: Vec<i64>, dists: Vec<Vec<(String, f64)>>) -> PyResult<()> {
        crate::same_len(hashes.len(), &[dists.len()])?;
        for (h, d) in hashes.into_iter().zip(dists) {
            self.r.set_dist(h, ours, d);
        }
        Ok(())
    }

    fn advance(&mut self, py: Python<'_>) {
        let r = &mut self.r;
        py.detach(|| r.advance());
    }

    /// (leaves as a list of convertibility_eval leaf tuples, book_mass, exits increments)
    fn results<'py>(&mut self, py: Python<'py>) -> PyResult<(Bound<'py, PyList>, f64, Bound<'py, PyList>)> {
        let out = PyList::empty(py);
        for l in self.r.leaves.drain(..) {
            let cp: Option<i64> = if l.cp == crate::rollout::MISSING { None } else { Some(l.cp) };
            let ex: Option<f64> = if l.exact.is_nan() { None } else { Some(l.exact) };
            out.append((l.w, l.h, cp, l.us, ex, l.epd, l.ply, l.wk, l.si, l.k))?;
        }
        let ex = PyList::empty(py);
        for (h, ply, v, e) in self.r.exits.drain(..) {
            ex.append((h, ply, v, e))?;
        }
        Ok((out, self.r.book_mass, ex))
    }

    /// weak_edges' records: (start, k, hash, EPD before, hash after, EPD after)
    fn weak_edges<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for w in self.r.weak.drain(..) {
            out.append(w)?;
        }
        Ok(out)
    }
}

/// sampled_stop.terminal_outcomes on the collector's columns: (child keys, results)
#[pyfunction]
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn terminal_outcomes<'py>(py: Python<'py>, parents: PyReadonlyArray1<i64>, epds: Vec<Option<String>>,
                              ptr: PyReadonlyArray1<i64>, child: PyReadonlyArray1<i64>,
                              has_child: PyReadonlyArray1<bool>, san: PyReadonlyArray1<i64>,
                              sans: Vec<String>, our_white: bool)
                              -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<f64>>)> {
    let (p, pt, c, hc, s) = (parents.as_slice()?, ptr.as_slice()?, child.as_slice()?,
                             has_child.as_slice()?, san.as_slice()?);
    crate::same_len(p.len(), &[epds.len()])?;
    crate::csr(pt, p.len(), c.len(), "ptr")?;
    crate::same_len(c.len(), &[hc.len(), s.len()])?;
    if s.iter().any(|&x| x < 0 || x as usize >= sans.len()) {
        return Err(PyValueError::new_err("a SAN code outside the table"));
    }
    let ch: Vec<Option<i64>> = c.iter().zip(hc).map(|(&x, &h)| if h { Some(x) } else { None }).collect();
    let out = py.detach(|| crate::rollout::terminal_outcomes(p, &epds, pt, &ch, s, &sans, our_white));
    Ok((PyArray1::from_vec(py, out.iter().map(|x| x.0).collect()),
        PyArray1::from_vec(py, out.iter().map(|x| x.1).collect())))
}
