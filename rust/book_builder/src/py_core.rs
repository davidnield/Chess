//! RsGraph: a graph owned by Rust (core.rs) and the stages that run on it, for Python.
//!
//! Built from the CSR dict build_graph_csr returns (or budget_rs.graph_to_csr makes from any
//! budget_core.Graph), plus c_bare (bare SAN codes) and Python's set order. Keys come and go
//! as int64 handles; position_ids restores full-key ordering for colliding low halves.

use std::collections::HashMap;

use numpy::{PyArray1, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::core::{pk, Core};
use crate::dp::{self, Book, Curve};
use crate::polish;
use crate::price;
use crate::{key, lo};

fn get<'py, T: numpy::Element>(d: &Bound<'py, PyDict>, k: &str) -> PyResult<Vec<T>>
where T: Copy {
    let v = d.get_item(k)?.ok_or_else(|| PyValueError::new_err(format!("missing {k}")))?;
    let a: PyReadonlyArray1<T> = v.extract()?;
    Ok(a.as_slice()?.to_vec())
}

fn ptr(v: Vec<i64>, n: usize, len: usize, what: &str) -> PyResult<Vec<usize>> {
    crate::csr(&v, n, len, what)?;
    Ok(v.into_iter().map(|x| x as usize).collect())
}

fn opt_keys(k: Vec<i64>, has: Vec<bool>) -> Vec<Option<crate::reach::Key>> {
    k.into_iter().zip(has).map(|(k, h)| if h { Some(key(k)) } else { None }).collect()
}

#[pyclass(module = "book_builder_rs")]
pub struct RsGraph {
    core: Core,
    curves: Vec<Vec<Curve>>,
    #[cfg(feature = "optimizer")]
    position_ids: Option<crate::position_ids::SharedIds>,
}

fn book_dict<'py>(py: Python<'py>, g: &Core, b: &Book) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("booked_keys", PyArray1::from_vec(py, b.booked.iter().map(|&(u, _)| g.pyk(u)).collect()))?;
    d.set_item("booked_san", PyArray1::from_vec(py, b.booked.iter().map(|&(_, j)| g.c_san[j]).collect()))?;
    d.set_item("alloc_keys", PyArray1::from_vec(py, b.alloc.keys.iter().map(|&k| lo(k)).collect()))?;
    d.set_item("alloc_vals", PyArray1::from_vec(py, b.alloc.vals.clone()))?;
    d.set_item("visited", PyArray1::from_vec(py, b.visited.iter().map(|&h| g.pyk(h)).collect()))?;
    d.set_item("realized_keys", PyArray1::from_vec(py, b.realized.iter().map(|&(h, _)| g.pyk(h)).collect()))?;
    d.set_item("realized_vals", PyArray1::from_vec(py, b.realized.iter().map(|&(_, v)| v).collect()))?;
    d.set_item("spent_paths", b.spent_paths)?;
    d.set_item("spent_distinct", b.spent_distinct())?;
    d.set_item("root_value_curve", b.root_value_curve)?;
    d.set_item("root_value_realized", b.root_value_realized)?;
    Ok(d)
}

#[pymethods]
impl RsGraph {
    /// csr: the arrays (node_keys, node_our, l_node, crack_cont, crack_mass, c_ptr, c_san,
    /// c_bare, c_child, c_has_child, c_in_sub, c_lv, const_base, k_ptr, k_p, k_child,
    /// k_has_child, k_in_sub, k_lv, f_ptr, f_p, f_child, f_lv, reach_keys, reach_vals);
    /// set_order: list(set(g.our) | set(g.opp)).
    #[new]
    #[pyo3(signature=(csr, set_order, root, crack_at_parents, position_ids=None))]
    fn new(csr: &Bound<'_, PyDict>, set_order: PyReadonlyArray1<i64>, root: i64,
           crack_at_parents: bool, position_ids: Option<PyRef<'_, crate::position_ids::RsPositionIds>>) -> PyResult<Self> {
        let nk: Vec<i64> = get(csr, "node_keys")?;
        let no: Vec<bool> = get(csr, "node_our")?;
        crate::same_len(nk.len(), &[no.len()])?;
        let mut keys = Vec::with_capacity(nk.len());
        keys.extend(nk.iter().zip(&no).filter(|x| *x.1).map(|x| key(*x.0)));
        let n_our = keys.len();
        keys.extend(nk.iter().zip(&no).filter(|x| !*x.1).map(|x| key(*x.0)));
        let n_opp = keys.len() - n_our;
        let id: HashMap<_, _> = keys.iter().enumerate().map(|(i, &k)| (k, i as u32)).collect();
        if id.len() != keys.len() {
            return Err(PyValueError::new_err("node keys must be distinct (a key in both g.our and g.opp?)"));
        }
        let l_node: Vec<f64> = get(csr, "l_node")?;
        let cc: Vec<f64> = get(csr, "crack_cont")?;
        let cm: Vec<f64> = get(csr, "crack_mass")?;
        crate::same_len(n_our, &[l_node.len(), cc.len(), cm.len()])?;
        let c_san: Vec<i64> = get(csr, "c_san")?;
        let nc = c_san.len();
        let c_ptr = ptr(get(csr, "c_ptr")?, n_our, nc, "c_ptr")?;
        let c_bare: Vec<i64> = get(csr, "c_bare")?;
        let c_key = opt_keys(get(csr, "c_child")?, get(csr, "c_has_child")?);
        let c_in: Vec<bool> = get(csr, "c_in_sub")?;
        let c_lv: Vec<f64> = get(csr, "c_lv")?;
        crate::same_len(nc, &[c_bare.len(), c_key.len(), c_in.len(), c_lv.len()])?;
        let const_base: Vec<f64> = get(csr, "const_base")?;
        crate::same_len(n_opp, &[const_base.len()])?;
        let k_p: Vec<f64> = get(csr, "k_p")?;
        let nkid = k_p.len();
        let k_ptr = ptr(get(csr, "k_ptr")?, n_opp, nkid, "k_ptr")?;
        let k_key = opt_keys(get(csr, "k_child")?, get(csr, "k_has_child")?);
        let k_in: Vec<bool> = get(csr, "k_in_sub")?;
        let k_lv: Vec<f64> = get(csr, "k_lv")?;
        crate::same_len(nkid, &[k_key.len(), k_in.len(), k_lv.len()])?;
        let f_p: Vec<f64> = get(csr, "f_p")?;
        let f_ptr = ptr(get(csr, "f_ptr")?, n_opp, f_p.len(), "f_ptr")?;
        let f_key: Vec<_> = get::<i64>(csr, "f_child")?.into_iter().map(key).collect();
        let f_lv: Vec<f64> = get(csr, "f_lv")?;
        crate::same_len(f_p.len(), &[f_key.len(), f_lv.len()])?;
        let rk: Vec<i64> = get(csr, "reach_keys")?;
        let rv: Vec<f64> = get(csr, "reach_vals")?;
        crate::same_len(rk.len(), &[rv.len()])?;
        let reach: HashMap<_, _> = rk.iter().zip(&rv).map(|(&k, &v)| (key(k), v)).collect();
        let node_reach = keys.iter().map(|k| reach.get(k).copied().unwrap_or(0.0)).collect();
        let so = set_order.as_slice()?;
        if so.len() != keys.len() {
            return Err(PyValueError::new_err("set_order must list every node once"));
        }
        let set_order = so.iter().map(|&k| id.get(&key(k)).copied()
            .ok_or_else(|| PyValueError::new_err("set_order holds a key that is not a node")))
            .collect::<PyResult<Vec<u32>>>()?;
        #[cfg(feature = "optimizer")]
        let shared_position_ids = position_ids.as_ref().map(|p| p.inner.clone());
        let mut core = Core {
            sort_aliases: position_ids.map(|p| p.inner.lock().unwrap().ordering_exceptions()).unwrap_or_default(),
            keys, n_our, id, root: key(root), l_node,
            crack: cc.into_iter().zip(cm).map(|(a, b)| if b.is_nan() { None } else { Some((a, b)) }).collect(),
            c_ptr, c_san, c_bare, c_key, c_in, c_lv, c_node: Vec::new(),
            const_base, k_ptr, k_p, k_key, k_in, k_lv, k_node: Vec::new(),
            f_ptr, f_p, f_key, f_lv, node_reach, set_order, crack_at_parents,
            stick: false, inc_bare: vec![-1; n_our], margin: 0.0,
        };
        core.link();
        Ok(RsGraph { core, curves: Vec::new(),
            #[cfg(feature = "optimizer")]
            position_ids: shared_position_ids,
        })
    }

    /// g.meta["stick"] = (moves, margin): per our node (key) the incumbent's bare code; a
    /// code no candidate has never matches
    fn set_stick(&mut self, keys: PyReadonlyArray1<i64>, bare: PyReadonlyArray1<i64>, margin: f64)
                 -> PyResult<()> {
        let (k, b) = (keys.as_slice()?, bare.as_slice()?);
        crate::same_len(k.len(), &[b.len()])?;
        let g = &mut self.core;
        g.inc_bare = vec![-1; g.n_our];
        for (&k, &b) in k.iter().zip(b) {
            if let Some(&i) = g.id.get(&key(k)) {
                if g.is_our(i) {
                    g.inc_bare[i as usize] = if b < 0 { -2 } else { b };
                }
            }
        }
        g.stick = true;
        g.margin = margin;
        Ok(())
    }

    fn topo_up<'py>(&self, py: Python<'py>) -> (Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>) {
        let (o, e) = self.core.topo_up();
        let g = &self.core;
        (PyArray1::from_vec(py, o.iter().map(|&h| g.pyk(h)).collect()),
         PyArray1::from_vec(py, e.iter().map(|&h| g.pyk(h)).collect()))
    }

    fn topo_down<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<i64>> {
        let g = &self.core;
        PyArray1::from_vec(py, g.topo_down().iter().map(|&h| g.pyk(h)).collect())
    }

    /// stop_model_positions as two sorted key arrays
    fn stop_positions<'py>(&self, py: Python<'py>) -> (Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>) {
        let (x, y) = self.core.stop_positions();
        (PyArray1::from_vec(py, x.into_iter().map(pk).collect()),
         PyArray1::from_vec(py, y.into_iter().map(pk).collect()))
    }

    /// apply_stop_values (W, R as key/value arrays); returns its counts. Drops any curves.
    fn apply_stop_values<'py>(&mut self, py: Python<'py>, wk: PyReadonlyArray1<i64>,
                              wv: PyReadonlyArray1<f64>, rk: PyReadonlyArray1<i64>,
                              rv: PyReadonlyArray1<f64>) -> PyResult<Bound<'py, PyDict>> {
        let m = |k: &[i64], v: &[f64]| -> PyResult<HashMap<_, f64>> {
            crate::same_len(k.len(), &[v.len()])?;
            Ok(k.iter().zip(v).map(|(&k, &v)| (key(k), v)).collect())
        };
        let w = m(wk.as_slice()?, wv.as_slice()?)?;
        let r = m(rk.as_slice()?, rv.as_slice()?)?;
        let (a, b, c, e) = self.core.apply_stop_values(&w, &r);
        self.curves.clear();
        let d = PyDict::new(py);
        d.set_item("our_l", a)?;
        d.set_item("cand_leaf", b)?;
        d.set_item("opp_leaf", c)?;
        d.set_item("opp_floor", e)?;
        Ok(d)
    }

    /// The value arrays a stop model changes (to rebuild a Python Graph from the CSR dict)
    fn values<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let g = &self.core;
        let d = PyDict::new(py);
        d.set_item("l_node", PyArray1::from_vec(py, g.l_node.clone()))?;
        d.set_item("c_lv", PyArray1::from_vec(py, g.c_lv.clone()))?;
        d.set_item("k_lv", PyArray1::from_vec(py, g.k_lv.clone()))?;
        d.set_item("const_base", PyArray1::from_vec(py, g.const_base.clone()))?;
        Ok(d)
    }

    /// build_curves: (curve set id, stranded cycle nodes)
    fn build_curves(&mut self, bmax: i64, k_atoms: usize) -> (usize, i64) {
        let (c, f) = dp::build_curves(&self.core, bmax, k_atoms);
        self.curves.push(c);
        (self.curves.len() - 1, f)
    }

    /// one node's curve: (base, costs, gains)
    fn curve<'py>(&self, py: Python<'py>, set: usize, h: i64)
                  -> PyResult<Option<(f64, Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<f64>>)>> {
        let cs = self.curves.get(set).ok_or_else(|| PyValueError::new_err("no such curve set"))?;
        Ok(self.core.id.get(&key(h)).map(|&i| {
            let c = &cs[i as usize];
            (c.base, PyArray1::from_vec(py, c.costs.clone()), PyArray1::from_vec(py, c.gains.clone()))
        }))
    }

    fn extract_book<'py>(&self, py: Python<'py>, set: usize, budget: i64)
                         -> PyResult<Bound<'py, PyDict>> {
        let cs = self.curves.get(set).ok_or_else(|| PyValueError::new_err("no such curve set"))?;
        let order = self.core.topo_down();
        book_dict(py, &self.core, &dp::extract_book(&self.core, cs, budget, &order))
    }

    /// match_distinct: (book, charged, hit target, spent distinct)
    #[pyo3(signature = (set, target, bmax, probe_set=None, scan_cap=64, plain_bmax=None))]
    fn match_distinct<'py>(&self, py: Python<'py>, set: usize, target: i64, bmax: i64,
                           probe_set: Option<usize>, scan_cap: i64, plain_bmax: Option<i64>)
                           -> PyResult<(Bound<'py, PyDict>, i64, bool, i64)> {
        let cs = self.curves.get(set).ok_or_else(|| PyValueError::new_err("no such curve set"))?;
        let pc = match probe_set {
            None => None,
            Some(p) => Some(self.curves.get(p).ok_or_else(|| PyValueError::new_err("no such curve set"))?.as_slice()),
        };
        let (b, ch, hit, s) = dp::match_distinct(&self.core, cs, target, bmax, pc, scan_cap, plain_bmax);
        Ok((book_dict(py, &self.core, &b)?, ch, hit, s))
    }

    /// book_result(g, moves, topo_up order): booked (keys, sans), visited, realized values,
    /// root value, policy reach (keys, values), in Python's insertion orders
    fn book_result<'py>(&self, py: Python<'py>, keys: PyReadonlyArray1<i64>,
                        sans: PyReadonlyArray1<i64>) -> PyResult<Bound<'py, PyDict>> {
        let m = self.moves_of(keys.as_slice()?, sans.as_slice()?)?;
        let g = &self.core;
        let (order, _) = g.topo_up();
        let pos = polish::positions(g, &order);
        let reg = polish::book_region(g, &m, &pos, &polish::statics(g));
        region_dict(py, g, &reg, &pos)
    }

    /// sampled_stop.stop_positions' rows (EPDs attached by the caller): (our turn?, hash, ply,
    /// reach, parent hash, has parent, n_plies)
    #[allow(clippy::type_complexity)]
    fn stop_dump<'py>(&self, py: Python<'py>, ply_keys: PyReadonlyArray1<i64>, ply_vals: PyReadonlyArray1<i64>,
                      route_keys: PyReadonlyArray1<i64>, route_vals: PyReadonlyArray1<i64>, horizon: i64)
                      -> PyResult<(Bound<'py, PyArray1<bool>>, Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>,
                                   Bound<'py, PyArray1<f64>>, Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<bool>>,
                                   Bound<'py, PyArray1<i64>>)> {
        let m = |k: &[i64], v: &[i64]| -> PyResult<HashMap<_, i64>> {
            crate::same_len(k.len(), &[v.len()])?;
            Ok(k.iter().zip(v).map(|(&k, &v)| (key(k), v)).collect())
        };
        let ply = m(ply_keys.as_slice()?, ply_vals.as_slice()?)?;
        let route = m(route_keys.as_slice()?, route_vals.as_slice()?)?;
        let rows = self.core.stop_dump(&ply, &route, horizon).map_err(PyValueError::new_err)?;
        Ok((PyArray1::from_vec(py, rows.iter().map(|r| r.x).collect()),
            PyArray1::from_vec(py, rows.iter().map(|r| pk(r.h)).collect()),
            PyArray1::from_vec(py, rows.iter().map(|r| r.ply).collect()),
            PyArray1::from_vec(py, rows.iter().map(|r| r.reach).collect()),
            PyArray1::from_vec(py, rows.iter().map(|r| r.parent.map_or(0, pk)).collect()),
            PyArray1::from_vec(py, rows.iter().map(|r| r.parent.is_some()).collect()),
            PyArray1::from_vec(py, rows.iter().map(|r| r.plies.count_ones() as i64).collect())))
    }

    /// const_base += delta at the given opponent nodes (sampled_other.apply's change)
    fn add_const_base(&mut self, keys: PyReadonlyArray1<i64>, deltas: PyReadonlyArray1<f64>) -> PyResult<()> {
        let (k, d) = (keys.as_slice()?, deltas.as_slice()?);
        crate::same_len(k.len(), &[d.len()])?;
        let ks: Vec<_> = k.iter().map(|&x| key(x)).collect();
        self.core.add_const_base(&ks, d).map_err(PyValueError::new_err)?;
        self.curves.clear();
        Ok(())
    }

    /// policy_reach(g, moves, topo_up order): (keys, values) in insertion order
    fn policy_reach<'py>(&self, py: Python<'py>, keys: PyReadonlyArray1<i64>, sans: PyReadonlyArray1<i64>)
                         -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<f64>>)> {
        let m = self.moves_of(keys.as_slice()?, sans.as_slice()?)?;
        let g = &self.core;
        let (order, _) = g.topo_up();
        let r = price::policy_reach(g, &m, &order);
        Ok((PyArray1::from_vec(py, r.order.iter().map(|&h| g.pyk(h)).collect()),
            PyArray1::from_vec(py, r.order.iter().map(|&h| r.val[h as usize]).collect())))
    }

    /// price_search(g, target, seed_reach=(keys, values) or g.reach, ...): (result, info),
    /// the result as book_result's (or, polish=False, the selected solve's) arrays
    #[pyo3(signature = (target, seed_keys=None, seed_vals=None, steps=40, select_tol=0.01,
                        polish=true, line_k=8))]
    #[allow(clippy::too_many_arguments)]
    fn price_search<'py>(&self, py: Python<'py>, target: usize, seed_keys: Option<PyReadonlyArray1<i64>>,
                         seed_vals: Option<PyReadonlyArray1<f64>>, steps: usize, select_tol: f64,
                         polish: bool, line_k: usize)
                         -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>)> {
        self.moves_of(&[], &[])?;            // the closed-graph check
        let g = &self.core;
        let reach: Vec<f64> = match (seed_keys, seed_vals) {
            (Some(k), Some(v)) => {
                let (k, v) = (k.as_slice()?, v.as_slice()?);
                crate::same_len(k.len(), &[v.len()])?;
                let mut r = vec![0.0; g.n()];
                for (&k, &v) in k.iter().zip(v) {
                    if let Some(&i) = g.id.get(&key(k)) {
                        r[i as usize] = v;
                    }
                }
                r
            }
            (None, None) => g.node_reach.clone(),
            _ => return Err(PyValueError::new_err("seed_keys and seed_vals go together")),
        };
        let pp = polish::Params { line_k, max_verify: 24, max_steps: 100_000, fill_width: 256,
                                  max_exchange: 16, verify_after_hit: 4 };
        let out = py.detach(|| price::price_search(g, target, steps, &reach, select_tol, polish, &pp));
        let (order, _) = g.topo_up();
        let pos = polish::positions(g, &order);
        let res = match &out.selected {
            None => region_dict(py, g, &out.region, &pos)?,
            Some(s) => {
                let d = PyDict::new(py);
                d.set_item("booked_keys", PyArray1::from_vec(py, s.booked.iter().map(|&(u, _)| g.pyk(u)).collect()))?;
                d.set_item("booked_san", PyArray1::from_vec(py, s.booked.iter().map(|&(_, x)| x).collect()))?;
                d.set_item("visited", PyArray1::from_vec(py, s.visited.iter().map(|&h| g.pyk(h)).collect()))?;
                d.set_item("realized_vals", PyArray1::from_vec(py, s.visited.iter().map(|&h| s.t[h as usize]).collect()))?;
                d.set_item("value", s.root_t)?;
                d.set_item("value_penalized", s.root_v)?;
                d.set_item("w_keys", PyArray1::from_vec(py, s.reach.order.iter().map(|&h| g.pyk(h)).collect()))?;
                d.set_item("w_vals", PyArray1::from_vec(py, s.reach.order.iter().map(|&h| s.reach.val[h as usize]).collect()))?;
                d
            }
        };
        let info = PyDict::new(py);
        info.set_item("lam", out.lam)?;
        info.set_item("under", out.under)?;
        info.set_item("over", out.over)?;
        info.set_item("upper_bound", out.upper_bound)?;
        info.set_item("sweep_lam", PyArray1::from_vec(py, out.sweep.iter().map(|x| x.0).collect()))?;
        info.set_item("sweep_booked", PyArray1::from_vec(py, out.sweep.iter().map(|x| x.1 as i64).collect()))?;
        info.set_item("sweep_value", PyArray1::from_vec(py, out.sweep.iter().map(|x| x.2).collect()))?;
        info.set_item("cut", out.cut)?;
        if let Some(ps) = &out.polish {
            let l = pyo3::types::PyList::empty(py);
            for st in ps {
                l.append(stats_dict(py, st)?)?;
            }
            info.set_item("polish", l)?;
        }
        Ok((res, info))
    }

    /// polish_book(g, moves, target, topo_up order, ...): (booked keys, sans, stats)
    #[pyo3(signature = (keys, sans, target, line_k=8, max_verify=24, max_steps=100_000,
                        fill_width=256, max_exchange=16, verify_after_hit=4))]
    #[allow(clippy::too_many_arguments)]
    fn polish<'py>(&self, py: Python<'py>, keys: PyReadonlyArray1<i64>, sans: PyReadonlyArray1<i64>,
                   target: usize, line_k: usize, max_verify: i64, max_steps: i64,
                   fill_width: usize, max_exchange: usize, verify_after_hit: i64)
                   -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>, Bound<'py, PyDict>)> {
        let m = self.moves_of(keys.as_slice()?, sans.as_slice()?)?;
        let g = &self.core;
        let (order, _) = g.topo_up();
        let p = polish::Params { line_k, max_verify, max_steps, fill_width, max_exchange,
                                 verify_after_hit };
        let (booked, st) = py.detach(|| polish::polish_book(g, &m, target, &order, &p));
        let d = stats_dict(py, &st)?;
        Ok((PyArray1::from_vec(py, booked.iter().map(|&(u, _)| g.pyk(u)).collect()),
            PyArray1::from_vec(py, booked.iter().map(|&(_, s)| s).collect()), d))
    }

    /// Opt-in native optimizer study on the already resident graph. Python supplies
    /// only configuration/receipts and source-move arrays; no graph materialization.
    #[cfg(feature = "optimizer")]
    #[staticmethod]
    fn optimizer_validate_config(config_json: &str) -> PyResult<usize> {
        let cfg:crate::optimizer::benchmark::Config=serde_json::from_str(config_json)
            .map_err(|e|PyValueError::new_err(e.to_string()))?;
        cfg.validate().map_err(PyValueError::new_err)?;
        Ok(cfg.budget)
    }

    #[cfg(feature = "optimizer")]
    fn optimizer_compare(&self, py: Python<'_>, config_json: &str, provenance_json: &str,
        output_dir: &str,
        source_keys: PyReadonlyArray1<i64>, source_sans: PyReadonlyArray1<i64>, move_names:Vec<String>) -> PyResult<String> {
        use crate::optimizer::{benchmark,snapshot,Book};
        let config:benchmark::Config=serde_json::from_str(config_json).map_err(|e|PyValueError::new_err(e.to_string()))?;
        config.validate().map_err(PyValueError::new_err)?;
        self.optimizer_check_names(&move_names)?;
        let provenance:std::collections::BTreeMap<String,String>=serde_json::from_str(provenance_json).map_err(|e|PyValueError::new_err(e.to_string()))?;
        for name in ["candidate-graph","engine-overlay","opponent-policy","stop-values","horizon-and-cuts","price-source-policy"] {
            if !provenance.get(name).is_some_and(|v|v.len()==64 && v.bytes().all(|c|c.is_ascii_hexdigit())) {
                return Err(PyValueError::new_err(format!("missing SHA256 input receipt binding: {name}")));
            }
        }
        let (keys,sans)=(source_keys.as_slice()?,source_sans.as_slice()?);crate::same_len(keys.len(),&[sans.len()])?;
        let mut source=Book::new();
        let mut seen=std::collections::HashSet::new();
        let (mut absent,mut inadmissible)=(0usize,0usize);
        for (&k,&s) in keys.iter().zip(sans){
            if !seen.insert(k){return Err(PyValueError::new_err("duplicate source prescription"));}
            // The unconstrained source policy is normally a superset of the
            // admitted graph. Match legacy policy_reach's ignore behavior,
            // explicitly reporting each exclusion rather than rejecting it.
            let Some(&u)=self.core.id.get(&key(k)) else {absent+=1;continue;};
            if !self.core.is_our(u) || !self.core.cands(u as usize).any(|j|self.core.c_san[j]==s){inadmissible+=1;continue;}
            source.insert(u,s);
        }
        let ids=self.position_ids.clone().ok_or_else(||PyValueError::new_err("optimizer comparison requires the graph's original full-key registry"))?;
        let g=&self.core;
        py.detach(||{
            let table=ids.lock().map_err(|_|PyValueError::new_err("position ID table poisoned"))?;
            let decode=|k|table.pair(pk(k)).map(|(lo,hi)|((hi as u64 as u128)<<64)|(lo as u64 as u128)).ok_or_else(||"graph key absent from full-key registry".to_string());
            let graph_sha=snapshot::core_digest(g,decode).map_err(PyValueError::new_err)?;
            drop(table);
            let input=snapshot::digest(&(graph_sha,&provenance,&move_names)).map_err(PyValueError::new_err)?;
            let report=benchmark::run_core(g,&source,&input,&config,std::path::Path::new(output_dir),env!("OPTIMIZER_CODE_SHA")).map_err(PyValueError::new_err)?;
            // Export full identities for every result; run-local indices alone are not portable.
            let table=ids.lock().map_err(|_|PyValueError::new_err("position ID table poisoned"))?;
            let mut books=std::collections::BTreeMap::new();
            let mut content_ids=std::collections::BTreeMap::new();
            for cell in &report.cells {
                let mut prescriptions=Vec::new();
                let mut content=std::collections::BTreeMap::new();
                for (&u,&san) in &cell.outcome.book {
                    let (lo,hi)=table.pair(g.pyk(u)).ok_or_else(||PyValueError::new_err("result key missing from full-key registry"))?;
                    prescriptions.push(serde_json::json!({"lo":lo,"hi":hi,"san_code":san,"san":move_names[san as usize]}));
                    content.insert(snapshot::hex(((hi as u64 as u128)<<64)|(lo as u64 as u128)),move_names[san as usize].clone());
                }
                content_ids.insert(cell.name.clone(),snapshot::digest(&content).map_err(PyValueError::new_err)?);
                books.insert(cell.name.clone(),prescriptions);
            }
            let result=serde_json::json!({"report":report,"full_key_books":books,"provenance":provenance,
                "book_content_sha256":content_ids,"move_names":move_names,
                "source_filter":{"input":keys.len(),"accepted":source.len(),"absent":absent,"inadmissible":inadmissible}});
            benchmark::publish_json(&std::path::Path::new(output_dir).join("portable-results.json"),&result).map_err(PyValueError::new_err)?;
            serde_json::to_string(&result).map_err(|e|PyValueError::new_err(e.to_string()))
        })
    }

    /// Bounded real-data calibration export, with native full-key decoding.
    #[cfg(feature = "optimizer")]
    fn optimizer_snapshot(&self, py:Python<'_>, root:i64,
        base_keys:PyReadonlyArray1<i64>, base_sans:PyReadonlyArray1<i64>,
        max_nodes:usize, max_depth:usize, output_file:&str, move_names:Vec<String>) -> PyResult<()> {
        use crate::optimizer::{Book,View,snapshot,benchmark};
        self.optimizer_check_names(&move_names)?;
        let root=*self.core.id.get(&key(root)).ok_or_else(||PyValueError::new_err("cone root absent"))?;
        let (keys,sans)=(base_keys.as_slice()?,base_sans.as_slice()?);crate::same_len(keys.len(),&[sans.len()])?;
        let mut base=Book::new();
        for (&k,&s) in keys.iter().zip(sans){
            let u=*self.core.id.get(&key(k)).ok_or_else(||PyValueError::new_err("boundary policy key absent"))?;
            if base.insert(u,s).is_some(){return Err(PyValueError::new_err("duplicate boundary prescription"));}
        }
        let ids=self.position_ids.clone().ok_or_else(||PyValueError::new_err("cone export requires the graph's original full-key registry"))?;
        py.detach(||{
            let v=View::new(&self.core).map_err(PyValueError::new_err)?;
            let table=ids.lock().map_err(|_|PyValueError::new_err("position ID table poisoned"))?;
            let decode=|k|table.pair(pk(k)).map(|(lo,hi)|((hi as u64 as u128)<<64)|(lo as u64 as u128)).ok_or_else(||"graph key absent from full-key registry".to_string());
            let mut s=snapshot::extract_decoded(&v,root,&base,max_nodes,max_depth,decode).map_err(PyValueError::new_err)?;
            s.provenance.insert("move_names_json".into(),serde_json::to_string(&move_names).map_err(|e|PyValueError::new_err(e.to_string()))?);
            benchmark::write_new(std::path::Path::new(output_file),&s).map_err(PyValueError::new_err)
        })
    }

    /// Supply independent starts without exporting the graph to Python.
    #[cfg(feature = "optimizer")]
    fn optimizer_polish_starts<'py>(&self,py:Python<'py>,starts:Vec<(Vec<i64>,Vec<i64>)>,target:usize)
        ->PyResult<Bound<'py,PyDict>> {
        use crate::optimizer::{Book,View,search};
        let mut books=Vec::<Book>::new();
        for (keys,sans) in starts {
            crate::same_len(keys.len(),&[sans.len()])?;let mut book=Book::new();
            for (k,s) in keys.into_iter().zip(sans) {
                let u=self.core.id.get(&key(k)).copied().ok_or_else(||PyValueError::new_err("start position absent"))?;
                if book.insert(u,s).is_some(){return Err(PyValueError::new_err("duplicate start prescription"));}
            }
            books.push(book);
        }
        let (out,pos,stat)=py.detach(||{
            let v=View::new(&self.core)?;let out=search::polish_starts(&v,&books,target)?;
            Ok::<_,String>((out,v.pos,v.stat))
        }).map_err(PyValueError::new_err)?;
        let m=out.book.into_iter().collect();let region=polish::book_region(&self.core,&m,&pos,&stat);
        region_dict(py,&self.core,&region,&pos)
    }
}

impl RsGraph {
    #[cfg(feature = "optimizer")]
    fn optimizer_check_names(&self,names:&[String])->PyResult<()> {
        if names.iter().any(|s|s.is_empty()) || names.iter().collect::<std::collections::HashSet<_>>().len()!=names.len()
            || self.core.c_san.iter().any(|&s|s<0 || s as usize>=names.len()) {
            return Err(PyValueError::new_err("optimizer export needs an unambiguous SAN dictionary covering every candidate"));
        }
        Ok(())
    }

    fn moves_of(&self, keys: &[i64], sans: &[i64]) -> PyResult<HashMap<u32, i64>> {
        crate::same_len(keys.len(), &[sans.len()])?;
        if !self.core.closed() {
            return Err(PyValueError::new_err(
                "an in-subgraph child is not a node of the graph (or the root is not): not supported"));
        }
        Ok(keys.iter().zip(sans).filter_map(|(&k, &s)| {
            self.core.id.get(&key(k)).copied().filter(|&i| self.core.is_our(i)).map(|i| (i, s))
        }).collect())
    }
}

fn region_dict<'py>(py: Python<'py>, g: &Core, reg: &polish::Region, pos: &[u32])
                    -> PyResult<Bound<'py, PyDict>> {
    let mut visited = reg.w_keys.clone();
    visited.sort_by_key(|&h| std::cmp::Reverse(pos[h as usize]));
    let d = PyDict::new(py);
    d.set_item("booked_keys", PyArray1::from_vec(py, reg.booked.iter().map(|&(u, _)| g.pyk(u)).collect()))?;
    d.set_item("booked_san", PyArray1::from_vec(py, reg.booked.iter().map(|&(_, s)| s).collect()))?;
    d.set_item("visited", PyArray1::from_vec(py, visited.iter().map(|&h| g.pyk(h)).collect()))?;
    d.set_item("realized_vals", PyArray1::from_vec(py, visited.iter().map(|h| reg.t[h]).collect()))?;
    d.set_item("value", reg.value)?;
    d.set_item("w_keys", PyArray1::from_vec(py, reg.w_keys.iter().map(|&h| g.pyk(h)).collect()))?;
    d.set_item("w_vals", PyArray1::from_vec(py, reg.w_vals.clone()))?;
    Ok(d)
}

fn stats_dict<'py>(py: Python<'py>, st: &polish::Stats) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("start_size", st.start_size)?;
        d.set_item("start_value", st.start_value)?;
        d.set_item("size", st.size)?;
        d.set_item("value", st.value)?;
        d.set_item("trims", st.trims)?;
        d.set_item("accepted", st.by_kind.iter().map(|x| x.1).sum::<i64>())?;
        let bk = PyDict::new(py);
        for (k, v) in &st.by_kind {
            bk.set_item(*k, *v)?;
        }
        d.set_item("by_kind", bk)?;
        d.set_item("steps", st.steps)?;
        d.set_item("seconds", st.seconds)?;
        if let Some((so, o, mg, ch, rc)) = st.sticky {
            d.set_item("start_objective", so)?;
            d.set_item("objective", o)?;
            d.set_item("margin", mg)?;
            d.set_item("changed", ch)?;
            d.set_item("reach_changed", rc)?;
        }
        Ok(d)
}
