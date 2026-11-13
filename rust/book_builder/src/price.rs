//! budget_core's price per move: price_book, policy_reach and price_search (one pricing
//! round, the selection rule, polish from up to three starts), operation for operation.
//! `order` is topo_up's; "a child already valued" is "earlier in the order", as the dict
//! walk's `ch in V` is. exp/ln are the C runtime's, as CPython's math module's are.

use std::collections::{HashMap, HashSet};

use crate::core::{fcmp, Core};
use crate::polish::{self, Region, Stats};

/// The top-down reach dict R in insertion order (dense values, first-insertion order)
pub struct Reach {
    pub val: Vec<f64>,
    pub has: Vec<bool>,
    pub order: Vec<u32>,
}
impl Reach {
    fn new(n: usize) -> Reach {
        Reach { val: vec![0.0; n], has: vec![false; n], order: Vec::new() }
    }
    #[inline]
    fn add(&mut self, h: u32, x: f64) {
        let i = h as usize;
        if self.has[i] {
            self.val[i] += x;
        } else {
            self.has[i] = true;
            self.order.push(h);
            self.val[i] = 0.0 + x;
        }
    }
    #[inline]
    fn get(&self, h: u32) -> f64 {
        if self.has[h as usize] { self.val[h as usize] } else { 0.0 }
    }
}

pub struct Solve {
    pub booked: Vec<(u32, i64)>,
    pub visited: Vec<u32>,
    pub t: Vec<f64>,
    pub reach: Reach,
    pub root_t: f64,
    pub root_v: f64,
    pub lam: f64,
}
impl Solve {
    pub fn size(&self) -> usize {
        self.booked.len()
    }
}

/// price_book at price lam; reach = the base reach per node (reach.get(h, 0.0))
pub fn price_book(g: &Core, lam: f64, order: &[u32], reach: &[f64], stat: &[f64]) -> Solve {
    let n = g.n();
    let mut v = vec![0.0f64; n];
    let mut t = vec![0.0f64; n];
    let mut done = vec![false; n];
    let mut choice: Vec<i64> = vec![i64::MIN; g.n_our];
    for &h in order {
        let hu = h as usize;
        if g.is_our(h) {
            let mut best: Option<(f64, f64, f64, i64)> = None;
            for j in g.cands(hu) {
                let (cv, ct) = if g.c_in[j] && g.c_key[j].is_some() {
                    let ch = g.c_node[j] as usize;
                    if done[ch] { (v[ch], t[ch]) } else { (stat[ch], stat[ch]) }
                } else {
                    (g.c_lv[j], g.c_lv[j])
                };
                let key = cv + g.bonus(hu, j);
                if best.map_or(true, |b| key > b.0) {
                    best = Some((key, cv, ct, g.c_san[j]));
                }
            }
            let r = reach[hu];
            match best {
                Some((_, bv, bt, san)) if r > 0.0 && bv - lam / r > g.l_node[hu] + 1e-15 => {
                    v[hu] = bv - lam / r;
                    t[hu] = bt;
                    choice[hu] = san;
                }
                _ => {
                    v[hu] = g.l_node[hu];
                    t[hu] = g.l_node[hu];
                }
            }
        } else {
            let c0 = g.const_base[hu - g.n_our];
            let (mut a, mut b) = (c0, c0);
            for j in g.kids(hu) {
                let p = g.k_p[j];
                if g.k_in[j] && g.k_key[j].is_some() {
                    let ch = g.k_node[j] as usize;
                    if done[ch] {
                        a += p * v[ch];
                        b += p * t[ch];
                    } else {
                        let s = stat[ch];
                        a += p * s;
                        b += p * s;
                    }
                } else {
                    a += p * g.k_lv[j];
                    b += p * g.k_lv[j];
                }
            }
            v[hu] = a;
            t[hu] = b;
        }
        done[hu] = true;
    }
    let mut rr = Reach::new(n);
    let root = g.root_id();
    rr.add(root, 1.0);
    let mut visited = Vec::new();
    let mut booked = Vec::new();
    for &h in order.iter().rev() {
        let r = rr.get(h);
        if r <= 0.0 {
            continue;
        }
        visited.push(h);
        let hu = h as usize;
        if g.is_our(h) {
            let mv = choice[hu];
            if mv == i64::MIN {
                continue;
            }
            booked.push((h, mv));
            for j in g.cands(hu) {
                if g.c_san[j] == mv {
                    if g.c_in[j] && g.c_key[j].is_some() {
                        rr.add(g.c_node[j], r);
                    }
                    break;
                }
            }
        } else {
            for j in g.kids(hu) {
                if g.k_in[j] && g.k_key[j].is_some() {
                    rr.add(g.k_node[j], r * g.k_p[j]);
                }
            }
        }
    }
    let ri = root as usize;
    Solve { booked, visited, root_t: t[ri], root_v: v[ri], t, reach: rr, lam }
}

/// policy_reach of `moves` (our node -> SAN code)
pub fn policy_reach(g: &Core, moves: &HashMap<u32, i64>, order: &[u32]) -> Reach {
    let mut rr = Reach::new(g.n());
    rr.add(g.root_id(), 1.0);
    for &h in order.iter().rev() {
        let r = rr.get(h);
        if r <= 0.0 {
            continue;
        }
        let hu = h as usize;
        if g.is_our(h) {
            let Some(&want) = moves.get(&h) else { continue };
            for j in g.cands(hu) {
                if g.c_san[j] == want {
                    if g.c_in[j] && g.c_key[j].is_some() {
                        rr.add(g.c_node[j], r);
                    }
                    break;
                }
            }
        } else {
            for j in g.kids(hu) {
                if g.k_in[j] && g.k_key[j].is_some() {
                    rr.add(g.k_node[j], r * g.k_p[j]);
                }
            }
        }
    }
    rr
}

pub struct SearchOut {
    pub region: Region,
    pub lam: f64,
    pub under: Option<usize>,
    pub over: Option<usize>,
    pub upper_bound: f64,
    pub sweep: Vec<(f64, usize, f64)>,      // sorted (lam, booked, value)
    pub polish: Option<Vec<Stats>>,
    pub selected: Option<Solve>,            // polish = False: the selected solve itself
    pub cut: usize,
}

/// price_search (one pricing round; polish from the selected solve, the best-valued solve
/// that fits and the smallest solve over the target)
#[allow(clippy::too_many_arguments)]
pub fn price_search(g: &Core, target: usize, steps: usize, reach: &[f64], select_tol: f64,
                    do_polish: bool, pp: &polish::Params) -> SearchOut {
    let (order, cut) = g.topo_up();
    let stat = polish::statics(g);
    let mut pool: Vec<(usize, f64, Vec<(u32, i64)>)> = Vec::new();
    let mut tried: Vec<(f64, usize, f64, f64)> = Vec::new();
    let mut near: Vec<usize> = Vec::new();      // indices into `solves`
    let mut solves: Vec<Solve> = Vec::new();
    let tf = target as f64;
    let mut solve = |lam: f64, solves: &mut Vec<Solve>| -> usize {
        let s = price_book(g, lam, &order, reach, &stat);
        pool.push((s.size(), s.root_t, s.booked.clone()));
        tried.push((lam, s.size(), s.root_t, s.root_v));
        if ((s.size() as i64 - target as i64).abs() as f64) <= select_tol * tf {
            near.push(solves.len());
        }
        solves.push(s);
        solves.len() - 1
    };
    // bisect: (best under, best over) as solve indices
    let (u, o): (Option<usize>, Option<usize>) = 'b: {
        let (mut lo, mut hi) = ((1e-10f64).ln(), (1.0f64).ln());
        let r_hi = solve(hi.exp(), &mut solves);
        if solves[r_hi].size() >= target {
            break 'b (None, Some(r_hi));
        }
        let r_lo = solve(lo.exp(), &mut solves);
        if solves[r_lo].size() <= target {
            break 'b (Some(r_lo), None);
        }
        let (mut bu, mut bo) = (r_hi, r_lo);
        for _ in 0..steps {
            let mid = (lo + hi) / 2.0;
            let r = solve(mid.exp(), &mut solves);
            let n = solves[r].size();
            if n == target {
                break 'b (Some(r), Some(r));
            }
            if n < target {
                hi = mid;
                bu = r;
            } else {
                lo = mid;
                bo = r;
            }
            if hi - lo < 1e-7 {
                break;
            }
        }
        (Some(bu), Some(bo))
    };
    let sel = if !near.is_empty() {
        let mut b = near[0];
        for &x in &near[1..] {
            if solves[x].root_t > solves[b].root_t {
                b = x;
            }
        }
        b
    } else {
        match (u, o) {
            (Some(u), None) => u,
            (None, Some(o)) => o,
            (Some(u), Some(o)) => {
                if target as i64 - solves[u].size() as i64 <= solves[o].size() as i64 - target as i64 { u } else { o }
            }
            (None, None) => unreachable!(),
        }
    };
    let lam = solves[sel].lam;
    let upper_bound = {
        let mut m = f64::INFINITY;
        for (i, &(l, _n, _t, vp)) in tried.iter().enumerate() {
            let x = vp + l * tf;
            if i == 0 || x < m {
                m = x;
            }
        }
        m
    };
    let mut sweep: Vec<(f64, usize, f64, f64)> = tried.clone();
    sweep.sort_by(|a, b| fcmp(a.0, b.0).then(a.1.cmp(&b.1)).then(fcmp(a.2, b.2)).then(fcmp(a.3, b.3)));
    let under = u.map(|x| solves[x].size());
    let over = o.map(|x| solves[x].size());
    if !do_polish {
        let s = solves.swap_remove(sel);
        let pos = polish::positions(g, &order);
        let m: HashMap<u32, i64> = s.booked.iter().copied().collect();
        let region = polish::book_region(g, &m, &pos, &stat);
        return SearchOut { region, lam, under, over, upper_bound,
                           sweep: sweep.iter().map(|x| (x.0, x.1, x.2)).collect(),
                           polish: None, selected: Some(s), cut: cut.len() };
    }
    let mut starts: Vec<Vec<(u32, i64)>> = vec![solves[sel].booked.clone()];
    let mut best_fit: Option<usize> = None;
    for (i, e) in pool.iter().enumerate() {
        if e.0 <= target && best_fit.map_or(true, |b| e.1 > pool[b].1) {
            best_fit = Some(i);
        }
    }
    if let Some(b) = best_fit {
        starts.push(pool[b].2.clone());
    }
    let mut over_min: Option<usize> = None;
    for (i, e) in pool.iter().enumerate() {
        if e.0 > target {
            let better = match over_min {
                None => true,
                Some(b) => e.0 < pool[b].0 || (e.0 == pool[b].0 && fcmp(-e.1, -pool[b].1) == std::cmp::Ordering::Less),
            };
            if better {
                over_min = Some(i);
            }
        }
    }
    if let Some(b) = over_min {
        starts.push(pool[b].2.clone());
    }
    let mut seen: HashSet<Vec<(u32, i64)>> = HashSet::new();
    let mut best_p: Option<(Vec<(u32, i64)>, f64)> = None;
    let mut stats = Vec::new();
    for st in starts {
        let mut k = st.clone();
        k.sort_unstable();
        if !seen.insert(k) {
            continue;
        }
        let m: HashMap<u32, i64> = st.iter().copied().collect();
        let (moves, ps) = polish::polish_book(g, &m, target, &order, pp);
        let objective = ps.sticky.map_or(ps.value, |s| s.1);
        if best_p.as_ref().map_or(true, |b| objective > b.1 + 1e-15) {
            best_p = Some((moves, objective));
        }
        stats.push(ps);
    }
    let pos = polish::positions(g, &order);
    let m: HashMap<u32, i64> = best_p.unwrap().0.into_iter().collect();
    let region = polish::book_region(g, &m, &pos, &stat);
    SearchOut { region, lam, under, over, upper_bound,
                sweep: sweep.iter().map(|x| (x.0, x.1, x.2)).collect(),
                polish: Some(stats), selected: None, cut: cut.len() }
}
