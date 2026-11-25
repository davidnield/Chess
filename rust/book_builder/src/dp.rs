//! The curve engine (budget_core's "the DP" and "extraction" sections), operation for
//! operation: Curve, curve_from_points, cap_curve, merge_weighted, build_curves,
//! extract_book, match_distinct.
//!
//! numpy's cumsum is a sequential left fold and `gains / costs` divides by the cost cast to
//! float64; argmin returns the first minimum (the first NaN if any); a stable argsort of
//! `-dens` orders NaN last and keeps -0.0 == 0.0 ties in index order. Python's `sum()`
//! starts at 0 and adds left to right.

use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use crate::core::{fcmp, Core, MinFK, NONE};
use crate::reach::{Key, OrderedMap};

#[derive(Clone, Debug)]
pub struct Curve {
    pub base: f64,
    pub costs: Vec<i64>,
    pub gains: Vec<f64>,
    pub cum_costs: Vec<i64>,
    pub cum_gains: Vec<f64>,
}

impl Curve {
    pub fn new(base: f64, costs: Vec<i64>, gains: Vec<f64>) -> Curve {
        let mut cc = Vec::with_capacity(costs.len());
        let mut cg = Vec::with_capacity(gains.len());
        let (mut a, mut b) = (0i64, 0.0f64);
        for (i, (&c, &g)) in costs.iter().zip(&gains).enumerate() {
            a = if i == 0 { c } else { a + c };
            b = if i == 0 { g } else { b + g };
            cc.push(a);
            cg.push(b);
        }
        Curve { base, costs, gains, cum_costs: cc, cum_gains: cg }
    }
    pub fn flat(v: f64) -> Curve {
        Curve::new(v, Vec::new(), Vec::new())
    }
    pub fn eval(&self, b: i64) -> f64 {
        if b <= 0 || self.costs.is_empty() {
            return self.base;
        }
        let k = self.cum_costs.partition_point(|&c| c <= b);
        self.base + if k > 0 { self.cum_gains[k - 1] } else { 0.0 }
    }
    pub fn capacity(&self) -> i64 {
        self.cum_costs.last().copied().unwrap_or(0)
    }
}

/// curve_from_points on points already sorted by budget, budgets distinct
pub fn curve_from_points(pts: &[(i64, f64)]) -> Curve {
    assert!(!pts.is_empty() && pts[0].0 == 0, "curve must define V(0)");
    let mut vals = Vec::with_capacity(pts.len());
    let mut run = f64::NEG_INFINITY;
    for &(_, v) in pts {
        if v > run {
            run = v;
        }
        vals.push(run);
    }
    let base = vals[0];
    let mut stack: Vec<(i64, f64)> = Vec::new();
    let (mut prev_b, mut prev_v) = (pts[0].0, vals[0]);
    for (&(b, _), &v) in pts[1..].iter().zip(&vals[1..]) {
        let (dc, dg) = (b - prev_b, v - prev_v);
        prev_b = b;
        prev_v = v;
        if dg <= 1e-15 {
            stack.push((dc, 0.0));
            continue;
        }
        stack.push((dc, dg));
        while stack.len() >= 2 {
            let (c1, g1) = stack[stack.len() - 1];
            let (c0, g0) = stack[stack.len() - 2];
            if !(g1 * c0 as f64 > g0 * c1 as f64) {
                break;
            }
            stack.pop();
            let t = stack.last_mut().unwrap();
            t.0 += c1;
            t.1 += g1;
        }
    }
    while let Some(&(_, g)) = stack.last() {
        if g <= 1e-15 {
            stack.pop();
        } else {
            break;
        }
    }
    if stack.is_empty() {
        return Curve::flat(base);
    }
    Curve::new(base, stack.iter().map(|x| x.0).collect(), stack.iter().map(|x| x.1).collect())
}

/// cap_curve
pub fn cap_curve(mut c: Curve, bmax: i64, k_atoms: usize) -> Curve {
    if !c.costs.is_empty() && *c.cum_costs.last().unwrap() > bmax {
        let keep = c.cum_costs.partition_point(|&x| x <= bmax);
        c = Curve::new(c.base, c.costs[..keep].to_vec(), c.gains[..keep].to_vec());
    }
    #[cfg(feature = "optimizer")]
    crate::optimizer::search::record_cap(c.costs.len(), k_atoms);
    if c.costs.len() <= k_atoms {
        return c;
    }
    let (mut costs, mut gains) = (c.costs, c.gains);
    let mut dens: Vec<f64> = gains.iter().zip(&costs).map(|(&g, &k)| g / k as f64).collect();
    while costs.len() > k_atoms {
        // np.argmin(dens[:-1] - dens[1:])
        let mut i = 0usize;
        let mut best = f64::NAN;
        let mut nan_at = None;
        for x in 0..dens.len() - 1 {
            let gap = dens[x] - dens[x + 1];
            if gap.is_nan() {
                nan_at = Some(x);
                break;
            }
            if x == 0 || gap < best {
                best = gap;
                i = x;
            }
        }
        if let Some(x) = nan_at {
            i = x;
        }
        costs[i] += costs[i + 1];
        gains[i] += gains[i + 1];
        costs.remove(i + 1);
        gains.remove(i + 1);
        dens.remove(i + 1);
        dens[i] = gains[i] / costs[i] as f64;
    }
    Curve::new(c.base, costs, gains)
}

/// merge_weighted
pub fn merge_weighted(children: &[(f64, &Curve)], const_base: f64, bmax: i64, k_atoms: usize)
                      -> Curve {
    let mut s = 0.0;
    for &(p, ch) in children {
        s += p * ch.base;
    }
    let base = const_base + s;
    let mut costs = Vec::new();
    let mut gains = Vec::new();
    for &(p, ch) in children {
        costs.extend_from_slice(&ch.costs);
        gains.extend(ch.gains.iter().map(|&g| g * p));
    }
    if costs.is_empty() {
        return Curve::flat(base);
    }
    let nd: Vec<f64> = gains.iter().zip(&costs).map(|(&g, &k)| -(g / k as f64)).collect();
    let mut order: Vec<usize> = (0..costs.len()).collect();
    // numpy's stable sort: a < b, NaN last
    order.sort_by(|&a, &b| {
        let (x, y) = (nd[a], nd[b]);
        match (x.is_nan(), y.is_nan()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            _ => fcmp(x, y),
        }
    });
    let c = Curve::new(base, order.iter().map(|&i| costs[i]).collect(),
                       order.iter().map(|&i| gains[i]).collect());
    cap_curve(c, bmax, k_atoms)
}

fn node_curve(g: &Core, h: usize, curves: &[Option<Curve>], bmax: i64, k_atoms: usize) -> Curve {
    if g.is_our(h as u32) {
        // pts / keys: budget -> (value, key)
        let mut pts: std::collections::BTreeMap<i64, (f64, f64)> = std::collections::BTreeMap::new();
        pts.insert(0, (g.l_node[h], g.l_node[h]));
        for j in g.cands(h) {
            let bonus = g.bonus(h, j);
            let c = g.c_node[j];
            let mut upd = |b: i64, v: f64| -> bool {
                if b > bmax {
                    return false;
                }
                let k = v + bonus;
                match pts.get(&b) {
                    Some(&(_, kb)) if !(k > kb) => {}
                    _ => {
                        pts.insert(b, (v, k));
                    }
                }
                true
            };
            if c != NONE && curves[c as usize].is_some() {
                let cv = curves[c as usize].as_ref().unwrap();
                if upd(1, cv.base) {
                    for (&cc, &cg) in cv.cum_costs.iter().zip(&cv.cum_gains) {
                        if !upd(1 + cc, cv.base + cg) {
                            break;
                        }
                    }
                }
            } else {
                upd(1, g.c_lv[j]);
            }
        }
        let p: Vec<(i64, f64)> = pts.iter().map(|(&b, &(v, _))| (b, v)).collect();
        return cap_curve(curve_from_points(&p), bmax, k_atoms);
    }
    let o = h - g.n_our;
    let mut cst = g.const_base[o];
    let mut ch: Vec<(f64, &Curve)> = Vec::new();
    for j in g.kids(h) {
        let c = g.k_node[j];
        if c != NONE && curves[c as usize].is_some() {
            ch.push((g.k_p[j], curves[c as usize].as_ref().unwrap()));
        } else {
            cst += g.k_p[j] * g.k_lv[j];
        }
    }
    merge_weighted(&ch, cst, bmax, k_atoms)
}

/// build_curves: (a curve per node id, stranded cycle nodes flattened)
pub fn build_curves(g: &Core, bmax: i64, k_atoms: usize) -> (Vec<Curve>, i64) {
    let n = g.n();
    let (children, parents) = g.adjacency();
    let mut pending: Vec<i64> = children.iter().map(|c| c.len() as i64).collect();
    let mut curves: Vec<Option<Curve>> = vec![None; n];
    let mut q: VecDeque<u32> = g.set_order.iter().copied().filter(|&h| pending[h as usize] == 0).collect();
    while let Some(h) = q.pop_front() {
        curves[h as usize] = Some(node_curve(g, h as usize, &curves, bmax, k_atoms));
        for &par in &parents[h as usize] {
            pending[par as usize] -= 1;
            if pending[par as usize] == 0 {
                q.push_back(par);
            }
        }
    }
    let mut remaining: Vec<bool> = curves.iter().map(|c| c.is_none()).collect();
    let mut n_rem = remaining.iter().filter(|&&r| r).count();
    let mut heap: BinaryHeap<MinFK> = (0..n as u32).filter(|&h| remaining[h as usize])
        .map(|h| MinFK(g.node_reach[h as usize], g.sorted_key(g.keys[h as usize]), h)).collect();
    let mut flattened = 0i64;
    while n_rem > 0 {
        let mut victim = NONE;
        while let Some(MinFK(_, _, c)) = heap.pop() {
            if remaining[c as usize] {
                victim = c;
                break;
            }
        }
        if victim == NONE {
            break;
        }
        let v = victim as usize;
        curves[v] = Some(Curve::flat(g.static_value(v)));
        remaining[v] = false;
        n_rem -= 1;
        flattened += 1;
        for &par in &parents[v] {
            pending[par as usize] -= 1;
            if pending[par as usize] == 0 && remaining[par as usize] {
                q.push_back(par);
            }
        }
        while let Some(h) = q.pop_front() {
            let hu = h as usize;
            if !remaining[hu] {
                continue;
            }
            curves[hu] = Some(node_curve(g, hu, &curves, bmax, k_atoms));
            remaining[hu] = false;
            n_rem -= 1;
            for &par in &parents[hu] {
                pending[par as usize] -= 1;
                if pending[par as usize] == 0 && remaining[par as usize] {
                    q.push_back(par);
                }
            }
        }
    }
    (curves.into_iter().map(|c| c.expect("every node gets a curve")).collect(), flattened)
}

/// extract_book's result, in Python's insertion orders
#[derive(Clone, Default)]
pub struct Book {
    pub booked: Vec<(u32, usize)>,     // (our node, candidate index j)
    pub alloc: OrderedMap<i64>,
    pub visited: Vec<u32>,
    pub realized: Vec<(u32, f64)>,
    pub spent_paths: i64,
    pub root_value_curve: Option<f64>,
    pub root_value_realized: Option<f64>,
}
impl Book {
    pub fn spent_distinct(&self) -> i64 {
        self.booked.len() as i64
    }
}

/// extract_book over `order` (topo_down's)
pub fn extract_book(g: &Core, curves: &[Curve], budget: i64, order: &[u32]) -> Book {
    let n = g.n();
    let mut booked_at: Vec<usize> = vec![usize::MAX; g.n_our];
    let mut booked: Vec<(u32, usize)> = Vec::new();
    let mut alloc = OrderedMap::<i64>::default();
    let mut reached: HashSet<Key> = HashSet::new();
    reached.insert(g.root);
    alloc.set(g.root, budget);
    let mut visited: Vec<u32> = Vec::new();
    let mut spent_paths = 0i64;
    for &h in order {
        let hk = g.keys[h as usize];
        if !reached.contains(&hk) {
            continue;
        }
        let b = alloc.get(&hk).unwrap_or(0);
        visited.push(h);
        let hu = h as usize;
        if g.is_our(h) {
            if b < 1 {
                continue;
            }
            let mut best: Option<(f64, f64, usize)> = None;
            for j in g.cands(hu) {
                let c = g.c_node[j];
                let v = if c != NONE { curves[c as usize].eval((b - 1).max(0)) } else { g.c_lv[j] };
                let key = v + g.bonus(hu, j);
                if best.map_or(true, |(bk, _, _)| key > bk) {
                    best = Some((key, v, j));
                }
            }
            let Some((_, v, j)) = best else { continue };
            if v <= g.l_node[hu] + 1e-12 {
                continue;
            }
            booked_at[hu] = booked.len();
            booked.push((h, j));
            spent_paths += 1;
            if let (true, Some(ck)) = (g.c_in[j], g.c_key[j]) {
                reached.insert(ck);
                let a = alloc.get(&ck).unwrap_or(0).max((b - 1).max(0));
                alloc.set(ck, a);
            }
        } else {
            let ks = g.kids(hu);
            let k0 = ks.start;
            // (density, child idx, atom j, cost)
            let mut entries: Vec<(f64, usize, usize, i64)> = Vec::new();
            for j in ks.clone() {
                let c = g.k_node[j];
                if c == NONE {
                    continue;
                }
                let cv = &curves[c as usize];
                for a in 0..cv.costs.len() {
                    entries.push((cv.gains[a] * g.k_p[j] / cv.costs[a] as f64, j - k0, a, cv.costs[a]));
                }
            }
            entries.sort_by(|x, y| fcmp(-x.0, -y.0).then(x.1.cmp(&y.1)).then(x.2.cmp(&y.2)));
            let mut remaining = b;
            let mut child_alloc = vec![0i64; ks.len()];
            let mut closed = vec![false; ks.len()];
            for &(_d, idx, _a, cost) in &entries {
                if closed[idx] {
                    continue;
                }
                if cost <= remaining {
                    child_alloc[idx] += cost;
                    remaining -= cost;
                } else {
                    closed[idx] = true;
                }
            }
            for j in ks {
                let c = g.k_node[j];
                if c != NONE {
                    let ck = g.keys[c as usize];
                    reached.insert(ck);
                    let a = alloc.get(&ck).unwrap_or(0).max(child_alloc[j - k0]);
                    alloc.set(ck, a);
                }
            }
        }
    }
    // realized, bottom-up over the visited set
    let mut real: Vec<f64> = vec![f64::NAN; n];
    let mut has: Vec<bool> = vec![false; n];
    let mut realized: Vec<(u32, f64)> = Vec::with_capacity(visited.len());
    for &h in visited.iter().rev() {
        let hu = h as usize;
        let val;
        if g.is_our(h) {
            let bi = booked_at[hu];
            if bi == usize::MAX {
                val = Some(g.l_node[hu]);
            } else {
                let mv = g.c_san[booked[bi].1];
                let mut out = None;
                for j in g.cands(hu) {
                    if g.c_san[j] == mv {
                        let c = g.c_node[j];
                        out = Some(if c != NONE && has[c as usize] {
                            real[c as usize]
                        } else if c != NONE {
                            curves[c as usize].eval(alloc.get(&g.keys[c as usize]).unwrap_or(0))
                        } else {
                            g.c_lv[j]
                        });
                        break;
                    }
                }
                val = out;
            }
        } else {
            let mut v = g.const_base[hu - g.n_our];
            for j in g.kids(hu) {
                let c = g.k_node[j];
                v += g.k_p[j] * if c != NONE && has[c as usize] {
                    real[c as usize]
                } else if c != NONE {
                    curves[c as usize].eval(0)
                } else {
                    g.k_lv[j]
                };
            }
            val = Some(v);
        }
        if let Some(v) = val {
            real[hu] = v;
            has[hu] = true;
            realized.push((h, v));
        }
    }
    let r = g.root_id();
    Book {
        booked, alloc, visited, spent_paths,
        root_value_curve: if r != NONE { Some(curves[r as usize].eval(budget)) } else { None },
        root_value_realized: if r != NONE && has[r as usize] { Some(real[r as usize]) } else { None },
        realized,
    }
}

/// match_distinct: (book, charged budget, hit target, spent distinct)
pub fn match_distinct(g: &Core, curves: &[Curve], target: i64, bmax: i64,
                      probe: Option<&[Curve]>, scan_cap: i64, plain_bmax: Option<i64>)
                      -> (Book, i64, bool, i64) {
    let order = g.topo_down();
    let res = extract_book(g, curves, target, &order);
    if res.spent_distinct() >= target || target >= bmax {
        let s = res.spent_distinct();
        return (res, target, s >= target, s);
    }
    let pc = probe.unwrap_or(curves);
    let mut seen: HashMap<i64, Book> = HashMap::new();
    let probe_at = |seen: &mut HashMap<i64, Book>, b: i64| -> i64 {
        seen.entry(b).or_insert_with(|| {
            let cs = match plain_bmax {
                Some(pb) if b <= pb => curves,
                _ => pc,
            };
            extract_book(g, cs, b, &order)
        }).spent_distinct()
    };
    // best_lo / best_hi: None = the target extraction `res`, Some(b) = seen[b]
    let (mut lo, mut best_lo): (i64, Option<i64>) = (target, None);
    let (mut hi, mut best_hi): (i64, Option<i64>) = (-1, None);
    let mut p = target;
    while p < bmax {
        p = bmax.min((p + 1).max((p as f64 * 1.5) as i64));
        if probe_at(&mut seen, p) >= target {
            hi = p;
            best_hi = Some(p);
            break;
        }
        lo = p;
        best_lo = Some(p);
    }
    let Some(_) = best_hi else {
        let r = match best_lo { None => res, Some(b) => seen.remove(&b).unwrap() };
        let s = r.spent_distinct();
        return (r, lo, false, s);
    };
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if probe_at(&mut seen, mid) >= target {
            hi = mid;
            best_hi = Some(mid);
        } else {
            lo = mid;
        }
    }
    let bh = best_hi.unwrap();
    if seen[&bh].spent_distinct() > target {
        let mut spent_probes = 0i64;
        for b in target + 1..=bmax {
            if !seen.contains_key(&b) {
                if spent_probes >= scan_cap {
                    break;
                }
                spent_probes += 1;
            }
            if probe_at(&mut seen, b) == target {
                return (seen.remove(&b).unwrap(), b, true, target);
            }
        }
    }
    let r = seen.remove(&bh).unwrap();
    let s = r.spent_distinct();
    (r, hi, true, s)
}
