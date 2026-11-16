//! budget_core's region evaluator (_book_region, book_result) and polish_book, operation
//! for operation.
//!
//! polish_book has two Python paths that give the same book bit for bit (the dict path and
//! the compiled one, held together by _test_budget_speed.py); this is one implementation of
//! both: the dict path's semantics (stickiness included) with the compiled path's loop
//! bounds, which skip only iterations that cannot update anything.
//!
//! Python object identity matters in polish: line-table entries are tuples that the "at
//! most j moves" pass aliases between adjacent lengths, and `b is prev` / `sub[j] is not
//! sub[j - 1]` / `c is comb[j - 1]` test that aliasing. Entries are Rc's here and those tests
//! are Rc::ptr_eq.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::rc::Rc;

use crate::core::{fcmp, Core, NONE};

/// heapq of (-pos, h) popping the largest position first; positions are distinct
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ByPos(u32, u32);

pub struct Region {
    pub w_keys: Vec<u32>,
    pub w_vals: Vec<f64>,
    pub w_idx: HashMap<u32, usize>,
    pub t: HashMap<u32, f64>,
    pub booked: Vec<(u32, i64)>,
    pub b_idx: HashMap<u32, usize>,
    pub child: Vec<u32>,             // parallel to booked: the in-subgraph child, NONE = none
    pub value: f64,
}

impl Region {
    pub fn size(&self) -> usize {
        self.booked.len()
    }
    #[inline]
    pub fn w(&self, h: u32) -> f64 {
        self.w_vals[self.w_idx[&h]]
    }
    #[inline]
    pub fn is_booked(&self, h: u32) -> bool {
        self.b_idx.contains_key(&h)
    }
    pub fn moves(&self) -> HashMap<u32, i64> {
        self.booked.iter().copied().collect()
    }
}

/// The first candidate of our node u with SAN code `san`
#[inline]
fn cand_of(g: &Core, u: usize, san: i64) -> Option<usize> {
    g.cands(u).find(|&j| g.c_san[j] == san)
}

/// _book_region: policy reach and value of `moves` on the reached nodes (pos = position in
/// topo_up's order; `stat` = every node's _static_value)
pub fn book_region(g: &Core, moves: &HashMap<u32, i64>, pos: &[u32], stat: &[f64]) -> Region {
    let root = g.root_id();
    let mut w_keys = vec![root];
    let mut w_vals = vec![1.0];
    let mut w_idx: HashMap<u32, usize> = HashMap::new();
    w_idx.insert(root, 0);
    let mut heap = BinaryHeap::new();
    heap.push(ByPos(pos[root as usize], root));
    let mut booked: Vec<(u32, i64)> = Vec::new();
    let mut child: Vec<u32> = Vec::new();
    let mut b_idx: HashMap<u32, usize> = HashMap::new();
    let mut seen: HashSet<u32> = HashSet::new();
    seen.insert(root);
    let mut edges: Vec<(f64, u32)> = Vec::new();
    while let Some(ByPos(ph, h)) = heap.pop() {
        let r = w_vals[w_idx[&h]];
        let hu = h as usize;
        edges.clear();
        if g.is_our(h) {
            let Some(&want) = moves.get(&h) else { continue };
            let Some(j) = cand_of(g, hu, want) else { continue };
            b_idx.insert(h, booked.len());
            booked.push((h, want));
            let c = if g.c_in[j] && g.c_key[j].is_some() { g.c_node[j] } else { NONE };
            child.push(c);
            if c != NONE {
                edges.push((1.0, c));
            }
        } else {
            for j in g.kids(hu) {
                if g.k_in[j] && g.k_key[j].is_some() {
                    edges.push((g.k_p[j], g.k_node[j]));
                }
            }
        }
        for &(p, ch) in &edges {
            if pos[ch as usize] > ph {
                continue;
            }
            match w_idx.get(&ch) {
                Some(&i) => w_vals[i] += r * p,
                None => {
                    w_idx.insert(ch, w_keys.len());
                    w_keys.push(ch);
                    w_vals.push(0.0 + r * p);
                }
            }
            if seen.insert(ch) {
                heap.push(ByPos(pos[ch as usize], ch));
            }
        }
    }
    let mut ord: Vec<u32> = w_keys.clone();
    ord.sort_by_key(|&h| pos[h as usize]);
    let mut t: HashMap<u32, f64> = HashMap::with_capacity(ord.len());
    for &h in &ord {
        let hu = h as usize;
        let v = if g.is_our(h) {
            match b_idx.get(&h) {
                Some(&bi) => {
                    let ch = child[bi];
                    if ch == NONE {
                        g.c_lv[cand_of(g, hu, booked[bi].1).unwrap()]
                    } else {
                        match t.get(&ch) {
                            Some(&tv) if pos[ch as usize] < pos[hu] => tv,
                            _ => stat[ch as usize],
                        }
                    }
                }
                None => g.l_node[hu],
            }
        } else {
            let mut s = g.const_base[hu - g.n_our];
            for j in g.kids(hu) {
                if g.k_in[j] && g.k_key[j].is_some() {
                    let ch = g.k_node[j];
                    s += g.k_p[j] * match t.get(&ch) {
                        Some(&tv) if pos[ch as usize] < pos[hu] => tv,
                        _ => stat[ch as usize],
                    };
                } else {
                    s += g.k_p[j] * g.k_lv[j];
                }
            }
            s
        };
        t.insert(h, v);
    }
    let value = t[&root];
    Region { w_keys, w_vals, w_idx, t, booked, b_idx, child, value }
}

pub fn statics(g: &Core) -> Vec<f64> {
    (0..g.n()).map(|i| g.static_value(i)).collect()
}

/// pos[node] = index in `order`
pub fn positions(g: &Core, order: &[u32]) -> Vec<u32> {
    let mut pos = vec![u32::MAX; g.n()];
    for (i, &h) in order.iter().enumerate() {
        pos[h as usize] = i as u32;
    }
    pos
}

type Moves = Vec<(u32, i64)>;
type Ent = Rc<(f64, Moves)>;
type Table = Rc<Vec<Option<Ent>>>;

#[inline]
fn same(a: &Option<Ent>, b: &Option<Ent>) -> bool {
    matches!((a, b), (Some(x), Some(y)) if Rc::ptr_eq(x, y))
}

fn cat(a: &[(u32, i64)], b: &[(u32, i64)]) -> Moves {
    let mut v = Vec::with_capacity(a.len() + b.len());
    v.extend_from_slice(a);
    v.extend_from_slice(b);
    v
}

pub struct Params {
    pub line_k: usize,
    pub max_verify: i64,
    pub max_steps: i64,
    pub fill_width: usize,
    pub max_exchange: usize,
    pub verify_after_hit: i64,
}

pub struct Stats {
    pub start_size: usize,
    pub start_value: f64,
    pub size: usize,
    pub value: f64,
    pub trims: i64,
    pub by_kind: Vec<(&'static str, i64)>,
    pub steps: i64,
    pub sticky: Option<(f64, f64, f64, i64, f64)>,  // start_objective, objective, margin, changed, reach_changed
    pub seconds: f64,
}

struct Polish<'a> {
    g: &'a Core,
    pos: Vec<u32>,
    stat: Vec<f64>,
    margin: f64,
    inc: Vec<bool>,           // our node: the incumbent's move is one of its candidates
    any_inc: bool,
    floor: f64,
    line_k: usize,
    free_memo: HashMap<u32, (usize, Table)>,
}

const EPS: f64 = 1e-12;

impl<'a> Polish<'a> {
    #[inline]
    fn churn(&self, h: u32, j: usize) -> f64 {
        if self.inc[h as usize] && self.g.c_bare[j] != self.g.inc_bare[h as usize] { self.margin } else { 0.0 }
    }
    /// churn(u, booked san): the first candidate with that SAN carries the bare form
    fn churn_san(&self, h: u32, san: i64) -> f64 {
        match cand_of(self.g, h as usize, san) {
            Some(j) => self.churn(h, j),
            None => 0.0,
        }
    }
    fn evaluate(&self, m: &HashMap<u32, i64>) -> Region {
        book_region(self.g, m, &self.pos, &self.stat)
    }
    fn obj(&self, rg: &Region) -> f64 {
        if !self.any_inc {
            return rg.value;
        }
        let mut s = 0.0;
        for &(u, m) in &rg.booked {
            if self.inc[u as usize] {
                s += rg.w(u) * self.churn_san(u, m);
            }
        }
        rg.value - s
    }
    fn leaves(&self, rg: &Region) -> Vec<u32> {
        let g = self.g;
        let mut out = Vec::new();
        for (bi, &(u, _)) in rg.booked.iter().enumerate() {
            let ch = rg.child[bi];
            if ch == NONE || g.is_our(ch) {
                out.push(u);
                continue;
            }
            if !g.kids(ch as usize).any(|j| g.k_in[j] && g.k_node[j] != NONE && rg.is_booked(g.k_node[j])) {
                out.push(u);
            }
        }
        out
    }
    fn freed(&self, rg: &Region, u: u32) -> i64 {
        let g = self.g;
        let mut through: HashMap<u32, f64> = HashMap::new();
        through.insert(u, rg.w(u));
        let mut heap = BinaryHeap::new();
        heap.push(ByPos(self.pos[u as usize], u));
        let mut n = 0;
        let mut edges: Vec<(f64, u32)> = Vec::new();
        while let Some(ByPos(ph, h)) = heap.pop() {
            let r = through[&h];
            edges.clear();
            if g.is_our(h) {
                let Some(&bi) = rg.b_idx.get(&h) else { continue };
                if r >= rg.w(h) * (1.0 - 1e-9) {
                    n += 1;
                }
                if rg.child[bi] != NONE {
                    edges.push((1.0, rg.child[bi]));
                }
            } else {
                for j in g.kids(h as usize) {
                    if g.k_in[j] && g.k_key[j].is_some() {
                        edges.push((g.k_p[j], g.k_node[j]));
                    }
                }
            }
            for &(p, ch) in &edges {
                if rg.w_idx.contains_key(&ch) && self.pos[ch as usize] < ph {
                    match through.get_mut(&ch) {
                        Some(x) => *x += r * p,
                        None => {
                            heap.push(ByPos(self.pos[ch as usize], ch));
                            through.insert(ch, 0.0 + r * p);
                        }
                    }
                }
            }
        }
        n
    }
    fn loss(&self, rg: &Region, u: u32) -> f64 {
        let bi = rg.b_idx[&u];
        rg.w(u) * (rg.t[&u] - self.churn_san(u, rg.booked[bi].1) - self.g.l_node[u as usize])
    }
    /// val(rg, h): the value of entering h under book rg
    fn val(&self, rg: &Region, h: u32) -> f64 {
        if let Some(&t) = rg.t.get(&h) {
            return t;
        }
        let g = self.g;
        if g.is_our(h) {
            return g.l_node[h as usize];
        }
        let hu = h as usize;
        let mut t = g.const_base[hu - g.n_our];
        for j in g.kids(hu) {
            if g.k_in[j] && g.k_key[j].is_some() {
                let k = g.k_node[j];
                t += g.k_p[j] * match rg.t.get(&k) { Some(&v) => v, None => self.stat[k as usize] };
            } else {
                t += g.k_p[j] * g.k_lv[j];
            }
        }
        t
    }

    /// _extend: best[j] = (local value, moves) with at most j new moves at our node k; rg =
    /// None is the book-free table (static values, nothing booked)
    fn extend(&mut self, k: u32, kk_max: usize, depth: usize, rg: Option<&Region>) -> Vec<Option<Ent>> {
        let g = self.g;
        let ku = k as usize;
        let big = kk_max;
        let mut best: Vec<Option<Ent>> = vec![None; big + 1];
        best[0] = Some(Rc::new((g.l_node[ku], Vec::new())));
        for j in g.cands(ku) {
            let cs = self.churn(k, j);
            let san = g.c_san[j];
            let ch = g.c_node[j];
            let mut acc: Vec<Option<Ent>>;
            if !(g.c_in[j] && g.c_key[j].is_some()) {
                acc = vec![None, Some(Rc::new((g.c_lv[j] - cs, vec![(k, san)])))];
            } else {
                let base = match rg { Some(r) => self.val(r, ch), None => self.stat[ch as usize] };
                if g.is_our(ch) {
                    acc = vec![None, Some(Rc::new((base - cs, vec![(k, san)])))];
                } else {
                    acc = vec![None; big + 1];
                    acc[1] = Some(Rc::new((base - cs, vec![(k, san)])));
                    if big > 1 && depth < 2 * self.line_k {
                        // share: reply -> summed share, in first-seen order
                        let mut share: Vec<(u32, f64)> = Vec::new();
                        let mut sidx: HashMap<u32, usize> = HashMap::new();
                        for x in g.kids(ch as usize) {
                            let kk = g.k_node[x];
                            if !(g.k_in[x] && kk != NONE && g.is_our(kk) && kk != k) {
                                continue;
                            }
                            if let Some(r) = rg {
                                if r.is_booked(kk) {
                                    continue;
                                }
                            }
                            if g.node_reach[kk as usize] >= self.floor {
                                match sidx.get(&kk) {
                                    Some(&i) => share[i].1 += g.k_p[x],
                                    None => {
                                        sidx.insert(kk, share.len());
                                        share.push((kk, 0.0 + g.k_p[x]));
                                    }
                                }
                            }
                        }
                        for (kk, sh) in share {
                            let sub = self.line_free(kk, big - 1, depth + 1);
                            let stop = g.l_node[kk as usize];
                            let mut gains: Vec<(usize, f64, &Moves)> = Vec::new();
                            for jj in 1..big {
                                if let Some(e) = &sub[jj] {
                                    if e.0 > stop + EPS && !same(&sub[jj], &sub[jj - 1]) {
                                        gains.push((jj, sh * (e.0 - stop), &e.1));
                                    }
                                }
                            }
                            if gains.is_empty() {
                                continue;
                            }
                            let mut nxt = acc.clone();
                            for j1 in 1..big {
                                let Some(a) = &acc[j1] else { continue };
                                for &(j2, gn, mv) in &gains {
                                    let jj = j1 + j2;
                                    if jj > big {
                                        break;
                                    }
                                    let v = a.0 + gn;
                                    if nxt[jj].as_ref().map_or(true, |c| v > c.0) {
                                        nxt[jj] = Some(Rc::new((v, cat(&a.1, mv))));
                                    }
                                }
                            }
                            acc = nxt;
                        }
                    }
                }
            }
            for jj in 1..acc.len().min(big + 1) {
                if let Some(a) = &acc[jj] {
                    if best[jj].as_ref().map_or(true, |b| a.0 > b.0) {
                        best[jj] = Some(a.clone());
                    }
                }
            }
        }
        for jj in 1..=big {
            let take = match (&best[jj], &best[jj - 1]) {
                (None, _) => true,
                (Some(b), Some(p)) => p.0 > b.0,
                _ => false,
            };
            if take {
                best[jj] = best[jj - 1].clone();
            }
        }
        best
    }

    fn line_free(&mut self, k: u32, kk: usize, depth: usize) -> Table {
        if let Some((kg, t)) = self.free_memo.get(&k) {
            if *kg >= kk {
                return t.clone();
            }
        }
        let t: Table = Rc::new(self.extend(k, kk, depth, None));
        self.free_memo.insert(k, (kk, t.clone()));
        t
    }
}

/// polish_book: (book in the final region's insertion order, stats)
pub fn polish_book(g: &Core, moves: &HashMap<u32, i64>, target: usize, order: &[u32], p: &Params)
                   -> (Vec<(u32, i64)>, Stats) {
    let t0 = std::time::Instant::now();
    let margin = if g.stick && g.margin != 0.0 { g.margin } else { 0.0 };
    let mut inc = vec![false; g.n()];
    if margin != 0.0 {
        for u in 0..g.n_our {
            let im = g.inc_bare[u];
            if im >= 0 && g.cands(u).any(|j| g.c_bare[j] == im) {
                inc[u] = true;
            }
        }
    }
    let any_inc = inc.iter().any(|&x| x);
    let mut pl = Polish {
        g, pos: positions(g, order), stat: statics(g), margin, inc, any_inc, floor: 0.0,
        line_k: p.line_k, free_memo: HashMap::new(),
    };
    let mut reg = pl.evaluate(moves);
    let (start_size, start_value, start_obj) = (reg.size(), reg.value, pl.obj(&reg));
    let mut trims = 0i64;
    let mut accepted: Vec<(&'static str, i64)> = Vec::new();
    while reg.size() > target {
        let mut lv = pl.leaves(&reg);
        if lv.is_empty() {
            lv = reg.booked.iter().map(|x| x.0).collect();
        }
        let mut u = lv[0];
        let mut ul = pl.loss(&reg, u);
        for &x in &lv[1..] {
            let l = pl.loss(&reg, x);
            if fcmp(l, ul).then(pl.pos[x as usize].cmp(&pl.pos[u as usize])) == std::cmp::Ordering::Less {
                u = x;
                ul = l;
            }
        }
        let mut m = reg.moves();
        m.remove(&u);
        reg = pl.evaluate(&m);
        trims += 1;
    }
    if !reg.booked.is_empty() {
        let lv0: Vec<f64> = pl.leaves(&reg).iter().map(|&u| pl.loss(&reg, u)).collect();
        let mut mn: Option<f64> = None;
        for &x in &lv0 {
            if x > 0.0 && mn.map_or(true, |m| x < m) {
                mn = Some(x);
            }
        }
        pl.floor = 0.5 * mn.unwrap_or(0.0);
    }
    let kmax = p.line_k;
    let mut steps = 0i64;
    while steps < p.max_steps {
        steps += 1;
        let slack = target as i64 - reg.size() as i64;
        // the step's top-level line memo: k -> (K, table)
        let mut memo: HashMap<u32, (usize, Table)> = HashMap::new();
        let mut line = |pl: &mut Polish, reg: &Region, k: u32, kk: usize| -> Table {
            if let Some((kg, t)) = memo.get(&k) {
                if *kg >= kk {
                    return t.clone();
                }
            }
            let t: Table = Rc::new(pl.extend(k, kk, 0, Some(reg)));
            memo.insert(k, (kk, t.clone()));
            t
        };
        // (gain, kind, adds, dels)
        let mut props: Vec<(f64, &'static str, Moves, Vec<u32>)> = Vec::new();
        let lvs = pl.leaves(&reg);
        let leafset: HashSet<u32> = lvs.iter().copied().collect();
        let mut drops: Vec<(f64, u32)> = lvs.iter().map(|&u| (pl.loss(&reg, u), u)).collect();
        drops.sort_by(|a, b| fcmp(a.0, b.0).then(pl.pos[a.1 as usize].cmp(&pl.pos[b.1 as usize])));
        let mut lines: Vec<(f64, usize, Moves)> = Vec::new();
        let mut ftab: Vec<(u32, Vec<(usize, f64, Moves)>)> = Vec::new();
        for (wi, &u) in reg.w_keys.iter().enumerate() {
            let w = reg.w_vals[wi];
            if !g.is_our(u) || reg.is_booked(u) || w <= 0.0 {
                continue;
            }
            let best = line(&mut pl, &reg, u, kmax);
            let stop = g.l_node[u as usize];
            let mut prev: Option<Ent> = None;
            let mut tab: Vec<(usize, f64, Moves)> = Vec::new();
            for j in 1..=kmax {
                let b = &best[j];
                let Some(be) = b else { continue };
                if be.1.is_empty() || same(b, &prev) {
                    continue;
                }
                prev = b.clone();
                let gain = w * (be.0 - stop);
                if gain > EPS {
                    lines.push((gain, be.1.len(), be.1.clone()));
                    tab.push((be.1.len(), gain, be.1.clone()));
                }
            }
            if !tab.is_empty() {
                ftab.push((u, tab));
            }
        }
        let mut fill_from: Vec<usize> = (0..ftab.len()).collect();
        let fmax: Vec<f64> = ftab.iter().map(|(_, t)| {
            let mut m = t[0].1;
            for x in &t[1..] {
                if x.1 > m {
                    m = x.1;
                }
            }
            -m
        }).collect();
        fill_from.sort_by(|&a, &b| fcmp(fmax[a], fmax[b]));
        fill_from.truncate(p.fill_width);
        lines.sort_by(|a, b| fcmp(-a.0, -b.0));
        let mut comb: Vec<Option<Ent>> = vec![None; kmax + 1];
        comb[0] = Some(Rc::new((0.0, Vec::new())));
        for &fi in &fill_from {
            let tab = &ftab[fi].1;
            let mut nxt = comb.clone();
            for j1 in 0..kmax {
                let Some(c1) = &comb[j1] else { continue };
                for (j2, gn, mv) in tab {
                    let j = j1 + j2;
                    if j <= kmax && nxt[j].as_ref().map_or(true, |c| c1.0 + gn > c.0) {
                        nxt[j] = Some(Rc::new((c1.0 + gn, cat(&c1.1, mv))));
                    }
                }
            }
            comb = nxt;
        }
        let cheapest = |adds: &Moves, need: i64| -> Option<(f64, Vec<u32>)> {
            let added: HashSet<u32> = adds.iter().map(|x| x.0).collect();
            let mut s = 0.0;
            let mut out = Vec::new();
            if need <= 0 {
                return None;   // Python's _cheapest never returns for need 0 (len(out) == 0 is never hit)
            }
            for &(lo, d) in &drops {
                if !added.contains(&d) {
                    s += lo;
                    out.push(d);
                    if out.len() as i64 == need {
                        return Some((s, out));
                    }
                }
            }
            None
        };
        for j in 1..=kmax {
            let Some(c) = &comb[j] else { continue };
            if c.0 <= EPS || (comb[j - 1].is_some() && same(&comb[j], &comb[j - 1])) {
                continue;
            }
            let n_new = c.1.iter().map(|x| x.0).collect::<HashSet<u32>>().len() as i64;
            if n_new <= slack {
                props.push((c.0, "fill", c.1.clone(), Vec::new()));
            } else if let Some((lost, dels)) = cheapest(&c.1, n_new - slack) {
                if c.0 - lost > EPS {
                    props.push((c.0 - lost, "fill", c.1.clone(), dels));
                }
            }
        }
        let mut nonleaf: Vec<(f64, u32)> = reg.booked.iter().filter(|x| !leafset.contains(&x.0))
            .map(|&(u, _)| (pl.loss(&reg, u), u)).collect();
        nonleaf.sort_by(|a, b| fcmp(a.0, b.0).then_with(|| {
            g.sorted_key(g.keys[a.1 as usize]).cmp(&g.sorted_key(g.keys[b.1 as usize]))
        }));
        for &(lo, u) in nonleaf.iter().take(p.max_exchange) {
            let freed = pl.freed(&reg, u);
            let top = (kmax as i64).min(slack + freed);
            for j in 1..=top.max(0) as usize {
                let Some(c) = &comb[j] else { continue };
                if comb[j - 1].is_some() && same(&comb[j], &comb[j - 1]) {
                    continue;
                }
                props.push((c.0 - lo, "exchange", c.1.clone(), vec![u]));
            }
        }
        for (gain, n_new, mv) in &lines {
            let n_new = *n_new as i64;
            if n_new <= slack {
                props.push((*gain, "line", mv.clone(), Vec::new()));
            } else if let Some((lost, dels)) = cheapest(mv, n_new - slack) {
                if gain - lost > EPS {
                    props.push((gain - lost, "swap", mv.clone(), dels));
                }
            }
        }
        for &(_lo, u) in &drops {
            let kk = (p.line_k as i64).min(slack + 1).max(0) as usize;
            let best = line(&mut pl, &reg, u, kk);
            let bi = reg.b_idx[&u];
            let tu = reg.t[&u] - pl.churn_san(u, reg.booked[bi].1);
            for j in 1..best.len() {
                let Some(b) = &best[j] else { continue };
                if b.1.is_empty() {
                    continue;
                }
                let gain = reg.w(u) * (b.0 - tu);
                if gain > EPS && b.1.len() as i64 <= slack + 1 {
                    props.push((gain, "rebook", b.1.clone(), Vec::new()));
                }
            }
        }
        for &(u, w) in &reg.booked {
            if leafset.contains(&u) {
                continue;
            }
            let uu = u as usize;
            let cw = pl.churn_san(u, w);
            for j in g.cands(uu) {
                let san = g.c_san[j];
                if san == w {
                    continue;
                }
                let nv = if g.c_in[j] && g.c_key[j].is_some() { pl.val(&reg, g.c_node[j]) } else { g.c_lv[j] };
                let gain = reg.w(u) * ((nv - pl.churn(u, j)) - (reg.t[&u] - cw));
                if gain > EPS {
                    props.push((gain, "switch", vec![(u, san)], Vec::new()));
                }
            }
        }
        if pl.any_inc {
            for &(lo, u) in &drops {
                if lo < -EPS {
                    props.push((-lo, "drop", Vec::new(), vec![u]));
                }
            }
        }
        props.sort_by(|a, b| fcmp(-a.0, -b.0));
        let mut took: Option<(Region, &'static str, f64)> = None;
        let mut left = p.max_verify;
        let cur = pl.obj(&reg);
        for (_gain, kind, adds, dels) in &props {
            if left <= 0 {
                break;
            }
            left -= 1;
            let mut m = reg.moves();
            for &(h, s) in adds {
                m.insert(h, s);
            }
            for d in dels {
                m.remove(d);
            }
            let rg = pl.evaluate(&m);
            let j = pl.obj(&rg);
            let bar = took.as_ref().map_or(cur, |t| t.2);
            if rg.size() <= target && j > bar + EPS {
                if took.is_none() {
                    left = left.min(p.verify_after_hit);
                }
                took = Some((rg, kind, j));
            }
        }
        let Some((rg, kind, _)) = took else { break };
        reg = rg;
        match accepted.iter_mut().find(|x| x.0 == kind) {
            Some(x) => x.1 += 1,
            None => accepted.push((kind, 1)),
        }
    }
    let sticky = if pl.any_inc {
        let mut changed = 0i64;
        let mut rc = 0.0;
        for &(u, m) in &reg.booked {
            if pl.churn_san(u, m) != 0.0 {
                changed += 1;
                rc += reg.w(u);
            }
        }
        Some((start_obj, pl.obj(&reg), margin, changed, rc))
    } else {
        None
    };
    let stats = Stats {
        start_size, start_value, size: reg.size(), value: reg.value, trims,
        by_kind: accepted, steps, sticky, seconds: t0.elapsed().as_secs_f64(),
    };
    (reg.booked.clone(), stats)
}
