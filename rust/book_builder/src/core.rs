//! A budget_core.Graph owned by Rust: what every stage after build_graph reads.
//!
//! Nodes get ids: our nodes 0..n_our in g.our's order, then opponent nodes in g.opp's order.
//! A candidate's or reply's child is kept as its key (Python's `ch`) and, where it is a node
//! of the graph, as that node's id; Python's `ch in g.our` / `ch in curves` / `ch in T`
//! become id tests. `set_order` is `list(set(g.our) | set(g.opp))`, computed by Python
//! (CPython's set iteration order is not reimplemented here): topo_up's Tarjan roots,
//! topo_down's and build_curves' queues start in it, as budget_core's do.
//!
//! Comparisons follow Python: floats inside tuples and heaps compare with IEEE semantics
//! (-0.0 == 0.0, then the next tuple field decides), keys compare as Python ints.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};

use crate::reach::Key;

pub const NONE: u32 = u32::MAX;

/// A key's value as a Python int (today's keys are signed 64-bit).
#[inline]
pub fn pk(k: Key) -> i64 {
    k as u64 as i64
}

/// Match the signed (lo, hi) order of the pool. Legacy keys all have hi=0.
pub fn key_order(k: Key) -> (i64, i64) {
    (pk(k), (k >> 64) as i64)
}

/// Python's `<`/`==` on floats inside a tuple: IEEE, so -0.0 == 0.0 (NaN: treated as equal).
#[inline]
pub fn fcmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// heapq's min-heap of (f64, key) tuples on a BinaryHeap (which pops the greatest).
#[derive(PartialEq)]
pub struct MinFK(pub f64, pub (i64, i64), pub u32);
impl Eq for MinFK {}
impl Ord for MinFK {
    fn cmp(&self, o: &Self) -> Ordering {
        fcmp(o.0, self.0).then(o.1.cmp(&self.1)).then(o.2.cmp(&self.2))
    }
}
impl PartialOrd for MinFK {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

/// stage3.aux_our_blend(value, cont, crack, 1.0)
#[inline]
pub fn our_blend(value: f64, cont: f64, crack: f64) -> f64 {
    let total = cont + crack;
    if crack <= 0.0 || total <= 0.0 {
        return value;
    }
    (value * cont + 1.0 * crack) / total
}

#[derive(Default, Clone)]
pub struct Core {
    pub sort_aliases: HashMap<i64, (i64, i64)>,
    pub keys: Vec<Key>,
    pub n_our: usize,
    pub id: HashMap<Key, u32>,
    pub root: Key,
    // our nodes
    pub l_node: Vec<f64>,
    pub crack: Vec<Option<(f64, f64)>>,
    pub c_ptr: Vec<usize>,
    pub c_san: Vec<i64>,
    pub c_bare: Vec<i64>,
    pub c_key: Vec<Option<Key>>,
    pub c_in: Vec<bool>,
    pub c_lv: Vec<f64>,
    pub c_node: Vec<u32>,
    // opponent nodes (index o = id - n_our)
    pub const_base: Vec<f64>,
    pub k_ptr: Vec<usize>,
    pub k_p: Vec<f64>,
    pub k_key: Vec<Option<Key>>,
    pub k_in: Vec<bool>,
    pub k_lv: Vec<f64>,
    pub k_node: Vec<u32>,
    pub f_ptr: Vec<usize>,
    pub f_p: Vec<f64>,
    pub f_key: Vec<Key>,
    pub f_lv: Vec<f64>,
    /// g.reach.get(h, 0.0) per node
    pub node_reach: Vec<f64>,
    pub set_order: Vec<u32>,
    pub crack_at_parents: bool,
    /// stickiness (g.meta["stick"]): per our node the incumbent's bare code (-1 none)
    pub stick: bool,
    pub inc_bare: Vec<i64>,
    pub margin: f64,
}

impl Core {
    pub fn sorted_key(&self, k: Key) -> (i64, i64) {
        if self.sort_aliases.is_empty() { return key_order(k); }
        self.sort_aliases.get(&pk(k)).copied().unwrap_or_else(|| key_order(k))
    }
    pub fn n(&self) -> usize {
        self.keys.len()
    }
    #[inline]
    pub fn is_our(&self, i: u32) -> bool {
        (i as usize) < self.n_our
    }
    #[inline]
    pub fn cands(&self, u: usize) -> std::ops::Range<usize> {
        self.c_ptr[u]..self.c_ptr[u + 1]
    }
    #[inline]
    pub fn kids(&self, i: usize) -> std::ops::Range<usize> {
        let o = i - self.n_our;
        self.k_ptr[o]..self.k_ptr[o + 1]
    }
    #[inline]
    pub fn floors(&self, i: usize) -> std::ops::Range<usize> {
        let o = i - self.n_our;
        self.f_ptr[o]..self.f_ptr[o + 1]
    }
    pub fn root_id(&self) -> u32 {
        self.id.get(&self.root).copied().unwrap_or(NONE)
    }
    #[inline]
    pub fn pyk(&self, i: u32) -> i64 {
        pk(self.keys[i as usize])
    }

    /// incumbent_move + _bonus: the selection-key bonus of candidate j at our node u
    #[inline]
    pub fn bonus(&self, u: usize, j: usize) -> f64 {
        if !self.stick {
            return 0.0;
        }
        let im = self.inc_bare[u];
        if im >= 0 && self.c_bare[j] == im { self.margin } else { 0.0 }
    }

    /// Fill c_node / k_node from the keys (after the arrays are loaded).
    pub fn link(&mut self) {
        let id = &self.id;
        self.c_node = self.c_key.iter().zip(&self.c_in).map(|(k, &ins)| match k {
            Some(k) if ins => id.get(k).copied().unwrap_or(NONE),
            _ => NONE,
        }).collect();
        self.k_node = self.k_key.iter().zip(&self.k_in).map(|(k, &ins)| match k {
            Some(k) if ins => id.get(k).copied().unwrap_or(NONE),
            _ => NONE,
        }).collect();
    }

    /// _adjacency's children of node i: in-subgraph children with a key, as (key, node id or
    /// NONE), in list order
    pub fn children(&self, i: usize) -> Vec<(Key, u32)> {
        let mut out = Vec::new();
        if i < self.n_our {
            for j in self.cands(i) {
                if let (true, Some(k)) = (self.c_in[j], self.c_key[j]) {
                    out.push((k, self.c_node[j]));
                }
            }
        } else {
            for j in self.kids(i) {
                if let (true, Some(k)) = (self.k_in[j], self.k_key[j]) {
                    out.push((k, self.k_node[j]));
                }
            }
        }
        out
    }

    /// _adjacency over node ids: (children that are nodes, with duplicates; parents in
    /// set-order of the parent, duplicates kept)
    pub fn adjacency(&self) -> (Vec<Vec<u32>>, Vec<Vec<u32>>) {
        let n = self.n();
        let mut ch: Vec<Vec<u32>> = (0..n)
            .map(|i| self.children(i).into_iter().map(|(_, c)| c).filter(|&c| c != NONE).collect())
            .collect();
        let mut par: Vec<Vec<u32>> = vec![Vec::new(); n];
        for &h in &self.set_order {
            for &c in &ch[h as usize] {
                par[c as usize].push(h);
            }
        }
        ch.shrink_to_fit();
        (ch, par)
    }

    /// _static_value: a node's value with nothing booked below it
    pub fn static_value(&self, i: usize) -> f64 {
        if i < self.n_our {
            return self.l_node[i];
        }
        let o = i - self.n_our;
        let mut s = 0.0;
        for j in self.kids(i) {
            let c = self.k_node[j];
            s += self.k_p[j] * if c != NONE && self.is_our(c) {
                self.l_node[c as usize]
            } else {
                self.k_lv[j]
            };
        }
        self.const_base[o] + s
    }

    /// topo_up: (bottom-up order, nodes placed early to cut cycles), as node ids
    pub fn topo_up(&self) -> (Vec<u32>, Vec<u32>) {
        let nodes = &self.set_order;
        let n = nodes.len();
        // ids in set order -> position in `nodes`
        let mut pos_of = vec![u32::MAX; self.n()];
        for (x, &h) in nodes.iter().enumerate() {
            pos_of[h as usize] = x as u32;
        }
        let succ: Vec<Vec<u32>> = nodes.iter().map(|&h| {
            self.children(h as usize).into_iter()
                .filter(|&(_, c)| c != NONE).map(|(_, c)| pos_of[c as usize]).collect()
        }).collect();
        let mut index = vec![-1i64; n];
        let mut low = vec![0i64; n];
        let mut onst = vec![false; n];
        let mut stack: Vec<u32> = Vec::new();
        let mut order: Vec<u32> = Vec::with_capacity(n);
        let mut early: Vec<u32> = Vec::new();
        let mut counter = 0i64;
        for root in 0..n {
            if index[root] >= 0 {
                continue;
            }
            let mut wn: Vec<usize> = vec![root];
            let mut wc: Vec<usize> = vec![0];
            while let Some(&node) = wn.last() {
                let mut ci = *wc.last().unwrap();
                if ci == 0 {
                    index[node] = counter;
                    low[node] = counter;
                    counter += 1;
                    stack.push(node as u32);
                    onst[node] = true;
                }
                let kids = &succ[node];
                let mut advanced = false;
                while ci < kids.len() {
                    let ch = kids[ci] as usize;
                    ci += 1;
                    if index[ch] < 0 {
                        *wc.last_mut().unwrap() = ci;
                        wn.push(ch);
                        wc.push(0);
                        advanced = true;
                        break;
                    }
                    if onst[ch] && index[ch] < low[node] {
                        low[node] = index[ch];
                    }
                }
                if advanced {
                    continue;
                }
                wn.pop();
                wc.pop();
                if low[node] == index[node] {
                    let mut w = stack.pop().unwrap() as usize;
                    onst[w] = false;
                    if w == node {
                        order.push(nodes[w]);
                    } else {
                        let mut scc = vec![w];
                        while w != node {
                            w = stack.pop().unwrap() as usize;
                            onst[w] = false;
                            scc.push(w);
                        }
                        self.drain_cycle(&scc, &succ, nodes, &mut order, &mut early);
                    }
                }
                if let Some(&parent) = wn.last() {
                    if low[node] < low[parent] {
                        low[parent] = low[node];
                    }
                }
            }
        }
        (order, early)
    }

    /// _drain_cycle on one multi-node SCC (positions in set order)
    fn drain_cycle(&self, scc: &[usize], succ: &[Vec<u32>], nodes: &[u32],
                   order: &mut Vec<u32>, early: &mut Vec<u32>) {
        let members: std::collections::HashSet<usize> = scc.iter().copied().collect();
        let mut pending: HashMap<usize, i64> = HashMap::new();
        let mut rev: HashMap<usize, Vec<usize>> = HashMap::new();
        for &h in scc {
            pending.insert(h, succ[h].iter().filter(|&&c| members.contains(&(c as usize))).count() as i64);
            rev.insert(h, Vec::new());
        }
        for &h in scc {
            for &c in &succ[h] {
                if members.contains(&(c as usize)) {
                    rev.get_mut(&(c as usize)).unwrap().push(h);
                }
            }
        }
        let mut done: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut q: VecDeque<usize> = scc.iter().copied().filter(|h| pending[h] == 0).collect();
        let mut by_reach: Vec<usize> = scc.to_vec();
        by_reach.sort_by(|&a, &b| {
            let (ia, ib) = (nodes[a], nodes[b]);
            fcmp(self.node_reach[ia as usize], self.node_reach[ib as usize])
                .then_with(|| self.sorted_key(self.keys[ia as usize]).cmp(&self.sorted_key(self.keys[ib as usize])))
        });
        let mut k = 0usize;
        while done.len() < scc.len() {
            if q.is_empty() {
                while done.contains(&by_reach[k]) {
                    k += 1;
                }
                early.push(nodes[by_reach[k]]);
                q.push_back(by_reach[k]);
            }
            let h = q.pop_front().unwrap();
            if done.contains(&h) {
                continue;
            }
            done.insert(h);
            order.push(nodes[h]);
            for &par in &rev[&h] {
                if !done.contains(&par) {
                    let p = pending.get_mut(&par).unwrap();
                    *p -= 1;
                    if *p <= 0 {
                        q.push_back(par);
                    }
                }
            }
        }
    }

    /// topo_down: top-down order, as node ids
    pub fn topo_down(&self) -> Vec<u32> {
        let (children, parents) = self.adjacency();
        let n = self.n();
        let mut indeg: Vec<i64> = parents.iter().map(|p| p.len() as i64).collect();
        let mut q: VecDeque<u32> = self.set_order.iter().copied()
            .filter(|&h| indeg[h as usize] == 0).collect();
        let root = self.root_id();
        if root != NONE && indeg[root as usize] != 0 {
            q.push_front(root);
        }
        let mut order = Vec::with_capacity(n);
        let mut placed = vec![false; n];
        let mut remaining = n;
        let mut heap: BinaryHeap<MinFK> = self.set_order.iter()
            .map(|&h| MinFK(-self.node_reach[h as usize], self.sorted_key(self.keys[h as usize]), h)).collect();
        loop {
            while let Some(h) = q.pop_front() {
                if placed[h as usize] {
                    continue;
                }
                placed[h as usize] = true;
                remaining -= 1;
                order.push(h);
                for &c in &children[h as usize] {
                    if !placed[c as usize] {
                        indeg[c as usize] -= 1;
                        if indeg[c as usize] <= 0 {
                            q.push_back(c);
                        }
                    }
                }
            }
            if remaining == 0 {
                break;
            }
            let mut victim = NONE;
            while let Some(MinFK(_, _, c)) = heap.pop() {
                if !placed[c as usize] {
                    victim = c;
                    break;
                }
            }
            if victim == NONE {
                break;
            }
            q.push_back(victim);
        }
        order
    }

    /// stop_model_positions: (our-turn x, opponent-turn y), as sets of keys
    pub fn stop_positions(&self) -> (Vec<Key>, Vec<Key>) {
        let mut xs: std::collections::HashSet<Key> = self.keys[..self.n_our].iter().copied().collect();
        let mut ys: std::collections::HashSet<Key> = std::collections::HashSet::new();
        for j in 0..self.c_key.len() {
            if let (Some(k), false) = (self.c_key[j], self.c_in[j]) {
                ys.insert(k);
            }
        }
        for j in 0..self.k_key.len() {
            if let (Some(k), false) = (self.k_key[j], self.k_in[j]) {
                xs.insert(k);
            }
        }
        xs.extend(self.f_key.iter().copied());
        let mut x: Vec<Key> = xs.into_iter().collect();
        let mut y: Vec<Key> = ys.into_iter().collect();
        x.sort_by_key(|&k| self.sorted_key(k));
        y.sort_by_key(|&k| self.sorted_key(k));
        (x, y)
    }

    /// apply_stop_values: W for our-turn positions, R for the opponent-turn position after
    /// our book's last move. Returns (our_l, cand_leaf, opp_leaf, opp_floor).
    pub fn apply_stop_values(&mut self, w: &HashMap<Key, f64>, r: &HashMap<Key, f64>)
                             -> (i64, i64, i64, i64) {
        let (mut our_l, mut cand_leaf, mut opp_leaf, mut opp_floor) = (0, 0, 0, 0);
        for u in 0..self.n_our {
            if let Some(&wv) = w.get(&self.keys[u]) {
                self.l_node[u] = match self.crack[u] {
                    Some((cont, crack)) if !self.crack_at_parents => our_blend(wv, cont, crack),
                    _ => wv,
                };
                our_l += 1;
            }
            for j in self.cands(u) {
                if let (Some(k), false) = (self.c_key[j], self.c_in[j]) {
                    if let Some(&rv) = r.get(&k) {
                        self.c_lv[j] = rv;
                        cand_leaf += 1;
                    }
                }
            }
        }
        for i in self.n_our..self.n() {
            for j in self.kids(i) {
                if let (Some(k), false) = (self.k_key[j], self.k_in[j]) {
                    if let Some(&wv) = w.get(&k) {
                        self.k_lv[j] = wv;
                        opp_leaf += 1;
                    }
                }
            }
            let o = i - self.n_our;
            for j in self.floors(i) {
                if let Some(&wv) = w.get(&self.f_key[j]) {
                    self.const_base[o] += self.f_p[j] * (wv - self.f_lv[j]);
                    opp_floor += 1;
                }
            }
        }
        (our_l, cand_leaf, opp_leaf, opp_floor)
    }
}

impl Core {
    /// Every in-subgraph child with a key is a node of the graph (what the region walk and
    /// polish assume; budget_compiled compiles ok=False otherwise and Python falls back)
    pub fn closed(&self) -> bool {
        let r = self.root_id() != NONE;
        r && self.c_in.iter().zip(&self.c_key).zip(&self.c_node)
            .all(|((&i, k), &c)| !(i && k.is_some()) || c != NONE)
            && self.k_in.iter().zip(&self.k_key).zip(&self.k_node)
            .all(|((&i, k), &c)| !(i && k.is_some()) || c != NONE)
    }
}

/// One row of sampled_stop.stop_positions (its EPDs are attached in Python)
pub struct DumpRow {
    pub x: bool,
    pub h: Key,
    pub ply: i64,
    pub reach: f64,
    pub parent: Option<Key>,
    pub plies: u64,          // the plies <= the horizon it is reached at (n_plies = popcount)
}

impl Core {
    /// sampled_stop.ply_sets: the plies (<= horizon) at which each position is reached, walking
    /// the subgraph level by level from the root, as a bitmask
    fn ply_sets(&self, horizon: i64) -> HashMap<Key, u64> {
        let mut out: HashMap<Key, u64> = HashMap::new();
        let mut level: std::collections::HashSet<Key> = std::collections::HashSet::new();
        level.insert(self.root);
        let mut t = 0i64;
        while !level.is_empty() && t <= horizon {
            let mut nxt = std::collections::HashSet::new();
            for &h in &level {
                *out.entry(h).or_insert(0) |= 1u64 << t;
                if let Some(&i) = self.id.get(&h) {
                    for (k, _c) in self.children(i as usize) {
                        nxt.insert(k);
                    }
                }
            }
            level = nxt;
            t += 1;
        }
        out
    }

    /// sampled_stop.stop_positions: every stop position, (side, hash) once, with the ply and
    /// reach of its most likely route and the plies it is reached at
    pub fn stop_dump(&self, ply: &HashMap<Key, i64>, route: &HashMap<Key, i64>, horizon: i64)
                     -> Result<Vec<DumpRow>, String> {
        if !(0..63).contains(&horizon) {
            return Err(format!("horizon {horizon} out of range"));
        }
        let plies = self.ply_sets(horizon);
        let keep = (1u64 << (horizon + 1)) - 1;
        let bit = |p: i64| if (0..64).contains(&p) { 1u64 << p } else { 0 };
        let mut rows: Vec<DumpRow> = Vec::new();
        let mut at: HashMap<(bool, Key), usize> = HashMap::new();
        let mut add = |x: bool, h: Key, p: i64, reach: f64, parent: Option<Key>, seen: u64| {
            match at.get(&(x, h)) {
                None => {
                    at.insert((x, h), rows.len());
                    rows.push(DumpRow { x, h, ply: p, reach, parent, plies: seen & keep });
                }
                Some(&i) => {
                    let r = &mut rows[i];
                    r.plies |= seen & keep;
                    if reach > r.reach || (reach == r.reach && p < r.ply) {
                        r.ply = p;
                        r.reach = reach;
                        r.parent = parent;
                    }
                }
            }
        };
        let p0_of = |h: Key| route.get(&h).copied().unwrap_or_else(|| ply.get(&h).copied().unwrap_or(0));
        for u in 0..self.n_our {
            let h = self.keys[u];
            let p0 = p0_of(h);
            let ph = plies.get(&h).copied().unwrap_or(bit(p0));
            let r0 = self.node_reach[u];
            add(true, h, p0, r0, None, ph);
            for j in self.cands(u) {
                if let (Some(ch), false) = (self.c_key[j], self.c_in[j]) {
                    add(false, ch, p0 + 1, r0, Some(h), ph << 1);
                }
            }
        }
        for i in self.n_our..self.n() {
            let h = self.keys[i];
            let p0 = p0_of(h);
            let r0 = self.node_reach[i];
            let nxt = plies.get(&h).copied().unwrap_or(bit(p0)) << 1;
            for j in self.kids(i) {
                if let (Some(ch), false) = (self.k_key[j], self.k_in[j]) {
                    add(true, ch, p0 + 1, r0 * self.k_p[j], Some(h), nxt);
                }
            }
            for j in self.floors(i) {
                add(true, self.f_key[j], p0 + 1, r0 * self.f_p[j], Some(h), nxt);
            }
        }
        let late: Vec<i64> = rows.iter().filter(|r| r.ply > horizon).map(|r| r.ply).collect();
        if !late.is_empty() {
            return Err(format!("{} stop positions past the horizon {horizon}, up to ply {}",
                               late.len(), late.iter().max().unwrap()));
        }
        Ok(rows)
    }

    /// sampled_other.apply's change: const_base += delta at the given opponent nodes
    pub fn add_const_base(&mut self, keys: &[Key], deltas: &[f64]) -> Result<(), String> {
        for (k, d) in keys.iter().zip(deltas) {
            let i = *self.id.get(k).ok_or("not a node")? as usize;
            if i < self.n_our {
                return Err("not an opponent node".into());
            }
            self.const_base[i - self.n_our] += d;
        }
        Ok(())
    }
}
