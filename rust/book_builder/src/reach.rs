//! budget_core.optimistic_reach and route_plies, operation for operation.
//!
//! The graph arrives as CSR: parent row i has edges ptr[i]..ptr[i+1], in the order of
//! edges_by_parent[h] (the collector's fixed order; the push counter that breaks heap ties
//! follows it). Results come back in Python's dict insertion orders, which decide the node
//! order of everything built from them.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;

/// A position key. Today's keys are 64-bit (the low half); 128-bit keys fill the high half.
pub type Key = u128;

pub struct Edges<'a> {
    pub parents: &'a [Key],
    pub ptr: &'a [i64],
    pub child: &'a [Option<Key>],
    pub total: &'a [i64],
    pub share: &'a [f64], // reach_share, NaN = absent
    /// --gated-reach: per parent row, the admitted child keys (None = every recorded move)
    pub admitted: Option<(&'a [i64], &'a [Key])>,
}

impl<'a> Edges<'a> {
    pub fn row_of(&self) -> HashMap<Key, usize> {
        self.parents.iter().enumerate().map(|(i, &h)| (h, i)).collect()
    }
    fn range(&self, row: usize) -> std::ops::Range<usize> {
        self.ptr[row] as usize..self.ptr[row + 1] as usize
    }
    fn admits(&self, row: usize, ch: Key) -> bool {
        match self.admitted {
            None => true,
            Some((p, keys)) => keys[p[row] as usize..p[row + 1] as usize].contains(&ch),
        }
    }
    /// sum(e["total"] for e in es): integers, exact
    fn tot(&self, row: usize) -> i64 {
        self.total[self.range(row)].iter().sum()
    }
    /// budget_core.reply_share
    fn share(&self, e: usize, tot: i64) -> f64 {
        let s = self.share[e];
        if !s.is_nan() {
            return s;
        }
        if tot != 0 { self.total[e] as f64 / tot as f64 } else { 0.0 }
    }
}

#[derive(PartialEq)]
struct Item {
    neg_r: f64,
    depth: i64,
    tie: u64,
    h: Key,
}
impl Eq for Item {}
impl Ord for Item {
    // heapq pops the smallest (-cr, tie); BinaryHeap pops the greatest, so reverse it
    fn cmp(&self, o: &Self) -> Ordering {
        o.neg_r.partial_cmp(&self.neg_r).unwrap().then(o.tie.cmp(&self.tie))
    }
}
impl PartialOrd for Item {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

/// Insertion-ordered map, as a Python dict iterates.
#[derive(Default, Clone)]
pub struct OrderedMap<V> {
    pub keys: Vec<Key>,
    pub vals: Vec<V>,
    pub index: HashMap<Key, usize>,
}
impl<V: Copy> OrderedMap<V> {
    pub fn get(&self, k: &Key) -> Option<V> {
        self.index.get(k).map(|&i| self.vals[i])
    }
    pub fn contains(&self, k: &Key) -> bool {
        self.index.contains_key(k)
    }
    pub fn set(&mut self, k: Key, v: V) {
        match self.index.get(&k) {
            Some(&i) => self.vals[i] = v,
            None => {
                self.index.insert(k, self.keys.len());
                self.keys.push(k);
                self.vals.push(v);
            }
        }
    }
}

/// budget_core.optimistic_reach: (reach, ply) in Python's insertion orders.
pub fn optimistic_reach(g: &Edges, rows: &HashMap<Key, usize>, root: Key, root_our: bool,
                        eps: f64, max_ply: i64, share_floor: f64)
                        -> (OrderedMap<f64>, OrderedMap<i64>) {
    let mut reach = OrderedMap::<f64>::default();
    let mut ply = OrderedMap::<i64>::default();
    let mut heap = BinaryHeap::new();
    heap.push(Item { neg_r: -1.0, depth: 0, tie: 0, h: root });
    let mut tie = 1u64;
    while let Some(Item { neg_r, depth: p_ply, h, .. }) = heap.pop() {
        let r = -neg_r;
        // Previously popped labels have at least this reach. Only a shorter
        // arrival can still reach a new continuation inside the horizon.
        if ply.get(&h).is_some_and(|depth| depth <= p_ply) {
            continue;
        }
        if !reach.contains(&h) {
            reach.set(h, r);
        }
        ply.set(h, p_ply);
        if p_ply >= max_ply {
            continue;
        }
        let Some(&row) = rows.get(&h) else { continue };
        let rg = g.range(row);
        if rg.is_empty() {
            continue;
        }
        let our_turn = (p_ply % 2 == 0) == root_our;
        let tot = g.tot(row);
        for e in rg {
            let Some(ch) = g.child[e] else { continue };
            let cr = if our_turn {
                if !g.admits(row, ch) {
                    continue;
                }
                r
            } else {
                let s = g.share(e, tot);
                if s < share_floor {
                    continue;
                }
                r * s
            };
            if cr < eps || ply.get(&ch).is_some_and(|depth| depth <= p_ply + 1) {
                continue;
            }
            heap.push(Item { neg_r: -cr, depth: p_ply + 1, tie, h: ch });
            tie += 1;
        }
    }
    (reach, ply)
}

/// budget_core.route_plies: each position's highest-reach in-time route ply, in Python's
/// insertion order.
#[allow(clippy::too_many_arguments)]
pub fn route_plies(g: &Edges, rows: &HashMap<Key, usize>, root: Key, root_our: bool,
                   reach: &OrderedMap<f64>, ply: &OrderedMap<i64>, max_ply: i64,
                   share_floor: f64) -> OrderedMap<i64> {
    let mut best = OrderedMap::<(f64, i64)>::default();
    best.set(root, (1.0, 0));
    let mut level = OrderedMap::<f64>::default();
    level.set(root, 1.0);
    let mut t = 0i64;
    while !level.keys.is_empty() && t < max_ply {
        let mut nxt = OrderedMap::<f64>::default();
        let our_turn = (t % 2 == 0) == root_our;
        for (i, &h) in level.keys.iter().enumerate() {
            let r = level.vals[i];
            if ply.get(&h).unwrap() >= max_ply {
                continue;
            }
            let Some(&row) = rows.get(&h) else { continue };
            let rg = g.range(row);
            if rg.is_empty() {
                continue;
            }
            let tot = g.tot(row);
            for e in rg {
                let Some(ch) = g.child[e] else { continue };
                if !reach.contains(&ch) {
                    continue;
                }
                let cr = if our_turn {
                    if !g.admits(row, ch) {
                        continue;
                    }
                    r
                } else {
                    let s = g.share(e, tot);
                    if s < share_floor {
                        continue;
                    }
                    r * s
                };
                if cr > nxt.get(&ch).unwrap_or(0.0) {
                    nxt.set(ch, cr);
                }
            }
        }
        t += 1;
        for (i, &ch) in nxt.keys.iter().enumerate() {
            let cr = nxt.vals[i];
            match best.get(&ch) {
                Some(b) if !(cr > b.0 * (1.0 + 1e-12)) => {}
                _ => best.set(ch, (cr, t)),
            }
        }
        level = nxt;
    }
    let mut out = OrderedMap::<i64>::default();
    for (i, &h) in best.keys.iter().enumerate() {
        out.set(h, best.vals[i].1);
    }
    out
}
