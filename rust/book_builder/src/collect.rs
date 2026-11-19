//! build_budget_books.collect_subgraph_edges' walk and GateAdmit, operation for operation.
//!
//! DuckDB still scans the pool: each level's rows arrive here sorted (parent, move_san,
//! child), as the Python collector sorts them, with SAN and bare-SAN codes (any consistent
//! coding: the gates only compare codes for equality). The walk keeps reach, ply and the
//! fetched set; the gates' eval cache follows GateAdmit exactly: a hash is known once a
//! level's prefetch has looked it up (an eval stored only when the arrays have one), or
//! when the engine candidates gave it a fresh eval.

use std::collections::{HashMap, HashSet};

use crate::gates::{self, Cand, GateParams, Ref};
use crate::reach::Key;
use crate::value::cp_to_expected_score;

pub struct Gate {
    pub params: GateParams,
    pub src: HashMap<Key, i64>,              // the source book's move (SAN code)
    pub excluded: HashMap<Key, Vec<i64>>,    // vetoed bare codes
    pub verified: HashMap<Key, Vec<i64>>,    // bare codes searched afresh
    /// eval_ws + _seen: NaN = looked up, no eval
    pub cache: HashMap<Key, f64>,
}

pub struct Collector {
    pub our_white: bool,
    pub eps: f64,
    pub max_ply: i64,
    pub share_floor: f64,
    pub reach: HashMap<Key, f64>,
    depth: i64,
    pub source_children: HashMap<Key, Key>,
    pub fetched: HashSet<Key>,
    pub frontier: Vec<Key>,
    pub reopened: i64,
    pub gate: Option<Gate>,
    /// engine proposals per parent: (SAN code, bare code, child)
    pub engine: HashMap<Key, Vec<(i64, i64, Option<Key>)>>,
}

/// One level's rows, sorted by parent
pub struct Level<'a> {
    pub parent: &'a [Key],
    pub child: &'a [Option<Key>],
    pub total: &'a [i64],
    pub san: &'a [i64],
    pub bare: &'a [i64],
    /// the opponent prior's reach_share per row (NaN = none: total / the parent's total)
    pub share: Option<&'a [f64]>,
}

impl Collector {
    pub fn new(root: Key, our_white: bool, eps: f64, max_ply: i64, share_floor: f64) -> Collector {
        let mut reach = HashMap::new();
        reach.insert(root, 1.0);
        Collector { our_white, eps, max_ply, share_floor, reach, depth: 0, fetched: HashSet::new(),
                    source_children: HashMap::new(), frontier: vec![root], reopened: 0, gate: None, engine: HashMap::new() }
    }

    /// The frontier's parents not fetched before (their rows are kept); marks the whole
    /// frontier fetched.
    pub fn begin_level(&mut self) -> Vec<Key> {
        let fresh: Vec<Key> = self.frontier.iter().copied().filter(|h| !self.fetched.contains(h)).collect();
        self.fetched.extend(self.frontier.iter().copied());
        fresh
    }

    /// GateAdmit.prefetch's new hashes: those the cache has not looked up (unique)
    pub fn unseen(&self, lv: &Level) -> Vec<Key> {
        let Some(gt) = &self.gate else { return Vec::new() };
        let mut out: HashSet<Key> = HashSet::new();
        for &p in lv.parent {
            if !gt.cache.contains_key(&p) {
                out.insert(p);
            }
        }
        for c in lv.child.iter().flatten() {
            if !gt.cache.contains_key(c) {
                out.insert(*c);
            }
        }
        out.into_iter().collect()
    }

    /// The prefetch's results: cp per new hash (MISSING = -32768)
    pub fn store(&mut self, keys: &[Key], cps: &[i16]) {
        if let Some(gt) = &mut self.gate {
            for (&k, &c) in keys.iter().zip(cps) {
                let v = if c == -32768 { f64::NAN } else { cp_to_expected_score(c as f64) };
                gt.cache.entry(k).or_insert(v);
            }
        }
    }

    fn admit(gt: &Gate, ph: Key, rows: &[(Option<Key>, i64, i64, i64, bool)]) -> HashSet<Key> {
        let veto = gt.excluded.get(&ph);
        let ev = |k: &Option<Key>| -> f64 {
            match k {
                Some(k) => gt.cache.get(k).copied().unwrap_or(f64::NAN),
                None => f64::NAN,
            }
        };
        let cands: Vec<Cand> = rows.iter().enumerate()
            .filter(|(_, r)| veto.map_or(true, |v| !v.contains(&r.2)))
            .map(|(i, r)| Cand { r: Ref::Edge(i), san: r.1, bare: r.2, eval: ev(&r.0), total: r.3, engine: r.4 })
            .collect();
        let own = gt.cache.get(&ph).copied().unwrap_or(f64::NAN);
        let vr: &[i64] = gt.verified.get(&ph).map_or(&[], |v| v.as_slice());
        let (kept, _) = gates::gate_candidates(&cands, gt.src.get(&ph).copied(), own, vr, &gt.params, false);
        kept.iter().filter_map(|c| match c.r { Ref::Edge(i) => rows[i].0, _ => None }).collect()
    }

    /// The walk over one level: returns the next frontier (in first-improvement order)
    pub fn walk(&mut self, lv: &Level) -> Vec<Key> {
        let n = lv.parent.len();
        let mut nxt: Vec<Key> = Vec::new();
        let mut in_nxt: HashSet<Key> = HashSet::new();
        let mut s = 0;
        while s < n {
            let ph = lv.parent[s];
            let mut e = s + 1;
            while e < n && lv.parent[e] == ph {
                e += 1;
            }
            let (a, b) = (s, e);
            s = e;
            let r = self.reach.get(&ph).copied().unwrap_or(0.0);
            let p_ply = self.depth;
            if p_ply >= self.max_ply {
                continue;
            }
            let tot: i64 = lv.total[a..b].iter().sum();
            let our_turn = (p_ply % 2 == 0) == self.our_white;
            // (child, san, bare, total, engine)
            let mut rows: Vec<(Option<Key>, i64, i64, i64, bool)> =
                (a..b).map(|i| (lv.child[i], lv.san[i], lv.bare[i], lv.total[i], false)).collect();
            if our_turn {
                if let Some(em) = self.engine.get(&ph) {
                    let existing: HashSet<Option<Key>> = rows.iter().map(|r| r.0).collect();
                    for &(san, bare, ch) in em {
                        if !existing.contains(&ch) {
                            rows.push((ch, san, bare, 0, true));
                        }
                    }
                }
            }
            let allowed = match (&self.gate, our_turn) {
                (Some(gt), true) => Some(Self::admit(gt, ph, &rows)),
                _ => None,
            };
            // The collector is a superset. A replayed source can be restored after the
            // ordinary gates, so fetch its descendants without changing the gate reference.
            let ordinary = rows.len();
            if our_turn {
                if let Some(&child) = self.source_children.get(&ph) {
                    rows.push((Some(child), -1, -1, 0, false));
                }
            }
            for (k, row) in rows.iter().enumerate() {
                let Some(ch) = row.0 else { continue };
                let cr = if our_turn {
                    if let Some(al) = &allowed {
                        if k < ordinary && !al.contains(&ch) {
                            continue;
                        }
                    }
                    r
                } else {
                    // reply_share(row, tot); engine rows are only added at our turns, so
                    // row k here is the level's row a + k
                    let given = lv.share.map_or(f64::NAN, |s| s[a + k]);
                    let share = if !given.is_nan() { given }
                                else if tot != 0 { row.3 as f64 / tot as f64 } else { 0.0 };
                    if share < self.share_floor {
                        continue;
                    }
                    r * share
                };
                if cr < self.eps {
                    continue;
                }
                if cr > self.reach.get(&ch).copied().unwrap_or(0.0) {
                    if self.fetched.contains(&ch) {
                        self.reopened += 1;
                    }
                    self.reach.insert(ch, cr);
                    if in_nxt.insert(ch) {
                        nxt.push(ch);
                    }
                }
            }
        }
        self.depth += 1;
        self.frontier = nxt.clone();
        nxt
    }
}
