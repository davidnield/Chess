//! Opt-in Rust optimizer study. No production defaults or engine gates change.
//! All algorithms use the same frozen, admitted Core. Cycles use Core's fixed
//! topological-cut/static-value convention, NOT a claim of exact chess repetition.
pub mod benchmark;
pub mod exact;
pub mod fixtures;
pub mod search;
pub mod snapshot;
#[cfg(test)]
mod tests;

use crate::{
    core::{Core, NONE},
    polish,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub type Book = BTreeMap<u32, i64>;
pub type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Outcome {
    pub book: Book,
    pub value: f64,
    pub status: String,
    pub bound: Option<f64>,
    pub bound_scope: Option<String>,
    pub states: u64,
    pub seconds: f64,
}

pub struct View<'a> {
    pub g: &'a Core,
    pub order: Vec<u32>,
    pub pos: Vec<u32>,
    pub stat: Vec<f64>,
    pub cuts: usize,
}
#[derive(Clone, Copy)]
pub enum Child {
    Node(u32),
    Terminal(f64),
}
impl<'a> View<'a> {
    pub fn new(g: &'a Core) -> Result<Self> {
        snapshot::validate(g)?;
        let (order, cut) = g.topo_up();
        let pos = polish::positions(g, &order);
        Ok(Self {
            g,
            order,
            pos,
            stat: polish::statics(g),
            cuts: cut.len(),
        })
    }
    fn child(&self, parent: u32, child: u32, leaf: f64) -> Child {
        if child == NONE {
            Child::Terminal(leaf)
        } else if self.pos[child as usize] < self.pos[parent as usize] {
            Child::Node(child)
        } else {
            Child::Terminal(self.stat[child as usize])
        }
    }
    pub fn candidate(&self, u: u32, j: usize) -> Child {
        self.child(u, self.g.c_node[j], self.g.c_lv[j])
    }
    pub fn reply(&self, u: u32, j: usize) -> Child {
        self.child(u, self.g.k_node[j], self.g.k_lv[j])
    }
    pub fn candidate_index(&self, u: u32, san: i64) -> Result<usize> {
        if !self.g.is_our(u) {
            return Err(format!("not our position: {u}"));
        }
        self.g
            .cands(u as usize)
            .find(|&j| self.g.c_san[j] == san)
            .ok_or_else(|| format!("move {san} not admitted at {u}"))
    }
    pub fn evaluate(&self, book: &Book, budget: usize) -> Result<Outcome> {
        for (&u, &s) in book {
            self.candidate_index(u, s)?;
        }
        let m: HashMap<_, _> = book.iter().map(|(&u, &s)| (u, s)).collect();
        let region = polish::book_region(self.g, &m, &self.pos, &self.stat);
        if region.size() > budget {
            return Err(format!("budget exceeded: {} > {budget}", region.size()));
        }
        if !region.value.is_finite() {
            return Err("nonfinite book value".into());
        }
        Ok(Outcome {
            book: region.booked.into_iter().collect(),
            value: region.value,
            status: "feasible".into(),
            bound: None,
            bound_scope: None,
            states: 0,
            seconds: 0.,
        })
    }
    /// Independent bottom-up evaluator used by the oracle and MIP validation.
    pub fn independent_values(&self, book: &Book) -> Vec<f64> {
        let mut v = vec![0.; self.g.n()];
        for &u in &self.order {
            let ch = |c| match c {
                Child::Node(n) => v[n as usize],
                Child::Terminal(x) => x,
            };
            v[u as usize] = if self.g.is_our(u) {
                match book.get(&u) {
                    Some(&s) => ch(self.candidate(u, self.candidate_index(u, s).unwrap())),
                    None => self.g.l_node[u as usize],
                }
            } else {
                self.g
                    .kids(u as usize)
                    .fold(self.g.const_base[u as usize - self.g.n_our], |x, j| {
                        x + self.g.k_p[j] * ch(self.reply(u, j))
                    })
            };
        }
        v
    }
    pub fn unbudgeted_bound(&self) -> f64 {
        let mut v = vec![0.; self.g.n()];
        for &u in &self.order {
            let ch = |c| match c {
                Child::Node(n) => v[n as usize],
                Child::Terminal(x) => x,
            };
            v[u as usize] = if self.g.is_our(u) {
                self.g
                    .cands(u as usize)
                    .fold(self.g.l_node[u as usize], |a, j| {
                        a.max(ch(self.candidate(u, j)))
                    })
            } else {
                self.g
                    .kids(u as usize)
                    .fold(self.g.const_base[u as usize - self.g.n_our], |a, j| {
                        a + self.g.k_p[j] * ch(self.reply(u, j))
                    })
            };
        }
        v[self.g.root_id() as usize]
    }
}
