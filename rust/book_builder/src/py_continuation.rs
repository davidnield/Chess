//! Bounded native short-continuation DAG. Python supplies batched policies/evals;
//! Rust owns legal replay, transpositions, terminal results and backwards induction.
use std::collections::{HashMap, HashSet};
use numpy::{PyArrayLike1, AllowTypeChange};
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use shakmaty::{Chess, Position, Color, EnPassantMode, KnownOutcome,
    fen::Epd, san::SanPlus, zobrist::Zobrist128};
use crate::position_ids::{RsPositionIds,SharedIds};

enum State { Pending, Leaf, Edges(Vec<(f64,Option<usize>)>) }
struct Node { board:Chess, id:i64, depth:u8, state:State, lo:f64, hi:f64 }

#[pyclass(module="book_builder_rs", unsendable)]
pub struct RsContinuation {
    nodes:Vec<Node>, index:HashMap<(i64,u8),usize>, roots:Vec<usize>,
    pending:HashMap<i64,Vec<usize>>,
    keys:SharedIds, white:bool, max_states:usize, min_expand:f64,
}

impl RsContinuation {
    fn intern(&mut self, board:Chess, depth:u8) -> usize {
        let z:Zobrist128=board.zobrist_hash(EnPassantMode::PseudoLegal);
        let id=self.keys.lock().unwrap().intern(z.0 as i64,(z.0>>64) as i64);
        if let Some(i)=self.index.get(&(id,depth)) {return *i;}
        let (state,lo,hi)=if board.is_game_over() {
            let v=match board.outcome() {
                shakmaty::Outcome::Known(KnownOutcome::Decisive{winner}) => if (winner==Color::White)==self.white {1.} else {0.},
                _=>0.5,
            }; (State::Leaf,v,v)
        } else { (if depth==0 {State::Leaf} else {State::Pending},0.,1.) };
        let i=self.nodes.len(); self.index.insert((id,depth),i);
        if matches!(state,State::Pending) {self.pending.entry(id).or_default().push(i);}
        self.nodes.push(Node{board,id,depth,state,lo,hi}); i
    }
}

#[pymethods]
impl RsContinuation {
    #[new]
    fn new(epds:Vec<String>, depths:Vec<u8>, white:bool, position_ids:PyRef<'_,RsPositionIds>,
           max_states:usize,min_expand:f64) -> PyResult<Self> {
        if epds.len()!=depths.len() || max_states<epds.len() || !min_expand.is_finite() || !(0. ..=1.).contains(&min_expand) {
            return Err(PyValueError::new_err("invalid continuation limits"));
        }
        let mut c=Self{nodes:Vec::new(),index:HashMap::new(),roots:Vec::new(),pending:HashMap::new(),keys:position_ids.inner.clone(),white,max_states,min_expand};
        for (epd,depth) in epds.iter().zip(depths) {
            // Position-level inputs carry no halfmove/history claim evidence.
            if epd.split_whitespace().count()!=4 {return Err(PyValueError::new_err("continuation requires position-only EPD"));}
            let board=crate::rollout::from_epd(epd).map_err(PyValueError::new_err)?;
            let i=c.intern(board,depth);c.roots.push(i);
        }
        Ok(c)
    }

    /// Unique pending boards, shared across continuation depths; full legal vectors required.
    fn needed(&self) -> Vec<(i64,String,bool)> {
        let mut seen=HashSet::new();
        self.nodes.iter().filter(|n| matches!(n.state,State::Pending) && seen.insert(n.id))
            .map(|n|(n.id,Epd::from_position(&n.board,EnPassantMode::PseudoLegal).to_string(),(n.board.turn()==Color::White)==self.white)).collect()
    }

    fn expand(&mut self, id:i64, distribution:Vec<(String,f64)>) -> PyResult<()> {
        let indices=self.pending.get(&id).cloned().unwrap_or_default();
        if indices.is_empty() {return Err(PyValueError::new_err("unknown pending continuation board"));}
        let board=&self.nodes[indices[0]].board;
        let p:Vec<f64>=distribution.iter().map(|d|d.1).collect();
        crate::human_policy::expectation(&p,&vec![0.;p.len()],&vec![1.;p.len()]).map_err(PyValueError::new_err)?;
        let mut seen=HashSet::new();let mut moves=Vec::new();
        for (san,_) in &distribution {
            let mv=SanPlus::from_ascii(san.as_bytes()).map_err(|e|PyValueError::new_err(e.to_string()))?
                .san.to_move(board).map_err(|e|PyValueError::new_err(e.to_string()))?;
            if !seen.insert(mv) {return Err(PyValueError::new_err("duplicate policy move"));}
            moves.push(mv);
        }
        if seen.len()!=board.legal_moves().len() {return Err(PyValueError::new_err("policy must cover every legal move"));}
        self.pending.remove(&id);
        for i in indices {
            let mut edges=Vec::new();
            for (&mv,&prob) in moves.iter().zip(&p) {
                if prob==0. {continue;}
                let mut child=self.nodes[i].board.clone();child.play_unchecked(mv);
                let depth=self.nodes[i].depth-1;
                let z:Zobrist128=child.zobrist_hash(EnPassantMode::PseudoLegal);
                let cid=self.keys.lock().unwrap().intern(z.0 as i64,(z.0>>64) as i64);
                let existing=self.index.get(&(cid,depth)).copied();
                let target=if let Some(j)=existing {Some(j)}
                    else if prob>=self.min_expand && self.nodes.len()<self.max_states {Some(self.intern(child,depth))}
                    else {None};
                edges.push((prob,target));
            }
            self.nodes[i].state=State::Edges(edges);
        }
        Ok(())
    }

    fn leaves(&self) -> Vec<i64> {
        let mut seen=HashSet::new();
        self.nodes.iter().filter(|n|matches!(n.state,State::Leaf) && n.lo!=n.hi && seen.insert(n.id)).map(|n|n.id).collect()
    }

    fn set_cp(&mut self, ids:Vec<i64>, cp:PyArrayLike1<f64,AllowTypeChange>) -> PyResult<()> {
        let cp=cp.as_slice()?;
        if ids.len()!=cp.len() {return Err(PyValueError::new_err("continuation eval lengths differ"));}
        let values:HashMap<i64,f64>=ids.into_iter().zip(cp.iter().copied()).collect();
        for n in &mut self.nodes {
            if matches!(n.state,State::Leaf) && n.lo!=n.hi {
                if let Some(&c)=values.get(&n.id) {
                    if c.is_finite() && c != crate::rollout::MISSING {let v=crate::human_policy::utility(c,self.white);n.lo=v;n.hi=v;}
                }
            }
        }
        Ok(())
    }

    fn results(&mut self) -> PyResult<Vec<(i64,f64,f64)>> {
        if !self.pending.is_empty() {return Err(PyValueError::new_err("pending continuation policies"));}
        let mut order:Vec<_>=(0..self.nodes.len()).collect();order.sort_by_key(|i|self.nodes[*i].depth);
        for i in order {
            if let State::Edges(edges)=&self.nodes[i].state {
                let (mut lo,mut hi)=(0.,0.);
                for &(p,j) in edges {let (l,h)=j.map(|j|(self.nodes[j].lo,self.nodes[j].hi)).unwrap_or((0.,1.));lo+=p*l;hi+=p*h;}
                self.nodes[i].lo=lo;self.nodes[i].hi=hi;
            }
        }
        Ok(self.roots.iter().map(|i|{let n=&self.nodes[*i];(n.id,n.lo,n.hi)}).collect())
    }

    fn state_count(&self) -> usize {self.nodes.len()}
}
