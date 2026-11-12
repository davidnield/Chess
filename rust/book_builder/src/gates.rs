//! The candidate gates: budget_core.passes_abs_gate, rel_gate_keep, eval_loss_keep,
//! gate_candidates and final_candidates, operation for operation (value.rs states the
//! float rules). Moves are interned: `san` codes are assigned in sorted SAN order (so
//! comparing codes orders strings as Python does), `bare` codes identify the SAN without
//! its check suffix (`.rstrip("+#")`). A Python `None` eval is NaN.
//!
//! An edge reference (`Ref`) names a recorded edge by its index or an engine-proposed
//! move by its index in the node's engine list; the kept list keeps Python's order.

use crate::value::ws_to_cp;

/// budget_core.CP_EPS
pub const CP_EPS: f64 = 1e-6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ref {
    Edge(usize),
    Engine(usize),
}

/// One candidate move as the gates see it.
#[derive(Clone, Copy, Debug)]
pub struct Cand {
    pub r: Ref,
    pub san: i64,
    pub bare: i64,
    pub eval: f64, // the child's eval (White view), NaN = None
    pub total: i64,
    pub engine: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct GateParams {
    pub our_white: bool,
    pub slice_prior: f64,
    pub robustness_floor: f64,
    pub gate_rel_floor: f64,
    pub gate_rel_own_margin: f64,
    pub require_eval: bool,
    pub gates: bool,
    pub max_eval_loss: Option<f64>,
    pub rare_floor: Option<i64>,
    pub strict_source: bool,
}

/// What a dispute records (budget_core's disputes[h] dict). Moves are SAN codes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Dispute {
    pub reference_cp: Option<f64>,
    pub threats: Vec<(i64, f64)>,
    pub at_risk: Vec<i64>,
    pub fallback: bool,
    pub unresolved: bool,
}

pub fn passes_abs_gate(cev: f64, slice_prior: f64, our_white: bool, robustness_floor: f64) -> bool {
    if cev.is_nan() {
        return false;
    }
    if our_white {
        cev >= slice_prior - robustness_floor
    } else {
        cev <= slice_prior + robustness_floor
    }
}

/// Python's max over a non-empty sequence: the first maximal element.
fn py_max(xs: impl IntoIterator<Item = f64>) -> f64 {
    let mut it = xs.into_iter();
    let mut best = it.next().expect("max() of an empty sequence");
    for x in it {
        if x > best {
            best = x;
        }
    }
    best
}

fn py_min(xs: impl IntoIterator<Item = f64>) -> f64 {
    let mut it = xs.into_iter();
    let mut best = it.next().expect("min() of an empty sequence");
    for x in it {
        if x < best {
            best = x;
        }
    }
    best
}

/// max(a, b) in Python: a unless b > a.
fn max2(a: f64, b: f64) -> f64 {
    if b > a { b } else { a }
}

fn min2(a: f64, b: f64) -> f64 {
    if b < a { b } else { a }
}

/// budget_core.rel_gate_keep: the keep-mask (NaN evals are exempt).
pub fn rel_gate_keep(evals: &[f64], own: f64, our_white: bool, floor: f64, margin: f64,
                     binding: Option<&[bool]>) -> Vec<bool> {
    let n = evals.len();
    if floor >= 1.0 || n == 0 {
        return vec![true; n];
    }
    let mut elig: Vec<f64> = evals.iter().copied().filter(|e| !e.is_nan()).collect();
    if elig.is_empty() {
        return vec![true; n];
    }
    if let Some(b) = binding {
        let bound: Vec<f64> = evals.iter().zip(b).filter(|(e, &k)| !e.is_nan() && k)
            .map(|(&e, _)| e).collect();
        if !bound.is_empty() || !own.is_nan() {
            elig = bound;
        }
    }
    let base_rv = if own.is_nan() {
        None
    } else if our_white {
        Some(own - margin)
    } else {
        Some(own + margin)
    };
    if base_rv.is_none() && n < 2 {
        return vec![true; n];
    }
    if our_white {
        let mut best = if elig.is_empty() { base_rv.unwrap() } else { py_max(elig) };
        if let Some(b) = base_rv {
            best = max2(best, b);
        }
        evals.iter().map(|&e| e.is_nan() || e >= best - floor).collect()
    } else {
        let mut best = if elig.is_empty() { base_rv.unwrap() } else { py_min(elig) };
        if let Some(b) = base_rv {
            best = min2(best, b);
        }
        evals.iter().map(|&e| e.is_nan() || e <= best + floor).collect()
    }
}

/// budget_core.eval_loss_keep. `cands` are (candidate, eval) in order; returns the kept
/// candidates in Python's order. `binding` None = the old rule; `dispute` is filled as
/// the Python dict would be updated.
pub fn eval_loss_keep(cands: &[Cand], own: f64, our_white: bool, max_loss: f64,
                      binding: Option<&[bool]>, mut dispute: Option<&mut Dispute>) -> Vec<Cand> {
    // scored: (position in cands, cp) for the evaluated ones
    let scored: Vec<(usize, f64)> = cands.iter().enumerate()
        .filter(|(_, c)| !c.eval.is_nan())
        .map(|(i, c)| (i, ws_to_cp(c.eval, our_white))).collect();
    if scored.is_empty() {
        return cands.to_vec();
    }
    let first_max = |s: &[(usize, f64)]| -> usize {
        let mut b = s[0];
        for &x in &s[1..] {
            if x.1 > b.1 {
                b = x;
            }
        }
        b.0
    };
    let Some(binding) = binding else {
        let mut best = py_max(scored.iter().map(|x| x.1));
        if !own.is_nan() {
            best = max2(best, ws_to_cp(own, our_white));
        }
        let keep: Vec<Cand> = scored.iter().filter(|x| x.1 >= best - max_loss)
            .map(|x| cands[x.0]).collect();
        return if keep.is_empty() { vec![cands[first_max(&scored)]] } else { keep };
    };
    let binds = |i: usize| binding[i];
    let mut refs: Vec<f64> = scored.iter().filter(|x| binds(x.0)).map(|x| x.1).collect();
    if !own.is_nan() {
        refs.push(ws_to_cp(own, our_white));
    }
    let best = if refs.is_empty() { py_max(scored.iter().map(|x| x.1)) } else { py_max(refs.iter().copied()) };
    let cut = max_loss + CP_EPS;
    let mut keep: Vec<(usize, f64)> = scored.iter().copied().filter(|x| x.1 >= best - cut).collect();
    if let Some(d) = dispute.as_deref_mut() {
        if !refs.is_empty() {
            let threats: Vec<(i64, f64)> = scored.iter()
                .filter(|x| !binds(x.0) && x.1 > best + CP_EPS)
                .map(|x| (cands[x.0].san, x.1)).collect();
            if !threats.is_empty() {
                let top = py_max(threats.iter().map(|t| t.1));
                let risk: Vec<i64> = scored.iter()
                    .filter(|x| best - cut <= x.1 && x.1 < top - cut)
                    .map(|x| cands[x.0].san).collect();
                d.reference_cp = Some(best);
                d.threats = threats;
                d.at_risk = risk;
            }
        }
    }
    let trusted: Vec<(usize, f64)> = scored.iter().copied().filter(|x| binds(x.0)).collect();
    if !trusted.is_empty() && !keep.iter().any(|x| binds(x.0)) {
        let fb = trusted[{
            let mut bi = 0;
            for (j, x) in trusted.iter().enumerate() {
                if x.1 > trusted[bi].1 {
                    bi = j;
                }
            }
            bi
        }];
        let rare: Vec<(usize, f64)> = if !keep.is_empty() {
            keep.clone()
        } else {
            scored.iter().copied().filter(|x| !binds(x.0) && x.1 > fb.1 + CP_EPS).collect()
        };
        if let Some(d) = dispute.as_deref_mut() {
            if !rare.is_empty() {
                d.reference_cp = Some(best);
                d.fallback = true;
                d.at_risk = vec![cands[fb.0].san];
                d.threats = rare.iter().map(|x| (cands[x.0].san, x.1)).collect();
            }
        }
        keep = rare;
        keep.push(fb);
    }
    if keep.is_empty() {
        vec![cands[first_max(&scored)]]
    } else {
        keep.iter().map(|x| cands[x.0]).collect()
    }
}

/// budget_core.gate_candidates. `edges` = the node's moves in order (engine proposals
/// included), `verified` = bare codes searched afresh at this node. Returns the kept
/// candidates and, when `want_dispute`, the node's dispute if Python would record one.
pub fn gate_candidates(edges: &[Cand], src: Option<i64>, own: f64, verified: &[i64],
                       p: &GateParams, want_dispute: bool) -> (Vec<Cand>, Option<Dispute>) {
    let binds = |c: &Cand| -> bool {
        let rf = p.rare_floor.unwrap();
        c.engine || c.total >= rf || verified.contains(&c.bare)
    };
    let mut cand: Vec<Cand> = Vec::with_capacity(edges.len());
    for c in edges {
        if Some(c.san) == src || !p.gates {
            cand.push(*c);
            continue;
        }
        if p.require_eval && c.eval.is_nan() {
            continue;
        }
        if !passes_abs_gate(c.eval, p.slice_prior, p.our_white, p.robustness_floor) {
            continue;
        }
        cand.push(*c);
    }
    let rare = p.rare_floor.is_some();
    if rare && !cand.is_empty() && own.is_nan()
        && !cand.iter().any(|c| !c.eval.is_nan() && binds(c)) {
        // nothing trusted to measure from: decide nothing on an unverified rare move's score
        let mut naive = cand.clone();
        if p.gates {
            let ev: Vec<f64> = naive.iter().map(|c| c.eval).collect();
            let keep = rel_gate_keep(&ev, f64::NAN, p.our_white, p.gate_rel_floor,
                                     p.gate_rel_own_margin, None);
            naive = naive.into_iter().zip(keep).filter(|(c, k)| *k || Some(c.san) == src)
                .map(|(c, _)| c).collect();
        }
        if let Some(ml) = p.max_eval_loss {
            naive = eval_loss_keep(&naive, f64::NAN, p.our_white, ml, None, None);
        }
        let stay = |c: &Cand| naive.iter().any(|n| n.r == c.r);
        let risk: Vec<i64> = cand.iter().filter(|c| !c.eval.is_nan() && !stay(c))
            .map(|c| c.san).collect();
        let mut dispute = None;
        if !risk.is_empty() && want_dispute {
            let mut threats: Vec<(i64, f64)> = naive.iter().filter(|c| !c.eval.is_nan())
                .map(|c| (c.san, ws_to_cp(c.eval, p.our_white))).collect();
            // sorted(key=(-cp, san)); codes sort as the SAN strings do
            threats.sort_by(|a, b| (-a.1).partial_cmp(&-b.1).unwrap().then(a.0.cmp(&b.0)));
            dispute = Some(Dispute { reference_cp: None, unresolved: true, at_risk: risk,
                                     threats, fallback: false });
        }
        return (cand, dispute);
    }
    if p.gates && !cand.is_empty() {
        let ev: Vec<f64> = cand.iter().map(|c| c.eval).collect();
        let bl: Option<Vec<bool>> = if rare { Some(cand.iter().map(binds).collect()) } else { None };
        let keep = rel_gate_keep(&ev, own, p.our_white, p.gate_rel_floor, p.gate_rel_own_margin,
                                 bl.as_deref());
        cand = cand.into_iter().zip(keep).filter(|(c, k)| *k || Some(c.san) == src)
            .map(|(c, _)| c).collect();
    }
    let mut dispute = None;
    if let Some(ml) = p.max_eval_loss {
        if !cand.is_empty() {
            let mut d = Dispute::default();
            let track = rare && want_dispute;
            let bl: Option<Vec<bool>> = if rare { Some(cand.iter().map(binds).collect()) } else { None };
            cand = eval_loss_keep(&cand, own, p.our_white, ml, bl.as_deref(),
                                  if track { Some(&mut d) } else { None });
            // `if d:` -- a non-empty dict; every update sets reference_cp
            if track && d.reference_cp.is_some() {
                dispute = Some(d);
            }
        }
    }
    (cand, dispute)
}

/// One our-node's inputs to final_candidates.
pub struct NodeIn<'a> {
    pub own: f64,
    pub src: Option<i64>,      // the source book's move (SAN code)
    pub src_bare: i64,
    pub src_eval: f64,         // the restored source move's child eval (NaN = None)
    pub recorded: &'a [Cand],  // the recorded edges, in order (Ref::Edge)
    pub engine: &'a [Cand],    // engine proposals, in order (Ref::Engine)
    pub verified: &'a [i64],
    pub veto: &'a [i64],
}

pub struct NodeOut {
    pub kept: Vec<Ref>,
    pub restored: bool,
    pub n_rej: i64,
    pub n_eng: i64,
    pub dispute: Option<Dispute>,
}

/// budget_core.final_candidates (max_cands 0, the recipes' setting). The restored source
/// move's child is resolved by the caller (python-chess from the EPD); only its eval
/// enters here.
pub fn final_candidates(n: &NodeIn, p: &GateParams, want_dispute: bool) -> NodeOut {
    let mut src = n.src;
    let es: Vec<Cand> = if n.veto.is_empty() {
        n.recorded.to_vec()
    } else {
        if src.is_some() && n.veto.contains(&n.src_bare) {
            src = None;
        }
        n.recorded.iter().copied().filter(|c| !n.veto.contains(&c.bare)).collect()
    };
    let mut es_g = es.clone();
    let mut n_eng = 0;
    if !n.engine.is_empty() {
        let have: Vec<i64> = es.iter().map(|c| c.bare).collect();
        let extra: Vec<Cand> = n.engine.iter().copied()
            .filter(|c| !have.contains(&c.bare) && !n.veto.contains(&c.bare)).collect();
        if !extra.is_empty() {
            n_eng = extra.len() as i64;
            es_g.extend(extra);
        }
    }
    let (mut cand, dispute) = gate_candidates(&es_g, src, n.own, n.verified, p, want_dispute);
    let out = |cand: Vec<Cand>, restored: bool, n_rej: i64, dispute: Option<Dispute>| NodeOut {
        kept: cand.iter().map(|c| c.r).collect(), restored, n_rej, n_eng, dispute,
    };
    let Some(s) = src else {
        return out(cand, false, 0, dispute);
    };
    let seen_src = cand.iter().any(|c| c.san == s);
    let recorded_src = es.iter().any(|c| c.san == s);
    if !seen_src && p.strict_source && recorded_src {
        return out(cand, false, 1, dispute);
    }
    if !seen_src {
        if p.strict_source {
            if let Some(ml) = p.max_eval_loss {
                if n.src_eval.is_nan() {
                    return out(cand, false, 1, dispute);
                }
                let mut pairs = cand.clone();
                // ({"move_san": src}, cev): no total, so e.get("total", rare_floor) binds
                pairs.push(Cand { r: Ref::Edge(usize::MAX), san: s, bare: n.src_bare,
                                  eval: n.src_eval, total: i64::MAX, engine: false });
                let bl: Option<Vec<bool>> = p.rare_floor.map(|rf| pairs.iter()
                    .map(|c| c.engine || c.total >= rf || n.verified.contains(&c.bare)).collect());
                let kept = eval_loss_keep(&pairs, n.own, p.our_white, ml, bl.as_deref(), None);
                if !kept.iter().any(|c| c.san == s) {
                    return out(cand, false, 1, dispute);
                }
                cand.retain(|c| kept.iter().any(|k| k.san == c.san));
            }
        }
        return out(cand, true, 0, dispute);
    }
    out(cand, false, 0, dispute)
}
