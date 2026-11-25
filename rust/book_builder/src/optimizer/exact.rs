//! Independent exhaustive oracle, exact tree allocation, and a bounded native MIP.
use super::*;
use microlp::{
    ComparisonOp as Op, OptimizationDirection, Problem, SolveOptions, SolveOutcome, Variable,
};
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub milliseconds: u64,
    pub states: u64,
    pub max_nodes: usize,
    pub max_variables: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            milliseconds: 2000,
            states: 10000,
            max_nodes: 2000,
            max_variables: 20000,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        if self.milliseconds == 0
            || self.states == 0
            || self.max_nodes == 0
            || self.max_variables == 0
        {
            Err("explicit positive solver limits required".into())
        } else {
            Ok(())
        }
    }
}

pub fn enumerate(v: &View<'_>, budget: usize, limits: &Limits) -> Result<Outcome> {
    limits.validate()?;
    if v.g.n() > limits.max_nodes || v.g.n_our > 24 {
        return Err("exhaustive oracle size limit".into());
    }
    let begin = Instant::now();
    let mut states = 0;
    let mut finished = true;
    let mut best = v.evaluate(&Book::new(), budget)?;
    fn visit(
        v: &View<'_>,
        u: usize,
        b: usize,
        m: &mut Book,
        best: &mut Outcome,
        count: &mut u64,
        complete: &mut bool,
        t: Instant,
        lim: &Limits,
    ) {
        if *count >= lim.states || t.elapsed() >= Duration::from_millis(lim.milliseconds) {
            *complete = false;
            return;
        }
        if u == v.g.n_our {
            *count += 1;
            let val = v.independent_values(m)[v.g.root_id() as usize];
            if val > best.value + 1e-12 {
                best.value = val;
                best.book = m.clone();
            }
            return;
        }
        visit(v, u + 1, b, m, best, count, complete, t, lim);
        if !*complete || m.len() >= b {
            return;
        }
        for j in v.g.cands(u) {
            m.insert(u as u32, v.g.c_san[j]);
            visit(v, u + 1, b, m, best, count, complete, t, lim);
            m.remove(&(u as u32));
            if !*complete {
                break;
            }
        }
    }
    visit(
        v,
        0,
        budget,
        &mut Book::new(),
        &mut best,
        &mut states,
        &mut finished,
        begin,
        limits,
    );
    let mut out = v.evaluate(&best.book, budget)?;
    if (out.value - best.value).abs() > 1e-10 {
        return Err("oracle/production evaluator disagreement".into());
    }
    out.status = if finished {
        "oracle-optimal"
    } else {
        "oracle-limited"
    }
    .into();
    out.bound = Some(if finished {
        out.value
    } else {
        v.unbudgeted_bound()
    });
    out.states = states;
    out.bound_scope = Some("whole admitted graph under fixed topological cuts".into());
    out.seconds = begin.elapsed().as_secs_f64();
    Ok(out)
}

/// Full integer-budget max-plus convolution, without hull fusion or greedy packages.
/// Refuse transpositions/cycles; callers must not label tree DP exact on a DAG.
pub fn tree_values(v: &View<'_>, budget: usize, max_cells: usize) -> Result<Vec<f64>> {
    if v.cuts != 0 {
        return Err("exact tree DP refuses cycles".into());
    }
    if v.g
        .n()
        .checked_mul(budget + 1)
        .is_none_or(|x| x > max_cells)
    {
        return Err("tree table resource limit".into());
    }
    if (v.g.k_p.len() as u128) * (budget as u128 + 1) * (budget as u128 + 2) / 2 > 50_000_000 {
        return Err("tree convolution operation limit".into());
    }
    let mut incoming = vec![0; v.g.n()];
    for u in 0..v.g.n() as u32 {
        let children: Vec<_> = if v.g.is_our(u) {
            v.g.cands(u as usize).map(|j| v.candidate(u, j)).collect()
        } else {
            v.g.kids(u as usize).map(|j| v.reply(u, j)).collect()
        };
        for c in children {
            if let Child::Node(c) = c {
                incoming[c as usize] += 1;
            }
        }
    }
    if incoming.iter().any(|&n| n > 1) {
        return Err("exact tree DP refuses shared descendants".into());
    }
    let mut tab = vec![vec![0.; budget + 1]; v.g.n()];
    for &u in &v.order {
        let mut row = if v.g.is_our(u) {
            vec![v.g.l_node[u as usize]; budget + 1]
        } else {
            vec![v.g.const_base[u as usize - v.g.n_our]; budget + 1]
        };
        if v.g.is_our(u) {
            for j in v.g.cands(u as usize) {
                for b in 1..=budget {
                    let x = match v.candidate(u, j) {
                        Child::Node(c) => tab[c as usize][b - 1],
                        Child::Terminal(x) => x,
                    };
                    row[b] = row[b].max(x);
                }
            }
        } else {
            for j in v.g.kids(u as usize) {
                let child = match v.reply(u, j) {
                    Child::Node(c) => tab[c as usize].clone(),
                    Child::Terminal(x) => vec![x; budget + 1],
                };
                let mut next = vec![f64::NEG_INFINITY; budget + 1];
                for b in 0..=budget {
                    for k in 0..=b {
                        next[b] = next[b].max(row[b - k] + v.g.k_p[j] * child[k]);
                    }
                }
                row = next;
            }
        }
        tab[u as usize] = row;
    }
    Ok(tab.swap_remove(v.g.root_id() as usize))
}

fn constraint(p: &mut Problem, terms: Vec<(Variable, f64)>, op: Op, rhs: f64) {
    // Duplicate reply edges may reference the same column. microlp requires unique columns.
    let mut merged: BTreeMap<usize, (Variable, f64)> = BTreeMap::new();
    for (var, c) in terms {
        merged.entry(var.idx()).or_insert((var, 0.)).1 += c;
    }
    p.add_constraint(
        merged
            .into_values()
            .filter(|(_, c)| *c != 0.)
            .collect::<Vec<_>>(),
        op,
        rhs,
    );
}

/// Exact model of the admitted, fixed-cycle-cut graph. With unlocked=Some, choices
/// outside the neighborhood are fixed WHEN REACHED. Boolean reachability lets unused
/// fixed branches release budget, while shared descendants are charged only once.
pub fn mip(
    v: &View<'_>,
    budget: usize,
    base: &Book,
    unlocked: Option<&HashSet<u32>>,
    lim: &Limits,
) -> Result<Outcome> {
    lim.validate()?;
    let begin = Instant::now();
    let incumbent = v.evaluate(base, budget)?;
    let choices = |u: u32| -> Vec<usize> {
        v.g.cands(u as usize)
            .filter(|&j| {
                unlocked.is_none_or(|s| s.contains(&u)) || base.get(&u) == Some(&v.g.c_san[j])
            })
            .collect()
    };
    let mut active = HashSet::new();
    let mut q = VecDeque::from([v.g.root_id()]);
    while let Some(u) = q.pop_front() {
        if !active.insert(u) {
            continue;
        }
        if active.len() > lim.max_nodes {
            return Err("MIP active-node resource limit".into());
        }
        let children: Vec<_> = if v.g.is_our(u) {
            choices(u).into_iter().map(|j| v.candidate(u, j)).collect()
        } else {
            v.g.kids(u as usize).map(|j| v.reply(u, j)).collect()
        };
        for c in children {
            if let Child::Node(c) = c {
                q.push_back(c);
            }
        }
    }
    let order: Vec<_> = v
        .order
        .iter()
        .copied()
        .filter(|u| active.contains(u))
        .collect();
    let count = 2 * order.len()
        + order
            .iter()
            .filter(|&&u| v.g.is_our(u))
            .map(|&u| 2 * choices(u).len())
            .sum::<usize>();
    if count > lim.max_variables {
        return Err("MIP variable resource limit".into());
    }
    // Budget-relaxed backward values are valid node upper bounds. In particular,
    // avoid giving a long forced chain the artificial LP ceiling of 1.0.
    let mut ceilings = HashMap::new();
    for &u in &order {
        let child = |c| match c {
            Child::Node(c) => ceilings[&c],
            Child::Terminal(x) => x,
        };
        let ceiling = if v.g.is_our(u) {
            choices(u)
                .into_iter()
                .map(|j| child(v.candidate(u, j)))
                .fold(v.g.l_node[u as usize], f64::max)
        } else {
            v.g.const_base[u as usize - v.g.n_our]
                + v.g
                    .kids(u as usize)
                    .map(|j| v.g.k_p[j] * child(v.reply(u, j)))
                    .sum::<f64>()
        };
        ceilings.insert(u, ceiling.min(1.));
    }
    let mut p = Problem::new(OptimizationDirection::Maximize);
    let values: HashMap<_, _> = order
        .iter()
        .map(|&u| {
            (
                u,
                p.add_var(if u == v.g.root_id() { 1. } else { 0. }, (0., ceilings[&u])),
            )
        })
        .collect();
    let reach: HashMap<_, _> = order
        .iter()
        .map(|&u| (u, p.add_var(0., (0., 1.))))
        .collect();
    let mut incoming: HashMap<u32, Vec<(Variable, f64)>> = HashMap::new();
    let mut selected = Vec::new();
    let mut hint = Vec::new();
    for &u in &order {
        if v.g.is_our(u) {
            let mut ys = Vec::new();
            for j in choices(u) {
                let y = p.add_integer_var(0., (0, 1));
                ys.push((y, 1.));
                selected.push((u, v.g.c_san[j], y));
                hint.push((
                    y,
                    if base.get(&u) == Some(&v.g.c_san[j]) {
                        1.
                    } else {
                        0.
                    },
                ));
                // y=1 => value[u] = candidate value, with tight M=1 on [0,1].
                let (mut diff, rhs) = match v.candidate(u, j) {
                    Child::Node(c) => (vec![(values[&u], 1.), (values[&c], -1.)], 0.),
                    Child::Terminal(x) => (vec![(values[&u], 1.)], x),
                };
                let mut lower = diff.clone();
                lower.push((y, -1.));
                diff.push((y, 1.));
                constraint(&mut p, diff, Op::Le, rhs + 1.);
                constraint(&mut p, lower, Op::Ge, rhs - 1.);
                if let Child::Node(c) = v.candidate(u, j) {
                    let f = p.add_var(0., (0., 1.));
                    constraint(&mut p, vec![(f, 1.), (reach[&u], -1.)], Op::Le, 0.);
                    constraint(&mut p, vec![(f, 1.), (y, -1.)], Op::Le, 0.);
                    constraint(
                        &mut p,
                        vec![(f, 1.), (reach[&u], -1.), (y, -1.)],
                        Op::Ge,
                        -1.,
                    );
                    incoming.entry(c).or_default().push((f, 1.));
                }
            }
            constraint(&mut p, ys.clone(), Op::Le, 1.);
            // No selected move => stop value.
            let mut upper = vec![(values[&u], 1.)];
            upper.extend(ys.iter().map(|&(y, _)| (y, -1.)));
            let mut lower = vec![(values[&u], 1.)];
            lower.extend(ys.iter().copied());
            constraint(&mut p, upper, Op::Le, v.g.l_node[u as usize]);
            constraint(&mut p, lower, Op::Ge, v.g.l_node[u as usize]);
            if unlocked.is_some_and(|s| !s.contains(&u)) && base.contains_key(&u) {
                let mut terms = vec![(reach[&u], 1.)];
                terms.extend(ys.iter().map(|&(y, _)| (y, -1.)));
                constraint(&mut p, terms, Op::Le, 0.);
            }
        } else {
            let mut terms = vec![(values[&u], 1.)];
            let mut rhs = v.g.const_base[u as usize - v.g.n_our];
            for j in v.g.kids(u as usize) {
                let w = v.g.k_p[j];
                match v.reply(u, j) {
                    Child::Node(c) => {
                        terms.push((values[&c], -w));
                        // Reachability, not probability mass: even a very rare
                        // positive-probability branch must preserve a fixed move.
                        // The value equation above still uses its actual weight.
                        if w > 0. {
                            incoming.entry(c).or_default().push((reach[&u], 1.));
                        }
                    }
                    Child::Terminal(x) => rhs += w * x,
                }
            }
            constraint(&mut p, terms, Op::Eq, rhs);
        }
    }
    for &u in &order {
        let arrivals = incoming.remove(&u).unwrap_or_default();
        if u == v.g.root_id() {
            constraint(&mut p, vec![(reach[&u], 1.)], Op::Eq, 1.);
            continue;
        }
        // OR of incoming active edges. Once move binaries are integral, these
        // continuous [0,1] indicators are exactly 0/1, without more integer vars.
        for &(x, _) in &arrivals {
            constraint(&mut p, vec![(reach[&u], 1.), (x, -1.)], Op::Ge, 0.);
        }
        let mut terms = vec![(reach[&u], 1.)];
        terms.extend(arrivals.into_iter().map(|(x, c)| (x, -c)));
        constraint(&mut p, terms, Op::Le, 0.);
    }
    constraint(
        &mut p,
        selected.iter().map(|&(_, _, y)| (y, 1.)).collect(),
        Op::Le,
        budget as f64,
    );
    let mut opts = SolveOptions::default();
    opts.time_limit = Some(Duration::from_millis(lim.milliseconds));
    opts.node_limit = Some(lim.states);
    opts.int_tol = 1e-9;
    opts.tolerances.feasibility = 1e-9;
    opts.tolerances.prune_epsilon = 1e-10;
    opts.warm_start = Some(hint);
    let solved = p.solve_with(opts).map_err(|e| format!("native MIP: {e}"))?;
    let stats = solved.stats();
    let reason = format!("{:?}", solved.termination_reason());
    let mut out = match solved {
        SolveOutcome::Solution(sol) => {
            let book = selected
                .iter()
                .filter(|&&(_, _, y)| sol.var_value(y) > 0.5)
                .map(|&(u, s, _)| (u, s))
                .collect();
            let candidate = v.evaluate(&book, budget)?;
            if (candidate.value - sol.objective()).abs() > 1e-7 {
                return Err("MIP rounded policy/objective mismatch".into());
            }
            if let Some(free) = unlocked {
                let moves = candidate.book.iter().map(|(&u, &s)| (u, s)).collect();
                let region = polish::book_region(v.g, &moves, &v.pos, &v.stat);
                for &u in &region.w_keys {
                    if v.g.is_our(u)
                        && !free.contains(&u)
                        && region.w(u) > 0.
                        && candidate.book.get(&u) != base.get(&u)
                    {
                        return Err(
                            "MIP rounded policy changed a reached fixed prescription".into()
                        );
                    }
                }
            }
            if candidate.value + 1e-10 < incumbent.value {
                incumbent
            } else {
                candidate
            }
        }
        SolveOutcome::Interrupted(_) => incumbent,
    };
    out.status = format!("mip-{reason}");
    out.states = stats.nodes_solved;
    out.bound = stats.best_bound.filter(|x| x.is_finite());
    out.bound_scope = out.bound.map(|_| {
        if unlocked.is_some() {
            "local neighborhood; outside prescriptions fixed when reached".into()
        } else {
            "whole admitted graph under fixed topological cuts".into()
        }
    });
    if out.bound.is_some_and(|b| b + 1e-7 < out.value) {
        return Err("MIP bound below feasible incumbent".into());
    }
    out.seconds = begin.elapsed().as_secs_f64();
    Ok(out)
}
