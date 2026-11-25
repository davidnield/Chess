use super::*;
use crate::{dp, price};
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::time::Instant;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct CapStats {
    pub calls: u64,
    pub binding: u64,
    pub merged_packages: u64,
    pub largest_input: usize,
    pub root_budget_values: Vec<f64>,
}
thread_local! {static PROFILE:RefCell<Option<CapStats>>=const {RefCell::new(None)};}
pub(crate) fn record_cap(n: usize, k: usize) {
    PROFILE.with(|p| {
        if let Some(s) = p.borrow_mut().as_mut() {
            s.calls += 1;
            s.largest_input = s.largest_input.max(n);
            if n > k {
                s.binding += 1;
                s.merged_packages += (n - k) as u64;
            }
        }
    });
}
pub fn cap_profile<T>(f: impl FnOnce() -> T) -> (T, CapStats) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            PROFILE.with(|p| *p.borrow_mut() = None);
        }
    }
    PROFILE.with(|p| {
        assert!(p.borrow().is_none(), "nested cap profiler");
        *p.borrow_mut() = Some(CapStats::default());
    });
    let _guard = Reset;
    let x = f();
    let s = PROFILE.with(|p| p.borrow_mut().take().unwrap());
    (x, s)
}

pub fn polish_params() -> polish::Params {
    polish::Params {
        line_k: 8,
        max_verify: 24,
        max_steps: 100_000,
        fill_width: 256,
        max_exchange: 16,
        verify_after_hit: 4,
    }
}

/// Rust API for arbitrary valid starts. Counts/values always recomputed, never trusted.
pub fn polish_starts(v: &View<'_>, starts: &[Book], budget: usize) -> Result<Outcome> {
    let begin = Instant::now();
    let mut seen = HashSet::new();
    let mut best = v.evaluate(&Book::new(), budget)?;
    let mut count = 0;
    for book in starts {
        let seed = v.evaluate(book, usize::MAX)?;
        let key: Vec<_> = seed.book.iter().map(|(&u, &s)| (u, s)).collect();
        if !seen.insert(key) {
            continue;
        }
        if seed.book.len() <= budget && seed.value > best.value {
            best = seed.clone();
        }
        let moves = seed.book.iter().map(|(&u, &s)| (u, s)).collect();
        let (book, _) = polish::polish_book(v.g, &moves, budget, &v.order, &polish_params());
        let candidate = v.evaluate(&book.into_iter().collect(), budget)?;
        if seed.book.len() <= budget && candidate.value + 1e-10 < seed.value {
            return Err("polish lowered a feasible start".into());
        }
        if candidate.value > best.value + 1e-12 {
            best = candidate;
        }
        count += 1;
    }
    best.status = "portfolio-feasible".into();
    best.states = count;
    best.seconds = begin.elapsed().as_secs_f64();
    Ok(best)
}
pub fn dp_baseline(v: &View<'_>, budget: usize, cap: usize) -> Result<(Outcome, CapStats)> {
    if cap == 0 || budget == 0 || budget > i64::MAX as usize / 2 {
        return Err("positive bounded budget/cap required".into());
    }
    let begin = Instant::now();
    let bmax = (budget as f64 * 1.5).ceil() as i64;
    let ((curves, probe), mut stats) = cap_profile(|| {
        (
            dp::build_curves(v.g, budget as i64, cap).0,
            dp::build_curves(v.g, bmax, cap).0,
        )
    });
    stats.root_budget_values = (0..=budget)
        .map(|b| curves[v.g.root_id() as usize].eval(b as i64))
        .collect();
    let (book, _, _, _) = dp::match_distinct(
        v.g,
        &curves,
        budget as i64,
        bmax,
        Some(&probe),
        64,
        Some(budget as i64),
    );
    let seed = book
        .booked
        .into_iter()
        .map(|(u, j)| (u, v.g.c_san[j]))
        .collect();
    let mut out = polish_starts(v, &[seed], budget)?;
    out.status = "dp-match-polish-feasible".into();
    out.seconds = begin.elapsed().as_secs_f64();
    Ok((out, stats))
}
pub fn price_baseline(v: &View<'_>, budget: usize) -> Result<Outcome> {
    price_with_source(v, budget, &Book::new())
}
pub fn price_with_source(v: &View<'_>, budget: usize, source: &Book) -> Result<Outcome> {
    let begin = Instant::now();
    for (&u, &s) in source {
        v.candidate_index(u, s)?;
    }
    let source: HashMap<_, _> = source.iter().map(|(&u, &s)| (u, s)).collect();
    let rr = price::policy_reach(v.g, &source, &v.order);
    let reach: Vec<_> =
        v.g.node_reach
            .iter()
            .zip(&rr.val)
            .map(|(&a, &b)| a.max(b))
            .collect();
    let raw = price::price_search(v.g, budget, 40, &reach, 0.01, true, &polish_params());
    let mut out = v.evaluate(&raw.region.booked.into_iter().collect(), budget)?;
    out.status = "price-polish-feasible".into();
    out.seconds = begin.elapsed().as_secs_f64();
    Ok(out)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeighborhoodConfig {
    pub width: usize,
    pub rounds: usize,
    pub discovery_nodes: usize,
    pub limits: exact::Limits,
}
impl Default for NeighborhoodConfig {
    fn default() -> Self {
        Self {
            width: 12,
            rounds: 8,
            discovery_nodes: 2000,
            limits: exact::Limits::default(),
        }
    }
}

/// Deterministic seed paths cover ALL admitted candidates, including unbooked moves.
/// Discovery has its own explicit budget; this is heuristic, not a pruning certificate.
pub fn neighborhoods(v: &View<'_>, cfg: &NeighborhoodConfig) -> Result<Vec<HashSet<u32>>> {
    if cfg.width == 0 || cfg.rounds == 0 || cfg.discovery_nodes == 0 {
        return Err("positive neighborhood limits required".into());
    }
    cfg.limits.validate()?;
    let root = v.g.root_id();
    let mut paths: HashMap<u32, Option<u32>> = HashMap::from([(root, None)]);
    let mut queue = VecDeque::from([root]);
    let mut seeds = Vec::new();
    while let Some(u) = queue.pop_front() {
        if v.g.is_our(u) {
            seeds.push(u);
        }
        let cs: Vec<_> = if v.g.is_our(u) {
            v.g.cands(u as usize).map(|j| v.candidate(u, j)).collect()
        } else {
            v.g.kids(u as usize).map(|j| v.reply(u, j)).collect()
        };
        for c in cs {
            if let Child::Node(c) = c {
                if !paths.contains_key(&c) && paths.len() < cfg.discovery_nodes {
                    paths.insert(c, Some(u));
                    queue.push_back(c);
                }
            }
        }
    }
    let mut result = Vec::new();
    for round in 0..cfg.rounds {
        if seeds.is_empty() {
            break;
        }
        // Spread seeds across the discovery order instead of only the first opening.
        let seed = seeds[round * seeds.len() / cfg.rounds];
        let mut set = HashSet::new();
        let mut u = Some(seed);
        while let Some(x) = u {
            if v.g.is_our(x) {
                set.insert(x);
            }
            u = paths[&x];
        }
        if set.len() > cfg.width {
            continue;
        } // never silently omit the connecting prefix
        let mut queue = VecDeque::from([seed]);
        let mut visited = HashSet::new();
        while let Some(u) = queue.pop_front() {
            if visited.len() >= cfg.discovery_nodes {
                break;
            }
            if !visited.insert(u) {
                continue;
            }
            if v.g.is_our(u) && !set.contains(&u) {
                if set.len() == cfg.width {
                    continue;
                }
                set.insert(u);
            }
            let cs: Vec<_> = if v.g.is_our(u) {
                v.g.cands(u as usize).map(|j| v.candidate(u, j)).collect()
            } else {
                v.g.kids(u as usize).map(|j| v.reply(u, j)).collect()
            };
            for c in cs {
                if let Child::Node(c) = c {
                    queue.push_back(c);
                }
            }
        }
        if !result.contains(&set) {
            result.push(set);
        }
    }
    Ok(result)
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NeighborhoodStep {
    pub index: usize,
    pub unlocked: usize,
    pub outcome: Outcome,
    pub accepted: bool,
}
pub fn improve(
    v: &View<'_>,
    budget: usize,
    start: &Book,
    cfg: &NeighborhoodConfig,
    checkpoint: impl FnMut(&NeighborhoodStep) -> Result<()>,
) -> Result<Outcome> {
    improve_from(v, budget, start, cfg, 0, checkpoint)
}
pub fn improve_from(
    v: &View<'_>,
    budget: usize,
    start: &Book,
    cfg: &NeighborhoodConfig,
    next: usize,
    mut checkpoint: impl FnMut(&NeighborhoodStep) -> Result<()>,
) -> Result<Outcome> {
    let begin = Instant::now();
    let mut best = v.evaluate(start, budget)?;
    let mut states = 0;
    for (index, free) in neighborhoods(v, cfg)?.iter().enumerate().skip(next) {
        let candidate = match exact::mip(v, budget, &best.book, Some(free), &cfg.limits) {
            Ok(c) => c,
            Err(e) if e.contains("resource limit") => {
                let mut c = best.clone();
                c.status = e;
                c.bound = None;
                c.bound_scope = None;
                c.states = 0;
                c.seconds = 0.;
                c
            }
            Err(e) => return Err(e),
        };
        let accepted = candidate.value > best.value + 1e-10;
        states += candidate.states;
        if accepted {
            best = candidate.clone();
        }
        checkpoint(&NeighborhoodStep {
            index,
            unlocked: free.len(),
            outcome: candidate,
            accepted,
        })?;
    }
    best.status = "neighborhood-feasible".into();
    best.bound = None;
    best.bound_scope = None;
    best.states = states;
    best.seconds = begin.elapsed().as_secs_f64();
    Ok(best)
}
