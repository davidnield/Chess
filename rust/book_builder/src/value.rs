//! Valuation kernels: the leaf and aux rules build_graph applies to every edge and node.
//!
//! Each function is the Python one it names, operation for operation, so the floats are
//! identical (held at tolerance 0 by python/_test_book_builder_rs.py). Rules that keep
//! that true:
//! - the same IEEE operations in the same order, never an algebraic rearrangement;
//! - Python's `sum()` on 3.11 adds left to right from integer 0 (0 + x == x), so a
//!   running `acc += x` from 0.0 matches it; 3.12 switched float `sum()` to compensated
//!   summation, which would break this contract (the venv pins 3.11);
//! - a Python `None` eval is NaN here (an eval is never NaN);
//! - counts are converted to f64 where Python mixes int and float (exact below 2^53).

pub const LICHESS_CP_SCALE: f64 = 0.00368208;

/// stage3_backwards_induction.cp_to_expected_score: 1 / (1 + exp(-scale * cp)).
#[inline]
pub fn cp_to_expected_score(cp: f64) -> f64 {
    1.0 / (1.0 + (-LICHESS_CP_SCALE * cp).exp())
}

/// budget_core.ws_to_cp: the inverse, from our side's view (centipawns).
#[inline]
pub fn ws_to_cp(ws: f64, our_white: bool) -> f64 {
    let ws = ws.max(1e-9).min(1.0 - 1e-9);
    let cp = (ws / (1.0 - ws)).ln() / LICHESS_CP_SCALE;
    if our_white { cp } else { -cp }
}

/// stage3.smoothed_score: (k * prior + empirical * n) / (k + n).
#[inline]
pub fn smoothed_score(empirical: f64, n: f64, prior: f64, k: f64) -> f64 {
    (k * prior + empirical * n) / (k + n)
}

/// stage3.effective_eval_weight.
#[inline]
pub fn effective_eval_weight(ew_max: f64, ew_min: f64, k: f64, n: f64) -> f64 {
    if ew_max <= 0.0 || k <= 0.0 || ew_min >= ew_max {
        return ew_max;
    }
    ew_min + (ew_max - ew_min) * k / (k + n)
}

/// The blend parameters every leaf value reads.
#[derive(Clone, Copy, Debug)]
pub struct Blend {
    pub slice_prior: f64,
    pub our_white: bool,
    pub prior_strength: f64,
    pub eval_weight: f64,
    pub eval_weight_min: f64,
    pub eval_weight_k: f64,
}

/// budget_core.leaf_edge_value: Stage 3's out-of-book child blend, our perspective.
/// ws_avg and eval_ws are White's view; eval_ws NaN = no eval.
#[inline]
pub fn leaf_edge_value(ws_avg: f64, total: f64, eval_ws: f64, b: &Blend) -> f64 {
    let emp = smoothed_score(ws_avg, total, b.slice_prior, b.prior_strength);
    let v = if !eval_ws.is_nan() && b.eval_weight > 0.0 {
        let ew = effective_eval_weight(b.eval_weight, b.eval_weight_min, b.eval_weight_k, total);
        (1.0 - ew) * emp + ew * eval_ws
    } else {
        emp
    };
    if b.our_white { v } else { 1.0 - v }
}

/// budget_core.leaf_node_value: the stop value of an our-turn node from its edges'
/// (white_score_avg, total). Totals are integers in Python, summed exactly.
pub fn leaf_node_value(ws: &[f64], totals: &[i64], own_eval_ws: f64, b: &Blend) -> f64 {
    let tot: i64 = totals.iter().sum();
    let w = if tot != 0 {
        let mut num = 0.0f64;
        for (x, &t) in ws.iter().zip(totals) {
            num += x * (t as f64);
        }
        num / (tot as f64)
    } else {
        b.slice_prior
    };
    leaf_edge_value(w, tot as f64, own_eval_ws, b)
}

/// One aux sidecar row as aux_row_buckets returns it (White-score units).
#[derive(Clone, Copy, Debug, Default)]
pub struct AuxBuckets {
    pub term_tot: f64,
    pub term_sum: f64,
    pub crack: f64,
    pub oth_tot: f64,
    pub oth_sum: f64,
    pub oth_eval: f64, // NaN = no eval-covered child
    pub oth_cov: f64,
    pub hor_tot: f64,
    pub hor_sum: f64,
}

/// The raw columns aux_row_buckets reads, each already `float(x or 0.0)`; other_eval_mean
/// NaN where it was None. Groups are term_normal, term_other, term_flag (in that order).
#[derive(Clone, Copy, Debug, Default)]
pub struct AuxRow {
    pub term_total: [f64; 3],
    pub term_white_wins: [f64; 3],
    pub term_black_wins: [f64; 3],
    pub term_draws: [f64; 3],
    pub other_total: f64,
    pub other_white_wins: f64,
    pub other_draws: f64,
    pub other_eval_mean: f64,
    pub other_eval_cov: f64,
    pub horizon_total: f64,
    pub horizon_white_wins: f64,
    pub horizon_draws: f64,
}

/// budget_core.aux_row_buckets (term_flags=True). `None` row = all zeros, NaN eval.
pub fn aux_row_buckets(aux: Option<&AuxRow>, our_white: bool) -> AuxBuckets {
    let Some(a) = aux else {
        return AuxBuckets { oth_eval: f64::NAN, ..Default::default() };
    };
    // Python: sum(generator) starts from int 0; 0 + x == x for every float x.
    let mut term_tot = 0.0f64;
    let mut term_sum = 0.0f64;
    let mut crack = 0.0f64;
    for i in 0..3 {
        term_tot += a.term_total[i];
    }
    for i in 0..3 {
        term_sum += a.term_white_wins[i] + 0.5 * a.term_draws[i];
    }
    for i in 0..3 {
        crack += if our_white { a.term_white_wins[i] } else { a.term_black_wins[i] };
    }
    AuxBuckets {
        term_tot,
        term_sum,
        crack,
        oth_tot: a.other_total,
        oth_sum: a.other_white_wins + 0.5 * a.other_draws,
        oth_eval: a.other_eval_mean,
        oth_cov: a.other_eval_cov,
        hor_tot: a.horizon_total,
        hor_sum: a.horizon_white_wins + 0.5 * a.horizon_draws,
    }
}

/// stage3.aux_opp_mix with horizon_mode 'empirical' (the only mode the builder uses):
/// the White-score (numerator, denominator) the aux buckets add to an opponent node.
pub fn aux_opp_mix(x: &AuxBuckets, eval_weight: f64, eval_weight_min: f64, eval_weight_k: f64,
                   cov_blend: bool) -> (f64, f64) {
    let mut num = 0.0f64;
    let mut den = 0.0f64;
    if x.term_tot != 0.0 {
        num += x.term_sum;
        den += x.term_tot;
    }
    if x.oth_tot != 0.0 {
        let emp_o = x.oth_sum / x.oth_tot;
        let has_eval = !x.oth_eval.is_nan();
        let v_o = if has_eval && eval_weight > 0.0 && cov_blend {
            let w_o = effective_eval_weight(eval_weight, eval_weight_min, eval_weight_k, x.oth_tot);
            emp_o + x.oth_cov * w_o * (x.oth_eval - emp_o)
        } else if has_eval && eval_weight > 0.0 {
            let n_eff = x.oth_tot * x.oth_cov;
            let w_o = effective_eval_weight(eval_weight, eval_weight_min, eval_weight_k, n_eff);
            (1.0 - w_o) * emp_o + w_o * x.oth_eval
        } else {
            emp_o
        };
        num += v_o * x.oth_tot;
        den += x.oth_tot;
    }
    if x.hor_tot != 0.0 {
        let v_h = x.hor_sum / x.hor_tot;
        num += v_h * x.hor_tot;
        den += x.hor_tot;
    }
    (num, den)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cp_round_trip() {
        for cp in [-2000.0, -300.0, 0.0, 1.0, 300.0, 2000.0] {
            let ws = cp_to_expected_score(cp);
            assert!((ws_to_cp(ws, true) - cp).abs() < 1e-6);
            assert!((ws_to_cp(ws, false) + cp).abs() < 1e-6);
        }
        assert_eq!(cp_to_expected_score(0.0), 0.5);
    }

    #[test]
    fn leaf_values_flip() {
        let b = Blend { slice_prior: 0.52, our_white: true, prior_strength: 500.0,
                        eval_weight: 0.75, eval_weight_min: 0.0, eval_weight_k: 0.0 };
        let w = leaf_edge_value(0.6, 100.0, 0.55, &b);
        let bb = Blend { our_white: false, ..b };
        assert_eq!(leaf_edge_value(0.6, 100.0, 0.55, &bb), 1.0 - w);
        // no eval: the smoothed empirical score
        assert_eq!(leaf_edge_value(0.6, 100.0, f64::NAN, &b), smoothed_score(0.6, 100.0, 0.52, 500.0));
        // no games: the prior
        assert_eq!(leaf_node_value(&[], &[], f64::NAN, &b), 0.52);
    }

    #[test]
    fn aux_empty_row() {
        let x = aux_row_buckets(None, true);
        assert!(x.oth_eval.is_nan());
        assert_eq!(aux_opp_mix(&x, 0.75, 0.0, 0.0, false), (0.0, 0.0));
    }
}
