//! Frozen Cloud/NNUE-Fishnet/ChessDB source policy. All output is White POV.
//! Fallback is the explicitly bound pre-change lookup, never renormalized weights.
use crate::value::{cp_to_expected_score, ws_to_cp};

pub const POLICY: &str = "cloud-nnue-chessdb-score-334-v1";
pub const TERMINAL: u8 = 3;
pub const ENSEMBLE: u8 = 6;
pub const CHESSDB_NO_CLOUD: u8 = 7;

#[derive(Clone, Copy, Debug)]
pub struct Evaluation {
    pub cp: f64,
    pub score: f64,
    pub source: u8,
}

fn present(value: f64) -> Result<Option<f64>, &'static str> {
    if value.is_nan() { Ok(None) }
    else if value.is_finite() { Ok(Some(value)) }
    else { Err("infinite source evaluation") }
}

/// Source CP is already White POV. A recorded CP takes precedence over mate,
/// matching the frozen study normalization. A missing score stays missing.
pub fn normalize(cp: f64, mate: f64) -> Result<Option<f64>, &'static str> {
    let c = present(cp)?;
    let m = present(mate)?;
    Ok(c.map(|v| v.clamp(-2000., 2000.))
        .or_else(|| m.map(|v| if v == 0. { 0. } else { v.signum() * 2000. })))
}

fn evaluated(cp: f64, source: u8) -> Evaluation {
    Evaluation { cp, score: cp_to_expected_score(cp), source }
}

/// Authoritative terminal rows precede sources. Qualified local searches remain
/// a higher layer in the builder's EvalSource; this cached-source API cannot write
/// that store or promote source values into qualified local evidence.
#[allow(clippy::too_many_arguments)]
pub fn select(cloud_cp: f64, cloud_mate: f64, fishnet_cp: f64, fishnet_mate: f64,
              nnue: bool, chessdb_stm: f64, white_to_move: bool,
              fallback_cp: f64, fallback_source: u8) -> Result<Evaluation, &'static str> {
    let c = normalize(cloud_cp, cloud_mate)?;
    let f = normalize(fishnet_cp, fishnet_mate)?;
    let cb = present(chessdb_stm)?.map(|v| {
        let white = if white_to_move { v } else { -v };
        // Also maps decisive/tablebase claims (|v| >= 15000) to the existing cap.
        white.clamp(-2000., 2000.)
    });
    let base = present(fallback_cp)?;
    if fallback_source > 5 || (fallback_source == 0) != base.is_none()
        || base.is_some_and(|v| !(-2000. ..=2000.).contains(&v)) {
        return Err("invalid pre-change fallback evaluation/provenance");
    }
    if fallback_source == TERMINAL {
        return Ok(evaluated(base.unwrap(), TERMINAL));
    }
    if let (Some(c), Some(f), Some(cb), true) = (c, f, cb, nnue) {
        let score = 0.3 * cp_to_expected_score(c) + 0.3 * cp_to_expected_score(f)
            + 0.4 * cp_to_expected_score(cb);
        return Ok(Evaluation { cp: ws_to_cp(score, true).clamp(-2000., 2000.), score,
                               source: ENSEMBLE });
    }
    if let (None, Some(cb)) = (c, cb) {
        return Ok(evaluated(cb, CHESSDB_NO_CLOUD));
    }
    Ok(base.map(|cp| evaluated(cp, fallback_source))
        .unwrap_or(Evaluation { cp: f64::NAN, score: f64::NAN, source: 0 }))
}

#[cfg(test)]
mod tests {
    use super::*;
    const NA: f64 = f64::NAN;

    #[test]
    fn score_space_and_stm_orientation() {
        let r = select(600., NA, -100., NA, true, -200., false, 600., 1).unwrap();
        let expected = 0.3*cp_to_expected_score(600.) + 0.3*cp_to_expected_score(-100.)
            + 0.4*cp_to_expected_score(200.);
        assert!((r.score-expected).abs() < 1e-15);
        assert!((cp_to_expected_score(r.cp)-expected).abs() < 1e-15);
        assert!((r.cp-230.).abs() > 20.); // not a CP-space mean
        assert_eq!(r.source, ENSEMBLE);
    }

    #[test]
    fn missingness_and_eligibility_do_not_renormalize() {
        for (c, f, cb, nnue, cp, source) in [
            (100., 200., 300., false, 77., 1),
            (100., 200., NA, true, 77., 1),
            (100., NA, 300., true, 77., 1),
            (NA, 200., 300., true, 300., CHESSDB_NO_CLOUD),
            (NA, NA, 300., false, 300., CHESSDB_NO_CLOUD),
            (NA, 200., NA, true, 77., 1),
        ] {
            let r = select(c, NA, f, NA, nnue, cb, true, 77., 1).unwrap();
            assert_eq!((r.cp, r.source), (cp, source));
        }
        let r = select(NA, NA, NA, NA, false, NA, true, NA, 0).unwrap();
        assert!(r.cp.is_nan() && r.score.is_nan());
        assert_eq!(r.source, 0);
    }

    #[test]
    fn mates_caps_terminals_and_malformed_inputs() {
        assert_eq!(normalize(NA, -4.).unwrap(), Some(-2000.));
        assert_eq!(normalize(40., -4.).unwrap(), Some(40.));
        let r = select(NA, NA, NA, NA, false, 30000., false, NA, 0).unwrap();
        assert_eq!(r.cp, -2000.);
        let r = select(-10., NA, -20., NA, true, -100., true, 2000., TERMINAL).unwrap();
        assert_eq!((r.cp, r.source), (2000., TERMINAL));
        assert!(select(NA, NA, NA, NA, false, NA, true, 0., 0).is_err());
        assert!(select(f64::INFINITY, NA, NA, NA, false, NA, true, NA, 0).is_err());
    }

    #[test]
    fn mirrored_positions_complement_scores_without_integer_rounding() {
        for cp in (-2000..2001).step_by(13) {
            let a = select(cp as f64, NA, 83., NA, true, 127., true, 0., 1).unwrap();
            let b = select(-cp as f64, NA, -83., NA, true, 127., false, 0., 1).unwrap();
            assert!((a.score+b.score-1.).abs() < 3e-16);
            assert!((cp_to_expected_score(a.cp)-a.score).abs() < 3e-16);
        }
    }
}
