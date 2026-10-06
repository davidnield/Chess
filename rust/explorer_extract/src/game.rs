//! One game: the filters (`extract_file` :724-765) and the walk (`_walk_game`
//! :359-462). Every mode drives the same `walk`, so there is one implementation
//! of the per-game semantics and the modes differ only in what they record.

use shakmaty::{Chess, Position};

use crate::chesspos::{hash, START_HASH};
use crate::pyre::Tokens;
use crate::san::{parse_san, Parsed};

/// One source row, borrowed from the decoded columns.
#[derive(Clone, Copy, Debug)]
pub struct GameRow<'a> {
    pub movetext: Option<&'a str>,
    pub white_score: Option<f64>,
    pub termination: Option<&'a str>,
    pub mean_elo: Option<i32>,
    pub white_title: Option<&'a str>,
    pub black_title: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Drop {
    Elo,
    NoScore,
    Termination,
    Bot,
}

pub struct Filters {
    pub min_elo: i32,
    pub exclude_bots: bool,
    pub excluded_terminations: Vec<String>,
}

impl Filters {
    /// The first filter a game fails, in extract_file's order, or None to keep it.
    pub fn drop_reason(&self, g: &GameRow) -> Option<Drop> {
        match g.mean_elo {
            None => return Some(Drop::Elo),
            Some(e) if e < self.min_elo => return Some(Drop::Elo),
            _ => {}
        }
        // NaN is a value, not a NULL: such a game is kept and counts only
        // toward `total`.
        if g.white_score.is_none() {
            return Some(Drop::NoScore);
        }
        if let Some(t) = g.termination {
            if self.excluded_terminations.iter().any(|x| x == t) {
                return Some(Drop::Termination);
            }
        }
        // An exact, case-sensitive test on possibly-NULL fields: NULL passes.
        if self.exclude_bots && (g.white_title == Some("BOT") || g.black_title == Some("BOT")) {
            return Some(Drop::Bot);
        }
        None
    }
}

/// `_term_reason`: Normal 0, Time forfeit 1, Abandoned 2, anything else
/// (NULL included) 3.
#[inline]
pub fn term_reason(termination: Option<&str>) -> i32 {
    match termination {
        Some("Normal") => 0,
        Some("Time forfeit") => 1,
        Some("Abandoned") => 2,
        _ => 3,
    }
}

/// W/D/B as `_agg_ps` counts them: ws == 1.0 / 0.5 / 0.0. Anything else (NaN,
/// 0.25, ...) counts only toward the total.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    White,
    Draw,
    Black,
    Other,
}

impl Outcome {
    #[inline]
    pub fn of(ws: f64) -> Outcome {
        if ws == 1.0 {
            Outcome::White
        } else if ws == 0.5 {
            Outcome::Draw
        } else if ws == 0.0 {
            Outcome::Black
        } else {
            Outcome::Other
        }
    }
}

/// Win/draw/loss/total counts, as the partials store them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub w: u32,
    pub d: u32,
    pub b: u32,
    pub t: u32,
}

impl Counts {
    #[inline]
    pub fn add(&mut self, o: Outcome) {
        match o {
            Outcome::White => self.w += 1,
            Outcome::Draw => self.d += 1,
            Outcome::Black => self.b += 1,
            Outcome::Other => {}
        }
        self.t += 1;
    }

    #[inline]
    pub fn merge(&mut self, o: &Counts) {
        self.w += o.w;
        self.d += o.d;
        self.b += o.b;
        self.t += o.t;
    }
}

/// What a mode records along the walk.
pub trait Walker {
    /// Before the move at `ply` is parsed, at the position it is played from.
    /// The partials' EPD memo lives here: Python fills it before parse_san, so a
    /// game that fails at this ply still leaves its entry for later games.
    fn before_parse(&mut self, _ply: u32, _pos: &Chess, _ph: i64) {}
    /// A parsed and played ply: `pos` is the position BEFORE the move.
    fn row(&mut self, ply: u32, pos: &Chess, ph: i64, san: &str, ch: i64);
    /// The term row, for a game that did not fail; `end_ply` is the plies
    /// walked (0 for a game with no moves).
    fn term(&mut self, hash: i64, kind: i32, end_ply: u32);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkEnd {
    Done,
    /// parse_san failed at this ply: the earlier rows stand, no term row.
    Failed { ply: u32 },
}

/// A null move while in check: python-chess pushes it, shakmaty cannot
/// represent the result, so the run stops and names the game.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NullInCheck {
    pub ply: u32,
}

/// `_walk_game` for one kept game, over its first `max_ply + 1` tokens (the
/// extra one is what tells an ended game from one cut at the horizon).
pub fn walk<W: Walker>(
    w: &mut W,
    toks: &Tokens,
    max_ply: usize,
    start: &Chess,
) -> Result<WalkEnd, NullInCheck> {
    if toks.is_empty() {
        w.term(START_HASH, 0, 0);
        return Ok(WalkEnd::Done);
    }
    let n = toks.len();
    let maxply = max_ply.min(n);
    let mut pos = start.clone();
    let mut ph = START_HASH;
    for ply in 1..=maxply as u32 {
        let san = toks.get(ply as usize - 1);
        w.before_parse(ply, &pos, ph);
        let next = match parse_san(&pos, san) {
            Err(_) => return Ok(WalkEnd::Failed { ply }),
            Ok(Parsed::Move(m)) => {
                let mut next = pos.clone();
                next.play_unchecked(m);
                next
            }
            Ok(Parsed::Null) => {
                if pos.is_check() {
                    return Err(NullInCheck { ply });
                }
                match pos.clone().swap_turn() {
                    Ok(p) => p,
                    Err(_) => return Err(NullInCheck { ply }),
                }
            }
        };
        let ch = hash(&next);
        w.row(ply, &pos, ph, san, ch);
        pos = next;
        ph = ch;
    }
    w.term(ph, if maxply >= n { 0 } else { 1 }, maxply as u32);
    Ok(WalkEnd::Done)
}

/// Null-move tokens, counted for provenance (Python never counts them).
#[inline]
pub fn is_null_token(t: &str) -> bool {
    matches!(t, "--" | "Z0" | "0000" | "@@@@")
}

/// A Walker that also takes the per-game context and keeps the run counters.
pub trait GameSink: Walker {
    fn start_game(&mut self, band: i64, ws: f64, reason: i32);
    fn counters(&mut self) -> &mut crate::stats::Counters;
    /// An error raised inside the walk (a token too long for the key).
    fn take_err(&mut self) -> Option<anyhow::Error>;
}

/// One source row as extract_file's loop body: the filters in order, the band,
/// then the walk. `row` names the game in errors.
pub fn drive_game<S: GameSink>(
    s: &mut S,
    row: u64,
    g: &GameRow,
    filters: &Filters,
    max_ply: usize,
    toks: &mut Tokens,
    start: &Chess,
) -> anyhow::Result<()> {
    s.counters().games += 1;
    if let Some(d) = filters.drop_reason(g) {
        s.counters().drop(d);
        return Ok(());
    }
    s.counters().kept += 1;
    s.start_game(
        crate::chesspos::rating_band(g.mean_elo.unwrap()),
        g.white_score.unwrap(),
        term_reason(g.termination),
    );
    toks.fill(g.movetext, max_ply + 1);
    match walk(s, toks, max_ply, start) {
        Ok(WalkEnd::Done) => {}
        Ok(WalkEnd::Failed { .. }) => s.counters().failed += 1,
        Err(NullInCheck { ply }) => anyhow::bail!(
            "row {row}: null move {:?} while in check at ply {ply}; python-chess would push \
             it, shakmaty cannot",
            toks.get(ply as usize - 1)
        ),
    }
    match s.take_err() {
        Some(e) => Err(e.context(format!("row {row}"))),
        None => Ok(()),
    }
}

/// Per-ply counters every sink keeps the same way.
#[inline]
pub fn count_ply(c: &mut crate::stats::Counters, o: Outcome, san: &str) {
    c.plies += 1;
    match o {
        Outcome::White => c.plies_white += 1,
        Outcome::Draw => c.plies_draw += 1,
        Outcome::Black => c.plies_black += 1,
        Outcome::Other => {}
    }
    if is_null_token(san) {
        c.null_tokens += 1;
    }
}
