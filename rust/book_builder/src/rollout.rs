//! convertibility_eval.rollouts' games in Rust (shakmaty): the per-game work of the referee's and
//! the samplers' sampled games. Python keeps each ply's batch work (move counts, Maia, blend) and
//! hands the distributions in; every game's book or sampled move, the move itself, its hash,
//! game over and the leaves are here.
//!
//! Conceptually the Python loop, not bit for bit (user, 2026-11-12: random/sampler work need not
//! match python-chess exactly). The draws are the same: u01 is our own counter-based mixer
//! (splitmix64 over the integers), computed identically. Hashes are polyglot Zobrist (shakmaty
//! Zobrist64, pseudo-legal en passant = zobrist_int64); EPDs show en passant only when a capture
//! is legal (python-chess's epd()). Game over is python-chess's is_game_over(claim_draw=False):
//! checkmate, stalemate, insufficient material, the 75-move rule and fivefold repetition (counted
//! on the game's own positions since its start, as a board copied without its move stack does).

use std::collections::{HashMap, HashSet};

use shakmaty::fen::{Epd, Fen};
use shakmaty::san::SanPlus;
use shakmaty::zobrist::Zobrist64;
use shakmaty::{CastlingMode, Chess, Color, EnPassantMode, KnownOutcome, Position};

pub const MISSING: i64 = -32768;

/// convertibility_eval.u01: splitmix64's mixer chained over the integers
pub fn u01(xs: &[i64]) -> f64 {
    let mut z: u64 = 0x9E3779B97F4A7C15;
    for &x in xs {
        z = (z ^ (x as u64)).wrapping_mul(0xBF58476D1CE4E5B9);
        z ^= z >> 31;
        z = z.wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 29;
    }
    (z >> 11) as f64 / 9007199254740992.0
}

pub fn hash(pos: &Chess) -> i64 {
    let z: Zobrist64 = pos.zobrist_hash(EnPassantMode::PseudoLegal);
    z.0 as i64
}

pub fn epd(pos: &Chess) -> String {
    Epd::from_position(pos, EnPassantMode::Legal).to_string()
}

pub fn from_epd(epd: &str) -> Result<Chess, String> {
    let fen: Fen = format!("{epd} 0 1").parse().map_err(|e| format!("{epd}: {e}"))?;
    fen.into_position(CastlingMode::Standard).map_err(|e| format!("{epd}: {e}"))
}

/// python-chess's result as our score: 1 a win, 0 a loss, 0.5 a draw
fn outcome(pos: &Chess, our_white: bool) -> f64 {
    match pos.outcome() {
        shakmaty::Outcome::Known(KnownOutcome::Decisive { winner }) =>
            if (winner == Color::White) == our_white { 1.0 } else { 0.0 },
        _ => 0.5,
    }
}

struct Game {
    pos: Chess,
    h: i64,
    w: f64,
    ply: i64,
    cp: i64,           // MISSING = None
    wk: f64,
    si: usize,
    k: i64,
    f: f64,
    seen: Vec<i64>,    // the game's positions so far (fivefold repetition)
    remembered: bool,
}

pub struct Leaf {
    pub w: f64,
    pub h: i64,
    pub cp: i64,
    pub us: bool,
    pub exact: f64,    // NaN = None
    pub epd: String,
    pub ply: i64,
    pub wk: f64,
    pub si: usize,
    pub k: i64,
}

pub struct Rollouts {
    our_white: bool,
    max_ply: i64,
    seed: i64,
    recall: f64,
    start_h: Vec<i64>,
    start_ply: Vec<i64>,
    book: HashMap<i64, String>,
    want_exits: bool,
    games: Vec<Game>,
    pub leaves: Vec<Leaf>,
    pub book_mass: f64,
    /// exits: (hash, ply, w * f, epd) in the order the Python loop adds them
    pub exits: Vec<(i64, i64, f64, String)>,
    dists: HashMap<(i64, bool), Vec<(String, f64)>>,
}

pub struct Start<'a> {
    pub h: i64,
    pub epd: &'a str,
    pub pr: f64,
    pub ply: i64,
    pub cp: i64,
    pub ws: f64,
    pub f: f64,
}

impl Rollouts {
    #[allow(clippy::too_many_arguments)]
    pub fn new(starts: &[Start], our_white: bool, k_games: usize, max_ply: i64, seed: i64,
               recall: f64, book: HashMap<i64, String>, want_exits: bool) -> Result<Rollouts, String> {
        let kf = k_games as f64;
        let mut r = Rollouts {
            our_white, max_ply, seed, recall, book, want_exits, games: Vec::new(),
            start_h: starts.iter().map(|s| s.h).collect(),
            start_ply: starts.iter().map(|s| s.ply).collect(),
            leaves: Vec::new(), book_mass: 0.0, exits: Vec::new(), dists: HashMap::new(),
        };
        for (si, s) in starts.iter().enumerate() {
            let b0 = from_epd(s.epd)?;
            let over = b0.is_game_over();
            if over || s.ply >= max_ply {
                let us0 = (b0.turn() == Color::White) == our_white;
                let ex = if over { outcome(&b0, our_white) } else { f64::NAN };
                for k in 0..k_games {
                    r.leaves.push(Leaf { w: s.pr / kf, h: s.h, cp: s.cp, us: us0, exact: ex,
                                         epd: s.epd.to_string(), ply: s.ply, wk: s.ws / kf, si, k: k as i64 });
                }
                continue;
            }
            for k in 0..k_games {
                r.games.push(Game { pos: b0.clone(), h: s.h, w: s.pr / kf, ply: s.ply, cp: s.cp,
                                    wk: s.ws / kf, si, k: k as i64, f: s.f, seen: vec![hash(&b0)],
                                    remembered: false });
            }
        }
        Ok(r)
    }

    pub fn alive(&self) -> usize {
        self.games.len()
    }

    pub fn hashes(&self) -> Vec<i64> {
        self.games.iter().map(|g| g.h).collect()
    }

    pub fn set_cp(&mut self, cps: &[i64]) {
        for (g, &c) in self.games.iter_mut().zip(cps) {
            if c != MISSING {
                g.cp = c;
            }
        }
    }

    fn us(&self, g: &Game) -> bool {
        (g.pos.turn() == Color::White) == self.our_white
    }

    /// The positions whose distributions are not known yet, (ours, theirs), each as (hash, EPD of
    /// the first game there); marks the games that play the book from memory this ply
    pub fn needed(&mut self) -> (Vec<(i64, String)>, Vec<(i64, String)>) {
        let mut out: (Vec<(i64, String)>, Vec<(i64, String)>) = (Vec::new(), Vec::new());
        let mut seen: HashSet<(i64, bool)> = HashSet::new();
        for i in 0..self.games.len() {
            let us = self.us(&self.games[i]);
            let g = &self.games[i];
            let mut rem = false;
            if us && self.book.contains_key(&g.h) {
                rem = self.recall == 1.0 || u01(&[self.seed, self.start_h[g.si], self.start_ply[g.si],
                                                  g.k, g.ply, 0x524543]) < self.recall;
            }
            let h = g.h;
            if !rem && !self.dists.contains_key(&(h, us)) && seen.insert((h, us)) {
                let e = epd(&g.pos);
                if us { out.0.push((h, e)) } else { out.1.push((h, e)) }
            }
            self.games[i].remembered = rem;
        }
        out
    }

    pub fn set_dist(&mut self, h: i64, ours: bool, d: Vec<(String, f64)>) {
        self.dists.insert((h, ours), d);
    }

    /// One ply of every game
    pub fn advance(&mut self) {
        let games = std::mem::take(&mut self.games);
        let mut nxt = Vec::with_capacity(games.len());
        for mut g in games {
            let us = self.us(&g);
            let san: String;
            if g.remembered {
                san = self.book[&g.h].clone();
            } else {
                if us && self.want_exits && g.f > 0.0 {
                    self.exits.push((g.h, g.ply, g.w * g.f, epd(&g.pos)));
                }
                if us {
                    g.f = 0.0;
                }
                let d = self.dists.get(&(g.h, us)).map(|v| v.as_slice()).unwrap_or(&[]);
                if d.is_empty() {
                    self.leaf(&g, us, f64::NAN, &g.pos);
                    continue;
                }
                let u = u01(&[self.seed, self.start_h[g.si], self.start_ply[g.si], g.k, g.ply]);
                let mut acc = 0.0;
                let mut pick = &d[d.len() - 1].0;
                for (s, p) in d {
                    acc += p;
                    if u < acc {
                        pick = s;
                        break;
                    }
                }
                san = pick.clone();
                if us {
                    g.wk += g.w;
                }
            }
            let mv = SanPlus::from_ascii(san.as_bytes()).ok().and_then(|s| s.san.to_move(&g.pos).ok());
            let Some(mv) = mv else {
                // an illegal book move ends the game as an engine leaf, as Python's ValueError does;
                // an illegal sampled move (a hash collision in a table) does too
                self.leaf(&g, us, f64::NAN, &g.pos);
                continue;
            };
            if g.remembered {
                self.book_mass += g.w;
            }
            g.pos.play_unchecked(mv);
            g.ply += 1;
            g.h = hash(&g.pos);
            g.seen.push(g.h);
            let rep5 = g.seen.iter().filter(|&&x| x == g.h).count() >= 5;
            let over = g.pos.is_game_over() || rep5 || g.pos.halfmoves() >= 150;
            if over {
                let ex = if g.pos.is_checkmate() { outcome(&g.pos, self.our_white) } else { 0.5 };
                let us2 = (g.pos.turn() == Color::White) == self.our_white;
                self.leaf(&g, us2, ex, &g.pos);
            } else if g.ply >= self.max_ply {
                let us2 = (g.pos.turn() == Color::White) == self.our_white;
                self.leaf(&g, us2, f64::NAN, &g.pos);
            } else {
                nxt.push(g);
            }
        }
        self.games = nxt;
    }

    fn leaf(&mut self, g: &Game, us: bool, exact: f64, pos: &Chess) {
        self.leaves.push(Leaf { w: g.w, h: g.h, cp: g.cp, us, exact, epd: epd(pos), ply: g.ply,
                                wk: g.wk, si: g.si, k: g.k });
    }
}
