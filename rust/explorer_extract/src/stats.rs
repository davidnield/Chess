//! Run counters. Python keeps games / kept / drops and never logs failed games;
//! these also count parse failures, null-move tokens and plies, which is what
//! lets month mode check its output against an independent count.

use std::sync::Mutex;

use serde::Serialize;

use crate::game::Drop;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Counters {
    pub games: u64,
    pub kept: u64,
    pub drop_elo: u64,
    pub drop_no_score: u64,
    pub drop_termination: u64,
    pub drop_bot: u64,
    pub failed: u64,
    pub null_tokens: u64,
    pub plies: u64,
    pub plies_white: u64,
    pub plies_draw: u64,
    pub plies_black: u64,
}

impl Counters {
    #[inline]
    pub fn drop(&mut self, d: Drop) {
        match d {
            Drop::Elo => self.drop_elo += 1,
            Drop::NoScore => self.drop_no_score += 1,
            Drop::Termination => self.drop_termination += 1,
            Drop::Bot => self.drop_bot += 1,
        }
    }

    pub fn add(&mut self, o: &Counters) {
        self.games += o.games;
        self.kept += o.kept;
        self.drop_elo += o.drop_elo;
        self.drop_no_score += o.drop_no_score;
        self.drop_termination += o.drop_termination;
        self.drop_bot += o.drop_bot;
        self.failed += o.failed;
        self.null_tokens += o.null_tokens;
        self.plies += o.plies;
        self.plies_white += o.plies_white;
        self.plies_draw += o.plies_draw;
        self.plies_black += o.plies_black;
    }
}

#[derive(Default)]
pub struct RunStats(Mutex<Counters>);

impl RunStats {
    pub fn add(&self, c: &Counters) {
        self.0.lock().unwrap().add(c);
    }

    pub fn get(&self) -> Counters {
        *self.0.lock().unwrap()
    }
}
