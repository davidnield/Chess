//! The explorer extract in Rust: Lichess games -> the explorer's position
//! statistics, exact against the Python extract (build_pooled_stats.py at
//! d79a0c7) and python-chess 1.11.2. See rust/explorer_extract/README.md and
//! the `explorer-extract` binary's subcommands.

pub mod chesspos;
pub mod dump;
pub mod fasthash;
pub mod game;
pub mod keys;
pub mod month;
pub mod partials;
pub mod pydigits;
pub mod pyre;
pub mod san;
pub mod selftest;
pub mod source;
pub mod stats;
pub mod sys;
