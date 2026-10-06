//! A port of python-chess 1.11.2 `Board.parse_san` (chess/__init__.py:3121-3209,
//! with `find_move` :2449 and `_from_chess960` :3768) over shakmaty's legal
//! move list. shakmaty's own SAN parser is stricter, so it is never used.
//!
//! The parts that decide exactness:
//!   * castling only through the 12-string table, or the file+rank `find_move`
//!     path (e1g1, e1h1, ...). In the regex path python-chess's castling
//!     candidate is the ROOK square, which its own-piece mask removes, so
//!     `Move::Castle` never matches there: `Kg1` fails.
//!   * `x`, `-`, `+` and `#` are never validated; there is no `P` letter; the
//!     promotion `=` is optional and its letter case-insensitive (and `=K` is
//!     accepted by the syntax, then matches nothing).
//!   * no piece letter + file + rank is `find_move`: any piece, a pawn to the
//!     back rank without a promotion defaults to a queen and then fails the
//!     promotion comparison, and e1h1/e1a1 remap to castling when a king of
//!     EITHER colour stands on the from-square.
//!   * otherwise a pawn move, from the given file or else the target's file.
//!   * null tokens (`--`, `Z0`, `0000`, `@@@@`) only when the regex fails.
//! All errors are treated alike by the extract; the kind is kept for tests.

use shakmaty::{CastlingSide, Chess, File, Move, MoveList, Position, Rank, Role, Square};

use crate::pyre::match_san;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parsed {
    Move(Move),
    Null,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SanError {
    Invalid,
    Illegal,
    Ambiguous,
}

fn role_of(letter: u8) -> Role {
    match letter.to_ascii_lowercase() {
        b'n' => Role::Knight,
        b'b' => Role::Bishop,
        b'r' => Role::Rook,
        b'q' => Role::Queen,
        b'k' => Role::King,
        _ => Role::Pawn,
    }
}

fn castle(pos: &Chess, side: CastlingSide) -> Result<Parsed, SanError> {
    pos.legal_moves()
        .into_iter()
        .find(|m| m.is_castle() && m.castling_side() == Some(side))
        .map(Parsed::Move)
        .ok_or(SanError::Illegal)
}

fn unique<'a>(mut it: impl Iterator<Item = &'a Move>) -> Result<Parsed, SanError> {
    match (it.next(), it.next()) {
        (Some(m), None) => Ok(Parsed::Move(*m)),
        (None, _) => Err(SanError::Illegal),
        (Some(_), Some(_)) => Err(SanError::Ambiguous),
    }
}

/// Where python-chess's move for a castle lands: the king's destination square.
fn castle_target(king: Square, rook: Square) -> Square {
    let file = if rook.file() > king.file() { File::G } else { File::C };
    Square::from_coords(file, king.rank())
}

/// `find_move(from, to, promotion)` followed by parse_san's promotion check.
fn find_move(
    pos: &Chess,
    moves: &MoveList,
    from: Square,
    to: Square,
    promotion: Option<Role>,
) -> Result<Parsed, SanError> {
    let board = pos.board();
    if promotion.is_none()
        && board.pawns().contains(from)
        && matches!(to.rank(), Rank::First | Rank::Eighth)
    {
        // find_move defaults this to a queen, and parse_san then raises
        // "missing promotion piece type" (or the move was illegal anyway).
        return Err(SanError::Illegal);
    }
    let target = if promotion.is_none() && board.kings().contains(from) {
        match (from, to) {
            (Square::E1, Square::H1) => Square::G1,
            (Square::E1, Square::A1) => Square::C1,
            (Square::E8, Square::H8) => Square::G8,
            (Square::E8, Square::A8) => Square::C8,
            _ => to,
        }
    } else {
        to
    };
    moves
        .iter()
        .find(|m| match **m {
            Move::Castle { king, rook } => {
                promotion.is_none() && king == from && castle_target(king, rook) == target
            }
            Move::Normal { from: f, to: t, promotion: p, .. } => {
                f == from && t == target && p == promotion
            }
            Move::EnPassant { from: f, to: t } => f == from && t == target && promotion.is_none(),
            Move::Put { .. } => false,
        })
        .map(|m| Parsed::Move(*m))
        .ok_or(SanError::Illegal)
}

pub fn parse_san(pos: &Chess, san: &str) -> Result<Parsed, SanError> {
    match san {
        "O-O" | "O-O+" | "O-O#" | "0-0" | "0-0+" | "0-0#" => return castle(pos, CastlingSide::KingSide),
        "O-O-O" | "O-O-O+" | "O-O-O#" | "0-0-0" | "0-0-0+" | "0-0-0#" => {
            return castle(pos, CastlingSide::QueenSide)
        }
        _ => {}
    }
    let Some(g) = match_san(san) else {
        // "," raises a different message in python-chess; both are errors here.
        return match san {
            "--" | "Z0" | "0000" | "@@@@" => Ok(Parsed::Null),
            _ => Err(SanError::Invalid),
        };
    };
    let to = Square::from_coords(
        File::new(u32::from(g.to_file - b'a')),
        Rank::new(u32::from(g.to_rank - b'1')),
    );
    let promotion = g.promo.map(role_of);
    let file = g.file.map(|f| File::new(u32::from(f - b'a')));
    let rank = g.rank.map(|r| Rank::new(u32::from(r - b'1')));
    let moves = pos.legal_moves();
    let from_ok = |m: &Move, file: Option<File>, rank: Option<Rank>| {
        m.from().is_some_and(|f| {
            file.is_none_or(|x| f.file() == x) && rank.is_none_or(|x| f.rank() == x)
        })
    };
    if let Some(letter) = g.piece {
        let role = role_of(letter);
        unique(moves.iter().filter(|m| {
            !m.is_castle()
                && m.role() == role
                && m.to() == to
                && m.promotion() == promotion
                && from_ok(m, file, rank)
        }))
    } else if let (Some(f), Some(r)) = (file, rank) {
        find_move(pos, &moves, Square::from_coords(f, r), to, promotion)
    } else {
        // No pawn captures without a file: the from-file defaults to the target's.
        let file = file.or(Some(to.file()));
        unique(moves.iter().filter(|m| {
            m.role() == Role::Pawn
                && !m.is_castle()
                && m.to() == to
                && m.promotion() == promotion
                && from_ok(m, file, rank)
        }))
    }
}

/// UCI in python-chess's notation (castling as the king's two-square move),
/// for comparisons with fixtures.
pub fn py_uci(m: &Move) -> String {
    let (from, to, promo) = match *m {
        Move::Castle { king, rook } => (king, castle_target(king, rook), None),
        Move::Normal { from, to, promotion, .. } => (from, to, promotion),
        Move::EnPassant { from, to } => (from, to, None),
        Move::Put { to, .. } => (to, to, None),
    };
    let mut s = format!("{from}{to}");
    if let Some(p) = promo {
        s.push(p.char());
    }
    s
}
