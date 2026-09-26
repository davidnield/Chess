//! Position keys: the polyglot hash, and the EPD as python-chess prints it.
//!
//! * Hash: `zobrist_hash::<Zobrist64>(EnPassantMode::PseudoLegal)` as i64.
//!   PseudoLegal is polyglot's rule (the ep file counts whenever a pawn of the
//!   side to move could capture pseudo-legally); `Legal` would differ on a
//!   pinned en-passant capture.
//! * EPD: `board.epd()` -- 4 fields, ep printed only when the capture is LEGAL.
//!   Stored as a 34-byte `Packed` position and rendered only when written, so
//!   a month's worth of keys costs 34 bytes each instead of a ~60-byte string.
//!   `render` equals shakmaty's `Epd::from_position(pos, Legal)` (tested) and
//!   python-chess's `board.epd()` (the fixtures and the per-ply diff).

use shakmaty::{
    zobrist::Zobrist64,
    CastlingSide, Chess, Color, EnPassantMode, Position, Role,
};

pub const START_HASH: i64 = 5_060_803_636_482_931_868;

#[inline]
pub fn hash(pos: &Chess) -> i64 {
    let z: Zobrist64 = pos.zobrist_hash(EnPassantMode::PseudoLegal);
    z.0 as i64
}

/// A position reduced to what its EPD shows.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Packed {
    /// One nibble per square, a1 = low nibble of byte 0: 0 empty, 1..=6 white
    /// P N B R Q K, 9..=14 black.
    board: [u8; 32],
    /// bit 0 white to move; bits 1..=4 castling K Q k q.
    meta: u8,
    /// File of the LEGAL en-passant square, or 0xFF.
    ep: u8,
}

const ROLE_NIBBLE: [(Role, u8); 6] = [
    (Role::Pawn, 1),
    (Role::Knight, 2),
    (Role::Bishop, 3),
    (Role::Rook, 4),
    (Role::Queen, 5),
    (Role::King, 6),
];
const NIBBLE_CHAR: [u8; 16] = *b".PNBRQK??pnbrqk?";

pub fn pack(pos: &Chess) -> Packed {
    let b = pos.board();
    let mut board = [0u8; 32];
    for (role, nib) in ROLE_NIBBLE {
        for (color, bit) in [(Color::White, 0u8), (Color::Black, 8u8)] {
            for sq in b.by_piece(role.of(color)) {
                let i = usize::from(sq);
                board[i >> 1] |= (nib | bit) << ((i & 1) * 4);
            }
        }
    }
    let c = pos.castles();
    let mut meta = u8::from(pos.turn() == Color::White);
    for (k, (color, side)) in [
        (Color::White, CastlingSide::KingSide),
        (Color::White, CastlingSide::QueenSide),
        (Color::Black, CastlingSide::KingSide),
        (Color::Black, CastlingSide::QueenSide),
    ]
    .into_iter()
    .enumerate()
    {
        if c.has(color, side) {
            meta |= 2 << k;
        }
    }
    let ep = pos.legal_ep_square().map_or(0xFF, |s| u32::from(s.file()) as u8);
    Packed { board, meta, ep }
}

impl Packed {
    #[inline]
    pub fn white_to_move(&self) -> bool {
        self.meta & 1 == 1
    }

    /// Append the EPD to `out`.
    pub fn render_into(&self, out: &mut String) {
        for rank in (0..8).rev() {
            let mut empty = 0u8;
            for file in 0..8 {
                let i = rank * 8 + file;
                let nib = (self.board[i >> 1] >> ((i & 1) * 4)) & 0xF;
                if nib == 0 {
                    empty += 1;
                } else {
                    if empty > 0 {
                        out.push((b'0' + empty) as char);
                        empty = 0;
                    }
                    out.push(NIBBLE_CHAR[nib as usize] as char);
                }
            }
            if empty > 0 {
                out.push((b'0' + empty) as char);
            }
            if rank > 0 {
                out.push('/');
            }
        }
        out.push_str(if self.white_to_move() { " w " } else { " b " });
        if self.meta & 0x1E == 0 {
            out.push('-');
        } else {
            for (k, ch) in ['K', 'Q', 'k', 'q'].into_iter().enumerate() {
                if self.meta & (2 << k) != 0 {
                    out.push(ch);
                }
            }
        }
        out.push(' ');
        if self.ep == 0xFF {
            out.push('-');
        } else {
            out.push((b'a' + self.ep) as char);
            // The ep square is behind the pawn that just double-pushed.
            out.push(if self.white_to_move() { '6' } else { '3' });
        }
    }

    pub fn render(&self) -> String {
        let mut s = String::with_capacity(72);
        self.render_into(&mut s);
        s
    }
}

/// shakmaty's own EPD, for cross-checking `Packed::render`.
pub fn shakmaty_epd(pos: &Chess) -> String {
    shakmaty::fen::Epd::from_position(pos, EnPassantMode::Legal).to_string()
}

/// The Lichess explorer's rating group (rating_bands.lichess_rating_group,
/// collapse_top): the lower bound of the group of the stored mean_elo.
#[inline]
pub fn rating_band(mean_elo: i32) -> i64 {
    const EDGES: [(i32, i64); 9] = [
        (1000, 0),
        (1200, 1000),
        (1400, 1200),
        (1600, 1400),
        (1800, 1600),
        (2000, 1800),
        (2200, 2000),
        (2500, 2200),
        (2800, 2500),
    ];
    for (upper, label) in EDGES {
        if mean_elo < upper {
            return label;
        }
    }
    2500
}
