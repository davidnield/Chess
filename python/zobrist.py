"""Shared position-hash helper. Every pipeline stage keys positions by this
exact hash (parquet position_hash columns, dict/join keys, DAG nodes) — a
second, drifting definition would silently break joins across stages."""
from __future__ import annotations

import chess
import chess.polyglot

INT64_MAX = 2**63 - 1
INT64_RANGE = 2**64

# Upstream's own hasher instance — used to SEED the incremental placement
# component below, so the seed is exact by construction rather than by a
# reimplementation that could drift.
_HASHER = chess.polyglot.ZobristHasher(chess.polyglot.POLYGLOT_RANDOM_ARRAY)
_P = chess.polyglot.POLYGLOT_RANDOM_ARRAY

# Polyglot key layout (chess/polyglot.py:225-271):
#   [64 * ((piece_type - 1) * 2 + pivot) + square]  pieces, pivot 0=BLACK 1=WHITE
#   [768..771]  castling W-K, W-Q, B-K, B-Q
#   [772 + file]  en-passant file
#   [780]  turn (XOR'd in iff WHITE to move)
_CASTLE_K = (768, 769, 770, 771)
_TURN = 780
_EP0 = 772


def zobrist_int64(board: chess.Board) -> int:
    """Polyglot Zobrist hash cast to signed int64 (parquet/polars compatible)."""
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


def _piece_key(piece_type: int, color: chess.Color, square: int) -> int:
    """Polyglot piece key. `pivot` is 0 for BLACK / 1 for WHITE, matching the
    enumeration order of board.occupied_co in ZobristHasher.hash_board."""
    return _P[64 * ((piece_type - 1) * 2 + (1 if color else 0)) + square]


class IncrementalZobrist:
    """Maintains the polyglot hash across a game replay without rescanning the
    board every ply.

    Only the PIECE-PLACEMENT component is carried incrementally — that is the
    entire expense (hash_board is a 64-square Python scan; profiling measured
    37.9M piece_at() calls for 592K positions, ~29% of total replay time).
    The castling / en-passant / turn components are recomputed on each
    `current()` call: together they are four boolean checks and one bitboard
    mask, and recomputing them keeps every fiddly rights-tracking edge case
    (rights lost by king move, rook move, or the rook being CAPTURED; ep
    expiry; polyglot's "pawn ready to capture, legality irrelevant" ep rule)
    inside python-chess, where it is already correct.

    Usage — `push_move` performs the board push itself so the caller cannot get
    the pre/post ordering wrong (the delta must read the PRE-push board):

        h = IncrementalZobrist(board)
        for san in moves:
            ph = h.current(board)          # == zobrist_int64(board)
            h.push_move(board, board.parse_san(san))
    """

    __slots__ = ("_pc",)

    def __init__(self, board: chess.Board):
        self._pc = _HASHER.hash_board(board)

    def reset(self, board: chess.Board) -> None:
        """Re-seed from `board` (reuse one hasher across games)."""
        self._pc = _HASHER.hash_board(board)

    def current(self, board: chess.Board) -> int:
        """Signed int64 polyglot hash of `board`. Equals zobrist_int64(board)."""
        h = self._pc
        if board.has_kingside_castling_rights(chess.WHITE):
            h ^= _P[_CASTLE_K[0]]
        if board.has_queenside_castling_rights(chess.WHITE):
            h ^= _P[_CASTLE_K[1]]
        if board.has_kingside_castling_rights(chess.BLACK):
            h ^= _P[_CASTLE_K[2]]
        if board.has_queenside_castling_rights(chess.BLACK):
            h ^= _P[_CASTLE_K[3]]
        ep = board.ep_square
        if ep:  # matches upstream's `if board.ep_square:` (square 0 can never be an ep square)
            if board.turn == chess.WHITE:
                ep_mask = chess.shift_down(chess.BB_SQUARES[ep])
            else:
                ep_mask = chess.shift_up(chess.BB_SQUARES[ep])
            ep_mask = chess.shift_left(ep_mask) | chess.shift_right(ep_mask)
            # Legality of the potential capture is deliberately irrelevant here —
            # polyglot.py:253 hashes the file whenever a pawn is merely ADJACENT.
            if ep_mask & board.pawns & board.occupied_co[board.turn]:
                h ^= _P[_EP0 + chess.square_file(ep)]
        if board.turn == chess.WHITE:
            h ^= _P[_TURN]
        return h - INT64_RANGE if h > INT64_MAX else h

    def push_move(self, board: chess.Board, move: chess.Move) -> None:
        """XOR the placement delta for `move`, then push it onto `board`."""
        if move:  # chess.Move.null() is falsy — no placement change
            color = board.turn
            pt = board.piece_type_at(move.from_square)
            self._pc ^= _piece_key(pt, color, move.from_square)
            if board.is_castling(move):
                # a_side is derived from the move's own files, so this is correct
                # for BOTH encodings python-chess may hand us: king-to-destination
                # (e1g1 / e1c1, what parse_san yields in 1.11.2) and king-takes-rook
                # (e1h1 / e1a1, the internal Chess960 form).
                a_side = chess.square_file(move.to_square) < chess.square_file(move.from_square)
                white = color == chess.WHITE
                if a_side:
                    rook_from = chess.A1 if white else chess.A8
                    king_to, rook_to = (chess.C1, chess.D1) if white else (chess.C8, chess.D8)
                else:
                    rook_from = chess.H1 if white else chess.H8
                    king_to, rook_to = (chess.G1, chess.F1) if white else (chess.G8, chess.F8)
                # Defensive: standard-chess castling only. Under Chess960 the rook
                # does not start on a1/h1 and the delta below would be wrong, so
                # fail loudly rather than silently corrupt every downstream key.
                if board.piece_type_at(rook_from) != chess.ROOK or \
                        not (board.occupied_co[color] & chess.BB_SQUARES[rook_from]):
                    raise ValueError(
                        f"IncrementalZobrist: castling {move.uci()} expects a {'white' if white else 'black'} "
                        f"rook on {chess.square_name(rook_from)} (standard chess only, no Chess960)")
                self._pc ^= _piece_key(chess.KING, color, king_to)
                self._pc ^= _piece_key(chess.ROOK, color, rook_from)
                self._pc ^= _piece_key(chess.ROOK, color, rook_to)
            else:
                if board.is_en_passant(move):
                    # The captured pawn is NOT on the to-square.
                    cap_sq = move.to_square + (-8 if color == chess.WHITE else 8)
                    self._pc ^= _piece_key(chess.PAWN, not color, cap_sq)
                else:
                    cap = board.piece_type_at(move.to_square)
                    if cap is not None:
                        self._pc ^= _piece_key(cap, not color, move.to_square)
                # A promotion lands as the promoted piece, not a pawn.
                self._pc ^= _piece_key(move.promotion or pt, color, move.to_square)
        board.push(move)
