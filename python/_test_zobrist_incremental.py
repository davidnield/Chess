"""Verify IncrementalZobrist agrees with chess.polyglot on EVERY position.

The incremental hasher carries only the piece-placement component and recomputes
castling/ep/turn per ply. That decomposition is only safe if the placement delta
handles every way pieces can move that is not "lift from A, drop on B":
en-passant (captured pawn is off the to-square), promotion (a different piece
lands), and castling (two pieces move). And the recomputed components must match
polyglot's exact conventions — notably its en-passant rule, which hashes the file
whenever a pawn is merely ADJACENT to the ep square ("legality of the potential
capture is irrelevant", polyglot.py:253), unlike board.epd()'s legal-only rule.

Every check asserts IncrementalZobrist.current(board) == zobrist_int64(board)
after each push, so a wrong delta fails at the exact ply it happens.

Usage:  python _test_zobrist_incremental.py [--games N]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from zobrist import IncrementalZobrist, zobrist_int64

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def walk(label: str, fen: str | None, moves: list[str], uci: bool = False) -> None:
    """Replay `moves` asserting hash agreement after every push (and at the start)."""
    board = chess.Board(fen) if fen else chess.Board()
    h = IncrementalZobrist(board)
    if h.current(board) != zobrist_int64(board):
        check(False, f"{label}: seed mismatch at {board.fen()}")
        return
    for i, m in enumerate(moves, 1):
        mv = chess.Move.from_uci(m) if uci else board.parse_san(m)
        h.push_move(board, mv)
        if h.current(board) != zobrist_int64(board):
            check(False, f"{label}: mismatch after ply {i} ({m}) at {board.fen()}")
            return
    check(True, f"{label} ({len(moves)} plies)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", type=int, default=3000,
                    help="Games for the differential replay (default 3000; use 50000 for the full net).")
    args = ap.parse_args()

    CASTLE = "r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1"

    print("\n-- basic movement --")
    walk("quiet moves + captures", None,
         ["e4", "e5", "Nf3", "Nc6", "Bb5", "a6", "Bxc6", "dxc6", "Nxe5", "Qd4"])
    walk("pawn double push creates ep square", None, ["e4", "e5", "d4"])

    print("\n-- en passant (captured pawn is OFF the to-square) --")
    walk("white ep capture", None, ["e4", "e6", "e5", "d5", "exd6"])
    walk("black ep capture", None, ["e4", "d5", "Nf3", "d4", "c4", "dxc3"])

    print("\n-- polyglot's ep rule: adjacency, not legality --")
    # After 1.e4 the ep square is e3 but no black pawn is adjacent -> polyglot
    # contributes NOTHING. After 1.e4 d5 2.e5 f5 the ep square is f6 and White's
    # e5 pawn IS adjacent -> the file key IS mixed in. Both must agree.
    walk("ep square with NO adjacent pawn (no key)", None, ["e4"])
    walk("ep square WITH adjacent pawn (key mixed in)", None, ["e4", "d5", "e5", "f5"])
    walk("ep key expires on the next ply", None, ["e4", "d5", "e5", "f5", "Nf3", "Nc6"])
    # Pseudo-legal but ILLEGAL ep: black's d4 pawn is adjacent to the e3 ep square
    # but pinned to its king by the d1 rook. Polyglot still hashes the e-file;
    # board.epd() prints "-". This is the exact case where the two conventions
    # diverge, and it is what makes hash -> EPD (but not EPD -> hash) a function.
    PINNED_EP = "3k4/8/8/8/3pP3/8/8/3RK3 b - e3 0 1"
    pin = chess.Board(PINNED_EP)
    check(pin.has_pseudo_legal_en_passant() and not pin.has_legal_en_passant(),
          "fixture really is pseudo-legal-but-illegal ep "
          f"(pseudo={pin.has_pseudo_legal_en_passant()}, legal={pin.has_legal_en_passant()})")
    check(pin.epd().split()[3] == "-",
          f"...and board.epd() omits the ep square (epd field {pin.epd().split()[3]!r})")
    check(zobrist_int64(pin) != zobrist_int64(chess.Board(PINNED_EP.replace(" e3 ", " - "))),
          "...while polyglot DOES mix in the ep file (hashes differ)")
    walk("pseudo-legal-but-pinned ep still hashes", PINNED_EP, ["Kc8"])

    print("\n-- promotions --")
    for pc in ("q", "r", "b", "n"):
        walk(f"promotion to {pc.upper()}", "4k3/1P6/8/8/8/8/8/4K3 w - - 0 1", [f"b7b8{pc}"], uci=True)
    for pc in ("q", "r", "b", "n"):
        # Capture-promotion that ALSO destroys black's queenside castling right
        # (the rook is taken; rights change with no move by their owner).
        walk(f"capture-promotion to {pc.upper()} + rook-capture rights loss",
             "r3k2r/1P6/8/8/8/8/8/4K3 w kq - 0 1", [f"b7a8{pc}"], uci=True)

    print("\n-- castling (two pieces move) --")
    # All four castlings, each in its own walk (after White castles, its rook
    # rakes a file that makes the mirrored Black castling illegal).
    CASTLE_B = "r3k2r/8/8/8/8/8/8/R3K2R b KQkq - 0 1"
    walk("white O-O", CASTLE, ["O-O"])
    walk("white O-O-O", CASTLE, ["O-O-O"])
    walk("black O-O", CASTLE_B, ["O-O"])
    walk("black O-O-O", CASTLE_B, ["O-O-O"])
    walk("white O-O then black O-O-O", CASTLE, ["O-O", "O-O-O"])
    walk("white O-O-O then black O-O", CASTLE, ["O-O-O", "O-O"])
    walk("king-takes-rook encoding (e1h1)", CASTLE, ["e1h1"], uci=True)
    walk("king-takes-rook encoding (e1a1)", CASTLE, ["e1a1"], uci=True)

    print("\n-- castling-rights loss (recomputed, not tracked) --")
    walk("rights lost by king move", CASTLE, ["Ke2", "Ke7"])
    walk("rights lost by kingside rook move", CASTLE, ["Rhg1", "Rhg8"])
    walk("rights lost by queenside rook move", CASTLE, ["Rab1", "Rab8"])
    walk("rights lost by rook being CAPTURED",
         "r3k2r/8/8/8/8/8/8/R3K2R w KQkq - 0 1", ["a1a8"], uci=True)

    print("\n-- Chess960 guard --")
    # is_castling() is true for any king move of 2+ files, even with no rook to
    # castle with. The delta would be wrong, so it must raise rather than corrupt.
    b = chess.Board("4k3/8/8/8/8/8/8/4K3 w - - 0 1")
    h = IncrementalZobrist(b)
    try:
        h.push_move(b, chess.Move(chess.E1, chess.G1))
        check(False, "Chess960/absent-rook castling raises ValueError (no raise)")
    except ValueError as e:
        check("standard chess only" in str(e),
              f"Chess960/absent-rook castling raises ValueError ({type(e).__name__})")

    print(f"\n-- differential replay over real games (--games {args.games}) --")
    src = Path("D:/data/chess/standard-chess-games-compressed/year=2019/month=3/event=Blitz")
    files = sorted(src.glob("*.parquet")) if src.is_dir() else []
    if not files:
        print(f"  SKIP: no source parquet under {src} (targeted checks above still ran)")
    else:
        import pyarrow.parquet as pq
        from stage1_extract_positions import iter_san_moves
        n_games = n_plies = n_bad = 0
        pf = pq.ParquetFile(files[0])
        board = chess.Board()
        hasher = IncrementalZobrist(board)
        for batch in pf.iter_batches(batch_size=20_000, columns=["movetext"]):
            for mt in batch.column("movetext").to_pylist():
                if n_games >= args.games:
                    break
                n_games += 1
                board.reset()
                hasher.reset(board)
                # Full game length (NOT capped at ply 30) so promotions and late
                # castling actually occur in the sample.
                for san in iter_san_moves(mt or ""):
                    try:
                        mv = board.parse_san(san)
                    except (ValueError, AssertionError):
                        break
                    hasher.push_move(board, mv)
                    n_plies += 1
                    if hasher.current(board) != zobrist_int64(board):
                        n_bad += 1
                        if n_bad <= 3:
                            print(f"    MISMATCH after {san} at {board.fen()}")
                        break
            if n_games >= args.games:
                break
        check(n_bad == 0,
              f"differential replay: {n_games:,} games / {n_plies:,} plies, {n_bad} mismatches")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
