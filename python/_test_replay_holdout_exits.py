"""replay_holdout's exit position (2026-10-18): walk_books records, per book, the
first position OUTSIDE the book a following game reached, so a policy value can
read the games that played the book's last move.

Book (white): 1.e4 at the start; the position after 1.e4 (Black to move) and after
1.e4 e5 (White to move, book move Nf3) are rows; nothing after 2.Nf3.

  A  1.e4 e5 2.Nf3 Nc6  -> the book ends after our 2.Nf3: exit = the position after it
  B  1.e4 c5            -> the opponent leaves: exit = the position after 1...c5
  C  1.d4               -> we deviate at a book node: no exit position
  D  every game resets the previous game's exit
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from replay_holdout import Book, walk_books  # noqa: E402
from zobrist import IncrementalZobrist, zobrist_int64  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def h_after(*sans) -> int:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return zobrist_int64(b)


def main() -> int:
    tmp = Path(__file__).parent / "_tmp_exits_book.parquet"
    pl.DataFrame({
        "position_hash": [h_after(), h_after("e4"), h_after("e4", "e5")],
        "side_to_move": ["white", "black", "white"],
        "value": [0.55, 0.55, 0.56],
        "best_move": ["e4", None, "Nf3"]}).write_parquet(tmp)
    try:
        book = Book(tmp, "white")
    finally:
        tmp.unlink(missing_ok=True)
    board = chess.Board()
    hasher = IncrementalZobrist(board)

    walk_books("1. e4 e5 2. Nf3 Nc6", "white", [book], board, hasher, 30)
    check("A: the book ends after our last move -> exit is the position after it",
          book.exit_hash == h_after("e4", "e5", "Nf3"))
    walk_books("1. e4 c5", "white", [book], board, hasher, 30)
    check("B: the opponent leaves -> exit is the position after their move",
          book.exit_hash == h_after("e4", "c5"))
    walk_books("1. d4", "white", [book], board, hasher, 30)
    check("C/D: a deviation has no exit position (and the last one is reset)",
          book.exit_hash is None)

    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
