"""replay_holdout classifies exits correctly and counts the right population.

This harness is about to decide things — the crush re-sweep, the recency-weighted
merge, LCB selection all get judged through it — so the classification rules are
worth pinning against hand-built cases rather than trusting a plausible-looking
summary table. A miscounted exit does not crash; it quietly moves a number that
an A/B is read off.

Two properties matter most, and both were wrong in the first working version:

  1. The FOUR exit reasons are distinct and mean different things. our_deviation
     is a SAMPLING exit (the game stopped being evidence about our book);
     opp_out_of_book is a COVERAGE exit (prep ran out against a legal move);
     book_end is a BUDGET exit (we chose to stop preparing); game_end is neither.
     Collapsing them into one "coverage" number is the conflation
     --reply-shrink's help text warns about.

  2. Only BOOK-FAITHFUL games are evidence about the book. Stage 3's `value` is
     a prediction conditional on our side playing best_move thereafter, so a
     game that deviated at a node must still count toward that node's "all"
     population but NOT its faithful one. The first cut binned deviation exits
     into the calibration and dragged every bin toward the pool mean.

Also pins the two bugs found by running it, since both are silent:
  * side_to_move is spelled "white"/"black" in the repertoire parquet, not
    "w"/"b" — guessing wrong yields a book with zero playable moves and every
    game exiting at ply 0, which reads as a coverage collapse, not a bug.
  * IncrementalZobrist.push_move pushes onto the board itself; a following
    board.push() is a double-push.

Run: .venv/Scripts/python.exe python/_test_replay_holdout.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from replay_holdout import (BOOK_END, DEVIATION, GAME_END, OUT_OF_BOOK,
                            load_book, walk_game)
from zobrist import IncrementalZobrist, zobrist_int64

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def hash_after(sans: list[str]) -> int:
    """Node key after playing `sans` from the start — the production way."""
    b = chess.Board()
    h = IncrementalZobrist(b)
    for s in sans:
        h.push_move(b, b.parse_san(s))      # pushes onto b itself
    return zobrist_int64(b) if not sans else h.current(b)


# A three-move white book: 1.e4 e5 2.Nf3 Nc6 3.Bb5, with values on every node
# it contains. Opponent nodes carry a value but no best_move, ours carry both.
LINE = ["e4", "e5", "Nf3", "Nc6", "Bb5"]
BOOK_MOVES = {(): "e4", ("e4", "e5"): "Nf3", ("e4", "e5", "Nf3", "Nc6"): "Bb5"}


def build_book() -> tuple[dict, dict]:
    moves, values = {}, {}
    for i in range(len(LINE) + 1):
        prefix = tuple(LINE[:i])
        h = hash_after(list(prefix))
        values[h] = 0.55 + 0.01 * i
        if prefix in BOOK_MOVES:
            moves[h] = BOOK_MOVES[prefix]
    return moves, values


def run(movetext: str, moves: dict, values: dict, perspective: str = "white",
        max_ply: int = 30) -> tuple[str, int, float | None, list[int]]:
    board, hasher, path = chess.Board(), IncrementalZobrist(chess.Board()), []
    r, ply, v = walk_game(movetext, perspective, moves, values, board, hasher,
                          max_ply, path)
    return r, ply, v, list(path)


def main() -> int:
    print("=" * 70)
    print("replay_holdout exit classification + faithful accounting")
    print("=" * 70)
    moves, values = build_book()

    # --- the four exits -------------------------------------------------
    r, ply, _v, _p = run("1. d4 d5", moves, values)
    check(r == DEVIATION and ply == 0,
          f"our side plays off-book at ply 0 -> our_deviation (got {r}, ply {ply})")

    # `ply` counts plies successfully PLAYED, so the departing move is included:
    # 1.e4 d5 plays 2 plies and the node after them is the one not in the book.
    r, ply, _v, _p = run("1. e4 d5 2. exd5", moves, values)
    check(r == OUT_OF_BOOK and ply == 2,
          f"opponent leaves the book -> opp_out_of_book at ply 2 (got {r}, ply {ply})")

    r, ply, _v, _p = run("1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4", moves, values)
    check(r == OUT_OF_BOOK and ply == 6,
          f"opponent leaves at the book's edge -> opp_out_of_book at ply 6 "
          f"(got {r}, ply {ply})")

    r, ply, _v, _p = run("1. e4 e5 2. Nf3 Nc6 3. Bb5", moves, values)
    check(r == GAME_END and ply == 5,
          f"moves run out inside the book -> game_end (got {r}, ply {ply})")

    # A leaf we own but never prepared: value present, best_move absent.
    m2 = dict(moves)
    del m2[hash_after(["e4", "e5"])]
    r, ply, _v, _p = run("1. e4 e5 2. Nf3 Nc6", m2, values)
    check(r == BOOK_END and ply == 2,
          f"our node with no best_move -> book_end, a BUDGET exit (got {r}, ply {ply})")

    r, ply, _v, _p = run("1. e4 e5 2. Nf3 Nc6 3. Bb5", moves, values, max_ply=3)
    check(r == BOOK_END and ply == 3,
          f"--max-ply cap -> book_end at the cap (got {r}, ply {ply})")

    # --- faithful vs all ------------------------------------------------
    # Bc4 instead of the book's Nf3 — a genuine OUR-side deviation at ply 2.
    # (An opponent-side departure like 2...Nf6 is a coverage exit, not this.)
    _r, _p2, _v, dev_path = run("1. e4 e5 2. Bc4 Nf6", moves, values)
    _r2, _p3, _v2, faith_path = run("1. e4 e5 2. Nf3 Nc6 3. Bb5", moves, values)
    root = hash_after([])
    check(root in dev_path and root in faith_path,
          "both a deviating and a faithful game traverse the root node")
    check(len(dev_path) == 3,
          f"a game deviating at ply 2 still contributes its first 3 nodes "
          f"(got {len(dev_path)})")
    # The deviating game's LAST node is where it went off-book; it must not be
    # credited past that point.
    check(hash_after(["e4", "e5", "Nf3"]) not in dev_path,
          "a deviating game contributes NO node beyond its deviation point")

    # --- perspective ----------------------------------------------------
    # walk_game is perspective-aware only through whose turn is "ours"; from
    # black's side the same movetext must classify white's move as opponent play.
    # A black book must contain the ROOT too — it is an opponent node (White to
    # move), and omitting it exits at ply 0 before anything is tested.
    bmoves = {hash_after(["e4"]): "e5"}
    bvalues = {hash_after([]): 0.5, hash_after(["e4"]): 0.5,
               hash_after(["e4", "e5"]): 0.5}
    r, ply, _v, _p = run("1. e4 e5", bmoves, bvalues, perspective="black")
    check(r == GAME_END and ply == 2,
          f"black perspective: our reply matched, game ends in book (got {r}, ply {ply})")
    r, ply, _v, _p = run("1. e4 c5", bmoves, bvalues, perspective="black")
    check(r == DEVIATION and ply == 1,
          f"black perspective: our reply differs -> our_deviation (got {r}, ply {ply})")

    # --- load_book contract ---------------------------------------------
    # The spelling of side_to_move is the bug that produced a zero-move book.
    tmp = Path(__file__).parent / "_tmp_replay_book.parquet"
    pl.DataFrame({
        "position_hash": [1, 2],
        "side_to_move": ["white", "black"],
        "value": [0.6, 0.4],
        "best_move": ["e4", None],
    }).write_parquet(tmp)
    try:
        mv, val, n = load_book(tmp, "white")
        check(n == 2 and len(val) == 2 and mv == {1: "e4"},
              "load_book: 'white'/'black' spelling yields our-turn moves")
        mv_b, val_b, _ = load_book(tmp, "black")
        check(mv_b == {} and abs(val_b[1] - 0.4) < 1e-12,
              "load_book: black perspective flips value and takes black's moves")
    finally:
        tmp.unlink(missing_ok=True)

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print()
    print(f"{'ALL PASS' if not n_fail else f'{n_fail} FAILED'} ({len(_checks)} checks)")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
