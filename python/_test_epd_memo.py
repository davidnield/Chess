"""V3 — the per-batch EPD memo in build_pooled_stats._walk_game is exact.

The memo caches board.epd() keyed on the position hash. That is only sound if
hash -> EPD is a FUNCTION. It is, and the reason is structural rather than
lucky:

    polyglot mixes in the en-passant file whenever a pawn is merely ADJACENT to
    the ep square ("legality of the potential capture is irrelevant",
    chess/polyglot.py:253), while board.epd() defaults to en_passant="legal"
    and prints the square only when the capture is actually legal.

    Legal captures are a SUBSET of adjacent ones, and placement / side-to-move /
    castling rights are encoded identically by both. So the hash distinguishes
    every pair of positions the EPD distinguishes, plus some it does not:
        same hash  =>  same EPD          (a function — safe to memoize)
        same EPD   =>  same hash         (FALSE — measured 16 counterexamples
                                          across 13.28M real positions)

    Hence: memoize hash -> EPD, never the reverse.

Checks below: the direction that holds, the direction that does not (with the
concrete pinned-pawn witness), and a direct memo-vs-no-memo replay diff.

Usage:  python _test_epd_memo.py [--visits N]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import _new_buf, _walk_game
from stage1_extract_positions import iter_san_moves
from zobrist import IncrementalZobrist, zobrist_int64

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--visits", type=int, default=40_000,
                    help="Ply visits for the aliasing scan. The default keeps "
                         "run_tests.py fast; the aliasing property has been "
                         "verified at 887,359 visits (0 aliases) — rerun the wide "
                         "net with --visits 500000.")
    args = ap.parse_args()

    print("\n-- the direction that does NOT hold (why the memo is one-way) --")
    # Black's d4 pawn is adjacent to the e3 ep square but pinned to its king by
    # the d1 rook. polyglot hashes the e-file; epd() prints "-". Drop the ep
    # square and the EPD is unchanged while the hash moves.
    WITH_EP = "3k4/8/8/8/3pP3/8/8/3RK3 b - e3 0 1"
    NO_EP = "3k4/8/8/8/3pP3/8/8/3RK3 b - - 0 1"
    a, b = chess.Board(WITH_EP), chess.Board(NO_EP)
    check(a.epd() == b.epd(),
          f"two positions share an EPD ({a.epd()!r})")
    check(zobrist_int64(a) != zobrist_int64(b),
          "...but have DIFFERENT hashes -> EPD -> hash is not a function")
    check(a.has_pseudo_legal_en_passant() and not a.has_legal_en_passant(),
          "...because the ep capture is pseudo-legal but illegal (pinned pawn)")

    print("\n-- the direction the memo relies on, over real games --")
    src = Path("D:/data/chess/standard-chess-games-compressed/year=2019/month=3/event=Blitz")
    files = sorted(src.glob("*.parquet")) if src.is_dir() else []
    if not files:
        print(f"  SKIP: no source parquet under {src}")
    else:
        import pyarrow.parquet as pq
        seen: dict[int, str] = {}
        alias = 0
        visits = 0
        first_alias = None
        pf = pq.ParquetFile(files[0])
        board = chess.Board()
        hasher = IncrementalZobrist(board)
        for batch in pf.iter_batches(batch_size=20_000, columns=["movetext"]):
            for mt in batch.column("movetext").to_pylist():
                if visits >= args.visits:
                    break
                board.reset()
                hasher.reset(board)
                for san in list(iter_san_moves(mt or ""))[:30]:
                    h = hasher.current(board)
                    e = board.epd()
                    visits += 1
                    prev = seen.get(h)
                    if prev is None:
                        seen[h] = e
                    elif prev != e and first_alias is None:
                        alias += 1
                        first_alias = (prev, e)
                    try:
                        mv = board.parse_san(san)
                    except (ValueError, AssertionError):
                        break
                    hasher.push_move(board, mv)   # must advance BOTH, or the hash desyncs
            if visits >= args.visits:
                break
        check(alias == 0,
              f"hash -> EPD is a function: {visits:,} ply visits, "
              f"{len(seen):,} distinct positions, {alias} aliases"
              + (f" (first: {first_alias})" if first_alias else ""))
        hit = 100 * (1 - len(seen) / visits) if visits else 0
        print(f"  info  memo hit rate {hit:.1f}% (the speedup this buys)")

    print("\n-- memoized replay == unmemoized replay --")
    if not files:
        print("  SKIP: no source parquet")
    else:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(files[0])
        recs = []
        for batch in pf.iter_batches(batch_size=5_000,
                                     columns=["movetext", "white_score", "termination",
                                              "move_count", "mean_elo"]):
            for r in batch.to_pylist():
                if r["mean_elo"] is not None and r["mean_elo"] >= 1800 \
                        and r["white_score"] is not None:
                    recs.append(r)
            # Small on purpose: this replays the same games three times (plain /
            # memoized / periodically-cleared), and _test_extract_equivalence.py
            # already covers the same ground at scale through the production path.
            if len(recs) >= 1200:
                break

        def replay(hasher, memo):
            buf = _new_buf()
            for r in recs:
                ws = r["white_score"]
                normal = r["termination"] == "Normal"
                _walk_game(buf, r["movetext"], ws, normal and ws == 1.0,
                           normal and ws == 0.0, r["move_count"], None, 30,
                           hasher, memo)
            return buf

        plain = replay(None, None)
        memo_shared: dict[int, str] = {}
        memoized = replay(IncrementalZobrist(chess.Board()), memo_shared)
        cols_ok = all(plain[k] == memoized[k] for k in plain)
        check(cols_ok,
              f"{len(recs):,} games, {len(plain['parent_hash']):,} rows: every buffer "
              f"column identical with and without the memo")

        # Clearing the memo periodically must not change anything either — that is
        # what extract_file does per read batch to bound its size.
        cleared_memo: dict[int, str] = {}
        buf = _new_buf()
        h = IncrementalZobrist(chess.Board())
        for i, r in enumerate(recs):
            if i % 500 == 0:
                cleared_memo.clear()
            ws = r["white_score"]
            normal = r["termination"] == "Normal"
            _walk_game(buf, r["movetext"], ws, normal and ws == 1.0,
                       normal and ws == 0.0, r["move_count"], None, 30, h, cleared_memo)
        check(all(plain[k] == buf[k] for k in plain),
              "periodically-cleared memo also identical (bounding it is free)")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
