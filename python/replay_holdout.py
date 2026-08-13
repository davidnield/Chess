"""Replay held-out games against a repertoire and measure what actually happened.

Every selection decision this project has made -- the crush weight, cover-weight,
memo-leave-cost, reply-shrink, all three sweep rounds -- was judged on in-sample
diagnostics computed from the same pool the book was built from, plus face
validity. A book scores well on the pool it was fitted to almost by construction.
This is the out-of-sample check: build from games up to year Y, replay games from
after Y, and compare what Stage 3 PREDICTED against what those games DID.

Origin: task #113, from the Fable-5 clean-room review (REPERTOIRE_PLAN.md 8.1).

WHAT IT CAN AND CANNOT MEASURE
------------------------------
A repertoire prescribes OUR moves, so a real game can only be replayed against it
for as long as the player on our side happened to play our moves. The moment they
deviate, the rest of that game is a different game and tells us nothing about our
book. So this walks each game while our-side moves match the book and stops, and
the population it reports on is conditioned on that match.

That conditioning is not a defect to be corrected, but it IS a bias to state:
players who follow our lines are not a random sample of the pool. Read the
outputs as "among games that played these moves, here is what happened", which is
exactly the quantity a coverage/calibration claim needs, and NOT as "here is what
would happen if you played this book against a random opponent".

Four ways a walk ends, and they mean completely different things:

  our_deviation    our side played something other than the book move. A SAMPLING
                   exit -- says nothing about book quality, it is just where this
                   game stopped being evidence. Should dominate the counts.
  opp_out_of_book  the opponent played into a position the book does not contain.
                   A real COVERAGE exit: this is prep running out against a legal
                   move someone actually played.
  book_end         our turn, position is in the book, but the book has no move
                   here -- a leaf. A BUDGET exit: we chose to stop preparing.
  game_end         the game itself ended while still inside the book.

Conflating the last three is precisely what --reply-shrink's help text warns
about, so they are counted separately and never summed into one "coverage".

OUTPUTS
-------
  1. Per-node parquet (position_hash, n_games, score_sum, value, side_to_move):
     the realized our-perspective score of every game that reached each book
     node, next to the value Stage 3 predicted for it. This is the calibration
     input, and it is written rather than only summarised so a later run can
     re-bin it without replaying anything.
  2. A printed summary: exit-reason breakdown, exit-ply distribution, realized
     score by exit reason, and predicted-vs-realized calibration bins.

Hashing goes through the SAME IncrementalZobrist the extract uses, so node keys
are directly comparable to position_stats and to the repertoire parquet. Do not
substitute a local hash here.

Usage:
    .venv/Scripts/python.exe python/replay_holdout.py \\
        --repertoire E:/chess/repertoire/repertoire_pooled_white_sharp.parquet \\
        --perspective white --start-year 2025 --end-year 2026 \\
        --limit-games 200000 --out E:/chess/holdout/white_2025_26.parquet
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import discover_source_files
from stage1_extract_positions import iter_san_moves
from zobrist import IncrementalZobrist, zobrist_int64

EVENTS = ["Blitz", "Rapid", "Classical"]
# movetext/white_score/mean_elo are all the walk needs; termination and
# move_count are extract-only concerns.
SRC_COLUMNS = ["movetext", "white_score", "mean_elo"]
READ_BATCH_GAMES = 50_000

DEVIATION, OUT_OF_BOOK, BOOK_END, GAME_END, PARSE_ERROR = (
    "our_deviation", "opp_out_of_book", "book_end", "game_end", "parse_error")
REASONS = (DEVIATION, OUT_OF_BOOK, BOOK_END, GAME_END, PARSE_ERROR)


def load_book(path: Path, perspective: str) -> tuple[dict, dict, int]:
    """position_hash -> best_move (our nodes only), and -> value (all nodes).

    `value` is stored as expected WHITE score throughout the pipeline; it is
    flipped to our perspective here, once, so every number downstream of this
    function is already our-side.
    """
    df = pl.read_parquet(path, columns=["position_hash", "side_to_move",
                                        "value", "best_move"])
    # side_to_move is spelled out ("white"/"black") in the repertoire parquet,
    # not the single letter the EPD/FEN convention would suggest. Getting this
    # wrong silently yields a book with zero playable moves and every game
    # exiting at ply 0, which reads as a coverage collapse rather than a bug.
    our_turn = perspective
    values = {}
    for h, v in zip(df["position_hash"].to_list(), df["value"].to_list()):
        if v is not None:
            values[h] = v if perspective == "white" else 1.0 - v
    moves = {}
    for h, stm, mv in zip(df["position_hash"].to_list(),
                          df["side_to_move"].to_list(),
                          df["best_move"].to_list()):
        if stm == our_turn and mv:
            moves[h] = mv
    return moves, values, df.height


def walk_game(movetext: str, perspective: str,
              moves: dict, values: dict, board: chess.Board,
              hasher: IncrementalZobrist, max_ply: int,
              path: list[int]) -> tuple[str, int, float | None]:
    """Follow one game while our side matches the book.

    Returns (reason, ply, V) and fills `path` with every book node visited.
    Node accounting is left to the caller because whether a game COUNTS as
    evidence depends on how it ended, which is not known until the walk is over
    — see the faithful/all split in main().

    `ply` is the number of plies successfully played inside the book, so it is
    the depth at which the walk stopped. `V` is the book's predicted value at
    that deepest in-book node, or None if the node carried no value.
    """
    path.clear()
    board.reset()
    hasher.reset(board)
    our_white = perspective == "white"
    h = zobrist_int64(board)
    ply = 0
    try:
        for san in iter_san_moves(movetext):
            if ply >= max_ply:
                return BOOK_END, ply, values.get(h)
            # A node the book does not contain. Which exit this is depends on
            # WHOSE MOVE BROUGHT US HERE, which is the opposite of whose turn it
            # now is -- and reading it off the turn directly (the first version
            # of this) inverts the two labels:
            #   our turn now   -> the OPPONENT moved last, into a position we
            #                     have no entry for. Coverage ran out.
            #   their turn now -> WE moved last, playing a book move whose
            #                     result the book does not continue. Budget.
            # At ply 0 nobody has moved; a missing root just means no book.
            if h not in values:
                if ply == 0:
                    return BOOK_END, 0, None
                our_turn_now = (board.turn == chess.WHITE) == our_white
                return (OUT_OF_BOOK if our_turn_now else BOOK_END), ply, None
            path.append(h)

            us_to_move = (board.turn == chess.WHITE) == our_white
            if us_to_move:
                book_mv = moves.get(h)
                if not book_mv:
                    return BOOK_END, ply, values.get(h)
                if book_mv != san:
                    return DEVIATION, ply, values.get(h)

            mv = board.parse_san(san)
            hasher.push_move(board, mv)   # pushes onto `board` itself
            h = hasher.current(board)
            ply += 1
        # Ran out of moves while still following the book.
        if h in values:
            path.append(h)
        return GAME_END, ply, values.get(h)
    except (ValueError, AssertionError):
        return PARSE_ERROR, ply, None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repertoire", required=True)
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--start-year", type=int, default=2025)
    ap.add_argument("--end-year", type=int, default=2026)
    ap.add_argument("--months", type=str, default=None,
                    help="Comma-separated month numbers; default all.")
    ap.add_argument("--events", type=str, default=",".join(EVENTS))
    ap.add_argument("--min-elo", type=int, default=1800,
                    help="Match the pool floor the book was built from.")
    ap.add_argument("--limit-games", type=int, default=0,
                    help="Stop after N games read (0 = all). Kept games, not raw.")
    ap.add_argument("--max-ply", type=int, default=30,
                    help="Match the extract's cap; beyond it the book is empty "
                         "by construction and the exit would be an artifact.")
    ap.add_argument("--out", default=None, help="Per-node parquet to write.")
    a = ap.parse_args()

    months = [int(m) for m in a.months.split(",")] if a.months else None
    events = a.events.split(",")

    moves, values, n_rows = load_book(Path(a.repertoire), a.perspective)
    print(f"book      : {a.repertoire}")
    print(f"            {n_rows:,} rows -> {len(values):,} valued nodes, "
          f"{len(moves):,} our-turn moves ({a.perspective})")

    files = discover_source_files(a.start_year, a.end_year, months, events)
    print(f"holdout   : {len(files):,} source files, "
          f"{a.start_year}-{a.end_year}, mean_elo>={a.min_elo}")
    if not files:
        print("FATAL: no source files matched.")
        return 1

    board = chess.Board()
    hasher = IncrementalZobrist(board)
    # Two populations per node, and the distinction is the whole point.
    #   all       every game that reached the node, including ones that later
    #             played something other than our move. Answers "how do games
    #             through this position go in the wild".
    #   faithful  games that never deviated from the book for as long as they
    #             were in it. ONLY these are evidence about the book, because
    #             only these actually played it. Stage 3's `value` assumes our
    #             side plays best_move thereafter, so this is the population it
    #             is a prediction about.
    node_n: dict[int, int] = {}
    node_s: dict[int, float] = {}
    node_fn: dict[int, int] = {}
    node_fs: dict[int, float] = {}
    reasons: Counter = Counter()
    ply_hist: dict[str, Counter] = defaultdict(Counter)
    score_sum: dict[str, float] = defaultdict(float)
    depth_v: list[tuple[float, float]] = []          # (predicted V, realized)
    path: list[int] = []
    n_games = 0
    t0 = time.time()

    import pyarrow.parquet as pq
    for src, _y, _m, _ev in files:
        pf = pq.ParquetFile(src)
        for batch in pf.iter_batches(batch_size=READ_BATCH_GAMES,
                                     columns=SRC_COLUMNS):
            df = pl.from_arrow(batch)
            df = df.filter((pl.col("mean_elo") >= a.min_elo)
                           & pl.col("white_score").is_not_null())
            for mt, ws in zip(df["movetext"].to_list(),
                              df["white_score"].to_list()):
                reason, ply, v = walk_game(mt or "", a.perspective,
                                           moves, values, board, hasher,
                                           a.max_ply, path)
                reasons[reason] += 1
                ply_hist[reason][ply] += 1
                realized = float(ws) if a.perspective == "white" else 1.0 - float(ws)
                score_sum[reason] += realized
                faithful = reason in (OUT_OF_BOOK, BOOK_END, GAME_END)
                for h in path:
                    node_n[h] = node_n.get(h, 0) + 1
                    node_s[h] = node_s.get(h, 0.0) + realized
                    if faithful:
                        node_fn[h] = node_fn.get(h, 0) + 1
                        node_fs[h] = node_fs.get(h, 0.0) + realized
                if faithful and v is not None:
                    depth_v.append((v, realized))
                n_games += 1
            if a.limit_games and n_games >= a.limit_games:
                break
        if a.limit_games and n_games >= a.limit_games:
            break

    dt = time.time() - t0
    print(f"replayed  : {n_games:,} games in {dt:,.0f}s "
          f"({n_games/max(dt,1e-9):,.0f}/s), {len(node_n):,} book nodes reached")

    print("\nexit reason           games      share   mean score   mean exit ply")
    for r in REASONS:
        n = reasons[r]
        if not n:
            continue
        plies = ply_hist[r]
        mean_ply = sum(p * c for p, c in plies.items()) / max(sum(plies.values()), 1)
        print(f"  {r:<18} {n:>9,}  {100*n/max(n_games,1):>7.2f}%  "
              f"{score_sum[r]/n:>10.4f}  {mean_ply:>13.2f}")

    # Coverage curve: of games that got past ply p inside the book, how many.
    print("\nin-book survival (all exits pooled)")
    surv = Counter()
    for r in REASONS:
        for p, c in ply_hist[r].items():
            surv[p] += c
    cum = n_games
    for p in range(0, min(a.max_ply, 20) + 1):
        if p:
            cum -= surv[p - 1]
        if p in (0, 2, 4, 6, 8, 10, 12, 16, 20):
            print(f"  past ply {p:>2}: {cum:>9,}  ({100*cum/max(n_games,1):>6.2f}%)")

    # Calibration: does a node the book says is worth V deliver V?
    n_faith = sum(reasons[r] for r in (OUT_OF_BOOK, BOOK_END, GAME_END))
    print(f"\nbook-faithful games (never deviated): {n_faith:,} "
          f"({100*n_faith/max(n_games,1):.3f}% of replayed)")
    if n_faith:
        fs = sum(score_sum[r] for r in (OUT_OF_BOOK, BOOK_END, GAME_END))
        base = sum(score_sum[r] for r in REASONS) / max(n_games, 1)
        print(f"  mean score {fs/n_faith:.4f} vs {base:.4f} for the whole "
              f"replayed pool ({fs/n_faith - base:+.4f})")

    if depth_v:
        # Faithful games ONLY. Binning deviation exits here would compare a
        # node's predicted value against the outcome of games that stopped
        # playing our moves at that very node — the prediction is not about
        # them, and including them dragged every bin toward the pool mean.
        print("\ncalibration at the exit node, book-faithful games only")
        print("  V bin          n      predicted   realized   gap")
        bins: dict[int, list[tuple[float, float]]] = defaultdict(list)
        for v, r in depth_v:
            bins[min(int(v * 20), 19)].append((v, r))
        for b in sorted(bins):
            rows = bins[b]
            pv = sum(x[0] for x in rows) / len(rows)
            rv = sum(x[1] for x in rows) / len(rows)
            if len(rows) < 50:
                continue
            print(f"  {b/20:.2f}-{(b+1)/20:.2f}  {len(rows):>9,}  "
                  f"{pv:>10.4f}  {rv:>9.4f}  {rv-pv:>+6.4f}")

    if a.out:
        out = Path(a.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        hs = sorted(node_n)
        pl.DataFrame({
            "position_hash": hs,
            "n_games": [node_n[h] for h in hs],
            "score_sum": [node_s[h] for h in hs],
            "n_faithful": [node_fn.get(h, 0) for h in hs],
            "score_sum_faithful": [node_fs.get(h, 0.0) for h in hs],
            "value": [values.get(h) for h in hs],
        }).write_parquet(out, compression="zstd")
        print(f"\nwrote {len(hs):,} node rows -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
