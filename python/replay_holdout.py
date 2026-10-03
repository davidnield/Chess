"""Replay held-out games against one or more repertoires and measure what happened.

Every selection decision this project has made -- the crush weight, cover-weight,
memo-leave-cost, reply-shrink, all three sweep rounds -- was judged on in-sample
diagnostics computed from the same pool the book was fitted to, plus face
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

That conditioning is not a defect to be corrected, but it IS a bias to state, and
it bites hardest when COMPARING books: a book prescribing rare moves selects
unusual players, so its realized score reflects who plays it as much as how good
it is. Measured on 2026-01 Blitz, a greedy-winrate book scored +0.056 over the
pool on 267 games with a mean exit ply of 0.11 -- an artifact of picking 1.Nf3 at
the root, not a finding. `faithful_pct` (coverage) has no such problem and is the
comparable number; realized score across DIFFERENT books needs a selection-robust
statistic before it means anything.

Four ways a walk ends, and they mean completely different things:

  our_deviation    our side played something other than the book move. A SAMPLING
                   exit -- says nothing about book quality, it is just where this
                   game stopped being evidence. Should dominate the counts.
  opp_out_of_book  the opponent played into a position the book does not contain.
                   A real COVERAGE exit: prep ran out against a legal move
                   someone actually played.
  book_end         our turn, position is in the book, but the book has no move
                   here -- a leaf. A BUDGET exit: we chose to stop preparing.
  game_end         the game itself ended AT a position the book still covers.
                   A game whose last move left the book is a coverage or budget
                   exit that happens to be terminal, not a game_end -- see
                   walk_books' tail. Which of the two it was is a fact about the
                   book; the game also ending there is recorded separately as
                   the termination class.

Conflating the last three is precisely what --reply-shrink's help text warns
about, so they are counted separately and never summed into one "coverage".

game_end IS PART SIGNAL AND PART CLOCK -- SPLIT IT, DO NOT DROP IT
------------------------------------------------------------------
A first pass at the sweep read treated the whole game_end population as
non-evidence and excluded it from the realized-score comparison. That is wrong
and it biases against exactly the books this project is trying to build.
Resigning or being mated inside the book is a real chess result (forcing it
early was the entire point of the since-retired crush term).

Measured on 7.56M 2025 games at >=1800, with no book involved:

    ply 1-2    53.1% ended on the clock, mean score 0.3576
    ply 3-4    29.6%                                0.5535
    ply 5-8    17.4%                                0.5398
    ply 9-16    9.3%                                0.5423

Only the first bucket is majority-clock; from ply 3 on the population is
resignations and mates scoring at or above the 0.5188 pool. But ply 1-2 is 0.14%
of all games and ~58% of game_end exits, because a game can only end in-book if
it ends early -- and its sign is fixed by PARITY, not play: an abandonment
before White's first move is a White loss and a Black win regardless of the
position. That is why the same games give White's game_end 0.408 and Black's
0.591, and it is the whole apparent colour gap in the sweep.

Backing it out of baseline_white: 132,397 game_end exits average 0.408, and if
58% sit at ply <= 2 scoring 0.113, the remaining 42% average 0.817 -- the
highest-scoring population in the replay. Dropping game_end deletes it. Measured
on the sweep, c0.4_black (the highest crush setting) beats baseline by +0.028 on
raw delta with its ENTIRE advantage inside game_end (0.680 vs 0.591, half a move
deeper) while its game_end-excluded delta sits BELOW baseline.

So the exits are split by ply and by Lichess's `termination`, and two adjusted
deltas are printed that drop only the shallow clock slice. Neither raw nor
adjusted is a clean ranking statistic on its own -- raw carries the artifact,
whose direction differs by colour -- which is why the split is reported rather
than one number being chosen for the reader.

CALIBRATION IS PER EXIT REASON, AND game_end IS NOT EVIDENCE
-----------------------------------------------------------
Binning every faithful exit into one calibration table measures game
TERMINATION PARITY, not prediction quality. Diagnosed 2026-08-31 on the sweep
holdout, where the single table read as a catastrophic miscalibration -- one bin
holding 84% of the mass, predicted 0.683 against realized 0.307 -- and was
entirely an artifact:

  * 127,899 of the ~128k binned exits were game_end, mean exit ply 3.93;
  * 58% of those ended at ply 2 scoring 0.1125 -- White played the book's first
    move, Black replied, White forfeited on time. The "prediction" is the book's
    value for an ordinary opening position and the "realization" is a walkover;
  * the sign is pure parity. A game ending on an EVEN ply ended on Black's move,
    so White was mated or flagged (realized 0.11-0.40); an ODD ply ended on
    White's move, so White won (realized 1.00 in 8 of 10 odd plies). Even-ply
    forfeits are shallow and land in low-V bins, odd-ply wins run deeper and
    land in high-V bins, which manufactures a clean-looking S-curve out of
    nothing;
  * meanwhile the exits that DO test the book -- opp_out_of_book and the
    coverage flavour of book_end, 83,513 of them at mean ply 13-15 -- were
    silently excluded, because the exit node is by definition not in the book so
    the old code recorded v=None for them.

So: bin by `values[path[-1]]`, the book's value at the last position it still
recognized, which is well defined for EVERY exit; report one table per reason;
and never read game_end as calibration. Its rows are kept, labelled, because
their absence would be just as easy to misread as their old silent inclusion.

MANY BOOKS, ONE REPLAY
----------------------
Pass --repertoire more than once and every book is evaluated in a single pass
over the games. The sweep in task #114 is 4 rules x 2 colours x 4 footprints, and
running those separately would re-parse and re-hash the same games 32 times when
only the dict lookups differ.

The replay advances in LOCKSTEP and stops as soon as every book has exited --
not at max_ply. That matters: games exit at a mean ply under 2, so a shared walk
that always ran to ply 30 would cost ~15x more per game and give back more than
the sharing saved. Books that exit early simply stop being consulted.

OUTPUTS
-------
Per book: exit-reason breakdown, in-book survival curve, faithful share and
score, calibration bins, and optionally a per-node parquet (position_hash,
n_games, score_sum, n_faithful, score_sum_faithful, value) so a later run can
re-bin without replaying.

Node counters track TWO populations. `all` is every game that reached the node.
`faithful` is games that never deviated -- only those are evidence about the
book, because Stage 3's `value` is a prediction conditional on our side playing
best_move thereafter. Binning deviation exits into the calibration drags every
bin toward the pool mean.

Hashing goes through the SAME IncrementalZobrist the extract uses, so node keys
are directly comparable to position_stats and to the repertoire parquet.

Usage:
    .venv/Scripts/python.exe python/replay_holdout.py \\
        --repertoire E:/chess/repertoire/repertoire_pooled_white_sharp.parquet \\
        --repertoire E:/chess/repertoire/baseline_white_popular.parquet \\
        --perspective white --start-year 2025 --end-year 2025 \\
        --out-dir E:/chess/holdout/
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import chess
import polars as pl
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import discover_source_files
from stage1_extract_positions import iter_san_moves
from zobrist import IncrementalZobrist, zobrist_int64

EVENTS = ["Blitz", "Rapid", "Classical"]
SRC_COLUMNS = ["movetext", "white_score", "mean_elo"]
# Read only where the partition has it; older partitions may not. Absent =>
# every exit is classed UNKNOWN and the adjusted deltas are suppressed rather
# than silently computed off a wrong denominator.
TERM_COLUMN = "termination"
READ_BATCH_GAMES = 50_000

# How the GAME ended. Only meaningful for a game_end exit, where the exit IS the
# game ending; for a coverage or budget exit the game ran on past us and this
# describes something later, so it is reported but never used to adjust.
CLOCK, NORMAL, OTHER, UNKNOWN = "clock", "normal", "other", "unknown"
TERM_CLASSES = (NORMAL, CLOCK, OTHER, UNKNOWN)
# Lichess writes "Time forfeit" for a flag and "Normal" for resignation, mate,
# stalemate and agreed draws -- it does NOT separate resignation from mate.
TERM_MAP = {"Time forfeit": CLOCK, "Normal": NORMAL}
# The ply at or below which a game_end exit stops being about the opening.
# Measured on 7.56M 2025 games: 53.1% of ply 1-2 endings are clock forfeits
# (against 9-17% at ply 5-16), and their sign is fixed by parity rather than by
# play -- an abandonment before White's first move is a White loss and a Black
# win whatever the position. From ply 3 on the population is dominated by
# resignations and mates scoring at or above pool, which are real results and
# are exactly what a sharp repertoire is built to force.
ABANDON_PLY = 2

DEVIATION, OUT_OF_BOOK, BOOK_END, GAME_END, PARSE_ERROR = (
    "our_deviation", "opp_out_of_book", "book_end", "game_end", "parse_error")
REASONS = (DEVIATION, OUT_OF_BOOK, BOOK_END, GAME_END, PARSE_ERROR)
FAITHFUL = (OUT_OF_BOOK, BOOK_END, GAME_END)
# Printed next to each calibration table so a reader cannot pick up the one that
# is not evidence. See the CALIBRATION section of the module docstring.
CALIB_NOTE = {
    OUT_OF_BOOK: "  [COVERAGE exit: prep ran out]",
    BOOK_END: "  [BUDGET exit: we chose to stop]",
    GAME_END: "  [NOT EVIDENCE: the game ended, so this bins termination "
              "parity -- even ply = we were mated/flagged, odd ply = we won]",
}
# Expected-score outcomes are ~50/45/5 win/loss/draw at this band, so SD ~ 0.49.
# Used only to print an SE next to each mean, because the first run of this
# reported a +0.0065 difference whose standard error was 0.0081.
SCORE_SD = 0.49


class Book:
    """One repertoire plus its accumulated counters."""

    def __init__(self, path: Path, perspective: str):
        self.path = path
        self.label = path.stem
        df = pl.read_parquet(path, columns=["position_hash", "side_to_move",
                                            "value", "best_move"])
        self.n_rows = df.height
        # side_to_move is spelled out ("white"/"black") in the repertoire
        # parquet, NOT "w"/"b". Guessing wrong yields a book with zero playable
        # moves and every game exiting at ply 0, which reads as a coverage
        # collapse rather than a bug.
        self.values: dict[int, float] = {}
        self.moves: dict[int, str] = {}
        for h, stm, v, mv in df.iter_rows():
            if v is not None:
                self.values[h] = v if perspective == "white" else 1.0 - v
            if stm == perspective and mv:
                self.moves[h] = mv
        self.node_n: dict[int, int] = {}
        self.node_s: dict[int, float] = {}
        self.node_fn: dict[int, int] = {}
        self.node_fs: dict[int, float] = {}
        self.reasons: Counter = Counter()
        self.ply_hist: dict[str, Counter] = defaultdict(Counter)
        self.score_sum: dict[str, float] = defaultdict(float)
        # (predicted V at the last IN-BOOK node, realized score, exit reason).
        # Keyed on path[-1], not on the exit node: a coverage exit leaves the
        # book by definition, so the exit node has no value to bin.
        self.depth_v: list[tuple[float, float, str]] = []
        # (reason, shallow?, termination class) -> [games, score sum]. Splits
        # every faithful exit finely enough to separate the ply<=2 clock slice
        # -- which is parity, not play -- from the resignations and mates, which
        # are the crush payoff. Excluding all of game_end deletes the second
        # along with the first and penalises the sharpest books hardest.
        self.exit_split: dict[tuple[str, bool, str], list] = defaultdict(
            lambda: [0, 0.0])


def _exit_reason(ply: int, our_turn_now: bool) -> str:
    """Label a missing node by WHOSE MOVE BROUGHT US HERE -- always the opposite
    side from whose turn it now is. Reading it off the turn directly inverts the
    two labels, which does not crash and produces plausible counts."""
    if ply == 0:
        return BOOK_END            # nobody has moved; a missing root = no book
    return OUT_OF_BOOK if our_turn_now else BOOK_END


def walk_books(movetext: str, perspective: str, books: list[Book],
               board: chess.Board, hasher: IncrementalZobrist, max_ply: int
               ) -> list[tuple[str, int, float | None, list[int]]]:
    """Replay one game once, evaluating every book in lockstep.

    Returns per book (reason, ply, predicted V at the exit node, path). `ply` is
    the number of plies successfully PLAYED inside that book, so the departing
    move is included in the count.
    """
    board.reset()
    hasher.reset(board)
    our_white = perspective == "white"
    n = len(books)
    out: list[tuple[str, int, float | None, list[int]] | None] = [None] * n
    paths: list[list[int]] = [[] for _ in range(n)]
    alive = list(range(n))
    h = zobrist_int64(board)
    ply = 0

    try:
        for san in iter_san_moves(movetext):
            still: list[int] = []
            us_to_move = (board.turn == chess.WHITE) == our_white
            for i in alive:
                b = books[i]
                if ply >= max_ply:
                    out[i] = (BOOK_END, ply, b.values.get(h), paths[i])
                    continue
                if h not in b.values:
                    out[i] = (_exit_reason(ply, us_to_move), ply, None, paths[i])
                    continue
                paths[i].append(h)
                if us_to_move:
                    mv = b.moves.get(h)
                    if not mv:
                        out[i] = (BOOK_END, ply, b.values.get(h), paths[i])
                        continue
                    if mv != san:
                        out[i] = (DEVIATION, ply, b.values.get(h), paths[i])
                        continue
                still.append(i)
            alive = still
            if not alive:
                break
            mv_obj = board.parse_san(san)
            hasher.push_move(board, mv_obj)      # pushes onto `board` itself
            h = hasher.current(board)
            ply += 1
        # The GAME ran out of moves. Membership at this final position was never
        # tested: the in-loop check runs at the top of the iteration that would
        # consume the NEXT token, and there is no next token. Assigning game_end
        # unconditionally therefore labels a book that had ALREADY ended as "the
        # game ended inside the book", with a null exit value because the
        # position is absent. `e4 e5` reports game_end where `e4 e5 Nf3` reports
        # opp_out_of_book at the SAME position -- the label depended on whether
        # the game happened to continue.
        #
        # It runs the wrong way for this project specifically: game_end is read
        # as the crush payoff (the ply>2 remainder is the best-scoring population
        # in the replay), so contaminating it with coverage exits inflates
        # exactly the number the sharp recipe is judged on.
        #
        # Apply the two tests the loop would have applied, in the same order, so
        # game_end means what it claims: the game ended at a position the book
        # still covers. The termination class is recorded separately for every
        # exit, so nothing is lost by narrowing the label.
        us_now = (board.turn == chess.WHITE) == our_white
        for i in alive:
            b = books[i]
            if ply >= max_ply:
                out[i] = (BOOK_END, ply, b.values.get(h), paths[i])
                continue
            if h not in b.values:
                out[i] = (_exit_reason(ply, us_now), ply, None, paths[i])
                continue
            paths[i].append(h)
            out[i] = (GAME_END, ply, b.values.get(h), paths[i])
    except (ValueError, AssertionError):
        for i in alive:
            out[i] = (PARSE_ERROR, ply, None, paths[i])
    return [o if o is not None else (PARSE_ERROR, 0, None, []) for o in out]


def walk_game(movetext: str, perspective: str, moves: dict, values: dict,
              board: chess.Board, hasher: IncrementalZobrist, max_ply: int,
              path: list[int]) -> tuple[str, int, float | None]:
    """Single-book wrapper, kept so callers and tests read naturally."""
    b = Book.__new__(Book)
    b.moves, b.values = moves, values
    r, ply, v, p = walk_books(movetext, perspective, [b], board, hasher, max_ply)[0]
    path.clear()
    path.extend(p)
    return r, ply, v


def _report_exit_split(b: Book, base: float, nf: int, fs: float) -> None:
    """The game_end breakdown, and the deltas that drop only the noise.

    Two cuts are printed because the choice is a judgement, not a fact:
      adj(clock)  drops ply<=2 game_end exits that ended ON THE CLOCK -- the
                  narrowest defensible cut, removing only abandonments.
      adj(all<=2) drops every ply<=2 game_end exit. A "Normal" termination two
                  plies in is a resignation nobody plays out, so it is arguably
                  the same artifact wearing a different label.
    Both KEEP the deeper resignations and mates. Dropping game_end wholesale
    would delete the crush payoff: measured on the sweep, the ply>2 remainder
    scores ~0.82, the best population in the replay.
    """
    ge = {(sh, how): v for (r, sh, how), v in b.exit_split.items()
          if r == GAME_END}
    if not ge:
        return
    tot = sum(v[0] for v in ge.values())
    print(f"\ngame_end breakdown ({tot:,} exits) -- is it the opening, or the clock?")
    print(f"  {'segment':<22}{'games':>10}{'share':>9}{'mean score':>12}")
    for sh, lbl in ((True, f"ply <= {ABANDON_PLY}"), (False, f"ply > {ABANDON_PLY}")):
        for how in TERM_CLASSES:
            n, s = ge.get((sh, how), [0, 0.0])
            if not n:
                continue
            print(f"  {lbl + ' / ' + how:<22}{n:>10,}{100*n/tot:>8.1f}%"
                  f"{s/n:>12.4f}")

    unknown = sum(n for (_sh, how), (n, _s) in ge.items() if how == UNKNOWN)
    if unknown:
        print(f"  [{unknown:,} exits have no termination column; "
              f"adjusted deltas suppressed]")
        return

    f_n = sum(v[0] for v in b.exit_split.values())
    f_s = sum(v[1] for v in b.exit_split.values())
    for lbl, drop in (
            ("adj(clock)",
             [(sh, how) for (sh, how) in ge if sh and how == CLOCK]),
            ("adj(all<=2)", [(sh, how) for (sh, how) in ge if sh])):
        dn = sum(ge[k][0] for k in drop)
        ds = sum(ge[k][1] for k in drop)
        n, s = f_n - dn, f_s - ds
        if n <= 0:
            continue
        m = s / n
        se = SCORE_SD / (n ** 0.5)
        print(f"  {lbl:<12} drop {dn:>8,} -> mean {m:.4f} +/- {se:.4f} "
              f"vs {base:.4f} pool -> {m-base:+.4f}"
              f"   (raw {fs-base:+.4f} on {nf:,})")


def report(b: Book, n_games: int, max_ply: int) -> None:
    print(f"\n{'=' * 70}\n{b.label}   ({b.n_rows:,} rows -> {len(b.values):,} "
          f"valued nodes, {len(b.moves):,} our-turn moves)\n{'=' * 70}")
    print("exit reason           games      share   mean score   mean exit ply")
    for r in REASONS:
        n = b.reasons[r]
        if not n:
            continue
        plies = b.ply_hist[r]
        mp = sum(p * c for p, c in plies.items()) / max(sum(plies.values()), 1)
        print(f"  {r:<18} {n:>9,}  {100*n/max(n_games,1):>7.2f}%  "
              f"{b.score_sum[r]/n:>10.4f}  {mp:>13.2f}")

    surv = Counter()
    for r in REASONS:
        for p, c in b.ply_hist[r].items():
            surv[p] += c
    print("\nin-book survival")
    cum = n_games
    for p in range(0, min(max_ply, 20) + 1):
        if p:
            cum -= surv[p - 1]
        if p in (0, 2, 4, 6, 8, 10, 12, 16, 20):
            print(f"  past ply {p:>2}: {cum:>9,}  ({100*cum/max(n_games,1):>6.2f}%)")

    nf = sum(b.reasons[r] for r in FAITHFUL)
    print(f"\nbook-faithful games (never deviated): {nf:,} "
          f"({100*nf/max(n_games,1):.3f}% of replayed)")
    if nf:
        fs = sum(b.score_sum[r] for r in FAITHFUL) / nf
        base = sum(b.score_sum[r] for r in REASONS) / max(n_games, 1)
        se = SCORE_SD / (nf ** 0.5)
        print(f"  mean score {fs:.4f} +/- {se:.4f} (1 SE) vs {base:.4f} pool "
              f"-> {fs-base:+.4f}"
              f"{'  [WITHIN NOISE]' if abs(fs-base) < se else ''}")
        print("  NB: realized score across DIFFERENT books is selection-"
              "confounded; compare on faithful% / survival.")
        _report_exit_split(b, base, nf, fs)

    if b.depth_v:
        by_reason: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for v, r, reason in b.depth_v:
            by_reason[reason].append((v, r))
        for reason in FAITHFUL:
            rows_all = by_reason.get(reason)
            if not rows_all:
                continue
            note = CALIB_NOTE.get(reason, "")
            print(f"\ncalibration at the last in-book node -- {reason} exits "
                  f"only ({len(rows_all):,}){note}")
            print("  V bin          n      predicted   realized   gap     "
                  "1 SE")
            bins: dict[int, list[tuple[float, float]]] = defaultdict(list)
            for v, r in rows_all:
                bins[min(int(v * 20), 19)].append((v, r))
            for k in sorted(bins):
                rows = bins[k]
                if len(rows) < 50:
                    continue
                pv = sum(x[0] for x in rows) / len(rows)
                rv = sum(x[1] for x in rows) / len(rows)
                # Bins here run to a few dozen rows, where a +0.12 gap is under
                # two SEs. Printing the gap without it invites the same
                # over-reading the old single table produced.
                se = SCORE_SD / (len(rows) ** 0.5)
                print(f"  {k/20:.2f}-{(k+1)/20:.2f}  {len(rows):>9,}  "
                      f"{pv:>10.4f}  {rv:>9.4f}  {rv-pv:>+6.4f}  {se:>6.4f}"
                      f"{'' if abs(rv-pv) >= se else '  [noise]'}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repertoire", action="append", required=True,
                    help="Repeatable: every book is evaluated in one pass.")
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--start-year", type=int, default=2025)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--months", type=str, default=None)
    ap.add_argument("--events", type=str, default=",".join(EVENTS))
    ap.add_argument("--min-elo", type=int, default=1800)
    ap.add_argument("--limit-games", type=int, default=0)
    ap.add_argument("--max-ply", type=int, default=30)
    ap.add_argument("--out-dir", default=None,
                    help="Write <label>.nodes.parquet per book.")
    a = ap.parse_args()

    months = [int(m) for m in a.months.split(",")] if a.months else None
    books = [Book(Path(p), a.perspective) for p in a.repertoire]
    for b in books:
        print(f"book: {b.label}  {b.n_rows:,} rows -> {len(b.moves):,} our-turn moves")

    files = discover_source_files(a.start_year, a.end_year, months,
                                  a.events.split(","))
    print(f"holdout: {len(files):,} source files, {a.start_year}-{a.end_year}, "
          f"mean_elo>={a.min_elo}, {len(books)} book(s) in one pass")
    if not files:
        print("FATAL: no source files matched.")
        return 1

    board, hasher = chess.Board(), IncrementalZobrist(chess.Board())
    n_games = 0
    n_no_term = 0
    t0 = time.time()
    for src, _y, _m, _ev in files:
        pf = pq.ParquetFile(src)
        has_term = TERM_COLUMN in pf.schema_arrow.names
        if not has_term:
            n_no_term += 1
        cols = SRC_COLUMNS + ([TERM_COLUMN] if has_term else [])
        for batch in pf.iter_batches(batch_size=READ_BATCH_GAMES, columns=cols):
            df = pl.from_arrow(batch).filter(
                (pl.col("mean_elo") >= a.min_elo)
                & pl.col("white_score").is_not_null())
            terms = (df[TERM_COLUMN].to_list() if has_term
                     else [None] * df.height)
            for mt, ws, tm in zip(df["movetext"].to_list(),
                                  df["white_score"].to_list(), terms):
                res = walk_books(mt or "", a.perspective, books, board, hasher,
                                 a.max_ply)
                realized = float(ws) if a.perspective == "white" else 1.0 - float(ws)
                how = UNKNOWN if not has_term else TERM_MAP.get(tm, OTHER)
                for b, (reason, ply, _v, path) in zip(books, res):
                    b.reasons[reason] += 1
                    b.ply_hist[reason][ply] += 1
                    b.score_sum[reason] += realized
                    if reason in FAITHFUL:
                        cell = b.exit_split[(reason, ply <= ABANDON_PLY, how)]
                        cell[0] += 1
                        cell[1] += realized
                    faithful = reason in FAITHFUL
                    for h in path:
                        b.node_n[h] = b.node_n.get(h, 0) + 1
                        b.node_s[h] = b.node_s.get(h, 0.0) + realized
                        if faithful:
                            b.node_fn[h] = b.node_fn.get(h, 0) + 1
                            b.node_fs[h] = b.node_fs.get(h, 0.0) + realized
                    # walk_books' `v` is the value at the EXIT node, which a
                    # coverage exit has left -- it is None for exactly the two
                    # reasons that test the book. path[-1] is the last node the
                    # book still recognized and is defined for every exit.
                    if faithful and path:
                        vp = b.values.get(path[-1])
                        if vp is not None:
                            b.depth_v.append((vp, realized, reason))
                n_games += 1
            if a.limit_games and n_games >= a.limit_games:
                break
        if a.limit_games and n_games >= a.limit_games:
            break

    dt = time.time() - t0
    print(f"\nreplayed {n_games:,} games in {dt:,.0f}s "
          f"({n_games/max(dt,1e-9):,.0f}/s) against {len(books)} book(s)")
    if n_no_term:
        print(f"NB: {n_no_term} of {len(files)} source files carry no "
              f"'{TERM_COLUMN}' column; their exits are classed {UNKNOWN} and "
              f"the adjusted deltas are suppressed.")
    for b in books:
        report(b, n_games, a.max_ply)

    if a.out_dir:
        d = Path(a.out_dir)
        d.mkdir(parents=True, exist_ok=True)
        for b in books:
            hs = sorted(b.node_n)
            pl.DataFrame({
                "position_hash": hs,
                "n_games": [b.node_n[h] for h in hs],
                "score_sum": [b.node_s[h] for h in hs],
                "n_faithful": [b.node_fn.get(h, 0) for h in hs],
                "score_sum_faithful": [b.node_fs.get(h, 0.0) for h in hs],
                "value": [b.values.get(h) for h in hs],
            }).write_parquet(d / f"{b.label}.nodes.parquet", compression="zstd")
        print(f"\nwrote per-node parquet for {len(books)} book(s) -> {d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
