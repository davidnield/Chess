# Chess opening repertoire builder

This builds a chess opening repertoire out of Lichess game data. It ingests the
monthly game dumps (4.8 TB of Lichess pgn data), prunes and compresses that games
data to 1.03 TB, then extracts every position-move edge from 1.33 billion rated games
in Blitz, Rapid, and Classical played between players with an average rating of at
least 1800,
aggregates those into per-position statistics, then runs backwards induction over
the position graph to pick a move at every position you can reach.

The goal is to empirically determine the best opening repertoire for ≥1800 Blitz, Rapid
and Classical: the moves with the best expected score, blending how the games
actually went with Stockfish's evaluation, while staying sound enough not to lose
against best play. Stockfish pruning throws out
lines that score well only because opponents keep falling for them but
otherwise are losing or throw away an advantage.

The current build covers 1.33 billion games, which give 31.27M position-move edges
across 17.79M distinct positions, checked against an eval database of 5.93B
positions (every position of the explorer book that Lichess's cloud or server
analysis ever evaluated).

That full book has a move at millions of positions, so the books you actually
learn are cut to a memorisation budget of a couple of thousand moves per colour,
checked against held-out games and published opening courses, and drilled in a
local trainer app.

## How it works

For every position in the data, we know which moves people played, how often,
and how those games turned out. A repertoire picks one move per position.

You can't just take whichever move has the best winning record. A move can look
great because the replies people happen to pick against it are bad, and it falls
apart the moment somebody finds the good reply. And a move's real worth depends
on what happens later — which depends on what *you* play later. You can't score
a move until you've filtered out all of the game results from the moves you
*won't* play after.

So the work runs backwards: score the deepest positions first, then the ones
before them, all the way back to the starting position.

### Two rules, depending on whose turn it is

Chess scores run from 0 to 1 (win = 1, draw = 0.5, loss = 0).

**Your move:** pick the best option. You choose, so the position is worth
whatever your best option is worth.

**Their move:** average over what they actually play, weighted by frequency.
You don't choose, and the repertoire is for playing people, not a perfect
engine, so it doesn't assume they find the strongest reply.

A small tree with made-up numbers, built from the bottom up:

```
                        start position
                   (our move — take the best)
                             0.53
                ┌─────────────┴─────────────┐
             1.e4                         1.d4
             0.53                         0.51
                │
      (their move — average by how often)
        0.60 × 0.55  +  0.40 × 0.50  =  0.53
        ┌────────────────┴────────────────┐
    1...e5  (60% of games)           1...c5  (40%)
        0.55                             0.50
        │
 (our move — take the best)
        0.55
   ┌────┴────┐
 2.Nf3     2.Bc4
 0.55       0.49
```

Read it upward. 2.Nf3 beats 2.Bc4, so the position after 1...e5 is worth 0.55.
Black answers 1.e4 with 1...e5 six times in ten and 1...c5 four, so 1.e4 is worth
0.60 × 0.55 + 0.40 × 0.50 = 0.53. That beats 1.d4, so 1.e4 becomes the book move.

It's worth noting that the opponent move probability weight means that
a line scoring really well but almost never comes up barely moves
the number above it. Preparation you rarely get to use isn't worth much.

### Where the leaf numbers come from

The metric being optimized is based on expected score, which in this repo can
come from either the actual Lichess score percentages, the Stockfish evaluation,
or both, with Stockfish evaluations converted to expected score from 0 to 1 using
Lichess's own model that it uses for computing Accuracy, details here: https://lichess.org/page/accuracy#first-compute-win
e.g. Dead level is 0.50, a pawn up is about 0.59, three pawns up about 0.75,
and the weight between the actual score percentages and Stockfish evaluations
controlled by the --eval-weight CLI tag, from 0 to 1, with 1 being based only
on expected Stockfish evaluation, and 0 being based only on expected score
based on actual win/draw/loss rates.

In order to not be led astray by small sample sizes (e.g. a move that's been played
once but the player won causing the win rate to be 100% or 1.0 for the move),
every move starts with 500 imaginary games scored at the Lichess average score,
which is 0.52 for White so that real games only gradually outweigh them. E.g. a move
that's been played 500 times with an average score of 0.6 would get a value of 
[(500 x 0.52) + (500 x 0.6)] / 1000 = 0.56. And some games record no reply at all
(resignation, mate, or depth limit). Ignoring them would quietly drop games where
the opponent resigned — which is the opposite of noise. Those games are kept and
added back into the score calculation at each step (effectively treating resignations
as opponent moves)

Note: moves on our side also get pruned by two Stockfish evaluation criteria, that
prune moves that would result in a position that Stockfish evaluates as either
1. being equal to or worse than a given evaluation (the absolute eval gate,
I default to -1.0 for the side to move, keeping stuff like the King's Gambit, but
dropping objectively dubious stuff like the Alien Gambit)
2. losing a given amount or more of centipawns of evaluation (the relative gate,
I default to -1.0 for the side to move, this is so that we aren't choosing moves
that drop us from objectively winning to objectively dead equal (which wouldn't trip
the absolute eval gate pruning) simply because opponents have typically misplayed the position,
I'd rather play a move keeps a position objectively winning even if human opponents
usually don't find the right moves afterwards).

## Fitting the book in your head

The method above picks a move for every reachable position — about 7 million per
colour. Nobody is learning 7 million moves.

The simple fix is to build the whole book and cut it down: keep the lines you're
most likely to reach, drop the rest. That's the baseline this repo measures
against (`build_baseline_books.py`). But it finds the best 20 moves *of a book
designed for someone who knows all 7 million*. If you only have room for 20, you
might want different openings entirely — ones that pay off right away, not the
first 20 moves of a system whose point arrives on move 15.

Example. Two first moves:

```
  Solid   →  worth 0.54, and knowing more moves barely helps
  Trap    →  worth 0.47 if you stop there (and play the remainder of the game playing each subsequent move as often as the average player in your color does)
             worth 0.72 if you also know the next three moves
```

With room for one move, Solid is right: 0.54 beats 0.47. With room for four, Trap
is right: 0.72 beats 0.54. Cutting the four-move book down to one leaves you
playing into the trap with none of the follow-up, which is the worst of the three
outcomes at 0.47. The budget has to be part of the choice, not something applied
afterwards.

### One score becomes a table

Instead of "what is this position worth?", ask "what is it worth if I can learn
N more moves below it?" — for every N. Each position now carries a small table
instead of a single number, still built from the bottom up.

**Your move:** stop here and wing it, or spend one from the budget to learn a
move and hand what's left to the position it leads to. Pick whichever is worth
more. Budget zero always stops.

**Their move:** split the budget across their replies. Not proportional to
frequency — proportional to frequency × improvement-per-move-learned. A rare
reply where preparation helps a lot gets budget before a common reply where it
barely matters:


```
  common reply   0.60 × 0.03 = 0.018
  rarer reply    0.25 × 0.08 = 0.020   ← the budget goes here
```

The rarer branch wins despite coming up less than half as often. In practice
these calls are decided by margins under one percent.

### What one move costs

One position where you have to remember what to play — one flashcard. Opponent
moves are free (you aren't memorizing anything there).

One known overcount: if the same position can be reached by two move orders, the
planner charges for it twice even though you'd only learn it once. Books report
both the charged count and the true distinct count, and there's a flag
(`--match-distinct`) that keeps spending until the distinct count hits the target.

The output is a value-per-budget table for each colour, so a 20-move book and a
1000-move book are genuinely different openings, not the same tree cut at
different depths.

### How the budget books are built

`build_budget_books.py` turns the full book into one book per `--budget`. Its
header docstring has the CLI; `budget_core.py` has the algorithms and the
approximations each one makes. The choices that matter:

- **Engine.** `--method dp` is the value-per-budget tables above. `--method price`
  puts a price on each move learned and searches for the price that lands the book
  on the budget. It needs no tables, and it counts a transposed position once.
  `--price-dag-mode polish` then improves that book by local moves (book a line,
  switch a move, trade cheap leaves for a better line) without going over the budget.
- **After the book ends.** By default you play on like the pool you were fitted to.
  `--stop-model weak` assumes you play one move like a weaker population and then
  values the position by engine eval (`weak_stop.py`). `--weak-eval-curve` converts
  that eval with curves fitted per rating pairing (`fit_eval_curve.py`).
  `--stop-model empirical` plays the line out with real moves (`empirical_stop.py`).
- **Soundness.** `--max-eval-loss CP` books only moves within CP centipawns of the
  engine's best, on top of the gates above.
- **Rebuilds.** `--incumbent BOOK` keeps the move you already learned at each
  position unless another beats it by `--stick-margin`, and the meta records the
  churn.
- **Mix and match.** `--allowed-moves` limits the book to moves that opening
  courses recommend.

## Measuring a book

A book scores well on the games it was fitted to almost by construction, so the
comparisons that count are out of sample:

- `replay_holdout.py` replays held-out games against one or more books and reports
  where each walk left the book and why.
- `explorer_policy_value.py` reads a book's policy value straight from a month of
  held-out games already aggregated into an explorer month. That takes minutes,
  where a replay takes hours, so it can score hundreds of books at once.
- `build_baseline_books.py` builds the simple-rule books (most popular move, best
  engine eval, best win rate, reach truncation) that a result needs as its floor.
- `score_repertoire.py` and `plan_consistency_report.py` are in-sample scorecards:
  expected score and soundness, and how consistent the plans are across variations.

**Opening-course benchmarks** (`course_*.py`) put published opening courses on
the same footing. `course_ingest.py` reads the course PGNs into position-keyed
tables, and `course_books.py` turns each course into a book in this repo's format.
`course_dedup.py` groups duplicate editions, and `course_report.py` and
`explorer_policy_value.py --courses` compare the course books with ours.
`course_comments.py` finds the course prose that covers a book's positions.
Course text is copyrighted and never leaves the owner's machine. Only the
position-keyed tables are derived from it, and they stay local too.

## The pipeline

```
Lichess monthly PGN dumps
   │  process_pgn_parquets.py        clean + compress to Hive-partitioned parquet
   ▼
source parquets (year=/month=/event=)
   ├─ build_pooled_stats.py          Python extract + two-phase merge
   │
   └─ explorer-extract month         (Rust) a month of games -> 512 hash buckets
      explorer-extract merge         every month -> the all-time explorer book,
         │                           split by speed and rating band
         ├─ pool_from_book.py        any slice set, ply cap and floor -> a pool
         └─ explorer-extract evals   Lichess cloud + fishnet evals -> eval DB
               eval_arrays.py        -> memory-mapped arrays every stage reads
   ▼
position_stats_*.parquet       per-edge win/draw/loss counts
position_stats_aux_*.parquet   per-position: the games those edges can't see
   │  build_sharp_reps.py            the locked recipe: one engine run per colour
   │    └─ stage3_backwards_induction.py
   ▼
repertoire_{white,black}_sharp.parquet     a move at every reachable position
   │  build_budget_books.py          budget_core.py: the books you learn
   ▼
budget books, N moves per colour
   ├─ replay_holdout.py, explorer_policy_value.py, score_repertoire.py   measure
   ├─ annotate/                      plan notes for each position you drill
   └─ trainer_app/                   local web app: drills, explorer, deviations
```

## Repo layout

```
python/
  zobrist.py                       the position hash every stage keys on
  process_pgn_parquets.py          ingest + compression (recipe v3)
  build_pooled_stats.py            Python extract + aggregate
  pool_from_book.py                the pool + aux sidecar from the explorer book
  pool_from_book_gates.py          ... and its acceptance gates
  backfill_epd.py, bucket_month.py the Python producers of explorer months
  ply_cap.py                       a lower ply cap from a ply-keyed month
  verify_book.py, verify_evals.py  independent checks of the Rust book and eval DB
  compare_explorer_outputs.py      holds two producers' outputs equal
  eval_arrays.py                   the eval DB as mmap'd arrays every consumer shares
  eval_arrays_build.py             ... built from the eval DB, plus its checkmates
  stage3_backwards_induction.py    the valuation engine
  build_sharp_reps.py              the locked recipe (one Stage-3 run per colour)
  budget_core.py                   budget books: DP, price search, polish
  budget_compiled.py               ... its price search as array sweeps
  build_budget_books.py            ... and its CLI
  weak_stop.py, empirical_stop.py  out-of-book value models for the budget books
  build_weak_edges.py              weak-population move counts they read
  fit_eval_curve.py                eval-to-score curves by rating pairing
  build_baseline_books.py          simple-rule baseline books
  replay_holdout.py                out-of-sample replay of held-out games
  explorer_policy_value.py         holdout policy value of many books at once
  repertoire_stats.py              size and shape statistics for any book
  course_*.py                      opening-course benchmarks
  score_repertoire.py              scorecard
  plan_consistency_report.py       idea-consistency measurement
  repertoire_explorer.py           browsing UI
  analyze_coverage.py              forward-flow coverage walk
  export_chessbook.py              export as variation-tree PGN
  duck_compat.py                   the one place DuckDB results become frames
  status.py, notify.py             run status at a glance; email on completion
  stage1..stage4_*.py              legacy per-(event, elo band) sliced path
  annotate/                        plan-note annotations for the trainer
  trainer_app/                     local web app (its own README)
  run_tests.py, _test_*.py         the test suite
rust/explorer_extract/             the Rust explorer extract (its own README)
docs/                              build reports and handoff briefs
archive/                           the original R implementation this replaced
CLAUDE.md                          technical notes: invariants, contracts, and
                                   known failure modes
```

The data isn't in this repo. Inputs and outputs live on local drives and run to
several terabytes; paths are constants at the top of each script, and CLAUDE.md
has a table of them. Nothing here will run end to end without that data, but each
script's header docstring documents its own CLI.

## Setup

Python 3.11, managed with [uv](https://docs.astral.sh/uv/):

```bash
uv venv
uv pip install -r requirements.txt
```

The Rust explorer extract builds with `cargo build --release --locked` in
`rust/explorer_extract/`. Its README has the pinned toolchain, the CPU it needs,
and its subcommands.

## Running it

Each script's header docstring has its current CLI. Check there before running.

```powershell
# Aggregate stats from the source parquets (both phases are resumable)
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year 2019 --end-year 2025 --phase extract
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year 2019 --end-year 2025 --phase merge --tmp-dir D:\chess_duckdb_tmp

# ...or build the pool + aux sidecar from the explorer book (any slice set, ply cap, floor)
.venv\Scripts\python.exe python\pool_from_book.py --events Blitz Rapid Classical --elo-bands 2000 2200 2500 --max-ply 20 --min-games 50 --tag ge2000_2013_2026_brc_p20 --work H:\chess\pool_work\ge2000_2013_2026_brc_p20

# Build the canonical White + Black repertoires
.venv\Scripts\python.exe python\build_sharp_reps.py

# Cut one to a memorisation budget
.venv\Scripts\python.exe python\build_budget_books.py --rep <rep> --stats <pool> --perspective white --out-prefix <prefix> --budget 2000 --method price

# Score one / browse one
.venv\Scripts\python.exe python\score_repertoire.py --repertoire <path> --stats <path>
.venv\Scripts\python.exe python\repertoire_explorer.py

# Opening-course benchmarks: PGNs -> tables -> course books -> comparisons
.venv\Scripts\python.exe python\course_ingest.py --phase all
.venv\Scripts\python.exe python\course_books.py
.venv\Scripts\python.exe python\explorer_policy_value.py --explorer <month dir> --courses
```

Long runs are assumed to die partway through. Every stage writes its output
atomically (`.tmp`, then rename), skips work whose output already exists, and
checkpoints the expensive intermediate steps, so restarting picks up where it
stopped.

### Trainer app

`run_trainer.bat` starts a local web app on `http://127.0.0.1:8321` with spaced-
repetition drills over your books, the explorer, and a Lichess import that
shows where your real games left the book. `run_trainer_remote.bat` serves it to
your phone or laptop on the home network as well. See
`python/trainer_app/README.md`.

### Annotations

`python/annotate/` writes a plan note for each position you drill, in four
stages: fact sheets from the book and the pool (what you play next, the replies
that change it, the ideas that recur), a targeted Stockfish pass for the lines a
note quotes, notes generated with headless Claude Code, and a validator that
checks every move and percentage a note cites against the facts.
`annotate.export_book` renders the notes as an HTML book with board diagrams.
Each stage's module docstring has its CLI.

## Tests

```bash
.venv/Scripts/python.exe python/run_tests.py
```

Every `python/_test_*.py` is a standalone pass/fail script, and `run_tests.py`
runs them all and prints a summary. They import the production code instead of
reimplementing it. Several check exact equivalence: the optimised extract has to
produce identical aggregates to the original replay path, the Rust extract has to
match the Python one row for row, and the incremental Zobrist hash is
differentially tested against python-chess over millions of plies. Tests that
need the local datasets skip when those aren't present, and the tests of the
Rust extract skip when they find no built `explorer-extract` (set
`EXPLORER_EXTRACT_EXE`). The crate has its own `cargo test`.

## License

No license has been chosen yet, so default copyright applies.
