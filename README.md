# Chess opening repertoire builder

This builds a chess opening repertoire out of Lichess game data. It ingests the
monthly game dumps (4.8 TB of Lichess pgn data), prunes and compresses that games
data to 1.03 TB, then extracts every position-move edge from 1.33 billion rated games
in Blitz, Rapid, and Classical played between players with an average rating of at
least 1800,
aggregates those into per-position statistics, then runs backwards induction over
the position graph to pick a move at every position you can reach.

The goal is to empirically determine the best opening repertoire for ≥1800 Blitz, Rapid
and Classical: moves that tend to reach a winning position quickly, while staying
sound enough not to lose against best play. Stockfish pruning throws out
lines that score well only because opponents keep falling for them but
otherwise are losing or throw away an advantage.

The current build covers 1.33 billion games, which give 31.27M position-move edges
across 17.79M distinct positions, checked against an eval database of 400.0M
positions.

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

It's worth nothing that the opponent move probability weight means that
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
I default to -1.0 for the side to move, this is so that we aren't chosing moves
that drop us from objectively winning to objectively dead equal (which wouldn't trip
the absolute eval gate pruning) simply because opponents have typically misplayed the position,
I'd rather play a move keeps a position objectively winning even if human opponents
usually don't find the right moves afterwards.

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

## How it works

```
Lichess monthly PGN dumps
   │  process_pgn_parquets.py      clean + compress to Hive-partitioned parquet
   ▼
source parquets (year=/month=/event=)
   │  build_pooled_stats.py        --phase extract   filter by elo, replay once,
   │                                                 emit pre-aggregated partials
   │  build_pooled_stats.py        --phase merge     two-phase external GROUP BY
   ▼
position_stats_*.parquet      per-edge win/draw/loss counts
position_stats_aux_*.parquet  per-position: the mass those edges can't see
crush_hist_*.parquet          "winning position reached early", same replay
   │  build_sharp_reps.py          two-pass driver around the engine below
   │    └─ stage3_backwards_induction.py
   ▼
repertoire_{white,black}_sharp.parquet
   │
   ├─ score_repertoire.py          effectiveness / soundness / coverage scorecard
   ├─ plan_consistency_report.py   how habitual are the ideas across variations
   ├─ repertoire_explorer.py       browse the tree
   └─ trainer_app/                 local web app: drills, explorer, deviations
```

## Repo layout

```
python/
  zobrist.py                     the position hash every stage keys on
  process_pgn_parquets.py        ingest + compression (recipe v3)
  build_pooled_stats.py          canonical extract + aggregate
  winpos_fused.py                "winning position reached early", inside that replay
  build_crush_winpos*.py         the SQL definition of that event, kept as the test oracle
  eval_arrays.py                 the eval DB as mmap'd arrays workers can share
  build_{lichess,fishnet}_eval_db.py   Stockfish eval databases
  stage3_backwards_induction.py  the valuation engine
  build_sharp_reps.py            the locked recipe, as a two-pass driver
  build_baseline_books.py        reach-pruned books — the truncation baseline
  budget_core.py                 value-vs-budget curves
  build_budget_books.py          budget-constrained books
  stage1..stage4_*.py            legacy per-(event, elo band) sliced path
  score_repertoire.py            scorecard
  plan_consistency_report.py     idea-consistency measurement
  repertoire_explorer.py         browsing UI
  analyze_coverage.py            forward-flow coverage walk
  export_chessbook.py            export as variation-tree PGN
  annotate/                      engine + LLM line annotations
  trainer_app/                   local web app (FSRS drills, explorer, deviations)
  run_tests.py, _test_*.py       the test suite
archive/                         the original R implementation this replaced
CLAUDE.md                        technical notes: invariants, contracts, and
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

## Running it

Each script's header docstring has its current CLI. Check there before running.

```powershell
# Aggregate stats from the source parquets (both phases are resumable)
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year 2019 --end-year 2025 --phase extract
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year 2019 --end-year 2025 --phase merge --tmp-dir D:\chess_duckdb_tmp

# Build the canonical White + Black repertoires
.venv\Scripts\python.exe python\build_sharp_reps.py

# Score one / browse one
.venv\Scripts\python.exe python\score_repertoire.py --repertoire <path> --stats <path>
.venv\Scripts\python.exe python\repertoire_explorer.py
```

Long runs are assumed to die partway through. Every stage writes its output
atomically (`.tmp`, then rename), skips work whose output already exists, and
checkpoints the expensive intermediate steps, so restarting picks up where it
stopped.

### Trainer app

`run_trainer.bat` starts a local web app on `http://127.0.0.1:8321` with spaced-
repetition drills over the repertoire, the explorer, and a Lichess import that
shows where your real games left the book. See `python/trainer_app/README.md`.

## Tests

```bash
.venv/Scripts/python.exe python/run_tests.py
```

Every `python/_test_*.py` is a standalone pass/fail script, and `run_tests.py`
runs them all and prints a summary. They import the production code instead of
reimplementing it. Several check exact equivalence: the optimised extract has to
produce identical aggregates to the original replay path, and the incremental
Zobrist hash is differentially tested against python-chess over millions of plies.
Tests that need the local datasets skip when those aren't present.

## License

No license has been chosen yet, so default copyright applies.
