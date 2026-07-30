# Chess opening repertoire builder

Builds a chess opening repertoire from empirical evidence rather than theory: it
ingests Lichess's monthly game dumps, extracts every position-move edge from about
a billion games, aggregates them into per-position statistics, then runs backwards
induction over the position graph to pick a move at every position you can reach.

The current product is a **memorization-efficient "sharp" repertoire** for pooled
≥1800 Blitz/Rapid/Classical play: find the moves most likely to reach a winning
position *early*, without booking anything that loses to best play. A Stockfish
eval overlay gates out lines that score well empirically only because opponents
keep falling for them.

Scale, for the current build: **1.03 billion games** → 23.45M position-move edges
→ 13.28M distinct positions, cross-referenced against a **397.6M-position** eval
database.

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
position_stats_*.parquet  +  crush_hist_*.parquet
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

### The interesting part: how a move gets chosen

`stage3_backwards_induction.py` values every position exactly once in topological
order (Zobrist-hashed, so transpositions collapse automatically; residual cycles
from reversible shuffles get Tarjan SCC condensation and a damped fixpoint). At
positions where *you* move, it maximizes a weighted sum of:

- **empirical value** — expected score against the *average* opponent at this
  rating, propagated up the tree, with Beta-Binomial shrinkage at the leaves so
  lucky small samples can't win the argmax;
- **crush** — how often the line reaches a winning position early (mate,
  resignation, or crossing +300cp), discounted by how long it takes;
- **coverage efficiency** — how much of the opponent's traffic stays on prepared
  rails per line you have to memorize;
- **memorization cost** — what you lose if you forget and play the natural move.

…subject to two **refutation gates**: a move is rejected if the engine says the
position it reaches is losing against best play (absolute), or if it concedes too
much versus the best alternative (relative). Finally, a **learnability tiebreak**
collapses near-equal candidates onto whichever idea the rest of the repertoire
already plays most often — so rare sidelines reuse your habits instead of
demanding fresh memorization.

Opponent nodes average over the *empirical* move distribution, not best play. That
is deliberate: this is a repertoire for playing humans, and the gates are what
guard the worst case.

## Repo layout

```
python/
  zobrist.py                     the position hash every stage keys on
  process_pgn_parquets.py        ingest + compression (recipe v3)
  build_pooled_stats.py          canonical extract + aggregate
  build_crush_winpos*.py         "winning position reached early" histogram
  build_{lichess,fishnet}_eval_db.py   Stockfish eval databases
  stage3_backwards_induction.py  the valuation engine (the core of the project)
  build_sharp_reps.py            the locked recipe — two-pass driver
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
CLAUDE.md                        deep technical notes: invariants, contracts,
                                 failure modes, and the painful lessons
```

**The data is not in this repo.** Inputs and outputs live on local drives and run
to several terabytes; paths are constants at the top of each script (see the data
table in `CLAUDE.md`). Nothing here will run end-to-end without them, but every
script's header docstring documents its exact CLI.

## Setup

Python 3.11, managed with [uv](https://docs.astral.sh/uv/):

```bash
uv venv
uv pip install -r requirements.txt
```

## Running it

Every script carries an authoritative header docstring with its exact CLI — read
that rather than trusting remembered flags.

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

Long runs assume they can die: every stage writes atomically (`.tmp` then rename),
skip-gates on its own output, and checkpoints anything expensive, so restarting is
always cheap. That is a load-bearing design property at this scale, not a nicety.

### Trainer app

`run_trainer.bat` starts a local web app on `http://127.0.0.1:8321` with spaced-
repetition drills over the repertoire, the explorer, and a Lichess import that
shows where your real games left the book. See `python/trainer_app/README.md`.

## Tests

```bash
.venv/Scripts/python.exe python/run_tests.py
```

Every `python/_test_*.py` is a standalone pass/fail script; `run_tests.py` runs
them all and prints a summary. They import the production code rather than
reimplementing it, and several assert *exact* equivalence — e.g. the optimized
extract must produce byte-for-byte identical aggregates to the original replay
path, and the incremental Zobrist hash is differentially tested against
python-chess over millions of plies. Tests that need the local datasets skip
cleanly when those aren't present.

## License

No license has been chosen yet, so default copyright applies.
