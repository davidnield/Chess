# Chess opening repertoire builder

This builds a chess opening repertoire out of Lichess game data. It ingests the
monthly game dumps, extracts every position-move edge from about a billion games,
aggregates those into per-position statistics, then runs backwards induction over
the position graph to pick a move at every position you can reach.

What it currently produces is a sharp repertoire for pooled ≥1800 Blitz, Rapid and
Classical: moves that tend to reach a winning position quickly, while staying
sound enough not to lose against best play. A Stockfish eval overlay throws out
lines that score well only because opponents keep falling for them.

The current build covers 1.03 billion games, which give 23.45M position-move edges
across 13.28M distinct positions, checked against an eval database of 397.6M
positions.

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

### How a move gets chosen

`stage3_backwards_induction.py` values every position once, in topological order.
Positions are keyed by Zobrist hash, so transpositions collapse into a single
node. Cycles created by reversible piece shuffles get Tarjan SCC condensation and
a damped fixpoint.

At positions where you move, it maximises a weighted sum of four terms:

- Empirical value: expected score against the average opponent at this rating,
  propagated up the tree. Leaves get Beta-Binomial shrinkage, so a lucky
  three-game line can't win the argmax.
- Crush: how often the line reaches a winning position early, counting mate,
  resignation, or the eval crossing +300cp, discounted by how long it takes.
- Coverage efficiency: how much opponent traffic stays on prepared lines, per
  position you have to memorise.
- Memorization cost: what you give up when you forget a line and play the natural
  move instead.

Two gates then veto candidates. The absolute gate rejects a move if the engine
says the position it reaches is losing against best play. The relative gate
rejects it if it concedes too much compared with the best alternative. When
several candidates are still close after all that, a tiebreak prefers whichever
idea the rest of the repertoire already plays most often, so rare sidelines reuse
habits you have instead of adding new ones.

Opponent nodes average over the empirical move distribution instead of assuming
best play, because the repertoire is for playing humans. The gates are what cover
the worst case.

## Repo layout

```
python/
  zobrist.py                     the position hash every stage keys on
  process_pgn_parquets.py        ingest + compression (recipe v3)
  build_pooled_stats.py          canonical extract + aggregate
  build_crush_winpos*.py         "winning position reached early" histogram
  build_{lichess,fishnet}_eval_db.py   Stockfish eval databases
  stage3_backwards_induction.py  the valuation engine
  build_sharp_reps.py            the locked recipe, as a two-pass driver
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
