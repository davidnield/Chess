# Opening Trainer

Local web app with three sections: **Explorer** (the full repertoire_explorer
UI), **Trainer** (FSRS spaced-repetition drills over the sharp repertoires),
and **Deviations** (Lichess game import + where-did-I-leave-book review).

## Launch

Double-click `run_trainer.bat` at the repo root (or a Windows shortcut pointing
at it — right-click the .bat → Send to → Desktop). It starts the server on
http://127.0.0.1:8321 and opens the browser. All assets are vendored — no
internet needed except the Lichess import.

## Data layout

```
trainer_data/            ← the unit of portability (everything user-specific)
  trainer.db             FSRS card states, review log, sessions, imported games,
                         deviations, settings (SQLite, WAL)
  pack/                  training pack built from the repertoires:
    {white,black}_tree.parquet     drill-line tree (parent pointers)
    {white,black}_cards.parquet    trainable positions + reach + memo_cost
    dev_lookup_{white,black}.parquet  full-book (hash → best_move) for the scanner
    pack_meta.json       provenance + build params
```

## Building / refreshing the training pack

After any repertoire rebuild (the app shows a staleness banner):

```powershell
cd C:\Users\David\Documents\Chess
$env:PYTHONPATH = "$PWD\python"
.venv\Scripts\python.exe -m trainer_app.build_training_pack
```

Defaults read the canonical rep/stats paths; `--min-reach` (default 0.0002 =
1-in-5000 games) is the training cut-off; `--max-ply 40` the depth stop.

## Forcing a specific line / branch

To train a different move than the repertoire's pick at some position — e.g. the
Alapin (2.c3) against the Sicilian instead of the repertoire's 2.d4 — add an
entry to `trainer_data/forced_moves.json` and rebuild the pack:

```json
[
  { "line": "1. e4 c5", "move": "c3", "note": "Alapin - easier to learn" }
]
```

`line` is the moves reaching the position (move numbers optional), `move` is the
move YOU want forced there. This works with **zero repertoire rebuild**: the
repertoire already scored every alternative and its whole subtree during
backwards induction, so the forced branch is followed using the repertoire's own
(self-consistent, engine-gated) recommendations below your forced move. The
override also flips the deviation lookup — after forcing 2.c3, playing 2.d4 in a
game now registers as a deviation, and 2.c3 as correct.

Only force moves the repertoire actually has data below (mainline alternatives
like the Alapin, Smith-Morra, Exchange lines). A truly offbeat forced move the
repertoire never evaluated will just train a shallow stub. Rebuild after editing:
`.venv\Scripts\python.exe -m trainer_app.build_training_pack`. Cards you already
studied in the old branch go dormant (they leave the queue); the forced branch's
cards enter as new.

## Trainer notes

- Only the repertoire's `best_move` is accepted; wrong answer = FSRS **Again**
  (shown the book move, play it to continue), first-try correct = **Good**.
- Drills are full lines from move 1 — except when the line passes through a
  position already shown **this session**, the drill starts there instead.
- Scheduling priority = `reach^a × (1+memo_cost)^b` — presets in Settings
  (frequency / value / balanced), plus new-cards-per-day cap, desired
  retention, and the once-per-day grading rule.
- `review_log` is append-only and holds exact timestamps/ratings — a future
  FSRS-optimizer pass can retrain the scheduler parameters from it.

## Lichess deviations

Set your **Lichess username** and **import since** date in Settings (API token
optional — only raises the rate limit), then Deviations → *Import Lichess
games*. Rated blitz/rapid/classical standard games are fetched (public API),
each game replayed, and every in-book position where you played something
other than the book move becomes a deviation row + an FSRS "Again" dated at
the game (never rewinding newer training state). Re-imports are idempotent.

## Moving to another PC

1. Install [uv](https://docs.astral.sh/uv/), copy the repo folder (or at least
   `python/` + `run_trainer.bat`) and your `trainer_data/` folder.
2. In the repo root: `uv venv --python 3.11` then
   `uv pip install -r python/trainer_app/requirements.txt`.
3. Double-click `run_trainer.bat`. Trainer + Deviations work immediately from
   `trainer_data/` — no pipeline drives needed. The Explorer tab needs the big
   rep/stats parquets; copy them and update their paths in Settings, or leave
   it — the other sections are unaffected.
