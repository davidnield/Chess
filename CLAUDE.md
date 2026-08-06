# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Maintenance rule:** this file documents invariants, contracts, and failure-mode knowledge — not
current state. Precise flag sets, tuning constants, and output inventories live in the code; this
file points at the owning script instead of copying them. A change that alters pipeline shape or
entry points updates this file in the same commit.

## Project purpose

Chess opening repertoire builder. Ingests Lichess monthly PGN dumps, extracts position-move edges
from millions of games, aggregates them into per-position move statistics, then runs backwards
induction on the position DAG to produce an empirical "best move at each position" repertoire.
The current product is a memorization-efficient "sharp" repertoire for ≥1800 Blitz/Rapid/Classical
pooled data: find moves most likely to reach a winning position as early as possible without being
outright losing against best play. A Stockfish eval overlay gates out empirically-good-but-losing
trap lines.

## Repo layout

`README.md` is the public front door (what the project is, how the stages chain, how to run
them); this file is the deep technical companion. Keep them consistent when entry points move.

- `python/` — the pipeline, the analysis tools, `annotate/`, `trainer_app/`, and the test suite
  (`run_tests.py` + `_test_*.py`, fixtures in `python/_test_fixtures/`).
- `archive/` — the original R implementation this replaced. Reference only, never run.
- `scratch/` — **gitignored.** Ad-hoc one-off scripts: benchmark harnesses whose result is
  already baked into the code, drivers for runs that have completed, throwaway probes. Put
  new one-offs here rather than in `python/`, so a fresh checkout shows only things that are
  actually part of the pipeline or the tests. Scripts there still import first-party modules
  from `python/`, so running one needs `PYTHONPATH=<repo>/python`. The directory and its own
  `README.md` are local-only and will not be present in a fresh clone — that is intentional.

Anything in `python/` is implicitly a claim that it is a maintained part of the pipeline,
the analysis surface, or the tests. If that stops being true, move it to `scratch/`.

## Data paths — critical

| Path | Role |
|------|------|
| `D:/data/chess/standard-chess-games-compressed/year=Y/month=M/event=E/` | **Canonical source.** Hive-partitioned parquets (2013–present). Every extraction starts here. |
| `F:/chess/standard-chess-games/data/` | Pre-compression archive only — **never a pipeline input.** F: is a USB spinning disk — never point DuckDB spill/temp at it either. Also the **only** copy retaining `[%clk]`/`[%eval]` movetext comments (recipe v3 strips them from `D:` at ingest). |
| `E:/chess/position-moves-*` | Legacy Stage-1 edge extracts (2024/2025 only). The `-all` dirs are NTFS-hardlinked unions of the per-month dirs — same physical bytes, so deleting per-month dirs frees nothing; space is only freed when the `-all` links go too. |
| `E:/chess/crush-per-game-v2/` | Per-game decisive-result facts (`extract_crush_per_game.py`): win flags, termination, move_count. Input to crush histogram builders. |
| `E:/chess/position-stats/` | Aggregated stats: `position_stats_*.parquet` (per-edge win/draw/loss counts), `position_stats_aux_*.parquet` (the per-position sidecar of unseen mass), and `crush_hist_*.parquet` (crush histograms). `_pooled_partials_*/` are resumable working dirs for in-flight pooled builds. |
| `E:/chess/eval_arrays/` | `unified_eval_db.parquet` materialised as sorted `.npy` (`eval_hash`, `eval_cp` int16) so extract workers mmap ONE shared resident copy instead of N private 4.8 GB loads. **Derived** — `eval_arrays.meta.json` fingerprints the source (size/mtime/rows) and readers verify, because a stale copy answers every query with the previous DB's evaluations and nothing downstream would notice. Rebuild: `python/eval_arrays.py [--force]`. |
| `E:/chess/repertoire/` | Stage-3 outputs, with `.meta.json` provenance sidecars recording the exact inputs/flags that built each one. |
| `E:/chess/lichess_eval_db.parquet` | `position_hash → eval_cp` cloud-eval Stockfish DB (`build_lichess_eval_db.py`). The raw `lichess-evals/` dump it was built from has been deleted — rebuilding requires re-downloading from database.lichess.org. Superseded as the Stage-3 default by `unified_eval_db.parquet` below, but still a direct input to it. |
| `E:/chess/unified_eval_db.parquet` | **Canonical eval DB.** `lichess_eval_db.parquet` unioned with the aggregated fishnet-evals dump (`build_fishnet_eval_db.py`, cloud-preferred, median-of-replicates per era tier) — broader position coverage than the cloud DB alone. This is the default `--eval-db` for Stage-3 and `build_sharp_reps.py`. |

**Do not** reference `D:/data/chess/standard-chess-games/` (with no `-compressed`). It was a
deprecated path that has been deleted; pipelines that pointed at it silently produced incomplete
output.

## Pipeline map

### Canonical path (pooled ≥1800 Blitz/Rapid/Classical)

```
D: source parquets
   │  build_pooled_stats.py --phase extract   (ONE replay, four payloads per file:
   │                                           ps + terminal-accounting + winpos
   │                                           histograms + child_hash/child_eval)
   │  build_pooled_stats.py --phase merge     (monthly consolidation → final GROUP BYs)
   ▼
position_stats_pooled_<tag>.parquet          ← surviving edges (min_games 50)
position_stats_aux_pooled_<tag>.parquet      ← the sidecar: per position, the mass the
   │                                           outgoing edges cannot see (term / horizon /
   │                                           below-floor "other"). See Stage 3 below.
crush_hist_relwin_pooled_<tag>.parquet       ← winpos histogram, one per threshold
   │  build_sharp_reps.py                     (locked Stage-3 recipe; TWO-PASS wrapper around
   │                                           stage3_backwards_induction.py: pass-1 build →
   │                                           plan_consistency_report.py --export-prefix →
   │                                           pass-2 with the learnability plan prior)
   ▼
repertoire_pooled_{white,black}_sharp.parquet
   │  score_repertoire.py                     (evaluation harness / objective function)
   │  plan_consistency_report.py              (idea-consistency lens: % of games per idea-token,
   │                                           split by opponent's first move)
   │  repertoire_explorer.py                  (browsing UI)
```

**The winpos histogram is produced by the extract's own replay** (`--fuse-winpos`,
default on, thresholds via `--winpos-thresholds`). `build_crush_winpos_phase2.py` was a
SECOND full pass over D: (measured 47 h, peak 76 GB) and is now redundant — kept only
because `build_crush_winpos.winpos_sql` remains the *definition* of the win event and the
oracle `_test_winpos_fused.py` holds the fused path to. The extract cannot apply the
pool's `min_games` floor (a global merge decision), so it emits a row per edge and the
merge semi-joins to the survivors — same restriction, later.

**The extract requires the mmap'd eval arrays** (`E:/chess/eval_arrays/`, built by
`eval_arrays.py`): winpos needs per-position evals for the crossing test, and `child_eval`
feeds the aux bucket's aggregate evaluation. `--phase extract` verifies them up front and
refuses to start if they are missing or stale.

Pooled outputs are stamped `event='Pooled', elo_band=0` — a synthetic slice, so Stage 3 consumes
them unchanged. Skipped corrupt source months are encoded in `SKIP_PARTITIONS` in
`build_pooled_stats.py` (and `MONTHS_2024_SKIP` in the legacy
`scratch/python/run_2025_combined.py`).

**The resignation-proxy crush histogram is retired.** `build_pooled_stats.py` no longer merges
`crush_hist_rel_pooled_*` unless `--crush-hist` is passed: nothing consumes it, and consolidating
its partials was the largest avoidable cost in the merge. The extract still writes
`.crush.parquet` partials, so it can be merged later without re-extracting.

**The `.crush.parquet` partials are still written and still unconsumed.** Stopping that
saves ~163 GB and some extract time; it is deliberately deferred until the fused winpos
output has been validated end-to-end on a full build.

### Legacy sliced path (per-(event, elo_band) stats)

`stage1_extract_positions.py → stage2_aggregate.py → stage3_backwards_induction.py →
stage4_engine_filter.py`. Kept for per-band/per-event slicing; not the current workflow. Stage 2's
tier/fragment/sentinel machinery is documented in its docstrings. One durable lesson: if a Stage-2
final slice pass OOMs, the right knob is lowering `MAX_FINAL_INPUTS` (forces another tier), not
raising the memory limit.

### Stage 3 — the shared engine

Backwards induction over the position DAG (zobrist int64 hashes, Kahn topological order — each
position valued once despite transpositions; residual cycles get Tarjan SCC condensation +
damped fixpoint sweeps).

**Opponent-node denominator (`--aux-stats`, default off = exact no-op).** An opponent node's
value is a mean over their replies, and that mean used to divide by the node's OUTGOING edges
only — so games that ENDED there, replies below `min_games`, and games cut by the ply cap all
contributed nothing. The first is directional: the side to move scores ~0.095 at a terminal
node, so dropping them deleted precisely the opponent's collapses (measured 0.9015 vs 0.5087,
understating winning lines by ~0.027 and compounding up the tree). With the sidecar the
denominator becomes `out + term + other + horizon`, each bucket carrying its own value —
terminations their empirical result (a finished game is a fact), the below-floor bucket its
empirical score blended with the engine on the eval-COVERED mass, the horizon its own eval or
empirical fallback. `--reply-shrink` is forced to 0 when the sidecar is supplied: both correct
overlapping missing mass and stacking them double-counts it.

OUR nodes are deliberately asymmetric — `values[ph]` there is prescriptive ("what our book
gets from here"), not an empirical mean, so only the half of a termination where the OPPONENT
resigned before we moved is folded in. Games where *we* resigned abandoned the book and must
not be charged to the recipe.

Move selection combines five ingredients:

1. **Empirical value** — smoothed propagated score (Beta-Binomial prior on leaves so lucky
   low-count lines don't win the max).
2. **Crush bonus** — reward for lines that reach winning positions early (see metric below).
3. **Memorization penalty** — cost per position the user must learn, plus a leaving-book cost.
4. **Refutation gate** — a candidate is rejected if its worst-case/robust value against the
   opponent's best replies falls below threshold ("never outright losing against best play"),
   plus a relative gate against conceding vs the best sibling candidate.
5. **Learnability tiebreak** (two-pass only) — among candidates within a δ window of the best
   selection key, prefer the move whose idea-token (piece destination / pawn break) the
   repertoire plays most often in that opponent-first-move context (`--plan-prior` /
   `--plan-reach`, exported by `plan_consistency_report.py` from the pass-1 rep). δ is loose
   only at shallow-but-rare nodes; measurement and selection MUST share `idea_token()`
   (defined in stage3, imported by the report).

The blessed flag set is not documented here — it lives in `build_sharp_reps.py`, with
provenance in each output's `.meta.json`.

### Crush metric — precise definition

"Crush" measures how early a game was effectively won. Relative histogram keyed
`(parent_hash, move_san, move_bucket)` where `move_bucket = clip(move_count − (ply−1)//2, 1, 60)`
— moves-to-win counted *from the edge*, not from move 1. Two variants:

- **Resignation-proxy** (`build_crush_stats.py`): win = decisive result with `termination='Normal'`
  (mate or resignation). No eval component.
- **Winpos** (**canonical crush source for `build_sharp_reps.py`**): win event = the *earliest*
  of (first position strictly after the edge with eval ≥ +300cp for our side, decisive-normal
  end). One event per game per side — no double counting; a +3 advantage counts even if later
  thrown away; uncovered positions fall back to normal terminations. Now computed inside the
  extract by `winpos_fused.py`; `build_crush_winpos.winpos_sql` remains the definition and the
  test oracle. Uncovered positions must be masked explicitly — the `MISSING` sentinel is
  −32768, which would otherwise satisfy `eval ≤ −threshold` and manufacture a black crossing at
  every position the eval DB has never seen.

The distinction matters: the original design intended the eval clause, and its absence went
unnoticed for weeks because the definition wasn't written down.

## Running things

All commands use `.venv/Scripts/python.exe` (uv-managed, Python 3.11) from
`C:/Users/David/Documents/Chess`. Every pipeline script carries an authoritative header docstring
with its exact CLI — read it before running; don't trust remembered flags.

```powershell
# Pooled stats build (extract, then merge; both resumable)
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year Y1 --end-year Y2 --phase extract
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year Y1 --end-year Y2 --phase merge --tmp-dir D:\chess_duckdb_tmp

# Sharp repertoires from pooled stats
.venv\Scripts\python.exe python\build_sharp_reps.py

# Score a repertoire / browse it
.venv\Scripts\python.exe python\score_repertoire.py --repertoire <path> --stats <path>
.venv\Scripts\python.exe python\repertoire_explorer.py
```

There is no lint config or build step, and validation still leans on direct script runs on small
slices plus inspection of output parquets. But there is now a lightweight test suite: every
`python/_test_*.py` is a standalone pass/fail script (exit 0/1), and `python/run_tests.py`
subprocess-runs all of them with a summary — `.venv/Scripts/python.exe python/run_tests.py` is the
one-command "did I break anything" check. Convention: synthetic tests import the production
query/logic rather than copying it (`_test_winpos.py` is the template).

## Resumability conventions

Long runs die (crashes, OOMs, silent external kills) — every stage must survive a restart. Patterns
to preserve when modifying:

- Atomic writes: parquet goes to `<path>.parquet.tmp`, then `Path.replace()`. An output exists only
  if it is complete.
- Skip gates: every stage/chunk checks for its output and skips if present. Forcing a redo = delete
  the output (or its sentinel), rerun.
- Checkpoint expensive mid-stage results (e.g. the pre-`child_hash` aggregation checkpoint in
  `build_pooled_stats.py`) so a crash in a cheap later phase doesn't repeat a multi-hour one.
- **min_games must only be applied at the final aggregation pass** — filtering at an intermediate
  tier drops keys whose cumulative count would have qualified. This contract holds in both
  pipelines.

## Operational gotchas (the painful lessons)

### Windows native-allocator degradation (two faces, one gremlin)
Long-lived processes doing repeated multi-GB alloc/free cycles degrade or die:
- **PyArrow**: tight loops calling `pq.read_table()` hundreds of times eventually segfault with
  0xC0000005 (no traceback, no log — just process death). Mitigation: every ~25 iterations,
  `gc.collect()` + `pyarrow.default_memory_pool().release_unused()`.
- **DuckDB**: consecutive multi-GB GROUP BYs in one process decay throughput 3–5× with no error
  (measured on flat same-size inputs, zero spill). Mitigation: per-task process isolation —
  `ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1)`, as in `build_pooled_stats.py`'s
  monthly consolidation.

### DuckDB out-of-memory triage — two errors that look alike
- `failed to offload data block ... max_temp_directory_size` → the **temp drive is full**, not RAM.
  Fix: point the spill dir at a roomy NVMe volume (`--tmp-dir` in `build_pooled_stats.py`). Never
  a USB HDD — spill is random I/O and will crawl.
- `could not allocate block ... (X/X used)` → the **memory_limit is genuinely exhausted**. Fix:
  fewer threads (smaller per-thread hash partitions) and/or a higher limit. Set
  `preserve_insertion_order=false` always. If it *still* OOMs at the full machine budget, the
  group cardinality doesn't fit RAM at all — partition the GROUP BY into disjoint key-hash
  buckets and union the results (`N_MERGE_BUCKETS` in `build_pooled_stats.py` is the template;
  bucketing on a column of the grouping key keeps HAVING-filter semantics exact).

### Windows working-set trim vs. big DuckDB memory limits
A DuckDB process whose memory_limit approaches physical RAM while scanning hundreds of GB of
files can get its working set trimmed by Windows (file-cache standby growth wins the fight), then
crawl indefinitely on hard page faults. The tell: private bytes ≫ working set (e.g. 97 GB vs
6 GB) **with plenty of free physical RAM**, ~1 core busy, zero spill, zero output. The fix is a
*lower* memory_limit (leave ~half of RAM for the OS cache when the query scans big inputs), not a
higher one — this is a third OOM-adjacent failure that looks like a hang rather than an error.

### Polars on Windows at scale
- `LazyFrame.sink_parquet()` on >10 GB inputs silently produces **no output file** (no exception).
  Use eager `collect().write_parquet()` on large inputs; if you must stream, add file-existence
  checks after every write.
- `collect(streaming=True)` crashes (0xC0000005) on >10 GB volumes. Use DuckDB for big
  aggregations instead.

### Long-running jobs on this machine
Processes attached to a session (including harness background jobs) die when the session ends, and
even detached processes have been killed with no OS trace. Launch pattern that works: write a
`.bat` and `Start-Process -FilePath <bat> -WindowStyle Hidden`, logging to a file. But the real
defense is the resumability conventions above — assume any run can die silently and make restarts
cheap. Windows Task Scheduler has been unreliable for this; don't build on it.

## Compression and storage standards

New month ingestion (2025-05+, 2026+) uses `python/process_pgn_parquets.py` with **recipe v3**
(locked by the `parquet_recipe_202607` benchmark — harness now in
`scratch/python/benchmark_*.py`; results at
`E:/bench/parquet_recipe_202607/fullmonth_results.csv`): **zstd-19**, **1M-row row groups**, and
**M1 movetext comment-stripping**. Two earlier claims here were wrong and are corrected: the level
was zstd-6, not the tuned optimum, and the "1M-row groups" were aspirational — the old
`write_dataset` streaming path emitted ~one group per tiny scan batch (~300 rows/group,
4,776 groups/file). The row-group size is now **actually enforced** by a per-event buffered
`pq.write_table(row_group_size=1M)` (raises peak RAM — keep `--workers` low for big recent months).
The elo-sort tested in the benchmark was **not** adopted: the one heavy consumer
(`build_pooled_stats` extract) is replay-bound, so row-group pruning bought nothing (−4.8%).

**Movetext invariant (LOSSY):** canonical `D:` movetext is **comment-stripped** — `[%clk]`/`[%eval]`
brace annotations are removed at ingest. The comments survive **only** in the `F:` raw archive.
This changes no downstream aggregate: the strip removes exactly what `iter_san_moves` discards at
parse time, and `has_eval` / `move_count` are computed on the original movetext *before*
stripping. Every non-movetext column is byte-identical to the R schema, so output stays a
drop-in match for the existing `D:` partitions. The original R script lives in `archive/` —
don't use it.

That invariant rests on **two independent implementations agreeing** — the polars chain in
`process_pgn_parquets.movetext_strip_expr` and the tokenizer in `iter_san_moves` — and they are
NOT interchangeable in general: `1...e5` with no space strips to `e5` but tokenizes as the
unparseable `1...e5`. It holds only because Lichess always writes the space. So
`_test_movetext_strip.py` asserts the equality (SAN tokens *and* `move_count`, on real
comment-era games) **and** that precondition, so a source-format change fails the test instead
of silently eating a move. Keep the patterns in `MOVETEXT_STRIP_STEPS`: the test exercises the
production expression rather than a copy of it.

## Notifications

`python/notify.py`: stdlib-only SMTP. Creds in `%USERPROFILE%/.chess_pipeline_notify.json`
(outside the repo, gitignored pattern; `notify_config.example.json` is the template) or
`CHESS_NOTIFY_*` env vars. Silently no-ops if unconfigured — pipelines run identically without it.
`scratch/python/pipeline_watchdog.py` was a one-off babysitter for the completed 2024/2025 run
(self-deleting scheduled task); historical, but a useful template if a future run needs one.

## Working principles

- **Surgical changes.** Every edited line should trace to the user's request.
  Don't tidy adjacent code, don't reformat, don't "improve" what's not broken.
  If you notice unrelated issues worth fixing, list them — don't silently fix them.

- **Verify before claiming done.** For pipeline changes, verification means
  one of: (a) a small offline test on a single partition, (b) reading the
  actual log/output file (not just "looks like it started"), (c) compile-check
  + dry-run for resumability-affecting changes. "Process is running" ≠ done.

- **State assumptions before tuning constants.** This codebase has many
  empirical constants (batch sizes, memory limits, min_games). Before changing
  one, name the failure you expect to fix and the test that would prove it.
  Otherwise you're guessing.
