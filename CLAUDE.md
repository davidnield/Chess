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

## Data paths — critical

| Path | Role |
|------|------|
| `D:/data/chess/standard-chess-games-compressed/year=Y/month=M/event=E/` | **Canonical source.** Hive-partitioned parquets (2013–present). Every extraction starts here. |
| `F:/chess/standard-chess-games/data/` | Pre-compression archive only — **never a pipeline input.** F: is a USB spinning disk — never point DuckDB spill/temp at it either. |
| `E:/chess/position-moves-*` | Legacy Stage-1 edge extracts (2024/2025 only). The `-all` dirs are NTFS-hardlinked unions of the per-month dirs — same physical bytes, so deleting per-month dirs frees nothing; space is only freed when the `-all` links go too. |
| `E:/chess/crush-per-game-v2/` | Per-game decisive-result facts (`extract_crush_per_game.py`): win flags, termination, move_count. Input to crush histogram builders. |
| `E:/chess/position-stats/` | Aggregated stats: `position_stats_*.parquet` (per-edge win/draw/loss counts) and `crush_hist_*.parquet` (crush histograms). `_pooled_partials_*/` are resumable working dirs for in-flight pooled builds. |
| `E:/chess/repertoire/` | Stage-3 outputs, with `.meta.json` provenance sidecars recording the exact inputs/flags that built each one. |
| `E:/chess/lichess_eval_db.parquet` | `position_hash → eval_cp` Stockfish eval DB (`build_lichess_eval_db.py`). Used by Stage-3 `--eval-db`, the winpos crush builder, and Stage 4. The raw `lichess-evals/` dump it was built from has been deleted — rebuilding requires re-downloading from database.lichess.org. |

**Do not** reference `D:/data/chess/standard-chess-games/` (with no `-compressed`). It was a
deprecated path that has been deleted; pipelines that pointed at it silently produced incomplete
output.

## Pipeline map

### Canonical path (pooled ≥1800 Blitz/Rapid/Classical)

```
D: source parquets
   │  build_pooled_stats.py --phase extract   (fused extract+aggregate; per-file partials)
   │  build_pooled_stats.py --phase merge     (monthly consolidation → final GROUP BYs)
   ▼
position_stats_pooled_<tag>.parquet + crush_hist_rel_pooled_<tag>.parquet
   │  build_sharp_reps.py                     (locked Stage-3 recipe; wraps stage3_backwards_induction.py)
   ▼
repertoire_pooled_{white,black}_sharp.parquet
   │  score_repertoire.py                     (evaluation harness / objective function)
   │  repertoire_explorer.py                  (browsing UI)
```

Pooled outputs are stamped `event='Pooled', elo_band=0` — a synthetic slice, so Stage 3 consumes
them unchanged. Skipped corrupt source months are encoded in `SKIP_PARTITIONS` in
`build_pooled_stats.py` (and `MONTHS_2024_SKIP` in the legacy `run_2025_combined.py`).

### Legacy sliced path (per-(event, elo_band) stats)

`stage1_extract_positions.py → stage2_aggregate.py → stage3_backwards_induction.py →
stage4_engine_filter.py`. Kept for per-band/per-event slicing; not the current workflow. Stage 2's
tier/fragment/sentinel machinery is documented in its docstrings. One durable lesson: if a Stage-2
final slice pass OOMs, the right knob is lowering `MAX_FINAL_INPUTS` (forces another tier), not
raising the memory limit.

### Stage 3 — the shared engine

Backwards induction over the position DAG (zobrist int64 hashes, Kahn topological order — each
position valued once despite transpositions). Move selection combines four ingredients:

1. **Empirical value** — smoothed propagated score (Beta-Binomial prior on leaves so lucky
   low-count lines don't win the max).
2. **Crush bonus** — reward for lines that reach winning positions early (see metric below).
3. **Memorization penalty** — cost per position the user must learn, plus a leaving-book cost.
4. **Refutation gate** — a candidate is rejected if its worst-case/robust value against the
   opponent's best replies falls below threshold ("never outright losing against best play").

The blessed flag set is not documented here — it lives in `build_sharp_reps.py`, with
provenance in each output's `.meta.json`.

### Crush metric — precise definition

"Crush" measures how early a game was effectively won. Relative histogram keyed
`(parent_hash, move_san, move_bucket)` where `move_bucket = clip(move_count − (ply−1)//2, 1, 60)`
— moves-to-win counted *from the edge*, not from move 1. Two variants:

- **Resignation-proxy** (`build_crush_stats.py`): win = decisive result with `termination='Normal'`
  (mate or resignation). No eval component.
- **Winpos** (`build_crush_winpos.py`): win event = the *earliest* of (first position strictly
  after the edge with eval ≥ +300cp for our side, decisive-normal end). One event per game per
  side — no double counting; a +3 advantage counts even if later thrown away; uncovered positions
  fall back to normal terminations.

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

There is no formal test suite, lint config, or build step. Validation is via direct script runs on
small slices and inspection of output parquets. Emerging convention: synthetic tests import the
production query/logic rather than copying it (`_test_winpos.py` is the template).

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

New month ingestion (2025-05+, 2026+) uses `python/process_pgn_parquets.py`: zstd level 6
(benchmarked knee in `benchmark_zstd_levels.py`) and 1M-row row groups (~0% metadata overhead,
~50× faster reads than the archived R script's tiny groups). Schema is a drop-in match for the
existing `D:` partitions. The original R script lives in `archive/` — don't use it.

## Notifications

`python/notify.py`: stdlib-only SMTP. Creds in `%USERPROFILE%/.chess_pipeline_notify.json`
(outside the repo, gitignored pattern; `notify_config.example.json` is the template) or
`CHESS_NOTIFY_*` env vars. Silently no-ops if unconfigured — pipelines run identically without it.
`pipeline_watchdog.py` was a one-off babysitter for the completed 2024/2025 run (self-deleting
scheduled task); historical, but a useful template if a future run needs one.

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
