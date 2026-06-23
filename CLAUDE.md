# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project purpose

Chess opening repertoire builder. Ingests Lichess monthly PGN dumps, extracts every position-move edge from millions of games, aggregates them by `(event, elo_band, position, move)`, then runs backwards induction on the position DAG to produce an empirical "best move at each position" repertoire — sliced per game-type (Blitz/Bullet/Rapid/etc.) and per skill band, for both White and Black perspectives. Optional Stockfish overlay flags empirically-good-but-engine-bad trap lines.

## Data paths — critical

| Path | Role |
|------|------|
| `D:/data/chess/standard-chess-games-compressed/year=Y/month=M/event=E/` | **Canonical pipeline source.** Hive-partitioned parquets, one (year, month, event) per directory, multiple parquet shards per directory. This is the input to Stage 1. |
| `F:/chess/standard-chess-games/data/` | Raw original PGN-derived parquets. Pre-compression archive only — **never use this as a pipeline input.** |
| `E:/chess/position-moves-YYYY-MM/` | Stage 1 output (one per month). |
| `E:/chess/position-moves-YYYY-all/` | NTFS-hardlinked combined-year directory (created by `run_2025_combined.py`). Zero-cost on the same volume; downstream stages glob this. |
| `E:/chess/position-stats/position_stats_*.parquet` | Stage 2 final output. |
| `E:/chess/position-stats/position_stats_*.intermediates/` | Per-chunk Stage 2 intermediates (resumable). |
| `E:/chess/position-stats/position_stats_*.slices/_fragments/` | Per-slice fragment dirs used during Stage 2 merge phase. |
| `E:/chess/repertoire/repertoire_*.parquet` | Stage 3 output. Named `repertoire_<tag>_<persp>_f<fw>_e<ew>.parquet`. |
| `E:/chess/lichess_eval_db.parquet` | Stockfish eval cache for Stage 4 (built by `build_lichess_eval_db.py`). |

**Do not** reference `D:/data/chess/standard-chess-games/` (with no `-compressed`). It was a deprecated path that has been deleted; pipelines that pointed at it silently produced incomplete output.

## Pipeline architecture

Four stages, each writing parquet, each resumable via output-file skip gates.

```
PGN parquets (D:)
   │   stage1_extract_positions.py   (per-game move iteration via python-chess)
   ▼
position-moves/year=Y_month=M_event=E_part-N.parquet         ← one row per (game, ply)
   │   stage2_aggregate.py            (per-chunk → partition → tier-N → final)
   ▼
position_stats_<tag>.parquet                                  ← one row per (event, elo_band, parent_hash, move_san)
   │   stage3_backwards_induction.py  (topological sort + smoothed value propagation)
   ▼
repertoire_<tag>_<persp>_f<fw>_e<ew>.parquet                  ← one row per (event, elo_band, position) with chosen best_move
   │   stage4_engine_filter.py        (Stockfish overlay; optional)
   ▼
repertoire annotated with engine_eval + unsafe flag
```

The pipeline is orchestrated by `python/run_2025_combined.py` (multi-month, multi-year batch runner). For single-year work, `python/run_2016_and_compare.py` and `python/run_convergence.py` are simpler entry points.

### Stage 1: position-move edge extraction

- Two dispatch modes:
  - **partition-level** (default): one output parquet per (year, month, event) partition. Produces ≤6 files per month.
  - **file-level** (`--file-level`): one output parquet per source parquet file. Produces 40-50 files per recent month. Massively better worker utilisation on Blitz/Bullet (which have 20+ source shards).
- Source-row schema: PascalCase columns (`Site`, `White`, `WhiteElo`, `movetext`, ...) — Lichess convention preserved from upstream parsing.
- Output edge schema: `parent_hash` (i64 zobrist), `move_san`, `parent_epd`, `ply`, `white_score`, `event`, `elo_band` (computed via `mean_elo // 100 * 100`).
- `--max-ply 30` is the default depth (15 full moves).

### Stage 2: aggregation

Stage 2 has **three distinct sub-phases**, each independently resumable:

1. **Per-chunk aggregation** — each Stage 1 output parquet → one intermediate parquet under `position_stats_*.intermediates/`. Aggregates within the chunk only (no min_games filter applied yet). Files > 5 GB use a row-group-batched code path (`_aggregate_chunk_batched`) because Polars `sink_parquet` segfaults on 15+ GB inputs.
2. **Partition phase** — reads each intermediate **once**, splits its rows by `(event, elo_band)` into per-slice fragment files at `position_stats_*.slices/_fragments/slice_<event>_elo<eb>/`. Marked complete via `.partition_complete` sentinel. **Deleting the sentinel forces re-partition.**
3. **Per-slice aggregation via DuckDB** — recursive tier-based GROUP BY. For each slice:
   - Tier-1: aggregate fragments in batches of `TIER_BATCH=10`.
   - Recursive sub-batching at `SUB_BATCH=2` per tier, looping until the next-tier file count ≤ `MAX_FINAL_INPUTS=4`.
   - Final HAVING pass on those ≤4 files applies `--min-games` (currently default 50).
   - Then a Python pass adds `child_hash` via `python-chess` (Zobrist of the position reached after `move_san`).

DuckDB settings that matter (in `merge_intermediates`):
- `memory_limit='110GB'` — pushed up from 80 GB after repeated tier-3 OOMs on 2024 data.
- `threads=6` — fewer threads = smaller per-thread hash table partitions.
- `preserve_insertion_order=false` — lets DuckDB drop intermediate buffers early.

If a slice's final pass OOMs, the right knob is **lowering `MAX_FINAL_INPUTS`** (forces another tier), not raising `memory_limit`.

### Stage 3: backwards induction

- Operates on the position DAG (no cycles — positions are zobrist hashes).
- Topological order via Kahn's algorithm so each position is valued exactly once despite transpositions.
- **Beta-Binomial smoothing on terminal leaves**: `(k * mu_slice + score * n) / (k + n)` with `k = --prior-strength` (default 100). Prevents lucky 10-game leaves from being picked by the max operator.
- Two tuning weights produce variants: `--forcing-weight` (bias toward forcing moves) and `--eval-weight` (blend in Stockfish eval if `--eval-db` provided).
- 12 standard variants per dataset: `fw ∈ {0.00, 0.30}` × `ew ∈ {0.00, 0.50, 1.00}` × `{white, black}`.

### Stage 4: Stockfish safety filter

- Walks the recommended tree from the start position, evaluates each unique resulting position with Stockfish, flags positions whose engine eval falls below threshold from our perspective.
- Persistent EPD-keyed cache (`E:/chess/lichess_eval_db.parquet`), checkpointed every `--save-every` evals — interrupted runs only re-evaluate uncached positions.

## Running the pipeline

All commands use `.venv/Scripts/python.exe` (uv-managed venv, Python 3.11.15) and must run from `C:/Users/David/Documents/Chess`.

```powershell
# Full multi-year pipeline (Stage 1 → 2 → 3 for 2024+2025 combined)
.venv\Scripts\python.exe python\run_2025_combined.py

# Single Stage 1 partition (debugging)
.venv\Scripts\python.exe python\stage1_extract_positions.py --partition year=2024/month=1/event=Blitz --limit-games 10000

# Stage 1 across many partitions
.venv\Scripts\python.exe python\stage1_run_all.py --year-range 2024 2024 --months 1 2 3 --file-level

# Stage 2 with custom min-games
.venv\Scripts\python.exe python\stage2_aggregate.py --input E:/chess/position-moves-2024-all --output E:/chess/position-stats/position_stats_2024.parquet --min-games 50

# Stage 3 single variant
.venv\Scripts\python.exe python\stage3_backwards_induction.py --input E:/chess/position-stats/position_stats_2024.parquet --output E:/chess/repertoire/repertoire_2024_white_f030_e050.parquet --perspective white --forcing-weight 0.30 --eval-db E:/chess/lichess_eval_db.parquet --eval-weight 0.50

# Stage 4 engine overlay
.venv\Scripts\python.exe python\stage4_engine_filter.py --repertoire E:/chess/repertoire/repertoire_2024_white_f030_e050.parquet

# Test the notification system (sends a real email if configured)
.venv\Scripts\python.exe python\notify.py "subject" "body"

# Watchdog (used by scheduled task, can be run manually)
.venv\Scripts\python.exe python\pipeline_watchdog.py [--dry-run] [--no-email]

# Streamlit explorer UI
.venv\Scripts\python.exe -m streamlit run python\explorer_app.py
```

There is no formal test suite, lint config, or build step. Validation is via direct script runs and the per-stage output parquets.

## Resumability conventions

The orchestrator and stages are designed to survive crashes and resumes cleanly. Patterns to preserve when modifying:

- Atomic writes: every parquet write goes to `<path>.parquet.tmp` then `Path.replace()` to `<path>.parquet`.
- Skip gates: every stage checks for its output file and skips if present.
- `check_stage1_complete(year, month, dir)` handles BOTH partition-level (≥6 files) and file-level (count matches source) outputs — detected via filename pattern (`_part-` or `_batch` suffix).
- Stage 2 sub-phase sentinels: `.partition_complete` marks finished partition phase; tier-1 has `_tier1_meta.json` sidecar tracking `n_batches` so a TIER_BATCH change is detected and old files purged.
- When you want to force a Stage 2 phase to redo: delete the sentinel/tier file, the next run cleanly rebuilds.

## Operational gotchas (the painful lessons)

### PyArrow C++ memory-pool fragmentation
Any tight loop calling `pq.read_table()` more than a few hundred times on Windows will eventually segfault with **0xC0000005 (no Python traceback, no error log, just process death)**. Affects:
- Stage 2 per-slice merge (fixed)
- Stage 2 slice discovery (fixed)
- Any future read-loops

**Required mitigation**: every ~25 iterations, call `gc.collect()` followed by `pyarrow.default_memory_pool().release_unused()`. Without these, the bug is invisible until the loop is large enough.

### Polars streaming sink_parquet on Windows
`pl.LazyFrame.sink_parquet()` on large (>10 GB) inputs silently produces no output file (no exception). The eager `df.collect().write_parquet()` path is reliable. Stage 1's `_aggregate_chunk_batched` switches to this path for files over `LARGE_FILE_THRESHOLD = 5 GB`. **Don't restore `sink_parquet` in the hot path** unless you're prepared to add file-existence checks after every write.

### Polars `collect(streaming=True)` engine
Crashes (0xC0000005) on Windows for slice volumes >10 GB. Stage 2 deliberately uses eager DuckDB aggregation instead; never use streaming Polars for large slices on this codebase.

### Stage 2 min_games filter must NOT be applied at intermediate tiers
Applying HAVING at tier-1 or tier-N would drop positions whose per-batch count is below the threshold even though cumulative count across all batches would have qualified. Only apply at the **final** tier pass. The `_ddb_agg(..., apply_min_games=True)` flag is the contract — intermediate tiers always pass False.

### October 2024 source data is corrupt
Broken move numbering (`...0. {white} 1. {black}`). Excluded from the 2024 combined dataset via `MONTHS_2024_SKIP = {10}` in `run_2025_combined.py`. If the upstream Lichess data is fixed and reprocessed, remove this skip.

## Notification & watchdog

- `python/notify.py`: stdlib-only SMTP. Reads creds from `%USERPROFILE%/.chess_pipeline_notify.json` (gitignored; the file lives **outside** the repo so it never gets committed). Falls back to env vars `CHESS_NOTIFY_*`. Silently no-ops if unconfigured — pipeline runs identically with or without it. `python/notify_config.example.json` is the format template.
- `python/pipeline_watchdog.py`: registered as Windows Task Scheduler task `ChessPipelineWatchdog`, runs daily at 09:00 local. On each tick it (1) finds any `python.exe` running `run_2025_combined.py`, (2) if dead and the final outputs don't exist, relaunches with `DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP` so it survives the task ending, (3) emits a one-line status email. Self-deletes its scheduled task once the pipeline is fully done.

## Compression and storage standards

The R script `01_processing_pgn_files.R` (now in `archive/`) produced the original `D:/data/chess/standard-chess-games-compressed/` files with zstd-3 and tiny row groups (~400 rows/group, ~10% metadata overhead). The replacement is `python/process_pgn_parquets.py`:

- **zstd level 6** (benchmarked optimal in `benchmark_zstd_levels.py` — past level 6 it's only marginally smaller for substantially more compression time).
- **1M row groups** (vs R's ~400) — drops metadata overhead to ~0% and makes reads 50× faster.
- Schema matches the R output exactly (same 27 columns + `year/month/event` partition columns). Drop-in replacement.

When you ingest new months (May–Dec 2025, 2026+), use `process_pgn_parquets.py`, not the archived R script.

## Other useful scripts

- `convergence_compare.py`: cross-dataset agreement / value-correlation analysis. Used to validate that 2016 vs 2025 repertoires converge on shared positions.
- `analyze_coverage.py`: forward-flow analysis on the position DAG. Answers "what fraction of opponent games stay in our prepared lines at ply N?"
- `compare_forcing_variants.py`: side-by-side comparison of repertoires that differ only in `--forcing-weight`.
- `repertoire_browser.py`: lightweight Flask server that serves a chessboard UI at `http://127.0.0.1:8765/` for walking the recommended tree.
- `explorer_app.py`: heavier Streamlit-based variant with more filtering controls.
- `benchmark_zstd_levels.py`, `compare_row_group_sizes.py`: one-off perf studies (kept for reference / re-running on new data).
- `epd_debug.py`, `epd_inspect.py`, `repertoire_trace.py`: ad-hoc query scripts for poking at specific positions in the repertoire output. Useful templates when you need to answer a specific "what does the repertoire say at this EPD?" question.

## Working principles

- **Surgical changes.** Every edited line should trace to the user's request.
  Don't tidy adjacent code, don't reformat, don't "improve" what's not broken.
  If you notice unrelated issues worth fixing, list them — don't silently fix them.

- **Verify before claiming done.** For pipeline changes, verification means
  one of: (a) a small offline test on a single partition, (b) reading the
  actual log/output file (not just "looks like it started"), (c) compile-check
  + dry-run for resumability-affecting changes. "Process is running" ≠ done.

- **State assumptions before tuning constants.** This codebase has many
  empirical constants (TIER_BATCH, memory_limit, min_games). Before changing
  one, name the failure you expect to fix and the test that would prove it.
  Otherwise you're guessing.