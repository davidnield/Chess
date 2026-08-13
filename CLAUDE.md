# CLAUDE.md

Guidance for Claude Code when working in this repository.

**Maintenance rule:** this file documents invariants, contracts, and failure-mode knowledge — not
current state. Precise flag sets, tuning constants, and output inventories live in the code; this
file points at the owning script instead of copying them. A change that alters pipeline shape or
entry points updates this file in the same commit.

**When compacting, always preserve in-flight run state:** process IDs, log file paths, chunk
counts and their denominators, armed watchdog thresholds, and which `--phase` is running. Runs
here last days, and a compaction that drops them turns a status check into a re-investigation.

## Project purpose

Chess opening repertoire builder. Ingests Lichess monthly PGN dumps, extracts position-move edges
from millions of games, aggregates them into per-position move statistics, then runs backwards
induction on the position DAG to produce an empirical "best move at each position" repertoire.
The current product is a memorization-efficient "sharp" repertoire for ≥1800 Blitz/Rapid/Classical
pooled data: reach a winning position as early as possible without being outright losing against
best play, with a Stockfish eval overlay gating out empirically-good-but-losing trap lines.

Deeper detail lives in two skills, loaded on demand rather than every session:

- **`chess-pipeline`** — stage-by-stage map, Stage 3's engine and its five selection ingredients,
  the crush/winpos metric definition, the legacy sliced path.
- **`parquet-recipe`** — recipe v3 compression standards and the lossy movetext invariant.

## Repo layout

`README.md` is the public front door; this file is the technical companion. Keep them consistent
when entry points move.

`scratch/` is **gitignored**: ad-hoc probes, benchmark harnesses whose result is already baked in,
drivers for completed runs. Put new one-offs there rather than in `python/`, so a fresh checkout
shows only maintained pipeline and tests. Scripts there import first-party modules, so running one
needs `PYTHONPATH=<repo>/python`. Anything in `python/` is implicitly a claim that it is maintained;
if that stops being true, move it to `scratch/`.

## Data paths — critical

| Path | Role |
|------|------|
| `D:/data/chess/standard-chess-games-compressed/year=Y/month=M/event=E/` | **Canonical source.** Hive-partitioned parquets (2013–present). Every extraction starts here. |
| `F:/chess/standard-chess-games/data/` | Pre-compression archive only — **never a pipeline input.** F: is a USB spinning disk; never point DuckDB spill/temp at it either. Also the **only** copy retaining `[%clk]`/`[%eval]` movetext comments (recipe v3 strips them from `D:` at ingest). |
| `E:/chess/position-stats/` | Aggregated stats: `position_stats_*.parquet` (per-edge win/draw/loss counts), `position_stats_aux_*.parquet` (the per-position sidecar of unseen mass), `crush_hist_*.parquet` (histograms). `_pooled_partials_*/` are resumable working dirs; their `_monthly/` subdir is the **only** artifact carrying a year, so year-scoped pools depend on it surviving. |
| `E:/chess/eval_arrays/` | `unified_eval_db.parquet` materialised as sorted `.npy` (`eval_hash`, `eval_cp` int16) so extract workers mmap ONE shared resident copy instead of N private 4.8 GB loads. **Derived** — `eval_arrays.meta.json` fingerprints the source and readers verify, because a stale copy answers every query with the previous DB's evaluations and nothing downstream would notice. Rebuild: `python/eval_arrays.py [--force]`. |
| `E:/chess/repertoire/` | Stage-3 outputs, with `.meta.json` provenance sidecars recording the exact inputs/flags that built each one. |
| `E:/chess/unified_eval_db.parquet` | **Canonical eval DB.** `lichess_eval_db.parquet` (cloud-eval Stockfish, whose raw dump has been deleted — rebuilding needs a re-download from database.lichess.org) unioned with the aggregated fishnet-evals dump. Default `--eval-db` for Stage 3 and `build_sharp_reps.py`. |

**IMPORTANT: never reference `D:/data/chess/standard-chess-games/`** (without `-compressed`). It is
a deleted path, and pipelines that pointed at it silently produced incomplete output rather than
failing.

## Running things

All commands use `.venv/Scripts/python.exe` (uv-managed, Python 3.11) from
`C:/Users/David/Documents/Chess`. Every pipeline script carries an authoritative header docstring
with its exact CLI — **read it before running; don't trust remembered flags.**

```powershell
# Pooled stats build (extract, then merge; both resumable)
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year Y1 --end-year Y2 --phase extract
.venv\Scripts\python.exe python\build_pooled_stats.py --start-year Y1 --end-year Y2 --phase merge --tmp-dir D:\chess_duckdb_tmp

# Sharp repertoires from pooled stats
.venv\Scripts\python.exe python\build_sharp_reps.py

# Score a repertoire / browse it
.venv\Scripts\python.exe python\score_repertoire.py --repertoire <path> --stats <path>
.venv\Scripts\python.exe python\repertoire_explorer.py

# Did I break anything
.venv\Scripts\python.exe python\run_tests.py
```

There is no lint config or build step. Every `python/_test_*.py` is a standalone pass/fail script
(exit 0/1); `run_tests.py` subprocess-runs all of them with a summary. Convention: synthetic tests
import the production query/logic rather than copying it (`_test_winpos.py` is the template).

**Before changing an empirical constant** (batch sizes, memory limits, `min_games`), name the
failure you expect to fix and the test that would prove it. This codebase has many tuned constants
and changing one on intuition is guessing.

## Validation ladder

Four tiers, cheapest first. Each catches a class the tier below cannot, and **a change may not
reach a tier without passing the ones under it.**

| tier | cost | what only this tier catches |
|------|------|-----------------------------|
| 1 · `run_tests.py` | seconds | logic, contracts, schema drift, doc-path rot |
| 2 · real CLI over a slice (`--source`, one partition) | minutes | real movetext and real schemas; A/B equivalence of two implementations over identical input |
| 3 · single-month prototype (extract → merge → aux → small Stage-3) | hours | accounting identities, bucket mass shares, peak-RAM and wall-clock projections |
| 4 · full build | days | the answer itself — **requires explicit go-ahead; never launched on test-green alone** |

Rules that make the ladder load-bearing:

- **No tier may be skipped for high-blast-radius changes**: the value calculation (`value_node`,
  the aux buckets, the selection key), the columns the extract emits, or anything that changes a
  stored column's meaning. A wrong `child_hash` corrupts every downstream join and nothing fails.
- **A new flag defaults to off and must be an exact no-op** — rebuild both repertoires without it
  and diff against canonical at tolerance 0. Known exemption: `crush_rate` / `crush_potential`
  wobble ~2e-16 from a DuckDB parallel float SUM.
- **Measure across scales, never at a single slice.** One-slice tier-2 numbers have flipped a
  verdict here twice: the winpos fusion's marginal cost read +19%, then +34.6%, then +13.1%
  depending on which slice was measured. Report the trend, not the fastest run.
- Tiers 2 and 3 produce the numbers the go-ahead is based on; they do not replace it. No tier
  settles a statistical judgment call (is `--reply-shrink 1.0` right? does a confound invalidate a
  comparison?) — those need a human regardless of how green the suite is.

## Resumability conventions

Long runs die (crashes, OOMs, silent external kills) — every stage must survive a restart. Patterns
to preserve when modifying:

- Atomic writes: parquet goes to `<path>.parquet.tmp`, then `Path.replace()`. An output exists only
  if it is complete.
- Skip gates: every stage/chunk checks for its output and skips if present. Forcing a redo = delete
  the output (or its sentinel), rerun.
- Skip gates that check several outputs are **conjunctive**, so the kind renamed LAST is what
  reopens the gate after an interrupt. If you stop writing one kind, move that slot deliberately.
- Checkpoint expensive mid-stage results so a crash in a cheap later phase doesn't repeat a
  multi-hour one.
- **`min_games` must only be applied at the final aggregation pass** — filtering at an intermediate
  tier drops keys whose cumulative count would have qualified. Holds in both pipelines.

## Operational gotchas (the painful lessons)

### Concurrent memory budgets — the ceiling is COMMIT, not RAM
Commit limit on this machine is ~149 GB and a 3-worker extract holds ~111 GB of it. A second
process must be budgeted against what is **left**, not against physical RAM or total commit. A
12 GB DuckDB `memory_limit` probe launched during the extract drove commit to 134/149 GB and
tripped the watchdog. Check before launching anything large:
`Get-CimInstance Win32_OperatingSystem` → `TotalVirtualMemorySize − FreeVirtualMemory`.

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
  Fix: point the spill dir at a roomy NVMe volume (`--tmp-dir`). Never a USB HDD — spill is random
  I/O and will crawl.
- `could not allocate block ... (X/X used)` → the **memory_limit is genuinely exhausted**. Fix:
  fewer threads (smaller per-thread hash partitions) and/or a higher limit. Set
  `preserve_insertion_order=false` always. If it *still* OOMs at the full budget, the group
  cardinality doesn't fit RAM at all — partition the GROUP BY into disjoint key-hash buckets and
  union (`N_MERGE_BUCKETS` is the template; bucketing on a column of the grouping key keeps
  HAVING-filter semantics exact).

### Windows working-set trim vs. big DuckDB memory limits
A DuckDB process whose memory_limit approaches physical RAM while scanning hundreds of GB can get
its working set trimmed by Windows (file-cache standby growth wins), then crawl indefinitely on
hard page faults. The tell: private bytes ≫ working set (e.g. 97 GB vs 6 GB) **with plenty of free
physical RAM**, ~1 core busy, zero spill, zero output. The fix is a *lower* memory_limit (leave
~half of RAM for the OS cache when scanning big inputs), not a higher one — an OOM-adjacent failure
that looks like a hang rather than an error.

### Polars on Windows at scale
- `LazyFrame.sink_parquet()` on >10 GB inputs silently produces **no output file** (no exception).
  Use eager `collect().write_parquet()`; if you must stream, check file existence after every write.
- `collect(streaming=True)` crashes (0xC0000005) on >10 GB volumes. Use DuckDB for big aggregations.

### Long-running jobs on this machine
Processes attached to a session (including harness background jobs) die when the session ends, and
even detached processes have been killed with no OS trace. Launch pattern that works: write a
`.bat` and `Start-Process -FilePath <bat> -WindowStyle Hidden`, logging to a file. The real defense
is the resumability conventions above — assume any run can die silently and make restarts cheap.
Windows Task Scheduler has been unreliable; don't build on it.

## Notifications

`python/notify.py`: stdlib-only SMTP. Creds in `%USERPROFILE%/.chess_pipeline_notify.json` (outside
the repo; `notify_config.example.json` is the template) or `CHESS_NOTIFY_*` env vars. Silently
no-ops if unconfigured — pipelines run identically without it.
