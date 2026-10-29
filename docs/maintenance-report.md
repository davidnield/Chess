# Maintenance report: the DuckDB 2.0 port and a README refresh (2026-10-29)

Brief: `docs/handoff_maintenance.md`. Branch `claude/laughing-brahmagupta-26z6vd`, cut from `main` at
56f92b6. This ran in a Linux cloud session with no local data. It used Python 3.11.15 in two venvs
built from `requirements.txt`, identical except for DuckDB: 1.5.6 in one and 2.0.0 (the latest
release) in the other. Both had polars 2.1.0 and pyarrow 26.0.0. I also built the Rust
`explorer-extract` (`cargo build --release --locked`, toolchain 1.98.1; `selftest` passes) so that
the three tests that need it ran instead of skipping.

## Outcome

- **The fetch API is ported.** Every DuckDB result that becomes a frame now goes through
  `python/duck_compat.py`. That covers 17 call sites in 8 files, and it works on 1.5 and 2.x.
  Column types are unchanged at every site: the checks are below.
- **2.0 broke one more thing, now fixed.** `verify_evals.py` crashed on 2.0 because of the new
  open-result rule. The test that catches it skips without the exe, which is why the brief's list
  missed it.
- **The pin is not relaxed.** The gate was "the full suite passes under both versions". On 2.0 it
  does not. DuckDB 2.0's parquet writer marks a column REQUIRED wherever the plan's statistics prove
  it holds no NULLs, where 1.5 marks every column OPTIONAL. `_test_bucket_month` fails on that
  alone (no exe needed), and so does `_test_rust_extract` with the exe. The brief's list of eight
  missed it because `_test_bucket_month` crashed earlier, at `fetch_arrow_table()`. The comment on
  `requirements.txt` now gives this reason, CLAUDE.md has it as a gotcha, and the options are below.
- **The README is refreshed** (section 2).

## 1. DuckDB 2.0

### What 2.0 changed that this repo touches

Each item was measured on 2.0.0 against 1.5.6 with probes, and checked against the
[duckdb-python v2.0.0 release notes](https://github.com/duckdb/duckdb-python/releases/tag/v2.0.0).

| change | effect here |
|---|---|
| `.pl()`, `.arrow()`, `.fetch_arrow_table()` and `.fetch_record_batch()` are gone from `con.execute()` results | the eight failing test files; 17 call sites (below) |
| `execute().to_arrow_table()` still works, but copies the rows twice and raises a `UserWarning` | 2 sites in `verify_evals.py`, ported too |
| `fetchone()`, `fetchmany()` and `execute()` are gone from relations | no site: the repo never used relations before the helper |
| **Open results.** `execute()` starts a query and rows are read as you fetch. A SELECT not read to its end (`.fetchone()` on it, even on a one-row result) keeps the connection busy. Until the next `con.execute()` replaces it, `register()`, `unregister()` of a registered view, relation fetches, `load_extension()` and `create_function()` raise `ResourceInUseException`. A later `execute()`, `executemany()`, `fetchall()` or `fetchnumpy()` is fine. | `verify_evals.py`'s in_book loop (fixed below). Every other `register` / `unregister` and every helper call follows a write, a `fetchall()` or a fresh connection; I checked each. |
| **Parquet nullability.** The writer marks a column REQUIRED when the plan's statistics say it holds no NULLs, so the schema depends on the data and the plan. `SET disabled_optimizers='statistics_propagation'` (or `enable_optimizer=false`) restores 1.5's all-OPTIONAL output. No `COPY` option or setting controls it, and neither changelog mentions it. | see "Why the pin stays" |
| `con.sql()` on DDL/DML returns a relation; `write_parquet(overwrite=False)` refuses an existing file; the expression API's string rules | unused here |

Relation `.arrow()` returns a `RecordBatchReader` on both versions, so 2.0 did not change that.

### The helper

`python/duck_compat.py`, two one-line functions:

```python
def duck_pl(con, sql: str) -> pl.DataFrame:      # was con.execute(sql).pl()
    return con.sql(sql).pl()

def duck_arrow(con, sql: str) -> pa.Table:       # was con.execute(sql).fetch_arrow_table()
    return con.sql(sql).to_arrow_table()
```

`con.sql(sql).pl()` and `.to_arrow_table()` exist on 1.5 and 2.x. They are the replacements the 2.0
notes name, and on a relation they make no extra copy. A site that fetched polars still gets
DuckDB's own polars export, and a site that fetched arrow stays on arrow, so no caller switches
between the two exports (they can differ, e.g. HUGEINT). The dtype checks below confirm the frames
are the legacy calls' frames. The module
docstring states the open-result rule, because the helpers are among the calls it refuses.

### Call sites

| file | function | before | after |
|---|---|---|---|
| `weak_stop.py` | `_edges` | `execute(...).pl()` | `duck_pl` |
| `build_budget_books.py` | `collect_subgraph_edges` | `execute(...).pl()` | `duck_pl` |
| `explorer_policy_value.py` | `HoldoutIndex.__init__` (`our`, `tot`, `opp`, `stops`) | `execute(...).pl()` ×4 | `duck_pl` ×4 |
| `course_dedup.py` | `overlaps` | `execute(...).pl()` | `duck_pl` |
| `build_baseline_books.py` | `edges_for` (no test reaches it) | `execute(...).pl()` | `duck_pl` |
| `backfill_epd.py` | `_seed_task`, `_replay_task`, `_pending_task`, `_shrink_task`, `_missing_task` | `execute(...).fetch_arrow_table()` ×5 | `duck_arrow` ×5 |
| `pool_from_book_gates.py` | `gate_reimpl`, `gate_evals` (no test reaches them) | `pl.from_arrow(execute(...).arrow())` ×2 | `pl.from_arrow(duck_arrow(...))` ×2 |
| `verify_evals.py` | `main`, the ep-variant and strata samples | `execute(...).to_arrow_table()` ×2 | `duck_arrow` ×2 |

To find them I grepped `python/` for every method 2.0 removed from either a result or a relation
(`pl`, `arrow`, `fetch_arrow_table`, `fetch_record_batch`, `fetchone`, `fetchmany`,
`fetch_df_chunk`, `execute`), multi-line chains included, and for `to_arrow_table` /
`to_arrow_reader`. `fetchone()` is still fine on `execute()` results, which is how every remaining
site uses it.

The extra fix, in `verify_evals.py`'s in_book loop: it registered a view `pp`, read a COUNT with
`.fetchone()[0]`, then unregistered `pp`. On 2.0 the unread result made the `unregister` raise. It
now reads with `.fetchall()[0][0]`. Before the fix, `_test_verify_evals` with the exe failed two
checks on 2.0 with that traceback. After it, the test passes on both versions.

### Dtype checks

The brief asked for an explicit check on `build_budget_books.py` and `explorer_policy_value.py`,
since both are on the value path. I checked every site, two ways.

1. **Every call the test suite makes.** A `sitecustomize` shim (scratch only, not committed)
   wrapped `duck_pl` and `duck_arrow` whenever a process imported `duck_compat`, forked pool
   workers and subprocesses included. On each call it recorded four things: the polars schema,
   the arrow schema for arrow sites, the row count, and a digest of the rows sorted on every column.
   On 1.5 it also ran the legacy call (`execute(sql).pl()` or `.fetch_arrow_table()`) on the same
   connection and recorded it the same way. Then the whole suite ran under it in both venvs, with
   the exe.
2. **The sites no test reaches,** on inputs with the production schemas. The pool used
   `pool_from_book.POOL_SCHEMA` (large_string, int32 `ply`), the book ps files used book-v1
   (`merge.rs` `book_ps_schema`, every column REQUIRED), and the explorer month and term table used
   `keys.rs` `ps_schema(false)` and `term_schema`. On those I ran `edges_for` (zero-row frontier
   included), `collect_subgraph_edges` and its returned frame, `weak_stop._edges`,
   `HoldoutIndex.__init__`, and the two `pool_from_book_gates` queries verbatim against their
   exact legacy expression, `pl.from_arrow(con.execute(sql).arrow())`.

Results:

| check | calls | result |
|---|---|---|
| 1.5: each new frame vs the legacy call on the same connection | 830 (809 from the suite, 21 from the second check) | Identical polars schema, arrow schema, row count and sorted-row digest at every call. |
| 2.0's new frames vs 1.5's legacy frames, per test and call site (the multiset of every call's schema, rows and digest) | 809 + 809 from the suite, 15 test×site pairs; about 22 + 22 from the second check | Identical at every site. The one exception is how many per-level fetches `collect_subgraph_edges` makes, which follows its run-to-run level count (below); its returned frame is identical. `backfill_epd._replay_task`'s `list()` columns are compared as each row's sorted (san, c0, c1) tuples: those aggregates have no ORDER BY, so element order inside a list is undefined on either version (the three lists of a row stay aligned). The two backfill tests were rerun with that digest (734 + 734 calls). |
| `ORDER BY` sites | every `opp` frame in both runs; `overlaps` on the test fixture and 40 random courses (345 pairs) | `opp` is sorted on `parent_hash` and `overlaps` on `jaccard` descending, on both versions. |
| Row order where the SQL leaves it unspecified | the same join and GROUP BYs on 2M rows over 40 row groups, 10 runs per path | Run to run it varies through every path, the legacy `execute().pl()` on 1.5 included (10 distinct orders in 10 runs). On small inputs every path gives the same order. The port changes neither the content nor that property. |

What the types are, unchanged on both versions and identical to the legacy calls:

| site | polars schema of the frame |
|---|---|
| `collect_subgraph_edges` | parent_hash i64, move_san str, child_hash i64, parent_epd str, total/white_wins/draws/black_wins i64, white_score_avg f64 |
| `HoldoutIndex` `our` | parent_hash i64, san str, child i64, n f64, ws f64 |
| `HoldoutIndex` `tot` / `opp` / `stops` | parent_hash i64 [, child_hash i64 / position_hash i64, kind i32], n f64, ws f64 |
| `weak_stop._edges` | parent i64, child i64, n f64 |
| `edges_for` | as `collect_subgraph_edges` plus ply i32, without white_score_avg |
| `gate_reimpl` arrivals | child_hash i64, w/d/b/t Decimal(38,0): SUM over int64 is HUGEINT, on both versions, as before |
| `gate_evals` below-floor | parent_hash i64, move_san str, child_hash i64, total i64 |
| `backfill_epd` (arrow) | int64 hashes, `string` (not large_string) text, `list<string>` / `list<int64>` in `_replay_task` |

Order: `HoldoutIndex`'s `opp` relies on `ORDER BY 1` (it binary-searches `parent_hash`). Every
recorded `opp` frame was sorted on `parent_hash`, on both versions. Ties within one `parent_hash`
come back in any order, on 1.5 as before.

One thing that looked like a difference and is not: `collect_subgraph_edges` logs a "reopened N
node-visits" count. The count, and sometimes the number of BFS levels (8 or 9), change from run to
run, and the original code does the same: on 1.5 at 56f92b6, 20 runs gave "reopened 1" 14 times and
5 to 31 otherwise. The cause is most likely the collector's own `df.group_by("parent_hash")`
(polars, no `maintain_order`), since DuckDB's row order for that query was identical in 20 runs
through every fetch path. The frame the collector returns had the same digest in every run, before
and after the port, on both versions.

### Test runs

`run_tests.py`, all 68 files, on the final code. The exe columns set `EXPLORER_EXTRACT_EXE` to the
built `explorer-extract`, so `_test_rust_extract`, `_test_verify_book` and `_test_verify_evals`
run instead of skipping.

| run | DuckDB 1.5.6 | DuckDB 2.0.0 |
|---|---|---|
| before the port (56f92b6), no exe | 68/68 | 60/68: the brief's eight |
| final code, no exe | **68/68** | 67/68: `_test_bucket_month` |
| final code, with the exe | **68/68** (`_test_rust_extract` 39/39, `_test_verify_book` 10/10, `_test_verify_evals` 7/7) | 66/68: `_test_bucket_month`, `_test_rust_extract` |

On 2.0 every remaining failing check is a parquet schema check, and the rows match in each:

- `_test_bucket_month`: "the same bucket-file schema" and "compare_explorer_outputs month:
  backfill vs bucket_month". The compare's own log reads "rows (every column but ply): IDENTICAL"
  and fails only on `bucket schema`. `parent_epd` is `not null` in `bucket_month`'s month (no NULLs
  in its input) and nullable in the backfill's, whose plan filled NULLs in. Every other column is
  `not null` on both sides.
- `_test_rust_extract`: "Rust month vs B1" fails only on `bucket schema`: B1's DuckDB-written
  columns are `not null` and the Rust month's are nullable. Its rows are identical too.

`_test_verify_evals` with the exe failed on 2.0 before the `verify_evals.py` fix (the open-result
crash) and passes after it.

Data-dependent tests that skip here: `_test_docs_paths` (CLAUDE.md's data paths; none of D:/E:/F:/G:
exist, so all 10 paths are SKIPs), and the three exe tests in the no-exe columns. Everything
else runs on synthetic fixtures.

### Why the pin stays, and the options

What 2.0's writer change breaks:

- **Producer equality.** `compare_explorer_outputs.py` compares schemas with
  `pyarrow.Schema.equals`, nullability included. Months written by DuckDB (the backfill, and
  `bucket_month` for B1) no longer equal Rust months, and two DuckDB months can differ from each
  other: in `_test_bucket_month` the backfill's month and `bucket_month`'s month hold the same rows
  under different nullability.
- **The Rust merge.** It refuses a month whose arrow fields are not `ps_schema(false)`
  (`merge.rs`, `PsIn::open`), and arrow-rs field equality includes nullability. In practice it reads
  only Rust months (only the Rust tool writes ply-keyed months), but CLAUDE.md's contract ("the A2 merge reads any producer's month") would no longer
  hold for a DuckDB-written one.
- **Nothing else I could find.** DuckDB, polars and pyarrow read REQUIRED columns fine (the book
  itself is all REQUIRED). `pool_from_book`'s own schema check compares names and types only. The
  one `pa.concat_tables` (`process_pgn_parquets.py`) concatenates pyarrow-read source data.

Options, for the owner:

1. **Keep `duckdb<2`** (this branch). Nothing changes, and the code is ready for 2.x.
2. **Restore OPTIONAL at the writer.** Run `SET disabled_optimizers='statistics_propagation'` on
   the connections that `COPY` parquet (`build_pooled_stats._duck`, `pool_from_book._duck` and the
   ad-hoc ones). It is exact in my probes, but it changes planning on the multi-billion-row merges
   (filter pruning and the aggregate optimisations statistics feed), so it needs tier-2/3 timings.
   CLAUDE.md calls that an empirical constant.
3. **Make the consumers ignore nullability.** That means `compare_explorer_outputs.py`'s schema
   checks, the Rust merge's field check, and `_test_bucket_month`. It is a contract change, it
   needs a Rust rebuild, and a book is continued only by the exe that began it.

## 2. README

`README.md` had described the pipeline as it stood before three weeks of work on `main`. Changes:

- **Intro:** one paragraph on what happens to the full book: it is cut to a memorisation budget,
  checked against held-out games and courses, and drilled in the trainer.
- **New "How the budget books are built"** (under "Fitting the book in your head"): the `dp` and
  `price` engines and `--price-dag-mode polish`, the out-of-book models (`--stop-model
  weak|empirical`, `--weak-eval-curve`), `--max-eval-loss`, `--incumbent` / `--stick-margin`, and
  `--allowed-moves`. Each points at `build_budget_books.py`'s header and `budget_core.py` rather
  than listing flags.
- **New "Measuring a book":** `replay_holdout.py`, `explorer_policy_value.py`, the baselines and
  the in-sample scorecards. The opening-course benchmarks (`course_*.py`) get one paragraph that
  says course text stays on the owner's machine. No course text and no course names are included.
- **"The pipeline"** (the second "How it works" heading, renamed): the Rust explorer-book path
  (`explorer-extract month` and `merge`, `pool_from_book.py`), the eval DB (`explorer-extract
  evals`, `eval_arrays.py`), budget books, annotations and the trainer.
- **Repo layout:** the new modules, `rust/explorer_extract/` and `docs/`.
- **Setup, running it, tests:** building the Rust extract (pointing at its README), a budget-book
  and course-benchmark command with placeholder paths, an annotations paragraph, and how the Rust
  tests skip without the exe.
- **The owner's explanatory sections** are unchanged apart from three typos ("worth nothing",
  "chosing", an unclosed parenthesis).

No owner data paths were added: the only paths are the ones the README already had, which
CLAUDE.md's "Running things" also has. `_test_docs_paths.py` reads only CLAUDE.md's data-paths
table, and it still passes.

## Commits

1. `Fetch DuckDB frames through duck_compat, which works on 1.5 and 2.x`: the helper, the 17 call
   sites, and the `verify_evals.py` fix.
2. `Keep duckdb<2 for 2.0's parquet nullability, not for the fetch API`: the `requirements.txt`
   comment and a CLAUDE.md gotcha (the helper rule, the nullability change, the open-result rule).
3. `README: budget books, holdout and course benchmarks, the Rust extract`.
4. This report.

To reproduce the dtype check, build the two venvs. The shim and its comparison script were
throwaway scratch files. The method is in "Dtype checks": wrap the two helpers so each call also
runs the legacy call on 1.5, record schema, rows and a sorted digest, then compare per call site
across the versions.
