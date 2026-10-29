# Handoff: DuckDB 2.0 port, and a README refresh

For a Claude Code cloud session on `davidnield/Chess`, branch `main` (cut a new branch, e.g.
`maintenance`). Two independent small jobs; no local data is available or needed. Read `CLAUDE.md`
first.

## 1. Lift the `duckdb<2` pin

DuckDB 2.0 removed `DuckDBPyConnection.pl()` (and relation `.pl()` if applicable; check the 2.0
changelog for the replacement, e.g. `.pl()` -> `.arrow()` + `polars.from_arrow`, or `.fetch_df`
equivalents). `requirements.txt` pins `duckdb>=1.5,<2` because eight test files fail under 2.0:
`_test_budget_allowed`, `_test_budget_frontier`, `_test_budget_gated_reach`, `_test_weak_stop`,
`_test_course_dedup`, `_test_explorer_policy_value`, `_test_bucket_month`, `_test_backfill_epd`.

- Find every call site (`grep -rn "\.pl()" python/`, also multi-line `con.sql(...)`/`execute(...)`
  chains ending in `.pl()`), and port them through ONE small helper (e.g. `duck_pl(rel)` in a shared
  module) that works on both 1.5 and 2.0, so a future API change is a one-line fix.
- Column types must not change: `.pl()` and an arrow round trip can differ (e.g. large_string vs
  string, decimal handling, int widths). `build_budget_books.py` and `explorer_policy_value.py` are
  on the value path, so check dtypes explicitly and say what you checked.
- Gate: the full `python python/run_tests.py` passes under BOTH duckdb 1.5.x and the current 2.x
  (two venvs), then relax the pin to `duckdb>=1.5`. Data-dependent tests skip without local data —
  say which.

## 2. Refresh `README.md`

`README.md` is the public front door; `CLAUDE.md` is the technical companion ("Keep them consistent
when entry points move"). `main` just gained three weeks of work: memorisation-budget books
(`build_budget_books.py`, `budget_core.py`: price search + `--price-dag-mode polish`, DP engine,
weak-us stop model, `--max-eval-loss`, `--incumbent` stickiness), opening-course benchmarks
(`course_*.py`, `explorer_policy_value.py`), the trainer app (`python/trainer_app/`, its own
README), plan-note annotations (`python/annotate/`), the Rust explorer extract
(`rust/explorer_extract/`), and the docs in `docs/`.

- Read the code's header docstrings (each pipeline script carries its exact CLI) and update
  README.md so a newcomer can see what the project does, the pipeline end to end, and the entry
  points, pointing at the owning scripts rather than copying flag lists (CLAUDE.md's maintenance
  rule). Keep it concise.
- Do not describe or link the owner's data paths beyond what CLAUDE.md already says, and do not
  include any opening-course text (copyrighted; it never leaves the owner's PC).
- `python/_test_docs_paths.py` checks doc paths; keep it passing.

## Deliverables

Commits on your branch (not `main`), and a short `docs/maintenance-report.md`: the call sites and
the helper, the dtype checks, the test runs under both DuckDB versions, and a summary of the README
changes.
