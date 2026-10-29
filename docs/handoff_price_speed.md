# Handoff: make the price search faster, bit-identically

For a Claude Code cloud session on `davidnield/Chess`, branch `main` (cut a new branch, e.g.
`price-speed`). Self-contained: synthetic graphs only; no local data is available or needed. Read
`CLAUDE.md` (especially "Measure across scales, never at a single slice"), `docs/dag-price-report.md`
and the docstrings of `budget_core.price_search`, `price_book`, `policy_reach`, `policy_value`,
`topo_up`.

## Why

Every memorisation-budget book build runs `budget_core.price_search` on graphs of up to ~5M
positions. On the owner's PC (single-threaded Python): White b2000 (4.7M positions) takes about
65 min with `--price-rounds 1 --price-dag-mode polish`, 75+ min with four rounds; Black (smaller)
about 13 min. Most of the time is repeated `price_book` solves (bisection on the price lambda: 11
solves with one round, ~77-120 with four) and `policy_reach` / `policy_value` walks, each O(graph)
in pure-Python dict loops. Experiments cost hours per variant, so a 3-5x speed-up compounds.

## Task

1. **Profile** `price_search` on `_test_budget_dag.py --scale` graphs (`big_dag`, 100k and 1M
   nodes, targets 400 and 2,000) with the production flags (`rounds=1, select="best",
   reprice="max", seed_reach=<source seed>, dag_mode="polish"`), and report where the time goes.
2. **Speed it up without changing any result.** Likely candidates: a compact integer-indexed
   (CSR / numpy) representation of `Graph` built once, so `price_book` and `policy_reach` become
   array sweeps in topological order instead of dict walks; reusing the topological order across
   solves; warm-starting the lambda bisection. Keep the public functions' signatures, or add a
   compiled-graph object that the existing ones accept.
3. **Exactness gate:** for every random DAG and budget in `_test_budget_dag.py` and every graph in
   `_test_budget_price.py`, the new code returns the SAME booked set, value and sweep as the old
   (tolerance 0 on the booked moves; values equal up to float summation order, and if summation
   order changes say how much). Keep the old implementation callable (e.g. behind a flag or in the
   test) so the comparison stays in the suite.
4. **Report scaling**, not one number: wall-clock old vs new at 10k / 100k / 1M nodes and both
   targets, peak memory (the owner's commit limit is ~136 GB and a White build peaks at ~51 GB; a
   faster version must not peak meaningfully higher).

Constraints: Python 3.11 + numpy (numba or Cython only if the gain is large, and say why); no
multiprocessing pools with `max_tasks_per_child` (CLAUDE.md explains the Windows deadlock). `python
python/run_tests.py` stays green; data-dependent tests skip without local data — say which. Pin
`duckdb<2` as `requirements.txt` does.

## Deliverables

Commits on your branch (not `main`) and `docs/price-speed-report.md`: the profile, the change,
the exactness evidence, the scaling table, and anything the owner should check on the real
graphs (one rebuild of the White book with old and new code, diffed at tolerance 0).
