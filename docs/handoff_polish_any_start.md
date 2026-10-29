# Handoff: polish from any starting book, and polish with stickiness

For a Claude Code cloud session on `davidnield/Chess`, branch `main` (cut a new branch, e.g.
`polish-start`). Self-contained: everything runs on the synthetic graphs in
`python/_test_budget_dag.py`; no local data is available or needed. Read `CLAUDE.md`, then
`docs/dag-price-report.md` (the previous cloud session's report, which built polish), then the
docstrings of `budget_core.price_search` and `budget_core.polish_book`.

## Background

`budget_core.polish_book` is an exact-verified coordinate ascent (line / fill / swap / rebook /
switch / exchange moves) that runs after `price_search(..., dag_mode="polish")`. On the synthetic
DAGs it reaches the exact optimum on 99.6-100% of instances. On the owner's real graphs
(2026-10-28):

| 2,000-move book, in-model | price alone | price + polish | DP engine (`build_curves` + `extract_book`, match-distinct) |
|---|---|---|---|
| White (4.7M positions) | 0.6516 | **0.6664** | broken on White (starves lumpy lines) |
| Black (2000+ pool) | 0.5922 | 0.6005 | **0.6011** |

White switched to price+polish. Black stays on DP: polish lands 0.0006 below it in-model, and
scored 0.004-0.006 below it on the owner's out-of-sample evaluation. Also measured: one repricing
round + polish equals four rounds on all 5,169 synthetic instances, so `--price-rounds 1` is the
recipe with polish.

Two gaps:

1. **Polish only starts from the price search's own books** (the selected book, the best book
   that fits, the smallest over target). Starting it ALSO from an arbitrary book, e.g. the DP
   engine's, would make "DP then polish" never worse than DP in-model, and possibly better.
2. **Polish ignores stickiness.** `build_budget_books.py` refuses `--price-dag-mode polish` with
   `--incumbent` (rebuild stickiness: keep the previous book's move unless a new one is better by
   more than `--stick-margin`; see `budget_core.incumbent_move` / `_bonus`). The owner memorises
   these books, so future rebuilds need polish to respect it.

## Tasks

1. **Polish from a given book.** Add an option (default off, an exact no-op when off) so polish
   can start from a caller-supplied book, and expose it in `build_budget_books.py` for the DP
   method (e.g. `--method dp --polish`: extract the DP book, then polish it to the same target).
   Check on the random DAGs of `_test_budget_dag.py`: DP+polish is never below DP, and report how
   often it reaches the exact optimum vs DP alone and price+polish. Keep the region evaluator
   (`_book_region`) the only value used to accept a step.
2. **Polish with stickiness.** Define the objective polish maximises under `--incumbent`
   consistently with what the price search and `incumbent_move` already do (a margin credited to
   keeping the incumbent's move), so a step that swaps an incumbent move for a new one must beat it
   by more than the margin. Then lift the refusal in `build_budget_books.py`. Test: with a margin
   of 0 polish is unchanged (exact no-op); with a large margin polish never changes an incumbent
   move that the starting book kept; report churn vs value on the random DAGs at margins
   0 / 0.005 / 0.01 / 0.02.
3. Every new option defaults off and leaves existing outputs bit-identical; `python
   python/run_tests.py` stays green (data-dependent tests skip without local data — say which).
   Pin `duckdb<2` as `requirements.txt` already does.

## Deliverables

Commits on your branch (not `main`), a short report `docs/polish-start-report.md` with the
tables above filled in, and the exact `build_budget_books.py` command lines the owner should run
on the real Black and White graphs. Runtime note: polish adds ~1 min to a 13-65 min build at
4.7M nodes; flag anything that would add more than a few minutes.
