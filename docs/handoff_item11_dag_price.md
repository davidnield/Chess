# Handoff: a DAG-exact price optimiser (plan item 11, steps 1-2)

For a Claude Code cloud session on `davidnield/Chess`, branch `stability-and-fixes`. Self-contained:
everything below runs on synthetic graphs in the repo's own code; no local data (pools, eval
arrays, explorer books) is needed or available. Read `CLAUDE.md` first (conventions, validation
ladder) and the docstrings named below.

## The problem

`python/budget_core.py` builds memorisation-budget opening books ("learn at most B moves; maximise
expected score") with two engines:

- **DP / curve engine** (`build_curves`, `_node_curve`, `merge_weighted`, `extract_book`): value-vs-
  budget curves per node, merged bottom-up. Exact only under diminishing returns; it starves
  **lumpy** lines (a move gains ~0 until a deeper payoff): on 1,028 random trees it fell below the
  exact optimum in 65 cases (max 0.097), and on the real White b2000 graph it left whole first-move
  answers unprepared.
- **Price engine** (`price_book`, `price_search`, `policy_reach`, `policy_value`): Lagrangian, a
  price lambda per booked position, bisection on lambda to hit the budget. On TREES every book it
  returns is the best book of its own size (`_test_budget_price.py` A-C, held to an exact max-plus
  oracle). On DAGs it misprices **transpositions**: a position reached by several move orders is
  learned once (one budget unit) but its value accrues on every path. The current fix is heuristic
  (round 2 reprices with the book's total reach, `--price-seed source --price-reprice max
  --price-rounds 4 --price-select best`). On the real Black b2000 graph it stays 0.012-0.02 below
  the DP engine in-model (best variant 0.5945 vs DP 0.6067).

So White uses price and Black uses DP. Goal: **one engine that is (near-)exact on DAGs**, so both
colours use it.

## Step 1 — measure the gap where the optimum is known

1. Build an exact oracle for small DAGs with transpositions. The tree oracle in
   `_test_budget_price.py` (exact budget table, max-plus convolution at opponent nodes) does not
   apply to DAGs; on small graphs, exhaustive enumeration of booked-position sets (or an ILP if you
   prefer, but keep tests dependency-light) is fine. The objective is exactly `policy_value` of the
   book with each distinct booked position counted once against the budget (see how `price_book`
   and `policy_reach` count).
2. A random DAG generator in the style of the tree generator there: both colours to move at the
   root, mixed leaf values, lumpy chains, and transpositions (an opponent reply leading to a
   position also reachable by another move order). Include the hand cases D (transposition) and F
   (repetition cycle) from `_test_budget_price.py`.
3. Report, over a few hundred random DAGs at several budgets: gap to exact for `price_search`
   (current flags), for the DP engine (`build_curves` + `extract_book`), and how often each is
   exact. This is the baseline to beat. Commit it as a standalone test/report script (repo
   convention: `python/_test_*.py`, exit 0/1, imports the production code, no copied logic).

## Step 2 — candidate fixes, cheapest first

(a) **Charge shared positions once, by total reach, to a fixed point**: a node's decision depends on
its summed arrival reach, which depends on decisions above; iterate price_book with each node's
price scaled by the book's total reach until the booked set stops changing (watch for cycling;
damp or take the best visited).
(b) **Explicit "position booked" variables**: Lagrangian relaxation with one booking decision per
distinct position and consistency multipliers linking it to each arrival path.
(c) **Coordinate-ascent polish** after any solution: add/drop/swap single booked positions while the
in-model value improves within budget (cheap, may close most of the remaining gap).

Each candidate goes behind a new option that **defaults off and is an exact no-op when off**
(e.g. `price_search(..., dag_mode=None)`); existing tests must stay green. Measure each against
the step-1 oracle.

## Gate for this handoff

- On the random DAGs: the best candidate is within ~0 of exact (report max and mean gap, and the
  share exact), and never worse than both existing engines on any instance.
- Runtime: it must stay usable on real graphs of ~5M nodes (the White b2000 graph is 4.7M
  positions; price_search already takes ~75 min there), so report scaling on the largest synthetic
  graphs you can generate and flag anything worse than ~2-3x the current price_search.
- All `python/_test_budget_*.py` pass (`python python/run_tests.py` runs them; data-dependent tests
  elsewhere in the suite will skip or fail without local data — that's expected, note which).

Steps 3-4 (the real b2000 graphs, the convertibility evaluation, rebuilds) need the owner's PC and
data; stop after step 2 with a short report in `docs/` and commits on a new branch off
`stability-and-fixes` (e.g. `dag-price`). Do not merge or push to `main`.

## Environment

Python 3.11; `pip install -r requirements.txt` (polars, numpy, duckdb, python-chess, ...). Set
`PYTHONPATH=python`. The budget tests are synthetic and run in seconds to minutes.
