# DAG-exact price optimiser — plan item 11, steps 1–2

Branch `dag-price` (off `stability-and-fixes`). Brief: `docs/handoff_item11_dag_price.md`.
Everything here ran on synthetic graphs in a cloud session (Python 3.11 venv from
`requirements.txt`); no pools, eval arrays or books were touched. Steps 3–4 (real b2000
graphs, rebuilds) are for the owner's PC.

**Verdict:** candidate (c), polish (`price_search(..., dag_mode="polish")`), passes the
quality gate. On 300 random DAGs (1,302 graph×budget instances) it matches the exact
optimum on every instance. On three held-out seeds it matches on 99.6–99.9%, with a worst
gap of 0.016. It never scores below both existing engines on any instance. Its run time
does not grow with graph size (see Runtime): it adds ~5–35 s on top of the price search
on graphs of 100k–1M nodes. Candidate (a), the fixed point, is not worth it: it adds
little and costs 3–10× the search. Candidate (b) was not built (see below).

## Step 1 — the oracle and the baseline

`python/_test_budget_dag.py` (standalone, exit 0/1, imports production code only):

- **Exact oracle** `exact_dag`: exhaustive enumeration of books. Walking from the root,
  it branches at the first reached undecided our-node (in top-down order) on "stop" or
  each candidate, so every reachable book is visited exactly once. The value is
  `policy_value` (the shared evaluator). The cost is the number of distinct positions
  the book plays, counted once however many paths reach them. Checks: it equals
  `_test_budget_price`'s tree oracle on random trees, and gives the hand answers on
  case D (transposition), case F (repetition cycle, both hash orders) and T8 (two
  replies into one position).
- **Random DAG generator** `random_dag`: either colour to move at the root, mixed leaf
  values, lumpy chains (a forced 2–3 move line that only pays when complete), and
  transpositions of both kinds: an opponent reply into a position another branch
  reaches, and one of our moves into another branch's opponent node (including
  duplicate reply edges into one position). Up to 18 our-nodes; budgets 1, 2, 3, 5, 8
  and 12.

Baseline at the development seed (300 DAGs, 246 with a transposition, 1,302 instances).
**fits** = the book is within budget; **exact** = it fits and equals the optimum;
**gap** = optimum(budget) − value, over the books that fit; **own** = optimum(its own
size) − value.

| engine | fits | exact | gap mean | gap max | own mean | own max |
|---|---|---|---|---|---|---|
| price (production DAG flags¹) | 87.6% | 76.7% | 0.00268 | 0.0958 | 0.00030 | 0.144 |
| price (defaults) | 88.2% | 77.7% | 0.00245 | 0.0958 | 0.00012 | 0.051 |
| dp (`build_curves` + `extract_book`) | 100% | 88.7% | 0.00241 | 0.247 | 0.00013 | 0.038 |
| dp `--leftover` | 100% | 90.3% | 0.00202 | 0.247 | 0.00018 | 0.038 |

¹ `--price-seed source --price-reprice max --price-rounds 4 --price-select best`. The
"source rep" is the unbudgeted best book (`price_book` at a vanishing price), standing
in for the Stage-3 book.

Where the price engine loses:
1. **Size.** One book in eight overshoots the budget, because the Lagrangian hull jumps
   past it (a chain flips in whole). The sizes in between are unreachable at any price.
   Below a hull point the best book is often a *different* line, not a truncation.
2. **Transpositions.** Duplicate reply edges and re-merging lines make the max-path
   reach undercount what a position is worth. Re-pricing fixes only positions the book
   already reaches.

The DP engine's losses are the documented fused-atom ones (worst 0.247).

## Step 2 — the candidates

All three sit behind `price_search(..., dag_mode=None)`. The default is unchanged code:
check N pins that `dag_mode=None` returns the same book, value and sweep. The other
`_test_budget_*.py` files pass unmodified.

| engine | fits | exact | gap mean | gap max | below both | below the better |
|---|---|---|---|---|---|---|
| (a) `fixpoint` | 100% | 83.9% | 0.00401 | 0.135 | 1 | 164 |
| (c) `polish` | 100% | **100%** | **0** | **0** | **0** | **0** |
| (a)+(c) `fixpoint+polish` | 100% | 100% | 0 | 0 | 0 | 0 |

Held-out seeds (the development seed above is where polish was tuned):

| seed / transposition rate | instances | polish exact | gap mean | gap max | below both | below the better |
|---|---|---|---|---|---|---|
| 1 / 0.35 | 1,298 | 99.9% | 0.00001 | 0.0143 | 0 | 0 |
| 2 / 0.65 | 1,081 | 99.9% | 0.00001 | 0.0158 | 0 | 0 |
| 3 / 0.15 | 1,488 | 99.6% | 0.00002 | 0.0129 | 0 | 4 |

On the same seeds the price engine was exact on 76–78% and DP on 87–91%.
`fixpoint+polish` was within ±0.1% of `polish`.

### (a) Charge shared positions once, to a fixed point

Each solve re-prices the positions its book reaches at the book's own total reach and
re-solves. It stops when the booked set repeats, or after `fixpoint_iters` (8)
iterations, keeping the iterate with the best value − λ·size. It then picks the best
book of any solve that fits. Fitting fixes the overshoot (fits 100%), but exactness
only rises from 77% to 84%. The fixed point never reprices positions the book does not
reach, and it cannot reach the off-hull sizes. It is also 3–10× slower (Runtime), so
it is not recommended.

### (b) Explicit "position booked" variables

Not built. With positional policies the per-path decisions are already shared, so the
only thing a booking variable changes is how each node's price is scaled: λ/r(u),
where r is what (a) iterates on. A subgradient on per-node multipliers would be a
smarter search over the same r. It would still be a Lagrangian, still blind to
off-hull sizes, and (a) showed that getting r right is not where the gap is.

### (c) Coordinate-ascent polish — `budget_core.polish_book`

Runs after the price search, from three starts: the selected book, the best book that
fits, and the smallest book over the target. It returns the best result.

1. **Trim** an over-budget start by dropping the leaf that loses least.
2. **Ascend** with exactly verified moves. Each step proposes, then verifies the best
   proposals exactly (at most `max_verify` = 24, and 4 more after the first that
   improves). It takes the best verified improvement that stays within the target, and
   stops when none improves. The moves are:
   - **line** — book up to `line_k` (8) moves below a reached unbooked position. This
     uses an exact max-plus table over that position's cone, so a lumpy chain that pays
     only when complete is one proposal.
   - **fill** — the best *combination* of lines over the frontier, up to the slack.
     Several independent single moves can beat one 2-move line, and greedy choice
     among single proposals misses that.
   - **swap** — drop the d cheapest leaves and fill the slack + d that frees.
   - **rebook** — replace a leaf's move with a line.
   - **switch** — change a booked position's move.
   - **exchange** — drop a non-leaf together with everything only it reaches, and fill
     the budget that frees. Moves below it stay in the book where something else still
     reaches them, which is how a transposition gets re-routed.

Every verification uses the **region evaluator** `_book_region`. It is `policy_value`
and `policy_reach` restricted to the positions the book reaches, in the same order with
the same sums. Check E pins it to `policy_value` bit for bit, including case F's cycle.
Its cost scales with the book, not the graph. Value never goes down, and the size never
exceeds the target (check P).

Each improvement on the way to 100%, measured at the development seed:

| step | exact |
|---|---|
| prototype single add / drop / switch / swap | 95% |
| trim to the best fitting single drop | 98.6% |
| line moves | 99.5% |
| branching (max-plus) line tables | 99.8% |
| fill combinations | 99.8% (fixpoint+polish 100%) |
| exchange | 100% |

The proposal estimates are exact for a single move on a DAG: W(u) × (new local value −
old). Multi-move estimates are approximate, which is why every accepted step is
verified.

Limitations:
- **Stickiness is not applied.** Polish compares true values, so
  `build_budget_books.py` refuses `--price-dag-mode polish` with `--incumbent`.
- **Cycles.** The size counted is the number of reached booked positions (price_book's
  `spent_distinct`). On a repetition cycle this can differ from `policy_value`'s DFS
  `played` count, which also follows the cut back edge. That is the existing
  convention.
- **Line tables below the first level are book-free.** They use stop values only,
  memoised for the whole run, and replies whose optimistic reach is below half the
  cheapest booked leaf's loss are not explored. Rebuilding the full depth-8 cone every
  step cost 98 s per polish at 1M nodes; this costs 5.8 s for an equal-or-better book.
  The held-out exact share moved by about 0.1%.

## Runtime

`_test_budget_dag.py --scale SIZES TARGETS` times `big_dag` graphs: the same node shapes
grown breadth-first, with stop values just under the best move so that large books
compete for the budget. Times are single-threaded in this container, for the full
`price_search` with the production flags; ×price is the ratio to the same search
without `dag_mode`.

| nodes | target | price | fixpoint | polish | fixpoint+polish | value: price → polish |
|---|---|---|---|---|---|---|
| 10k | 400 | 0.8 s | 2.1 s (2.8×) | 1.4 s (1.8×) | 2.4 s (3.1×) | 0.49864 (404 positions, over) → 0.49892 (400) |
| 10k | 2,000 | 0.1 s | 0.2 s (2.0×) | 0.1 s (1.2×) | 0.2 s (2.5×) | 0.49902 → 0.49902 (graph holds 1,000 worth booking) |
| 100k | 400 | 6.1 s | 39.3 s (6.5×) | 7.7 s (1.3×) | 41.5 s (6.8×) | 0.63794 → 0.63875 |
| 100k | 2,000 | 12.4 s | 44.3 s (3.6×) | 17.3 s (1.4×) | 42.6 s (3.4×) | 0.63911 (2,013, over) → 0.63912 (2,000) |
| 1M | 400 | 100.6 s | 1,001.6 s (9.8×)² | 109.9 s (1.09×) | — | 0.58021 (403, over) → 0.58061 (400) |
| 1M | 2,000 | 99.7 s | — | 151.6 s (1.52×) | — | 0.58143 (2,016, over) → 0.58164 (2,000) |

² From the run before the line-table fix. The fixed point does not use the line tables,
so its time is unaffected. The other fixpoint cells at 1M were not timed.

Polish adds 1–9 s at target 400 and 5–52 s at 2,000, because what it works on is the
book and its frontier, not the graph. The search itself is O(graph) per solve. So the
ratio stays at or below 1.5× from 100k nodes up (1.8× at 10k, where the whole search
takes under a second). On the White b2000 graph (4.7M positions, about 75 min of
search) the extra should be a minute or two. Polish also always lands exactly on the
target, where the price search overshot it in four of these six cases. The fixed point multiplies every solve by up to `fixpoint_iters`
re-solves: 3–6× at 100k and 9.8× at 1M, so it is flagged as too slow.

## Tests

`python run_tests.py` here: **68/68 pass** (the data-dependent tests skip without local
data), including every `_test_budget_*.py` and the new `_test_budget_dag.py` (about 25 s).

One thing about the environment: without an upper bound, a fresh `pip install -r
requirements.txt` resolves **DuckDB 2.0.0**, which removed `DuckDBPyConnection.pl()`.
Under it, eight test files fail: `_test_budget_allowed`, `_test_budget_frontier` and
`_test_budget_gated_reach` (all on `.pl()`), `_test_weak_stop`, `_test_course_dedup`,
`_test_explorer_policy_value`, `_test_bucket_month` and `_test_backfill_epd`.
`_test_budget_allowed` fails the same way on the base branch. With DuckDB 1.5.6 all
eight pass, so this branch pins `duckdb>=1.5,<2` in `requirements.txt` (the "Pin
duckdb<2" commit, cherry-picked from `claude/gracious-shannon-tn281z`). Lifting the pin
means porting the eight `.pl()` call sites, and `explorer_policy_value` and the budget
books are on the value path, so that port goes through the validation ladder.

## For step 3

`build_budget_books.py --method price --price-dag-mode polish` (plus the production
price flags) runs the candidate on a real graph. It defaults off, and the meta records
`price_dag_mode` and the per-start polish stats under `price.polish`. Worth watching on
the real graphs:

- Black b2000: does polish close the 0.012–0.02 gap to DP in-model (0.5945 vs 0.6067)?
- White b2000: does it change the first-move answers?
- Polish wall-clock at 4.7M nodes.
