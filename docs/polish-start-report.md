# Polish from any start, and polish with stickiness

Brief: `docs/handoff_polish_any_start.md`. Everything here ran on the synthetic graphs of
`python/_test_budget_dag.py` in a cloud session (Python 3.11 venv from `requirements.txt`,
DuckDB 1.5.6). No pools, eval arrays or books were touched.

**Verdict.**
- `--method dp --polish` is never below DP. On every synthetic graph it scored at least
  DP's value, and it was exact on 97.8–98.8% of instances where DP alone was exact on
  86.7–90.9%.
- `--polish-from BOOK` makes price+polish start from a given book as well, for example the
  DP book. The result is then never below either polish, and it was exact on 99.9–100%.
- Polish now respects `--incumbent`, and the refusal is lifted. A margin of 0 is the old
  polish bit for bit. With a margin, sticky polish is exact on its own objective on
  99.1–100% of instances. At the default margin of 0.01, it cuts changed-move reach by
  25–55% for a value loss of about 0.00005.
- Runtime: polish from a DP book that is near the target costs seconds. From a DP book far
  below the target it cost 31 s at 1M nodes. Stickiness adds nothing measurable to polish.

## Task 1 — polish from a given book

`budget_core.polish_book` already accepted any start book. What is new:

- **`build_budget_books.py --method dp --polish`.** Extract the DP book (plain or
  `--match-distinct`), then polish it to the same budget. The written book's `value`
  column is then `policy_value`'s (via `book_result`), not the curves'. Its `alloc_budget`
  is 0. The meta gains `polish` (the polish stats). Polish also trims an overshooting
  `--match-distinct` book to exactly the budget. In the CLI harness, a book of 101 for a
  target of 100 came back with exactly 100.
- **`price_search(..., polish_from=[books])` / `--polish-from BOOK`** (repeatable, needs
  `--price-dag-mode polish`). These books are polish starts after the search's own three
  starts. Moves that are not candidates on the graph are dropped. The meta records
  `polish_from`, and each given start's polish stats carry `"start": "given i"`.
- Every step is still accepted only on the region evaluator `_book_region`.

Exact share and gaps on the random DAGs. The development seed is 20261028 (300 DAGs).
Held-out seeds are 1, 2 and 3 at transposition rates 0.35, 0.65 and 0.15. Columns are as
in `docs/dag-price-report.md`; *polish* is price + polish (four rounds, production flags);
*polish+dp* is the same with the DP book as a fourth start.

| seed / transpositions | instances | dp | **dp+polish** | price+polish | polish+dp |
|---|---|---|---|---|---|
| dev / 0.35 | 1,302 | 88.7% | **98.2%** | 100% | 100% |
| 1 / 0.35 | 1,298 | 86.7% | **97.8%** | 99.9% | 99.9% |
| 2 / 0.65 | 1,081 | 88.6% | **98.5%** | 99.9% | 100% |
| 3 / 0.15 | 1,488 | 90.9% | **98.8%** | 99.6% | 99.9% |

| seed | dp gap mean / max | dp+polish gap mean / max | below dp | polish+dp ≠ max(polish, dp+polish) |
|---|---|---|---|---|
| dev | 0.00241 / 0.247 | 0.00035 / 0.111 | **0** | 0 |
| 1 | 0.00271 / 0.170 | 0.00025 / 0.076 | **0** | 0 |
| 2 | 0.00305 / 0.147 | 0.00026 / 0.069 | **0** | 0 |
| 3 | 0.00173 / 0.099 | 0.00020 / 0.069 | **0** | 0 |

DP+polish always fits, and it was never below DP on any of the 5,169 instances. It is
still below price+polish on 6–19 instances per seed. These are DP's fused-atom losses,
where the DP book sits in a different basin and coordinate ascent cannot leave it: the
worst gap fell from 0.247 to 0.111. Giving price+polish the DP book as an extra start
(`polish+dp`) returns the better of the two polishes on every instance (gated in the
test).

### At scale (`_test_budget_dag.py --scale`, `big_dag`)

`dp` is `build_curves` + `extract_book`. `+polish` is the polish alone, timed separately.
The machine was running two other jobs during this run, so the absolute seconds are high.
Compare within a row group.

| nodes | target | price+polish | dp (time, size, value) | +polish (time) | dp+polish |
|---|---|---|---|---|---|
| 10k | 400 | 400, 0.49892 | 0.3 s, 162, 0.49768 | 0.4 s | 400, 0.49886 |
| 10k | 2,000 | 1,000, 0.49902 | 0.3 s, 416, 0.49894 | 0.8 s | 746, 0.49900 |
| 100k | 400 | 400, 0.63875 | 3.5 s, 142, 0.63658 | 1.0 s | 400, 0.63860 |
| 100k | 2,000 | 2,000, 0.63912 | 3.9 s, 446, 0.63879 | 10.6 s | 1,944, 0.63904 |
| 1M | 400 | 400, 0.58061 | 34.5 s, 113, 0.57792 | 3.6 s | 400, 0.57989 |
| 1M | 2,000 | 2,000, 0.58164 | 47.7 s, 542, 0.58101 | 30.7 s | 2,000, 0.58164 |

On these graphs the DP book is far below the target, because its curves fuse whole lines
into atoms. Polish then has to add 240–1,460 positions, and that is where its time goes.
It can also stop short of the target in a local optimum: 746 of 1,000 worth booking at
10k, 1,944 of 2,000 at 100k. That is not the reach floor of the line tables; forcing the
floor to 0 gave identical books. On the real Black graph, `--match-distinct` DP books
already hold ~2,001 positions, so polish starts at the target and should take seconds.
`--polish-from` covers the other case.

## Task 2 — polish with stickiness

**The objective.** With `g.meta["stick"] = (incumbent moves, margin)`:

    J(book) = value(book) − margin × Σ W(u) over the changed positions u

W is the book's own total reach (`policy_reach`). A position u is changed when the book
plays it, the incumbent's move is a candidate there (exactly where `incumbent_move` would
credit it), and the book plays another move. Check marks are ignored.

In u's own units this is `incumbent_move`'s rule. A switch away from the incumbent must be
better by more than the margin. Keeping the incumbent's move rather than stopping is
decided on true values, as `_test_budget_stick` D requires. Dropping a position, or
booking one the incumbent never had, costs nothing.

There is one band where J and the price search's selection key part ways. In `price_book`,
a rival that beats the incumbent by more than the margin is then booked on its true value
against stopping. Under J, a changed position must beat stopping by the margin as well.
This only matters where the incumbent's move has fallen below stopping and the best rival
beats stopping by less than the margin. At such a position J stops, and the value at stake
is under margin × W. The search itself rarely books such positions. On the 10k / 2,000
graph, the sticky price search booked 775 positions and sticky polish removed exactly one
for this reason. A fully consistent version would be a constraint ("a changed move beats
the incumbent's line by the margin"), and the incumbent's line has no defined value in a
budgeted local search. So I chose the penalty, which is a state function the oracle can
check exactly.

**Implementation** (`polish_book`). The trim, the proposals and the accept test all read J:
- the leaf loss includes the margin a changed leaf carries;
- the line tables charge the margin on a changed move;
- rebook and switch estimates include it;
- every step is accepted on the exact J of the region evaluator;
- `price_search` picks between polished starts on J;
- one new move kind, `drop`, is proposed only under stickiness: drop a changed leaf whose
  move beats stopping by less than the margin.

The stats gain `start_objective`, `objective`, `margin`, `changed` and `reach_changed`,
only under stickiness. `build_budget_books` no longer refuses `--price-dag-mode polish`
with `--incumbent`. `--method dp --polish --incumbent` also works: the DP extraction is
sticky as before, and polish uses J.

**Checks** (`_test_budget_dag.py`):
- **S0:** no stick and margin 0 give the same polish, bit for bit (`polish_book` from three
  starts, and `price_search` polish) — 0 differ.
- **S1:** with a margin of 1e9, polish never changes a move its start kept from the
  incumbent. Starts: empty, random, the incumbent, the sticky price book and the sticky DP
  book. 0 of 852 kept moves changed.
- **S2:** at margin 0.01 sticky polish fits, never lowers its start's J, and reports J and
  the changed count as an independent `policy_value` / `policy_reach` computation gives
  them — 0 off.
- **Gates on the report:** sticky polish fits and is J-exact on at least 98% of instances,
  and churn falls as the margin rises.

**Churn vs value.** For each random DAG and budget, the incumbent is the exact optimum of
a copy whose stop and leaf values were moved by up to ±sd (the previous build's data). The
rebuild runs each engine under `stick = (incumbent, margin)`. The "J exact" share is
against the exact optimum of J from the same enumeration. "changed" is the number of
changed positions per instance; "reach chg" is their summed reach. Engines use the recipe
flags with one round. Rows are 300 DAGs (982 instances, seed 7).

sd = 0.03:

| engine | margin | gap to optimum | J exact | changed | reach chg |
|---|---|---|---|---|---|
| keep the incumbent | — | 0.00093 | — | 0 | 0 |
| price | 0 | 0.00396 | 73.1% | 0.066 | 0.0340 |
| price | 0.01 | 0.00378 | 74.1% | 0.035 | 0.0194 |
| **price+polish** | 0 | 0.00000 | 100% | 0.057 | 0.0241 |
| **price+polish** | 0.005 | 0.00002 | 100% | 0.036 | 0.0141 |
| **price+polish** | 0.01 | 0.00005 | 100% | 0.029 | 0.0109 |
| **price+polish** | 0.02 | 0.00015 | 99.9% | 0.011 | 0.0040 |
| dp | 0 | 0.00357 | 85.1% | 0.078 | 0.0396 |
| dp | 0.01 | 0.00345 | 85.3% | 0.055 | 0.0275 |
| **dp+polish** | 0 | 0.00040 | 97.4% | 0.070 | 0.0336 |
| **dp+polish** | 0.005 | 0.00029 | 97.6% | 0.049 | 0.0234 |
| **dp+polish** | 0.01 | 0.00028 | 97.6% | 0.043 | 0.0215 |
| **dp+polish** | 0.02 | 0.00038 | 98.0% | 0.023 | 0.0124 |

sd = 0.06 (a bigger change in the data):

| engine | margin | gap to optimum | J exact | changed | reach chg |
|---|---|---|---|---|---|
| keep the incumbent | — | 0.00372 | — | 0 | 0 |
| **price+polish** | 0 | 0.00000 | 100% | 0.105 | 0.0558 |
| **price+polish** | 0.005 | 0.00002 | 100% | 0.086 | 0.0475 |
| **price+polish** | 0.01 | 0.00006 | 100% | 0.075 | 0.0420 |
| **price+polish** | 0.02 | 0.00027 | 99.9% | 0.049 | 0.0291 |
| **dp+polish** | 0 | 0.00040 | 97.4% | 0.113 | 0.0615 |
| **dp+polish** | 0.01 | 0.00029 | 97.6% | 0.085 | 0.0485 |
| **dp+polish** | 0.02 | 0.00047 | 98.0% | 0.058 | 0.0348 |

At margin 0.01, sticky price+polish cuts changed reach by 55% (sd 0.03) or 25% (sd 0.06).
That costs 0.00005 of value against the unconstrained optimum. It also keeps polish's
exactness on its own objective. Margin 0.02 removes 48–83% of the reach-weighted churn for
0.00015–0.00027.

The same on a big graph (100k nodes, incumbent = the polished book of a perturbed copy,
one round). Polish seconds are the polish step only:

| target | margin | value | changed | polish seconds |
|---|---|---|---|---|
| 400 | none | 0.638749 | — | 1.1 / 1.3 |
| 400 | 0.005 | 0.638748 | 6 | 1.5 / 1.3 |
| 400 | 0.01 | 0.638760 | 3 | 1.1 / 1.4 |
| 400 | 0.02 | 0.638640 | 1 | 1.2 / 1.3 |
| 2,000 | none | 0.639116 | — | 3.6 / 3.3 / 3.4 |
| 2,000 | 0.005 | 0.639112 | 23 | 3.2 / 2.9 / 3.0 |
| 2,000 | 0.01 | 0.639111 | 12 | 3.8 / 3.4 |
| 2,000 | 0.02 | 0.639034 | 3 | 3.4 / 3.3 / 3.5 |

(The 400 / 0.01 value is above the unsticky value: a different search start. Values are
policy_value, not J.)

## Task 3 — no-ops and tests

- **Against HEAD's `budget_core`.** On 400 random DAGs × 5 budgets, without a stick and with
  `stick = (moves, 0.0)`: `price_search(dag_mode="polish")` books, values and per-start
  polish stats were identical, and so was `polish_book` from two starts — 0 of 4,000
  differ. On `big_dag` 10k and 50k at targets 400 and 2,000, the books and values were
  also identical.
- **CLI.** `build_budget_books.py` ran on a synthetic stats DAG (2,760 parents, eval lookup
  stubbed). The book parquets were byte-identical to HEAD's for `--method dp
  --match-distinct` and for the price+polish recipe. `--incumbent X --stick-margin 0` was
  byte-identical to no incumbent. `--polish`, `--polish-from`, and sticky price+polish and
  dp+polish all ran. `--polish` without `--method dp`, and `--polish-from` without
  `--price-dag-mode polish`, are refused.
- **`python run_tests.py`: 68/68 pass.** `_test_budget_dag.py` takes ~80 s, up from ~30 s;
  the stickiness report is most of it, and `N_GRAPHS N_STICK` set its size. These skip
  their data-dependent parts, which need local data:
  - `_test_epd_max_ply` (real games), `_test_epd_memo`, `_test_eval_arrays`,
    `_test_extract_accounting`, `_test_extract_child_hash`, `_test_extract_equivalence`,
    `_test_merge_child_hash`, `_test_replay_holdout_guard`, `_test_stage3_load_evals`,
    `_test_trainer_m5`, `_test_zobrist_incremental` (real-parquet part);
  - `_test_rust_extract`, `_test_verify_book`, `_test_verify_evals` (no explorer-extract
    exe);
  - `_test_docs_paths` (the D:/F: paths).
- `requirements.txt` keeps `duckdb>=1.5,<2`.

## What to run on the real graphs

Start from the exact command that built each current book: the `.meta.json` next to it
records every flag. Keep its `--rep`, `--stats`, `--perspective`, `--eval-weight`, stop
model, gate and `--max-ply` / `--share-floor` / `--min-games` flags as they are. Only the
flags below change. Paths in `<>` are yours.

**Black — DP, then polish** (never below DP in-model):

```powershell
.venv\Scripts\python.exe python\build_budget_books.py <current Black flags> --method dp --match-distinct --polish --budget 2000 --out-prefix E:\chess\repertoire\_budget\dpp_black --evaluate-after dp=<current Black DP book>
```

**Black — the better of both polishes.** Price+polish, also started from the book above:

```powershell
.venv\Scripts\python.exe python\build_budget_books.py <current Black flags> --method price --price-seed source --price-reprice max --price-rounds 1 --price-select best --price-dag-mode polish --polish-from E:\chess\repertoire\_budget\dpp_black_b2000.parquet --budget 2000 --out-prefix E:\chess\repertoire\_budget\ppdp_black --evaluate-after dp=<current Black DP book> --evaluate-after dpp=E:\chess\repertoire\_budget\dpp_black_b2000.parquet
```

The in-model gain over DP was 0.0006 for price+polish last time, and it did not survive
your out-of-sample evaluation (0.004–0.006 below DP). So judge `dpp` and `ppdp` on the
holdout as well, not in-model alone.

**White — rebuild with stickiness** (price+polish, as now, keeping the book you learn):

```powershell
.venv\Scripts\python.exe python\build_budget_books.py <current White flags> --method price --price-seed source --price-reprice max --price-rounds 1 --price-select best --price-dag-mode polish --incumbent <E:\chess\repertoire\final\ White book you learn> --stick-margin 0.01 --budget 2000 --out-prefix E:\chess\repertoire\_budget\pp_white_sticky
```

The meta's `incumbent_churn` and `price.polish[*].changed` / `reach_changed` give the
churn. `--stick-margin 0` gives the same book as no `--incumbent` (a churn report only).
For Black under stickiness, add the same `--incumbent <Black book> --stick-margin 0.01` to
either Black command. An incumbent book larger than the budget, such as an `_ext_c10`
version, is fine: positions outside the graph, or where its move is not a candidate, are
ignored.

**Runtime.** Price+polish and its sticky version cost what they cost now: stickiness added
nothing measurable to polish. `--method dp --polish` adds the polish of a near-target book,
which takes seconds. `--polish-from` adds one more polish start, about as long as one of
the existing three. None of this should add more than a minute or two to a 13–65 min
build. One thing to watch: if a DP book lands far under the budget (not the case with
`--match-distinct` on Black), polish has to fill the gap. On the synthetic 1M graph that
took 31 s for about 1,460 positions. The meta's `polish.seconds` reports it.
