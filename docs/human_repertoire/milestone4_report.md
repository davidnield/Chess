# Milestone 4: common graph integration and targeted engine audit

Completed 5 September 2026. The two frozen fallback policies now run on a common
legal graph, and a bounded Stockfish audit covers the influential targets and
every prescribed parent/child. All checks pass. **The book comparison remains
unresolved: omitted legal continuations account for almost all of the remaining
first-hit uncertainty.** More engine work on the current explicit graph is not
the main next priority.

These are model predictions conditional on the January training sample, the
frozen priors, recall assumptions and supplied evaluations. They are not observed
human outcomes, a new held-out prediction test, or evidence to change a repertoire.

## Fixed comparison and common topology

The [plan](milestone4_plan.md) and [config](milestone4_config.json) were committed
before graph prioritisation. Use the milestone-3 January sample: both players
>=1800, rating gap <=200, Blitz/Rapid/Classical, after the exact root
`1.e4 e5 2.Nf3 Nc6`. The two priors retain February's selected settings; no refit
or tuning occurs. Horizon is twelve additional plies, recall .85 independent per
decision with re-entry, and the competing first-hit thresholds are +/-300 cp.
Discount remains .99. The existing soundness, points and study constraints apply.

Compare three already specified plans: empty, the source's one-card `3.d4`, and
the two-card plan adding `3...exd4 4.Nxd4`. No book optimiser is run. Both policies
use identical explicit legal branches within each graph variant, so changing a
prior does not also change the comparison topology.

The observed graph retains all January moves and positions. Unobserved legal
moves retain an aggregate unknown frontier. Rank missing edges by maximum
expected traversal count across the two policies and three plans, ignoring
engine scores. At the top 64 edges, expose the exact child and two further plies
of the union of each policy's three most likely moves at unsupported positions.
Reconnect existing transpositions and retain omitted probability explicitly.

| Graph quantity | Observed | Expanded |
|---|---:|---:|
| Real chess positions | 12,275 | 14,244 |
| Total nodes, including aggregate frontiers | 22,515 | 24,829 |
| Newly expanded unsupported positions | 0 | 354 |

Expansion adds 1,969 real positions. New unsupported positions use the frozen move
prior, the existing W/D/L prior and an assumed zero human-ending hazard. These
assumptions have no new human observations behind them. The allocation heuristic
counts finite-horizon visits and ignores repetition; final evaluation retains
the original history/repetition semantics.

## Engine coverage and consistency

Sparse read-only lookups over the 14,244 positions find **2,848 missing canonical
evaluations**. Array/source fingerprints are verified with `adopt=False`; no
database scan, rebuild, adoption or canonical write occurs.

The frozen [target list](milestone4_engine_selection.json) contains 256 positions:
126 previously missing scores and 130 previously scored positions, including all
four distinct prescribed parents/children. Selection uses reach, not score values.
The installed executable identifies itself as **Stockfish 18**; its full binary
hash and reported options are preserved in the [provenance](milestone4_provenance.json).
Use one thread, 32 MiB hash, full strength, no tablebases, and fresh hash/game state
per position. Initial limits are 100,000 nodes/1 second; 32 fixed targets receive
400,000 nodes/2 seconds. The earliest limit wins.

The audit completed in **45.6 seconds**, searching 25,529,535 initial nodes and
12,808,609 deeper nodes. Recorded engine time totals 42.1 seconds. Non-mate initial
scores have completed depths 11–19; the deeper subset spans 14–19. One initial
mate score reaches a reported depth of 245 with bounded work; it is recorded as
mate separately and should not be interpreted as comparable search difficulty.

Among 130 targets with old scores, median absolute change is **13.5 cp**. None
changes its +/-300 cp first-hit category. None of the 32 deeper checks changes
that category either. This is a targeted, small-budget consistency check, not a
random validation sample or proof that all evaluations are accurate.

Both prescribed moves retain their soundness gate under all three score sets:

| Prescription | Canonical parent / child cp | 100k-budget parent / child cp | 400k-budget parent / child cp |
|---|---:|---:|---:|
| `3.d4` | +22 / +7 | +39 / +22 | +47 / +17 |
| `3...exd4 4.Nxd4` | +4 / +6 | +42 / +37 | +14 / +14 |

The initial UCI reader stopped before completing any audit because a node cap
could leave a bound-only final message. A protocol trace also exposed the
library's accumulation of earlier bound flags. The research reader now takes
the last completed, non-bound score within the unchanged search cap, recording
the final-message flag, completed depth and total work separately. There were
151/256 initial and 28/32 deeper searches with an interrupted final iteration.
Bound-only results with no completed score are rejected. This clarification and
its regression checks were committed before the completed audit and comparisons.
All four original graph/eval/target snapshot hashes remained unchanged; the
initial snapshots and diagnostic trace are preserved under ignored artifacts.

## What the common evaluator predicts

All **36 fixed comparisons** completed: two graph variants, two policies, three
plans and three evaluation maps. The maps are canonical scores, an explicitly
mixed-source audited overlay, and a sensitivity using only the 256 audited
Stockfish scores plus exact board terminals. The overlay uses the deeper result
for its fixed subset. Each map is common to both policies and all plans. Every
plan passes the configured gates and model-level feasibility constraints.

Using the expanded graph, shared prior and audited overlay:

| Plan | Cards | Discounted early-advantage prediction | Disadvantage prediction | Expected points | First-advantage observation interval |
|---|---:|---:|---:|---:|---:|
| Empty | 0 | 1.379% | .841% | .50017 | [1.49%, 87.42%] |
| Source `3.d4` | 1 | 2.774% | 1.135% | .49988 | [2.98%, 87.22%] |
| Add `4.Nxd4` | 2 | 3.026% | .678% | .49891 | [3.24%, 87.53%] |

The second card increases the primary point estimate by .002518 while slightly
reducing expected points. These point estimates cannot establish a reliable
ordering: the observation intervals overlap extensively. They bound missing
first-hit information under each fitted model; they are **not** sampling
confidence intervals and do not account for model error or human learning.

For the two-card plan with the shared prior:

| Graph / evaluation evidence | Discounted early advantage | Unresolved first-hit mass |
|---|---:|---:|
| Observed / canonical | 2.373% | 85.78% |
| Expanded / canonical | 2.815% | 85.00% |
| Observed / audited overlay | 2.512% | 85.17% |
| Expanded / audited overlay | 3.026% | 84.30% |
| Expanded / audited Stockfish only | .360% | 99.50% |

Expansion narrows the overlay interval by just **0.88 percentage points**. Early
frontier mass changes from 81.337% to 81.250%, only .087 percentage points. On the
same expanded/overlay setup, the uniform prior predicts 2.633% discounted early
advantage and has an observation interval [2.81%, 88.59%]. The shared prior's
better held-out move loss therefore has not resolved the book ranking.

The audited-only map gives a two-card first-hit interval of approximately
[0.08%, 99.58%]. It makes the limit of the homogeneous engine coverage explicit:
256 carefully selected positions cannot replace evaluation evidence across the
whole graph. The audited overlay remains mixed-source evidence.

## Where information is lost

A separate path diagnostic stops at the first known hit or first observation gap.
Its three gap buckets reproduce the evaluator's unresolved first-hit mass for
all 36 comparisons. For the expanded shared-prior two-card plan with the overlay:

| First observation gap | Model probability |
|---|---:|
| Omitted legal move entering an aggregate frontier | **83.874%** |
| Missing evaluation at an explicit real position | .399% |
| Known-eval real position whose continuation remains unexpanded | .022% |
| Total unresolved first-hit mass | **84.295%** |

Thus aggregate omitted moves are the first gap on **99.5% of unresolved mass**.
This includes missing knowledge at the last ply, so it is distinct from the
before-horizon frontier metric. It also classifies the *first* gap: filling one
gap may reveal another later one. In particular, the .399% bucket is not the
total future engine work needed after continuations are expanded.

The audit reduced the explicit-missing-eval first-gap bucket from 1.482% to .399%.
Much of that newly observed mass then reaches an omitted move. This explains why
the engine audit helps coverage without making the book intervals narrow.

## Verification and next priority

The final **50-test research suite passed**. The observed uniform graph matches
the unchanged prior adapter's node/edge semantics, with maximum probability
discrepancy 7.8e-16. Two real six-ply checks against independent complete-path
enumeration have maximum metric error below 1.9e-15. All probability partitions,
gate maps, shared topologies, target completeness and frozen hashes verify.
The 36 comparisons completed in 45.5 seconds. Graphs stay below 40,000 nodes and
the existing evaluator retains its 200,000-state cap. The pre-work resource check
found ample commit headroom; other running jobs and the original checkout were
left alone.

Tracked [results](milestone4_results.json), [verification](milestone4_verification.json),
provenance and the [commands](../../research/human_repertoire/README.md) preserve
the experiment. Raw graphs and UCI records remain in `_artifacts/m4/`. No
production implementation, canonical artifact, trainer database or original
branch was modified.

**Next priority, inferred from these results:** test a bounded trajectory sampler
that follows the frozen legal-move policies through the full horizon instead of
stopping at aggregate omitted-move nodes. Validate it against the exact tiny
graphs and use paired random draws for fixed-plan comparisons. Freeze trajectory
and unique-position/engine budgets first; look up or audit evaluations on visited
positions and keep any missing evaluations explicit. Report Monte Carlo error
separately from missing-information bounds and model assumptions. This targets
the computational truncation exposed here without enumerating the full legal
tree or scaling the book optimiser. Personal recall and causal benefit still
require direct human evidence.
