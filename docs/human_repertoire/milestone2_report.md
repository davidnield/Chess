# Milestone 2: documented BRC sample and twelve-ply validation

Completed 5 September 2026 UTC. **The next bounded step is complete.** A documented
sample now contains observed early advantages, and the independent correctness
checks pass. The empirical model still leaves too much continuation probability
unresolved to justify a repertoire recommendation.

This report distinguishes observations in recorded games, predictions made on
separate games, and model predictions about following a book. The last category
is not evidence of improved human play.

## Sample and reproducibility

Per the user's direction, target **Blitz, Rapid and Classical, with both individual
ratings at least 1800**. Retain the previously stated maximum rating gap of 200.
Root is the exact move order `1.e4 e5 2.Nf3 Nc6`. Extend the horizon from three to
twelve additional plies, through White/Black move 8 when the game continues.

The pre-analysis [plan](milestone2_plan.md) and [recipe](cohort_config.json) fixed
source frames, caps and assumptions before outcome analysis. Exactly 262,144 rows
were examined from row group 0 of `part-0.parquet` in each of six canonical
January/February 2024 BRC files: **1,572,864 rows total**. Filter on individual
ratings, rating gap, completed result and opening prefix; select by fixed SHA-256
game-ID rank without using the result, later moves or engine evaluations. The
per-stratum rank cap is 2,000, followed by a pooled cap of 2,000 per month. All
12,000 records selected at the stratum stage replayed successfully; 4,000 remain
in the final pooled cohorts. No game IDs overlap across cohorts.

| Cohort | Blitz | Rapid | Classical | Total |
|---|---:|---:|---:|---:|
| January: training/development | 675 | 714 | 611 | 2,000 |
| February: validation | 689 | 730 | 581 | 2,000 |

This is a documented **convenience frame**, not a random sample of a month or the
full BRC population. Blitz and Rapid records come from January 1 / February 1;
Classical spans January 1–14 / February 1–15. Pooled weights reflect eligible games
in these equal-sized source frames. There are 319 shared players across cohorts;
separate game IDs do not establish independent players.

The snapshots contain **19,709 distinct positions**, of which **2,129 (10.8%)**
lack an engine evaluation. A game may encounter several positions: 486 training
games and 446 validation games encounter at least one missing eval. There are
23 and 26 observed game endings respectively within the opening window. Endings,
including flags, retain their W/D/L in the model instead of disappearing.

The [provenance record](milestone2_provenance.json) preserves source size/mtime,
projection sizes, schemas, source-frame counts, exclusion reasons, engine-array
verification, sampling audits and full hashes of the small extracted snapshots.
Large source files were not scanned to compute content hashes. Engine versions
and analysis depths remain unavailable per position. These evaluations also
postdate the 2024 games; this is a retrospective research model.

## What the separate-game checks establish

Fit position move frequencies on January, with one total prior observation spread
uniformly over legal moves. Score February's actual moves; lower log loss and
Brier score are better. January data alone provide the predictions.

| Validation event | Decisions | Pooled-model log loss | Uniform-legal log loss | Previously unseen positions | Previously unseen moves |
|---|---:|---:|---:|---:|---:|
| Blitz | 8,221 | 2.235 | 3.460 | 32.0% | 41.4% |
| Rapid | 8,694 | 2.261 | 3.459 | 32.4% | 42.2% |
| Classical | 6,931 | 2.143 | 3.455 | 29.8% | 39.0% |
| Pooled | 23,846 | 2.218 | 3.458 | 31.5% | 41.0% |

The pooled move Brier score is .7436 versus .9644 for uniform legal moves, a weak
baseline. **47.8% of validation decisions have fewer than five training examples
at that position.** Fitting a separate same-recipe model per time control worsens
log loss to 2.418 / 2.460 / 2.374 for Blitz / Rapid / Classical. In this small
sample, pooling helps support; this does not prove that time-control differences
should always be ignored. Four validation games end before the next move and
therefore enter outcome checks but not move-prediction checks.

For outcomes, predict each February game's W/D/L from its root move using January
counts and 20 prior observations at (.45,.10,.45). Root-move-conditioned Brier is
.56117 versus .56168 for a single pooled January W/D/L prediction: little evidence
of useful added discrimination. Mean predicted points are .53070, versus .51400
observed, an overprediction of **1.67 percentage points**. A descriptive 95%
shared-player-component bootstrap interval for this mean error is
**[-0.65, +3.75] percentage points** (500 resamples, 1,409 components, largest
component 35 games). This conditions on the selected frame and fitted model;
it does not capture training/selection uncertainty or establish causal validity.

## Early-advantage observations and book predictions

The recorded games contain 100/2,000 January and 98/2,000 February **first known
advantage markers** at +300 cp before a known -300 cp marker: 5.0% and 4.9%.
First known disadvantage appears in 52 and 31 games. Missing earlier evals can
make the true first event unknown. These observations establish that the longer
window contains events; they are not measurements of the counterfactual books.

January alone chooses four study positions and their frequent candidate moves,
with the frozen source's `3.d4` also available at the root. Exact enumeration
checks **40 books costing at most two distinct cards**. Recall scenarios are
1.00, .85 and .65, both independent and session-correlated. The correlated model
remembers all cards with probability r and none otherwise; it shares the same
pregame marginal recall, not necessarily the same conditional recall at reached
positions. These recall settings remain assumptions, not player measurements.

Every real scenario selects `3.d4` at one card and adds `3...exd4 4.Nxd4` at two
cards. Recall/correlation changes values but **does not change the selected plan**
within this candidate set. The root agrees with the existing reference projection.
The candidate universe and the source projection are deliberately small; this
does not compare against the full original repertoire at equal capacity.

At independent recall .85:

| Plan | Cards | January discounted early-advantage prediction | February model prediction | February expected points |
|---|---:|---:|---:|---:|
| Source projection: `3.d4` | 1 | .023499 | .014600 | .497742 |
| Add `3...exd4 4.Nxd4` | 2 | .024857 | .013004 | .498673 |

The extra card improves January's primary objective by .001358 but decreases
February's by .001596. It slightly raises February's predicted expected points.
The February model is re-estimated from February data and evaluates the already
frozen books on common assumptions. **These are model predictions, not observed
outcomes from people assigned to play the books.** The separate-game predictive
checks above are a different estimand.

The central limitation is the unresolved tail. For the two-card, recall-.85 plan,
83.9% of February model probability reaches a frontier before H. Missing engine
evals or the prior's unobserved-legal-move frontier occur on 90.4% of model paths.
This is much larger than the 22.3% of recorded validation games with missing
engine evals: it also includes the model's explicit probability on unseen moves.
At singleton positions, one prior observation assigns substantial probability
away from the sole recorded continuation, compounding over several decisions.

Conditional on this fitted transition model, the first-advantage observation
interval is **[1.39%, 90.24%]** for the two-card plan and **[1.56%, 90.04%]** for the
reference projection. Known hits before missing evals retain their lower-bound
credit; unobserved continuations remain unresolved. These wide intervals are
**not statistical confidence intervals**. They expose missing continuation/eval
information, and cannot rank these alternatives credibly. Point estimates near
.5 expected points also reflect extensive fallback to the stated outcome prior.

## Implementation, tests and next decision

The core now supports optional arrival-level human endings, explicit early
frontier/first-hit uncertainty, and a 200,000-history-state evaluation cap.
Defaults preserve the first milestone's mathematical model. Independent
complete-path checks cover every selected real book in both cohorts at recall
.85: **six checks passed**. The **31-test research suite passed in .617 seconds**,
including all previous contracts plus sampling, zero-recall equivalence, end
hazards, correlated recall, uncertainty before/after first hit, unseen-move
prediction, player clustering, and table selection versus independent depth-first
search with reconstruction. The final 18-scenario run completed in
**122.7 seconds**, implementation commit `f0336d3`; source/config hashes are saved.

The initial run was stopped to remove redundant zero-recall evaluations and
scalar deep-copy overhead; the same fixed experiment then completed within its
declared runtime guard. No probability, budget, source cap or horizon was changed
to obtain the result. The pre-extraction check showed about 103.7 GiB free commit
headroom with Claude's independent workload active. Per-source projected row
groups were at most about 440 MiB, decoded in bounded batches on one thread.
The original checkout, canonical files and running Claude jobs were left alone.

[Commands and output inventory](../../research/human_repertoire/README.md) reproduce
the frozen-cohort reuse, tests and comparison. Generated files stay under
`research/human_repertoire/_artifacts/m2/`; original milestone-1 outputs remain
separate. This step does not support changing the user's repertoire.

The next useful experiment is to improve and validate the continuation model on
a sample spread across dates. Compare a simple shared fallback model against this
position-only baseline, quantify unseen-position prediction, and obtain consistent
engine coverage on a bounded set of influential positions. Freeze those choices
before evaluating book differences again. More optimisation scale is premature
while the observation intervals are this wide. Personal recall and study effort
still require direct learning evidence.
