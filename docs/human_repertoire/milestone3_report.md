# Milestone 3: a shared fallback prior improves separate-game prediction

Completed 5 September 2026. The next bounded predictive experiment is complete.
On **26,857 March decisions**, the shared prior lowered log loss from **2.17999 to
2.02705**, a 7.0% reduction relative to the tuned position-only baseline. It
improved all three time controls and unseen-position prediction, passing the
predeclared progression rule. This supports testing the prior in the research
evaluator; it does **not** establish better book choices or human playing results.

## Sample and separation

The [plan](milestone3_plan.md), [config](milestone3_config.json) and exact
[frames](milestone3_frames.json) were committed as `01acd54` before game extraction.
The target remains both players rated >=1800 across Blitz, Rapid and Classical,
with rating gap <=200. The root is the exact sequence `1.e4 e5 2.Nf3 Nc6`, followed
for twelve additional plies. Completed games that end inside this window are
retained; prediction scores only decisions that actually occurred.

The extractor inspected 86 Parquet footers, then read 21 selected row groups from
the canonical compressed source. Three 65,536-row windows per event/month produce
an examined frame of **1,769,472 rows**. Fixed game-ID hash ranking retained 250
eligible games/window from 34,874 eligible records. All 6,750 selected records
replayed successfully; no game IDs overlap within or between splits.

| Split | Games per time control | Total games | Recorded decisions | Actual selected date range |
|---|---:|---:|---:|---|
| January: fit | 750 | 2,250 | 26,914 | Jan 4–28 |
| February: tune | 750 | 2,250 | 26,903 | Feb 4–26 |
| March: final test | 750 | 2,250 | 26,857 | Mar 4–28 |

Each event has early, middle and late month coverage. For example, March Blitz
uses March 6/16/27 and Rapid March 5–6/15/25; Classical spans windows around
March 4–8/14–18/25–28. This improves date spread over milestone 2, but remains a
balanced **convenience frame**, not a population-random monthly sample. Equal
event/game caps do not reproduce Lichess's time-control mixture.

There are 21/32/35 observed endings within the January/February/March windows.
One January and one March game have zero decisions after the root. These remain
in cohort counts, while March's 2,249 games with decisions supply prediction
scores. Individual ratings range from 1800 to 2956/2950/3000 respectively.

The three splits contain 3,765/3,780/3,752 unique players, with 401 shared between
fit/tune, 333 between fit/test and 393 between tune/test. This tests later games,
not a strictly unseen-player population. January/February were used for earlier
development; March was kept out of model selection for this experiment.

## Model and frozen selection

January supplies exact-position move counts and pooled move-token counts. A token
records piece type, destination from the mover's perspective, capture, promotion
and castling. For example, comparable White and Black development moves share
counts after reflecting Black's ranks. This simple prior ignores tactical context.
For each legal move, give weight `(token count + 1)^t`, normalize across legal
moves, and mix that prior with the position's recorded counts using alpha prior
observations. No result, engine score, held-out move or recall measurement enters
the fitted counts.

February selected alpha from {0.1,1,5,20} and t from {0.5,1} using mean decision
log loss only. It selected **alpha=1 for both position models**, and **t=0.5** for
the shared prior. The token-only model also selected t=0.5. Thus the improvement
does not arise from giving the two final position models different smoothing
strengths. February log loss was 2.18806 for position/uniform and 2.03801 for
position/token. The counts and settings were frozen and committed as `f8ec184`
before March scoring. No February refit or March tuning followed.

## Final predictive results

Log loss uses natural logarithms; lower is better. Multiclass Brier is the sum
of squared probability errors over all legal moves, also lower better.

| Frozen model | March log loss | March Brier |
|---|---:|---:|
| Uniform legal moves | 3.45689 | .96412 |
| Shared token prior alone | 2.92401 | .92243 |
| Milestone-2 position model, alpha=1 | 2.17999 | .73609 |
| Tuned position/uniform | 2.17999 | .73609 |
| Tuned position/shared-token | **2.02705** | **.72650** |

The token prior alone remains substantially worse than exact-position
frequencies. Its value here is as a prior for sparse and unseen positions.

| March subset | Decisions | Position/uniform log loss | Position/token log loss |
|---|---:|---:|---:|
| Blitz | 8,973 | 2.19833 | 2.04257 |
| Rapid | 8,956 | 2.26503 | 2.10255 |
| Classical | 8,928 | 2.07626 | 1.93572 |
| White decisions | 13,437 | 2.12287 | 1.98790 |
| Black decisions | 13,420 | 2.23718 | 2.06626 |
| Seen positions | 18,683 | 1.61461 | 1.54867 |
| Unseen positions | 8,174 | 3.47225 | 3.12047 |

The primary paired difference, shared minus tuned uniform, is **-0.15294 nats per
decision**. A 500-resample shared-player-component bootstrap gives a descriptive
95% interval **[-0.16111, -0.14474]**. It groups test games connected by either
player, retaining paired model scores and decision weighting. There are 1,534
components; the largest contains 139 games. The interval conditions on this
selected frame and the already fitted/tuned models. It excludes training,
tuning, date-frame and population uncertainty and is not a causal interval.

The fixed progression rule required an interval entirely below zero, improved
point log loss in every event, and improved unseen-position log loss. All pass.
This is evidence for a bounded next modelling step, not proof that the model is
adequate for repertoire selection.

## What remains unresolved

**30.4%** of March decisions occur at positions absent from January training;
**46.7%** have fewer than five training observations, and **39.7%** are moves not
observed at their training position. The shared model can assign probabilities
there, but it does not create downstream game observations or engine evaluations.

On actual March positions, the average probability assigned to moves absent at
that training position changes only from **37.89% to 37.48%**. At a wholly unseen
position, all its moves are absent, so that mass remains 100%. This is a one-step
diagnostic conditional on recorded positions; it is not the probability that a
book simulation reaches an unresolved frontier. The better loss chiefly concerns
how probability is allocated within legal support. Milestone 2's broad first-hit
bounds therefore remain unresolved.

The prior ignores tactics, clocks, opening familiarity, personal recall and
study effort. No neural model, engine job or new outcome model was introduced.
The common book evaluator, optimisation machinery, soundness gates, recall
scenarios and early-advantage objective remain unchanged. There is no new book
ranking or recommendation to alter the user's repertoire.

## Verification, artifacts and next priority

All **40 research tests passed** before extraction, including independently
calculated posterior/Brier values, legal support, color symmetry, sampling across
batch edges, overlap/resource limits, frozen-input guards and clustered paired
weighting. The final audit checked snapshot hashes, disjoint game IDs, rating
filters and reconstruction of all pooled/event scores from per-game totals.
The unchanged milestone-2 diagnostic independently reproduced the new baseline
on the same March games, with maximum reported metric discrepancy below 5e-14.

Before extraction, the system had about 105.1 GiB free commit headroom while
Claude's independent workload remained active. Batch-rounded decoding covered
2,670,592 rows, below the four-million-row cap; the largest projected row group
was 464,017,220 uncompressed bytes, below 512 MiB. Extraction took 46.1 seconds,
fitting/tuning 16.5 seconds, and final scoring/bootstrap 11.2 seconds. No canonical
file, original checkout, trainer database or external process was changed.
An account usage limit delayed the final verification command; it subsequently
ran successfully after the user asked to continue. The experiment was not rerun
or adjusted in response to its final results.

Tracked [provenance](milestone3_provenance.json), [selection](milestone3_selection.json),
[results](milestone3_results.json) and [verification](milestone3_verification.json)
preserve the finding. [Runnable commands](../../research/human_repertoire/README.md)
and the isolated `_artifacts/m3/` snapshots support reproduction. The earlier
milestone snapshots remain separate.

**Next priority:** integrate both frozen priors into one bounded common graph and
audit influential missing engine evaluations. Freeze graph expansion, work caps
and eval-source consistency before comparing books again. Preserve explicit
frontier and first-hit uncertainty; do not substitute guessed outcomes for
missing branches. Assess whether that bounded integration narrows the observation
intervals enough to distinguish books before increasing optimiser scale. Personal
recall and learnability still require direct evidence.
