# Milestone 7: human-versus-model engine-loss calibration

Completed 2026-09-08 America/Los_Angeles. Protocol and implementation commit
`67e0970` preceded case selection and model draws. Human moves are observed
source data; sampled moves are **model predictions**. Engine losses are
bounded-search proxies, not exact best-move losses or causal learning evidence.

The frozen fallback prior substantially overproduces tactically costly moves
on this diagnostic panel. Losses >=150 cp occur in **52/192 model draws (27.08%)**,
versus **7/192 human moves (3.65%)** at the same boards. The balanced difference
is **+23.44 percentage points**, with an approximate paired component-bootstrap
interval of **[+17.89, +28.90] pp**.

The mismatch is concentrated at positions absent from January training:
**46/96 model draws (47.92%)**, versus **3/96 human moves (3.13%)** cross the
threshold. Their difference is **+44.79 pp**, interval **[+35.68, +54.28] pp**.
Seen positions show a small, uncertain difference. Deeper searches change none
of the fixed subset's 150/300-cp loss categories. These findings argue against
trusting the current unsupported-position prior for quantitative repertoire
uplift; completing more engine evaluations does not repair this limitation.

## Frozen frame and selection

The [protocol](milestone7_plan.md) and [config](milestone7_config.json) use the
existing March 2024 snapshot: both ratings >=1800, rating gap <=200, across
Blitz/Rapid/Classical, after `1.e4 e5 2.Nf3 Nc6`, over the next twelve plies.
The shared prior remains fitted on January and selected on February. March was
already evaluated in M3: this is a retrospective diagnostic, not a fresh
confirmatory test. No model fitting, book optimisation or new extraction occurred.

The frame contains 2,250 games and 26,857 actual decisions. Training support is
zero or positive January move count at the exact position. A fixed hash chooses
one decision per eligible game/support pair; an independent hash then selects
32 games per event/support stratum. All 192 cases and their model draws were
frozen before new searches. They come from 190 games; two games contribute once
to each support level. No case was dropped or replaced.

| Stratum | Eligible games | Eligible decisions | Selected |
|---|---:|---:|---:|
| Blitz / unseen | 625 | 2,743 | 32 |
| Blitz / seen | 749 | 6,230 | 32 |
| Rapid / unseen | 634 | 2,941 | 32 |
| Rapid / seen | 750 | 6,015 | 32 |
| Classical / unseen | 596 | 2,490 | 32 |
| Classical / seen | 750 | 6,438 | 32 |

Strata receive equal weight, eligible games within a stratum equal weight, and
each game's eligible decisions equal selection weight. The balanced panel
deliberately oversamples unseen support: 50% of cases versus 8,174/26,857 source
decisions. The overall rate is not a population-frequency estimate.

Exactly one move is drawn from the frozen shared kernel at each human-reached
board. This is a one-decision check, not a free-running rollout, a recall test
or a preparation intervention. Training-unseen does not mean unfamiliar to the
human player; their games can mix memorised preparation and improvised play.

## Loss results

Stored cp is White-oriented. Signed loss is parent minus child for a White
mover, child minus parent for Black. Negative values are retained as diagnostics,
then clipped to zero for loss markers. Thresholds are >=150 cp and >=300 cp;
the mean caps losses at 1,000 cp to limit the influence of mate-score magnitudes.

| Support | Human >=150 cp | Model >=150 cp | Difference | Approximate 95% component interval |
|---|---:|---:|---:|---:|
| Seen, n=96 | 4.17% | 6.25% | +2.08 pp | [-3.93, +7.86] pp |
| Unseen, n=96 | 3.13% | 47.92% | +44.79 pp | [+35.68, +54.28] pp |
| Balanced, n=192 | 3.65% | 27.08% | +23.44 pp | [+17.89, +28.90] pp |

The support pattern appears in each time control:

| Stratum, n=32 each | Human >=150 cp | Model >=150 cp | Difference |
|---|---:|---:|---:|
| Blitz / unseen | 0/32 | 10/32 | +31.25 pp |
| Blitz / seen | 1/32 | 2/32 | +3.13 pp |
| Rapid / unseen | 3/32 | 19/32 | +50.00 pp |
| Rapid / seen | 0/32 | 2/32 | +6.25 pp |
| Classical / unseen | 0/32 | 17/32 | +53.13 pp |
| Classical / seen | 3/32 | 2/32 | -3.13 pp |

For >=300-cp loss, no human move crosses the threshold in this sample; 29/192
sampled moves do (15.10%), including 27/96 at unseen positions. Zero observed
events does not establish a zero population rate. The balanced capped mean is
30.51 cp for humans and 134.60 cp for model draws: difference 104.09 cp,
approximate interval [81.12, 128.36] cp. Unseen-position means are 28.63 and
223.26 cp respectively.

Moves match in 42/192 cases: 32/96 seen and 10/96 unseen. One sampled loss exceeds
the 1,000-cp cap and involves a mate score; no human loss exceeds it. Negative
signed differences occur for 33 human and 12 model moves. Independent bounded
parent/child searches can disagree, so these are not exact optimality claims.

Seen cases tend to be earlier: median decision plies after the root are 2.5
Blitz, 3 Rapid and 4 Classical, versus 10, 10 and 9.5 unseen. Median seen training
counts are 8, 53.5 and 64.5, with ranges 1-2,249. The support strata thus involve
different position distributions. Within-case pairing controls the starting
board; it does not prove that training support itself causes an error.

## Uncertainty and deeper searches

The 2,000-draw bootstrap joins selected games sharing a player into connected
components, resamples components globally, recomputes stratum means and retains
fixed equal stratum weights. There are 181 components; the largest contains
three cases from three games. No resample has an empty required stratum.

Percentile intervals are approximate conditional diagnostics, not exact
design-based coverage or population guarantees. They exclude fitting/selection,
engine error and original date-frame uncertainty. One model draw per case adds
sampling noise; the model rates are not exact legal-move integrals. Twelve
overlapping groups are reported without simultaneous coverage guarantees.

The first four selected cases in each stratum form a fixed nested subset of 24.
All parents and both children have complete scores at both budgets. Base and
deeper rates are identical: human >=150 cp 1/24, model 6/24; human >=300 cp 0/24,
model 3/24. The capped mean-loss difference changes from 72.04 to 77.96 cp.
Across 63 subset positions, median absolute cp shift is 7, maximum 29. No loss
category changes. This is a small sensitivity check, not independent replication.

## Resources and verification

The installed Stockfish 18 binary matches prior audits. Searches use one thread,
32 MiB hash, no tablebases, cleared hash/fresh game state and the tested
last-completed-non-bound-score reader. Cached records are reused by budget;
deeper scores never replace base scores in the main panel. All required scores
are present. No canonical eval lookup or production write was needed.

| Budget | Required | Reused | New | New nodes | Engine-reported seconds |
|---|---:|---:|---:|---:|---:|
| 100,000 nodes / 1 second | 467 | 51 | 416 | 41,536,555 | 48.548 |
| 400,000 nodes / 2 seconds | 63 | 9 | 54 | 21,613,900 | 23.511 |

Median completed depths are 15 and 18. Base depths span 11-245, with the maximum
from a mate search; deeper depths span 14-23. Interrupted final bound flags
occur in 266/416 and 39/54 searches; retained scores come from completed non-bound
iterations. No new search reaches its time limit. Engine wall time is 77.5
seconds, preparation 1.25 seconds and verification 2.30 seconds. The initial
check records about 183.8 GiB free commit. The separate Chess.com crawl and
original artifacts remain untouched.

All **76 research tests pass**. Verification reconstructs selection, legally
replays all 192 human decisions from the original opening, independently
regenerates model draws, checks engine PVs/budgets, and checks 2,400 scalar loss
calculations. It reconstructs full/subset summaries, resampling output,
independent stratum arithmetic and frozen hashes.

See the [research README](../../research/human_repertoire/README.md) for commands.
Tracked [provenance](milestone7_provenance.json), [selection](milestone7_engine_selection.json),
[results](milestone7_results.json) and [verification](milestone7_verification.json)
preserve the compact audit. Full cases, player identifiers, FENs, per-case scores
and engine responses stay in ignored `research/human_repertoire/_artifacts/m7/`.

## Consequence and next priority

M3's improved move log loss does not imply satisfactory tactical calibration.
The current global move-token prior lacks board-interaction features, and M7
finds a large mismatch at unseen positions even on human-reached boards. M6's
complete rollouts rely materially on this unsupported behavior. Their book
differences should remain exploratory model outputs, not quantitative human
improvement claims.

Changing position-count smoothing alone cannot repair unseen behavior: with
zero counts, `(count + alpha*q)/(N + alpha)` reduces to `q` for every positive
alpha. The next bounded priority is a board-aware fallback benchmark that keeps
the current model as a reference, uses January/February for development, and
freezes a genuinely untouched test sample before claiming improvement. Evaluate
both move likelihood and loss calibration across sparse support and all three
time controls. Make any engine-informed features or constraints explicit.

Do not tune a replacement on this March diagnostic and call performance here
new validation. Personal unprepared behavior, recall, learning effort and
counterfactual rollout credibility remain separate questions. Broader book
optimisation should wait for a better-supported fallback policy, rather than
merely a larger simulation or engine budget.
