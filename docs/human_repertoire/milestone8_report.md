# Milestone 8: board features improve likelihood, but tactical mismatch remains

Completed 2026-09-08. The bounded engine-free board-aware candidate improves
April move prediction across all three time controls. It does **not** establish
adequate human fallback behavior: at January-unseen boards both models lose at
least 150 cp in 21/48 sampled moves, versus 0/48 recorded human moves. Larger
book comparisons remain premature. Sampled model moves are model predictions,
not observations of human fallback play or repertoire benefit.

## Frozen design and fresh data

The [plan](milestone8_plan.md), [configuration](milestone8_config.json), candidate,
orchestrator and tests were committed as `7ca60ef` before development. Nothing
in the frozen M1-M7 implementation was changed. The reference is the M3
shared-token prior with its January exact-position counts and alpha=1. The
candidate changes only the legal move prior:

`q'(m|s) = q(m|s) exp(beta * w.features(s,m)) / Z(s)`

Both use `(count(s,m) + q'(m|s)) / (N(s) + 1)`, with beta=0 for the reference.
Ten symmetric board features describe check, castling, captured material,
attacks on the moving piece, defense, promotion, and hanging friendly material.
They use geometric attacks, including pinned attackers; they are not a search
or static-exchange evaluator. All legal moves retain positive mass. No engine
evaluation, result, March decision or April decision trained the coefficients.

Fixed hashing selected 600 January games, 200 each Blitz/Rapid/Classical, with
7,164 decisions and 236,301 legal rows. The ten coefficients used the declared
150 gradient steps and L2=.01. Penalized training objective fell from 2.9162
to 2.6801 at the last recorded pre-update step. Common reference token and
position counts still use all 2,250 January games. A separate 600 February
games supplied 7,176 development decisions:

| Correction strength | February log loss | February Brier |
|---|---:|---:|
| 0, reference | 2.075715 | 0.739224 |
| 0.5 | 2.012156 | 0.734790 |
| **1, selected** | **1.983492** | **0.733789** |
| 2 | 2.040682 | 0.748768 |

Weights and selection froze at 14:38:52 UTC, before any April row extraction.
The isolated M3 extractor instance wrote exclusively under `_artifacts/m8/cohort/`.
Nine convenience frames, each 65,536 rows, yielded 900 accepted games, 300 per
event. Extraction scanned 589,824 framed rows, with a decoded-row bound of
848,522, and took 9.28 seconds. The fixed frame maximum was two million decoded
rows. No selected game IDs overlap January, February or March. Both players
are >=1800, with rating gap <=200. This remains the same opening after
`1.e4 e5 2.Nf3 Nc6` and at most twelve additional plies, not all chess openings.

Frames span early/middle/late April. Blitz dates are April 5/16/26; Rapid dates
are April 6/16/26; Classical windows cover April 4-7, 14-18 and 24-27. Source
fingerprints and footer/frame bounds are in [frame provenance](milestone8_holdout_frames.json).
These are date-spread convenience samples, not random samples of all games.
Players may recur across months; the fresh split separates games and time,
not necessarily people.

## Held-out move prediction

Lower scores are better. Scores weight decisions, not strata equally.

| April group | Decisions | Reference log loss | Candidate log loss | Reference Brier | Candidate Brier |
|---|---:|---:|---:|---:|---:|
| All | 10,760 | 1.986599 | 1.895138 | 0.716284 | 0.709447 |
| Blitz | 3,597 | 1.987558 | 1.901915 | 0.716232 | 0.710088 |
| Rapid | 3,584 | 2.098383 | 1.994571 | 0.746587 | 0.738738 |
| Classical | 3,579 | 1.873694 | 1.788754 | 0.685991 | 0.679469 |
| January-seen | 7,608 | 1.525873 | 1.506966 | 0.627997 | 0.626881 |
| January-unseen | 3,152 | 3.098654 | 2.832070 | 0.929384 | 0.908735 |

Candidate-minus-reference log loss is **-0.091461**, with approximate paired
player-component bootstrap 95% interval **[-0.099806,-0.082221]**. The 2,000
resamples use 746 connected components, largest 14 games. This supports improved
likelihood on the selected frame conditional on the fitted models; it does not
include training/selection uncertainty or imply causal or population coverage.

## Fresh paired tactical calibration

The predeclared hash design selected 16 cases per event/support stratum: 96
cases from 95 games, with 48 seen and 48 unseen boards. Each model drew one
move with a common case-specific uniform, in sorted SAN order, at the same
actual human-reached parent. Cases and draws froze before engine work. The
human moves were not selected by engine quality or game outcome.

The table uses equal stratum weights. Percentages are the fraction with
mover-oriented parent-to-child loss >=150 cp; apparent improvements are clipped
at zero for nonnegative loss summaries.

| Group | Cases | Actual human | Reference draw | Candidate draw |
|---|---:|---:|---:|---:|
| All | 96 | 0.00% | 25.00% (24) | 22.92% (22) |
| Blitz | 32 | 0.00% | 28.13% (9) | 18.75% (6) |
| Rapid | 32 | 0.00% | 28.13% (9) | 28.13% (9) |
| Classical | 32 | 0.00% | 18.75% (6) | 21.88% (7) |
| January-seen | 48 | 0.00% | 6.25% (3) | 2.08% (1) |
| January-unseen | 48 | 0.00% | 43.75% (21) | 43.75% (21) |

Overall candidate-minus-reference is **-2.08 percentage points**, approximate
paired 95% interval **[-10.66,+6.82]**. Candidate-minus-human remains **+22.92 pp**
with interval **[+15.56,+30.50]**. At unseen positions the candidate/reference
contrast is exactly zero in this sample, interval **[-16.10,+16.94] pp**.
The primary tactical result does not demonstrate improvement at unseen boards.

| Balanced secondary measure | Human | Reference | Candidate | Candidate minus reference, 95% interval |
|---|---:|---:|---:|---:|
| >=300-cp loss | 0.00% | 17.71% (17) | 13.54% (13) | -4.17 pp [-11.09,+2.70] |
| Mean loss capped at 1,000 cp | 29.54 cp | 140.21 cp | 121.39 cp | -18.82 cp [-47.57,+12.56] |

All event/support/stratum contrasts and uncertainty are preserved in
[results](milestone8_results.json), including the 12-case nested depth subset.
The tactical bootstrap uses 93 connected-player components, largest two cases,
and 2,000 paired resamples. Zero observed human threshold events in a small
sample does not mean the population rate is zero. In particular, a degenerate
empirical resampling interval is not an upper confidence bound for an unseen
rare human error. The diagnostic deliberately balances support strata, so its
overall loss rate is not the April decision-frequency mixture.

## Engine coverage and verification

| Search budget | Required positions | Reused | New | New nodes | New engine seconds |
|---|---:|---:|---:|---:|---:|
| 100,000 nodes / 1 second | 278 | 44 | 234 | 23,405,438 | 26.297 |
| 400,000 nodes / 2 seconds | 38 | 8 | 30 | 12,007,463 | 13.551 |

Every required parent and human/reference/candidate child has a score. The
same frozen Stockfish 18 binary runs one thread, 32 MiB hash, no tablebases,
and retains completed non-bound scores. No new search reaches its time cap.
Engine session wall time is 43.0 seconds. The fixed 12-case deeper subset
changes no human or model classifications at either threshold; this small
subset does not establish engine invariance for all 96 cases.

The resource check found about 185.1 GiB free commit. The independent Chess.com
crawl and original checkout remained untouched. New numerical work used one
BLAS thread; all outputs are in this worktree's ignored artifact directory.

All **84 research tests passed**, including mirror symmetry, en passant,
castling/promotion, hanging-queen features, finite-difference gradients,
probability conservation, baseline parity, outcome-independent panel selection,
output confinement and the model-before-extraction guard.

Independent verification reproduces training weights, February selection and
April aggregate likelihood/bootstrap results. It replays all 96 calibration
parents, regenerates both draws with scalar normalization, performs **384
probability checks and 3,600 scalar loss checks**, traces reused scores to frozen
sources, checks engine PVs/budgets/coverage, and reconstructs all summaries and
depth comparisons. The [verification](milestone8_verification.json),
[provenance](milestone8_provenance.json) and [engine selection](milestone8_engine_selection.json)
are tracked; games, player IDs, arrays, FENs and full engine records stay ignored.
Commands are in the [research README](../../research/human_repertoire/README.md).

## Decision and next priority

Keep this candidate as a useful board-feature likelihood benchmark. Do not
promote it into the repertoire utility evaluator as a calibrated human policy.
The declared likelihood requirements pass, but unseen primary tactical excess
does not decrease, and overall tactical improvement remains uncertain. The
predeclared combined interpretation therefore does not justify progression.

Next, make a bounded residual-error audit of the frozen severe candidate draws:
show concrete positions, the selected moves and engine continuations; distinguish
direct hanging-piece mistakes from multi-move tactics and feature blind spots.
Use that diagnostic to specify one stronger board-aware candidate and its
resource requirements before spending on a larger book experiment. Treat March
and April as consumed diagnostic evidence for any such revision. Reusing April
as a fresh validation claim after looking at these failures would be invalid;
new candidate development must be followed by another untouched test period.

Human preparation status, personal recall, learning cost and multi-step rollout
credibility remain separate unmeasured quantities. This benchmark changes no
book, study budget, recall law, utility horizon or original-method reference.
