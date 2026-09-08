# M10 prospective benchmark: legal exchange and checking replies

Protocol established after the M9 retrospective April audit and the January
cost pilot, before fitting the thirteen-feature candidate or opening May rows.
The cost pilot is complete; the full benchmark is not yet implemented or run.
Freeze and test its runner before development. If implementation changes this
protocol, record the revision before development and before May extraction.
Keep all M1-M9 frozen implementation and artifacts unchanged.

## Candidate and development

Use `exchange_prior.features`: the ten M8 features followed by best opponent
six-capture-ply exchange gain, gain against the just-moved piece, and number of
immediate legal checking replies. Gains are clipped at 9 and divided by 9;
checks are capped at 8 and divided by 8. En passant identifies the captured
pawn's square, not the reply destination. Optional stopping, horizon truncation
and excluded off-square tactics remain the declared M9 abstraction. No engine,
outcome, recall or study-cost feature is added. No move receives a hard veto.

Retain the frozen M3 token prior, position counts and alpha=1. Multiply its
prior by `exp(beta*w.features)` and renormalize, then apply the same count-plus-
prior posterior. All legal moves have positive mass. Train all thirteen
coefficients jointly from zero using the same 600 January games and 600 February
games, selected by the unchanged M8 hash seed. Verify the IDs against M8.
Only January observed moves fit coefficients. Use the existing conditional
prior-only softmax objective, L2=.01, 150 gradient steps and learning rate
`.5/sqrt(1+step/20)`. Initialize a thirteen-element vector explicitly; the frozen
M8 `fit_weights` function initializes ten elements and must not be called
unchanged for the new candidate. Reuse its dimension-independent objective.

February posterior log loss selects beta in [0,.5,1,2], ties favor smaller beta.
Publish all strengths' development scores. No extension for convergence or
retuning against March/April/May. Record training arrays, panel IDs, feature
names, weights, chosen strength, counts, code and protocol hashes. Fitting and
tuning finish before any May game rows are opened.

The named references are (1) frozen M3 shared-position/token policy and (2)
frozen M8 ten-feature policy with its original weights and beta=1. Do not refit
either reference. Primary improvement contrasts are candidate minus M8; report
candidate minus M3 as a secondary comparison. Beta=0 must reproduce M3, and
zero-padding M8 weights must reproduce M8 when using its beta. These are model
changes under a common evaluator, not changes to the repertoire optimizer.

## Resources and failure handling

The completed cost pilot used the first 1,000 sorted-SAN legal alternatives,
without deduplication, in M8's fixed January panel order. It imposed a 30-second
guard, 20,000 exchange nodes per child and 256-MiB sampled process private-memory
cap. It reads frozen January snapshots only, has no engine work and no fitting.
It is a small convenience cost probe, not representative performance evidence.

Before full development, check system commit headroom and active workloads.
Run one BLAS thread and reuse the isolated venv. Per feature/scoring phase:
360 seconds, 20,000 cached positions, 700,000 processed legal alternatives,
five million actual exchange nodes and 512 MiB process private memory. Sample
process memory regularly and record OS process peaks; Python allocations alone
are not a complete memory measure. Check runtime/node caps within feature
construction as well as between games. Count actual feature work on cached
positions separately from repeated scored alternatives. Record both counts.

On any bound hit, preserve a failed-phase record and do not silently remove
positions, shorten exchanges, extend the budget or change the sample. A revision
requires an explicit new development decision before test extraction. Store
everything under `_artifacts/m10/`; canonical data and other workloads remain
inputs only. No larger production jobs are part of this benchmark.

## Fresh May likelihood test

Use May 2024, Blitz/Rapid/Classical, both individual ratings >=1800 and gap <=200.
Preserve `1.e4 e5 2.Nf3 Nc6` and the twelve-ply window. Reuse the bounded M3
extractor in an isolated module instance redirected to `m10/cohort/`: three
65,536-row frames per event, at most 100 selected games per frame, at most 900
accepted games. Keep existing footer, projected-row-group, batch and runtime
bounds and the two-million decoded-row cap. This is a date-spread convenience
sample, not a population-random sample. Do not backfill invalid games using
observed model scores or outcomes.

Extraction must check the frozen candidate, both references, protocol and
implementation hashes first. Verify May IDs disjoint from January, February,
March and April snapshots; earlier-month IDs may be read for this leakage check
only. Players may overlap across months and that limitation must be reported.

For all three policies report decision-weighted log loss and Brier score pooled,
by event and by positive/zero January position count. Report paired per-game
connected-player bootstrap likelihood contrasts, 2,000 resamples, seed 20240910.
These intervals condition on fitted models and selected frames; they omit
training/model-selection uncertainty and are not causal or population guarantees.

## Paired tactical calibration

Before any new engine work, select 16 May cases per event/support stratum using
the existing M7 game/within-game hash design: 96 cases. Sample one move from
each of the three policies at the same actual human parent, using common
case-specific uniforms in sorted SAN order. Preserve the actual human move.
The first two cases per stratum form a fixed nested 12-case deeper subset.
Freeze selection, probabilities, draws, FENs and targets before engine scoring.

Parent plus human/M3/M8/candidate children need at most 480 unique base positions,
and at most 60 deeper positions. Evaluate with the frozen Stockfish 18 binary,
one thread, 32 MiB hash, no tablebases, the existing completed non-bound-score
reader and 600-second session guard. Base budget is 100,000 nodes/1 second;
deeper budget 400,000 nodes/2 seconds. Reuse only fingerprinted matching-budget
records with matching board/counter constraints. Never mix deeper scores into
base comparisons. Keep all preselected cases and require complete coverage.

Report each policy against actual human moves, plus candidate versus both
references, on mover-oriented >=150/300-cp loss and mean loss capped at 1,000 cp.
Preserve signed/raw losses and mate flags. Use equal stratum weights and the
existing paired connected-player bootstrap; make intervals unavailable when a
resample loses a required stratum. The diagnostic mixture differs from the
decision-frequency likelihood mixture. A zero observed human error rate is not
proof of zero population risk. Keep all deeper-subset results separate.

## Interpretation, verification and stopping

Call the candidate promising *relative to M8* only if all of these hold:

- Pooled likelihood difference has a 95% paired interval wholly below zero.
- Log loss improves separately in Blitz, Rapid, Classical and unseen positions.
- The unseen >=150-cp candidate-minus-M8 loss-rate interval is wholly below zero,
  and the balanced overall point difference is negative.

This deliberately conservative small benchmark may be inconclusive. Do not
relax the rule, increase draws/cases or select a secondary metric after May.
Secondary contrasts remain descriptive. Passing would support further policy
validation, not establish adequate human fallback or repertoire benefit.
Preparation status, personal recall, learning costs and rollout credibility
remain separate unknowns.

Test feature symmetry, baseline/reference parity, positive normalized mass,
thirteen-dimensional finite-difference gradients, en passant/promotion/check
cases, node/time/memory failure behavior and model-before-holdout guards.
Independently reproduce the frozen fit/selection, replay calibration parents
and all three sampled moves, check engine coverage/PVs/budgets, reconstruct
scalar losses and summaries, and verify source/code/output fingerprints.
Publish all declared comparisons, failed phases and limitations. Commit the
completed report and compact provenance on `codex/human-repertoire` only.
