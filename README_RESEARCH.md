# Human repertoire research

This is the permanent research worktree on `codex/human-repertoire`, based on original commit `f249c4dced5935619500514e26fd1ddb0dea78a1`.

**Current direction:** [use a pretrained Maia human policy](docs/human_repertoire/maia_direction.md).
The custom M10 benchmark is paused after its cost pilot. [Maia-2 integration](docs/human_repertoire/maia_integration.md)
is now working at the user's chosen **1400 fallback rating**, with actual
opponent conditioning and unchanged opponent behavior. All 98 tests pass and
both real checkpoints pass 96-case diagnostic inference. Next is a bounded
repertoire comparison under fallback and recall sensitivity. Maia-3's documented training period
overlaps our 2024 samples; do not call those samples held out for that model.

Start with [the research brief](docs/human_repertoire/human_repertoire_start_here.md). Its proposed workspace has now been created here. Context documents are copied into this worktree; their original paths and SHA-256 hashes are in [bootstrap.json](docs/human_repertoire/bootstrap.json).

The first milestone tests whether fallback play and imperfect recall change the best preparation plan. Implement a transparent small evaluator, an independent exhaustive oracle, and a bounded real opening comparison. Keep the initial code in `research/human_repertoire/`, notes and reports in `docs/human_repertoire/`, and generated data in `research/human_repertoire/_artifacts/` (ignored).

Claude's existing project continues in `C:/Users/David/Documents/Chess`. This worktree has its own files and branch. External datasets, processes and memory remain shared, so isolate outputs and bound workloads.

Milestone 1 is complete: [findings and limitations](docs/human_repertoire/milestone1_report.md),
[model contract](docs/human_repertoire/MODEL.md), and
[runnable commands](research/human_repertoire/README.md). The synthetic examples
show recall/budget-dependent book changes. The bounded real example cannot
distinguish early advantage and does not justify changing the user's repertoire.

Milestone 2 is also complete: [documented BRC cohort and validation report](docs/human_repertoire/milestone2_report.md).
It uses 4,000 games with both players rated >=1800 across Blitz, Rapid and
Classical (rating gap <=200), a twelve-ply horizon, and separate January/February
2024 cohorts. The move model has useful predictive signal against a weak baseline,
but sparse continuation support leaves book comparisons unresolved. Next work
should improve and validate fallback prediction and engine coverage on a sample
spread across dates before expanding optimisation or recommending a repertoire.

Milestone 3 is complete: [shared fallback and date-spread test](docs/human_repertoire/milestone3_report.md).
It freezes 6,750 games across January fitting, February tuning and March testing.
A simple shared move prior improves March log loss from 2.180 to 2.027 across
26,857 decisions, with gains in all three time controls and at unseen positions.
The 40-test suite and independent baseline parity audit pass. This supports
bounded integration into a common graph and an influential-position engine audit;
unresolved continuations still prevent a repertoire recommendation.

Milestone 4 is complete: [common graph and targeted engine audit](docs/human_repertoire/milestone4_report.md).
Both frozen priors now share a legal topology, with 64 missing-edge extensions
and 256 Stockfish targets plus 32 deeper checks. All 50 tests and real path-oracle
checks pass. The two-card shared-prior first-hit interval remains [3.24%,87.53%];
99.5% of unresolved mass first encounters an aggregate omitted-move frontier.
Next work should test bounded full-horizon trajectory sampling with explicit
engine coverage and Monte Carlo error, before any new book optimisation.

Milestone 5 is complete: [full legal trajectory experiment](docs/human_repertoire/milestone5_report.md).
The tested sampler removes computational frontiers with 6,144 final trajectories,
while separating missing scores from Monte Carlo error. All 61 tests, 24,576
exact-graph control trajectories and complete pilot/final replay checks pass.
The shared two-card first-hit interval is [10.35%,78.91%]; the primary discounted
two-minus-one contrast remains unresolved at [-30.09,+30.44] percentage points
including conservative simulation error. Only 12 of 512 new pilot engine targets
recur in the final sample. Next work should test complete engine coverage of a
small preselected paired path panel before expanding optimisation.

Milestone 6 is complete: [complete engine coverage of a paired panel](docs/human_repertoire/milestone6_report.md).
All 64 pairs have complete base-budget evaluations, and a fixed 16-pair subset
also has complete deeper evaluations. The 69 tests and independent replay pass.
The discounted two-minus-one estimate is -4.18 percentage points (MC SE 3.70),
with conservative 95% bounds [-38.14,+29.77]; no book ranking follows. New engine
work took about 130 seconds. Missing-score uncertainty is eliminated on this
panel, while simulation precision and fallback extrapolation remain limiting.
Next work should calibrate engine-loss predictions against a bounded held-out
human decision sample before investing in a larger book comparison.

Milestone 7 is complete: [human-versus-model engine-loss calibration](docs/human_repertoire/milestone7_report.md).
The frozen 192-case March diagnostic shows >=150-cp loss in 27.08% of model draws
versus 3.65% of human moves at the same boards. The mismatch is concentrated in
training-unseen positions: 47.92% versus 3.13%, difference +44.79 percentage
points with approximate component-bootstrap bounds [+35.68,+54.28]. The fixed
deeper subset changes no loss classifications. All 76 tests and independent
verification pass. Next work should benchmark a board-aware fallback replacement
with new held-out validation; larger book comparisons should wait for improved
human-policy credibility. March is retrospective diagnostic data, not a fresh
test set for tuning a replacement.

Milestone 8 is complete: [board-aware fallback benchmark](docs/human_repertoire/milestone8_report.md).
The frozen candidate improves log loss from 1.987 to 1.895 on 900 fresh April
games, across Blitz/Rapid/Classical and seen/unseen positions. Tactical credibility
remains insufficient: both models lose >=150 cp in 21/48 unseen-position draws,
versus 0/48 actual moves. Overall candidate/reference difference is -2.08 pp,
approximate paired 95% interval [-10.66,+6.82]. All 84 tests and independent
verification pass, with complete engine coverage. Next priority is a bounded
residual-error audit to specify a stronger board-aware candidate. April is now
consumed diagnostic evidence for revisions; broader book comparisons still wait.

Milestone 9 is complete: [residual tactical audit](docs/human_repertoire/milestone9_report.md).
Among 22 severe candidate draws, 15 frozen engine lines begin with a capture,
while only three trigger the moving-piece hanging feature. A bounded legal
exchange diagnostic finds positive exposure in 16, but also in 16/96 actual
human moves; it must not be a hard veto. The candidate resolves 10 reference
failures and introduces eight. All 89 tests pass; verification replays 1,516
PV plies and exports all 22 severe positions as annotated PGN. No new engines,
source scans or model fitting were used. Next priority is a January-only cost
pilot and prospective protocol for M8 plus legal-exchange/check-response
features, with fresh May validation only after freezing development.

Milestone 10 is in progress: [pilot and protocol status](docs/human_repertoire/milestone10_status.md).
The thirteen-feature January cost pilot passes: 1,000 alternatives in 0.727
seconds, 5,903 exchange nodes, about 71 MiB peak working set. All 92 tests pass,
and exact pilot replay reproduces every vector. The small pilot covers three
games and does not guarantee full-phase cost. The prospective May protocol is
established; no fitting or May extraction has run. Next priority is implementing
and testing the bounded runner before January/February development and freezing.
