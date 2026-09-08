# Milestone 7: held-out human-versus-model engine-loss calibration

Freeze this protocol and implementation before selecting cases or generating
model moves. This is a retrospective diagnostic on the already frozen March
cohort used in milestone 3. It is neither a fresh confirmatory test nor evidence
of causal learning benefit. No model fitting, book optimisation or new game
extraction is allowed in this experiment.

## Population, frame and selection

Use the bounded March 2024 snapshot: both players rated >=1800, rating gap <=200,
Blitz/Rapid/Classical, exact root `1.e4 e5 2.Nf3 Nc6`, and the following twelve
plies. Keep the January-fitted, February-selected shared prior unchanged.
Only positions at an actual recorded decision are eligible; no move is invented
after a human ending. Player ratings are Lichess ratings in the source, not a
conversion to over-the-board ratings or a personalised player model.

Define training support using January move counts at the exact position:
unseen = zero, seen = at least one. Cross these two levels with the three time
controls. Select 32 cases per stratum, 192 total. For each eligible game/support
pair choose one decision by its minimum fixed hash; then select games within
each stratum by a separate fixed hash. The two hash seeds are independent.
A game may contribute once to each support stratum, but never twice to one.
Record eligible games/decisions and within-game decision counts. Do not silently
replace a short stratum or drop a malformed selected case; stop and preserve
the declared frame if the requested design cannot be met.

The target is a diagnostic average with equal weight for each of the six strata,
equal weight for eligible games within each stratum, and equal weight for that
game's decisions of the specified support. It is not a decision-frequency or
population-weighted opening estimate. Observed outcomes and engine scores do
not enter case selection. The sample may contain repeated positions or players;
these dependencies and the original cohort's date-frame limitations remain.

## Paired moves and engine evidence

At each selected parent board, keep the actual human SAN and sample exactly one
SAN from the frozen shared fallback kernel, in sorted-SAN order, using a fresh
case-specific pseudo-random draw. Both moves start at the same human-reached
position. This is a one-decision check, not a free-running continuation or a
recall/study experiment. An equal sampled/human move produces equal scores.

Freeze every case, sampled move and engine target before new searches. Audit
the parent and both children, deduplicating position keys. At most 192*3 = 576
positions require the base budget. The first four selected cases in each
stratum (24 total) form a fixed nested sensitivity subset, at most 72 positions.
This subset is selected without seeing new scores and is not a replication.

Reuse matching-budget M4-M6 records only when the installed Stockfish binary,
position/FEN and recorded search limits match. The base comparison uses only
100,000-node/1-second records, the deeper subset only 400,000-node/2-second
records. Never substitute deeper scores into the base map. Use one thread,
32 MiB hash, no tablebases, reset hash/game state and the tested last-completed-
non-bound-score reader. Exact rule terminals use the existing exact cp markers.
No canonical eval lookup or production artifact is needed for this step.

Preparation scans at most 2,250 frozen games / 30,000 recorded decisions, with
a 60-second guard. Engine work has a 600-second session guard between requests,
plus existing per-request timeouts and the position/node/time caps. Save every
16 searches. An interrupted run retains every case and partial scores and
produces no completed calibration claim until coverage is complete. Resume
only with unchanged identities; no difficult case is dropped.

## Engine-loss proxy and comparisons

Let s=+1 for a White mover and -1 for a Black mover; all stored cp scores are
from White's perspective. The signed parent-to-child loss is
`s*(cp_parent-cp_child)`. The nonnegative loss proxy clips this at zero. Negative
signed values are recorded as apparent improvements/search inconsistency,
not silently treated as evidence of exact optimality. Fixed-budget parent and
child searches do not provide an exact best-move centipawn-loss oracle.

Primary endpoint: the sampled-minus-human difference in the frequency of loss
>=150 cp. Secondary: >=300 cp and mean loss capped at 1,000 cp. The cap keeps
arbitrary mate-score magnitudes from dominating the mean; also record raw and
signed values, mate involvement and the number of capped observations.
These are engine-loss markers, not categorical judgements about human mistakes
or a replacement for the early-advantage utility and soundness constraints.

Report human and sampled rates, paired differences, same-move frequency and
loss summaries for each stratum, each time control, support level and the
balanced overall panel. Report support counts/decision plies and frame coverage.
The sampled model rate uses one random move per case, so it includes move-draw
noise; do not present it as an exact integration over all legal moves.

Use a paired connected-player component bootstrap (2,000 draws, fixed seed) as
an approximate diagnostic uncertainty summary. Join all selected games sharing
a player, resample those components globally, and recompute each stratum's ratio
of weighted totals; aggregate with the same fixed equal stratum weights. This
preserves cross-stratum dependencies, including two cases from the same game.
Report component counts, largest component and empty-stratum draws. If any draw
for a reported group lacks a required stratum, mark that group's percentile
interval unavailable rather than condition away those draws or refill them.
These are approximate conditional resampling intervals, not exact confidence
coverage for the original stratified sampling design or the wider population.
They exclude training/model selection, engine error and cohort-frame uncertainty.

For the same 24 sensitivity cases, compare complete base and deeper loss proxies,
rates and paired differences. Report endpoint category changes and the direction
of cp shifts. Keep this nested result separate from the 192-case base result.
Do not choose a new threshold, sampling rule or model based on these findings.

## Verification and next decision

Test mover perspective, clipping/capping and exact terminals, deterministic
stratified selection and no score/outcome dependence, per-game quotas, full
paired support, matching-budget reuse and cluster/stratum resampling. Independently
replay each selected human move from its original opening, regenerate each
model draw, verify target completeness and engine PV legality, and reconstruct
per-case losses and all reported aggregates. Preserve compact tracked provenance,
selection, results and verification, with full cases/engine responses under the
ignored M7 artifact directory.

A systematic excess of sampled losses, especially at unseen positions, would
motivate a separately frozen model revision and new validation. Agreement on
this small human-reached panel would not validate free-running counterfactual
rollouts or human learning. Keep the original methodology as a reference and
do not recommend a repertoire from this diagnostic alone.
