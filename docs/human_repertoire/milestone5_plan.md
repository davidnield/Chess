# Milestone 5: bounded full-continuation sampling

Freeze this protocol before generating pilot trajectories. Preserve the >=1800
Blitz/Rapid/Classical population, rating gap <=200, January training counts and
February-selected priors from milestone 3. This is an inference experiment under
those frozen models, not new validation against human games. Use the same empty,
one-card `3.d4` and two-card `3.d4` / `3...exd4 4.Nxd4` plans. No optimisation.

## Sampler and estimand

Follow actual legal chess moves until the existing twelve-ply horizon, board
terminal, stipulated third position occurrence, or sampled human ending. Observe
engine markers at the root and final arrival, matching the exact evaluator.
Continue after a first known hit to preserve the outcome estimand. Re-entry,
independent recall .85, soundness gates, card cost, +/-300 cp competing first hit,
.99 discount and outcome priors remain unchanged.

At unsupported positions, use the frozen prior over **all** legal moves. Do not
construct every possible child, prune low-probability moves, or stop at an
aggregate omitted-move frontier. Register a child only when it is visited. Keep
the actual position history for repetition. At recorded positions use the same
ending hazard, endings/(endings+continued decisions); if there are no uncensored
observations, assume zero hazard. Horizon-censored arrivals do not dilute that
hazard. An unsupported position with observed endings but no continued decisions
therefore has hazard one. This extends the hazard model to positions the previous
graph truncated; it is an assumption, not evidence of future human behavior.

Use conditional W/D/L vectors at endpoints rather than sampling an extra game
result. These are the existing empirical/prior outcome predictions and remain
weak evidence for human performance. No engine W/D/L is substituted for them.
Leave missing evaluations explicit and score their first-hit uncertainty after
the legal trajectories have been generated, without selecting paths on scores.

For each trajectory index and ply, deterministic pseudo-random uniforms for
ending, recall and move selection are independent channels. The same uniforms
are shared across both policies and all three plans. Moves are ordered by SAN.
Sharing draws changes covariance, not marginal move probabilities. Distinct
pilot, final and truncated-control seeds are fixed in the config.

## Pilot and engine selection

Generate 256 pilot trajectories per arm (six arms, 1,536 trajectories), with at
most 20,000 distinct visited positions. Read only sparse canonical evaluations,
verifying array metadata with adopt=False and source fingerprints. Reuse the
milestone-4 audited scores only if the engine executable identity is unchanged.
The mandatory prepared parents/children retain their audited gate checks.

Choose at most 512 **additional missing-eval** targets from pilot positions.
Rank by the maximum count of being the first missing evaluation before a known
first hit in any arm, then total such counts, then overall pilot visit count,
then position key. Fill remaining slots from other missing visited positions
using that same order. Select the first 32 for a deeper check, without looking at
their new scores. Record all target selection before engine search.

Reuse the tested Stockfish reader and limits: one thread, 32 MiB hash, fresh
hash/game state, 100,000 nodes/1 second initially and 400,000 nodes/2 seconds for
the fixed deeper subset. Keep the last completed non-bound score, mate status,
actual work, and interrupted-final-iteration flag. Source and audited overlays
are labelled as mixed evidence; homogeneous audited-only sensitivity is separate.
No source/canonical artifact or original process is modified.

## Independent final sample and error

Freeze the completed engine map **before** generating final trajectories. Use
1,024 fresh indices per arm (6,144 trajectories), at most 80,000 distinct visited
positions. This cap exceeds the maximum 79,872 arrivals, so no trajectory is
censored to meet a data-dependent cap. The pilot's corresponding maximum is
19,968. Source lookups on final positions consult the same fixed canonical map;
final paths never select more engine targets or change the fitted policies.

Report sample mean metrics and Monte Carlo standard errors. For first advantage,
each path has a lower indicator (a known advantage before any unknown eval) and
upper indicator (lower plus unresolved first-hit status). Also report discounted
endpoints. Show their sample interval separately from a conservative 95% Monte
Carlo outer envelope: subtract/add sqrt(log(2/alpha)/(2N)) from the lower/upper
means, clipped to [0,1]. The two one-sided Hoeffding bounds jointly cover the
population endpoints with at least 95% under independent simulation replicates.
Coverage conditions on frozen policy/eval maps and does not include model,
training, engine, sampling-frame or human-performance uncertainty. It is per arm,
not simultaneous coverage of every reported comparison.

Primary plan contrast: two cards minus one card under the shared prior and audited
overlay. Use paired trajectory differences. Missing-information endpoints are
L_two-U_one and U_two-L_one. When both trajectories visit the identical position
sequence, their true first-hit value is identical even if evals are missing, so
both difference endpoints are exactly zero. Otherwise retain the conservative
endpoints. Show paired and independence-assumed standard errors for the observed
marker difference. A contrast endpoint lies in [-1,1], so its Hoeffding outer
envelope uses twice the arm margin. No positive recommendation follows unless
the outer difference interval excludes zero under the stated constraints.

## Validation, budgets and handoff

Check sampling and scoring against tiny exact graphs, including recall failure,
re-entry, shared positions, repetition, arrival ending/horizon priority, unknown
evals before/after hits, and paired identical paths. Run 4,096 control trajectories
per arm on the unchanged milestone-4 expanded graph/overlay and compare all metric
means with its saved exact results. These control graphs keep their frontiers;
the full-continuation experiment deliberately changes that truncation.

Sampling phases have a 300-second guard between trajectories; engine requests
have the existing per-request timeout and a 600-second phase guard. Save separate
pilot, engine, final and verification snapshots with source/config/code hashes.
Runtime or identity failures return no completed result. Reuse requires matching
identities; preserve prior snapshots before an explicit revision. Record actual
resources and all selected comparisons, including negative or unresolved results.

This step tests whether full legal continuation sampling removes computational
truncation at a bounded cost. It does not validate the prior far from observed
human positions or establish that the preparation is learnable or beneficial.
