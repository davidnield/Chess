# Milestone 4: common continuation graph and targeted engine audit

Freeze this plan before graph prioritisation or new evaluation reads. The user's
>=1800 Blitz/Rapid/Classical population and rating gap <=200 remain unchanged.
Use the milestone-3 January counts and February-selected settings; do not refit,
retune, or treat this step as another untouched March prediction test.

## Fixed comparisons and graph

Compare the frozen position/uniform and position/shared-token policies on exactly
the same node/edge topology. Start with all January observed positions and moves,
with unobserved legal moves retaining an aggregate unknown frontier. Observed
endings and W/D/L shrinkage retain milestone-2 semantics. Three fixed plans:
empty, the original reference projection `3.d4`, and the previously tested
two-card plan adding `3...exd4 4.Nxd4`. No new book optimisation. Recall is .85,
independent per decision with re-entry. Keep the twelve-ply horizon, +/-300 cp
first-hit event, .99 discount, soundness and study-cost definitions unchanged.

For a bounded expansion, rank missing legal edges by the maximum expected number
of traversals over the two policies and three plans. Compute finite-horizon
forward masses with human endings, ignoring repetition and engine gates only for
this allocation heuristic. It is not the final evaluator or a probability of
ever reaching a position. Use deterministic position/move ties. Freeze the top
64 edges without looking at evaluations or outcomes beyond the existing ending
hazard. Make each selected edge's exact child explicit. At unsupported child
positions, expose the union of each policy's three most likely legal moves for
two further plies, retaining every omitted move's mass in an unknown frontier.
Reconnect existing observed transpositions. Previously observed endings are not
overridden. Stop at 512 new expanded positions / 40,000 total graph nodes, raising
on overflow rather than silently changing the recipe.

New unsupported positions receive the frozen shared/uniform move prior, the
existing W/D/L prior, and an assumed zero human-ending hazard. These are model
assumptions, not newly observed human behavior. Report the new support and its
limitations. Compare the unexpanded and expanded common graphs to separate the
effect of adding branches from the effect of changing their probabilities.

## Engine provenance and bounded work

Verify the canonical eval arrays with adopt=False and fingerprint all sources.
Read only sparse lookups for the real positions in the frozen graphs; no array
build, database scan, or canonical write. Preserve missing values explicitly.
The original array scores have mixed engine/version/depth provenance.

The installed UCI engine identifies itself as Stockfish 18. Freeze its executable
hash, reported options and identity. Rank real positions by maximum expected
visits in the expanded graph under the six fixed scenarios, again without engine
scores. Mandatory audit positions are every prescribed parent and child. Fill
up to 126 additional missing-score positions, then the remaining slots with
highest-priority previously scored positions, to at most 256 total. Fill unused
quota from the remaining ranking. This samples both missing coverage and source
disagreement; it is not a random engine-validation sample.

Use one engine, one thread, 32 MiB hash, MultiPV=1, full strength, no tablebases,
and clear hash/new-game state per position. Every initial search has a 100,000-node
and 1-second limit (whichever stops first). Recheck the mandatory positions and
the highest-priority remaining targets, 32 total, at 400,000 nodes / 2 seconds.
Record actual nodes, depth, elapsed time, CP from White's perspective, mate status,
PV and W/D/L where returned. Reject bound-only or missing root scores. Engine
limits are work caps, not a proof of score accuracy. No position is selected for
the deeper pass based on the score it received.

Write isolated resumable snapshots, with a 420-second guard before each engine
request and per-request protocol timeout. Never interrupt another process.
Resource headroom and active workloads are checked before launch. All outputs,
including engine work/cache, remain in this worktree's ignored artifact directory.

## Evaluation and decision

Report source-only evaluations and an explicitly mixed-source overlay replacing
audited positions by the new scores, using the deeper score for its fixed subset.
Also report a homogeneous-engine sensitivity using only audited Stockfish scores
and exact board terminals, leaving every other evaluation unknown. Gate every
prescribed parent/child under the same map; reject an uncertified plan for that
map without weakening the gates. Do not present the overlay as globally consistent
engine coverage. Report disagreements at +/-300 cp, gate changes, and sensitivity
between the two fixed engine budgets.

Use the existing exact evaluator for all policies/graphs/eval maps. Show early
advantage, disadvantage, points, early frontier and first-hit observation bounds.
These remain model predictions and conditional missing-information bounds, not
confidence intervals or human outcomes. No ranking is reliable merely because a
point estimate rises. A clear ordering requires separated observation intervals
under the same assumptions, as well as the stated soundness/study constraints.

Run independent tiny legal-graph/path checks, probability/support conservation,
transposition and prescribed-move checks, and engine-selection/provenance tests.
Verify baseline parity against the unchanged milestone-2 builder on these exact
training games. Check real common-graph predictions with the independent path
oracle at a bounded six-ply horizon. Final comparisons have a 360-second guard
between evaluations and the existing 200,000-state cap; no partial result is
labelled complete. Save provenance, results and a focused research-branch commit.

If observation intervals remain too wide, quantify whether omitted continuations,
missing evaluations or unstable scores dominate before proposing larger work.
No production pipeline change, new data extraction, personal recall claim or
repertoire recommendation is part of this step.
