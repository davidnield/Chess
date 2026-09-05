# Milestone 1 model contract

The prototype is a finite graph model of preparation, not a replacement for Stage 3.
Baseline code is f249c4dced5935619500514e26fd1ddb0dea78a1; bootstrap is 39e0e74.
All quantities are from the prepared player's perspective. Synthetic numbers and
real-example evaluations are **model predictions**, not measured human uplift.

## Policies, recall and cost

A book maps each distinct position to at most one move. A card costs one by
default, including cards reached through several move orders. Positive integer
costs can represent a stated study-cost scenario, not measured study minutes.
Books may omit any position: later cards remain available with re-entry enabled.
There are no mandatory prefix cards; profitable follow-up packages must emerge
from evaluating the full policy. Unreachable cards still cost storage.

At each our-side occurrence of a booked position, independently remember with
probability r(position), then play its prescription. Otherwise sample the stated
fallback policy. Thus P(move) = r * indicator(prescription) + (1-r) * fallback.
Fallback may find the prescription. Independence applies across positions and
repeated visits, conditional on this fixed scenario; it excludes correlated
session lapses, learning, fatigue, and difficulty inferred from games. Opponent
probabilities are fixed by position, independent of our book and latent recall.

Default re-entry recognises any later booked position. With re-entry false, the
first unprepared our-position or latent recall failure disables all later recall,
even if fallback happens to choose the prescribed move. An opponent branch alone
does not disable recall: encountering an unprepared our-position does. This is an
explicit absorbing-fallback comparison, not an exact reproduction of production
stopping. Graph transitions represent one ply; opponent actions cost no cards.

## Objective and gates

Maximise E[gamma^T * 1(first qualifying observation is advantage by horizon H)].
Thresholds are symmetric (+/-300 cp by default). T=0 counts an already-qualifying
root. Earlier hits win through discount gamma=.99. First advantage, first
disadvantage and neither are mutually exclusive; later oscillations cannot earn
another hit. Continue play for W/D/L and conversion after the event.
Unknown cp is **unobserved**, not zero; report probability of encountering it.
Reported first-hit probabilities are only for observed markers.

Require root expected points >= configured minimum and first-disadvantage
probability <= configured maximum, plus total card cost <= budget. Among equal
early-advantage values, prefer expected points, then fewer cards. Quantities are
rounded at 12 decimals for deterministic objective ties; never use this to claim
empirical precision. Synthetic defaults for experiments are points >=.48 and
disadvantage <=.40; these are illustrative utility constraints, not personalisation.

Only prepared candidates with known parent and move engine values pass the gate:
child sound_cp >= -100 and >= parent cp -150 by default. There is no all-gated
relaxation. Sound_cp is distinct from human probabilities and W/D/L. This is a
local engine gate, conditional on accuracy of the supplied eval. It is **not** a
guarantee against forgotten moves, all legal opponent defences, or the complete
future played policy. Fallback is allowed to blunder; report that exposure through
the evaluator. No engine-score sigmoid is treated as calibrated outcome data.

## Horizon, history, terminals and frontiers

H counts plies from the supplied root, inclusive of observations at H. Observe
the node's cp first. Exact graph terminal outcomes take precedence; then draw on
the Nth visit to an identical node (N=3 default); then a frontier; then horizon
truncation. Disabling repetition still terminates at H. This Nth-visit rule is a
stipulated automatic claim policy. It does not implement claim-before-move,
50/75-move rules, or complete FIDE adjudication on arbitrary synthetic graphs.
History is carried in evaluator state, so (position, ply) alone is never mistaken
for enough repetition information. Cards are shared across history states.

Terminal nodes carry rule outcomes in chess adapters (synthetic terminals can
carry expressly invented outcome distributions). Frontier/horizon W/D/L is a
specified continuation model, not termination of the actual chess game. Unknown
evaluations cannot certify prepared moves. Report mass exiting by each reason,
W/D/L, expected points, first-hit categories, discounted early advantage, and
P(win | first advantage) when defined. Terminal/repetition mass plus frontier and
horizon mass must equal one. These are separate from the first-hit partition.

## Exactness and comparisons

`model.optimise` searches global position decisions depth-first with budget
pruning and stores the actual winning book. It raises at a configured search cap;
it never reports an interrupted search as exact. `oracle.py` independently
enumerates position subsets and move products and sums complete paths without
calling the evaluator, gate, objective or cost helpers. Compare every metric,
budget feasibility and objective, not just one selected book when ties exist.
Both are exponential, deliberately small tools. No scalable solver claim follows.

Use the same graph and evaluator for unrestricted exact search, source-move-only
search, empty preparation, and fixed source candidates. Change recall/fallback/
opponent parameters separately. Source-only versus unrestricted search changes
the action set; frozen perfect-recall book versus a newly optimised imperfect-
recall book isolates adaptation to assumptions. Production propagated values are
reference diagnostics only and never used as an independent outcome evaluator.

## Status

2026-09-04 (2026-09-05 UTC): milestone implemented and verified. Isolated environment
is in `research/human_repertoire/_artifacts/venv`. The real adapter is specifically
a White-to-move, three-ply demonstration after `1.e4 e5 2.Nf3 Nc6`. It replays every
recorded edge with python-chess, checks this checkout's signed Polyglot hashes,
and verifies the read-only eval-array provenance without adopting metadata.

At our positions, two most frequent recorded moves plus any observed reference
move are study candidates. At opponent positions, the two most frequent replies
are expanded to another our-decision. All recorded moves retain their probability;
unexpanded branches become frontiers. The allowed candidates are fixed across
comparisons. A fallback/stronger-opponent sensitivity exponentially tilts observed
frequencies toward better supplied engine values while preserving their support.
This is an explicit scenario, not a calibrated strength model.

Real W/D/L uses counts with 100 prior observations at (.45,.10,.45), using outgoing
counts for expanded nodes and extracted incoming counts at frontiers. This is a
descriptive shrinkage model, with no independence or counterfactual-validity claim.
Move frequencies condition on recorded surviving continuations. Aux term, flag,
other and horizon counts are preserved for audit but excluded from this explicitly
conditional population; conservation applies to this population only. No source
propagated value enters the evaluator. See the report for the resulting negative
early-advantage finding and the limitations of the tie-break outcome ranking.
