# First milestone: fallback play and imperfect recall

Completed 4 September 2026 local time / 5 September UTC. **All numerical policy
comparisons below are model predictions, not evidence of improved human play.**

The prototype establishes the modelling difference on transparent examples. The
bounded real opening example does not distinguish early advantage credibly. It
does not justify replacing the original method or changing the user's repertoire.

## What was built and verified

A finite-horizon evaluator mixes recalled prescriptions with configurable fallback
play and permits preparation re-entry. Exact search selects one move per distinct
learned position under a storage budget. It retains the selected book, and an
independent subset/product enumerator plus complete-path evaluator checks it.

**17 tests passed in 0.650 s.** Tests cover all three mass partitions, analytic
recall mixtures, shared-position cost, re-entry, history-dependent repetition,
horizon cutoffs, conversion after first hit, unknown evals, infeasible constraints,
search caps, legal chess transitions/hashes, frozen-source drift, and serialised
book reconstruction. Exhaustive fixture comparisons and 20 seeded tiny cyclic/
shared graphs check every feasible book across budgets 0–3. The final grid verifies
72 synthetic and 36 real scenarios, plus nine real sensitivity cases, against the
independent oracle. It completed in **7.09 s**, with at most 112 books per real
optimisation. These are small-instance correctness results, not scaling evidence.

Implementation revision: `13dee70` (core initially committed as `9edc934`). The
saved run records the full revision, Python 3.11.15, pinned dependencies and hashes
of every research Python source. Baseline remains `f249c4d`; setup is `39e0e74`.
Production implementation, trainer and canonical artifacts were not modified.

## Assumptions and comparison design

The primary objective is discounted probability of the first observed +/-300 cp
event being an advantage, by H plies. Gamma=.99 prefers earlier advantage.
Expected points break ties; fewer cards break remaining ties. Require expected
points >=.48, first-disadvantage probability <=.40, and a distinct-position budget.
Prepared candidates require known engine values >=-100 cp and no more than 150 cp
below the parent's eval. These are illustrative constraints, not personal settings
or an unconditional soundness guarantee when a player forgets.

Recall events are independent across decisions and visits. Fallback can find the
prescribed move. Re-entry recognises a later prepared position; the comparison
mode disables further recall at the first unprepared decision or latent recall
failure. Repetition is an explicit automatic draw on the third occurrence within
the supplied history. First-hit measurement stops at its first event; W/D/L
continues. Actual chess terminal transitions are checked; frontier W/D/L is a
continuation model. Full semantics are in [MODEL.md](MODEL.md).

The graph is fixed within each experiment. All arms share the same evaluator:
unrestricted exact search, exact search restricted to source moves, a fixed source
prefix/projection, a perfect-recall optimum replayed under changed recall, and
empty preparation. The synthetic source is a declared demanding-line fixture,
not production output. The real source is an existing saved book. Search machinery
is held exact; changing recall/fallback/opponent parameters changes assumptions.
Source-only versus unrestricted search changes the available prescriptions.
This does not measure or reproduce the production optimiser's approximation gap.

## Synthetic findings

The easy move buys a 50% chance of advantage at ply 2. The demanding route buys
90% at ply 6 if two further decisions are executed; slipping at either can produce
disadvantage. The root fallback is 10% demanding / 10% easy / 80% quiet. Fallback
success at each demanding follow-up is varied independently. Outcomes after
advantage are 80/15/5 W/D/L; after disadvantage 10/20/70; quiet 35/30/35.

| Follow-up fallback success | Recall | Budget / actual cards | Root prescription | Discounted early advantage | First disadvantage | Expected points |
|---:|---:|---:|---|---:|---:|---:|
| .10 | 1.00 | 1 / 1 | Easy | .490050 | .000000 | .687500 |
| .10 | 1.00 | 3 / 3 | Demanding | .847332 | .000000 | .837500 |
| .10 | .65 | 1 / 1 | Easy | .335981 | .034650 | .618161 |
| .10 | .65 | 3 / 3 | Easy + two demanding follow-ups | .349600 | .018577 | .628407 |
| .80 | .65 | 1 / 1 | Demanding | .388622 | .246600 | .580543 |
| .80 | .65 | 3 / 3 | Demanding | .519159 | .092543 | .678754 |

At recall .65 and weak fallback, replaying the three-card perfect-recall plan
scores .289501 discounted early advantage. Re-optimising scores .349600, a model
difference of .060099. The extra two cards in the easy plan protect the fallback
route into the demanding line: useful preparation need not be a prefix of the
nominal main line. At perfect recall those off-route cards provide no benefit and
the one-card easy plan is sufficient for its payoff.

The result follows directly from the stated probabilities. With recall r and
follow-up fallback success s, each prepared follow-up succeeds with r+(1-r)s;
the demanding route depends on two such decisions. Changing r or s changes its
execution probability. The grid also reduces the demanding payoff's opponent
error probability from .9 to .6; no opponent-strength model is inferred from that
scenario. In the shared-position fixture, one card yields advantage probability
1.0 with re-entry versus .4 with absorbing fallback. It costs once despite two
move orders. None of these deliberately constructed effects estimates human size.

## Bounded real example and negative result

Root: `1.e4 e5 2.Nf3 Nc6`, White to move. The small existing stats file contains
42,040 rows; the snapshot retains **78 move rows, 79 positions, 10 expanded
positions and all 79 engine evals**. Root candidates are `Bb5`, `Bc4` and the
source's `d4`. Two popular opponent replies per expanded branch lead to further
study candidates; all other recorded move probability ends at explicit frontiers.
The saved 20-card sharp truncation projects to **one card**, `3.d4`, in this graph.

There is **zero modelled first-advantage and first-disadvantage probability in
every main real scenario**. All mass reaches a frontier; conversion after an early
advantage is undefined. The horizon is at most three additional plies, with some
branches cut earlier. Zero here is not evidence that later advantages never occur.

With empirical fallback/opponent probabilities:

| Recall | Budget / actual | Selected plan | Expected points | Source projection points / actual cards |
|---:|---:|---|---:|---:|
| .85 | 0 / 0 | Empty | .526740 | .526740 / 0 |
| 1.00 | 1 / 1 | `3.Bb5` | .536037 | .534165 / 1 |
| .85 | 1 / 1 | `3.Bb5` | .534642 | .533051 / 1 |
| .85 | 2 / 2 | `3.Bb5`, `3...Nf6 4.d3` | .536364 | .533051 / 1 |
| .65 | 2 / 2 | `3.Bb5`, `3...Nf6 4.d3` | .533914 | .531566 / 1 |

At matched one-card cost and recall .85, the root disagreement is worth only
.001591 expected points in this uncalibrated model. The two-card comparison also
changes study depth: the reference projection contains only one available card.
It is not evidence that exact search defeated the full 20-card source book.

Recall alone does **not** change the selected real root in this grid. Tilting
fallback probabilities toward better engine moves changes the second card to
`3.Bb5 a6 4.Bxc6`. Opponent tilt and H=1/2/3 leave the root at `Bb5`; shorter H
makes the second card useless. Uniform +/- .02 frontier expected-point shifts
change reported levels by .02. More revealingly, adding .01 to `d4` descendants
and subtracting .01 from `Bb5` descendants reverses the optimum to `3.d4` plus
`3...exd4 4.Bc4`. This is a sensitivity scenario, **not a confidence interval**.
The small tie-break ranking is not sufficiently supported for a recommendation.

## Provenance, uncertainty and scope

The [durable provenance record](milestone1_provenance.json) includes source paths,
sizes, timestamps, full hashes of the small sources and all extracted snapshots,
schemas and adapter hashes. Large engine inputs use size/mtime, source-footer
verification and metadata hashes; no multi-GB hash scan was performed. Extraction
checks identities before and after reading and refuses stale snapshot reuse.

The stats filename indicates a January 2018 test slice, but its exact sampling
recipe/rating threshold is absent from the metadata. Its population cannot be
claimed to match the user. The saved reference lacks a build sidecar; its propagated
values are unused. Newer mixed-source engine evals lack per-position version/depth
information. This is not a train/validation/holdout comparison.

Frequencies condition on surviving recorded move continuations. At the root the
excluded aux counts are 5 normal endings, 25 flags, 69 OTHER continuations, zero
other termination/horizon counts. All 99 excluded records are retained in the audit
snapshot; conditioning is not arrival-population conservation. Frontier W/D/L is
historical, shrunk with 100 prior observations at (.45,.10,.45), and remains
confounded by player/action selection. No calibrated uncertainty interval or
causal uplift estimate is available. Correlated forgetting, opponent preparation,
time control, retention and measured study effort remain unmodelled.

The pre-extraction resource check recorded 164.50 GiB free commit headroom;
Claude's independent Python workloads were left running. Inputs were capped at
8 MiB/100,000 rows each; the final graph has 79 nodes. No full-data builds, model
training, canonical writes or automatic baseline updates were performed.

## Reproduce and continue

[Runnable commands and output inventory](../../research/human_repertoire/README.md)
cover environment setup, tests, synthetic-only execution, guarded extraction and
the full grid. [experiment.json](experiment.json) is the tracked recipe; the full
result under `_artifacts/runs/milestone1.json` contains resolved per-scenario
configs, selected books, all W/D/L and hit metrics, constraints and provenance.

The next empirical step is a separately frozen, longer opening extract with a
documented population and enough observed horizon to test the early-advantage
objective. Obtain the user's rating/time control, study budget and retention
preferences before personal recommendations. Fit/check fallback and opponent
probabilities on separate data, use consistent engine analysis around candidate
gates, and test correlated recall scenarios. Expand optimisation scale only after
the evaluator can distinguish the alternatives credibly. The original methodology
remains the reference; this milestone supplies a checked comparison tool.
