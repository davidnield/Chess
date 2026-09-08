# Milestone 5: bounded full legal trajectories

Completed 2026-09-07 America/Los_Angeles (2026-09-08 UTC). Protocol and sampler
were committed as `1f1050d` before pilot generation. All numbers below are
**model predictions or computational diagnostics**, not observed benefits from
studying a repertoire.

Full legal sampling removes the computational frontiers at modest cost, but
does not resolve the book decision. For the shared prior, the two-card plan's
first-advantage observation interval is **[10.35%, 78.91%]**, with a conservative
95% Monte Carlo outer envelope of **[6.11%, 83.15%]**. The primary discounted
two-card-minus-one-card contrast has an observation interval of
**[-21.60, +21.96] percentage points**, widening to **[-30.09, +30.44]** with
Monte Carlo error. Neither supports a positive repertoire recommendation.

The main practical finding is poor reuse of pilot-selected engine work: only
**12 of 512** newly audited positions appeared in the independent final sample.
On the final shared/two-card paths, that new work resolved just four additional
first-hit statuses out of 1,024. The next bounded priority is complete engine
coverage of a preselected paired trajectory panel, before attempting to rank
more books or expanding the model.

## Frozen comparison and interpretation

The [pre-analysis protocol](milestone5_plan.md) and [config](milestone5_config.json)
retain milestone 3's January training counts and February-selected policies:
position counts with a uniform legal prior (`tuned_position`), and position counts
with the shared move-token prior (`shared_position`). The underlying population
has both players rated >=1800 in Blitz, Rapid and Classical, rating gap <=200.
These priors pool those time controls. This experiment adds no human games,
training, tuning or independent human validation.

Start after `1.e4 e5 2.Nf3 Nc6`, from White's perspective. Compare no cards, the
original-source one-card projection `3.d4`, and two cards adding `3...exd4 4.Nxd4`.
Study costs are 0, 1 and 2 distinct positions. Recall is independently .85 at
each prepared decision, with re-entry. The horizon is twelve additional plies;
the competing first-hit markers are +/-300 cp, with .99 per-ply discount. The
existing soundness floor (-100 cp), maximum parent-to-child loss (150 cp),
points floor (.48) and disadvantage ceiling (.40) remain explicit assumptions.
Both prepared moves still pass the inherited deeper engine gate checks:

| Prepared move | Parent cp | Child cp |
|---|---:|---:|
| 3.d4 | 47 | 17 |
| 4.Nxd4 after 3...exd4 | 14 | 14 |

The sampler follows every selected legal move, including unsupported moves,
without creating or pruning the entire tree. Position history determines the
third-occurrence stopping rule. Arrival evaluations precede terminal, repetition,
human-ending and horizon checks, matching the exact evaluator. Unsupported
positions use the frozen full legal prior. The ending-hazard extension is
documented in the protocol; outcomes retain empirical/prior W/D/L vectors and
are not replaced with engine outcomes.

The final sample contains 1,024 trajectories per arm, coupled by index across
both policies and all three plans. Distinct seeds separate pilot, control and
final work. The pilot and its engine map were frozen before generating the final
sample. Final paths did not select further engine targets.

## Results on the full horizon

This table uses the explicitly mixed-source audited overlay. Percentages in
the first two numeric columns are sample estimates. The observed discounted
marker can follow an earlier unknown evaluation, so it is not a fully identified
true first-hit estimate. The observation interval addresses that ambiguity.

| Policy / cards | Observed discounted advantage | Observed disadvantage first | Expected points | First-advantage observation interval | 95% MC outer envelope |
|---|---:|---:|---:|---:|---:|
| Shared / 0 | 7.06% | 3.61% | .49941 | [7.32%, 80.47%] | [3.08%, 84.71%] |
| Shared / 1 | 9.56% | 5.76% | .49959 | [10.06%, 78.81%] | [5.81%, 83.05%] |
| Shared / 2 | 9.93% | 4.98% | .49805 | [10.35%, 78.91%] | [6.11%, 83.15%] |
| Uniform / 0 | 6.17% | 3.52% | .49985 | [6.25%, 82.32%] | [2.01%, 86.57%] |
| Uniform / 1 | 8.29% | 4.10% | .50008 | [8.59%, 82.32%] | [4.35%, 86.57%] |
| Uniform / 2 | 8.77% | 4.20% | .49855 | [9.08%, 82.23%] | [4.84%, 86.47%] |

The points predictions barely differ and rely heavily on a neutral outcome
prior; their small simulation error does not validate the outcome model. The
unresolved first-hit mass also prevents certifying the actual disadvantage
constraint from the observed disadvantage column. Passing the individual move
soundness gates does not settle either performance constraint.

For the primary shared/two-minus-one comparison, 693 of 1,024 pairs (67.68%) have
identical position sequences. Their first-hit difference is exactly zero even
where scores are missing. The remaining pairs retain conservative missing-score
bounds. All six predeclared policy/evidence contrasts are in
[the result snapshot](milestone5_results.json).

| Shared two-minus-one quantity | Estimate | MC standard error |
|---|---:|---:|
| Observed discounted marker difference | +0.364 pp | 0.662 pp |
| Same difference assuming independent arms | same mean | 1.262 pp |
| Expected-points difference | -.001543 | .000883 |

Coupling nearly halves the observed-marker standard error, but missing-score
uncertainty remains much larger. The 95% outer envelopes add 4.244 pp to each
arm endpoint or 8.488 pp to each paired contrast endpoint. They are per-comparison
conditional simulation bounds, not simultaneous coverage across every row and
not confidence statements about human performance or engine accuracy.

Source-only sensitivity gives shared/two-card bounds [9.86%, 79.49%] and a
discounted paired outer interval [-30.74, +30.67] pp. Using only homogeneous
audited scores leaves 99.51% of shared/two-card first hits unresolved, with
first-advantage bounds [0%, 99.51%] and paired outer interval
[-40.81, +40.52] pp. The conclusion is unresolved under every evidence map.

## Where the remaining uncertainty comes from

Milestone 4's shared/two-card first-hit interval was [3.24%, 87.53%], largely
because omitted legal moves stopped at aggregate frontiers. The new sampler
has **zero frontier stops**. This is a changed continuation model, including the
explicit ending-hazard extension, rather than a sampling estimate of that old
truncated graph. The separate control run establishes agreement when the graph
is held fixed.

The final sample visits 20,279 distinct positions. Canonical scores are missing
at 14,918; the combined M4/M5 engine overlay leaves 14,875 missing. For the
shared/two-card arm, unresolved first-hit mass falls from the old truncated
graph's 84.30% to 68.55% in the full-continuation sample. All remaining first-hit
uncertainty is due to missing real-position evaluations, not computational cuts.
This comparison is descriptive and includes simulation error.

Only 12/512 (2.34%) new pilot targets recur in the final sample. A post-experiment
coverage diagnostic on the same final paths isolates their incremental value:
the new M5 scores reduce unresolved first-hit mass by 0.00, 0.20 and 0.39 pp for
shared 0/1/2-card arms, and 0.10, 0.10 and 0.00 pp for uniform 0/1/2-card arms.
The larger change from M4 mostly comes from following continuations and reading
their existing canonical scores, rather than the new 512-position audit.

The sampler also relies on extrapolation: 3,279/12,241 (26.79%) shared/two-card
decisions occur at positions with no January move observations. Across all arms
that fraction ranges from 26.79% to 30.11%. Prior validation on held-out human
decisions does not validate the distributions induced after hypothetical
preparation or long unsupported continuations.

## Engine evidence, resources and validation

The installed Stockfish 18 binary matches milestone 4's SHA-256. All work uses
one thread, 32 MiB hash, cleared hash/fresh game state and no tablebases. The
pilot rank selected 512 additional missing positions before their scores were
known; the first 32 ranked positions received deeper searches.

| Work | Positions | Total nodes | Engine-reported seconds | Median completed depth |
|---|---:|---:|---:|---:|
| 100,000 nodes / 1 second cap | 512 | 50,964,133 | 54.462 | 16 |
| 400,000 nodes / 2 seconds cap | 32 | 12,808,463 | 13.268 | 18 |

The engine phase took 73.8 seconds wall time. Base non-mate depths span 11-19;
the overall maximum depth of 245 belongs to a mate result. Deeper depths span
16-21. Final bound-only interrupted iterations occurred in 172/512 and 11/32
searches; the tested reader retained the last completed non-bound score. The
deeper subset has median absolute cp change 8.5, maximum 157, and one +/-300 cp
marker change: position `-2793847243184516886` changes from -293 to -313 cp.
These targeted checks do not establish general engine accuracy.

Before work, system free commit was about 185.3 GiB. The existing Chess.com crawl
remained active and untouched. Pilot generation took 6.3 seconds, the exact-graph
control 6.1 seconds, final generation 23.3 seconds and final verification 28.1 seconds.
The final sample contains 79,716 arrivals: 6,122 horizon stops, 19 human endings
and three terminal stops across 6,144 arm trajectories, with no repetition or
frontier stop. Coupled arms can share those terminal paths; these are not counts
of independent human games. Position, time and engine caps were respected.

Validation passed:

- All 61 research tests, including tiny independent path-oracle comparisons,
  recall failure/re-entry, mass conservation, repeated/shared positions,
  horizon/ending priority, legal replay, missing-score and paired-bound checks.
- 24,576 control trajectories on the unchanged M4 expanded graph/overlay; all
  120 metric comparisons fall within the predeclared 6-SE-plus-2/N tolerance.
- Complete replay of all 7,680 pilot/final trajectories, including selected
  moves, stop reasons, endpoint vectors and visited-position hashes.
- 368,640 independent per-path metric checks, reconstruction of all 18 summaries
  and six paired contrasts, frozen target selection and source/config/code hashes.

Commands and artifact inventory are in the [research module README](../../research/human_repertoire/README.md).
Tracked [provenance](milestone5_provenance.json), [selection](milestone5_engine_selection.json),
[results](milestone5_results.json) and [verification](milestone5_verification.json)
preserve the compact audit. Full traces, FENs, engine responses and control results
remain under the ignored `_artifacts/m5/` directory. No canonical input or
production implementation was changed.

## Next priority

Freeze a small paired path panel by seed and index, then audit every
first-hit-relevant position in that panel with the same engine setup. Bound the
worst-case position count before sampling; retain intervals if the budget cannot
complete a path, rather than dropping difficult paths. Identical coupled paths
can cancel in the contrast. This tests the cost and stability of obtaining
complete observations when rare positions do not recur in a fresh pilot sample.

That panel should initially be an engine-coverage and variance experiment. Its
small sample will still have substantial Monte Carlo error, and a fresh final
comparison would be required after any tuning. Preserve the full horizon and
fixed plans, show budget sensitivity, and keep the population and prior
extrapolation limitations explicit. Broader book optimisation, personal recall
fitting and claims of human benefit remain later steps.
