# Milestone 6: complete engine coverage of a paired path panel

Completed 2026-09-07 America/Los_Angeles (2026-09-08 UTC). The protocol and
implementation were committed as `e3574ff` before panel generation. These are
**model predictions and computational diagnostics**, not observed benefits or
harms from studying a repertoire.

Complete engine coverage is practical for a small panel. All 128 trajectories
(64 coupled pairs) now have evaluations at every visited position, and the
fixed 16-pair subset also has complete coverage at a deeper budget. Missing-score
and computational-frontier uncertainty are both zero. New engine work took
about 130 seconds with one thread.

The book decision remains unresolved. The two-card-minus-one-card discounted
early-advantage estimate is **-4.18 percentage points**, with **3.70 pp Monte
Carlo standard error**. The predeclared conservative 95% outer envelope is
**[-38.14, +29.77] pp**. Deeper searches change one position's +/-300 cp category
but none of the subset's first-hit outcomes or times. This panel does not justify
recommending either plan over the other.

## Fixed design

The [protocol](milestone6_plan.md) and [configuration](milestone6_config.json)
retain the population with both players rated >=1800 in Blitz, Rapid and
Classical, rating gap <=200. The shared move prior remains fitted to January
and selected in February in milestone 3, pooled across those time controls.
No new human games, fitting, optimisation or personal recall measurements enter.

Start after `1.e4 e5 2.Nf3 Nc6`, from White's perspective. The original-source
one-card projection prescribes `3.d4`; the two-card plan adds `3...exd4 4.Nxd4`.
Study costs are one and two distinct positions. Independent recall .85 with
re-entry, the twelve-additional-ply horizon, competing +/-300 cp markers, .99
discount, third-occurrence rule, human-ending hazard and empirical/prior W/D/L
remain unchanged. Early advantage is preferred subject to the documented
soundness and performance constraints; no new weighted utility is introduced.

All 64 indices were drawn from a fresh fixed seed, coupled across the plans.
The first 16 indices were specified in advance for deeper searches. Every path
was retained. Both target lists were frozen before new engine searches. Every
visited nonterminal position was audited, including positions after a known
first hit and positions on identical paired paths. This supports complete
per-arm results as well as the contrast.

Matching cached Stockfish records were reused by budget. The base map contains
only 100,000-node/1-second results; the deeper subset only 400,000-node/2-second
results, plus exact rule terminals when applicable. Neither canonical source
scores nor deeper replacements enter the homogeneous base map. Existing source
scores are used only in labelled diagnostic comparisons.

## Complete base-budget results

| Quantity | One card | Two cards |
|---|---:|---:|
| First advantage | 29/64 = 45.31% | 26/64 = 40.63% |
| First disadvantage | 15/64 = 23.44% | 12/64 = 18.75% |
| Neither threshold reached | 20/64 = 31.25% | 26/64 = 40.63% |
| Discounted first advantage | 41.25% | 37.06% |
| Expected points from endpoint model | .50204 | .50028 |
| Missing evaluations / unresolved first hits | 0% / 0% | 0% / 0% |
| First-advantage 95% MC outer envelope | [28.34%, 62.29%] | [23.65%, 57.60%] |

Both prepared moves pass their soundness gates at both budgets:

| Move | Base parent / child cp | Deeper parent / child cp |
|---|---:|---:|
| 3.d4 | 39 / 22 | 47 / 17 |
| 4.Nxd4 after 3...exd4 | 42 / 37 | 14 / 14 |

The raw points and disadvantage means satisfy the configured .48 points floor
and .40 disadvantage ceiling, but this does not certify either real-world
performance constraint. Outcome predictions remain weakly validated, and the
small panel has substantial simulation uncertainty. Passing the move gates
alone also does not establish human benefit.

48 of 64 pairs have identical position sequences. Their first-hit contrast is
exactly zero. For the discounted contrast, coupling reduces the standard error
from an independence-assumed **8.03 pp** to **3.70 pp**. The expected-points
difference is -.001763 with paired MC SE .001134. All endpoint observation
intervals collapse to points under complete coverage; confidence about the
model's population mean does not.

The paired Hoeffding margin is 33.95 pp at N=64. It is deliberately conservative
and remains separate from the descriptive standard error. It conditions on
the human-policy assumptions and fixed engine-budget mapping. It does not
include uncertainty in engine accuracy, training, personal recall or human
generalisation. The uncertainty statement is per comparison, not simultaneous
coverage across every reported result.

## Coverage and budget sensitivity

On these same paths, the inherited mixed overlay leaves 65.63% of one-card and
64.06% of two-card first hits unresolved. Its observed discounted marker contrast
is +1.44 pp, with observation bounds [-17.31, +18.63] pp. Completing coverage
and standardising the engine budget gives the -4.18 pp estimate above. This
changes both evidence coverage and score provenance; it cannot be attributed
entirely to filling missing scores. The earlier bounds contain the complete
estimate, and neither evidence treatment yields a reliable ranking.

The predeclared budget curve begins with the inherited matching base-budget
cache only, then adds new positions in frozen index/plan/ply order. Canonical
scores and deeper records are excluded throughout this curve.

| New base evaluations | Unresolved first hits, one / two | Discounted two-minus-one observation interval (pp) |
|---:|---:|---:|
| 0 | 100% / 100% | [-25.00, +25.00] |
| 64 | 90.63% / 90.63% | [-21.75, +18.87] |
| 128 | 75.00% / 75.00% | [-18.63, +15.75] |
| 256 | 50.00% / 50.00% | [-12.35, +9.52] |
| 512 | 3.13% / 3.13% | [-4.18, -4.18] |
| 522 | 0% / 0% | [-4.18, -4.18] |

The contrast is already identified at 512 new positions because the remaining
unknown first hits are on paired paths that cancel. The final ten evaluations
complete the per-arm observations. This is a descriptive curve, not an adaptive
stopping experiment or evidence that the identified contrast is statistically
different from zero.

The fixed 16-pair subset has identical base/deeper first-hit metrics: one card
reaches advantage in 3/16 paths and two cards in 2/16, with discounted contrast
-5.77 pp and MC SE 5.77 pp. Its much wider 95% envelope is [-73.67, +62.14] pp.
It is a nested sensitivity check, not an independent replication.

Across 172 fully evaluated subset/gate positions, median absolute cp change is
7 and the maximum is 60. Position `9145010596282057386` changes from +267 to
+303 cp. No subset path changes its first competing event or event ply, so
the threshold change does not alter this subset's reported contrast. These
targeted searches do not establish general engine accuracy.

## Resources and verification

The same installed Stockfish 18 binary, one thread, 32 MiB hash, no tablebases,
cleared hash and fresh game state were used. No new request reached its time
limit; node limits bounded these runs. The tested reader retains the last
completed non-bound score when the final iteration is interrupted.

| Budget | Required positions | Reused | New | New nodes | Engine-reported seconds | Median completed depth |
|---|---:|---:|---:|---:|---:|---:|
| 100,000 nodes / 1 second | 603 | 81 | 522 | 52,231,374 | 57.464 | 15 |
| 400,000 nodes / 2 seconds | 172 | 21 | 151 | 60,436,966 | 64.730 | 18 |

Base depths span 10-26 and deeper depths 15-21. Final interrupted bound flags
occurred in 247/522 new base and 79/151 new deeper searches; those flags refer
to the discarded final iteration, not the retained score. The engine phase
took 129.7 seconds wall time. Panel preparation took .9 seconds and verification
1.5 seconds. The source lookup was sparse and read-only.

The initial resource check found about 184.8 GiB free commit. The existing
Chess.com crawl remained separate and untouched. Every output is confined to
this research worktree. Base/deeper caps were 1,668/420 positions; the panel
used 603/172. All 128 trajectories reach the twelve-ply horizon, yielding 1,664
arrivals; none was truncated, dropped or stopped by a resource cap.

Validation passed all **69 research tests**, including new budget-cache,
target-completeness, shared-position, terminal, gate and missing-score checks.
The independent verifier replays every selected move and endpoint, checks all
engine principal variations for legality and all record limits/provenance,
and reconstructs 11 comparisons: three full-panel evidence maps, two complete
subset maps and six budget checkpoints. All frozen hashes and source metadata
match, all prepared gates pass, and every complete-map unknown-eval metric is
zero.

Reproduction commands and artifact inventory are in the
[research README](../../research/human_repertoire/README.md). Compact tracked
[provenance](milestone6_provenance.json), [selection](milestone6_engine_selection.json),
[results](milestone6_results.json) and [verification](milestone6_verification.json)
preserve the audit. Full traces, FENs and engine responses are in the ignored
`research/human_repertoire/_artifacts/m6/` directory.

## Next priority

Coverage cost is manageable; Monte Carlo precision and human-policy credibility
remain limiting. One-quarter of one-card decisions (192/768) and 23.96% of
two-card decisions (184/768) occur at positions without January move observations.
The high simulated first-hit rates therefore depend materially on the fallback
prior outside observed training positions. Lower held-out move log loss alone
does not establish that the induced tactical-error rates resemble human play.

The next bounded priority should be an engine-loss calibration diagnostic:
preselect held-out human decisions across Blitz/Rapid/Classical and training
support levels, compare each actual move with a draw from the frozen fallback
model at the same board, and audit the parents and both children under one
budget. Keep the sample, paired draws and engine cap fixed before scores.
Reusing March data would be a retrospective diagnostic, not a new confirmatory
test; no policy should be retuned and then re-evaluated on that same sample.

That check should precede spending substantially more engine work to rank books.
A later fresh, more precise paired comparison can use the demonstrated
cancellation of shared paths to reduce evaluation cost. Neither this small
panel nor its nested depth check supports a repertoire change or a claim of
human learning benefit.
