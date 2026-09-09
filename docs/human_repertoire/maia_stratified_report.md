# Maia-1400 study allocation with deliberate rare-branch coverage

The branch-stratified comparison is complete and verified. It now gives every
candidate branch direct evidence instead of relying on accidental encounters.
**No overall book ranking is established by the predeclared conservative
bounds.** The useful local finding is that a move being unlikely under Maia
does not make it worth memorising: Nc3 after ...Nf6 sacrifices about a pawn of
engine evaluation and has worse conditional early utility in this sample.

This finishes the Maia integration and the follow-through from marginal card,
to equal-cost packages, to deliberate rare-branch coverage. These are model
predictions, not a validated personalised repertoire. Keep the user's 1400
fallback preference and do not recommend changing their book on these results.

## What changed

The [equal-budget experiment](maia_equal_report.md) encountered ...Nf6 zero
times and ...d6 twice per arm. This experiment keeps exactly its three two-card
packages, opponent policy, 85% recall, twelve-ply horizon, first +/-300 cp
utility, 0.99 discount, termination/repetition rules and soundness gates.
Only the sampling machinery and fresh seed change. No candidate is replaced
after viewing outcomes.

All books prescribe `3.d4` after `1.e4 e5 2.Nf3 Nc6`. The second card covers
either `4.Nxd4` after `3...exd4` (reference), `4.Nc3` after `3...Nf6`, or
`4.Nc3` after `3...d6`. Later moves use fallback, including after deviations
and re-entry. These are illustrative fixed packages, not an optimised set.

The complete first-two-ply distribution contains 836 prefixes per Maia policy.
It integrates root recall and includes all other first moves and any early
endings. Four strata each receive 32 paired continuations per package: 768
paths total. Each weighted estimate uses the stratum's model probability:

| Prefix stratum | Blitz weight | Rapid weight |
|---|---:|---:|
| d4 / exd4 | 79.352% | 77.437% |
| d4 / Nf6 | 1.789% | 1.746% |
| d4 / d6 | 3.786% | 3.694% |
| All remaining prefixes | 15.073% | 17.122% |

Weights sum to one separately for each fallback. They are unconditional from
the starting position, unlike the prior report's Black-reply probabilities
conditional on d4. The original ply count, prefix evaluations and visit history
remain in each path, preserving first-hit timing and repetition across the
prefix boundary. See the [method and formulas](stratified_sampling.md).

Maia self rating is still 1400; Maia's opponent-conditioning input is the
declared 2000 scenario. Opponent moves still use the separate shared-position
prior. Classical remains the identical Rapid transfer scenario, not another
independent measurement. No policy fitting or fresh human validation occurred.

## Weighted model predictions

Delta is alternative two-card package minus the Nxd4 two-card package.
Early utility is discounted first +300 cp before -300 cp. MC SE is the weighted
paired simulation standard error. The four primary contrasts use Bonferroni
alpha .0125 and conservative weighted Hoeffding bounds for family-wise 95%
sampling coverage conditional on the model.

| Fallback | Second card | Early utility | Delta | MC SE | Adjusted 95% bounds |
|---|---|---:|---:|---:|---:|
| Blitz | Nxd4 after ...exd4 | 60.725% | Reference | — | — |
| Blitz | Nc3 after ...Nf6 | 55.951% | -4.774 pp | 3.161 pp | [-50.326, +40.777] pp |
| Blitz | Nc3 after ...d6 | 56.397% | -4.328 pp | 3.172 pp | [-49.880, +41.223] pp |
| Rapid | Nxd4 after ...exd4 | 58.750% | Reference | — | — |
| Rapid | Nc3 after ...Nf6 | 58.462% | -0.289 pp | 0.164 pp | [-45.014, +44.437] pp |
| Rapid | Nc3 after ...d6 | 58.842% | +0.092 pp | 0.320 pp | [-44.633, +44.818] pp |

Reported Rapid SEs are much smaller than in the previous fresh sample, and
every rare branch is now observed. This is not a general proof of greater
efficiency: allocation, sample count and seed changed, and the worst-case
Hoeffding bounds remain wide. Do not substitute a narrow normal approximation
after seeing the results or claim significance from the point estimates.

First-disadvantage deltas are +2.536 pp and +2.361 pp for the Blitz alternatives
(SE 2.480 and 2.483 pp); Rapid deltas are -0.055 pp (SE 0.096) and zero observed.
Expected-points proxy deltas are +1.300 pp / +1.299 pp in Blitz and -0.026 pp /
-0.058 pp in Rapid. That uncalibrated W/D/L proxy does not establish the
overall-performance constraint. Zero observed disadvantage is not a guarantee.

## Where the effects come from

Conditional on d4/exd4, both alternative packages omit the Nxd4 card. Their
early-utility deltas are -5.597 pp (SE 3.980) in Blitz and +0.114 pp (SE 0.114)
in Rapid. These estimates fit the behavioral distinction already established:
Maia Rapid chooses Nxd4 about 91% of the time without a card, versus 53% in
Blitz. The finite-sample sign in Rapid is not evidence that remembering Nxd4
is harmful.

Conditional on d4/Nf6, prescribing Nc3 instead of fallback yields -18.582 pp
(SE 6.376) in Blitz and -21.576 pp (SE 7.908) in Rapid. After weighting by
the rare branch, those terms contribute only about -0.333 pp and -0.377 pp
to full-start utility. This is direct branch evidence; the previous zero-visit
sample could not measure it. It is a diagnostic conditional comparison, not
a separately predeclared significant result.

Conditional on d4/d6, Nc3 gives +2.993 pp (SE 7.790) in Blitz and +0.114 pp
(SE 8.342) in Rapid. Weighted contributions are about +0.113 pp and +0.004 pp.
Those estimates do not establish a useful card. The remainder stratum has no
observed paired utility difference; it was sampled and included, not discarded.

## Soundness, sensitivity and correctness

All packages pass the same fixed gates at both engine budgets. As previously
recorded, Nc3 after ...Nf6 drops from +103 to +4 cp at base and +125 to +17 cp
deeper. Passing the maximum 150 cp loss gate does not mean it preserves the
available advantage. Low fallback probability must not be the selection rule
for future cards. Other move gates are unchanged from the equal-budget report.

On the same first four replicates per stratum, deeper scoring leaves the
Nc3-after-Nf6 weighted contrast unchanged: Blitz -0.494 pp and Rapid -0.814 pp.
The Nc3-after-d6 contrast changes from -0.516 to +0.323 pp in Blitz and from
-0.0166 to +0.00034 pp in Rapid. Small subset signs are engine-sensitive;
these values are not comparisons of full and subset samples.

All 107 research tests pass, including independent tiny-book oracle checks of
complete prefix enumeration and sampled suffixes with position-specific recall.
Final verification reproduces all 768 traces and 9,207 legal moves, independently
checks 1,672 prefix probabilities, and reconstructs 4,320 scalar first-hit
checks across base/deeper rows. It verifies weighted means, SEs, Hoeffding
margins, source hashes, FENs, legal engine PVs, limits, gate eligibility and
complete target coverage. All checks pass.

The protocol and sampler were frozen in `939a37b`. Initial preparation and
its bounded reproduction stopped on a pinned-en-passant hash mismatch before
any new engine score. Commit `3ca7130` added the tested research-only
`HashedBoard` serialization adapter; no seed, candidate, probability rule or
limit changed. The successful rerun includes the failing case. Earlier frozen
modules and experiments remain unchanged. See the method note for the precise
Polyglot/X-FEN reason; this was not a production-pipeline redesign.

Sampling took 66.1 seconds. Base coverage comprises 2,263 scored positions
(218 reused, 2,045 new) and four exact terminal positions. Deeper coverage
comprises 349 positions (39 reused, 310 new). New engine calls totalled 221.9
base and 133.8 deeper seconds; wall time was 386.8 seconds, below the fixed
900-second guard. Limits stayed 100k nodes/1 second and 400k/2 seconds, one
thread, Hash 32, no Syzygy. No cases were dropped or adaptively added. Resource
headroom was checked; the original checkout, artifacts and running crawl were
left untouched.

## Evidence and decisions

[Results](maia_stratified_results.json), [verification](maia_stratified_verification.json),
[provenance](maia_stratified_provenance.json), [plan](maia_stratified_plan.md),
[runner](../../research/human_repertoire/maia_stratified.py),
[verifier](../../research/human_repertoire/verify_maia_stratified.py) and
[commands](../../research/human_repertoire/README.md). Full artifacts remain
under ignored `_artifacts/maia_stratified/`.

Decisions made autonomously: preserve Maia-1400 and pause custom M10 fitting;
compare fixed small packages before broader optimisation; retain the opponent
policy to isolate the fallback change; correct rare-branch exposure by exact
weighting; preserve the existing hash with a small serialization adapter; and
make no repertoire recommendation from unresolved model predictions.

The working integration now supports a concrete study-allocation evaluator
with explicit fallback, recall, cost, soundness and rare-branch accounting.
Credible personalised performance claims still require opponent-policy
sensitivity and human calibration. The current opponent prior's documented
sparse-position weaknesses can inflate absolute advantage estimates. Maia-1400
remains a user-chosen ease proxy, not measured memory failure or a personal
rating estimate. Those limits should constrain claims made from this work.
