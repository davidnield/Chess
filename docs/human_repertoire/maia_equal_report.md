# Equal-cost follow-up packages with Maia-1400

Three two-card packages have now been compared on 384 fresh paths. **None has
an established early-advantage advantage over the others.** All pass the fixed
soundness gates, but the rarer cards receive almost no exposure. This is a
completed bounded allocation experiment, not a recommendation to change the
user's repertoire. All outputs are model predictions conditional on the frozen
fallback, opponent and recall assumptions.

Every package prescribes `3.d4` after `1.e4 e5 2.Nf3 Nc6`. The second card is
either `4.Nxd4` after `3...exd4` (reference), `4.Nc3` after `3...Nf6`, or
`4.Nc3` after `3...d6`. Cost is exactly two distinct positions, including the
root dependency. All later moves use fallback. These illustrative candidates
were fixed before outcomes, not selected as optimal moves by an engine.

## Results

The [plan](maia_equal_plan.md), [config](maia_equal_config.json), runner and
package/coverage tests were frozen in commit `f9966c1` before sampling. Recall
is 85%; Maia self rating remains 1400, and its opponent-conditioning input is
the declared 2000 scenario. The separate shared-position opponent policy is
unchanged. Classical uses the identical Rapid transfer model. Horizon, first
+300 cp before -300 cp objective, discount 0.99, ending/repetition semantics
and soundness constraints match the prior comparison.

Each of six arms has 64 fresh paired indices, with common random uniforms and
a new seed. The first eight indices form a fixed deeper subset. Four primary
contrasts use Bonferroni-adjusted Hoeffding bounds (per-contrast alpha 0.0125)
for family-wise 95% sampling coverage. These bounds do not quantify model error.

| Fallback | Second card | Early utility | Delta vs Nxd4 | Paired MC SE | Adjusted 95% bounds |
|---|---|---:|---:|---:|---:|
| Blitz | Nxd4 after ...exd4 | 62.589% | Reference | — | — |
| Blitz | Nc3 after ...Nf6 | 62.619% | +0.029 pp | 4.051 pp | [-39.795, +39.854] pp |
| Blitz | Nc3 after ...d6 | 62.619% | +0.029 pp | 4.542 pp | [-39.795, +39.854] pp |
| Rapid | Nxd4 after ...exd4 | 58.205% | Reference | — | — |
| Rapid | Nc3 after ...Nf6 | 61.032% | +2.827 pp | 1.983 pp | [-36.998, +42.651] pp |
| Rapid | Nc3 after ...d6 | 61.032% | +2.827 pp | 2.856 pp | [-36.998, +42.651] pp |

The two alternatives have identical mean utilities in this finite sample;
that does not establish equivalence. First-disadvantage deltas are +1.563 pp
(SE 1.563) and +3.125 pp (SE 2.192) respectively in Blitz, both zero observed
in Rapid. Expected-points proxy deltas are -0.818 pp in Blitz and zero in
Rapid. Horizon W/D/L is uncalibrated and cannot certify overall performance.

## Why the outcome comparison is limited

| Second card | Opponent branch probability after d4 | Blitz fallback plays card | Rapid fallback plays card | Card encounters, Blitz / Rapid |
|---|---:|---:|---:|---:|
| Nxd4 after ...exd4 | 89.97% | 53.01% | 91.06% | 53/64 / 52/64 |
| Nc3 after ...Nf6 | 2.03% | 11.52% | 11.36% | 0/64 / 0/64 |
| Nc3 after ...d6 | 4.29% | 2.55% | 2.77% | 2/64 / 2/64 |

These are branch probabilities conditional on d4, not unconditional encounter
rates. With 85% recall, prescribing a move changes its local probability to
0.85 + 0.15q. That change alone is not benefit: a low-probability prescribed
move may be worse than the natural alternative, and the position may be rare.

The ...Nf6 card is never encountered. Its observed contrast therefore comes
from *omitting Nxd4*, not from studying Nc3. The ...d6 card is encountered only
twice. Rapid alternatives retain 62/64 and 60/64 identical position sequences
with the reference; Blitz retains 46/64 and 44/64. No path revisits its extra
card. Zero encounters or zero observed variance must not be treated as zero
population value, equivalence or safety.

The absolute utilities are not predicted human success rates validated on
games. The shared-position opponent prior has documented sparse-position
weaknesses, and 1400 is a chosen intuitive-fallback proxy, not measured recall
or personal playing strength. There is no new held-out human validation here.

## Soundness and engine sensitivity

All four distinct prescribed moves pass both budgets' child >= -100 cp and
maximum 150 cp loss gates. White-perspective parent -> child evaluations:

| Move | Base cp | Deeper cp |
|---|---:|---:|
| d4 | +39 -> +22 | +47 -> +17 |
| Nxd4 after ...exd4 | +42 -> +37 | +14 -> +14 |
| Nc3 after ...Nf6 | +103 -> +4 | +125 -> +17 |
| Nc3 after ...d6 | +80 -> +65 | +90 -> +66 |

Passing a gate does not make a move best. In particular, Nc3 after ...Nf6 gives
up about a pawn of engine evaluation while remaining within the declared
150 cp allowance. Its low Maia probability is not a reason to spend a card on
it. Do not tighten the gate retrospectively to manufacture a preferred answer.

On the same first eight paired indices, Blitz's alternative-minus-reference
early utility changes from -0.230 pp at base to -0.454 pp deeper for each
alternative: one alternative path changes first-hit timing in each arm. Rapid
contrasts remain zero on that subset. This is a matched engine-budget check,
not a comparison of all 64 base pairs against eight different deeper pairs.

## Completion and reproducibility

All 102 research tests pass. Verification replays 384 paths and 4,607 legal
moves from frozen policy tables, validates equal cost/dependencies, legal SAN
and UCI support, normalized probabilities, FEN/hash consistency, source reuse,
all engine limits/PVs and complete coverage. It independently reconstructs
2,160 scalar outcome checks across base/deeper rows, paired means, standard
errors, adjusted bounds and soundness eligibility. All checks pass.

Coverage: 930 base positions (161 reused, 769 new) and 156 deeper (24 reused,
132 new). Sampling took 20.5 seconds; engine wall time was 165.9 seconds.
New calls totalled 94.1 base and 60.4 deeper seconds. Limits remained 100k
nodes/1 second and 400k/2 seconds, one thread, Hash 32, no Syzygy, with the
900-second engine guard. No adaptive expansion, dropped paths, new human
extraction, fitting, canonical writes or original-checkout changes occurred.
Resource headroom was checked before the job; the existing crawl was untouched.

See [results](maia_equal_results.json), [verification](maia_equal_verification.json),
[provenance](maia_equal_provenance.json), [runner](../../research/human_repertoire/maia_equal.py),
[verifier](../../research/human_repertoire/verify_maia_equal.py) and
[commands](../../research/human_repertoire/README.md). Full local artifacts are
under ignored `_artifacts/maia_equal/`.

## Decisions and research boundary

Maia-2 integration and two concrete study-package experiments are complete.
Keep self rating 1400, separate opponent modeling, full legal probabilities
and the existing soundness/recall evaluator. Keep custom M10 fitting paused.
Do not recommend a book change from unresolved contrasts or treat infrequent
cards as useless. These experiments demonstrate working integration and how
fallback and encounter frequency affect study allocation; they do not validate
a personalised repertoire.

Before broader optimisation, the next methodological need is to evaluate rare
branches deliberately and weight them by reach probability, with transposition
and re-entry accounting, rather than merely increasing unstratified path counts.
Candidate prescriptions should preserve meaningful engine advantage, not just
pass the permissive soundness floor. Opponent-policy sensitivity also remains
necessary for credible performance claims. These are explicit limits on what
the completed integration establishes, not evidence for resuming custom model
development or changing production outputs.
