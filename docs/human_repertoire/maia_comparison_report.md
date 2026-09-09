# Maia-1400: is the Nxd4 follow-up worth a study card?

The first Maia-backed repertoire comparison is complete. It does **not establish
an early-advantage benefit from adding the Nxd4 card**. It does identify a useful
study-cost distinction: Maia-1400 already plays Nxd4 with 91.1% probability in
Rapid, compared with 53.0% in Blitz. A move usually found without preparation
has less scope for a memorised card to change play. This is a model prediction
under the user's chosen fallback preference, not measured human ease.

Keep Maia-2 at 1400 and the custom M10 predictor paused. Do not recommend adding
or removing this card based on these noisy outcome estimates. The next priority
is a small, fixed comparison of alternative follow-up cards at the same study
budget, including necessary continuations. The user has authorised continuing
through next priorities autonomously, making routine decisions with judgment
and reporting those decisions at the end.

## Frozen question and assumptions

- Start after `1.e4 e5 2.Nf3 Nc6`, playing White. One card prescribes `3.d4`;
  two cards additionally prescribe `4.Nxd4` after `3...exd4`. Costs are one and
  two distinct positions. This is a marginal-card question, not global optimisation.
- Compare the unchanged shared-position fallback reference with Maia-2 Blitz
  and Rapid at self rating 1400. Classical uses Rapid: it is the same model
  scenario, not an independent Classical measurement.
- Opponent moves always use the shared-position prior. Maia's opponent-rating
  input is a fixed **2000 scenario**, separate from opponent behavior. These
  are not paths sampled from an actual 2000-rated population. The empirical
  cohort remains both players >=1800, gap <=200, across Blitz/Rapid/Classical.
- Recall is independently 50%, 85%, or 100% per learned encounter, with book
  re-entry. It is an assumption, not measured retention. Fallback preserves
  all legal probability mass, with no engine filter or argmax.
- Preserve the common twelve-ply evaluator, third-visit repetition draw,
  automatic chess terminals, empirical ending hazards and terminal W/D/L.
  Primary utility is discounted first +300 cp before -300 cp, discount 0.99
  per ply. First disadvantage and study cost remain separate constraints.
  Horizon W/D/L remains an uncalibrated empirical proxy.
- 32 paired paths per context share random uniforms across books and contexts:
  3 policies x 3 recalls x 2 books x 32 = 576 paths. The first four indices in
  each context form the fixed deeper-engine subset.

The [plan](maia_comparison_plan.md) and [config](maia_comparison_config.json)
were committed in `938bd93` before scoring. Commit `0959c1b` corrected the
preparation manifest's engine metadata lookup before any new engine evaluation.
The first preparation attempt failed at that lookup; deterministic preparation
was rerun without changing policies, settings, seed, cases or outcome criteria.

## Results: model predictions

Values are percentage points of discounted first-advantage utility, except
recall. Delta is two cards minus one. MC SE is paired simulation standard error.
The conservative 95% Hoeffding bounds apply separately to each contrast, not
simultaneously across nine comparisons. They measure sampling error conditional
on the model, not model validity.

| Fallback | Recall | One card | Two cards | Delta | MC SE | 95% bounds |
|---|---:|---:|---:|---:|---:|---:|
| Shared reference | 50% | 42.63 | 39.75 | -2.88 | 2.88 | [-50.90, +45.13] |
| Shared reference | 85% | 36.97 | 31.38 | -5.60 | 7.01 | [-53.61, +42.42] |
| Shared reference | 100% | 40.15 | 37.44 | -2.71 | 7.65 | [-50.73, +45.30] |
| Maia Blitz | 50% | 63.17 | 63.05 | -0.11 | 4.19 | [-48.13, +47.90] |
| Maia Blitz | 85% | 56.99 | 54.11 | -2.88 | 6.53 | [-50.90, +45.13] |
| Maia Blitz | 100% | 60.34 | 60.46 | +0.12 | 7.20 | [-47.90, +48.13] |
| Maia Rapid | 50% | 62.93 | 62.93 | 0.00 | 0.00 | [-48.02, +48.02] |
| Maia Rapid | 85% | 56.93 | 59.82 | +2.88 | 2.88 | [-45.13, +50.90] |
| Maia Rapid | 100% | 54.58 | 57.46 | +2.88 | 2.88 | [-45.13, +50.90] |

No contrast resolves the card's value. Higher absolute Maia utilities reflect
a changed fallback assumption, not improved optimisation or validated human
advantage rates. The opponent prior's known sparse-position weaknesses can
inflate these numbers. Rapid's small differences rest on very few changed
paths: 32, 31 and 30 of 32 paired position sequences are identical at 50%, 85%
and 100% recall. Zero observed variance is not proof of zero true effect.

First-disadvantage deltas at recalls 50% / 85% / 100% are respectively:
shared reference 0 / +3.125 / +3.125 pp; Maia Blitz 0 / +3.125 / 0 pp;
Maia Rapid 0 / 0 / 0 pp. At 85%, paired MC SE is 5.47 pp for the reference,
3.125 pp for Blitz and zero observed for Rapid. Absence in a small panel is
not a safety guarantee. Expected-points deltas at 85% are reference +0.149 pp,
Blitz -0.216 pp, Rapid +0.074 pp, but that uncalibrated proxy cannot establish
the overall-performance constraint.

## What the card changes locally

At the extra position, let q be fallback probability of Nxd4. With recall r,
the card raises it to r + (1-r)q. The local action-distribution change is
r(1-q), conditional on reaching the position.

| Fallback | Nxd4 without card | With card, 85% recall | Local change | Bc4 without card |
|---|---:|---:|---:|---:|
| Shared reference | 55.72% | 93.36% | 37.64 pp | 33.45% |
| Maia Blitz | 53.01% | 92.95% | 39.94 pp | 39.08% |
| Maia Rapid | 91.06% | 98.66% | 7.60 pp | 4.10% |

Bc4 is the largest alternative in each policy. The card often chooses between
opening continuations; it cannot be interpreted as merely preventing a blunder.
No sampled path revisits this position, but that does not prove no possible
revisit. The local change is not asserted as a global bound on path utility.
No probabilities were adjusted in response to these findings.

## Engine checks and verification

Both book moves pass the -100 cp child floor and maximum 150 cp loss gates at
both budgets. White-perspective parent -> child evaluations: d4 +39 -> +22 cp
base and +47 -> +17 deeper; Nxd4 +42 -> +37 base and +14 -> +14 deeper. These
are prescribed-move gates, not guarantees about later fallback play.

Base coverage includes all 1,205 required positions: 125 reused and 1,080 new.
The deeper subset covers all 158: 22 reused and 136 new. Limits were 100,000
nodes/1 second and 400,000 nodes/2 seconds respectively, one thread, Hash 32,
no Syzygy. Sampling took 15.4 seconds and the engine phase about 182 seconds,
within the 900-second guard. New calls totalled 113.9 base and 53.8 deeper
seconds. No positions or paths were dropped or outcome-dependent expansion used.

Deeper scoring changes none of the nine paired early-advantage contrasts on
the fixed four-pair subset. That small subset does not establish general
engine stability or replace the full 32-pair estimates.

All 100 research tests passed before the experiment. Independent verification
replays 576 paths and 6,912 legal moves from frozen policy tables, checks full
coverage, hashes, FENs, legal engine PVs and limits, and reconstructs 2,880
scalar first-hit checks, paired means, standard errors, uncertainty margins
and soundness gates. Deeper summaries reproduce with the common evaluator.
All checks pass.

The separate Maia environment additionally needs `pyarrow==24.0.0` for existing
data adapters. Inference versions/checkpoints remain as in the integration
report. Resource headroom and the existing crawl were checked before the job;
the original environment, checkout, canonical artifacts and crawl were untouched.

## Evidence

- [Compact results](maia_comparison_results.json): all contexts, both arms,
  uncertainty, downside, local probabilities and deeper contrasts.
- [Verification](maia_comparison_verification.json) and
  [frozen provenance](maia_comparison_provenance.json): code/input/artifact hashes.
- [Runner](../../research/human_repertoire/maia_comparison.py),
  [verifier](../../research/human_repertoire/verify_maia_comparison.py) and
  [commands](../../research/human_repertoire/README.md).

Full traces, distributions, FENs and engine records remain in ignored
`_artifacts/maia_compare/`. No fresh human sample, fitting, canonical writes
or production changes were part of this completed bounded experiment.
