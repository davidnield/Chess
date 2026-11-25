# Locked contender rationale — November 25, 2026

Written after the completed selection reports were verified, and **before any
sealed final-confirmation scores were read**. This is a selection for an independent
test, not approval to change production evaluations or books.

## Decision

Select frozen recipe **`score_3_3_4`**: convert each source's White-perspective
centipawns to the existing expected-score scale, then take 30% Cloud, 30% NNUE
Fishnet and 40% ChessDB on the primary complete-case population. The locked
controls are the existing November 23 `proposal` and **ChessDB alone**
(`cp_0_0_10`), the best individual source on the pooled primary cohort.

The candidate already belonged to the six predeclared November 25 finalists.
Do not change its weights, source eligibility, conversion, gates or comparator
after opening the final panel. Only this candidate and its two controls may be
reported there. No new 135-recipe leaderboard is authorized on final labels.

## Evidence reviewed

The targeted diagnostic completed at 10:54 PST (939/942 qualified searches).
The fresh study completed at 12:13 PST (999/1,000 qualified parents), and pooling
completed at 12:14 PST. The pooled completion receipt and every bound input/output
checksum verified. Selection contains **2,499 distinct parents**, including
**1,124 three-source overlaps and 1,008 NNUE three-source overlaps**. The excluded
new reference was in `no_cloud_repeated`, so all 600 new NNUE overlaps remain.
451 qualified deeper parent rechecks replace older values; children add no rows.
1,049 historical references still have unknown actual depth (requested depth 24).

The separate 300-position sample has no board/mirror overlap with selection. Its
worker reports operational progress only, and neither `choice.json` nor
`OPENED.json` existed at this review. No sealed records were inspected.

All values below are **percentage points of absolute expected-score error**, not
repertoire win-rate gains. Primary means give equal weight to the four NNUE
source-quality strata; these are not repertoire-reach or population weights.

| Rule | Pooled MAE, N=1,008 | Old best references, N=408 | New only, N=600 | Measured depth >=26, N=738 | Pooled p95 / p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Selected 30/30/40, score space | 0.94053 | 0.93604 | 0.94721 | 0.97898 | 2.49991 / 3.88952 |
| Equal thirds, score space | 0.94687 | 0.93345 | 0.95869 | 0.99016 | 2.51653 / 3.94399 |
| Existing proposal | 0.97957 | 0.95221 | 1.00350 | 1.01734 | 2.68174 / 3.95918 |
| ChessDB alone | 1.18541 | 1.20163 | 1.16796 | 1.20119 | 3.18450 / 4.32557 |
| Cloud alone | 1.36186 | 1.43362 | 1.34718 | 1.45247 | 3.84172 / 4.89247 |
| Fishnet alone | 1.40023 | 1.26100 | 1.49456 | 1.52684 | 4.42705 / 8.68683 |

The selected rule ranks first pooled, seventh old-only, third new-only and second
on measured-depth-only evidence. It stays first when retaining original old
references in the pooled sensitivity. Equal panel weighting within each stratum
gives 0.94163 versus 0.97785 for the proposal. Actual-depth-28-only new comparisons
give 0.93883 versus 0.99992 for the proposal. The advantage is small but directionally
consistent across panels and reference choices. Pooled mean error is about 4.0%
lower than the proposal and 20.7% lower than ChessDB alone; this says nothing yet
about the magnitude of a book-level performance improvement.

On the new 600-overlap panel, the predeclared paired MAE difference versus proposal
is **-0.05629 points**, ordinary bootstrap 95% interval **[-0.10132, -0.01225]**.
The simultaneous interval across the original 15 comparisons is
**[-0.12285, +0.01325]**, so it does not establish a family-adjusted advantage.
Versus ChessDB the difference is **-0.22075**, simultaneous interval
**[-0.31970, -0.12605]**. Intervals are conditional on sampled positions/strata;
shared openings and approximate engine references remain limitations. Pooled
winner statistics are selection evidence, not an independent test.

The exact equal-thirds rule is close; no claim that the optimal weights are
precisely 30/30/40 is warranted. The new-only winner 40/20/40 has more gate errors
than 30/30/40 in every primary arm and poorer older-panel stability. The old
safety finalist 10/60/30 degrades in mean/tail error on new evidence (1.10794 new
MAE versus 1.00350 proposal), so it is not the final contender.

## Safety tradeoffs are unresolved

Clear false admissions exclude reference values within 0.2 expected-score points
of the absolute floor. Counts below are on the same 1,008 pooled primary positions,
evaluated prospectively for each color and arm; they are not game failure rates.

| Absolute-gate arm | Selected 30/30/40 | Proposal | ChessDB alone |
| --- | ---: | ---: | ---: |
| Permissive White | 5 | 9 | 4 |
| Permissive Black | 14 | 14 | 4 |
| Stricter White | **22** | **14** | 34 |
| Stricter Black | 17 | 18 | 19 |

The stricter-White regression occurs in both old (10 versus 8) and new (12 versus
6) comparisons. Its pooled unsafe shortfall rises from 12.4100 to 19.1121 summed
score points. Better average error does not erase this tradeoff. Conversely,
permissive-White shortfall falls from 11.9425 to 6.1499, permissive-Black from
17.5032 to 16.6184, and stricter-Black from 21.3416 to 17.5767. Report all arms in
confirmation; do not select an arm after seeing which result is favorable.

The older targeted diagnostic also supports caution: the proposal improves most
comparisons against step 1, but stricter-White clear admissions are 12 versus 10
on its 451 qualified parents. Reference changes alter 6/9/12/8 classifications
for permissive White/Black and stricter White/Black. The diagnostic is enriched
for disagreement and cannot supply population rates. It checks the existing
ChessDB menu, not the full blended-child relative-gate policy.

## Missing-source behavior is not approved for adoption

The frozen deployment sensitivity excludes legacy Fishnet, renormalizes available
weights, and falls back to ChessDB when selected sources are absent. While overall
macro error is lower for 30/30/40 (1.00702 versus proposal 1.06408), this hides a
clear no-Cloud weakness:

| Missing-Cloud stratum | N | 30/30/40 with renormalization | Proposal / ChessDB |
| --- | ---: | ---: | ---: |
| NNUE Fishnet once | 350 | 1.19746 | 0.96380 |
| NNUE Fishnet repeated | 349 | 1.12630 | 1.01966 |

The full fallback sensitivity also worsens p99 error (4.70747 versus 4.06989).
Therefore **do not deploy automatic renormalization as a global replacement** on
the strength of the complete-case result. The final 300 contain all three sources;
they can confirm only that complete-case comparison. Retain the proposal's
ChessDB-only no-Cloud handling unless a separately justified future policy is
frozen and validated. No conditional hybrid has been independently confirmed by
this experiment, and none is silently substituted for the locked recipe.

## What happens next

Use the pooling runner to lock this contender and its two controls before opening
final labels. Once all 300 attempts have completed, verify the sealed receipt and
run only `report-confirmation`. Review qualified counts, the depth-28 sensitivity,
paired ordinary and simultaneous uncertainty, all gate arms and tails. An interval
including zero is inconclusive, not evidence of equivalence. Do not tune on this
last sample if the result disappoints.

No production switch follows automatically. If confirmation does not establish a
convincing benefit, retain the existing proposal pending book-level evidence.
Even a confirmed mean-error gain leaves the stricter-White safety issue and
missing-source policy unresolved. Full blended-child relative gates, local
verification of consequential disagreements, move regret and reach effects must
be tested in the Black pilot after accepted pool publication.

Source artifacts reviewed: pooled `COMPLETE.json`, `summary.json`,
`ranking-comparison.csv`, all primary panel `*-metrics.csv` / `*-gates.csv`,
`reference-coverage.csv`, `primary-cohort-counts.csv`; main-study
`holdout-paired.csv`, `holdout-depth28-metrics.csv`, `reference-qualification.csv`;
targeted `summary.json`, `source-errors.csv`, `absolute-gates.csv`.
