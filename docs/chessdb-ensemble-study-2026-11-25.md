# Evaluation-source ensemble study — November 25, 2026

David requested a wider comparison of Cloud, Fishnet and ChessDB, independently on
matched positions and in ensembles. This authorizes a new tuning study, superseding
the earlier restriction to comparing only the two frozen source policies. It does
**not** authorize production adoption without verification in the Black pilot.

## Samples and independent evidence

The original November 13 overlap pilot had 200 positions at SF18 depth 22. The
November 22/23 panel has **1,500 parent positions** at requested depth 24 (plus
4,030 child searches at depth 23). The running November 25 diagnostic upgrades
**452 of those parents**, with 490 child searches, targeting depths 28/27.
Its 942 searches are not 942 new independent parent positions. It was deliberately
enriched for disagreements and changed gate decisions.

The old 1,500-parent panel has **424 three-source overlaps**, of which **408** have
NNUE Fishnet. The selected deeper diagnostic includes **142** three-source parent
overlaps. Therefore deeper references alone do not provide a larger independent
ensemble-selection sample. We will use:

1. **Development:** the old panel; primary tuning on its 408 NNUE triple overlaps.
   Freeze finalists before obtaining any new held-out engine evaluations.
2. **Stress test:** reuse the running deeper diagnostic after completion. Keep its
   errors separate; its selection cannot estimate average production accuracy.
3. **Fresh holdout:** 1,000 previously unused positions, with new SF18 references.
   No interim holdout leaderboard. Report all candidates once at the end; only
   the pre-frozen finalists support the confirmatory comparison.

## Candidate suite: 135 rules

- The three independent sources on the **same complete-case positions**.
- Every nonnegative blend with weights in tenths summing to one: 66 combinations,
  including single sources, all pairwise blends, and three-way blends.
- Blend either White centipawns, then convert to expected score, or convert each
  source to expected score first, then blend. The three identical single-source
  duplicates are omitted, leaving 129 grid variants.
- Exact equal-thirds blends in both spaces, plus the three-source median.
- Three existing policies: step-1 candidate, November 13 rule, November 23 proposal.

Names record the coordinate system and weights in Cloud/Fishnet/ChessDB order:
`score_3_3_4` means 30% Cloud, 30% Fishnet, 40% ChessDB in expected-score space.
The function is the builder's existing `1 / (1 + exp(-0.00368208 * cp))`; source
scores have White perspective and the existing ±2,000 cp/mate handling. No scale
factor, source reducer, depth threshold or gate tolerance is fitted in this study.

The median is not duplicated across spaces: for three available sources a monotone
score conversion preserves the middle value. The deployment/fallback sensitivity
uses the explicitly implemented centipawn median when only two eligible sources exist.

## Selection and acceptance criteria

Primary selection minimizes **mean absolute expected-score error**, giving equal
weight to four NNUE overlap strata (Cloud depth >=22 vs <22, Fishnet once vs repeated).
This deliberately balanced objective is not a census or repertoire-reach objective.
The source quality splits stay fixed at their previous definitions.

Freeze the best mean-error rule, its runner-up, a safety alternative with the fewest
worst-arm clear false admissions among rules within 10% of the best mean error,
and the best single source. Compare them with the two existing candidate policies.
Eight-bucket blocked cross-validation measures stability of the **selection procedure**
on development data; it is not an independent evaluation of a winner chosen on all
development rows, nor a separation by opening family.

Report matched counts, mean and tail errors (p95/p99/max), signed bias, RMSE,
centipawn MAE, and incorrect absolute-gate admissions/exclusions for both colors
and both approved gate arms. Separate reference values within 0.002 of the floor;
report cumulative unsafe floor shortfall as well as counts. A small mean-error gain
does not automatically justify worse gate safety or tails.

Use 4,000 paired stratified bootstrap replicates for the predeclared comparisons,
with ordinary 95% intervals and Bonferroni-adjusted simultaneous intervals across
that comparison family. These are approximate intervals conditional on the sampled
positions/strata; related opening positions may remain correlated. The 135-rule
holdout leaderboard is exploratory. Choosing a new rule from that table would
require further independent confirmation rather than calling it the validated winner.

## Fresh sample and source handling

| Cloud coverage | Fishnet tier/count | Positions |
| --- | --- | ---: |
| Deep | NNUE, once | 150 |
| Deep | NNUE, repeated | 150 |
| Shallow | NNUE, once | 150 |
| Shallow | NNUE, repeated | 150 |
| Deep | Legacy | 50 |
| Shallow | Legacy | 50 |
| Absent | NNUE, once | 100 |
| Absent | NNUE, repeated | 100 |
| Absent | Legacy | 100 |

All 1,000 have scored ChessDB and Fishnet; 700 also have Cloud. The primary fresh
three-source NNUE comparison has 600 positions. Legacy and missing-Cloud strata
test separately whether a proposed ensemble's fallback behavior is sound.
No-Cloud/no-Fishnet gaps are not included: there is no ensemble choice there; the
older gap diagnostic remains available.

Draw from eight deterministic full-key hash buckets disjoint from the earlier
panel's eight buckets, seed 202611251, with 1-in-8 hash thinning. Use native DuckDB
hash rank consistently for bucket top-K and global selection within each stratum.
Require >=2 historical observations, earliest ply <=50, unambiguous unique eval
identity, scored ChessDB and valid nonterminal boards. All joins retain both signed
64-bit halves; validate every selected FEN against its 128-bit key.

Exclude canonical boards and color mirrors of all known earlier panel parents and
children, the November 13 overlap and supplementary examples, the 1,000 missing-key
examples, and the public gap sample. Canonicalization removes inconsequential EP
spelling differences. This prevents exact-position leakage, not all shared-opening
or source-dataset dependence. Quota shortfalls stop preparation rather than silently
altering the panel. Counts by source, ply and game frequency should accompany results.

For source-only complete-case tables, missing sources remain missing: no hidden
fallback is credited to an individual source. In the separately labelled deployment
sensitivity, exclude legacy Fishnet, renormalize weights over available eligible
sources, and fall back to ChessDB if all positively weighted sources are missing.
This is a tested candidate policy, not a change to active lookup bindings.

## Execution, receipts and status

Maintained entry point: `python/chessdb_ensemble_study.py`, commands `initialize`,
`sample`, `pipeline`, `report`. Frozen release:
`D:/chess/bin/chessdb-ensemble-study-20261125-v1`.
Output: `D:/chess/chessdb-ensemble-study-20261125-v1`.

`definition.json` freezes candidate definitions, sampling, runtime versions, code,
source hashes and gates. `finalists.json` is hashed by `freeze.json` before sampling.
Per-bucket source/output receipts precede `holdout-inputs.parquet` and the search
manifest. Source publication hashes and full-key/FEN checks remain mandatory.
Searches reuse the corrected individual-UCI-record logic in the November 25 diagnostic;
no active frozen code was edited.

The sampling pilot finished its first real bucket (114) in **8.62 seconds** with
all nine strata represented, 3,501 retained candidates, and no errors. Five focused
tests passed, including native DuckDB selection, nullable values, full-width keys,
source receipts, fallback semantics, cross-color gates, resume and refusal to report
interim holdout results. Existing-policy predictions exactly reproduced all 1,500
stored step-1 / November-13 / proposal scores. A development-only reporting smoke
produced 1,620 metric rows and 15 paired intervals with finite values.

Pipeline launched **09:56 PST** (consult `launch-pipeline.json` for the exact time/PID).
It finishes sampling, waits for the existing 942-search diagnostic to complete, then
runs four one-thread SF18 processes, Hash 128 MiB, target depth 28 and 60-second caps.
Qualification requires depth >=26; report actual depth/nodes/time and unresolved
searches, plus a depth-28-only sensitivity table. Do not count shallow references as
qualified. Allow roughly **1.5–4 additional hours** for the new searches after the
current diagnostic, refined using live throughput.

At **09:57:42 PST**, all eight sampling buckets were completed and all 1,000 final
positions passed the full-key/FEN checks, exact quotas, and exclusion checks. There
are **zero duplicate keys or canonical/mirrored boards**. Earliest ply spans 4–49,
median 19, interquartile range 15–23. Historical observations span 2–2,607,353,
median 12, interquartile range 5–38.25. These counts are source coverage, not reach
under any proposed repertoire. The reference queue is **waiting-for-targeted**;
the preceding diagnostic was 231/942 at that snapshot. No fresh SF labels were
used to choose the candidate recipes or finalists.

Pipeline wrapper PID **32552**, coordinator **20860**. Definition SHA256
`d0f94b27e13ed659153ca4a2ee4e0eec6efe18d53388796c654a553cb5baafd8`;
finalists SHA256 `5798bb2bedcbe2c96d8c4bd2809f9a144232c837825879d41249e2e56acfaf8e`.
Buckets: 114, 187, 332, 252, 379, 331, 195, 413. The hourly monitor follows both
studies and the mini pool queue, and preserves the separation of research and adoption.

Development-only finalists, selected before fresh labels:

| Role | Rule | Development macro MAE, score points | Worst-arm clear false admissions / 408 |
| --- | --- | ---: | ---: |
| Lowest mean error | Equal thirds, score space | 0.9059 | 10 |
| Runner-up | 30/30/40, score space | 0.9072 | 9 |
| Existing proposal | Fixed depth-aware November 23 rule | 0.9318 | 8 |
| Safety alternative within 10% of best MAE | 10/60/30, score space | 0.9834 | 6 |
| Best single source | ChessDB | 1.1774 | 16 |
| Existing step-1 candidate | Cloud on this overlap cohort | 1.3994 | 19 |

These are **selection statistics, not evidence of an independently confirmed winner**.
The 0.026-point mean-error lead over the proposal is small and trades against gate
mistakes in this panel. The held-out test, uncertainty and per-stratum safety results
must decide whether any added complexity is justified.

The coordinator holds an OS lock; individual search results are hashed and atomically
published. Put `STOP` in the study output directory to stop new work and drain active
searches. Resume only on authorization with the same frozen recipe/code. The `pipeline`
command resumes completed sample buckets and reference results. A changed recipe uses
a new output identity. Keep the original diagnostic and its receipts intact.

After completion, inspect `summary.json`, `holdout-metrics.csv`, `holdout-gates.csv`,
`holdout-paired.csv`, `reference-qualification.csv`, `holdout-depth28-*` and
`targeted-stress-*`. No automatic source-policy adoption. Full blended-child relative
gate behavior, move regret, and reach-weighted book effects still require the Black
pilot after pool publication.

## Additional final confirmation approved

David subsequently approved **300 further NNUE triple-overlap positions**, 75 in
each of the four primary source-quality strata. They are sampled from new buckets
and exclude this study's positions and all previous known boards/mirrors. Their
scores stay separate until one final contender is selected and recorded from this
study's reports. Follow `chessdb-confirmation-plan-2026-11-25.md` for the queued job,
blinding, immutable choice and final comparison. This study's existing frozen
definition, finalists, data and running worker are unchanged.

## Approved pooling amendment

David approved pooling the older 1,500 parents and this study's 1,000 parents for
selection: up to 2,500 unique positions, 1,124 all-source overlaps and 1,008 NNUE
all-source overlaps, before qualification exclusions. Use qualified deeper parent
rechecks as replacements, never additional observations. Preserve old-only,
new-only, original-reference and depth sensitivities. The final 300 remain separate.
Follow `chessdb-pooling-plan-2026-11-25.md` and use its runner for the immutable
contender lock and final reporting. This study's frozen code/reports stay intact;
pooling is a separate waiting analysis job and adds no engine searches.

Completed **12:13 PST November 25**, with 999/1,000 qualified references; the one
excluded reference is in `no_cloud_repeated`. All 600 new NNUE triple overlaps
qualified, 595 at actual depth >=28. The pooled review and pre-confirmation choice
are in `chessdb-ensemble-selection-rationale-2026-11-25.md`. This study's reports
remain frozen; final confirmation and production adoption are separate.
