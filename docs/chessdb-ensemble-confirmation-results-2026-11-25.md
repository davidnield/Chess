# Independent ensemble confirmation — November 25, 2026

The pre-locked 30/30/40 Cloud/Fishnet/ChessDB expected-score blend improved mean
error against both fixed controls on the separate final panel. This supports its
complete-case accuracy advantage; it does not establish uniformly better gate
safety, missing-source behavior or repertoire performance. Production is unchanged.

## Protocol and completion

The immutable choice was recorded at 12:26:13 PST using only earlier selection
evidence. See `chessdb-ensemble-selection-rationale-2026-11-25.md`; that file and
the choice have not been changed. The engine worker completed all 300 searches at
12:36:49. The pooling wrapper verified its evidence and the locked choice before
opening results, and reported at **12:50:54 PST**. Only the locked blend, November
23 proposal, and ChessDB-alone comparator were evaluated on final labels.

299/300 references qualified; 297 reached actual depth >=28. The single unqualified
reference was in shallow/repeated Fishnet. Qualified counts are 75/75/75/74 across
deep-once/deep-repeated/shallow-once/shallow-repeated. Mean errors give each of
these four strata equal weight. This is not repertoire-reach weighting, and the
SF18 reference remains approximate.

## Matched results

All errors are percentage points of expected score, not book win-rate gains.

| Rule | Macro mean absolute error | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Locked 30/30/40, expected-score space | **0.95370** | **2.63330** | **3.79927** | **5.19571** |
| Existing proposal | 1.03799 | 3.10574 | 4.98138 | 6.83705 |
| ChessDB alone | 1.20019 | 3.48802 | 4.21676 | 6.21538 |

The blend's mean error is **8.1% lower than the proposal** and **20.5% lower than
ChessDB alone**. Paired mean-error differences (blend minus control):

| Control | Difference | Ordinary 95% bootstrap interval | Simultaneous interval, two comparisons |
| --- | ---: | ---: | ---: |
| Existing proposal | -0.08429 | [-0.15579, -0.01659] | **[-0.16566, -0.00648]** |
| ChessDB alone | -0.24649 | [-0.34717, -0.14678] | **[-0.36565, -0.12820]** |

The simultaneous intervals exclude zero for both comparisons. They are conditional
on the sampled positions/strata and do not model all shared-opening dependence or
systematic reference bias. The improvement over the proposal is modest in absolute
score units; it is not a measured increase in game wins.

The actual-depth-28-only sensitivity agrees: 0.94615 mean error for the blend,
1.02672 for the proposal, 1.20171 for ChessDB alone (N=297). No final-panel weight
search or retuning was performed, and no final labels were pooled into selection.

## Gate safety remains mixed

Clear false admissions exclude reference values within 0.2 expected-score points
of the floor. All columns use the same 299 qualified positions.

| Gate arm | 30/30/40 | Proposal | ChessDB alone |
| --- | ---: | ---: | ---: |
| Permissive White | 1 | 0 | 2 |
| Permissive Black | 5 | 5 | 1 |
| Stricter White | 5 | 4 | 14 |
| Stricter Black | 10 | 10 | 11 |

For stricter White, total false admissions are 7 versus proposal 5, and summed
unsafe shortfall is 4.8243 versus 2.7164 points. Thus the selection-panel warning
has not disappeared. Stricter-Black unsafe shortfall improves (8.7498 versus
10.7165), with equal counts. Small counts do not justify broad safety claims.
Mean/tail accuracy improved; a global guarantee of floor safety did not follow.

## Scope and next action

All final positions have Cloud, NNUE Fishnet and ChessDB. This experiment does not
validate missing-source weights. The frozen blend's deployment sensitivity simply
renormalizes remaining weights (50/50 Cloud/Fishnet, or 3/7 and 4/7 when ChessDB
and either other source remain). Legacy Fishnet is excluded there. The existing
selection evidence showed that without Cloud, ChessDB alone outperformed mixing
it with Fishnet. Do not adopt universal renormalization from this confirmation.
Cases lacking ChessDB were not represented by these panels.

Carry the now independently supported complete-case blend and the existing
proposal into the planned book-level validation. Retain the existing missing-source
policy pending separate justification, and preserve local checks for consequential
gate disagreements. Full blended-child relative gates, menu additions, selected
moves, regret and reach effects still require the Black pilot after pool publication.
No production policy, lookup binding or canonical repertoire was changed.

Primary artifacts:
`D:/chess/chessdb-ensemble-confirmation-20261125-v1/choice.json`,
`SEALED.COMPLETE.json`, `OPENED.json`, and `report/summary.json`,
`report/confirmation-metrics.csv`, `report/confirmation-gates.csv`,
`report/confirmation-depth28-metrics.csv`, `report/paired.csv`,
`report/qualification.csv`. Preserve these receipts and the selected recipe.
