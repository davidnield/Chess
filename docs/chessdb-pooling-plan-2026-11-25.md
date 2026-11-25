# Pooled ensemble selection — November 25, 2026

David approved combining the November 22–23 panel with the new November 25 study
for ensemble selection, while keeping the final 300-position confirmation panel
separate. This amends the selection workflow in the ensemble and confirmation
plans; it does not change their frozen sampling recipes, candidate definitions,
engine settings, inputs or active workers.

## Evidence and position accounting

| Panel | Parents | All three sources | All three, NNUE Fishnet | Role |
| --- | ---: | ---: | ---: | --- |
| November 22–23 | 1,500 | 424 | 408 | Previously exposed selection evidence |
| November 25 main study | 1,000 | 700 | 600 | Additional selection evidence once complete |
| Pooled selection | 2,500 | 1,124 | 1,008 | Choose one contender and best single-source control |
| Separate final panel | 300 | 300 | 300 | Confirmation after the choice is locked |

Selection counts above precede exclusion of unqualified new searches. A real-data
preflight verified exactly 2,500 distinct canonical boards and no intersection
with the final 300. Full signed 128-bit keys remain integer pairs, and keys are
checked against FENs. Canonicalization removes irrelevant en-passant spelling and
counts color mirrors only once. The original November 13 pilot is not added again.

The deeper targeted diagnostic completed at **10:54 PST**: 942 attempted searches,
939 qualified, comprising 452 parent rechecks and 490 child searches. **451 parent
references qualify to replace the older values; one retains its original value.**
The children remain move-menu/gate diagnostics and never increase the parent N.

Use one best qualified reference per position. An older reference is replaced only
by its matching parent result with a qualified actual depth at least two beyond
the old requested depth, or a terminal result. An unqualified recheck never
downgrades the old reference. Historical `depth=24` records requested depth; actual
completed depth was not recorded. Preserve that distinction: 1,049 older rows
currently retain references with unknown actual depth. Do not relabel them as
measured depth 24 or depth 26. Exclude unqualified new references, and report counts.

## Rankings, sensitivities and interpretation

Use the same frozen 135 candidate recipes, expected-score conversion, source
values, baseline evaluations and gate arms. Primary ranking is mean absolute
expected-score error, with equal weight to the four NNUE triple-overlap strata
(deep/shallow Cloud × once/repeated NNUE Fishnet), and equal position weights
within each stratum. This is source-stratified evaluation, not repertoire reach
weighting or an estimate of source prevalence in the full database.

Produce all of the following from the same qualified evidence:

- Pooled selection, older panel with replacements, and new panel alone.
- Older original references and pooled original-old references, to isolate the
  effect of replacing selected older values.
- Measured depth >=26 only, plus each reference-provenance/depth group separately.
- Equal older/newer panel weights within each primary stratum as an additional
  sensitivity; the main ranking instead weights positions equally within strata.
- Source-only matched comparisons, all-three including legacy Fishnet as a
  sensitivity, and separately labelled deployment/fallback cohorts. Missing
  Fishnet is distinct from legacy Fishnet, and ChessDB-only rows are distinct.
- Expected-score errors, tails, signed bias, cp errors, and each color/gate arm's
  false admissions, clear false admissions, false exclusions and unsafe shortfall.

The targeted rechecks are selected stress cases, so the deeper-only sample is not
an unbiased population sample. Older and newer panels have different sampling and
reference quality. Preserve the panel-specific rankings rather than hiding a
reversal inside an average. Pooled ranks are selection statistics, not an
independent confirmation of the best of 135 candidates. The main study still
reports its original predeclared finalist comparisons unchanged.

After reviewing pooled and panel-specific error, tails, gate safety and the
original new-only paired uncertainty, choose one contender from the frozen suite
and record the rationale. Retaining the existing proposal is allowed. The final
controls are the current November 23 proposal and the **best single source on the
pooled primary cohort**, fixed before opening confirmation labels.

## Blinding, receipts and operation

Maintained code: `python/chessdb_ensemble_pooling.py`.
Frozen release: `D:/chess/bin/chessdb-ensemble-pooling-20261125-v1`.
Output: `D:/chess/chessdb-ensemble-pooling-20261125-v1`.

Started **10:58:14 PST**, wrapper PID **13980**, coordinator PID **36468**;
verified healthy in `waiting-for-study` with empty stderr. See `launch-run.json`,
`run.stdout.log`, `run.stderr.log` and `status.json`. Definition SHA256:
`478c344cfdd192a13b7f00eec508c41715bd10208928edccdbc2c6300bcdce4e`.

The new worker waits until both the targeted diagnostic and full 1,000-position
study have completed. It performs small-table analysis only and launches **zero
additional Stockfish searches**. Existing search workers continue unchanged. The
final 300 engine searches may run while pooling analyzes the completed earlier
evidence, but their results remain sealed.

`definition.json` / `freeze.json` bind the pooling code, runtime, recipe and input
hashes. `reference-audit.parquet` explains reference selection and retention for
every input row. `pooled-positions.parquet` contains only retained parent rows;
`reference-coverage.csv` and `primary-cohort-counts.csv` preserve denominator
information. `ranking-comparison.csv`, the per-panel `*-metrics.csv` / `*-gates.csv`,
and `summary.json` are selection reports. `COMPLETE.json` binds the source records
and generated outputs; verifying it rejects changed evidence.

`POOL_SELECTION_REQUIRED.json` in the final confirmation directory points at this
protocol. From now on, use the **pooling runner** for `lock-choice` and
`report-confirmation`; the old confirmation runner is retained for engine
`pipeline` resume only. Frozen old code remains unchanged, so this routing marker
and operating instructions are a workflow safeguard, not an access-control system.
The new reporting wrapper rejects a choice lacking the pooled protocol identity.

```powershell
$runner = 'D:\chess\bin\chessdb-ensemble-pooling-20261125-v1\chessdb_ensemble_pooling.py'
$output = 'D:\chess\chessdb-ensemble-pooling-20261125-v1'
$python = 'C:\Users\David\Documents\Chess\.venv\Scripts\python.exe'
# After COMPLETE.json passes and the selection reports have been reviewed:
& $python -B $runner lock-choice --out $output `
  --contender SELECTED_RECIPE_NAME --rationale ABSOLUTE_RATIONALE_FILE
# After the separate panel has SEALED.COMPLETE.json and the choice is locked:
& $python -B $runner report-confirmation --out $output
```

Locking never reads sealed results. Its immutable receipt binds the pooled
evidence, chosen recipe, proposal and pooled-best individual control. Reporting
then uses the unchanged final-panel evaluator for those limited comparisons only.
Do not pool final confirmation scores back into selection and claim independence.
No production evaluation bindings or books are changed; blended-child relative
gates, move regret and repertoire effects still require the Black pilot.

For authorized resume, check for a matching live pooling process first and rerun
the same frozen `run --out ...` with fresh hidden-process logs. Its OS lock prevents
duplicate workers, and `STOP` halts the waiting worker. A valid `COMPLETE.json`
makes resume verify evidence and return without rebuilding it. Check `status.json`
and stderr, not the mere presence of a launched process.

## Verification

Nine focused tests passed. They cover qualified replacement and retention of old evidence,
child rejection, full-width keys, canonical/mirror deduplication, final-panel
exclusion, corrupt references, exact weighting and reference sensitivities,
refusal to analyze incomplete studies, blinded immutable selection, evidence
checksums, interoperability with the unchanged confirmation evaluator, rejection
of old unpooled choices, and full report/receipt generation on a synthetic panel.
The real preflight reads old and completed-targeted evidence plus new sampling
metadata only; it does not inspect interim new scores or any sealed final scores.
See `chessdb-pooling-verification-2026-11-25.json` for measured checks and launch
identities. No final contender has been selected at setup time.

## Completion and locked selection — 12:28 PST

The main study finished at 12:13 PST (999/1,000 qualified) and pooling at 12:14
(2,499 retained, 1,008 NNUE triple overlaps). The completion receipt and all bound
evidence verified. At **12:26:13 PST**, `score_3_3_4` was immutably locked against
`proposal` and ChessDB alone, before any sealed result was read. See
[the selection rationale](chessdb-ensemble-selection-rationale-2026-11-25.md) and
`chessdb-selection-review-2026-11-25.json`. Do not rerun `lock-choice` or change the
contender. Choice SHA256:
`9d74bbeb258d969ba09adfb453cc92733ee0de7308a057d45711e10b2a0afb38`.

The mean-error advantage has stricter-White gate and missing-Cloud tradeoffs;
production remains unchanged. Confirmation is still searching (175/300 attempted,
174 qualified at 12:27:51). Its scores remain unopened. After its sealed completion,
the next action is the pooling runner's `report-confirmation`, followed by review
of the limited comparisons. The hourly monitor now knows the choice is already
locked and must not select again.

At **12:50:54 PST**, the final comparison was verified, opened and reported using
the already locked choice; 299/300 qualified. Its reviewed results are documented
in `chessdb-ensemble-confirmation-results-2026-11-25.md`. The all-source mean-error
advantage survived independent confirmation; missing-source behavior and gate
safety still preclude automatic adoption. This research sequence is complete.
