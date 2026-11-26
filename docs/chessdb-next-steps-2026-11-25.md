# ChessDB next steps — November 25, 2026

David authorized proceeding after the lossless F: archive and independent deep audit
completed. The mini pool coordinator and hourly monitor resumed at 09:25 PST;
see `pool-transport-recovery-2026-11-24.md`. The archival conversion is finished.

## Execution plan

1. Freeze a targeted SF18 diagnostic from the existing 1,500-position panel. Test the
   November 23 proposal against the verified step-1 candidate without changing source
   bindings, gates, or cached engine evidence. Implement checkpointed searches and
   reporting; exercise interruption/resume and identity rejection before the full run.
2. Run the new searches on the main PC with four independent one-thread engines and
   128 MiB hash each, leaving the mini's one-job pool queue untouched. Observe real
   completions and qualify reference strength before reporting an ETA.
3. Inventory the old raw ChessDB copies and verify normal archive access. Prepare an
   exact cleanup list with retention/dependency evidence. No source deletion is part
   of this diagnostic launch; the earlier recommendation reserved an explicit cleanup
   step. Preserve the F: archive, lookup/menu receipts, manifests and producer code.
4. Review the source comparison, including adverse results and unresolved searches.
   A real pilot on accepted pools still precedes active policy adoption. Carry both
   candidate/control bindings into that pilot; do not silently switch the default.

## Frozen selection and measurement rules

Before seeing any deeper searches, select the union of:

- Every parent whose absolute admission changes between step 1 and the November 23
  proposal under either approved gate arm and either prospective repertoire color.
- Parent source disagreements of at least 0.02 expected score (two percentage points).
- Existing ChessDB top-move regret of at least one percentage point, or top-two gap
  error of at least three points, including the available shared cloud-move gap check.
- Ten deterministically selected controls per source group, seed 20261125.

Recheck selected parents at target depth 28. For the move-regret/gap cases, recheck
the full existing ChessDB top-three menu at target child depth 27. Preserve position,
move and source identities; illegal/missing references stop preparation. Each search
uses a fresh SF18 process, the pinned binary, MultiPV 1, Threads 1 and Hash 128.
Use a 60-second search-time cap. Qualify a result only if terminal or its completed
iteration is at least two depths beyond the old reference and not a bound. Keep
time-limited/unqualified searches as unresolved, never as replacements for old evidence.

Report paired score errors and tails, changed absolute classifications, old/deeper
reference disagreement, margins within 0.002 score of the floor, menu regret and
candidate-gate differences where all needed references qualify. Preserve actual
depth/nodes/time and engine identity. Menu-relative tests cover the searched ChessDB
menu; they do not establish the full blended child-evaluation policy in the builder.

This is a targeted follow-up on an already exposed panel, not fresh confirmation or
a reach-weighted repertoire result. Report selection reasons/source strata and the
control subset separately; do not extrapolate its aggregate error rate to all positions.
No new blend weights, thresholds or Stockfish scale are tuned on these results.
Whether the proposal is safe to adopt remains a later acceptance decision.

**Later November 25 authorization:** David requested a broader three-source ensemble
comparison. The [ensemble study](chessdb-ensemble-study-2026-11-25.md) freezes 135
rules and development-selected finalists, adds 1,000 fresh held-out references,
and uses this targeted diagnostic as a separate stress test. That new study supersedes
the earlier no-new-weights restriction for research; active lookup adoption still
requires review and the Black pilot. The current diagnostic's frozen recipe is unchanged.

## Progress

- Archive completion marker rechecked against the independent full-audit receipt.
- Four diagnostic regression tests passed: deterministic selection/full-width keys,
  reference-depth qualification, bounded interruption/resume and identity/corruption
  rejection, and individual UCI score records versus stale aggregate bounds.
- Selection frozen: **452 parent positions**, **168 menu checks**, **942 searches**.
  Parent strata: cloud deep 75, cloud shallow 88, gap 21, NNUE once 119,
  NNUE repeated 116, pre-NNUE 33. Sixty controls are included in this union.
- The first 12-search pilot exposed stale bound flags in the Python wrapper's
  aggregate engine info. Its records are preserved under the v1 directory. The
  corrected wrapper selects one complete exact score iteration and retains its
  own depth/nodes/PV; deeper unfinished bounds cannot inflate the reference depth.
- v2 freezes the **identical task list**, not a selection altered after results.
  Code SHA256 `1c94bf4374f3dcacb3089c04ea6027c762584fdb08130e841ed0728cf5cd773d`;
  manifest SHA256 `8921d947470df91957df40cffdcb628fa330112b099af7d2826aac2ee9e7bf05`;
  parsed identity `0c2047aa4b51abb10dea46074d761414b64af0ba01248dee2b75d4aaf7263434`.
  Frozen release `D:/chess/bin/chessdb-policy-validation-20261125-v2`;
  output `D:/chess/chessdb-policy-validation-20261125-v2`.
- Corrected bounded pilot started 09:40 PST: wrapper PID 3468, coordinator 43148,
  `launch-pilot.json`, `pilot.stdout.log`, `pilot.stderr.log` in the output directory.
  **Passed: all 12 qualified at depth 28**, 84.484 seconds wall time,
  224.061 summed engine seconds (18.672 seconds/search). Thirteen transient bound
  updates were correctly distinguished from complete exact score records.
- Full continuation launched **09:42:09 PST**, wrapper PID **48568**, coordinator
  **29120**, `launch-full.json`, `full.stdout.log`, `full.stderr.log`. It reuses the
  12 durable pilot results under the same identity. At 09:42:33, **17/942 completed,
  all 17 qualified**, live matching coordinator, empty stderr. Four engines maximum.
  Planning ETA **1.5–3 hours**, allowing roughly four if most searches hit the cap;
  this is a diagnostic ETA, not a book or source-policy adoption deadline.
- Existing hourly automation `track-chessdb-and-mini-pool-progress` is ACTIVE and
  now follows both this run and the mini bucket queue. The archive stays finished.
- Normal archive reads succeeded on first/middle/last shards using PyArrow, including
  raw binary keys/values, decoded moves and both distance fields. See the separate
  [cleanup inventory](chessdb-cleanup-inventory-2026-11-25.md).
- Active lookup bindings, canonical books and original databases remain unchanged.

## Run / resume / stop

Read `status.json`, actual process state and stderr together. Reuse only this frozen
release with this experiment directory; do not edit live release code. A completed
search is an individually hashed, atomically published JSON result. To stop, create
`STOP` inside the experiment directory: no new work starts and at most four active
searches drain (60-second search caps). Only remove that marker on authorized resume.
After the bounded pilot is accepted, resume without `--max-new`:

```powershell
& 'C:\Users\David\Documents\Chess\.venv\Scripts\python.exe' -B -u `
  'D:\chess\bin\chessdb-policy-validation-20261125-v2\chessdb_policy_validation.py' run `
  --out 'D:\chess\chessdb-policy-validation-20261125-v2' --workers 4
```

For persistent unattended execution, launch hidden with fresh redirected logs and
record the PID/time/command. Check no existing coordinator holds `run.lock` first.
Do not convert unresolved references to accepted ones to finish the report. Review
`summary.json`, `parent-results.csv` and `menu-results.csv` (as emitted by the runner)
and source-group/control results before proposing any next-stage adoption.

## Remaining acceptance work

1. Review the complete diagnostic, including stricter-White false admissions (the
   earlier proposal increased these from 29 to 36 on the old reference), not only
   average source error. Report how many older classifications change with deeper
   search and how many remain unresolved or close to a gate.
2. Do not interpret the deliberately enriched 452-parent sample as an unbiased
   production estimate. It tests known disagreement cases on an exposed panel;
   its 60 controls also come from that panel. No new source weights are being fitted.
3. Once the pool is globally merged and accepted, compare candidate/control source
   policies in the Black pilot, including full blended child references, gate
   decisions, menu additions, selected moves and reach-weighted effects. Merely
   finishing all 512 construction buckets is not final pool acceptance.
4. **Completed later November 25:** David authorized both raw-copy retirements
   after the Rust archive reader passed exact native parity. Both raw directories
   are deleted and the Ubuntu VHD is compacted. Use
   `D:/chess/bin/chessdb-archive-query-20261125-v1` for future point lookups; see
   [the retirement record](chessdb-archive-lookup-retirement-2026-11-25.md).
