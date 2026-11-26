# Native evaluation policy integration — November 25, 2026

The approved policy is implemented and has passed native tests, consumer
regressions, a 45-position White review and a bounded real-data pilot. **The full
candidate overlay has completed materialization and its independent sampled check; no active repertoire/pool binding has
changed.** The paired Black stricter pilot and production activation still wait
for accepted pool/opponent inputs and the acceptance checks below.

## Implementation and unchanged evidence

- `rust/book_builder/src/eval_policy.rs` computes the frozen 30% Cloud / 30% NNUE
  Fishnet / 40% ChessDB average in White expected-score space. Legacy Fishnet is
  excluded. With Cloud absent and ChessDB available it uses ChessDB alone. It
  does not renormalize weights in other missing-source cases.
- The fallback is explicitly the accepted step-1 generation
  `D:/chess/eval-lookup-p51-cdb2-20261122`, manifest SHA256
  `b4c90b5d923ef4a4baf2a0020ddebe31872914546be1a27207a7f4ae0777d1c6`.
  This is distinct from the research comparator named `proposal`. The paired
  Black pilot's fixed pre-change control must use this bound step-1 generation.
- `python/eval_policy_overlay.py` handles bounded DuckDB/Arrow I/O and receipts;
  every source-policy calculation goes through the Rust extension. It joins
  signed 128-bit keys and writes only rows using the new policy. Source 6 means
  the eligible three-source blend; source 7 means ChessDB without Cloud.
- The new `eval-lookup-128-score-overlay-v1` stores fractional equivalent
  centipawns as float64. `WideEvalLookup` composes it over the pinned old lookup.
  Parent and child consumers use the same interface and policy. Missing evidence
  retains the existing missing sentinel, never equality. Terminal rows cannot
  be replaced, and the actual `EvalSource` layer preserves qualified local
  Stockfish overrides above cached-source values.
- The pool's Other Moves aggregation and extension child-score adapter had
  integer casts. Those now retain float64. This is necessary even though the
  currently building min-games-1 pool has no Other Moves mass.
  The pool's separately implemented audit reader now composes the overlay over
  its pinned fallback too; it continues using its own low-half bisect/high-half
  comparison rather than calling the production lookup reader.
- New lookups pin source content, native binary, code/runtime, baseline generation
  and per-bucket outputs. Resume checks input/output hashes. Partial builds have
  only `pilot.json`; normal consumers cannot use them. Full publication requires
  all 512 buckets verified in the invocation. Full builds enforce the existing
  **250 GiB reserve plus 8 GiB working margin** before each new bucket.
- `sweep/publications.py` now understands the versioned schema but fails closed
  until a full candidate has a pinned `eval-policy-pilot-ready-v1` receipt with
  native-test, real-source, White-review and materialization evidence. This
  permits a later pilot binding, not automatic adoption of canonical books.

Raw source values, the old lookup, independent Stockfish searches and Maia caches
are preserved. Changed evaluation-policy bindings invalidate derived values and
book receipts; they do not justify deleting independent engine/model evidence.
All absolute/relative gates, priors/baselines, opponent settings and optimizer
settings remain unchanged. The main checkout's installed extension is unchanged;
the tested extension is installed only in the maintained worktree and frozen
candidate release.

## Verification completed

- Four native Rust policy tests: score-space arithmetic, side-to-move orientation,
  all missing-source branches, NNUE eligibility, mate/clipping normalization,
  terminal precedence, invalid inputs, mirrored scores and fractional precision.
- Six integration tests: actual Parquet-to-native-to-NPY-to-lookup flow, high-half
  hash twins, missing lookups, qualified local overrides in both scorer modes,
  byte-preserving resume, corrupted input/output/fallback rejection, duplicate
  keys across batches, unpublished lookup refusal and disk-reserve failure. A
  deliberately fractional Other Moves evaluation also survives actual pool
  aggregation and both independent pool audits with zero difference.
- Existing wide-lookup test and both end-to-end full-key builder/referee tests
  passed. The full Rust/Python differential builder suite passed, including
  collectors, gates, graph construction, DP, price search, polish and rollouts.
- Two publication tests passed, including the new full-generation/evidence gate.
- Four full-key verification regressions passed, including the existing
  no-quality-downgrade path. The optional installed-engine smoke test was skipped
  in that suite; the 14 actual pinned-SF18 White searches ran separately.

Native extension SHA256:
`76bd48af7786067b05333eff3797961078fdb3f010fecf1646c86d463eed5a9e`.

## Targeted White review

The frozen selection-only review contains 45 unique boards. Fourteen older
references were upgraded with the pinned Stockfish 18, target depth 30, minimum
actual exact depth 28, four independent one-thread searches, 180 seconds maximum
per task. **All 14 qualified**; every review board now has actual depth >=28.
The other 31 references were retained. Completed records remain separate from
their originals, with task/engine/code/content identities.

The review replayed raw source fields through the production native policy,
materializer, float64 serialization and actual lookup reader. The maximum
difference from the frozen selected recipe was **1.11e-16 score units**.
Whole-centipawn rounding could introduce **0.0457 percentage points** of error in
this review; it changed no admission here, but the implementation avoids it.

| Targeted subset | Boards | Deep-reference admission changes | New-policy false admissions | Clear false admissions | Research-control false admissions | New-policy-only false admissions |
|---|---:|---:|---:|---:|---:|---:|
| White permissive | 15 | 1 | 9 | 4 | 12 | 1 |
| White stricter | 30 | 6 | 20 | 19 | 12 | 9 |

These are **deliberately selected troublesome boards**, not failure rates for
random positions or for a repertoire. “Clear” excludes positions within 0.2
percentage points of the floor. The control in this table is the frozen research
`proposal`, not the step-1 lookup that will be used for the paired Black control.

The deeper checks changed seven reference classifications but did **not** remove
the stricter-White concern. The Rust implementation reproduces the intended
policy correctly; approximate cached sources still disagree with deep Stockfish
near some floors. Keep independent verification and qualified local overrides
for gate-critical selected moves. This audit completes the targeted review; it
does not declare cached-source admissions safe without that verification or
authorize loosening the agreed gates.

Evidence: `docs/evidence/eval-production-acceptance-20261125/white-production-review.json`
and `white-production-cases.csv`. Detailed immutable search checkpoints are in
the worktree's `scratch/white-audit-20261125-v1/results`; copies are retained under
the evidence directory. No final-confirmation labels were reused to tune weights.

## Real-data capacity and independent checks

Pilot buckets 000, 255 and 511 used the accepted complete source generations.

| Measurement | Observed |
|---|---:|
| Changed-policy rows stored | 11,708,242 |
| Overlay bytes | 292,706,626 (279 MiB) |
| Serial build time including input hashing | 57.56 seconds |
| Peak process RAM | approximately 1.4 GiB |
| Independent source-record checks | 16,893 |
| Maximum independent score discrepancy | 3.33e-16 |

The independent checker uses direct SQL over raw sources and separately written
policy arithmetic, not the native output as its expected answer. Samples cover
all output source kinds, unchanged fallback rows and authoritative terminals.
Full-key byte hashes and provenance also passed.

For batches of about 5,600 sampled keys within one bucket, warm overlay lookups
took 0.188–0.259 seconds versus 0.153–0.197 for the base alone: approximately
18–32% additional **lookup** time in this microbenchmark. That is not an estimate
of overall repertoire-builder slowdown. Cold lookup with first-use content
verification took 1.70–1.88 seconds per bucket. Measure real builder throughput
in the paired pilot.

Straight-line projection: **46.5 GiB stored overlay and about 2.7 hours serial**,
allow **3–4 hours** for runtime variation and final checks. This keeps both source
generations and avoids duplicating the roughly 12.8-billion-row baseline lookup.
The candidate run retains the same native/materialization kernel as the pilot;
the final launcher adds the per-bucket disk guard and inherited explorer/source
publication metadata, both tested before launch.

## Current run and resume

Started **13:27:40 PST, November 25**, launcher PID 35984:

- Frozen code: `D:/chess/bin/eval-source-policy-20261125-v1`.
- New candidate: `D:/chess/eval-lookup-p51-score334-20261125-v1`.
- Status: `status.json`; logs: `stdout.log`, `stderr.log`; launch: `launch.json`.
- Two DuckDB threads, 2 GB DuckDB memory setting, one bucket at a time.
- Initial D: free space 465.19 GiB. Projected completion leaves about 419 GiB
  before unrelated changes, above the 250 GiB reserve.
- The original mini queue continues unchanged. At 13:28 PST it had **303/512**
  total accepted buckets (295 new plus eight originals), bucket 300 running.
  About **17–19 hours** of bucket work remain at the recent ~284-second cadence;
  final pool merge, independent verification and other required pool bindings
  are additional work, not automatically queued.

Resume only after confirming no existing candidate coordinator is alive. Reuse
the frozen entry point and exact inputs; do not run edited worktree code against
this output root:

```powershell
& C:\Users\David\Documents\Chess\.venv\Scripts\python.exe -u `
  D:\chess\bin\eval-source-policy-20261125-v1\eval_policy_overlay.py `
  --base D:\chess\eval-lookup-p51-cdb2-20261122 `
  --eval-db D:\chess\eval-db-p51-20261115 `
  --menus E:\chess\chessdb-menus-20260702 `
  --out D:\chess\eval-lookup-p51-score334-20261125-v1 `
  --full --memory 2GB --threads 2 --reserve-gib 250
```

Completed bucket receipts are reused after content verification; at interruption
only the active unreceipted bucket needs reconstruction. Full completion means
`status.json` is verified **and** a complete 512-bucket `lookup.json` exists and
matches receipts, not merely that the process exited. Preserve all previous
lookup generations and the ChessDB archive/raw data.

## Remaining acceptance and next work

1. Finish and check the full overlay, preserve a verification/acceptance receipt,
   and prepare candidate-only bindings with all dependencies pinned. The new
   publication validator requires this receipt; none has been fabricated early.
2. Complete the mini's remaining buckets. Recalculate global merge capacity with
   current disk inventories, including this additional 46.5 GiB, before launching
   a global sort. Preserve the existing no-F:-spill rule for pool construction.
3. Publish/verify the pool and inventory the other required opponent components.
   The mini is only building BRC 1800+ ply30. Do not relabel it as all-rating BRC
   or Rapid 2200+/Classical 1800+. Bind accepted OTB counts to the correct policy.
4. Run the paired Black stricter, 2,000-position, ply30 pilot using fixed optimizer,
   opponent model, gates and baselines. Change only the evaluation policy. Check
   parent/child gates, complete menus, reach, transpositions, horizon handling,
   verification closure, changed choices and runtime/cache costs.
5. Review gate-critical choices against independent local evidence, including
   the documented White limitation. Activate only after the agreed checks; then
   continue the Rust optimizer comparison and approved tuning matrix.

## Input inventory checked November 25, approximately 14:00 PST

The generic earlier checklist understated completed work. Both targeted online
components already have completed export/validation receipts under
`D:/chess/sweep-inputs`: `rapid2200-p30` (170,549,231 move rows) and
`classical1800-p30` (297,201,156 move rows). Their metadata and gate-report hashes
match the saved receipts, all four reported validation gates pass, the Parquet
footers have full 128-bit keys, and both have cap30/min-games1/zero Other Moves
mass. They use the same accepted explorer generation as the current work. This
inventory checks receipts, metadata and footers, not a fresh hash of every large
Parquet; content hashes must be checked again when freezing pilot bindings.

The corrected OTB production publication is also complete:
`D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119`. Its independent receipt
marks `training_counts_ready: true`, and the consumer checks already passed.
Use only the training-role p30 table for construction; retain validation/test
partitions for their designated evaluation purposes.

The substantive missing count inputs are **`brc-all-p30`** and
**`weak1000-1799-p30`**. Neither has an accepted export in the current shared-input
workspace, and neither is being built by the current mini queue. The first is
the broad empirical fallback in the OTB opponent ensemble; the second is the
construction-time model of our weaker post-book play. The current builder's arm
validator requires both, alongside adult/targeted/OTB components. Do not substitute
an older 64-bit pool or label the mini's 1800+ pool as either of them.

Before pilot construction we still need the current adult pool's global
merge/verification, the finished evaluation overlay and pilot-ready receipt,
accepted broad/weak counts, and frozen policy/model/cache/engine bindings with a
small production-path smoke check. Maia inference can remain selective and
populate the versioned cache as positions are reached; an all-position prediction
precompute is not a prerequisite. The existing targeted pools' count rows should
be reused under explicit new bindings, without overwriting their historical
metadata or inferring that a changed evaluation policy requires re-aggregating
evaluation-independent counts.

The paired rollout portion additionally needs its player/population settings,
paired seeds and leaf-referee bindings frozen. The full sweep's BRC2200 controls,
50-ply exports, improved-own-play sensitivities and complete evaluation-panel
matrix are later stages, not blanket prerequisites for this first Black pilot.
Finishing the two jobs currently running does not by itself make the pilot ready.

### Opponent-policy continuity check found during role tracing

The selective OTB ensemble already supplies opponent edges during graph
construction (`FrontierOverlay.attach`). However,
`build_budget_books.py` currently calls `weak_stop_values(..., pool=stats, ...)`
without an opponent-policy adapter, and `weak_stop.py` derives post-book opponent
probabilities directly from the adult pool's raw move counts. A selective policy
is published and closed before this stop-value calculation. Recording its recipe
in stop-table metadata does not make the stop calculation use it.

Before the intended OTB-policy pilot, connect the same frozen opponent ensemble
to the short post-book continuation used for both W(our turn) and R(opponent
turn), including newly reached off-graph boards, and to the matched rollout
configuration. Test a position where raw-adult and ensemble replies differ;
the expected stop value must use the latter. Preserve complete legal vectors,
real nonmove mass, terminal behavior, cache identities, both key halves and local
engine precedence. Do not apply another Maia pseudocount to completed ensemble
probabilities. Measure selective-query/model costs before a full book launch.
Apply the same corrected opponent path to both evaluation-policy arms so the
comparison still changes only evaluation-source policy.

The own-side stop policy is distinct: weak1000–1799 move counts, with the existing
min-weak-games20 fallback to the adult pool. This fallback is a current modeling
choice, not evidence that a thinly observed position is easy for a weak player.
No code/model behavior was changed during this inventory; this integration gap
is now an explicit pilot prerequisite.

## Full generation completed and model review

The full candidate finished with verified producer status: 512 buckets,
1,998,220,987 sparse rows, 49,955,622,979 bytes, 9,727 seconds. The complete
manifest SHA256 is `15be7323a4af4e8360bf89a6f06652fcbbaf52764c74a9685a6a6774e2ee2f0c`.
The independent bounded check of buckets 0/255/511 passed on 16,893 positions,
maximum score error 3.33e-16. Receipt:
`evidence/repertoire-model-review-20261125/full-overlay-independent.json`.
This is sampled semantic verification, alongside the producer's complete bucket
receipts; it is not an independent arithmetic census of all rows. Do not rerun
materialization. Pilot-ready evidence packaging and explicit consumer binding
remain; canonical bindings have not changed.

David requested a consolidated review after discussing the human models. Follow
[the model review and implementation plan](repertoire-model-review-and-implementation-plan-2026-11-25.md)
before the Black pilot. It confirms the weak-player threshold and opponent-policy
gaps and adds probability-mass, horizon/ending, objective, native-referee precision,
evaluation-binding and paired-request-union requirements. These are queued fixes,
not changes already made. The data/model decisions and running mini queue remain
preserved. Apply the corrected human/value contracts equally to both source arms.

The subsequent [implementation milestone](repertoire-model-implementation-2026-11-25.md)
adds explicit immutable evaluation bindings and per-arm stored references in the
joint verifier, plus an opt-in shared native human/continuation path. Generation
bindings and verification evidence are under
`evidence/repertoire-model-implementation-20261125/`. These do not activate the
source policy or substitute for the remaining production acceptance requirements.
