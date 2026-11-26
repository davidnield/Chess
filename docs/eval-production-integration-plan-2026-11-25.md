# Approved evaluation policy: integration and acceptance — November 25, 2026

David accepted freezing **30% Cloud / 30% NNUE Fishnet / 40% ChessDB in expected-
score space** as the intended production recipe where all three eligible sources
exist. No further broad weight search is needed. The decision is recorded in
`eval-source-production-decision-2026-11-25.json`. This is an approved policy
contract, not an executable deployment manifest or a claim that active bindings
have changed. The accepted activation checks remain part of the work.

Keep ChessDB-only when Cloud is genuinely missing, retain other pre-change bound
fallback behavior, and preserve terminal handling and qualified local Stockfish
overrides. No universal renormalization. Legacy Fishnet is ineligible for the
three-source blend. Gates, their reference baselines and the rest of the tuning
recipe remain fixed. Preserve individual source values for diagnosis/verification.

## Work that can proceed before pool completion

1. **Implement the native source-policy path.** Add Rust computation and versioned
   provider/lookup binding. The current `python/eval_lookup_cdb.py` materializer
   implements only step-1 gap/legacy replacement; it does not implement this
   ensemble. Do not point consumers at it and claim the new policy is deployed.
   Preserve frozen releases. Bind the exact prior fallback generation rather than
   silently redefining "unchanged" for missing-source cases.
2. **Verify numerical and identity behavior.** Check score-space versus cp-space
   averaging, White/side-to-move orientation, eligibility, absent values, mate and
   clipping conventions, terminal/local precedence, and both 128-bit hash halves.
   If the provider serializes integer centipawns, quantify conversion/rounding
   errors at actual gate thresholds; do not assume byte-compatible layout implies
   identical decisions. Parent and child lookups must use the same recipe.
3. **Prepare and review the White gate cases.** The selection panel now has a
   reproducible diagnostic list of **45 distinct positions**: all candidate/control
   admission disagreements plus all candidate false admissions under the two White
   gate arms. Thirty-one have measured depth >=28 references; fourteen need stronger
   or better-recorded reference evidence before relying on the audit. This is a
   targeted review, not a new broad study or a production acceptance verdict.
4. **Measure materialization and merge readiness.** Use bounded representative
   inputs to decide whether a versioned lookup or changed-row overlay is most
   efficient, and measure disk/RAM/time before a full launch. Preserve raw source
   data, existing local Stockfish results and Maia predictions; invalidate only
   artifacts whose meaning depends on the changed evaluation policy. Separately
   plan pool global-sort workspace and receipt validation with current reserves.

The White review list is in
`docs/evidence/eval-production-acceptance-20261125/white-gate-review.csv` with a
hash-bound summary beside it. It uses **selection data only**, without touching
final confirmation for new tuning. It reproduced the published admission counts:
permissive White 6 classification disagreements / 10 candidate false admissions;
stricter White 13 disagreements / 26 false admissions. Counts include near-floor
cases. Candidate-only false admissions are 1 and 9 respectively. The 45-case union
is not a frequency-weighted estimate of repertoire risk. No extra engine searches
were launched during preparation; all entries remain diagnostic work items.

## Data prerequisites and pilot

The mini is constructing the **BRC 1800+, ply-30 pool**, not every required pool.
At 13:05 PST, 298/512 buckets were accepted (290 new plus eight originals), bucket
294 running and 296 preparing (295 was an already accepted original). Recent
cadence is about 284 seconds per bucket, suggesting **17–20 hours** of bucket work
remaining. Global merge/validation/publication is additional and not automatically
queued. Check live status and reserves before selecting the merge location.

After buckets finish, globally merge and independently verify this pool. Inventory
and complete/bind any missing components required by the approved opponent policy:
all-rating BRC, BRC 1800+, Rapid 2200+ / Classical 1800+, and accepted OTB training
counts as applicable. Do not relabel this one pool as the other populations or
silently run the pilot with an older opponent model. OTB count publication is
already complete, but binding/consumer validation is a separate requirement.

Inventory correction at approximately 14:00 PST November 25: Rapid2200-p30 and
Classical1800-p30 exports already have completed/validated receipts under
`D:/chess/sweep-inputs`; preserve and rebind these accepted counts. The missing
shared count inputs are all-rating BRC (`brc-all-p30`) and our post-book
1000–1799 population (`weak1000-1799-p30`). Neither is in the current mini queue.
See the integration-results document's input inventory for checked evidence and
the distinction between first-pilot inputs and later sweep controls.

Also connect the frozen OTB opponent ensemble to the post-book W/R stop-value
calculation and paired rollout adapter. Role tracing found that the current
short stop model still reads raw adult-pool opponent counts, even when graph
construction uses the ensemble. Require a differing-distribution regression and
a selective-query cost check before the pilot; apply the correction to both
evaluation-policy arms. Details are recorded in the integration-results document.

Then run the planned **Black stricter 2,000-position, ply-30 pilot**, including
a fixed-control comparison that changes only evaluation-source policy. Retain
the established Black baseline solver/settings so optimizer changes cannot
confound this acceptance check. Compare complete candidate menus, parent/child
scores, best-move ranking and loss, absolute/relative decisions, transposition/
horizon behavior, verification closure, reach, and matched rollout behavior.
Measure runtime, peak memory, storage, cache reuse and any changed prescriptions.

Acceptance requires reproducible identities, correct semantics and preserved
verification quality. Inspect materially changed choices with independent local
evidence; failures require investigation/correction, not relaxed gate settings.
The targeted White audit addresses the known White risk that a Black-only pilot
cannot establish. Publish receipts and preserve the previous policy for comparison
and rollback before activating the new binding. No fresh user confirmation is
being requested for work already authorized in this decision.

Once accepted, activate the versioned policy and advance the existing Rust
optimizer calibration/comparison and staged repertoire sweep. Reuse accepted
reference runs; do not multiply the entire sweep by obsolete source policies.
The broad sample study is complete; remaining uncertainty concerns integration,
gate-critical choices and practical book effects.

## Progress at decision capture

- [x] User approved the intended production policy and conditional activation.
- [x] Independent confirmation and matched Fishnet follow-up completed and recorded.
- [x] Machine-readable decision contract saved; existing active bindings preserved.
- [x] Targeted White review cases prepared and checked against existing gate counts.
- [x] Rust provider/materializer implementation and tests.
- [x] Stronger-reference/production-path White audit and bounded lookup acceptance.
- [x] Full versioned overlay materialization and bounded independent source check (512 buckets; 16,893 sampled positions passed).
- [ ] Package pilot-ready evidence and implement explicit consumer bindings.
- [ ] Full required pool publication and opponent-input binding.
- [ ] Paired Black pilot and final source-policy activation.
- [ ] Continue native optimizer experiments and main repertoire tuning matrix.

The full overlay materialization and its independent sampled check are complete;
the existing mini pool coordinator continues. Other unchecked stages are prerequisites/planned work, not background
jobs already running. See `eval-production-integration-results-2026-11-25.md` for
the implementation, verification, White audit limitations, current job paths and
resume commands. Active production bindings have not changed.

## Consolidated model requirements before the pilot

Follow [the November 25 model review and implementation plan](repertoire-model-review-and-implementation-plan-2026-11-25.md).
It expands the earlier opponent-continuity prerequisite to include retained weak
observations, complete probability accounting, explicit source/continuation
horizons and endings, native referee precision and policy binding, effective
objective auditing, and support for comparing two evaluation providers over the
same count inputs. The paired request union currently rejects different stored
scores at a shared parent; the new plan includes its reproduction and correction.
No human-model fixes or production activation are claimed by this plan update.
