# Sweep preparation: plan and progress

Started November 18, 2026. Branch codex/sweep-preparation-20261118, based on codex/full-key-consumers (34ace88). The adopted numerical recipe is in repertoire-tuning-plan-2026-11-18.md.

## Scope and safety

Develop and test the preparation work that is independent of the unfinished production data and Allie experiments. Keep all current jobs, their pinned code, model caches, and canonical books unchanged. Use small fixtures and isolated build outputs. No automatic launch of the main matrix: a verified Black stricter 2,000-position production pilot is the prerequisite.

## Execution plan

1. **Builder integration.** Merge score-based gates and corrected lookahead into the full-key consumer branch. Verify Python/Rust parity, complete menus, route horizons, evidence receipts, full-key collision isolation and monotone engine quality.
2. **Experiment specification.** Implement a validated, reproducible manifest for the 20 base arms, fixed gate baselines, shared inputs, reduced ladders, extension calibration and staged sensitivities. Explicitly represent dependencies and pending data/backend decisions.
3. **Input and policy preparation.** Implement shared input recipes and a resumable, receipt-checked preparation coordinator; connect the approved OTB probability recipe with full legal move support and separate nonmove mass. Test before materializing production pools.
4. **Verification preparation.** Decouple frequency-based move inclusion from evaluation-quality review; share verification requests and preserve stronger evidence. Instrument objective-weight influence and retain correct fallback behavior.
5. **Extensions and ladders.** Implement the approved engine-only continuation exposure and transposition-aware package accounting, without a four-move cap. Enforce fixed prescriptions and nesting for reduced/extended ladders.
6. **Evaluation and reports.** Prepare model-independent evaluator contracts, stable book identifiers, opening/prescription stability reports and pilot acceptance checks. Defer final Allie/backend selection and production-scale execution until the queued investigations and input publications finish.

Each step records implemented scope, tests, limitations, and remaining production checks below. A unit-tested foundation is not labeled a completed production integration.

## Progress

- [implemented and regression-tested] 1. Builder integration. Resolved full-key/score-gate/lookahead interfaces; restored source moves use the full-key adapter. Lookahead evidence and veto files now persist full keys instead of run-local IDs. Source-only child evaluations use the correct lookup generation. Isolated native build passes Rust tests and the differential suite.
- [implemented; fixture tested] 2. Experiment specification. Reproducible 20-arm manifest, 20 staged follow-up arms, fixed baselines, distinct reduced dependencies, shared pool recipes, extension calibration, evaluation/recall settings and explicit pilot release requirements. No production jobs started.
- [partly integrated] 3. Input and policy preparation. Resumable per-pool preparation CLI uses existing export and independent acceptance gates. The approved OTB formula, full-legal-vector cache and schema-3 overlay are implemented; the native builder consumes the overlay without double smoothing or losing nonmove mass. Selective reached-frontier materialization is now connected and tested (November 19 entry below); production source bindings and OTB split publication remain.
- [implemented for selected prescriptions; broader correction work remains] 4. Verification preparation. Quality mode separates min_games from trust, reviews selected stored/cloud moves independently, uses a 0.01 score boundary band, preserves deeper evidence and checks engine identity. Shared request union and evidence ordering helpers are tested. Reached unselected-candidate discovery and the joint verification coordinator are now implemented (November 19 below); retained-graph corrections remain unimplemented.
- [partly implemented] 5. Extensions and ladders. Hard --allowed-book restrictions produce nested fixed-prescription reductions; extension inheritance checks reject conflicts. Tested exposure/gap/connector/transposition accounting and unresolved-risk bounds, with no four-move cap. The resumable history-aware continuation driver is now implemented (November 19 below); its production efficiency and metric calibration remain outstanding. The old extend_book.py is not the new implementation.
- [partly implemented] 6. Evaluation and reports. Added paired full-key rollout engine with arbitrary horizon, actual histories, pluggable policies, recall callbacks and exact game terminals. Added stable evaluation recipes, ladder audits, eligibility-aware prescription stability, and pilot acceptance contracts. Added full-key, arbitrary-size equal-practice learning profiles as well. Production model adapters, ECO route tables and complete reporting CLI remain to finish.

### Validation so far

- Native build and cargo tests passed in D:/rust-target/sweep-preparation.
- Native/Python differential suite: 252,000 candidate-menu cases, 800 reach graphs, 1,000 assembled graphs, optimizer/collector/stop-table comparisons, zero differences.
- 14 integration tests run (13 passed; one opt-in real-engine smoke skipped): horizon, lookahead receipts, full-key builds/scoring, cache reuse, monotone verification and closure.
- Final combined targeted run: 25 tests run: 24 passed and the optional real-Stockfish smoke was skipped. Separate expected-score, rare-verification, opponent-prior and lookahead-reference suites passed.
- Latest preparation-only run: 11 tests passed, including actual-history rollouts, exact mate scoring, generalized recall and nonempty full-key veto receipts.
- Generated docs/sweep-manifest-2026-11-18.json. Readiness CLI correctly reports waiting-for-publication without starting work.
- Independent empty source headers and the hard fixed-parent reduction both passed through the production full-key builder.
- All work is isolated from the deployed migration and Allie code snapshots.

## Production-dependent checks

- Published explorer and eval lookup receipts and coverage.
- Shared pool export throughput and disk use.
- End-to-end Black stricter 2,000-position pilot with verification closure.
- Final evaluator model/backend choice from Allie evidence.
- Full matrix execution and adoption of any new book.


## Next implementation session

Read sweep-preparation-usage.md for commands and limitations. Those three execution components are implemented in the November 19 pass below. Continue from that entry for the remaining integration and pilot work. Production source binding and the Black pilot additionally depend on published inputs. The existing deployment queues have not been modified.


## November 19 continuation: execution plumbing

The second preparation pass is isolated on the same branch/worktree. No running migration, eval rebuild, Allie job, deployed executable or canonical repertoire was changed.

### Completed and tested in fixtures

- **Selective opponent materialization is connected to the native builder.** `--opponent-policy-config` requests counts and position-only Maia predictions only for reached opponent frontiers. It caches full legal vectors, preserves empirical outcomes and termination/horizon mass, and freezes the used vectors beside each book. Cache compatibility includes source roles/hashes, checkpoint, inference code and actual Maia runtime; identical code snapshots in different output directories can reuse predictions. First-writer cache semantics keep a shared prediction fixed. Worker responses have timeouts. Both colors pass a complete native-builder fixture, and dynamic versus frozen replay is exactly equal.
- **Unselected-candidate quality review is connected.** A reached own-move candidate with missing evidence, a consequential score-boundary uncertainty or inconsistent parent/child scores creates a saved quality-frontier request even if it was not selected. The regression catches a missing `1.c4 e5` candidate alongside the two selected fixture moves. This reviews the collected/reached graph; computational-pruning coverage still needs a pilot audit.
- **Joint verification and its coordinator are implemented.** `sweep.verify` unions comparison requests across frozen books, reuses existing parent/restricted caches, preserves deeper independent evidence, checks legal full-key children, publishes verified engine choices absent from historical counts as shared nominations, and resolves conflicting incoming evaluations through a full-key direct-child search. `sweep.coordinator` snapshots code, checks input/output receipts, rebuilds every participating arm on changed shared evidence and declares quality closure only after an empty request pass on that applied evidence. An undersized provisional book can reach verification; a final book must fill its target budget. A round cap yields an unfinished resumable checkpoint. Corrupted outputs are rejected. The matrix requires the separate pinned Black-pilot acceptance report.
- **The continuation/package driver is implemented.** `sweep.continuations` follows actual histories, uses automatic game outcomes for termination, deduplicates prescriptions by full key, includes connecting moves and has no depth or nonzero reply-frequency cutoff. It considers prefixes and unions of prefixes reaching a shared endpoint, using a documented greedy package heuristic. A work limit is an OPEN checkpoint, not completion. Unknown analysis and holes in an extended master remain unresolved. Checkpoints preserve engine analyses and the exact policy distributions used. `sweep.extend` supplies a cached independent Stockfish top-three adapter and the selective OTB adapter; it reports count support and model-only opponent visits. It produces a prescription artifact, not an automatically adopted PGN/learning ladder.

### Extension metric issue discovered by the new tests

The approved metric is the expected maximum severity **among covered added positions**. With two equally difficult moves on the same route, covering the first saturates that maximum: covering the second earns no additional credit. Removing the four-move cap does not fix this. A repeated-position fixture exposes the problem while separately verifying that a raw transposition is not treated as a draw.

I recommend comparing a metric that measures the reduction in maximum **remaining unbooked** severity. It only credits continuously connected prescriptions, counts the same advantage once, and can justify a package covering both equally difficult moves. Both metrics are implemented and tested. The second is selectable with `--metric remaining-maximum`; the approved default remains `covered-maximum` pending David's answer to the question sent November 19. No production metric switch has occurred. Reports now include known unbooked exposure so the masking is visible even under the approved metric.

The remaining-risk variant uses the entire unresolved route mass as a conservative uncertainty bound. The covered-risk variant retains its original bound. Neither bound certifies global optimality of the greedy package search.

### Validation and practical limits

- The combined targeted run executed **35 tests: 34 passed, one opt-in real-Stockfish smoke skipped**. The separate expected-score, rare-verification, opponent-prior, lookahead-reference and transposition-search scripts passed. A later focused run passed all 22 sweep tests, including the pilot-release preflight check; final shared-verifier regressions also confirm preservation of direct-child evidence when all incoming edges are replaced and publication of unobserved engine nominations.
- New tests cover both-color native policy publication, exact dynamic/frozen replay, warm cache reuse, one temperature transform, missing unselected evidence, shared search deduplication, 20M evidence preservation, receipt corruption, capped-run resumption, applied-overlay closure, seven-move connecting packages, history-dependent repetition and both extension metrics.
- Logs: `scratch/preparation-tests-20261119/`. Additional focused runs are under `scratch/*-current.log`.
- Model and engine responses are controlled fixtures in the new integration tests. No large GPU/Stockfish experiment was started. The production runtime fingerprint and actual checkpoint/backend parity still need the bounded pilot.
- Full graph rebuilds remain the correctness fallback. Incremental retained-graph correction is not implemented and no speedup is claimed for it.
- Selective count queries currently use batched Parquet joins. Measure their I/O cost on the published pools; if it dominates, add indexed/bucketed retrieval before the matrix.
- The continuation adapter currently uses independent 20M MultiPV top-three analysis at visited own positions, with a separate search for a required master move outside that set. That is a conservative correctness prototype, **not** the final efficient hybrid-screening implementation. It must reuse/screen published evaluation evidence and pass a cost pilot before large extension runs. MultiPV node totals are recorded as whole-search nodes, not as 20M per PV.
- Production ECO route exports, model/backend bindings for external evaluation, final extended-book/ladder publication and incremental graph updates remain separate work.

### Next concrete steps

1. Bind verified production explorer/eval publications and the frozen OTB training split; measure one pool export and selective frontier query costs.
2. Run the Black stricter 2,000-position base pilot through the new coordinator. Check complete candidate menus, policy/nonmove conservation, engine quality closure, monotone evidence, gate/lookahead audits, pruning coverage and clean-rebuild agreement.
3. Resolve the extension-metric choice and benchmark a bounded continuation sample. Add hybrid screening/shared tactical evidence before the four-anchor extension calibration.
4. Finish evaluator/backend and population-panel bindings when the Allie investigations publish; connect final reports and ladder/PGN publication.
5. Release the full matrix only after the documented pilot checks pass.


## November 19 third pass: production recovery, selective lookup, completed model evidence

### Prerequisite recovery

The production merge completed at 01:29 PST: 163 months, 512 buckets, 129,759,758,235 output rows, approximately 4,503 GB. Its three sampled source-to-book digests passed. Independent publication verification then failed at 01:47 in DuckDB's 4 GB duplicate/mapping check; the downstream eval launcher correctly stopped. Neither a completed merge marker nor an extant metadata file was sufficient to release sweep inputs.

Commit `4a6a64e` replaces the two simultaneous distinct-value aggregations with sequential exact minimum/maximum conflict checks, using fresh connections. The recovery command explicitly allows 32 GB. This retains both invariants: one EPD per full key, and one high half per low-half/EPD pair. Exact parity, failed-stage resumption and corrupted-receipt rejection pass unit tests. The new immutable release is `D:/chess/bin/ply51-verification-recovery-20261119`; it does not edit any earlier pinned release.

The recovery began at 06:40 PST. It first checks duplicate/mapping invariants for buckets 000, 251 and 511 separately, then repeats the original three digests, book-wide sums, terminal digest, collision audit and sample replay. Each successful stage gets a hashed log/receipt, so a later failure does not discard earlier successes. The complete approved 163-month generation, all 512 publication manifests, code hashes and input metadata are checked. The previous stopped status is retained. Only all-pass completion publishes `training explorer verified`; the original pinned eval workflow then resumes automatically, with its eight-bucket capacity pilot, H:/D:/E:/F: fallback ordering and existing reserves unchanged.

Operational status and receipts: `D:/chess/ply51-migration-20261115/verification-recovery-20261119/`. Top-level stdout/stderr use the same directory name with `.log`/`.err` under the migration root. Running code remains frozen while this branch continues preparation work. See the latest status there rather than treating this historical entry as a live completion claim.

Shared-pool preparation now **enforces completed publication receipts**. Its bindings must pin matching explorer, eval DB and full-key lookup generations, terminal corrections, all 512 lookup bucket entries and completed independent status receipts. The recovery check bundle is pinned when present. Tests reject a still-running verifier and a lookup from another explorer even when their metadata files exist and are freshly hashed.

### Selective count lookup and real model smoke

`keyed_parquet.py` uses existing Parquet row-group low-half bounds to skip impossible groups, then joins on both key halves. Missing statistics retain all groups; equal low halves never substitute for full-key identity. It streams selected groups and uses the native reader when no pruning is possible. No additional full-data index or production-data rewrite is required. The selective builder adapter records groups/rows/compressed bytes selected, and includes this implementation in cache identity.

An OS-warm, sorted one-million-row synthetic benchmark returned exactly the same rows as the old complete-file join. For batches of 1/16/128 keys it selected 1/16/128 of 245 groups and median lookup times were 4.34/5.41/19.81 ms versus 32.63/28.08/35.82 ms. These are microbenchmarks, **not a forecast of production speedup**. The real headstart fixture has only 531 rows and one group; exact results matched, with 4.17 ms versus 3.45 ms. Production pool group sizes, frontier sizes and disk behavior remain to measure. `python -m sweep.benchmark_counts` provides the bounded repeatable measurement.

The actual Maia-3 79M checkpoint loaded through the new explicit-checkpoint adapter and its frozen runtime fingerprint. Seven legal positions (both turns) returned complete normalized vectors and exactly repeated predictions. Startup plus the first batch took 10.03 s, the warm batch 0.047 s. This confirms real runtime/checkpoint plumbing and the single temperature transform; it is not a calibration or model-quality experiment. The detailed benchmark and smoke records are retained in `sweep-runtime-checks-2026-11-19.json`.

The focused regression run passed 24 tests, plus the separate publication-readiness test. This includes both-color native collection/publication, exact frozen replay, warm cache reuse, source identity, complete key matching, missing Parquet statistics, receipt failures, continuation accounting and joint verification/coordinator behavior. The pre-existing full native explorer regression subsequently passed all 16 checks: legal-game extraction and merge in both 64/128-bit formats, full digests/mapping checks, independent GROUP BY comparison, and intentional count/score/high-half corruption. The modified checks also passed every bucket of the compact real headstart book (192,858 rows, 512 buckets; 71 s). Logs: `scratch/verify-book-recovery-tests.log` and `scratch/headstart-compact-mapping-check.log`.

### Completed Allie findings and next model decisions

The backend validation jobs have finished. `sweep-model-evidence-2026-11-19.json` records source paths/hashes, runtime/native identities, metrics and history coverage.

| Panel | Positions / games | Rust minus reference Brier | Measured speedup |
|---|---:|---:|---:|
| OTB | 2,639 / 664 | -0.0000496 | 1.732x |
| Online 1000–1799 | 2,276 / 600 | +0.0000791 | 1.744x |
| Online 1800+ | 2,327 / 600 | +0.0000367 | 1.748x |
| Online 2200+ | 2,331 / 600 | +0.0002768 | 1.750x |

All four pass the agreed noninferiority screens (Brier .002, log loss .005, top-one accuracy .005). Use the validated Rust backend for further Allie work; bind its tested weights, native binary, quantization settings and runtime. Predictions are close, not identical, and quantized inference has some query-order effects. Use a frozen first-writer prediction cache. These tests compare **Allie's two backends**, not Allie versus Maia model accuracy. They do not authorize replacing the construction ensemble's calibrated Maia component.

The separate 256-game rollout/history pilot does **not** establish modal-history equivalence. Position-mode history was available for only 5 of 4,278 White queries and 9 of 4,451 Black queries, all single-observation contexts; zero histories were replaced. The identical position-mode scores resulted from actual-history fallback for almost everything. Exit-mode replaced only 3 White and 10 Black histories. Its score differences (+.00596 White, -.00312 Black) have intervals wider than the .002 equivalence margin.

Actual-history Allie cold rollouts took about 87 minutes for White and 94 minutes for Black (128 games each); Maia took 9.2 and 4.4 minutes, much of that count lookup. Allie used CPU while Maia used GPU, and this pilot used the slower Allie backend. Actual-history storage was only 24,628 packed bytes across 256 games (about 96 bytes/game), whereas inference and the 18.4 GB model dominate costs. The atlas took approximately 89 minutes to prepare and occupied under 1 MB. A modal shortcut has not shown a meaningful storage benefit here.

A direct read-only audit of the completed actual-history cache found 8,711 history/rating contexts but 8,692 position/rating contexts: even a perfect position collapse would remove only 19 predictions (0.218%) in this pilot. Eleven positions had multiple histories. Some pairwise prediction differences were substantial (maximum total-variation distance about 0.587), so history substitution is not automatically harmless. This is a retrospective pilot count, not a larger-sweep cost forecast; it strengthens the case for retaining actual histories while taking the separately validated Rust-backend speedup.

Keep actual sampled histories for further Allie rollout validation. Before a modal shortcut can be accepted, test positions with independently supported alternative histories, report actual substitutions and fallback rates, and compare at least two different frozen books. Fix the backend, query ordering and cache conditions while timing. The displayed Maia/Allie rollout-score differences are model-dependent predictions, not observed human improvement or a move-prediction-accuracy result.

### Still pending before the matrix

1. Finish the production verification recovery and the queued eval DB/lookup rebuild, then bind their verified receipts.
2. Publish the full-key OTB training split and measured shared-pool exports; benchmark count lookup on the actual frontier sizes.
3. Run the Black stricter 2,000-position pilot and its full quality-closure, candidate-menu, pruning and clean-rebuild checks.
4. Extension metric comparison, efficient hybrid tactical screening and production convergence/cost calibration remain unfinished. No 20M-per-visited-position prototype is being queued as a full extension run. The sum-of-unresolved-route bound is conservative and may be prohibitively expensive with full legal reply support; it needs measured convergence, not an assumed production guarantee. The existing metric default is unchanged pending that comparison.
5. Finish evaluator population bindings, final extension/ladder/PGN publication and ECO stability exports. The model-independent rollout and report contracts remain available, but are not labeled an end-to-end released evaluator.


Recovery observation at 06:57 PST: the first production duplicate/mapping stage remains active with no error. Bucket 000 contains 253,427,969 physical rows across 27 files (8.80 GB compressed). CPU time continues increasing. The 32 GB setting is DuckDB's tracked-memory budget, not a hard process working-set limit; the process working set was about 41 GiB. Do not describe this first production stage as passed until its receipt appears. All preparation/native/headstart tests above have completed successfully. The eval worker is queued behind all ten successful recovery stages and has not yet restarted its rebuild.
