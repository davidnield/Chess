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
