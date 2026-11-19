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
- [partly integrated] 3. Input and policy preparation. Resumable per-pool preparation CLI uses existing export and independent acceptance gates. The approved OTB formula, full-legal-vector cache and schema-3 overlay are implemented; the native builder consumes the overlay without double smoothing or losing nonmove mass. Production source bindings, OTB split publication and reached-frontier materialization still need integration with the published data.
- [implemented for selected prescriptions; broader correction work remains] 4. Verification preparation. Quality mode separates min_games from trust, reviews selected stored/cloud moves independently, uses a 0.01 score boundary band, preserves deeper evidence and checks engine identity. Shared request union and evidence ordering helpers are tested. Missing/uncertain unselected candidate discovery and retained-graph corrections are not yet implemented; full rebuilding remains the fallback.
- [partly implemented] 5. Extensions and ladders. Hard --allowed-book restrictions produce nested fixed-prescription reductions; extension inheritance checks reject conflicts. Tested exposure/gap/connector/transposition accounting and unresolved-risk bounds, with no four-move cap. The unlimited production continuation-expansion/package-search driver is still outstanding. The old extend_book.py is not the new implementation.
- [partly implemented] 6. Evaluation and reports. Added paired full-key rollout engine with arbitrary horizon, actual histories, pluggable policies, recall callbacks and exact game terminals. Added stable evaluation recipes, ladder audits, eligibility-aware prescription stability, and pilot acceptance contracts. Added full-key, arbitrary-size equal-practice learning profiles as well. Production model adapters, ECO route tables and complete reporting CLI remain to finish.

### Validation so far

- Native build and cargo tests passed in D:/rust-target/sweep-preparation.
- Native/Python differential suite: 252,000 candidate-menu cases, 800 reach graphs, 1,000 assembled graphs, optimizer/collector/stop-table comparisons, zero differences.
- 14 integration tests passed (one opt-in real-engine smoke skipped): horizon, lookahead receipts, full-key builds/scoring, cache reuse, monotone verification and closure.
- Final combined targeted run: 25 tests passed, with the optional real-Stockfish smoke skipped. Separate expected-score, rare-verification, opponent-prior and lookahead-reference suites passed.
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

Read sweep-preparation-usage.md for commands and limitations. The most useful independent coding work still outstanding is the reached-frontier count/model adapter and joint verification coordinator, followed by the unlimited continuation search driver. These are implementation tasks, not things to describe as completed merely because the accounting kernels pass. Production source binding and the Black pilot additionally depend on published inputs. The existing deployment queues have not been modified.
