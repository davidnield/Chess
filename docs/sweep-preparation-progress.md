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

The initially approved metric was the expected maximum severity **among covered added positions**. With two equally difficult moves on the same route, covering the first saturates that maximum: covering the second earns no additional credit. Removing the four-move cap does not fix this. A repeated-position fixture exposes the problem while separately verifying that a raw transposition is not treated as a draw.

I recommend comparing a metric that measures the reduction in maximum **remaining unbooked** severity. It only credits continuously connected prescriptions, counts the same advantage once, and can justify a package covering both equally difficult moves. Both metrics are implemented and tested. David subsequently approved `remaining-maximum` as the default on November 19; `--metric covered-maximum` preserves the earlier metric for the queued comparison. See the decision entry below. Reports now include known unbooked exposure so the masking is visible even under the approved metric.

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
3. Use the approved remaining-maximum default, run the queued metric comparison, and benchmark a bounded continuation sample. Add hybrid screening/shared tactical evidence before the four-anchor extension calibration.
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
4. Extension metric comparison, efficient hybrid tactical screening and production convergence/cost calibration remain unfinished. No 20M-per-visited-position prototype is being queued as a full extension run. The sum-of-unresolved-route bound is conservative and may be prohibitively expensive with full legal reply support; it needs measured convergence, not an assumed production guarantee. The metric default is now remaining-maximum by David's subsequent approval; the comparison remains queued.
5. Finish evaluator population bindings, final extension/ladder/PGN publication and ECO stability exports. The model-independent rollout and report contracts remain available, but are not labeled an end-to-end released evaluator.


Recovery observation at 06:57 PST: the first production duplicate/mapping stage remains active with no error. Bucket 000 contains 253,427,969 physical rows across 27 files (8.80 GB compressed). CPU time continues increasing. The 32 GB setting is DuckDB's tracked-memory budget, not a hard process working-set limit; the process working set was about 41 GiB. Do not describe this first production stage as passed until its receipt appears. All preparation/native/headstart tests above have completed successfully. The eval worker is queued behind all ten successful recovery stages and has not yet restarted its rebuild.


## November 19 decision: change extension default and queue comparison

David explicitly requested: "Change the default to the one that doesn't overlook a later one but queue the comparison."

- **Implemented default:** `remaining-maximum` in package selection, `Search`, CLI and manifest, all from one shared setting. Readiness output states the selected metric and threshold. Threshold 0.00003, engine-only severity, score gates, distinct connector cost and unlimited depth are unchanged.
- **Queued comparison:** the new `docs/sweep-manifest-2026-11-19.json` contains an `extension_comparison` stage with 24 extension variants: four existing 2,000-position anchors x three thresholds x both metrics. Twelve are the planned default calibration; twelve are old-metric controls. No additional base books. The queue waits for verified inputs, the Black pilot, verified anchors and the extension cost pilot. No worker is launched before these prerequisites.
- **Fair comparison:** share frozen base books, engine evidence and policy caches; use separate metric/threshold checkpoints; compare on a common frozen union of explored continuations, with paired evaluation seeds and a common horizon. Record residual risk, later difficult-move coverage, prescription/cost differences, performance and runtime. OPEN search bounds cannot become a completion claim.
- **Compatibility:** preserve the November 18 manifest and explicit old-metric option. Metric is already part of checkpoint identity, so a switch cannot reuse an old checkpoint silently. Base-arm IDs remain unchanged.
- **Validation:** 15 focused tests pass, including the repeated-position sequence where the new default covers both equally difficult prescriptions and the old metric covers one, rejection of a checkpoint resumed with another metric, all 24 queue combinations and queue-tampering rejection.


## November 19 status audit and verification repair (07:09 PST)

The unpartitioned recovery was alive, but its position-string aggregation grew to approximately 56 GB of working set despite a 32 GB DuckDB tracked-memory setting, with no intermediate receipt after more than 20 minutes. Increasing that setting had not solved the operational problem. The completed explorer itself was not altered or rebuilt.

Commit `e518f3c` partitions each of the three sampled duplicate/mapping buckets into 16 disjoint signed-low-key ranges. Both mapping groupings and the duplicate-key grouping contain that low key, so a conflicting group cannot split across ranges. Boundaries cover the entire signed 64-bit domain; the first range also retains NULLs to preserve unsliced semantics. Existing sorted Parquet row-group bounds allow irrelevant ranges to be skipped. Each range runs in a fresh process with an 8 GB DuckDB budget and receives a separate success receipt. The following three source digests, whole-book sums, terminal digest, collision audit and sample replay are unchanged. All 55 stages must pass before publication.

Four focused recovery tests passed: original distinct/extrema parity; all range-boundary/collision/NULL cases matching the unsliced counts and both mappings; all 48 duplicate-range stages present exactly once; child heartbeat/failure propagation and receipt resumption/corruption checks. A production range checked 15,848,769 rows and both invariants in 24 seconds. After deploying the immutable release at `D:/chess/bin/ply51-verification-sliced-20261119`, the superseded worker was deliberately stopped and the new launcher started at 07:06:57. Only unfinished verification work was abandoned; extraction, merge, caches and earlier logs are retained. The old run has a superseded.json explanation.

Live progress is now under `D:/chess/ply51-migration-20261115/verification-recovery-sliced-20261119/`. `status.json` includes current stage, child PID, elapsed seconds and a refreshed timestamp; the verifier reports its three inner query phases. A live free-space guard preserves the existing D: reserve. `launch.json` contains the exact resume command and automatic downstream dependency. At 07:09 the first five ranges had passed in 17–22 seconds each, the next was running at approximately 3.7 GB working set, and no new error was recorded. This is observed production progress, not yet full publication acceptance.

The original pinned eval workflow will start automatically after all 55 checks pass: eight-bucket capacity pilot, Cloud/Fishnet DB, independent verification, full-key lookup and terminal corrections. Its existing H:/D:/E:/F: fallback order and reserves are unchanged. Its old stopped status records the overnight failure; it is not a second live failure in the replacement queue.

Overall status: training extraction and merge complete (163 months, 512 buckets, 129.76 billion pooled rows, about 4.50 TB); August/September holdout input receipts exist; all three Allie jobs complete and report checksums match; sweep preparation is committed, including the new remaining-maximum default and queued 24-variant metric comparison. Production pools, full-key OTB training publication, Black stricter 2,000-position acceptance pilot, extension cost/convergence work and final evaluator integration remain before the full sweep. No new production repertoire is claimed from preparation tests.

Disk free space measured during this audit (decimal GB): C: 263, D: 1,655, E: 2,785, F: 3,125, H: 2,293. There is no current disk-pressure failure. The eval capacity pilot must still measure whether its projected temporary/output layout fits the reserves before the full rebuild starts.

ChessDB download/readability repair and the offline WSL probe succeeded earlier (931 scored hits in its 1,000-position probe). ChessDB is **not** imported by the current Cloud/Fishnet eval launcher; its integration remains separately tracked. Successful readability must not be reported as completed eval-source integration.


## November 19 fourth pass: explorer accepted, bounded exports, OTB split audit

The explorer completed all **55 independent verification stages at 10:36:52 PST**. The final receipt is `D:/chess/ply51-migration-20261115/verification-recovery-sliced-20261119/verified.json` (SHA-256 `959264dc725d708e43959a0b91362af32d90a0d5524b67c0c0e2daa851f8c31a`). The book metadata remains `729801c10254906fc31c9f89b504d4c981a1f70cbbf2c6546495e477c4dde632`. The original pinned eval workflow started automatically. At 10:53 PST it was progressing through Fishnet source partitions in its eight-bucket capacity pilot, with no parse failures reported. This is not completion of the full eval DB or lookup. The running release and its disk reserve/fallback logic were left unchanged.

### Fixed before large shared exports

`pool_from_book.phase_finalize` previously loaded every bucket's edges into one eager Polars frame, sorted it, then repeated that for the aux table. Min-games-1 exports could therefore exhaust RAM even after all resumable bucket work succeeded.

It now uses DuckDB's external sort under the configured memory/spill settings and writes 65,536-row Arrow batches into an atomic temporary Parquet output. Fixed Arrow types preserve large strings, signed full-key structs, optional fields and recipe metadata. Aux uniqueness compares both key halves, including the boundary between batches; null keys fail. Failures remove only the unfinished temporary file, preserve an existing output and never mark the pool complete. Existing per-bucket resumability and final row-count/meta checks remain.

The focused export tests check exact 64/128-bit parity with the old sorted frame, empty shards/output, schema/metadata, same-low-half twins, duplicate keys across batches and interrupted publication. The existing full-key integration test passes conservation, terminal, independent reimplementation and evaluation gates, deliberate corruption detection, resume and both-color uncut reads. A one-million-row external-sort smoke with a **32 MB DuckDB budget** completed in **2.406 seconds**, actually spilled **94,109,696 bytes**, and passed an independent check of every key and payload. This is a bounded synthetic check, not a full-export ETA or a hard process-RAM cap. Record: `sweep-stream-export-check-2026-11-19.json`.

### OTB split issue discovered; production input deliberately not published

The historical November 13 file contains 4,037,682 selected games and 807,625 test assignments. Its source snapshot differs from the current deduplicated database. Matching historical representative record IDs to current membership yields:

| Diagnostic | Count |
|---|---:|
| Missing representative records / source-name mismatches | 0 / 0 |
| Historical games with a changed current game ID | 16,452 |
| Distinct current games represented by the old selection | 4,022,859 |
| Current families merging multiple old selected games | 14,357 |
| Old selected games in those merged families | 29,180 |
| Current families crossing old test/non-test partitions | **4,690** |
| Old selected games in those crossing families | **9,623** |

Blindly reusing the old labels would let the same newly recognized game contribute on both sides of the train/test boundary. Re-running the old numeric-record-offset replay against refreshed shard files is also not a valid migration procedure. This does not establish that earlier online-pool rankings reverse; it means close comparisons involving an OTB-trained component need rechecking on a clean frozen split.

New `python -m sweep.audit_otb_split` reads the corpus without modifying it, hashes the historical assignment and source manifest, checks source stability, records the old/current identity candidates, and lists mixed test/training families separately as **quarantine**. Two tests verify quarantine rather than training assignment, missing/source-mismatch diagnostics and invalid historical identities. The real audit is `D:/chess/sweep-inputs-20261119/otb-audit/audit.json`; its candidate and quarantine Parquet outputs are hashed there. Current source manifest SHA is `1015009c0f8125e81127b21edff6c8bb354f7e8ccc4655110558caaf455a7ca3`; historical games SHA is `f2d4fb37900611c782e2eb98e9fde34c54ef65fcb69969b229be80983ac1643a`.

**These files are diagnostics, not a published training split.** Matching one representative record does not prove every descendant of a historically split game was found. The audit explicitly records `production_usable: false`. Complete the identity-history reconciliation, retain untouched holdout families, quarantine mixed families, freeze the actual game text/ratings and assignments, then replay to full-key training counts. Keep the frozen calibration/holdout distinctions and never include heldout counts in the opponent prior. Do not infer production readiness from the candidate file's existence or its hashes.

### Next actions

1. Let the pinned capacity pilot, full Cloud/Fishnet rebuild and independently verified lookup finish. ChessDB import remains separate.
2. Complete the OTB identity-history reconciliation and frozen full-key replay; rerun the relevant close OTB prediction comparisons on clean partitions.
3. Once lookup receipts pass, run a bounded shared-export capacity/throughput pilot before the full min-games-1 exports. The finalizer is now ready for that measurement; this turn did not launch a multi-terabyte export queue.
4. Bind accepted shared inputs and run the Black stricter 2,000-position quality-closure pilot before the matrix. Extension cost/convergence, the queued 24-variant metric comparison and final evaluator/publication work remain as listed above.

Final validation for this pass: five focused unittest cases passed (two export cases, one full-key integration case, two split-audit cases). The existing brute-force legacy/compact game-flow suite passed **29/29 checks**; log: `scratch/pool-flow-regression-20261119.log`. The large spill smoke passed separately. At 10:55 PST the eval source scan remained active with no reported parse failures; free space was C: 262.8 GB, D: 1,654.7 GB, E: 2,783.7 GB, F: 3,124.6 GB, H: 2,290.8 GB (decimal).


## November 19 14:05 PST update: Maia selected; full eval rebuild running

David chose to keep Maia for rollout evaluation because the measured Allie cost is unacceptable for the sweep. The approved position-only Maia construction ensemble stays unchanged. `sweep.spec.EVALUATION.backend` is now `maia3-79m-position`, published in the new `sweep-manifest-2026-11-19-maia.json`. Earlier manifests/evidence remain available. Allie follow-ups are deferred and removed from the critical path; no Allie worker is currently running. Actual simulation histories remain necessary for chess rules. Model/runtime/population bindings and production evaluator integration are still required.

The capacity pilot completed at 14:02:29 and the full Cloud/Fishnet rebuild began automatically. Pilot output: 177,463,155 rows, 7.40 GB, all eight sampled buckets. No full eval DB or lookup completion is claimed. Capacity/layout receipts are in `D:/chess/ply51-migration-20261115/evals/`; they choose **E:** for child shards. With a 20% scaling margin and existing 150 GB reserves, forecast allocations are H: 866.6 GB source shards, E: 2,239.0 GB child shards, D: 917.3 GB for output/lookup/spill. At 14:05, free space was H: 2,252.0 GB, E: 2,783.7 GB, D: 1,647.3 GB, F: 3,124.6 GB (decimal). F: remains unused fallback.

The only active heavy chess worker is the full eval builder. OTB ancestry reconciliation/frozen replay, measured shared exports, Black stricter 2,000-position pilot, extension cost/calibration and final evaluator bindings are preparation work remaining; they are not running in the background. The completed split audit is diagnostic only. ChessDB is readable but has not been imported by the active Cloud/Fishnet pipeline.


## November 19: OTB identity/split repair implemented and frozen

David requested fixing the main outstanding issue. Added `sweep.freeze_otb` and `sweep.verify_otb_split` on the isolated sweep-preparation branch. The source corpus and running Cloud/Fishnet release remain unchanged. This supersedes the earlier status that only a representative-record diagnostic was available.

### Complete ancestry, rather than representative matching

The preserved November 13 membership has zero-based source offsets. Its first 35,456,972 records reproduce the historical study's exact source fingerprint `31ed3eb35a272fd83bfdc0722d8326628d871ebff176eea89552bb2fd177d5a2`. The archived identity-code/shard stamp matches, and all 4,037,682 historical representative anchors match after the explicit `rid + 1` conversion. Following every member yields **8,484,681 ancestral source records**, all present in the current corpus with unchanged source/source-file/source-record locators. There are 4,022,859 current descendant families and no actual historical one-to-many split in this corpus. Synthetic tests cover one-to-many splits and transitive mixed-role quarantine nevertheless.

The repaired policy preserves historical test fold 0, validation fold 1, and training folds 2/3/4. It quarantines any connected family spanning different roles; merging two training folds does not remove a training family. This excludes both the previously discovered 4,690 test/non-test conflicts and 3,401 additional training/validation conflicts: 8,091 historical descendant families in total. Quarantine is an assignment, not deletion from the source corpus. Only a currently eligible family with no historical selected ancestor gets a new deterministic fold.

### Published replay input

Directory: `D:/chess/sweep-inputs-20261119/otb-v2-20261119/`.

| Eligible role, 2013-01-01 through 2026-11-19 | Games |
|---|---:|
| Training | 1,509,521 |
| Validation | 503,064 |
| Test | 502,273 |
| Quarantine | 4,828 |
| Total | 2,519,686 |

Of these, 33,427 games have no historical selected-family ancestor. Date, standard-start/variant, positive-rating and under-2000-on-at-least-one-side eligibility account for the smaller total than the all-years historical selection. The existing opening-analysis clock screen is retained. Each row freezes actual current movetext, both ratings and their systems, date, source, starting FEN and assignment. Replay must apply the under-2000 condition to the **mover**; a qualifying opponent does not make both players' moves eligible.

The four frozen Parquets total 905,014,062 bytes; game text/assignments occupy 786,257,428 bytes. The successful freeze took about four minutes with two DuckDB threads and a 4 GB tracked-memory budget, using temporary disk spill. `split.json` identity is `56a6e8dca4b85973964d846fcd3d3483456fc68358f09a1c9196d2a6b3d6dc2f`; receipt SHA-256 is `e5e446803b94581c84c44ecef1d11653648a19fe4ef99b00985165cd77f05536`. Exported ancestry, lineage and assignments are separately hashed. A completed directory cannot be reused with changed source/code.

### Validation and the remaining boundary

The independent verifier reopens the exported files, rederives the complete ancestral-record census and role checks, and compares counts. No historical role crosses into another training/validation/test partition. Small provenance inputs are copied and hashed under `frozen-inputs/`, so verification survives later source refreshes. Source DB/index size/mtime and input hashes were stable during freezing; frozen output hashes provide the durable replay boundary. `verified.json` is the independent acceptance receipt.

Ten focused tests cover source-prefix checks, zero/one-based anchors, lost nonrepresentative records, changed locators, split/merge family quarantine, unchanged historical folds, new assignments and date/rating eligibility, exact date-schema export, null/incorrect labels, source-refresh-independent verification, deliberate output corruption, and existing full-key export parity. The date-schema test fixed an export check that compared Arrow's `date32[day]` spelling against its `date32` input alias; no receipt had been published by the failed first attempt.

The first deterministic replay sample exposed a PGN reader edge case in quarantined game 6575347: leading comments followed by a blank line produced an empty parsed game before its actual legal score. The comment-aware reader now skips only empty, error-free preambles; empty scores and illegal moves still fail. The initial report is retained as `verified-before-pgn-comment-fix.json`, and the current verifier records its own content-addressed code snapshot. This behavior must also be preserved in full replay.

This fixes split identity and freezes the correct replay inputs. It does **not** publish OTB position counts: both receipts explicitly have `training_counts_ready: false`. Next replay the frozen texts into full 128-bit position/move tables, separate roles, exclude quarantine, report legal-prefix failures, retain mover-rating/ply eligibility and verify count conservation. Recheck close OTB-trained-component comparisons on the corrected partitions. These previously inspected partitions are not an untouched new confirmation set.

Final independent replay: **256/256 sampled games passed**, covering 11,808 positions through at most 50 plies, with zero malformed prefixes after the comment-reader fix. This is a sample, not a claim of complete legal replay. Current `verified.json` SHA-256: `dae6c7d365632b343c4418d4ef278023a9bf8a515f41a181030f594016ce4b73`. The full ancestral census and role checks passed again.


## November 19 15:30 PST: full OTB count publication started

User authorized generation and verification of the complete 128-bit OTB move-count tables. Commit `c810d7e` adds a separate Rust replay library, resumable Python export driver and independent count verifier. The active immutable release is `D:/chess/bin/otb-counts-20261119`; the active output is `D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119`. Its hidden launcher starts verification automatically after all exports. Runtime code is separate from the still-running Cloud/Fishnet eval release.

The first stored-ledger test exposed Arrow retaining NUL padding from native fixed-width strings. The preliminary v1 worker was stopped, its output marked superseded/unpublished, and explicit string trimming added before a new recipe/run. No padded tables were accepted. The corrected **full-path 4,096-game pilot** replayed 199,995 moves, including 165,113 qualifying under-2000 mover decisions, wrote all nine exports and passed exact count checks and independent Python replay of 96 games/4,644 positions. Its measured replay/export/verification work took 4.812 seconds after selecting the sample; this is not a large-data scaling forecast. Nine pilot games were quarantined and the other 4,087 parsed successfully. Pilot artifacts are in `D:/chess/sweep-inputs-20261119/otb-counts-pilot-20261119/` and are explicitly not production-usable.

Rust tests passed (four, including reused parser tests). Python coverage includes 100 random legal games, special moves, annotations, malformed-prefix exclusion, full-key transpositions, mover cutoffs, role separation, file round trips and deliberate aggregate-count corruption. An additional repeated-position test confirms keeping ply 51 and separately producing correct 30/50-ply counts without treating minimum ply as a row-level depth filter. Full production acceptance remains pending in this timestamped entry; use the later completion entry and verified receipt when available.


## 2026-11-19 17:30 PST: full 128-bit OTB counts independently accepted

Completed the authorized full replay, nine exports, independent verification and real consumer check. Accepted directory: `D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119`. `verified.json` is the count-level acceptance receipt: `training_counts_ready: true`, SHA-256 `2c19ff37e7bfb3bc69359000cd968fd0b71e3cad3267f135373c87bcfe6be5d9`. The earlier split-level receipts remain immutable and deliberately still say counts are not ready; they describe the boundary before this producer.

The complete census contains 2,519,686 frozen eligible games. Valid games: 1,509,520 training, 503,064 validation and 502,273 test. All 4,828 quarantined games contribute zero. One training game (2816742, lumbra-otb) contains illegal SAN `22. xc6`; both native and independent Python replay reject it. Its entire requested prefix is excluded, rather than guessing a pawn origin or counting an early ending.

The audit ledger contains 122,761,289 legally replayed moves through played ply 51. Of these, 101,000,635 qualify by **the moving player's rating below 2000**. Higher-rated opponent moves remain in the ledger only for game-flow audit; they are not added to these published counts. This game selection is not a representative 2000+ OTB corpus.

| Role / export | Distinct aggregate rows | Qualifying move observations |
|---|---:|---:|
| training / by-ply-band 1-51 | 42,814,696 | 60,616,548 |
| training / ply 30 | 17,763,076 | 36,945,738 |
| training / ply 50 | 40,251,755 | 59,593,882 |
| validation / by-ply-band 1-51 | 14,897,056 | 20,209,870 |
| validation / ply 30 | 6,509,568 | 12,313,808 |
| validation / ply 50 | 14,036,746 | 19,868,483 |
| test / by-ply-band 1-51 | 14,870,273 | 20,174,217 |
| test / ply 30 | 6,494,591 | 12,293,451 |
| test / ply 50 | 14,009,227 | 19,834,000 |

Rows mean distinct position/move aggregates within that role/export; observations include repetitions and multiple games. The depth exports overlap and must not be summed. All counts retain frequency-one moves. The master retains ply and the below-1500, 1500-1799, and 1800-1999 mover bands. Other depth caps and band unions can be aggregated from it; thresholds within a band require the exact-rating ledger. A pooled row's ply is its minimum occurrence ply, not a basis for reconstructing a shallower export.

Full verification checked every frozen game disposition, every ledger count/ply chain and mover/role assignment, artifact hashes, every master count, and every pooled count, including missing/extra/duplicate rows. Separately, independent Python replay checked 768 accepted games / 37,588 stored positions with zero mismatches. During generation the native cross-check sample covered 1,848 games / 89,715 positions. These samples are separate and may overlap; this is not a claim of independent Python replay of every game.

`consumer-check.json` passed exact signed `struct<lo:int64,hi:int64>` and int64 count schema checks on all nine files, then used the real selective policy reader on all six pooled files. It checked 811 legal SAN/UCI/full-child-key rows from early and sampled later positions, retained known stored positions, preserved count mass through the actual `frontier.vectors` policy adapter without fabricating outcomes/nonmove mass, and returned no false match for the deliberately wrong high-half key. The checker and its consumer-code dependencies are archived and hashed. `environment.json` pins runtime versions and executable/release identities.

The nine exports occupy 10,769,852,631 bytes (10.77 GB decimal); the resumable ledger/disposition Parquets occupy 5,762,548,039 bytes (5.76 GB). Replay/export ran from 15:29:44 to 16:21:11 (about 51.5 minutes); independent count verification reports 4,071.0 seconds. The run used two DuckDB threads, an 8 GB tracked budget and disk spill alongside the continuing Cloud/Fishnet rebuild. No engine/model inference or source-corpus modification was needed.

The implementation is committed in `c810d7e`; this completion entry also records the added ply-51/repetition regression and consumer checker. Rust replay tests passed 4/4; focused Python count tests passed 4/4, with the prior split/export checks unchanged and passing. The initial padded-string output remains explicitly unpublished, and the 4,096-game pilot remains an engineering fixture.

Next use only the accepted **training** export for construction-policy binding. Recheck close OTB-component prediction comparisons on the repaired partitions; these already inspected validation/test roles are not a fresh confirmation set. Player/event-cluster uncertainty still needs an identity-aligned frozen mapping. Shared online pool exports, final eval/lookup acceptance, policy/evaluator integration and the Black stricter pilot remain separate sweep prerequisites; this completion does not launch the full book matrix.


## November 19: horizon-budget and legacy Maia cache corrections

The approved horizon comparison remains **30 versus 50 game plies at independent B=2,000**, crossing both gate settings and colors. The previous generator accidentally used B=1,000 and its 1,000-position controls. The generator now selects the matching 2,000-position ply-30 masters, and semantic validation rejects wrong budgets/controls or additional objective changes even when recipe hashes are recomputed. The current manifest is `sweep-manifest-2026-11-19-maia-v2.json`; the earlier Maia manifest remains unchanged for audit and fails current execution validation. Only the four horizon IDs/dependencies change. The 20 primary arms, 16 other sensitivity arms, 24 extension variants, gate settings, and Maia decision are unchanged.

The legacy `convertibility_eval.MaiaCache` reader now ignores old `part-*.tmp.npz` files without deleting them. New writes use unique temporary names outside the completed-shard glob, close and flush/fsync before atomic publication, and retain pending answers for retry on failed writes/renames. Readers close the NPZ and its file handle even if opening fails. Corrupt completed shards still fail; finished old shards, exact probability order/values, rating-pair identity and full-key reuse are preserved. This does not redesign the model/runtime identity contract or modify the live shared cache.

Validation: four focused pre-fix regressions reproduced the two defects; the post-fix suite passed **25/25 tests, no skips, in 38.516 seconds**. Coverage includes interrupted/failed writes, concurrent readers/writers, old temporary files, corruption rejection, wrong/rehashed horizon recipes, and native full-key builder/referee cache reuse across different run-local IDs. The current manifest is reproduced exactly; attempts to overwrite the historical manifest are refused. Evidence and exact commands: `docs/repertoire-review-fixes-verification-2026-11-19.json` and `docs/evidence/review-fixes-20261119/`. No production worker, pinned release, or canonical book changed.


## November 19 (evening): fixes from Claude's review

Source: `docs/claude-review-of-codex-2026-11-19.md`; plan and progress in
`docs/claude-review-fixes-plan-2026-11-19.md`. Branch `claude/review-fixes-20261119` from `2ad11c8`.
The current manifest is **`sweep-manifest-2026-11-19-maia-v3.json`**. Every arm ID changes,
because the recipe gained the Black polish and the request-scope settings. v2 and earlier stay as
archived records, and current validation rejects them.

- **Black solver.** DP + match-distinct + **polish** at match-inflate **1.5**: the validated recipe
  of B30o5, the 0.30/0.60/1.00 screens and the full-menu books. `builder.command` refuses an
  unknown solver label.
- **Quality requests are scoped to the chosen book.**
  - `sweep.candidates.requests` now runs after selection, on the book's **policy reach** (booked
    positions and where it stops). It examines parents at or above `verification.min_reach`
    (0.0005), ranks them by reach, and keeps at most `verification.max_parents` (2,000) per round.
  - The build meta's `quality_frontier` records requested, deferred, below-threshold and
    off-policy counts, plus reach mass and reasons. Deferred parents are requested again next
    round, so closure still means none above the threshold remain.
  - The old whole-graph scan would have requested roughly 75–83% of about 3 million own positions.
- **New request reasons:**
  - `parent-above-children`: the parent's stored score exceeds every child's by more than the
    band. The relative gate measures from the parent, so this can empty the menu; the search
    nominates the engine's best move.
  - `unverified-rare-reference`: a thin best child, or a parent agreeing with it, cuts a move
    that passes against the trusted children.
- **Recipe-to-data binding.**
  - `sweep.bindings.check_arm`, called from `coordinator.preflight`, holds the bound candidate
    pool, the weak pool, and the ensemble sources or frozen overlay to the recipe's labels and
    depth.
  - A control overlay must declare its `reply_pool`. Its `source_pool` is the outcome pool.
  - The builder refuses `--max-ply` above a full-key pool's recorded cap.
  - `build_weak_edges.py` requires `--book` and `--max-ply`, and refuses full-key inputs.
- **Ensemble details.**
  - The strengths and discounts are read from the recipe. The defaults are the approved values,
    so output is identical.
  - An endings-only board keeps one pseudo-continuation when broad or OTB players moved on.
  - The selective cache identity moved to implementation 3.
  - The build meta reports model-only opponent replies and those without a child eval.

Tests: `_test_review_fixes_1119.py` (9), plus updated `_test_sweep_coordinator.py` and
`_test_full_key_book.py`. With the existing sweep/full-key/gate/horizon suites that is 51 passed and
1 opt-in skip. `_test_rare_verify`, `_test_book_builder_rs`, `_test_budget_eval_loss`,
`_test_opponent_prior`, `_test_lookahead_floor`, `_test_pool128` and `_test_docs_paths` all pass.
None of this was run on production data.

## November 19, 22:16 PST: user-requested overnight stop

Stopped the eval worker and all four known recovery/eval wrapper processes; no replacement was launched. Preserved 164 source receipts, 47 child-routing group receipts and 104/512 eval DB buckets (2,307,009,240 rows, 96.20 GB). All completed file sizes/footer row counts and three sampled SHA-256 hashes passed the post-stop audit. Eight partial bucket files remain for native resume; no source or completed output was deleted. The accepted explorer and OTB counts remain valid. Eval status explicitly says stopped; upstream explorer status remains verified.

The deployed full coordinator has a resumption gap: it repeats independent eval verification before resuming lookup, and the verifier has no internal durable checkpoints. This is documented for a new immutable deployment before the next stage; it was not changed in tonight's stop. The morning handoff includes a guarded join-only recipe preserving the old native generation and reserve checks, then verification/lookup and the conditional pool/pilot/sweep queue. See [morning resume handoff](repertoire-morning-resume-handoff-2026-11-20.md) and [checkpoint evidence](repertoire-morning-resume-evidence-2026-11-20.json). A separate Claude dashboard-copy task was observed and left alone; this stop did not shut down the PC.
