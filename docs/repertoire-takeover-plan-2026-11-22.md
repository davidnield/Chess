# Ownership takeover and hardening plan — 2026-11-22

Owner: ChatGPT / Codex. Authorized by David: “I accept your recommendations. Please create a plan and then execute it, tracking progress along the way.”

## Objective and scope

Complete the takeover repairs from the November 22 review: recover menu publication, preserve and commit maintained sources/evidence, replace the unsafe lookup resume path with a verified and benchmarked coordinator, and complete the available evaluation-policy checks before cleanup and pilot selection.

Keep the canonical books and accepted base lookup unchanged. The ChessDB lookup remains a candidate; source-policy adoption, large pool exports, and pilot/full-sweep acceptance retain their existing decision points. No source/archive cleanup until evidence, output validation, and verified archival prerequisites are satisfied.

## Execution checklist

| Step | State | Deliverable / acceptance |
|---|---|---|
| 1. Baseline and quiesce obsolete writer | Complete | Record process/output state; stop only the superseded lookup coordinator and its children; preserve every completed bucket and old release. |
| 2. Preserve maintained sources and repair menu publication | Complete | Versioned source package; unique completion filename; exact 512-bucket verification and atomic receipt; no repeated import. |
| 3. Harden lookup integrity and restart | Complete | Input-bound v2 receipts, single coordinator/lock, isolated workers, exact publication, old-output comparisons, meaningful regression tests. |
| 4. Benchmark and launch corrected lookup | Running | Measured worker/runtime/memory/disk choice; new pinned release and output generation; restart-safe launcher and progress status. |
| 5. Complete available policy validation | Complete (production comparison deferred to accepted inputs) | SF18 source/gate/tail/regret analysis and cloud-gap assessment; OTB exposure audit separating reused and genuinely new evidence; frozen next-test design. |
| 6. Finalize acceptance evidence and capacity plan | Snapshot and capacity checks complete; final snapshot queued | New immutable evidence snapshot including eight previously unlisted analysis files and final menu receipts; verified inventory; measured pool workspace and peak-space limits. |
| 7. Verify closure and hand back status | Final output audit queued after build | Commits, test evidence, running-job status/ETA and resume command; explicit remaining dependencies/approval points. |

## Known findings to resolve

- Menu publication failed because _DONE collides with the _done directory on Windows. All 512 per-bucket receipts exist; exists() is not proof of publication.
- The lookup reuses output without checking changed inputs. A synthetic update from 120 to 321 cp resumes as 120 while a fresh build gives 321.
- Shared global metadata writes make independent copies of the old lookup CLI unsafe as a parallelization strategy.
- The old acceptance manifest verifies its 1,336 listed files, but omits eight later-added OTB/entry-bias files.
- Historical scan pin files were overwritten during production; version 1 was lost. Preserve this limitation and Claude's producer history.
- Corrected OTB test assignments include 495,640 historical-family games and 6,633 new-family games. Correcting ancestry does not make historical model-selection observations fresh.
- The existing process-list restart guard is not an exclusive lock and can miss sleeping coordinators.
- SF18 panel and the eight-bucket pool probe have finished. Claimed 6–8-hour corrected lookup runtime is an estimate to benchmark, not an accepted measurement.

## Verification strategy

Use synthetic regression tests for input changes, interrupted publication, missing/extra buckets, duplicate writers, and resume behavior. Then verify real menu receipt hashes, compare rebuilt old lookup buckets, check exact publication, and preserve reproducible report inputs. Separate reference-search uncertainty and model-selection reuse from true independent validation. Never relabel old receipts with invented provenance.

## Progress log

- 2026-11-22: Plan created before changes. Read Claude's response and current launchers. Preparing a fresh implementation branch and a recorded stop of the old lookup loop.


- 2026-11-22 15:32 PST: Old lookup loop stopped; 167 checkpoints retained. Evidence: D:/chess/takeover-20261122/old-lookup-stopped.json. No production data deleted.
- Publication verifier has four passing synthetic regressions and is running from D:/chess/bin/chessdb-publication-20261122-v1; log D:/chess/takeover-20261122/menu-publication.log.

- Menu publication verification completed: all 512 position and move files matched their original SHA-256 checksums; 4,778,964,781 positions, 2,006,286,156 found, 11,311,142,269 moves, 20 duplicate keys resolved. New receipt: E:/chess/chessdb-menus-20260702/_MENUS.COMPLETE.json.
- 43 focused tests passed, including updated-input receipts, same-size/same-mtime byte changes, interrupted writes, old-output comparisons, and exclusive coordinator lock/crash release. The Windows subprocess test uses the real interpreter so terminating a venv wrapper cannot leave its test child holding the lock.

- Commit eb05705 preserves maintained sources and the checkpoint/publication repairs. New immutable release: D:/chess/bin/chessdb-takeover-20261122-v2.
- One-worker real benchmark: buckets 0 and 1 rebuilt in 55.9 and 52.0 seconds, both byte-identical to the original lookup. Two-/four-worker bounded benchmarks are running sequentially; no full build has been started yet.
- Available SF18/OTB diagnostics completed at D:/chess/takeover-20261122/validation-v2. Historical test ancestry remains potentially exposed; 6,633 new-family test games are candidates for fresh confirmation, not automatically certified unseen. No policy settings changed.

- Bounded benchmark completed: 1 worker ~54 s/bucket; 2 workers 55.8 s for two; 4 workers 62.5 s for four, maximum observed worker RSS 3.36 GB. All eight old/new bucket comparisons match. Selected four workers.
- Full candidate rebuild launched in D:/chess/eval-lookup-p51-cdb2-20261122 using immutable release eb05705, with 167 old buckets to compare. Log D:/chess/takeover-20261122/lookup-full.log; status _status.json; resume D:/chess/restart_jobs.bat. Estimate 2–2.5 hours, to update from sustained throughput. Old scan/probe/lookup launchers retired from restart_jobs.bat.
- Pool workspace audit: arrivals 227,653,557,960 bytes / 9,177,335 files; eight bucket outputs plus stats 13,039,492,639 bytes. Audit completed in 57.8 s. Full materialization still requires a final/spill/reserve capacity decision.

- Cloud verification completed: all 512 files match original receipts; deterministic 16-bucket top-PV comparison reproduced 1,653,277/1,653,277 exact cp/mate/depth matches, no missing rows. Receipt D:/chess/takeover-20261122/cloud-validation.json.
- Two-bucket finalization pilot launched under the original pool release, 2 threads / 2 GB memory / 64 GB spill cap. Output is explicitly a benchmark, never a published population pool.

- Immutable takeover evidence snapshot verified: D:/chess/acceptance/chessdb-takeover-20261122-v1, 2,023 files, manifest b1442e81c43cbd8f408b10454115fc14b0dc84a18b98cd63fefa93eafb2d5131; no unlisted files. Original bundle unchanged.
- Finalization pilot passed exact row counts and schema/ordering checks: 47,759,281 edges, 40,747,691 auxiliary rows; 169 s; 9.677 GB peak output+spill. Full-pool final output projects to ~919 GB, fixed resident footprint ~1.98 TB, scaled trial peak ~3.54 TB before reserve/extra merge passes. No full exports authorized or launched.

- Final acceptance stage implemented and three additional regressions passed (46 focused tests total across the relevant modules). It waits for the existing coordinator, checks all output hashes and old comparisons, and creates a fresh immutable final snapshot. It never adopts a source policy or changes bindings.
- Runtime status is authoritative while this document is a saved snapshot: D:/chess/eval-lookup-p51-cdb2-20261122/_status.json and D:/chess/takeover-20261122/completion-status.json. Final evidence/receipt paths will be written into the latter.
- Pending decisions/dependencies: source aggregation adoption after interpretation of diagnostics; truly fresh OTB confirmation and real-pilot Maia rate after accepted policy inputs; storage/source strategy for large pool exports; NAS archive and cleanup after output acceptance. None has been silently launched or marked complete.


## Execution snapshot — 2026-11-22 16:09 PST

- Branch: codex/takeover-hardening-20261122. Implementation commits: eb05705 and fd3867f.
- Publication and resume repairs are implemented; maintained import sources are committed. 46 relevant regressions passed.
- Full lookup: 60/512 buckets verified at 16:08:50; all 60 comparisons with original outputs match. Four workers, maximum observed worker peak ~3.14 GiB. About 2 hours 5 minutes remaining at that snapshot, plus final output/evidence verification; ETA is provisional.
- Full coordinator PID 9432 (launcher cmd 40500); log D:/chess/takeover-20261122/lookup-full.log. Output D:/chess/eval-lookup-p51-cdb2-20261122. Original 167 checkpoints remain untouched.
- Final acceptance launcher cmd 39604 is waiting for build completion. Its immutable code is D:/chess/bin/chessdb-acceptance-20261122-v1 (fd3867f). Runtime state D:/chess/takeover-20261122/completion-status.json. It will hash all outputs, confirm old comparisons, and publish a new immutable final evidence snapshot. Any build/audit failure is recorded and prevents acceptance.
- D:/chess/restart_jobs.bat resumes the locked lookup then final acceptance. It no longer launches the completed scan, panel, probe, or superseded lookup loop. Both current stages refuse duplicate coordinators through OS locks.
- Candidate adoption, cleanup, NAS archival verification, fresh OTB model confirmation, and large pool exports remain outside the completed acceptance checks. No canonical book or active pool binding has changed.

Read the two runtime status JSON files above for current progress; this document records checkpoints in time.


## Completed acceptance and archive pilot — 2026-11-22 evening

- Lookup build and final acceptance are complete: 512 new buckets verified; all 167 preserved old outputs match. Final evidence: D:/chess/acceptance/chessdb-takeover-20261122-final-b4c90b5d923e. Candidate-only; no active binding changes.
- David requested a bounded lossless Parquet archive pilot before moving ChessDB to the NAS. The existing filtered move menus cannot substitute for a full source archive. No NAS transfer or source deletion has started.
- Pilot completed: 3,000,000 distinct original records across 12 source regions, four layouts/codecs, all source-byte and Parquet round-trip comparisons passed. 129.6 seconds runtime; ~0.745 GB retained artifacts.
- Projected full size: binary keys/values ~0.970 TB (Zstd 9), or original bytes plus decoded chess fields ~1.935 TB (Zstd 9); approximate coverage-sample projections, not guaranteed sizes. Existing specialized database ~1.19 TB.
- Full-scale export needs a native streaming throughput benchmark, NAS capacity check, complete schema/metadata and coverage verification. The Python prototype is for compression/correctness evidence, not the production exporter. No full conversion queued.
- Report: docs/chessdb-parquet-archive-pilot-2026-11-22.md. Artifacts/status/report/checkpoints: D:/chess/chessdb-parquet-pilot-20261122-v1. Both original copies remain untouched.


## Native archive benchmark completed — 2026-11-22 evening

- User approved the native streaming benchmark and requested investigating Rust. Both completed: Rust writer 3.93x faster at Zstd 3, 2.49x at Zstd 9 on identical 3M-record input/schema. Reader remains the known C++/TerarkDB implementation with verified faster FEN conversion.
- Streaming 1/2/4/8-worker arms passed 15M source-record comparisons including repeated ranges; throughput 154k/271k/308k/325k records/s. Four workers are a sensible starting point; full export extrapolates to ~55-60h at the measured 4-8 worker rates, not a scheduled ETA.
- NAS capacity is ample (~24.63 TB free), but measured throughput is low: one-file 5.31 MB/s write, four-file 11.43 MB/s write and 10.32 MB/s read-back; native robocopy 7.25 MB/s. All test hashes matched. At best observed rates ~1.94TB takes ~47h write +52h read-back, before contention. Cause not established; PC's link is 1Gbps and SMB uses local IPs. No network/NAS settings changed.
- Full archive remains unstarted. Next: investigate NAS throughput; implement complete range coverage, both-orientation metadata, native verification, immutable/checkpoint receipts and a bounded spool before full export. Keep both raw copies until verified NAS acceptance.
- Report: docs/chessdb-native-archive-benchmark-2026-11-22.md. Frozen source/code/results: D:/chess/chessdb-archive-native-20261122-v1, including benchmark-source.zip and benchmark-provenance.json. Benchmark NAS files are explicitly labeled and total ~1.75GiB.


- F: alternative evaluated at David's request: 3.1246 TB free, enough for projected 1.94 TB archive with ~1.18 TB remaining. Verified 1-GiB test: 117 MB/s flushed writes and 136 MB/s unbuffered read+SHA-256, roughly 10x/13x the parallel NAS rates. Recommend F: primary archive and optional NAS copy later; NAS speed investigation can leave the critical path if this destination is chosen. Full exporter still unstarted; no original deleted. Results appended to docs/chessdb-native-archive-benchmark-2026-11-22.md.


## Tonight's implementation plan — approved 2026-11-22 19:14 PST

Destination F: accepted. Shutdown target approximately 21:12 PST; do not start an unbounded export tonight.
1. Implement maintained native source range enumeration/chunk reader, Rust writer/auditor and checkpoint coordinator.
2. Bind receipts to source/code/schema; preserve original bytes and both orientations' distance metadata; bounded staging and disk reserves.
3. Test corruption, range/count coverage, duplicate coordinators, source/settings changes and deliberate interruption/resume against an uninterrupted real bounded run.
4. Freeze a release, document tomorrow's exact launch/resume command, and stop bounded work safely before shutdown. Full extraction tomorrow only after these checks pass.
Progress: implementation started. Existing benchmark and evaluation jobs are complete; only WSL cache housekeeping was running at initial check.


## Archive implementation verified — 2026-11-22 19:47 PST

- Production exporter, native writer/auditor and independent archive verifier implemented. Frozen release D:/chess/bin/chessdb-archive-20261122-v4; release manifest SHA256 9efefcee4d55d028fbcb544d73196d4ac03a4d06d05e358d5332605e95b60869.
- All 291 source payload checksums passed. Final release passes 15 focused regressions and a real interruption/resume comparison: retained two completed shards unchanged; resumed four-shard archive equals uninterrupted output byte-for-byte. Duplicate workers, changed settings, same-size/same-mtime corruption and disk reserve tests pass. Source-independent deep audit passes on all 2M test records.
- Approved four-shard production head start launched at ~19:46 PST, coordinator PID 45420, currently rehashing source bytes. Hard limit --max-new-chunks 4; at most 80M new records, then automatic checkpoint stop. No unbounded run tonight. Session 46415; log D:/chess/chessdb-archive-headstart-20261122.log. Actual status: F:/chess/chessdb-archive-20260702-v1/status.json.
- Separate Monday launcher saved but NOT started: D:/chess/run_chessdb_archive.bat. It resumes verified work and runs the independent deep audit after full publication. Existing restart_jobs.bat remains assigned to the completed eval lookup/acceptance work.
- Full handoff, exact commands, stopping procedure and caveats: docs/chessdb-archive-resume-2026-11-23.md. Original sources and canonical bindings remain untouched.


## Final review correction — 2026-11-22 19:56 PST

- Deterministically reproduced a staging-space accounting race: one worker could remove a completed attempt between another worker's `is_file()` and `stat()`, raising FileNotFoundError and unnecessarily aborting the run. The new helper uses one stat per entry and ignores only disappearance; other access errors still propagate. The new regression passes, bringing focused tests to 16.
- Frozen v4 stopped gracefully after verifying all original source files, before exporting any production shards (0 records, 0 receipts). Its source-verification evidence remains in F:/chess/chessdb-archive-20260702-v1. No completed production data was discarded.
- A separate frozen v5 release is used for production, with a fresh output F:/chess/chessdb-archive-20260702-v2 and spool D:/chess/chessdb-archive-spool-v2, preserving strict identity checks. The four-shard head start and tomorrow's launcher use these paths. Never resume the empty v4 archive as the production run.
- Also corrected the head-start batch's exit-code text redirection; the process exit code had been correct, but `echo 0>file` was parsed as redirection and left an empty text receipt.


## Bounded production result â€” 2026-11-22 20:08 PST

- Stopped automatically with exit code 0. Exactly **80,000,000 original records** in **four 20M-record shards** are retained on F: for the next run (2.407 GB compressed).
- Source preflight freshly verified all 291 payloads in 521.0 seconds. Each production shard passed source/writer/native-auditor digest checks and durable F: copy plus unbuffered checksum read-back before receipt publication.
- Coordinator PID 32020 exited. No active attempts, partial F: files or shard staging directories remain. The archive is intentionally incomplete, with no full completion marker.
- Free space: D: 507.1 GB; F: 3.121 TB. Original sources remain intact.
- Exact machine-readable result: D:/chess/chessdb-archive-headstart-20261122-result.json. Resume with D:/chess/run_chessdb_archive.bat tomorrow; no automatic overnight work is armed by these launchers.
