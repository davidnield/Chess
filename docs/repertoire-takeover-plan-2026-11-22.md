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
| 3. Harden lookup integrity and restart | In progress | Input-bound v2 receipts, single coordinator/lock, isolated workers, exact publication, old-output comparisons, meaningful regression tests. |
| 4. Benchmark and launch corrected lookup | Pending | Measured worker/runtime/memory/disk choice; new pinned release and output generation; restart-safe launcher and progress status. |
| 5. Complete available policy validation | Pending | SF18 source/gate/tail/regret analysis and cloud-gap assessment; OTB exposure audit separating reused and genuinely new evidence; frozen next-test design. |
| 6. Finalize acceptance evidence and capacity plan | Pending | New immutable evidence snapshot including eight previously unlisted analysis files and final menu receipts; verified inventory; measured pool workspace and peak-space limits. |
| 7. Verify closure and hand back status | Pending | Commits, test evidence, running-job status/ETA and resume command; explicit remaining dependencies/approval points. |

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
