# ChessDB speedup implementation and controlled tests — November 23, 2026

User authorized implementing all three recommendations: parallel independent verification, a six-worker conversion test, and Zstd 3 versus 9 testing. Do not interpret this as permission to skip verification or delete original data.

## Implementation

`python/chessdb_archive_verify.py` now supports optional `--deep --workers N --scratch DIR --reserve-gb 250 --staging-gb 24 --progress FILE`. Its default remains serial. Parallel mode reads archive files sequentially with Windows unbuffered I/O, hashing and staging the exact bytes to NVMe. At most N staged files are in flight. Native auditors process those copies concurrently; both logical digests, full row/key coverage, physical checksums, file sizes, provenance and receipt sets must all match. The native auditor is pinned to the archived producer's executable hash. Failed/interrupted scratch is retained, counts against the staging cap, and does not constitute a verified checkpoint. Full verification restarts from scratch if interrupted.

The optional `copy_to` in `chessdb_archive_windows.unbuffered_sha` uses exclusive creation, preserves unaligned EOF handling, and flushes the staged bytes. Existing callers retain the previous behavior. No active exporter release was edited.

Independent verifier release: `D:/chess/bin/chessdb-verifier-20261123-v1`; its `verifier-release.json` pins the files. It is a verifier-only release, not a new exporter. The native binary is unchanged from the v5 exporter.

Validation: 14 new native verifier tests and 16 existing archive regressions passed. These cover serial/parallel result equivalence, arbitrary original bytes and both distance fields, same-size/same-mtime corruption, missing/extra files, mutated receipts, coverage gaps, resource bounds, overlapping paths, worker failures, retained failed staging, and refusing to overwrite existing staged files. Evidence: `D:/chess/archive-speedup-20261123/verifier-tests-v1/results.json` and `archive-regressions-v1/results.json`.

## Controlled benchmark and restart

Controller: `D:/chess/archive-speedup-20261123/archive_speedup.py control`, hidden PID 1092 at launch. Its frozen script, logs, current `status.json`, inputs, arm results and decision receipt are in that directory. Source exporter remains `D:/chess/bin/chessdb-archive-20261122-v5`.

Production stopped gracefully at 09:08:56 PST with **1,590,712,400 records** retained. The exact existing receipt hashes are in `checkpoint-baseline.json`. The old launcher is preserved as `run_chessdb_archive.before.bat`. Source copies and completed Parquet shards remain untouched.

Benchmark sequence:

1. Same four accepted 20M-record shards, serial versus 2 versus 4 audit workers, with all physical/logical checks. These are timing checks, not a full-archive acceptance receipt.
2. Same twelve source-region prefixes at 5M records each, using the actual production reader, writer, native audit, durable F: copy and unbuffered read-back. Four combinations: workers 4/6 crossed with Zstd 9/3. A second repetition reverses the order. Every arm must produce identical logical digests, counts and boundaries for each region. All per-arm outputs are retained under `F:/chess/chessdb-speed-benchmark-20261123-v1`; they are not production archive shards.
3. Resume production automatically. Select six workers only if median throughput exceeds four by more than 5% at the selected compression. Select a new Zstd-3 archive only if projected remaining duration, including repeating the already completed portion, improves by more than 10%, and projected size plus a 10% margin still leaves 500 GB free. Otherwise retain Zstd 9 and reuse all existing production shards. Any new archive has its own identity and spool binding; old completed shards are preserved in place.
4. The launcher queues the tested independent verifier after full conversion completion. A benchmark error resumes the unchanged v2/Zstd-9/four-worker archive only if the production stop is confirmed clean; unresolved production failures remain stopped for investigation.

The helper's estimates compare sampled workloads, not a guaranteed census-wide runtime. Startup costs, source-region complexity and later disk rates can differ. Record actual production throughput after the chosen restart before claiming a new ETA.

## Coordination with mini pool pilot

The mini's pool aggregation continues independently. Its main-PC validator was waiting for the remote job and is temporarily deferred to avoid adding CPU/I/O work during the controlled archive benchmark. It resumes automatically when the benchmark controller records `production-relaunched`.

Deferred validator wrapper: `D:/chess/archive-speedup-20261123/resume-pilot-validator.ps1`, hidden PID 40592 at launch; logs `D:/chess/mini-pool-20261123/validation-after-benchmark.stdout.log` and `.stderr.log`. It invokes the unchanged `mini-pool-validation-20261123-v2/validate_mini_pool_pilot.py`. No mini job was cancelled for these tests, and production pool buckets remain gated on exact pilot acceptance.

## Current progress

Implementation and regression tests complete; controlled audit benchmark running. Check live `status.json`, `audit-arms.json`, `conversion-arms.json`, and `decision.json` rather than treating this snapshot as current job state. Do not independently resume the archive while this controller holds the benchmark stop request.

## First measurements and corrected conversion harness — 09:16 PST

The audit benchmark passed on the same 80M records: serial 117.125s, two workers 60.219s, four workers 43.781s (2.675x versus serial). Four-worker projection is about 9.73 hours for 64B records, versus 26.03 hours serial; regional variation and disk rates still matter.

The first conversion harness failed before producing any benchmark shards: each isolated arm lacked its required copy of `source-verified.json`. The source guard rejected it. The controller resumed production automatically with its original four-worker/Zstd-9 settings; no completed production data changed. The corrected harness passed a 10,000-record Zstd-3 full-pipeline smoke test, including native audit and durable archive read-back.

**Current controller:** `D:/chess/archive-speedup-20261123-v2/archive_speedup.py control`, hidden PID 3188; use this directory's `status.json`, logs, arm results and decision from now on. It reuses the successful audit benchmark, waits for the required production source recheck and the mini's main-PC exact comparison, then checkpoints and runs the worker/compression matrix. Corrected per-arm outputs go to `F:/chess/chessdb-speed-benchmark-20261123-v2`. The first harness and its failure evidence remain preserved. Production launcher PID after the first automatic resume was 35168, coordinator 28752.

The mini aggregation succeeded in 1,538.622 seconds (25.64 minutes), peak whole-job commit 19,001,532,416 bytes. Its exact comparison is underway; do not confuse successful aggregation with accepted equivalence.

## Controller v3 and accepted mini pilot — 09:23 PST

The v2 controller's launcher guard encountered the project's virtual-environment Python wrapper between the coordinator and cmd.exe and stopped before issuing its stop request. The corrected controller identifies exactly one cmd.exe ancestor whose command line names the archive launcher. Current control script/status/logs: **D:/chess/archive-speedup-20261123-v3**, PID **39280**. It reuses the completed audit benchmark and writes conversion arm outputs under `F:/chess/chessdb-speed-benchmark-20261123-v3`. Source preflight passed all 291 files; the second graceful stop is waiting for already-started completed-shard rehashes to finish. Neither setup error bypassed a guard or changed a completed production shard.

The mini pilot is now accepted: both full tables match exactly in both directions and all non-timing statistics agree. Bucket 000 production job `7ac943ca7fbc48edb076f10a3b4c478d` started at 09:20:10 PST with the same 24 GiB job / 16 GB DuckDB limits. It is independent of the main CPU benchmark. Full pilot evidence and output paths are in `docs/mini-pool-pilot-2026-11-23.md`.

## Confirmed exporter race and immutable v6 deployment — 09:30 PST

The v3 concurrent benchmark exposed a real v5 production bug: `staged_bytes` caught a disappeared file at `stat()`, but its multi-level `Path.glob` could itself raise `FileNotFoundError` when another worker removed a completed attempt directory before descent. Eight 5M-record benchmark shards had already verified; the affected attempt aborted safely. This is a concurrency/accounting failure, not a digest mismatch or lost accepted data.

Fixed in `python/chessdb_archive.py`: enumerate the spool root once, then scan each explicitly matched attempt with a narrow disappearance handler; file disappearance remains benign, while root/permission/other errors propagate. Eighteen archive regressions passed, including the new directory-enumeration case. New immutable exporter: **D:/chess/bin/chessdb-archive-20261123-v6**. Its release manifest and source ZIP were regenerated; native reader, writer, auditor and source-verification code are unchanged. Do not mutate v5 in place.

Stopped the fallback's repeated source preflight (before it could start any shard conversion), verified no corresponding WSL source checker remained, and retained the last complete atomic source receipt. All 1,590,712,400 old production records remain in v2. Its mutable status now explicitly says `stopped-for-exporter-race-fix`; this is not a new acceptance marker.

**Current and only active benchmark controller:** `D:/chess/archive-speedup-20261123-v4/archive_speedup.py control`, hidden PID **23984**, started 09:29:49 PST. It writes arm outputs under `F:/chess/chessdb-speed-benchmark-20261123-v4`. Earlier controller versions have exited. The benchmark now uses v6 in every arm and reuses the successful independent-audit scaling result. A failure holds with the stop file for investigation, instead of repeatedly restarting source preflight.

Because producer code is identity-bound, production will restart into a **new** archive/spool generation `...-v6-zstd3` or `...-v6-zstd9` after the benchmark. The old 2.5% is preserved separately, not silently rebound to different code. Both candidate projections now include a full 64B-record run; choose Zstd 3 only for a >10% duration improvement and sufficient space including 10% size margin and the existing 500 GB reserve. Worker selection still requires >5% median gain to choose six over four. Four audit workers will follow the completed archive.
