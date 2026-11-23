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
