# Mini-PC worker acceptance and first project pilot — 2026-11-23

The user authorized suitable independent jobs within the existing repertoire project plan. The mini is usable as a separate worker. A bounded, real-data native audit passed through its persistent launcher, with versioned inputs, verified hashes and collected results. It is healthy and idle after this pilot.

## Connection and operational contract

- SSH alias `chess-mini`, host PICOBYTE / 192.168.68.64; standard account `chessworker`. Use Windows OpenSSH, the existing agent and pinned host verification. Never copy private keys, elevate this account, weaken package permissions or run the administrator installer for ordinary jobs.
- Main client: `C:/Users/David/Downloads/chess-mini-launcher-v1/Invoke-ChessMini.ps1`; guide: the sibling `README.md`. The guide has stale initial provisioning/PENDING text; the successful later boot receipt and current Health establish that provisioning and automatic startup now work.
- Latest supplied reboot receipt: `C:/Users/David/Downloads/chess-mini-followups-2026-11-22/boot-verification-2026-11-23.json`. It shows one healthy, non-elevated dispatcher after a real reboot, without manual startup. Today's live checks agree: one held worker lock, fresh heartbeat, empty queue and unpaused state.
- Mini workspace `C:/chess-worker`, protected package `C:/chess-worker/launcher-v1`. Persistent jobs must use Submit/Status/Logs/Collect, not background children of a transient SSH connection.
- One active job; default 16 GiB aggregate committed-memory limit, 75% CPU cap, 24-hour timeout and 50 GiB disk reserve. Maximum configurable memory 24 GiB, CPU 90%; explicit timeout_seconds=0 is needed for longer jobs. Set application threads and hash memory independently. Retrying interrupted jobs is explicit and application checkpoints remain the application's responsibility.
- Collect copies the job directory only. Use CHESS_ATTEMPT_DIR for results and CHESS_JOB_DIR for deliberate stable checkpoints. Inputs elsewhere must be managed separately. Stage immutable/versioned inputs and verify hashes before submission and again inside the job. No shared-database writers across PCs.

## Current hardware and runtime checks

- Ryzen 7 8700G, 16 logical processors confirmed; Windows PowerShell 5.1.26100.9818. The user reports 8 cores, Radeon integrated graphics and no NVIDIA GPU.
- Usable physical RAM reported by GlobalMemoryStatusEx: 31.12 GiB, with 27.51 GiB available during the pilot.
- NTFS C: volume approximately 4.00 TB total; 3.891 TB free after staging. Separate hardware: this does not enlarge the main PC's RAM.
- No python, python3, py, rustc, cargo or stockfish executable was found on the worker PATH. This is a PATH/dependency check, not a claim that every possible installation directory was searched. The current workload requires none of those runtimes: the precompiled standalone Rust auditor executed successfully. WSL's executable exists, but a functioning distribution was not established or needed.
- WMI/CIM hardware access was denied under this standard SSH account. Registry, DriveInfo and GlobalMemoryStatusEx provided the needed checks without permission changes.

## Representative real-data pilot

Input: one existing production shard, `F:/chess/chessdb-archive-20260702-v2/parquet/r003/c000000.parquet`, containing **20,000,000 records / 595,547,561 bytes**. Read-only verification only: no archive receipt was changed and this supplementary check does not replace the final archive-wide verification.

Executable: frozen `D:/chess/bin/chessdb-archive-20261122-v5/chessdb-archive.exe`, SHA256 `60dbc42906853544ac13490e5ad5ba2ccea63a81c855da317938bc817bf3b597`. Remote versioned inputs: `C:/chess-worker/inputs/chessdb-audit-pilot-20261123-v1`. No Python or Rust installation was needed.

Job `330baff9c6d94cdebf38cdd800ee3805`, request `9bbc198b6a124c7e9e85c8af8b9acdea`. Manifest requested 16 GiB / 75% CPU / 600-second timeout. It ran as PICOBYTE\chessworker through Task Scheduler's dispatcher after submission returned; status and results were accessed using subsequent SSH connections. Result files stayed in its attempt directory and were collected through the supported client.

| Measurement | Observed |
|---|---:|
| Shard plus 9.14 MB executable upload, including SSH overhead | 5.81 s |
| Approximate upload rate | 104.0 MB/s |
| Mini native deep audit | 14.35 s |
| Same audit on main PC | 22.56 s |
| Mini whole scheduled job, including input hashing/setup | 16.22 s |
| Result collection with before/after status checks | 2.63 s |
| Peak mini job committed memory | 93.3 MiB |

Mini, current local audit and original source receipt agree on **all seven** compared fields: row count, byte count, physical SHA256, first/last keys, original key/value digest, and full decoded-stream digest. The remote pre-submission and in-job file hashes also match the local input manifest. The mini's stderr is empty and launcher exit code is zero.

This is one warm/recently transferred shard, not a sustained disk or full-export benchmark. Main and mini storage/cache conditions differ. The audit itself is about 1.57x faster remotely here; adding upload, scheduled-job and collection times gives approximately 24.7 seconds, before separate preflight/submission overhead. That is comparable to the 22.6-second local audit. Do not advertise a wall-time speedup for a single offloaded shard on this evidence.

## Work allocation within the existing plan

1. **Best next use: independent Stockfish reference/gate/rare-move verification batches** when their input positions and source policy are ready. Tiny position lists and result receipts amortize network overhead, with explicit engine/thread/hash settings and the exact staged binary recorded. Stockfish still needs to be staged and benchmarked before assigning these jobs; this pilot is not an engine-speed benchmark.
2. **Possible: bounded CPU aggregation, lookup-validation or finalization buckets**, each with immutable inputs and isolated outputs, if measured working set and spill fit the mini's limits. The 4 TB NVMe may help the pool-export storage plan, but the previous full-pool peak estimate must be recalculated for the exact remote inventory. Millions of tiny arrival files should be packed into independently hashed bundles rather than copied one by one. Large pool exports and candidate/source adoption keep their existing prerequisites; none was launched here.
3. **Useful supplementary role: independent archive-shard audits** while the main PC does other work. Batch manifests and keep resident inputs where possible; return small reports. A future sustained multi-shard pilot would be needed before changing the full archive verification workflow.
4. **Keep the current ChessDB exporter on the main PC for now.** It uses the local ~1.19 TB verified TerarkDB source, WSL native reader and F: destination. Moving that whole setup is not justified by this single audit test. At today's payload transfer rate, one 1.19 TB copy alone extrapolates to roughly 3.2 hours, excluding verification/setup and sustained-throughput differences. No production exporter, source dataset or large pool has been moved.
5. Do not assign existing CUDA-dependent Maia jobs without a separately benchmarked supported runtime. The mini has no NVIDIA GPU; its CPU/Radeon is not an automatic substitute.

No new substantive source-policy or repertoire choices were made. Main archive status at this check remains `stopped-at-checkpoint`, **80M records saved**, with the full-run launcher prepared. The mini ended healthy, idle and unpaused, with no queued job. The user's authorization to use it persists; check Health and dependencies again before the next workload.

## Evidence and follow-up

Main artifacts: `D:/chess/mini-pilot-20261123-v1`, including immutable staged inputs, job manifest, transfer/preflight receipts, current-main audit, submission/status, collected attempt, summary and checksum evidence manifest. The payload remains in the versioned remote inputs directory (~605 MB including executable); no automatic cleanup was performed.

The unrelated minor installer preflight correction is still staged/unapplied as described by the side session. It does not block jobs; the administrator installer was not run. No accounts, permissions, network/router settings or dispatcher configuration changed.
