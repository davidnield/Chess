# Mini pool transport recovery — November 24, 2026

## Incident and impact

The status check around midnight found the mini idle. Bucket 064 had completed, been collected and accepted at about 20:04 PST on November 23, but the coordinator was stuck submitting bucket 065 from 20:05. The top-level status still said `collecting` because it had not published a new status before Submit. The idle interval was approximately four hours. No accepted output was lost and no chess computation needs repeating.

The old coordinator used `subprocess.run(..., capture_output=True, timeout=120)` to invoke the PowerShell client. Its PowerShell process (35332) had exited, but SSH child 44372 remained alive holding inherited capture handles. Python's timeout cleanup was waiting for pipe EOF. Thus the timeout did not bound wall-clock waiting. A new regression safely reproduces this failure under an independent outer deadline.

## Fix and verification

Maintained helper: `python/mini_worker_transport.py`; regression suite: `python/_test_mini_worker_transport.py`.

- Invoke Windows OpenSSH directly, retaining the configured `chess-mini` alias, existing agent and host verification.
- Use file-backed stdin/stdout/stderr, a single-line encoded request, explicit connection/keepalive limits, and process-tree termination on timeout. No PowerShell pipeline/EOF dependency or captured descendant output pipe remains.
- Continue calling the existing standard-user `Jobs.ps1` launcher. No remote package installation, privilege or worker-limit change.
- Preserve terminal-attempt checks before and after collection and preserve partial copies on failure.
- Never automatically retry an unknown Submit. Reconcile its intent before any resubmission.
- Publish `submitting` and `verifying-checkpoints` statuses so the displayed phase tracks actual work.

Seven tests passed: large stdout; complete file-backed input; bounded timeout with inheriting child cleanup; reproduction of the legacy capture-pipe hang; no automatic Submit retry; invalid job ID rejection; changed collection attempt rejection. Read-only Health/List and a real recovered Submit succeeded using the new transport.

## Reconciliation and resumption

Stopped only old main-PC coordinator processes 44632/41632 and orphan SSH 44372. The mini's worker and the independent ChessDB run were left running. A remote CIM process query was unavailable to the standard account, so the reconciliation used permitted `Get-Process` information instead; no privileges were changed.

Confirmed that the old remote submission bootstrap no longer existed, no manifest/job for production bucket 065 existed, and the mini was healthy and idle. Preserved the old intent in `D:/chess/mini-pool-20261123/recovery-20261124/b065-before.json` and the complete reconciliation evidence in `reconciliation.json` there. The already-prepared manifest checksum was unchanged.

Recovered bucket 065 was submitted once at **00:04:47 PST**:

- Job: `8b593bf2a7044a308913fbbe1893e9a4`
- Request: `d1b123a619c44218acc4176221586088`
- Normalized manifest SHA-256: `7567d481c6e7e8a0f8bb04c0a517c8eb32e15c12be967d565a16bef2cb5c1e31`

The repaired queue is frozen at `D:/chess/bin/mini-pool-queue-20261124-v1/mini_pool_queue.py`, alongside its pinned transport helper and `release.json`. Launcher PID **34812**. It uses the unchanged optimized producer policy at `D:/chess/bin/mini-pool-optimized-20261123-v1/policy.json`.

Logs: `D:/chess/mini-pool-20261123/queue-transport-v2.stdout.log` and `.stderr.log`. Status remains `queue/status.json`; prefetch remains `queue/prefetch-status.json`; `queue.pid` records the current launcher. Old queue logs/releases remain retained. On restart, accepted outputs are fully rehashed before the queue reaches the recovered job. The separately submitted job runs while those checks proceed.

All **71 previously accepted buckets** (eight original plus 63 newly built) remain usable. At the pre-stall ~4.9-minute cadence, the remaining bucket phase is approximately **36–40 hours from resumption**, excluding the final global merge/validation. The outage moves the expected bucket finish to Wednesday midday/afternoon rather than Wednesday morning.

ChessDB was unaffected: the midnight snapshot counted **49,560,469,617 / 63,996,865,341 records (77.44%)**, with approximately four hours of conversion remaining at the preceding hour's throughput, then the planned independent final audit. Its final archive/source-deletion rules remain unchanged.

## Follow-up transport verification — 00:18 PST

Bucket 065 succeeded in 217.15 seconds, was collected, and passed the output checks: **72/512 buckets accepted in total**. Automatic preparation and transfer of bucket 066 completed, but the first automatic Submit exposed a second problem in stdin request delivery. The remote bootstrap waited without creating a job. Crucially, the new timeout returned after 120 seconds and the queue safely entered `held-error`, rather than hanging indefinitely.

Removed SSH stdin request delivery entirely: short scripts use `-EncodedCommand`; long scripts are copied to unique retained files under `C:/chess-worker/inputs/transport-requests` and executed with `-File`. Read-only short and long live probes passed. All **nine** transport regressions pass outside the filesystem sandbox (the initial sandbox run could not create its temporary test directories). No remote launcher/package or privileges changed.

Reconciled bucket 066 separately, confirming that its old bootstrap had exited and no matching job existed before submitting once. Evidence: `recovery-20261124/reconciliation-b066.json`, `b066-before.json`, and `b066-recovered-submission.json`. Job `a420f99008ee4e728eab0121f88389e4`, request `2de0326e35fc4693bbc3e6800d26acf1`, submitted 00:17:20 PST and confirmed **running** at 00:17:55. Its normalized manifest SHA-256 is `ecb54e391bad2edd44c3ecff1c20e41f48c93ff40bd99b08f676ab3978758405`.

**Current release supersedes v1:** `D:/chess/bin/mini-pool-queue-20261124-v2`, with hashes/tests in `release.json`; launcher PID **1624**. Current logs are `queue-transport-v3.stdout.log` / `queue-transport-v3.stderr.log` under `D:/chess/mini-pool-20261123`. It is rehashing the accepted checkpoints while bucket 066 computes. Automatic next-job turnover with this final release has not yet been observed at this snapshot; the earlier release did complete collection and next-input preparation. Old releases and logs are retained.

## User-requested overnight stop — 00:36 PST November 24

David asked to stop main-PC work for shutdown and resume in the morning. Both main-PC coordinators have exited cleanly; do not restart until requested.

- ChessDB: `F:/chess/chessdb-archive-20260702-v6-zstd3/status.json` reports `stopped-at-checkpoint`, **51,161,276,358 records** (79.94% of 63,996,865,341), 2,670 newly completed shards. PIDs 44192/45772 exited. All active shards finished; no completed conversion work needs repeating. Startup will reverify source/archive bytes. The independent final audit has not run.
- Mini queue: 74 accepted buckets (eight original plus 66 new through bucket 067); stopped submissions at bucket 068. That remote job **succeeded** at 00:35:23 PST, exit 0, elapsed 228.379 seconds. It is retained on the mini and awaits collection/verification tomorrow; do not resubmit it. Job `f56f93ee9c504c1693f4ea7aa5699973`, request `5aa48d85391d46c29aaffd94042cb8b2`. Bucket 069 inputs and manifest finished preparation at 00:35:29; no submission made. Main queue PIDs 1624/27944 exited. The final transport release successfully performed automatic turnover through buckets 067 and 068 before this deliberate stop.
- The mini cannot keep advancing this pool while the main PC is off: input preparation, submission and collection are on the main PC. Its persistent worker remains available, but will be idle after the completed job above. No independent overnight queue has been created.

### Morning resume

1. Check main storage, WSL readiness and mini Health. Inspect saved queue states first; preserve the existing submission for bucket 068 and prepared bucket 069.
2. Remove only the two user-requested stop markers after confirming their contents: `D:/chess/chessdb-archive.STOP` and `D:/chess/mini-pool-20261123/queue.STOP`.
3. Launch `D:/chess/run_chessdb_archive.bat` hidden. It resumes the same frozen v6 archive and launches the independent verifier after full conversion completion. Preserve original ChessDB sources until independent acceptance.
4. Launch the venv Python with `-B -u D:/chess/bin/mini-pool-queue-20261124-v2/mini_pool_queue.py D:/chess/bin/mini-pool-optimized-20261123-v1/policy.json`, hidden, with fresh timestamped stdout/stderr logs; update `queue.pid`. Do not launch the older queue releases. It rehashes accepted checkpoints, collects existing bucket 068, and then submits prepared bucket 069.
5. Confirm live progress and refresh ETAs; prior wall-clock completion dates must move by the overnight pause plus startup verification time.
