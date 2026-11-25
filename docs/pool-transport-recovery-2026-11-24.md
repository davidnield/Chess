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

## Mini-only resume — 00:43 PST November 24

David opted to leave the main PC on to assess noise while supporting only the mini. Resumed the mini coordinator after confirming a healthy idle worker and no existing coordinator. Removed only `mini-pool-20261123/queue.STOP`. **ChessDB remains stopped; its STOP marker is retained.** This supersedes the overnight hold for the mini only.

Launcher PID 35896; unchanged frozen queue v2 and optimized policy. Logs: `D:/chess/mini-pool-20261123/queue-mini-only-20261124-004334.stdout.log` and `.stderr.log`. Launch record: `mini-only-resume-20261124.json` in that directory. At 00:43:41 the coordinator was progressing through checkpoint verification with no stderr. It will collect completed bucket 068 and submit prepared bucket 069, then resume normal prefetch. Allow roughly 5–10 minutes for startup verification/collection before judging steady-state fan noise. No full-load ChessDB conversion or audit was launched.

## Quiet overnight hold and scheduled restart — November 24, 00:50 PST

David found mini support too loud and requested another pause, leaving the PC on for an unrelated crawler. Do not touch the crawler. The mini queue STOP marker now reads `User requested quiet overnight hold; resume 2026-11-24 09:30 America/Los_Angeles`. ChessDB's original overnight STOP marker remains intact; its checkpoint remains 51,161,276,358 records.

At 00:49:26 the mini coordinator stopped further submissions with 67 new accepted buckets (75 including the eight original). Bucket 069 was already submitted as job `cab1cfb2d6c340cab694c87155310994`; preserve and collect its result rather than resubmitting. Preparation of bucket 070 was allowed to finish safely, without submission.

Created and verified Codex one-time thread automation **`resume-chessdb-and-mini-pool-at-9-30`**, ACTIVE, scheduled at **09:30 Pacific on November 24, 2026** (one occurrence). OS timezone confirmed Pacific Standard Time. It will check processes/storage/mini health, remove only the documented overnight STOP markers, resume both existing pipelines hidden with fresh logs, verify progress, and report revised ETAs. No separate Windows scheduled task or duplicate restart was installed. Leave the PC awake and Codex running for this local automation.

## Both jobs resumed early — 08:47 PST November 24

David requested cancellation of the 09:30 restart and immediate resumption with progress tracking. Deleted automation `resume-chessdb-and-mini-pool-at-9-30` through the app; confirmed deletion. Both documented overnight STOP markers were checked and removed. No existing chess coordinators were running; unrelated crawler left untouched.

Launch record: `D:/chess/morning-resume-20261124.json`. Mini launcher PID 23992, child 14936; unchanged frozen v2 queue and optimized policy. Logs `D:/chess/mini-pool-20261123/queue-morning-20261124-084721.stdout.log` / `.stderr.log`. Checkpoint verification advanced through bucket 007 by 08:47:46, with no stderr. Bucket 069's remote job is confirmed succeeded (228.099 seconds, exit 0); it awaits collection, not recomputation. Bucket 070 remains prepared.

ChessDB batch launcher PID 15560, coordinator 20828 (Python wrapper 15232). Source verification is actively advancing (8/291 files at 08:47), using the unchanged v6 release/output and subsequent independent audit. Conversion checkpoint remains **51,161,276,358 records / 79.94%**; startup source/archive verification adds time beyond remaining conversion and the final audit. Do not interpret `new_completed` after restart as the total archive count. Free space before resume: D 496.6 GB, F 1429.5 GB, H 2184.5 GB, above existing reserves.

Created ACTIVE hourly thread monitor **`track-chessdb-and-mini-pool-progress`** at David's request. It checks actual advancing logs/processes, phases, receipts, remote health, storage, and ETAs; investigates/fixes reversible operational faults; honors any later user-requested pause; and must stop monitoring after the archive's independent audit and the mini bucket phase finish. No duplicate monitor or Windows scheduler entry was created. Final global pool merge remains distinct and is not authorized by this monitoring schedule alone.

## Hourly check — 09:55 PST November 24

Both pipelines are alive and advancing; no production errors or intervention required. Mini: 81 new accepted buckets plus eight originals = **89/512**; bucket 083 running, next input preparing. Remote Health healthy. Submission cadence for buckets 070–081 is 51m 8.9s over 11 intervals, approximately **4.65 min/bucket**; about **33–36 hours** for remaining buckets, excluding global merge/validation (Wednesday evening at current cadence).

ChessDB source verification finished all **291/291** files at 08:55:38. Coordinator 20828 is now rehashing completed archive files via `existing()` before new conversion; status says `exporting` with `new_completed: 0`, which is not a stall. Its read counter increased from 463.409 GB around 09:51:40 to 490.075 GB by 09:54:50, with increasing CPU time. Saved conversion remains **79.94%**. At roughly 130–150 MB/s, the approximately 1.67 TB saved archive adds several hours of restart verification: roughly **2–3 more hours** of rechecking, **3–4 hours** conversion, then approximately **10 hours** independent audit. Tentative final acceptance is early Wednesday, not Tuesday afternoon. Reassess from actual phase transitions rather than treating this as a firm deadline.

Space: D 496.6 GB, F 1429.5 GB, H 2163.4 GB free; existing reserves intact. A full optional receipt metadata scan was slow on F while its archive hashes ran; stopped only that read-only monitoring scan (3964, wrapper 3316), not production. Avoid rescanning every F receipt on each heartbeat; use saved totals plus new receipts/status and advancing process I/O counters. No archive sources or accepted outputs changed.

## Hourly incident recovery — 10:52 PST November 24

The 10:50 check found the mini queue had stopped at **10:01:43**: a read-only pre-submission Health call hit a Windows sharing violation reading `C:/chess-worker/control/heartbeat.json`. The worker was healthy/idle when rechecked. Bucket 084 is already accepted; bucket 085 is prepared with **no submit_intent or submission**, so this is not an ambiguous Submit. **91/512 total accepted** (83 new plus eight originals). No completed work lost; approximately 50 minutes idle before recovery, plus restart checkpoint verification.

Added narrowly scoped, bounded retry to maintained `python/mini_worker_transport.py`: at most four attempts, delays 0.25/0.5/1 seconds, only for ReadAllText sharing violations in read-only Health/Status/List/Logs. Submit, timeouts, malformed JSON and unrelated failures remain fail-closed. All **13** regression tests pass, including transient recovery, exhausted retry bound, Submit sharing-violation nonretry and unrelated-error nonretry. No remote launcher or permissions changed.

New frozen release **`D:/chess/bin/mini-pool-queue-20261124-v3`** supersedes v2; `release.json` pins helper/queue hashes and test count. Queue/producer policy unchanged. Restart at 10:51:37; launcher PID **11576**; record `D:/chess/mini-pool-20261123/read-retry-resume-20261124.json`; logs `queue-read-retry-20261124-105137.stdout.log` / `.stderr.log` in that directory. At 10:51:56 checkpoint verification was advancing through bucket 006, with no stderr. It will verify saved outputs, then submit prepared bucket 085; actual next-job submission is not yet observed at this snapshot. No STOP marker or existing coordinator was present at launch. Hourly monitor prompt updated to consult this latest release/record.

ChessDB unaffected: coordinator 20828 read count advanced from approximately 490 GB at 09:55 to **936.615 GB at 10:51**, so the completed-archive rehash is progressing. Saved conversion remains 79.94%, `new_completed: 0` since restart; source verification already passed 291/291. Free space D **499.8 GB**, F **1429.5 GB**, H **2158.5 GB**, above reserves. Expect about 1.5–2 hours more archive rechecking, then 3–4 hours conversion and roughly 10 hours independent audit if rates hold (early Wednesday acceptance). Mini bucket phase remains roughly 33–36 hours from recovery, excluding final merge/validation. Neither job's original inputs or accepted outputs were modified.

## Hourly check — 11:48 PST November 24

Healthy progression; no intervention. Mini has **102/512 accepted** (94 new plus eight originals), bucket 096 running and 097 preparing. Since the v3 recovery, eleven further buckets (085–095) have completed/been collected, confirming automatic turnover works. No new stderr; remote worker healthy with the matching active job. Remaining bucket phase about 32–35 hours at recent cadence, excluding global merge/validation.

ChessDB coordinator 20828 remains in completed-archive byte verification: read counter **1,369,958,102,575 bytes**, up from 936.6 GB at 10:51; CPU time also increasing. No new converted shard yet; saved 79.94% remains valid. Roughly 0.3 TB of saved archive remains to recheck (about 40–60 minutes at current rates), then remaining conversion and independent audit. Tentative final acceptance remains early Wednesday. Free space D 499.7 GB, F 1429.5 GB, H 2140.5 GB. No reserve concern or user action needed.

## Hourly check — 12:51 PST November 24

ChessDB has resumed **new conversion** after the archive recheck: 59 newly completed shards since restart, **1,106,207,516 new rows** from the appended production log. Combined with the saved checkpoint: **52,267,483,874 / 63,996,865,341 = 81.67%**. No full F receipt scan was needed. Coordinator 20828 is live; new shards run around 130–142 seconds for 20M rows. Roughly 3–4 hours conversion remain, followed by approximately 10 hours independent audit; early Wednesday acceptance remains the estimate.

Mini is healthy, **115/512 accepted** (107 new plus eight originals), bucket 110 running, 111 preparing. No stderr or renewed transport failure. Remaining bucket phase roughly 31–34 hours, excluding global merge/validation. Space D 497.0 GB, F 1392.4 GB, H 2119.3 GB; reserves intact. No intervention or user action needed.

## Hourly check — 13:49 PST November 24

Both jobs progressing normally; no intervention. ChessDB now **55,304,127,740 rows (86.42%)**, 219 new shards since morning resume. About 3.04 billion rows added since the 12:51 snapshot; roughly 2.5–3 hours conversion remain at this rate, then the independent audit (approximately 10 hours). Mini **127/512 accepted** (119 new plus eight originals), bucket 122 running, 123 preparing; worker healthy, no stderr. Remaining mini bucket phase about 30–33 hours, excluding final merge/validation. Free space D 493.5 GB, F 1289.7 GB, H 2099.8 GB, all above reserves. No user action needed.

## Hourly check — 14:47 PST November 24

Healthy progress, no intervention. ChessDB **58,442,115,264 records (91.32%)**, 384 new shards since resume; about 3.14B additional rows since 13:49. Roughly 1.5–2 hours conversion remain, then independent final audit (~10 hours). Mini **139/512 accepted**, bucket 134 running, 135 preparing; worker healthy and no stderr. About 29–32 hours remain for bucket construction, excluding global merge/validation. D 496.6 GB, F 1186.0 GB, H 2080.2 GB free; reserves remain sufficient. No user action needed.

## Hourly check — 15:56 PST November 24

Healthy progression; no intervention. ChessDB **62,265,279,885 rows (97.29%)**, 582 new shards since resume; 1.732B records remain. Rough estimate **30–60 minutes** of conversion plus finalization, then independent audit (~10 hours); allow tail imbalance as fewer ranges remain. Mini **153/512 accepted**, bucket 149 running, 150 preparing, healthy remote heartbeat and no stderr. About **28–31 hours** for remaining buckets, excluding final merge/validation. Free space D 494.2 GB, F 1065.9 GB, H 2057.4 GB; reserves sufficient. No user action needed.

## Conversion complete; independent audit running — 17:00 PST November 24

ChessDB conversion completed and its launcher automatically started the independent deep verifier at **16:30:48 PST**. `ARCHIVE.COMPLETE.json` and final conversion status agree: **63,996,865,341 records**, **3,345 shards**, **2,093,381,470,759 Parquet bytes** (~2.09 TB decimal), `bounded: false`, `sources_deleted: false`. This is conversion completion, not independent acceptance; retain original sources.

Verifier wrapper/coordinator PIDs 25248/15840, four workers, existing 24 GB staging cap and 250 GB D reserve. `D:/chess/chessdb-final-audit-status.json` advanced to **222/3345 shards** at 16:59 with a fresh timestamp. Allow roughly **7–10 hours remaining** conservatively until a steadier audit rate is measured; final report remains `D:/chess/chessdb-archive-independent-verification.json`. No audit errors observed.

Mini healthy: **167/512 accepted**, bucket 163 running, 164 preparing; no stderr. Remaining bucket construction roughly 27–30 hours, excluding final global merge/validation. Free space D 497.0 GB, F 1011.6 GB, H 2034.6 GB. No intervention required; hourly monitor remains active for audit and pool completion.

## Hourly check — 18:02 PST November 24

Healthy, no intervention. Independent ChessDB audit advanced to **705/3345 shards (21.1%)**, up 483 shards since 16:59; live verifier PID 15840. Recent throughput implies roughly **5.5–7 hours remaining**, tentatively around midnight–01:00 Wednesday; completion still requires the final report. Mini **180/512 accepted**, bucket 176 running, 177 preparing, remote healthy, no stderr. Remaining bucket phase approximately 26–29 hours, excluding global merge/validation. Free space D 496.5 GB, F 1011.6 GB, H 2013.4 GB; reserves adequate. No user action needed.

## Hourly check — 19:07 PST November 24

Healthy, no intervention. ChessDB independent audit **1210/3345 shards (36.2%)**, live verifier with advancing CPU and fresh progress timestamp; approximately **4.5–6 hours** remaining at recent throughput. Mini **194/512 accepted**, bucket 190 running, 191 preparing; healthy matching remote job, no stderr. Remaining bucket phase approximately **25–28 hours**, excluding global merge/validation. Free space D 497.0 GB, F 1011.6 GB, H 1990.5 GB; reserves adequate. No user action needed.

## Hourly check — 20:04 PST November 24

Healthy, no intervention. ChessDB independent audit **1660/3345 shards (49.6%)**, advancing CPU and fresh progress timestamp; roughly **3.5–5 hours** remain. Mini **206/512 accepted**, bucket 202 running, 203 prepared; matching healthy worker, no stderr. Roughly **24–27 hours** remain for bucket construction, excluding global merge/validation. Free space D 494.7 GB, F 1011.6 GB, H 1971.0 GB, above reserves. No user action needed.

## Hourly check — 21:06 PST November 24

Healthy, no intervention. Independent ChessDB audit **2138/3345 shards (63.9%)**, live verifier with fresh status and advancing CPU; roughly **2.5–4 hours** remain. Mini **220/512 accepted**, bucket 216 running, 217 preparing; matching healthy worker, no stderr. Roughly **23–26 hours** remain for bucket construction, excluding final merge/validation. Free space D 497.7 GB, F 1011.6 GB, H 1948.2 GB; reserves adequate. No user action needed.

## Hourly check — 22:04 PST November 24

Healthy, no intervention. ChessDB independent audit **2570/3345 shards (76.8%)**, live verifier with fresh status and advancing CPU; roughly **1.5–3 hours** remain. Mini **232/512 accepted**, bucket 228 running, 229 preparing; matching healthy worker, no stderr. Roughly **22–25 hours** remain for bucket construction, excluding global merge/validation. Free space D 497.4 GB, F 1011.6 GB, H 1928.6 GB; reserves adequate. No user action needed.

## Hourly check — 23:07 PST November 24

Healthy, no intervention. ChessDB independent audit **3043/3345 shards (91.0%)**, live verifier with fresh progress and advancing CPU; approximately **40–90 minutes** plus final report work remain. Mini bucket 241 succeeded remotely (234.48 seconds, exit 0) and its local state is now `accepted: true`, bringing the total to **246/512**; the displayed collecting status briefly lags acceptance. Bucket 242 is prepared. Worker healthy and momentarily idle during normal collection/turnover, no stderr. Remaining bucket phase approximately **21–24 hours**, excluding global merge/validation. Free space D 494.4 GB, F 1011.6 GB, H 1906.0 GB; reserves adequate. No user action needed.

## Overnight hold and successful ChessDB acceptance — 23:48 PST November 24

David explicitly requested pausing both jobs and the monitor for the night. Automation **`track-chessdb-and-mini-pool-progress` is PAUSED** (not deleted); no scheduled restart exists. Both STOP markers now read `User requested overnight pause 2026-11-24 23:45 PST; do not restart until asked`. Unrelated crawler untouched.

**ChessDB conversion and independent audit are COMPLETE and PASSED.** The verifier finished naturally while the requested stop drained, after all 3,345 shards had already been checked. Final report `D:/chess/chessdb-archive-independent-verification.json`: `verified: true`, `deep: true`, `bounded: false`, **63,996,865,341 records**, **3,345 shards**, **2,093,381,470,759 bytes**, audit elapsed **26,065.406 seconds (~7h14m)**. Independently matched its marker hash to the current `ARCHIVE.COMPLETE.json`: `a19025416d8f7adbb60c16ba72b6a5899907539383533c9e1ce9553ffaebc13e`. Verifier SHA-256 `7c5220e33b9a674b08ffdec805f27a65134e5627f88c429706d77fe157fb4c47`. Source data remains preserved; no deletion authorized by this pause. **Do not rerun conversion or audit on morning resume.** Any subsequent migration/cleanup is a separate next step.

Mini: **254/512 buckets accepted** (246 new plus eight originals) before pausing. Current bucket 250 job `4aa393fc36bf49d5a70c684e9c67c613`, request `8a22f0dada4b42928770644f9e197af5`, was allowed to finish on the mini without further submissions. Bucket 251 finished input preparation at 23:46:42. Main coordinator processes have exited; frozen v3 queue is the current release. Retain saved bucket 250 submission and collect its result rather than resubmitting tomorrow. On user-requested resume, check Health/current state, remove only the mini STOP marker, launch the v3 coordinator hidden with fresh logs and updated queue.pid, and resume the monitor with its prompt revised to acknowledge ChessDB acceptance. Keep the archive STOP marker to prevent accidental rerun. No global pool merge/validation was started.

Final confirmation at 23:48: bucket 250 **succeeded** at 23:47:33 (233.59 seconds, exit 0); remote worker healthy and idle with no active or queued requests. Its result is retained remotely and awaits collection/acceptance. Both main-PC coordinators are exited, both STOP markers remain, and monitor status is PAUSED.

## User-requested resume — 09:25 PST November 25

David authorized resuming the data jobs and hourly monitor after optimizer implementation.
Confirmed the ChessDB independent audit and its exact current archive completion-marker
SHA256 match. **ChessDB is already finished; neither conversion nor audit was relaunched.**
Its STOP marker and original source data remain preserved.

The mini was healthy, idle and unpaused. Saved bucket 250 still had the same successful
job/request identity, exit 0, with no active/queued requests. Worker/runtime paths were
present and the optimized driver/config hashes matched. Available space: main D 462.5
GiB, H 1,762.8 GiB, F 942.2 GiB; mini C approximately 2,466 GiB. The last 30 accepted
outputs average 1.627 GB decimal each, projecting about 391 GiB additional H space for
the remaining 258 collections. Bucket construction/collection fits the current reserve;
this is not a new final-sort capacity acceptance.

Removed only the mini's exact overnight STOP marker. Launched the unchanged frozen v3
coordinator hidden at 09:25:10 PST: launcher PID **24564**, record
`D:/chess/mini-pool-20261123/resume-20261125.json`, logs
`queue-resume-20261125-092510.stdout.log` / `.stderr.log` in that directory.
`queue.pid` is updated. Startup rehashing advanced normally with no stderr; its
`completed` count during rehashing is not the total retained count. **254/512 remain
accepted**, plus completed remote bucket 250 awaiting collection. Prepared 251 is reused.

Resumed automation `track-chessdb-and-mini-pool-progress` as ACTIVE, hourly, with the
prompt revised to acknowledge completed ChessDB and follow the current mini launch.
It retains the existing restrictions on duplicate coordinators, ambiguous submissions,
reserves, accepted data, source deletion, unrelated jobs and the crawler. Global merge
and other populations are not automatically started by this bucket coordinator.

Thirty recent buckets (220–249, 29 start-to-start intervals) average **279.039 seconds**
per bucket. The remaining 257 computations project to **19.92 hours** before startup
rehashing/collection and variation. Planning range: **20–23 hours from resume**, roughly
**05:30–08:30 PST Thursday November 26** for bucket construction, not a published final
pool. Verify actual new-job submission after rehashing before closing this resume task.

Resume verified at 09:41 PST: checkpoint rehashing completed without errors; saved
bucket 250 was collected and accepted, bringing the total to **255/512** (247 new
plus eight original buckets). NEW bucket 251 is running remotely, job
`ff3969de5b9e4a7ba8c82ba7b813ade7`, request `c344de27401649c6ae39ee8f12d3f7af`,
started 09:40:38 PST, worker child PID 4480. Remote Status confirms the expected
24 GiB / 75% / six-hour limits. No duplicate of bucket 250 was submitted.
Main coordinator child PID **20540** (venv wrapper 24564). At 09:42, remote logs
show advancing aggregation I/O/CPU with no stderr, and main prefetch is packaging
252. Free space D 465.1 GiB, E 1,048.1 GiB, F 942.2 GiB, H 1,761.2 GiB.
The hourly monitor also follows the new bounded-budget ChessDB source diagnostic;
see `chessdb-next-steps-2026-11-25.md`. Archive conversion/audit remain complete.

## Hourly check — 10:23 PST November 25

Healthy; no intervention required. Mini **263/512 accepted** (255 new plus eight
originals), bucket 259 running under job `df272eacfcd041c4a846d9bf3ec02c53`,
260 packaging. Matching remote Health/Status, advancing CPU/I/O and empty stderr.
Main coordinator PID 20540 remains live. Seven post-resume start-to-start intervals
average **289.96 seconds/bucket**; allow approximately **20–23 hours remaining**
for the bucket phase, excluding global merge/publication.

Targeted SF18 diagnostic advanced to **558/942 completed, 556 qualified** at 10:22:44,
PID 29120. Two searches remain unqualified under the frozen rules; no evidence was
downgraded or silently promoted. Recent throughput suggests roughly **30–50 minutes**
remaining. The 1,000-position study (PID 20860) and separate 300-position confirmation
(PID 44696) are healthy in their expected sequential waits. All four local stderr
logs are empty and all three frozen evaluation releases still match their code
receipts. Confirmation scores remain unopened; no contender is locked.

Free space: D **462.5 GiB**, E **1,048.0 GiB**, F **942.2 GiB**, H **1,749.1 GiB**;
mini C **2,605,506,359,296 bytes** (~2,426.6 GiB). Reserves are adequate. The archive
remains accepted and untouched. No new merge, source adoption, cleanup or unrelated
job was launched; hourly monitoring continues.

## Hourly check — 11:29 PST November 25

Healthy; no job intervention required. Mini **277/512 accepted** (269 new plus
eight originals), bucket 273 running as job `d8daa4fdf5a94ec98bf2cac21010ef31`,
274 packaging. Remote Health confirms one active, unpaused worker with no queued
requests. Logs show advancing CPU/I/O and memory, no spill and empty stderr;
limits remain 24 GiB / 75% / six hours. Recent accepted output sizes, small stats
checksums, manifest hashes and successful remote receipts agree. Twenty-one
post-resume start-to-start intervals average **285.96 seconds**, projecting
**18.7 hours** of remaining buckets; allow **19–22 hours** for variation. This
excludes global merge/publication. Main coordinator PID 20540 remains healthy.

The fresh 1,000-position SF18 study advanced to **414 completed / 413 qualified**;
the one unqualified search remains unresolved under the frozen rules. Four engines
belong only to this batch. Current throughput projects about **49 minutes** left,
with a planning range of **45–75 minutes**. Pooled analysis (PID 36468) and the
300-position confirmation worker (PID 44696) remain in their expected waits.
Pooling adds no searches; final confirmation then needs roughly another 30–60
minutes, subject to actual throughput. No partial ranking or sealed result was read.

All four local stderr logs are empty, frozen selection/confirmation/pooling
identities pass, and no final choice or opening marker exists. The targeted
diagnostic remains completed at 942 attempted / 939 qualified. No source-policy
adoption, raw-source cleanup, archive restart or global merge was performed.

Free space: D **465.1 GiB**, E **1,048.0 GiB**, F **942.2 GiB**, H **1,727.8 GiB**;
mini C **2,540,170,383,360 bytes** (~2,365.7 GiB). Reserves remain adequate.
Direct remote CIM process inspection is denied for the intentionally standard
worker account; launcher Health/Status and per-process progress logs provide the
needed evidence. No permissions were changed.

## Hourly check — 12:28 PST November 25

Mini remains healthy: **290/512 accepted**, bucket 286 running, 287 packaging.
Launcher Health/Status, advancing workload logs, empty stderr and recent accepted
receipt/file-size/small-stats checks agree. Thirty-four start intervals average
**284.44 seconds**, projecting **17.5 hours** of buckets remaining; allow **18–21
hours** for variation, before global merge/publication. Limits and reserves are
unchanged. Free D/E/F/H: **465.1 / 1,048.0 / 942.2 / 1,709.6 GiB**; mini workload
reports about **2.482 TB** free (~2,311 GiB).

The fresh study completed at **12:13 PST**, **999/1,000 qualified**. Pooling
completed at **12:14**, retaining **2,499 unique parents** and all **1,008 NNUE
triple overlaps**. Complete evidence/receipt checks passed. The selected research
contender `score_3_3_4` was locked at **12:26:13 PST**, before final score access,
against `proposal` and ChessDB alone. See the immutable selection rationale and
`chessdb-selection-review-2026-11-25.json` for the mean-error advantage, stricter-
White gate regression and no-Cloud fallback regression. No production change.

The final 300-position confirmation started after the study and is at **175/300
attempted, 174 qualified** at 12:27:51, with scores unopened. Four one-thread
engines belong only to this batch. Allow roughly **10–25 minutes** more based on
observed throughput; qualification/reporting still follows completion. The monitor
now follows the already locked choice and will use only the pooling wrapper's
`report-confirmation` after sealed completion, never select again. No raw-source
cleanup, archive restart, global merge or unrelated job changes occurred.

## Confirmation completed — 12:51 PST November 25

While answering David's fallback-policy question, observed sealed completion at
12:36:49 and executed the authorized pooling wrapper's final report at 12:50:54.
The pre-locked choice and all evidence verified before score access. **299/300
qualified**, including 297 at actual depth >=28. The 30/30/40 blend improved mean
error versus both controls with simultaneous intervals excluding zero; stricter-
White gate safety remains worse and missing-source handling is not established by
this all-source panel. See `chessdb-ensemble-confirmation-results-2026-11-25.md`.
No production change. All source-study batches/pooling/confirmation are now finished
and reviewed; their receipts remain intact. The hourly monitor was updated to
follow the mini queue without repeating already reported research completions.

Mini queue at 12:51:46: **295/512 accepted**, bucket 291 running and 292 packaging.
No restart, source deletion, global merge or unrelated-job action was performed.

