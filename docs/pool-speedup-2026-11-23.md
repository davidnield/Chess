# Pool construction optimization — November 23, 2026

User approved a bounded optimization experiment, exact-output comparisons, and next-bucket preparation overlapping current computation. Preserve the existing pool recipe, all accepted buckets, raw inputs, and the independent ChessDB archive run.

## Plan and acceptance

1. Profile the frozen bucket builder on retained pilot bucket 005 with its original 4 threads / 16 GB DuckDB settings.
2. Test early release of the string-bearing edge table and sparse terminal aggregation. Keep the full flow table, negative-flow check, classified-ending reconciliation, and accounting statistics.
3. Test arrival-file compaction independently of the query changes. Concatenate every row, including duplicates, into bounded larger row groups; include compaction time in the comparison.
4. Compare schemas, every output row with duplicate multiplicities, and all non-timing statistics against the accepted baseline before deploying changes.
5. Test and deploy one-next-bucket prefetch. It may prepare/transfer inputs while one remote compute job runs; it must never submit from the background thread, overwrite an existing submission intent, or increase compute concurrency.
6. Freeze the selected producer, preserve per-bucket provenance and restart/resume protections, resume production, and report measured throughput rather than a speculative ETA.

## Safe pause and benchmark inventory

The previous production coordinator stopped at its existing stop-file boundary at 14:02 PST. Nine new buckets have been accepted in addition to the eight original buckets. Bucket 010's input manifest was staged and saved, without submission. `D:/chess/mini-pool-20261123/queue.STOP` prevents automatic production submissions during the isolated benchmark. No running production job was cancelled.

Experiment directory: `D:/chess/mini-pool-speedup-20261123`.
Remote scripts: `C:/chess-worker/inputs/pool-speedup-20261123-v1`.
All benchmark outputs go into separate launcher job directories, never production work directories.

| Arm | Job ID | State at submission |
|---|---|---|
| Instrumented baseline | `66a6e27b67a04cd3b2ee8649a207d875` | Running |
| Early release + sparse endings | `70282db94ce14e9da2ae21fcd5a5f1c0` | Queued |
| Same queries + compact arrivals | `0099e94663824991a82f2a737f79303f` | Queued |

The first setup attempt `ac57f93883984745a7b0d420402e5e63` failed before computation because retained packet JSON is reformatted during unpacking. The corrected probe compares canonical JSON content against the original ZIP's manifest, then hashes every input. No data was altered. A separate local SSH submission client stalled before submission; it was terminated and the launcher job list reconciled before the successful captured-output submission.

Probe receipts and `queries.jsonl` bind module hashes and record wall time, CPU time, and memory for each query. The baseline module is the existing `228e229c8681227d3d00908db83861c3830939a75be48332e96fc2b003e87213` frozen exporter. Candidate code is separate, with sparse/release behavior opt-in pending acceptance.

## Verification progress

- Three new bucket tests passed: full output/stat/schema parity at floors 1, 5 and 1000 with 128-bit twins and repeated positions; zero-summary sparsity while preserving full flow cells/negative checks; excess classified endings rejected.
- Existing full 128-bit pool/gates/consumer test passed with candidate optimizations enabled.
- Existing independent game-by-game pool suite passed all 29 checks with optimizations enabled; output in `regression-pool.log`.
- Six input-prefetch tests passed, including one-in-flight bound, stop before/during prefetch, error propagation, accepted-bucket skip, and protection of ambiguous submission state.

## Resume boundaries

Do not remove the production stop file while benchmark jobs are active/queued. Do not modify a running frozen script. If a benchmark fails, retain its outputs/logs and inspect the failure before further tests or deployment. Production's original one-at-a-time queue is still resumable from `D:/chess/bin/mini-pool-queue-20261123-v1/mini_pool_queue.py`; its saved input state and nine accepted new buckets remain authoritative. No final pool merge or publication is part of this experiment.

Independent exact-acceptance job: `3a22903394ff4c5f8e4ada1532c5c3a7`, queued after the three arms. Frozen prefetch coordinator: `D:/chess/bin/mini-pool-queue-20261123-v2`; six tests passed, not yet started. All benchmark jobs retain the 24 GiB whole-job ceiling and run serially.

## Measured bottleneck and fourth arm — 14:25 PST

Baseline's initial edge aggregation took 661.875 seconds (754.859 CPU seconds); writing edges took 45.69 seconds; departures aggregation took 112.45 seconds. The empty Other Moves queries took effectively zero time, so no extra specialization is justified there. The source bucket has 74,174,671 rows in 12 files / 291 row groups (2.592 GB).

A fourth arm tests eight disjoint signed-low-half ranges for the first edge aggregation, retaining full 128-bit grouping and all validation. Job `d5f77c32c55f43e1b81c4f96d6873036`, request `5163a69def964186bfc473d5e4d210b2`. Unit parity passed with four ranges, including high-half twins, floors 1/5/1000, signed extremes, and errors for incomplete keys. All new execution options remain opt-in in the maintained CLI.

The original queued verifier was cancellation-requested before starting; its attempt is retained. Replacement verifier `aa3f96f6465d44c0b8d5cae51d9f296f`, request `186ca00c90f24ae59c565221a1f605f5`, runs after all four arms. Its manifest SHA-256 is `3518473d0488aa5b8cea9e9d0006988b46716bf3d5203f43be7ad2ea2f3c0d04`.

An automatic finish controller is armed: `D:/chess/mini-pool-speedup-20261123/finish_pool_speedup.py`, launcher PID **26060**. Read `controller-status.json`, `controller.stdout.log`, and `controller.stderr.log` there. It requires the pinned successful verifier attempt, complete exact comparisons, and at least 1.10x throughput including compaction before selecting a new query implementation. Otherwise it retains the original worker with the tested prefetch queue. Five selection tests passed.

The optimized adapter requires a hash-pinned exact-acceptance receipt, matching result/module and flags; five adapter/compaction tests passed. A selected worker is frozen under `D:/chess/bin/mini-pool-optimized-20261123-v1`, with matching mini files under `C:/chess-worker/inputs/pool-optimized-20261123-v1`. Existing bucket receipts retain their original identities; new receipts also bind the optimization configuration. Regenerable compact-arrival scratch is removed only after full output checks, while source inputs and its receipt remain.

The controller removes only its own benchmark stop marker, checks the mini is healthy/idle, records a persistent launch intent before resuming, and starts exactly one new queue with at most one prefetched input batch. A changed stop marker, failed benchmark/verification, ambiguous launch or identity drift holds for investigation. Do not rerun a controller with `launch.json`; reconcile the launched queue first. Prepared old bucket 010 keeps its original manifest; subsequent buckets use the selected worker.

The controller does not publish a final pool, start the other populations, change the main ChessDB archive, or delete accepted production buckets. Its selection uses one representative bucket and a material-gain threshold; verify sustained speed on subsequent production buckets before tightening the ETA.
## Handoff checkpoint — 14:30 PST

- Both versions of the independent game-walk suite passed all 29 checks (query optimization alone, then range batching too). The public 128-bit CLI/consumer suite also passed with both execution options enabled. Targeted full-output parity and signed-boundary/null-key tests pass.
- The candidate queue verified the actual 23,877,125-edge accepted pilot output and correctly rejected a changed producer identity. Worker acceptance/compaction and prefetch/selection tests pass.
- Baseline profile also measured arrivals aggregation at **185.81s**, full flow join **19.38s**, and the zero-heavy terminal summary at **222.73s**. Thus sparse endings targets a measured multi-minute cost; empty Other Moves queries are already effectively free.
- The **current finish controller is v2**, `D:/chess/mini-pool-speedup-20261123/finish_pool_speedup_v2.py`, hidden launcher PID **22724**. Its logs are `controller-v2.stdout.log` and `controller-v2.stderr.log`; status remains `controller-status.json`. The previous controller (launcher PID 26060) was stopped while waiting, before any launch intent or deployment; remote jobs were untouched. V2 also waits for the dispatcher to become idle and verifies a live production job before reporting production resumed.
- At this checkpoint the baseline is still finishing; the three candidate arms and replacement verifier remain queued. No speedup or new production ETA has yet been accepted. Allow roughly another 1–2 hours for the serial candidate runs and complete comparisons; this is a planning estimate, not a measured completion deadline.
- The production queue is deliberately held at 17 completed buckets total (eight original plus nine newly accepted), with bucket 010 prepared. It resumes automatically after successful selection/verification. ChessDB remains independent and running.
- Code commits: `8b3edfa` (sparse/release queries and parity tests), `f182abf` (range batching, CLI options, boundary tests). All execution options remain opt-in; the benchmark-selected frozen worker records which were used.
## Accepted results and resumed production — 16:20 PST

All four arms passed exact schema/row-multiset and non-timing-stat comparisons against the accepted reference: 23,877,125 edges and 20,371,938 auxiliary rows, zero differences in both directions. Acceptance: `D:/chess/mini-pool-speedup-20261123/verified-comparisons/ACCEPTED.json`, SHA-256 `65f5c7a1dbfc6919f68dcbbb1d77f6efc265f240bb30eca1ebf22177f3d696c6`. Decision: `decision.json` in the same experiment directory.

| Arm | Aggregation + compaction seconds |
|---|---:|
| Frozen baseline | 1376.890 |
| Early release and sparse ending summary | 998.359 |
| Same queries plus compact arrivals | 774.204 |
| Same plus eight edge ranges | **137.267** |

Selected `partition`: **10.0307x** measured computation throughput including compaction. The finish controller resumed production at **15:17 PST** and confirmed a live job. Queue launcher PID **44632**; frozen queue `D:/chess/bin/mini-pool-optimized-20261123-v1/mini_pool_queue.py`, with `policy.json` beside it. The pre-staged bucket 010 ran under its original manifest; buckets 011 onward use the accepted optimized module and individually pinned optimizer identity. Old source/producer receipts remain intact.

Seven optimized production buckets (011–017) have now been accepted and collected. Their mean aggregation time is **122.50s**. Real compaction takes about **79s**, appreciably more than the warm benchmark's 10.8s, and full remote execution/collection adds overhead. The measured end-to-end cadence from bucket 011's start to bucket 017's accepted receipt is **294.69s/bucket (4.91 minutes)**. Use this real cadence, not the 10x inner-computation ratio, for projections: **about 40–45 hours remaining for bucket construction**, versus the previous 10–11 days. This remains a seven-bucket sample and excludes final global merge/validation.

At the snapshot: **25/512 total buckets complete** (eight original plus seventeen new), bucket 018 running, bucket 019 preparing in parallel. Queue status: `D:/chess/mini-pool-20261123/queue/status.json`; prefetch status beside it. `queue-speedup.stderr.log` and the finish controller stderr contain no reported errors. Mini worker is healthy; 3.789 TB free. H: has 2.266 TB free for collected outputs. Do not infer that the final global sort is already storage-approved or queued.

The independent ChessDB run is also healthy: 1,198 accepted shard receipts, **23,099,210,304 / 63,996,865,341 records (36.09%)**, 755.55 GB accepted archive bytes. Last-hour throughput gives about **12 hours remaining conversion**, followed by the previously measured ~10-hour independent final audit. F: free space is 2.351 TB, above the 500 GB reserve with room for the remaining projected archive. D: free is 497.69 GB, above its 250 GB reserve. These are live estimates as of this snapshot, not fixed completion promises.