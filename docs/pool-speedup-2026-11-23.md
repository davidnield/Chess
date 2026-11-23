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
