# Execution plan — Monday, November 23, 2026

User authorized proceeding with the existing day's plan after mini-worker acceptance.

1. **Resume the full lossless ChessDB archive on the main PC.** Check the frozen v5 release, free space and absence of a duplicate job; retain the four verified 20M-record shards. Run D:/chess/run_chessdb_archive.bat as a hidden persistent process. Observe fresh source verification and new accepted checkpoints before handing back status. Full independent deep verification follows automatically after publication. Keep both original database copies throughout this run.
2. **Prepare the remaining validation work while archiving.** Reconcile completed lookup/menu acceptance and existing SF18/OTB diagnostics; identify the exact unresolved source-policy and pool requirements. Preserve the current evaluation scale, source bindings, frozen cohorts and canonical books. Produce concrete recommendations before requesting any outstanding policy choice.
3. **Use the mini for a bounded CPU workload where useful.** It has passed a 20M-record archive-audit pilot. Next prepare/benchmark a versioned Stockfish batch from existing reference-panel positions, if its binaries and inputs can be verified. Use the persistent launcher, small immutable inputs, explicit threads/hash/node limits and collected receipts; it is additional diagnostic evidence, not a new book or held-out test.
4. **Update the storage and launch plan for the remaining pools.** The main-PC full-pool materialization is not safe on current D:/E: free space. Account for the mini's separate NVMe and existing source/data dependencies before choosing a bounded next pilot. Do not launch a multi-terabyte export based only on nominal capacity.
5. **Record progress and next dependencies.** Save actual PIDs, code/input identities, checkpoint counts, measured rates and resume/stop instructions. No archive cleanup before complete acceptance; full book/sweep adoption retains the existing acceptance procedure.

## Start state

- Candidate lookup/menu publication and final immutable acceptance are complete; the old takeover checklist's running/queued labels are historical snapshots.
- Archive: F:/chess/chessdb-archive-20260702-v2; 80,000,000 records in four verified shards; initially stopped. Frozen release D:/chess/bin/chessdb-archive-20261122-v5. Source and completed archive bytes are rechecked by the resumed coordinator.
- Main free space at start: D: 507 GB, E: 1.125 TB, F: 3.121 TB, H: 2.295 TB. Archive reserves remain D: 250 GB / F: 500 GB with 24 GB bounded staging.
- Mini: healthy/idle after today's native audit pilot, ~3.89 TB free. Standard user, one job, 16 GiB default committed-memory cap, 75% CPU, 50 GiB reserve. No Python/Rust/Stockfish executable found on its PATH; stage required executables explicitly.
- Baseline receipts preserved in D:/chess/day-20261123/archive-resume-baseline.json. Daily artifacts go under D:/chess/day-20261123. Runtime archive state remains authoritative.

## Progress

- Plan recorded and frozen release hashes verified before launch.


## Execution update — 2026-11-23T08:24:20.408841-08:00

- Full archive running: hidden cmd PID 42080, coordinator PID 3496. Fresh source verification passed all 291 files. Original four receipts and Parquet modification times are unchanged. Current accepted total: 160,000,000 records / 8 shards (4 newly accepted today); 4 active shard attempts. Snapshot: D:/chess/day-20261123/progress.json. Log: D:/chess/chessdb-archive-production.log. Preserve frozen v5 and the original database copies.
- Mini SF18 benchmark passed: 12 fixed existing-panel boards at 20M nodes each, fresh engines, one thread / 128 MB hash per engine. Serial 215.3s; four independent workers 85.1s (2.53x throughput). Both arms produced identical nodes, best moves and scores. Three matched main-PC searches also match exactly: main 113.5s vs mini 55.1s, 2.06x on this sample. Peak mini committed memory 1.16 GiB. Job c51479c8f21c43eeaf86386e8f518248 succeeded and was collected; mini now idle. Evidence: D:/chess/day-20261123/mini-stockfish-v2. Do not adopt these benchmark values as new reference-cache verdicts.
- Suitable next mini workload is a fixed independent SF18 verification batch with four single-thread workers, 128 MB hash each, within the existing 16 GiB / 75% job limits. Runtime/code/input receipts and application checkpointing must accompany a production batch. Existing prepared mini script is a bounded benchmark, not a general resume coordinator.
- Paired source-policy diagnostic and pool capacity recommendation completed: docs/repertoire-next-stage-readiness-2026-11-23.md. Candidate blend lowers mean error but raises stricter-White false admissions; source-policy choice and fresh boundary checks precede adoption. No default/binding change or full pool launch.
- Full archive publication automatically triggers the source-independent deep audit through run_chessdb_archive.bat. Initial broad planning remains roughly 55–60h conversion before final audit and production overhead; use about three days as a provisional planning scale, not a deadline. Re-estimate from sustained varied-range throughput. Graceful stop remains D:/chess/chessdb-archive.STOP; observe stopped-at-checkpoint before shutdown.

Integration details retained for future worker jobs: Windows PowerShell 5.1 needs direct assignment of ConvertFrom-Json arrays here; wrapping the pipeline in @() nested the task list in the first local 1,000-node probe. That probe used only 12,000 nodes, was retained under mini-stockfish-v1, and was not submitted remotely. The corrected v2 probe processed exactly one board. The launcher also requires backslash-form absolute executable paths (C:\Windows\...), so the initial forward-slash manifest was rejected before queuing; List confirmed no workload was queued, then the corrected manifest was submitted successfully. The retained prepare-source.py incorporates both corrections. No launcher/administrator package changes were made.

## Remote pool pilot — 08:47 PST

The user authorized a representative pool test on the mini while the main archive continues. A pinned user-local Python/native-DuckDB runtime and hash-bound bucket input bundle are staged. The dedicated 32 GB mini now runs the pilot with a 24 GiB job limit and 16 GB DuckDB limit (the initial conservative attempt was cancelled after 55 seconds). Job b5359340c0184999b659b1fd98ad2bbc is rebuilding known bucket 005; automatic collection and exact two-way row-multiset comparison are armed. Production buckets remain gated on pilot acceptance. Full details, hashes, logs and resume boundaries: [mini-pool-pilot-2026-11-23.md](mini-pool-pilot-2026-11-23.md).
