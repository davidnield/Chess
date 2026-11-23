# Mini-PC pool pilot — November 23, 2026

The user authorized using the dedicated mini PC for pool construction while the main PC completes the ChessDB archive. Begin with an exact comparison against an existing bucket before producing unfinished buckets. This document supplements the day's execution plan; live status and acceptance receipts remain authoritative.

## Implementation and resources

The selected pool exporter is `D:/chess/bin/sweep-2dd3a63/python/pool_from_book.py`, SHA-256 `228e229c8681227d3d00908db83861c3830939a75be48332e96fc2b003e87213`. It orchestrates DuckDB's native aggregation, joins, Parquet reads/writes and sorting; the final export uses bounded Arrow batches. There is no equivalent Rust pool exporter in the current worktree. The Rust repertoire builder and explorer extractor are separate stages. A faster pool algorithm remains possible, but replacing the wrapper language alone is not evidence of a speedup.

The mini has 32 GB RAM and is dedicated to the project. Following the user's clarification, the pilot uses **24 GiB whole-job committed-memory limit**, **16 GB DuckDB managed-memory limit**, four DuckDB threads, 75% aggregate CPU cap, and the existing 50 GiB disk reserve. The launcher supports a maximum 24 GiB memory request. DuckDB's limit does not cover every allocation, so leave the remainder for Arrow, unmanaged query allocations, Windows and the worker service. Record peak process/job commit, RSS, sampled spill size and I/O before considering another increase.

The initial 16 GiB / 8 GB attempt was cancelled after 54.7 seconds to adopt these limits; logs and any partial work remain. Its ID is `65f6e39c073a4fbcb4c02407fc95acd7`. Do not retry it accidentally.

## Active pilot

- Job: `b5359340c0184999b659b1fd98ad2bbc`; request `6ce96a30cbfd47698e3adf2873a023c3`.
- Submitted: November 23, 08:47:06 PST. Persistent mini launcher, not an SSH child.
- Manifest: `D:/chess/mini-pool-20261123/pilot-16gb-manifest.json`; SHA-256 `a192185bdf5fe109ca92ce622a05c2bbb054815e2e5a6f7e3ba1de61d1e671ef`.
- Pool: brc1800-p30, Blitz/Rapid/Classical, rating bands 1800/2000/2200/2500, min-games 1, full 128-bit keys.
- Recipe identity: `3e8eaf78e5131adba3bdebcb16359a47dff2b061b665ce85fa770dd249a3ec41`.
- Reference: bucket 005 under `D:/chess/sweep-inputs/brc1800-p30/work/buckets`; 23,877,125 edges and 20,371,938 auxiliary rows. Preserve it.
- Bundle: `D:/chess/mini-pool-20261123/packets/b005.zip`, 3,202,642,976 bytes, 17,918 files; SHA-256 `71a49711c50c531150a97a19cdb1086791808c64b7944098759809a430447648`. Includes the selected move slices, termination slice, all 32 arrival groups, and source/recipe metadata. Packaging took 186 seconds.
- Source book: `E:/chess/position-stats/brc-p51-train-201301-202607-20261115`; existing completed arrivals are reused. No input subset is published as a complete source book.

## Frozen worker and runtime

- Worker driver: `D:/chess/bin/mini-pool-20261123-v2/mini_pool.py`, SHA-256 `2f0dcff8dd78dbac1fe406934628bc9954fd06b5231c87538fac0b9fce098f9b`.
- Remote driver: `C:/chess-worker/inputs/pool-driver-20261123-v2/mini_pool.py`.
- Remote runtime: `C:/chess-worker/inputs/pool-runtime-20261123-v1/runtime`. User-local Python 3.11.15, DuckDB 1.5.2, PyArrow 24.0.0, NumPy 2.4.4, Polars 1.40.1, chess 1.11.2, psutil 7.2.2. No administrator installation or PATH changes.
- Runtime manifest SHA-256: `a83f5bdaedc2a1726335f30929f16dbafb1dc411c79ad255c065df4c6de13e53`; pins all 5,588 runtime/code files.
- Remote extracted inputs: `C:/chess-worker/inputs/pool-data-20261123-v1/b005`. Only the regenerable transfer ZIP is removed after verified extraction; original source files and extracted inputs remain.
- Outputs and telemetry reside inside `C:/chess-worker/jobs/<job-id>`. Completion receipt is written last and binds the driver, runtime, input bundle, recipe, settings and output hashes.
- Only the real eval-lookup metadata is transported. With min-games 1 the below-floor query is empty; unexpected evaluation-array use fails instead of inventing values. This pool's bucket aggregation does not depend on choosing the new ChessDB evaluation policy.

Transport checks passed 11 cases, including corrupted/missing/duplicate files, unsafe paths, changed staged files on resume, and an empty evaluation request opening no arrays. Results: `D:/chess/mini-pool-20261123/tests-v2/result.json`.

## Automatic acceptance

Main-PC validator: `D:/chess/bin/mini-pool-validation-20261123-v2/validate_mini_pool_pilot.py`. Hidden PID 37816 at launch; check `D:/chess/mini-pool-20261123/validation.pid` and the actual process before restarting.

It waits for the exact successful attempt, collects to `H:/chess/mini-pools/brc1800-p30/pilot-b005`, checks completion identity and output hashes, compares all non-timing statistics, and runs `EXCEPT ALL` in both directions for every row/column of both output tables. It preserves duplicate multiplicities and full 128-bit keys. Different row order is allowed. Four small comparison tests passed, covering reordering/nulls, duplicate-count changes, high-half key changes and value changes.

- Status: `D:/chess/mini-pool-20261123/pilot-validation-status.json`.
- Logs: `validation-v2.stdout.log`, `validation-v2.stderr.log` in that directory.
- Acceptance appears only after all checks pass: `pilot-accepted.json`.
- The exact comparison uses 8 GB DuckDB memory, two threads, isolated D: spill with a 128 GB cap, and preflights the main PC's 250 GB reserve.
- Failure halts acceptance; it does not launch production buckets or retry a failed mini attempt automatically.

## Remaining work and storage boundaries

After acceptance, use the measured bucket time, peak memory/spill and transfer cost to decide production concurrency and forecast the 504 unfinished buckets. Preserve the eight existing buckets (5, 28, 109, 146, 295, 346, 396, 509). One-bucket bundles avoid transferring 1.6 TB up front and avoid millions of tiny SCP operations. Use durable job IDs and independently verified result receipts for resumption.

Estimated complete remote bucket-phase footprint is about 2.47 TB (selected move/termination inputs, arrivals and outputs) against roughly 3.89 TB initially free. That does **not** authorize a global final merge on the mini while retaining all inputs: its final outputs and sorting spill need a separate capacity plan. No full pool publication or final merge is queued by this pilot. F: remains archive-only, not pool spill storage.

The main ChessDB archive remains independent and running under `D:/chess/run_chessdb_archive.bat`; its live status is `F:/chess/chessdb-archive-20260702-v2/status.json`. Do not interrupt it or change its frozen release for this test.


## Pilot accepted; first production bucket launched — 09:20 PST

Exact comparison passed at 09:18:25 PST: 23,877,125 move rows and 20,371,938 auxiliary rows, with zero differences in either direction and all non-timing statistics equal. Acceptance: `D:/chess/mini-pool-20261123/pilot-accepted.json`. Aggregation took 1,465.032s; total remote job 1,538.622s. Peak whole-job commit was 19,001,532,416 bytes (17.70 GiB); sampled spill peaked at 2,954,657,792 bytes. Collection and exact comparison are complete.

Production bucket 000 is now running with the same code/runtime/settings: job `7ac943ca7fbc48edb076f10a3b4c478d`, request `d925593b89ca467da4827457c9ee592d`, submitted 09:20:10 PST. Manifest/submission: `D:/chess/mini-pool-20261123/b000-manifest.json` and `b000-submission.json`. Bundle SHA-256 `01cb524dc520830465c692ddda439df389703d14c56530ddc0f199102fed7748`; 3,202,721,723 bytes; packaging 206.453s. The eight original production buckets remain untouched. No global final merge is queued. Further automatic bucket scheduling still needs its own durable coordinator; bucket 000 alone is currently submitted.
