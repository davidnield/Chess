# Ply-51 archive pilot

David approved the pilot and the extraction settings on 14 November 2026. Extract through **ply 51 inclusive**. A pool capped at 50 uses recorded departures on ply 51 to count games that continued. Use 128-bit position keys, ply-keyed move rows, and terminal counts by speed and game-mean rating band. The three events in extractor order are Blitz, Classical and Rapid.

## Input controls

The table gives raw game counts before filtering. Each current partition list was compared with the same month in the independently retained six-speed extraction provenance. Counts are from the current Parquet footers.

| Month | Source files | Blitz | Classical | Rapid | Total |
|---|---:|---:|---:|---:|---:|
| March 2026 | 27 | 37,604,136 | 532,211 | 13,299,718 | 51,436,065 |
| June 2024 | 27 | 38,084,761 | 493,298 | 11,667,117 | 50,245,176 |
| April 2020 | 19 | 27,206,267 | 934,611 | 7,443,334 | 35,584,212 |
| June 2013, historical availability control | 2 | 86,804 | 76,031 | absent | 162,835 |

The launcher checks partition names against the reference, records file sizes, modification times and footer row counts, and checks the extractor's provenance against that inventory. The SHA-256 readback applies to the published output. At restart each completed month's receipt and archived bytes must agree before the launcher releases leftover staged files.

## What a filtered pool counts

The `book-v3` terminal key contains both halves of the position key, terminal kind, reason, end ply, event ID, and rating band. Event IDs index `term_event_ids` in the book metadata. For games with at least one parsed move, the pool assigns real endings at or before its cap to normal, time forfeit, or other. A parse failure after a recorded move contributes to other. A game with a recorded next move contributes to horizon at the cap. The move statistics still include earlier moves of games that eventually ended on time. The existing pool exporter still emits 64-bit keys and excludes known collision parents. End-to-end 128-bit repertoire selection is separate work.

## Verification and launch record

The release executable identifies its Rust source as `57412e594039`. The launcher and coordinator are pinned at `cf0ac1e3572e`. The executable and all three runtime scripts have SHA-256 fingerprints in `pin.json`.

The restart tests interrupted publication on both sides of the destination marker, resumed from the stage, and detected same-size archive corruption. A six-game end-to-end fixture exercised all three speeds, two rating bands, a flag on ply 50, continuation on ply 51, and a parse failure on move 51. The filtered Blitz/1800 result at the ply-50 position was one White flag win, one Black win in the other-ending bucket and one continuing draw.

An independent python-chess replay used the first 100 rows of the first Parquet partition of each pilot speed in March 2026. Of the 300 sampled games, 295 passed the extraction filters. Every terminal key and outcome count agreed with the release executable, including both hash halves, speed, band, terminal kind, reason and end ply. This was a source-format and correctness check. Use the full months to estimate production costs.

The coordinator started on 14 November at 20:53 Pacific. Its first live status recorded 16 active builder, scorer or engine processes. Current status is in the pinned directory's `pilot.log` and `pilot.err.log`.

## Run and resume on David's PC

The approved pilot covers only the three modern months, in the order above. The coordinator waits for the existing builders, scorers and Stockfish work to finish, then checks CPU, available physical memory and commit headroom. It runs one month at a time with 12 threads and a 55 GB extraction budget. Staging, copying and readback finish for each month before it times the next one.

| Item | Location |
|---|---|
| Pinned pilot code, executable, checksums, logs and measurements | `D:\chess\bin\ply51-pilot-20261114\` |
| Raw games | `D:\data\chess\standard-chess-games-compressed\` |
| Independent six-speed monthly provenance | `F:\chess\explorer_banded\rust_p30k\_provenance\` |
| NVMe staging | `D:\chess\explorer_brc_p51_pilot_stage\` |
| Verified archive | `F:\chess\explorer_brc_p51_pilot\` |

After an interruption, check that the pilot coordinator is no longer running, then run the pinned `run_ply51_pilot.ps1` again. Months already archived are verified, completed staged months are copied, and an unfinished extraction restarts that month. Read `pilot-measurements.json` in the pinned directory for per-month timings, disk footprint, and peak extractor commit. The archive receipts retain the measurements across an interruption between publication and cleanup.

Before scheduling all months, compare the observed extraction, streaming write and readback times, peak commit and output sizes with available space and the historical input volume. The streaming measurement includes source reads and hashing. Output readback can benefit from Windows' file cache. The existing lookahead-floor books are experimental controls. A switch of either playing book requires the separate horizon and candidate-menu corrections, receipt checks, and Black 0.30 pilot.


## Sunday, November 15 migration and restart

Use the November 15 pins below for this generation. The Saturday pin predates the check for conflicting parents or child keys within a single extraction chunk. The March production-sized pilot used the fixed executable at Rust commit `b54e3a0`, SHA-256 `c4b03572818a713c3e40cb85b80f88e518641f8fd9584e070e91a372a5463e74`. The compact merger and native eval reader share release `b72b0dc`. The extraction worker uses its original pin. Training spans January 2013 through July 2026 and covers 4,530,190,835 raw Blitz, Rapid and Classical games in 2,525 local partitions across 163 months. Source counts describe the cleaned games retained by the project's original ingestion recipe.

| State or artifact | Path |
|---|---|
| Extraction, pilot coordinator, fingerprints, independent input inventory, direct cap test and pilot measurements | `D:\chess\bin\ply51-pilot-20261115` |
| Active migration status, forecast, pilot verification and worker logs | `D:\chess\ply51-migration-20261115` |
| Verified monthly archives and per-month hash receipts | `F:\chess\explorer_brc_p51_pilot` |
| Three-month and training explorers | `E:\chess\position-stats\brc-p51-pilot-20261115` and `E:\chess\position-stats\brc-p51-train-201301-202607-20261115` |
| Original eval release and diagnostic ChessDB tools | `D:\chess\bin\ply51-evals-20261115` |
| Separate eval status and measured scratch layout | `D:\chess\ply51-migration-20261115\evals` |
| August–September independent inventory receipts, upon reconciliation | `D:\chess\ply51-migration-20261115\holdout-inputs` |

March's 51,436,065 raw input games took 1,192 seconds to extract. The archive receipt covers 89,935,988,831 bytes, including 1,685,519,477 move rows and 50,473,854 terminal rows. Streaming took 685 seconds and the SHA-256 readback took 210 seconds. See the measured JSON for June and April as they finish. Disk projections become actionable at `forecast.json` only after merging and independently verifying all three months. Cap tests on 1,162 games (three speeds across three months plus controls) compared all move and terminal counts at plies 30 and 50 against directly capped extractions.

### How the queue advances

1. The three original pilot months publish individually with SHA-256 readback. The migration coordinator repeats a real March invocation, merges the three months, verifies all move/outcome sums and sliced terminal digests, checks selected move digests and replays sampled positions, then writes the forecast and pilot verification receipt.
2. Capacity permitting, the remaining training extraction starts after sign-in on Monday, November 16, with new batches bounded by the reserved window through Thursday, November 19. It skips and revalidates previously published months. If the window runs out, the coordinator records the remaining work. The production merge checks E: against measured output and D: against actual terminal staging. Existing repertoire artifacts can continue to use the previous generation.
3. The independent eval worker waits for `training explorer verified` and the matching book metadata digest. It first measures eight distributed eval buckets against all 512 child-source buckets. For temporary child files it tries H:, D:, E:, then F:. The measured forecast adds 20%, reserves 150 GB on each drive and first allocates the source shards on H: and the DB and lookup on D:. If F: is selected, the files go under `F:/chess/eval-work-p51-20261115-child`. The worker monitors the selected drive with the existing 80 GB runtime stop. The whole generation uses refreshed October Cloud and the original Fishnet dump. The executable checks payload SHA-256, individual receipts, the exact pinned revisions and explorer publication on resume. The completed DB is independently checked against the raw sources. The separate lookup retains both hash halves, authoritative mate overrides and source labels. Its production report checks scores, source and mates against the published arrays.
4. WSL 2 and Ubuntu are available. ChessDB's empty LOCK has been restored and all 291 declared sizes and recorded digests have been matched to the exact remote revision. The fresh 1.188 TB readback publishes its own completion receipt. The offline probe then opens the snapshot read-only and tests the fixed 1,000 previously missing parent positions. ChessDB integration depends on interpreting this report. It does not block Cloud/Fishnet.
5. The official August–September PGNs have a separate checksum and event-header audit. The local ingestion accepts specific rated event labels and can exclude tournament-labelled games. The audit reconciles each speed against the local Parquet footers and each full official month against the published total. The holdouts never enter the training merge.

### Compact merged explorer

The November 15 queue uses the three original monthly archives for a full rehearsal. The reference is the published book-v3 pilot on E:. The independent book-v4 pilot writes to `D:/chess/ply51-migration-20261115/compact-pilot-book`. Its native merger at `b72b0dc` writes rank strings, the EPD state, and two parent-to-child XOR columns directly into each move slice. Each completed bucket has a readback digest over the original logical values. Prepared pools, the uncut move graph and the eval lookup retain their ordinary column formats. DuckDB consumers can use `explorer_storage.relation(files)` and PyArrow consumers can use `read_columns(file, columns)`.

The three initial native buckets contained 25,752,025 move rows. They occupied 909,243,601 bytes, compared with 1,527,409,674 bytes in the reference. The native eval A/B read bucket 7 on the same SSD, used the same 262,144-row excerpt from each of Cloud and Fishnet and ran four alternating trials. All 105 stored evaluation rows matched. Mean wall time rose from 3.93 to 4.85 seconds. This sample measures the eval build path over one bucket. The timing of a full eval generation depends on its source scan, I/O and join volume.

The rehearsal checks all 512 move digests, the terminal digest, replayed positions, and every logical column of 81 files using DuckDB and PyArrow. The comparison and the whole-directory measurements are bound in `compact-verified.json`. Its `compact-forecast.json` includes terminal files and metadata. The queue uses the verified monthly forecast for the November 16–19 production window. After archival it checks the compact certificate and real available space before permitting the production merge. The eval worker waits for the verified production explorer. The files `status.json`, `compact-pilot-status.json`, and `evals/status.json` describe those three workers.

### Sunday head start and Monday sign-in

On November 15 David authorized a bounded training batch after the compact pilot verifies. The production coordinator resumes on Monday morning after sign-in. The Sunday worker uses the original extraction pin and the fourteen July training months from 2013 through 2026. They span the archive and total 396,346,195 source games. Each month finishes its F: SHA-256 readback before the next begins. The worker checks projected time against the evening cutoff, admits its last month before 8 p.m. Pacific and has a 9 p.m. hard stop. Its status and completed month list are in `D:/chess/ply51-migration-20261115/sunday-head-start/status.json`. To finish early, create an empty `stop-after-month` file in that same directory.

On Monday, sign in and run `D:/chess/bin/ply51-sunday-20261115/Resume-Trip.cmd` once. It checks the pinned releases and attached drives, recovers the compact pilot if needed, and starts missing training and eval workers in the background. It also resumes the independent August–September audit if its receipts are incomplete. The training coordinator counts and revalidates the months already published. The command can be run again after an interruption during the reserved window. `Resume-Trip.cmd --check` validates the paths and pins without starting jobs.

### End-to-end rehearsal on the July archives

On November 15 the complete July 2013 extraction was repeated from its 217,505 raw games. The 512 move files and one terminal file all matched the published archive byte for byte. A separate sample took 4,656 games across all 221 source partitions in the 14 July months. Of these, 4,634 passed the production filters. Python-chess replay agreed with all 192,858 compact move rows and 4,632 terminal rows, including both hash halves, outcomes, speed and rating band. Raw game lengths reconciled with every usable cap from 1 to 50. At cap 50 there were 3,056 continuations and 1,578 real endings. The production input inventory was also reconciled for all 163 months and 2,525 files.

The shortened Cloud and Fishnet inputs produced 6,004 eval rows and 6,413 lookup entries. The separate production preflight used all 20 Cloud and 144 Fishnet files and their original receipts. The rehearsal built a 50-ply 1800+ pool, ran all four pool acceptance checks, recovered uncut moves for both colors, and built and scored two small books to ply 30. The referee used separate July games, one seed, one sampled continuation per branch, and no fresh Stockfish searches. The builder used an empirical seed, a 30-decision budget and a three-observation move threshold. The Black 0.30 pilot, full-size timing, disk high-water marks, and production book comparisons remain separate.

Three downstream corrections came from this run. The eval verifier fills overlapping sample strata with distinct additional positions. The pool exporter creates a fresh output directory before publishing. Uncut move recovery accepts index rows that record zero files and zero bytes, and still checks each nonempty slice. The corrected verifier is in the eval release used by the Monday launcher. The source sample, replay scripts, logs and machine-readable results are in [the rehearsal directory](D:/chess/ply51-migration-20261115/headstart-e2e).

### After interruption

Check the three status files and active process command lines. Use the command for the stopped worker in a separate PowerShell window. A completed compact pilot has `compact-verified.json`. The queue reuses verified months and published merge buckets. Its configured extraction window ends at midnight Pacific on November 20.

```powershell
$compactPin = 'D:/chess/bin/ply51-compact-20261115'
$queuePin = 'D:/chess/bin/ply51-queue-20261115'
$evalPin = 'D:/chess/bin/ply51-evals-rehearsed-20261115'
$venvPython = 'C:/Users/David/Documents/Chess/.venv/Scripts/python.exe'

# Three-month rehearsal
& $venvPython -X utf8 -u "$compactPin/run_ply51_migration.py" --compact-pilot-only

# Scheduled training extraction and receipt-gated production merge
& $venvPython -X utf8 -u "$queuePin/run_ply51_migration.py" --compact-release $compactPin

# Cloud/Fishnet eval generation and 128-bit lookup
& $venvPython -X utf8 -u "$evalPin/run_ply51_evals.py"
```

The workers write separate logs under `D:/chess/ply51-migration-20261115` with the prefixes `compact-pilot`, `compact-training`, and `compact-eval-worker`. Their release directories contain their SHA-256 manifests. The active playing repertoires and the corrected Black 0.30 experiment have their own release and validation process.
