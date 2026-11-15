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

Use the November 15 pins below for this generation. The Saturday pin predates the check for conflicting parents or child keys within a single extraction chunk. The March production-sized pilot used the fixed executable at Rust commit `b54e3a0`, SHA-256 `c4b03572818a713c3e40cb85b80f88e518641f8fd9584e070e91a372a5463e74`. The native eval executable has a separate pin at `27c68f3`. Training spans January 2013 through July 2026 and covers 4,530,190,835 raw Blitz, Rapid and Classical games in 2,525 local partitions across 163 months. Source counts describe the cleaned games retained by the project's original ingestion recipe.

| State or artifact | Path |
|---|---|
| Extraction, pilot coordinator, fingerprints, independent input inventory, direct cap test and pilot measurements | `D:\chess\bin\ply51-pilot-20261115` |
| Active migration status, forecast, pilot verification and worker logs | `D:\chess\ply51-migration-20261115` |
| Verified monthly archives and per-month hash receipts | `F:\chess\explorer_brc_p51_pilot` |
| Three-month and training explorers | `E:\chess\position-stats\brc-p51-pilot-20261115` and `E:\chess\position-stats\brc-p51-train-201301-202607-20261115` |
| Cloud/Fishnet and diagnostic ChessDB workers and checksums | `D:\chess\bin\ply51-evals-20261115` |
| Separate eval status and measured scratch layout | `D:\chess\ply51-migration-20261115\evals` |
| August–September independent inventory receipts, upon reconciliation | `D:\chess\ply51-migration-20261115\holdout-inputs` |

March's 51,436,065 raw input games took 1,192 seconds to extract. The archive receipt covers 89,935,988,831 bytes, including 1,685,519,477 move rows and 50,473,854 terminal rows. Streaming took 685 seconds and the SHA-256 readback took 210 seconds. See the measured JSON for June and April as they finish. Disk projections become actionable at `forecast.json` only after merging and independently verifying all three months. Cap tests on 1,162 games (three speeds across three months plus controls) compared all move and terminal counts at plies 30 and 50 against directly capped extractions.

### How the queue advances

1. The three original pilot months publish individually with SHA-256 readback. The migration coordinator repeats a real March invocation, merges the three months, verifies all move/outcome sums and sliced terminal digests, checks selected move digests and replays sampled positions, then writes the forecast and pilot verification receipt.
2. Capacity permitting, training extraction starts on Monday, November 16, at 00:00 Pacific, with new batches bounded by the reserved window through Thursday, November 19. It skips and revalidates previously published months. If the window runs out, the coordinator records the remaining work. The production merge checks E: against measured output and D: against actual terminal staging. Existing repertoire artifacts can continue to use the previous generation.
3. The independent eval worker waits for `training explorer verified` and the matching book metadata digest. It first measures eight distributed eval buckets against all 512 child-source buckets. It chooses scratch placement using those measurements and remaining D:, H: and E: space. The whole generation uses refreshed October Cloud and the original Fishnet dump. The executable checks payload SHA-256, individual receipts, the exact pinned revisions and explorer publication on resume. The completed DB is independently checked against the raw sources. The separate lookup retains both hash halves, authoritative mate overrides and source labels. Its production report checks scores, source and mates against the published arrays.
4. WSL 2 and Ubuntu are available. ChessDB's empty LOCK has been restored and all 291 declared sizes and recorded digests have been matched to the exact remote revision. The fresh 1.188 TB readback publishes its own completion receipt. The offline probe then opens the snapshot read-only and tests the fixed 1,000 previously missing parent positions. ChessDB integration depends on interpreting this report. It does not block Cloud/Fishnet.
5. The official August–September PGNs have a separate checksum and event-header audit. The local ingestion accepts specific rated event labels and can exclude tournament-labelled games. The audit reconciles each speed against the local Parquet footers and each full official month against the published total. The holdouts never enter the training merge.

### After interruption

Inspect the process command lines and the JSON status files first. If an existing worker is alive, leave its slot alone. For the November 15 pinned pilot, run the following commands in PowerShell if the corresponding workers have stopped. If three measurements already exist in order (2026_3, 2024_6, 2020_4), skip the pilot script. A manually restarted migration can reuse a complete pilot receipt and complete month or merge checkpoints.

```powershell
$pilotPin = 'D:/chess/bin/ply51-pilot-20261115'
$evalPin = 'D:/chess/bin/ply51-evals-20261115'
$venvPython = 'C:/Users/David/Documents/Chess/.venv/Scripts/python.exe'
pwsh -NoProfile -File "$pilotPin/run_ply51_pilot.ps1"
& $venvPython -u "$pilotPin/run_ply51_migration.py"
& $venvPython -u "$evalPin/run_ply51_evals.py"
```

The detached workers already launched Sunday have separate stdout and error logs. The commands above are the sequential recovery path. If a worker reports a changed digest, a capacity shortfall, or the end of its reserved window, inspect the specific receipt or resource before resuming. The active playing repertoires and the corrected Black 0.30 experiment have no dependency on publishing the new training data.
