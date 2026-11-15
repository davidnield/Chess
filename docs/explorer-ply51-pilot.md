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

