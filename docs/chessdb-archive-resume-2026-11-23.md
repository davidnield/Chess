# ChessDB lossless archive: overnight stop and Monday resume

Prepared November 22, 2026. This is the F: archive job, separate from the completed eval lookup rebuild. No canonical repertoire, active eval source binding, original ChessDB copy, or pool has been replaced.

## Scope and implementation

Archive all **63,996,865,341 logical source entries**, without position, frequency, rating, evaluation, ply or variant filters. Preserve original binary keys and values, plus readable canonical FEN, move/score lists, and distance metadata for both mirrored orientations. The readable score conventions follow the source decoder; raw values preserve otherwise collapsed sentinel distinctions. This is logical database preservation, not a byte-for-byte copy of the SST filesystem layout.

The original source manifest and database metadata are retained with the decoder source, exporter source package, native auditor, and checksums. The archive can be verified and read without the original TerarkDB database. Both original database copies remain untouched; deletion requires a later decision after full archive acceptance.

- Worktree: `C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation`
- Branch: `codex/takeover-hardening-20261122`
- Maintained entry points: `python/chessdb_archive.py`, `python/chessdb_archive_source.py`, `python/chessdb_archive_verify.py`, `python/chessdb_archive_windows.py`, `python/chessdb_import/cpp/cdb_archive.cpp`, and `rust/chessdb_archive/`.
- Frozen production release: `D:/chess/bin/chessdb-archive-20261122-v5`. **Never modify it in place.**
- Release manifest SHA256: `b2c0de4693696926178b35b7bfaca93e8616280c2b6f9da5f86290980e3c1766`.
- Source manifest: `H:/chess/chessdb/manifest.tsv`, SHA256 `5f857194fbc1fdd34d49f14cf13325bca396f593a96f7b8bcd3b52eba104a11e`.
- Original native snapshot: `/var/lib/chessdb/chess-20260702/data`; coordinator recreates its read-only bind at `/mnt/chessdb-ro` after reboot.
- Archive: `F:/chess/chessdb-archive-20260702-v2`.
- Active-shard staging: `D:/chess/chessdb-archive-spool-v2`.
- Settings: four workers; 20,000,000 records per shard; Zstd 9; 24 GB staging cap; retain at least 250 GB free on D: and 500 GB free on F:. Copies to F: are serialized.

## Verification completed before the production head start

- All 291 original payload files passed fresh checksum verification against the upstream manifest. SHA256 applies to SSTs; Git-blob SHA1 applies to the small metadata files. The 291 downloader `.ok` sidecars are separately inventoried, not trusted as proof of source correctness.
- All 16 focused archive regressions passed for the final v5 release.
- Two 2M-record, four-shard test archives were built. The second coordinator was forcibly killed during its third shard. Its Windows child processes exited. The first two committed shards survived unchanged, including modification times. Resuming produced four files byte-identical to the uninterrupted baseline.
- Duplicate coordinator, changed settings, same-size/same-mtime output corruption, and insufficient free-space cases were rejected. Corrupted test data was restored and verified. Test archives cannot claim full-source completion.
- Separate source-independent deep verification passed: 2,000,000 records; four shards; 61,273,889 bytes; no original database required.
- Evidence: `D:/chess/chessdb-archive-tests-20261122/unit-v5/results.json`, `D:/chess/chessdb-archive-tests-20261122/e2e-v5/RESULTS.json`, and `D:/chess/chessdb-archive-tests-20261122/e2e-v5/independent-verification.json`.

## Tomorrow: start or resume

After Windows is up and D:, F: and H: are available, run this in PowerShell:

```powershell
& 'D:\chess\run_chessdb_archive.bat'
```

This uses the frozen v5 release and resumes the existing archive. It runs in the foreground; leave the console open. If starting through an agent's background helper, use a hidden process and retain its PID. Nothing is scheduled to start automatically overnight. Do not use `restart_jobs.bat` for this task: that launcher belongs to the already completed eval lookup and acceptance job.

The full launcher refuses to start while `D:/chess/chessdb-archive.STOP` exists. If a previous deliberate stop left it there, remove only that exact file when authorizing a resume:

```powershell
Remove-Item -LiteralPath 'D:\chess\chessdb-archive.STOP'
```

Monitor:

- `F:/chess/chessdb-archive-20260702-v2/status.json`: state and actual coordinator PID.
- `F:/chess/chessdb-archive-20260702-v2/source-verification.log`: initial source checksum progress.
- `D:/chess/chessdb-archive-production.log`: resumed production log and final independent audit.
- `F:/chess/chessdb-archive-20260702-v2/receipts/r*/c*.json`: accepted shard count and exact records.
- `F:/chess/chessdb-archive-20260702-v2/active/`: currently exporting shard attempts.

Startup **rehashes the full original source** and rechecks completed archive files from F:. This deliberately costs restart time; a nearly complete archive can require several hours of F: reads. It does not regenerate verified shards. Source, release, schema, compression and chunk size are identity-bound; changing them requires a new archive, not silently continuing this one. Worker count and a bounded new-shard budget do not change archive identity.

## Safe stopping and power-off

Request a graceful stop:

```powershell
New-Item -ItemType File -Path 'D:\chess\chessdb-archive.STOP' -Force
```

Wait for `stopped-at-checkpoint` and for the coordinator PID to exit. Active shards finish their write, native audit, F: copy, durable flush and unbuffered read-back before their checkpoints are committed. A stop requested during startup takes effect after source verification. Allow time for that stage rather than assuming the stop file interrupts hashing immediately.

An abrupt shutdown preserves committed shards; up to four in-flight shards may need redoing. Resume detects corrupted completed files and fails instead of silently accepting them. Original database copies are never deleted by the exporter. Clean owned staging remnants are handled on restart.

## Completion and acceptance

Only `ARCHIVE.COMPLETE.json` denotes full source coverage: all 279 key ranges exhausted, exact source count, exact receipt and output sets, and unchanged producer/source identity. A stopped head start intentionally has no completion marker.

After publication, the full launcher automatically runs a source-independent deep audit. Its success report is `D:/chess/chessdb-archive-independent-verification.json`. This adds another full archive read and logical digest reconstruction. Do not treat a batch exit or the existence of Parquet files as full acceptance. No cleanup of original copies is automated.

Planning estimates from the earlier distributed sample: approximately 1.94 TB output; ~55–60 hours conversion at the prototype's native throughput, before production audit/copy overhead and final deep verification. F: had ~3.12 TB free before the head start, implying ~1.18 TB remaining if the size projection holds. These are projections, not a committed completion date; replace them with production measurements when available. Short-range head-start measurements cannot establish the whole-source average.

## Tonight's bounded job

`D:/chess/run_chessdb_archive_headstart.bat` explicitly uses `--max-new-chunks 4`; it cannot automatically continue into the full run. It writes `D:/chess/chessdb-archive-headstart-20261122.log` and saves the actual exit code in `D:/chess/chessdb-archive-headstart-20261122.exit.txt`. Four complete-size shards would retain 80M records for tomorrow. Read the final execution entry below for the observed result.


Implementation committed locally as `6c9843e` on `codex/takeover-hardening-20261122`. The frozen release manifest was rechecked against every packaged file after the commit; all hashes match. A later v5 release fixes the staging cleanup race described below and incorporates the Cargo manifest whitespace cleanup. Every frozen release stays unchanged after creation.


## Final review correction — 2026-11-22 19:56 PST

- Deterministically reproduced a staging-space accounting race: one worker could remove a completed attempt between another worker's `is_file()` and `stat()`, raising FileNotFoundError and unnecessarily aborting the run. The new helper uses one stat per entry and ignores only disappearance; other access errors still propagate. The new regression passes, bringing focused tests to 16.
- Frozen v4 stopped gracefully after verifying all original source files, before exporting any production shards (0 records, 0 receipts). Its source-verification evidence remains in F:/chess/chessdb-archive-20260702-v1. No completed production data was discarded.
- A separate frozen v5 release is used for production, with a fresh output F:/chess/chessdb-archive-20260702-v2 and spool D:/chess/chessdb-archive-spool-v2, preserving strict identity checks. The four-shard head start and tomorrow's launcher use these paths. Never resume the empty v4 archive as the production run.
- Also corrected the head-start batch's exit-code text redirection; the process exit code had been correct, but `echo 0>file` was parsed as redirection and left an empty text receipt.


## Bounded production result â€” 2026-11-22 20:08 PST

- Stopped automatically with exit code 0. Exactly **80,000,000 original records** in **four 20M-record shards** are retained on F: for the next run (2.407 GB compressed).
- Source preflight freshly verified all 291 payloads in 521.0 seconds. Each production shard passed source/writer/native-auditor digest checks and durable F: copy plus unbuffered checksum read-back before receipt publication.
- Coordinator PID 32020 exited. No active attempts, partial F: files or shard staging directories remain. The archive is intentionally incomplete, with no full completion marker.
- Free space: D: 507.1 GB; F: 3.121 TB. Original sources remain intact.
- Exact machine-readable result: D:/chess/chessdb-archive-headstart-20261122-result.json. Resume with D:/chess/run_chessdb_archive.bat tomorrow; no automatic overnight work is armed by these launchers.

Final code commits: `6c9843e` (implementation) and `1cb15dc` (concurrent staging cleanup fix), on `codex/takeover-hardening-20261122`. The final v5 interruption/resume and source-independent audit both passed. The 80M-record production head start spent 521 seconds verifying sources and approximately 177 seconds exporting/auditing/archiving its four shards. These first four key ranges are not a representative whole-database timing sample; retain the broader pilot estimate until the full run supplies sustained measurements. Windows and Linux archive workers were checked after completion; none remained.
