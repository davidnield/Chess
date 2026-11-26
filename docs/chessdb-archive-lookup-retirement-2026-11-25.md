# ChessDB archive lookup and raw-copy retirement

David accepted replacing the native lookup dependency, validating correctness and speed, then retiring both redundant source copies and reclaiming the WSL virtual disk. The full verified F: archive, menus/evaluation lookups, manifests, code and receipts remain. Keep both pool queues running; do not shut down WSL if unrelated work requires it.

## Tracked plan

- [x] Confirm archive acceptance and raw-copy inventories; confirm no running native ChessDB reader.
- [x] Implement a Rust archive query command using the existing ordered shard bounds and Parquet row-group metadata.
- [x] Test encoding, mirror orientation, scores/distances, missing positions and failed/incomplete inputs; preserve existing archive writer/auditor behavior.
- [x] Compare a diverse, frozen real-position panel against the native reader; benchmark first-use and warm batches from F: and document cache limitations.
- [x] Freeze executable/index/binding and provide a maintained TSV import adapter, with a checked resume identity.
- [x] Recheck archive, native consumers/mounts, exact deletion targets and retained provenance immediately before cleanup.
- [x] Delete only the approved H: payload directory and native WSL payload directory; record reclaimed space.
- [x] Reclaim unused VHD space without deleting the Ubuntu distribution or interrupting unrelated workloads; verify remaining readers/jobs and update the handoff/monitor.

## Identity and scope

Archive: `F:/chess/chessdb-archive-20260702-v6-zstd3`, 63,996,865,341 records / 3,345 shards / 2,093,381,470,759 Parquet bytes. Accepted deep audit: `D:/chess/chessdb-archive-independent-verification.json`, completion-marker SHA256 `a19025416d8f7adbb60c16ba72b6a5899907539383533c9e1ce9553ffaebc13e`.

Deletion targets after acceptance: `H:/chess/chessdb/chess-20260702/data` and Ubuntu `/var/lib/chessdb/chess-20260702/data`. Each contains approximately 1,188,168,027,771 bytes. Keep `H:/chess/chessdb/manifest.tsv`, all parent provenance and the archive. `E:/wsl/Ubuntu/ext4.vhdx` is a live distribution disk, never a deletion target.

The replacement preserves cdbdirect's source-key canonicalization, rank/color mirroring, side-to-move score convention, empty-value/missing sentinel and both distance fields. Query results use already decoded archive scores, not a fresh engine evaluation or a new blending policy. Original raw key/value bytes remain the recovery authority.

No archival data rewrite, global sort, or new full-data index is planned. The small shard index binds the already audited archive generation and checks the complete receipt inventory. Queries inspect only relevant shard footers and row groups. Missing/corrupt files are errors, not evidence that a position is absent. Runtime size/mtime checks are not represented as a new full-byte audit; the independent full-audit tool remains available.

## Reader acceptance

Evidence root: `D:/chess/acceptance/chessdb-archive-query-20261125`.
Six Rust tests and two Python receipt/binding tests pass. Exact parity on all
971 frozen queries against both the independent pinned C++ key encoder and
native cdbdirect answers: 849 found, 122 missing, 2,108 moves, 142 promotions,
610 mate/tablebase scores. The 848 directly sampled/mirrored archive rows span
32 shards; the complete query panel touches 128 shards and 207 row groups.
Empty-value versus absent distinction is covered by synthetic fixtures (no
empty-value record was present in the real panel). Historical noncanonical
adjacent empty-square runs were found and are preserved in the key encoder.

First-process archive batch 6.783 s; warm batches 2.163 / 2.091 s; native warm
process 2.326 s. Persistent eight-position batch 0.196 s first / 0.041 s repeat.
These are observations with concurrent pool jobs, no OS cache flush and prior
panel sampling, not cold-storage or large-import throughput estimates. Current
menus/evaluation products remain the preferred hot lookup layer.

## Frozen release and retirement outcome

Reader source commit `ca36446` on `codex/takeover-hardening-20261122`.
Release `D:/chess/bin/chessdb-archive-query-20261125-v1` contains the executable,
index, adapter, binding, source ZIP, license and manifest. Final formatted binary
SHA256 `4f0c2272a524c03152b4a1d35c97e03e2916b385aa9006dd157c3a03dce9c0e1`.
Binding SHA256 `1206047173e135c2b603a75d4ff576914213e82a96393e062c7b17e362b4c2cd`.
The frozen adapter's 971-row TSV is byte-identical to the native answer file;
receipt-based reuse passed. `frozen-acceptance.json` binds this final binary,
separately from the pre-formatting benchmark executable. The index is
1,449,475 bytes rather than a per-position index over 64 billion records.

Both exact raw directories were deleted after final dry-run inventory and
open-file checks. Each held 582 files / 1,188,168,027,771 bytes. The two native
read-only bind layers were removed. `COPY.DONE` became `COPY.RETIRED-20261125`;
the distro has a `RAW.RETIRED-20261125.json` pointer to the replacement.
All non-SST source metadata/sidecars plus the download manifest were retained
in `original-source-metadata.zip`. The native query executable and copy receipt
were also copied into the acceptance directory; WSL tool sources remain intact.

H: free bytes increased from 1,708,738,527,232 to 2,896,907,259,904 around deletion
(concurrent pool writes can affect free-space deltas). After guest trim, the
installed WSL 3.0.5 built-in `wsl --manage Ubuntu --compact` completed in 13.33 s.
The VHD shrank from 1,195,016,978,432 to 6,697,254,912 bytes, reclaiming
1,188,319,723,520 bytes on E:. No distro deletion, reinstall, sparse-mode change,
administrator installer or Windows-wide shutdown was needed.

Raw-copy deletion plus VHD compaction reclaimed approximately **2.38 TB decimal
/ 2.16 TiB** across the two drives. Exact source inventories and operation logs
are `windows-retirement*.json`, `native-retirement*.json`, and
`wsl-compaction.json` in the acceptance directory. One-use retirement scripts
are kept in `scripts/retire_chessdb_*_20261125.*`; they deliberately refuse to
operate on another directory or inventory. Never rerun completed conversion or
legacy WSL import/scan jobs against the retired paths.

Final verification: the frozen reader reproduced all 971 native reference
answers **after both source deletions and compaction**, in 1.972 s; all seven
release-file hashes still match. Ubuntu booted, native tool checksum still
matches, retired data and bind mounts are absent. Recorded in
`post-retirement-verification.json`. Pool activity at 18:08 PST: main weak pool
bucket 004 running; mini bucket 361 active/healthy, 355 newly accepted plus eight
originals = 363/512 accepted. No pool restart or interruption was needed.

Free space at that check: D 415.76 GiB, E 2,145.80 GiB, F 942.15 GiB,
H 2,697.96 GiB. Ongoing pool writes will reduce these. Existing hourly monitor
`track-chessdb-and-mini-pool-progress` now records retirement/replacement paths
and continues tracking both queues; it no longer instructs retaining or using
the deleted sources. The main checkout receives mirrored documentation only;
implementation remains on the isolated takeover branch.
