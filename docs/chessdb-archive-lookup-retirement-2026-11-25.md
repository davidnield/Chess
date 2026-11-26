# ChessDB archive lookup and raw-copy retirement

David accepted replacing the native lookup dependency, validating correctness and speed, then retiring both redundant source copies and reclaiming the WSL virtual disk. The full verified F: archive, menus/evaluation lookups, manifests, code and receipts remain. Keep both pool queues running; do not shut down WSL if unrelated work requires it.

## Tracked plan

- [x] Confirm archive acceptance and raw-copy inventories; confirm no running native ChessDB reader.
- [x] Implement a Rust archive query command using the existing ordered shard bounds and Parquet row-group metadata.
- [x] Test encoding, mirror orientation, scores/distances, missing positions and failed/incomplete inputs; preserve existing archive writer/auditor behavior.
- [x] Compare a diverse, frozen real-position panel against the native reader; benchmark first-use and warm batches from F: and document cache limitations.
- [ ] Freeze executable/index/binding and provide a maintained TSV import adapter, with a checked resume identity.
- [ ] Recheck archive, native consumers/mounts, exact deletion targets and retained provenance immediately before cleanup.
- [ ] Delete only the approved H: payload directory and native WSL payload directory; record reclaimed space.
- [ ] Reclaim unused VHD space without deleting the Ubuntu distribution or interrupting unrelated workloads; verify remaining readers/jobs and update the handoff/monitor.

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
