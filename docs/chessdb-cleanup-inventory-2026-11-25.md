# ChessDB source-copy cleanup inventory — November 25, 2026

Read-only preparation; **no source deletion or WSL compaction performed**.

## Accepted replacement archive

`F:/chess/chessdb-archive-20260702-v6-zstd3` contains 3,345 Parquet shards,
63,996,865,341 logical records and 2,093,381,470,759 Parquet bytes. The independent
deep audit `D:/chess/chessdb-archive-independent-verification.json` reports
`verified: true`, `deep: true`, `bounded: false`, `original_source_required: false`.
Its completion-marker hash was rechecked on November 25:
`a19025416d8f7adbb60c16ba72b6a5899907539383533c9e1ce9553ffaebc13e`.

Ordinary PyArrow reads succeeded on eight records apiece from:

- `parquet/r000/c000000.parquet` (20,000,000 rows / 611 row groups).
- `parquet/r139/c000003.parquet` (20,000,000 rows / 611 row groups).
- `parquet/r278/c000004.parquet` (9,193,971 rows / 281 row groups).

All have raw binary `source_key`, raw binary `source_value`, string `fen`, structured
`moves` with UCI/score, and both `cdb_ply` and `cdb_mirror_ply`. These bounded reads
confirm normal access, not a replacement for the completed full audit. Raw bytes
preserve the source; derived FEN spelling and scores are not a substitute for them.

## Exact obsolete-copy candidates

| Path | Observed contents / size | Recommendation |
| --- | --- | --- |
| `H:/chess/chessdb/chess-20260702/data` | Regular directory, 582 files (291 payloads + 291 `.ok` sidecars), 1,188,168,027,771 bytes | First cleanup candidate after explicit source-copy retirement approval; retain parent manifests/provenance. |
| Ubuntu `/var/lib/chessdb/chess-20260702/data` | Resolved native directory; `du -sb` 1,188,168,027,771 bytes; read-only bind at `/mnt/chessdb-ro` | Retain for now: existing native point-lookup/import tools still use this layout. Parquet scans are available, but this is not an indexed random-lookup replacement. |
| `E:/wsl/Ubuntu/ext4.vhdx` | Host file 1,194,949,869,568 bytes | Never delete the distro/VHD as a cleanup shortcut. Removing native source files does not promise equal immediate host-space recovery; controlled trim/compaction would be separate work. |

`/var/lib/chessdb/COPY.DONE` records 2026-11-21T18:00:34-08:00. No native
`cdb_scan`, `cdb_lookup` or `cdb_archive` reader was active at inventory time.
A legacy cache-trimming batch loop remains, unrelated to a live conversion.
Recheck consumers, mounts and exact paths immediately before any later cleanup.

## Preserve

- Entire accepted F: archive, its receipts, identities and provenance.
- `H:/chess/chessdb/manifest.tsv` and provenance outside the candidate payload directory.
- Frozen archive producer/verifier releases and native reader source/binaries.
- `E:/chess/chessdb-menus-20260702`, its publication receipts, and the accepted
  `D:/chess/eval-lookup-p51-cdb2-20261122` candidate lookup.
- `D:/chess/acceptance/chessdb-takeover-20261122-final-b4c90b5d923e` and earlier
  import/native provenance, including the documented historical scan limitation.
- Current source policies and engine-result caches; archive retirement does not
  authorize changing lookup bindings or adopting the proposed source blend.

Deleting the H: payload would recover about **1.19 TB decimal / 1,106.56 GiB** while
retaining the native WSL database and verified archive. Current pool construction has
sufficient space without this deletion, so there is no need to combine irreversible
cleanup with the ongoing diagnostic. Retiring the second native copy should follow
an explicit decision about future point-lookup needs and a measured replacement path.
