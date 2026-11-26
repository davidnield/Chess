# ChessDB source-copy cleanup inventory — November 25, 2026

**Completed November 25:** David authorized retirement after a working archive
lookup replacement passed verification. Both raw payload directories are now
deleted, and Ubuntu's VHD was compacted successfully. See
[the implementation and retirement record](chessdb-archive-lookup-retirement-2026-11-25.md).
The inventories below record what was removed; the archive and all accepted
menus/evaluation/pool products remain intact.

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

## Exact retired copies

| Path | Observed contents / size | Recommendation |
| --- | --- | --- |
| `H:/chess/chessdb/chess-20260702/data` | Regular directory, 582 files (291 payloads + 291 `.ok` sidecars), 1,188,168,027,771 bytes | Deleted after parity and final inventory checks; parent manifests/provenance retained. |
| Ubuntu `/var/lib/chessdb/chess-20260702/data` | Same 582 files / 1,188,168,027,771 bytes; two stacked read-only binds at `/mnt/chessdb-ro` | Both exact binds unmounted; payload deleted. Native tools retained, but new lookups use the archive reader. |
| `E:/wsl/Ubuntu/ext4.vhdx` | Before compaction 1,195,016,978,432 bytes; after 6,697,254,912 bytes | Distro/VHD preserved. `fstrim` followed by WSL 3.0.5 `--manage Ubuntu --compact` reclaimed 1,188,319,723,520 bytes. |

`/var/lib/chessdb/COPY.DONE` is now `COPY.RETIRED-20261125`, preserving the old
receipt without falsely advertising an available source. `RAW.RETIRED-20261125.json`
points to the replacement. No native reader/open source files were found at final
preflight. Terminating the idle Ubuntu distro also ended the obsolete cache-trim
loop; neither Windows pool job depended on it. The distro boots after compaction.

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

Receipts, exact inventories, non-SST metadata/sidecars, native query binary and copy
receipt are retained at `D:/chess/acceptance/chessdb-archive-query-20261125`.
The frozen replacement is `D:/chess/bin/chessdb-archive-query-20261125-v1`.
Approximately **2.38 TB decimal / 2.16 TiB** was reclaimed across H: and E:.
Do not restart historical native imports, scans or archive conversion against the
retired paths. Use the new receipted TSV adapter or a sequential Parquet scan.
