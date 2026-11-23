# Lossless ChessDB Parquet archive

Maintained implementation: `python/chessdb_archive*.py`, `python/chessdb_import/cpp/cdb_archive.cpp`,
and `rust/chessdb_archive`. Run long jobs only from a frozen release containing all these tools,
the native binaries, `month_archive.py`, decoder source archive, and `release.json`.

## Contents and exactness

All original logical key/value entries are retained; no opening, rating, frequency, evaluation,
ply, legality or variant filters apply. This preserves the logical database, not its original
physical SST layout. Original database metadata and the upstream checksum manifest are also archived.

Each row has original `source_key` and `source_value` binary fields, canonical-orientation `fen`,
`moves` (list of structs with reader-convention `uci` and side-to-move `score`), `cdb_ply`, and
`cdb_mirror_ply`. Unknown non-position records retain their bytes with null chess fields/empty moves.
The position's color/rank mirror is implicit, avoiding duplicated move lists; its distance is explicit.
Ply fields are distance-to-root metadata, **not engine analysis depths**. Decoded scores follow cdbdirect's
backpropagation/sentinel normalization; original encodings remain recoverable from `source_value`.

Reader protocol CDBARC02: eight-byte header, then five uint32-little-endian-length-prefixed fields:
key, value, canonical FEN, scored moves including the a0a0 distance sentinel, mirrored distance text.
The reader independently hashes original bytes and the entire decoded stream. Rust recomputes both
digests during writing and during a separate Parquet audit. All must agree. Periodic reader samples
compare both distance fields and moves with the original cdbdirect implementation.

## Resume/publication contract

- Exclusive OS locks cover both archive and local spool. Windows job lifetime kills child workers when
  the coordinator dies; the source reader only writes its pipe, so a broken pipe ends remaining Linux work.
- A full run freshly hashes every source payload against the upstream manifest. Read-only mount and
  file identity checks continue during the run. Source/code/schema/chunk settings are identity-bound.
- SST-derived actual visible key boundaries partition the entire snapshot. Chunks continue at the first
  unconsumed key, with strict increasing keys and no overlapping/gapped checkpoint chains.
- Native output is audited in the local spool, copied to F: with a durable flush, and read back using
  unbuffered SHA-256. Only then is the per-shard receipt published. Startup rehashes completed archive
  files; same-size/same-mtime changes are not accepted. Completed output rechecking adds restart time.
- Partial local/archival files have no accepted receipt. Only narrowly identified files owned by the
  locked archive are cleaned up. No source file is ever deleted by this program.
- Defaults: four workers, 20M records/shard, Zstd 9, 24 GB spool cap, 250 GB D: reserve, 500 GB F: reserve.
  Local staging holds active shards only. Copies to the HDD are serialized. A reserve violation stops
  the run with prior receipts retained; it does not silently choose another destination.
- Full publication requires exact file/receipt sets, every source range exhausted, and exactly
  63,996,865,341 records. `ARCHIVE.COMPLETE.json` is written last. Bounded tests use a different marker,
  `BOUNDED.TEST.COMPLETE.json`, and cannot claim a full archive.

## Commands

From a frozen release using the project's Windows Python:

    python chessdb_archive.py --out F:/chess/chessdb-archive-20260702-v1 --spool D:/chess/chessdb-archive-spool-v1 --stop-file D:/chess/chessdb-archive.STOP

Reissue the same command to resume. Create the stop file to stop after active shards; remove that explicitly
requested stop file before a later resume. `--max-new-chunks N` runs a bounded head start without changing
the archive identity. A stopped run has status `stopped-at-checkpoint`, not a completion marker.

After completion, verification does not require the original source or WSL:

    python chessdb_archive_verify.py --out F:/chess/chessdb-archive-20260702-v1 --deep --report D:/chess/chessdb-archive-final-verification.json

Default verification checks coverage/manifests and hashes every archive file with unbuffered reads.
`--deep` also reconstructs native logical digests. Retain original copies until full archive acceptance.

`_test_chessdb_archive.py` covers native round trips, corruption, coverage and limits.
`_test_chessdb_archive_e2e.py` explicitly uses fresh bounded test directories and a recent full source
verification receipt to compare a forcibly interrupted/resumed real export with an uninterrupted one.
It is not part of lightweight default tests and must be invoked with explicit data/release paths.


## Parallel independent verification

The verifier also supports `--deep --workers 4 --scratch D:/chess/chessdb-final-audit-spool --reserve-gb 250 --staging-gb 24 --progress STATUS.json`. Choose worker count with a same-shard benchmark. A single reader bypasses the Windows cache, checks archive bytes and stages those exact bytes to NVMe; concurrent native auditors read the staged files, checking their physical SHA-256, row/key bounds and both logical digests. Full manifest, inventory, provenance and coverage checks still precede success. Parallel mode does not issue concurrent reads against the archive HDD.

Use a separate frozen verifier release when the exporter is already running. The verifier checks that its native auditor matches the binary pinned by the archive identity. Scratch is locked, disjoint from the archive and verifier release, bounded by total staged bytes and a free-space reserve. Successful staged files are removed only from their invocation-owned directory. Failed/interrupted files remain for inspection, count against the cap, and are never treated as verified checkpoints. A restarted verifier rechecks the complete archive. `python/_test_chessdb_archive_verify.py` covers serial/parallel equivalence and corruption/coverage/resource failures.
