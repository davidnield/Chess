# ChessDB lossless Parquet archive pilot — 2026-11-22

Status: completed; full export and NAS transfer have not started. Both original source copies are unchanged.

## Decision and scope

David requested a bounded pilot of a compressed Parquet replacement for the raw ChessDB NAS archive.
The existing repertoire move-menu tables are not an archive: they retain explorer positions with summed
observations >=2, apply standard-position matching and duplicate resolution, and omit original key/value bytes.

This experiment preserves the exact original database keys and values, including binary zeros and special
encodings. It applies no game-count, opening, rating, ply, evaluation, legality, variant, or move-rank filters
within each sampled range. A converted archive preserves logical records, not the original SST file bytes.

Working directory: `D:/chess/chessdb-parquet-pilot-20261122-v1`.
The Python/C++ harnesses, build script, binary, reader provenance, input identity, original sample frames,
four sets of Parquet files, per-region receipts, tests, and report JSONs are retained there. One-off source
copies are in `scratch/chessdb_parquet_pilot` in the sweep-preparation worktree; they are intentionally scratch,
not a maintained full exporter. The source was opened read-only through `/mnt/chessdb-ro` using the existing
verified WSL copy of snapshot `chess-20260702`.

## Measurements

Twelve 250,000-record contiguous samples start at evenly spaced sorted SST-file boundaries (indices spanning
279 files). All **3,000,000 keys are distinct**. This is coverage sampling, not a probability sample: estimates
are approximate and must not be presented as a statistically unbiased census prediction.

Parquet uses 32,768-row groups, no dictionaries, delta-byte-array encoding for source keys and decoded FENs,
and Zstandard compression. Decoded rows contain original key/value bytes plus the canonical-orientation FEN,
a nested scored-move list, and that orientation's decoded distance-to-root. Mirrored-position information
and all original sentinels remain intact in the binary values. Decoded scores follow the pinned cdbdirect
normalization, so decoded columns alone are not a lossless substitute for those values.

| Layout | Zstd level | Sample bytes | Bytes per source record | Full-size projection, decimal TB | Python write time, summed worker seconds |
|---|---:|---:|---:|---:|---:|
| Exact binary key/value | 3 | 47,575,738 | 15.86 | 1.015 | 12.06 |
| Exact binary key/value | 9 | 45,452,148 | 15.15 | 0.970 | 14.53 |
| Binary plus decoded chess fields | 3 | 97,384,353 | 32.46 | 2.077 | 25.93 |
| Binary plus decoded chess fields | 9 | 90,699,673 | 30.23 | 1.935 | 30.26 |

The existing database occupies approximately 1.19 TB. Thus the binary-only archive appears modestly smaller;
the readable archive is larger because it also preserves original bytes. Across the sampled regions,
Zstd-9 binary size ranged from 12.58 to 16.36 bytes/record and decoded size from 26.37 to 32.43 bytes/record.
These ranges describe samples, not full-dataset confidence bounds.

The entire four-variant pilot, including second source reads and round-trip verification, took **129.6 seconds**
with two concurrent region tasks. Retained pilot artifacts occupy **0.745 GB**. This does not include initial
compilation and fixture tests. Zstd 9 saved about 4.5% versus level 3 for binary and 6.9% for decoded data,
with about 20% and 17% more measured Python write time respectively.

## Verification

- Every sampled original key and value was compared byte-for-byte against a fresh read-only source iterator.
- Every row and field of all four Parquet variants was read back and compared against its original framed sample.
- Independent DISTINCT-key check confirms three million unique source records across the samples.
- Fixture tests cover both layouts and both codecs, embedded binary values, empty values, unknown/non-position
  keys, score sentinels, truncated frames, and deliberately changed Parquet data.
- Deliberately changing a source value in a copied smoke frame makes the C++ verifier fail with source mismatch.
- Completed-region resume rehashes retained artifacts; the real checkpoint passed and a changed identity was rejected.
- Each completed region has source verification, logical-content digest, output checksums, sizes, settings identity,
  and timing receipts. Partial outputs do not have a completion receipt.

Receipts: `report.json`, `additional-verification.json`, `source-tests.json`, `fixture-tests/tests.json`,
`identity.json`, and `region-00.json` through `region-11.json` in the working directory.

## Interpretation and next step

Lossless Parquet is feasible. Binary-only Parquet is portable storage, but interpreting its chess fields still
requires a decoder. The decoded layout permits ordinary analytical tools to read chess fields while retaining
exact bytes as the recovery authority. A future complete decoded schema should expose both orientations'
distance metadata; the prototype exposes the canonical orientation and preserves the rest in source_value.

Do not launch this Python prototype over the full 63,996,865,341-record snapshot. It materializes Python objects,
uses intermediate framed files, and repeats startup/seek work. Extrapolating just its encoding gives 71–179
single-worker hours depending on layout; source reading, verification, and NAS I/O are additional. These are
prototype arithmetic, not production ETAs, and concurrent Python workers can interfere through the GIL.

Next engineering step: a bounded native streaming exporter benchmark, choosing a layout after checking NAS
capacity. Measure sustained parallel throughput and write directly into bounded, independently verifiable shards.
Avoid a complete intermediate dump and a global sort. Freeze schema, reader and source identity, implement
fail-closed coverage/checkpoint publication, and verify all exported source records plus NAS file checksums
before deleting either raw copy. Include the reader/decoder source and database metadata in the archive.

No full conversion, NAS write, source deletion, policy adoption, or repertoire build was performed by this pilot.

## Related completed checkpoint

The evaluation lookup finished at 18:04 PST; its final output audit completed successfully on November 22.
All 512 new buckets verified, including exact comparisons with the 167 preserved old outputs.
Final evidence: `D:/chess/acceptance/chessdb-takeover-20261122-final-b4c90b5d923e`.
Evidence manifest: `1d09e126e4472b4ea1e701ed42892310c9b28ca94433516b79a48b332e56571e`.
The lookup remains candidate-only and existing bindings are unchanged.
