# Query the lossless ChessDB archive without WSL

`chessdb-archive-query` is a second binary in this crate. It leaves the archive
writer/auditor unchanged. Build from the repository root with `cargo build
--manifest-path rust/chessdb_archive/Cargo.toml --release --locked --offline
--bin chessdb-archive-query`. The accepted Windows build uses the default MSVC
target/CRT; no claim is made that the crate-local target configuration is loaded
when Cargo runs at the repository root.

Production release and binding: `D:/chess/bin/chessdb-archive-query-20261125-v1`.
The full archive remains at `F:/chess/chessdb-archive-20260702-v6-zstd3`.

## Supported interfaces

- `index ARCHIVE DEEP_AUDIT OUTPUT.json`: verify all receipts, totals and sorted
  nonoverlapping shard bounds, then publish a small immutable index.
- `query INDEX INPUT.jsonl OUTPUT.json`: each input contains `fen` and optional
  `id`. Output preserves input order, including duplicate requests.
- `serve INDEX`: persistent process; one JSON array of requests per stdin line,
  one JSON result per stdout line. The adapter below bounds batch memory.
- `encode INPUT.jsonl OUTPUT.json`: source keys and mirror flags, for audits.
- `lookup-tsv INDEX INPUT.tsv OUTPUT.tsv`: direct small-batch TSV compatibility.
  This command loads its requests into memory; use the adapter for large inputs.

For maintained, bounded and receipted TSV imports:

```powershell
& C:/Users/David/Documents/Chess/.venv/Scripts/python.exe `
  D:/chess/bin/chessdb-archive-query-20261125-v1/archive_lookup.py `
  --binding D:/chess/bin/chessdb-archive-query-20261125-v1/binding.json `
  --input C:/path/positions.tsv --output C:/path/NEW-results.tsv
```

Input is signed `lo<TAB>hi<TAB>FEN`; output is
`lo<TAB>hi<TAB>ply<TAB>uci:score;...`. The IDs are passed through, not recomputed.
The default batch size is 512 (maximum 4096). Existing output requires a matching
receipt binding the input, executable/index binding, adapter and output hashes.
Unreceipted partial/crashed results fail closed: preserve them and choose a new
output. No per-batch checkpoint is claimed. Historical frozen WSL import/scan
releases still refer to the retired source and must not be relaunched. Existing
menus, evaluation arrays and current pools do not use those sources. New large
full-archive jobs should scan Parquet sequentially rather than issue random
position lookups through this adapter.

## Semantics and limits

Supports standard-chess four/six-field FENs. It preserves the four position
fields exactly after whitespace normalization, including pseudo-legal en
passant and historical adjacent empty-square runs. Those spellings are part of
the original source key; normalizing them could silently query another record.
Variant castling fields and malformed inputs are errors, never absent results.
Counters are ignored. Keys use the pinned cdbdirect canonical rank/color mirror.

Moves use the caller's orientation. Already-decoded side-to-move scores are
copied unchanged, including mate/tablebase values: no second backprop or sign
flip. The two distance columns preserve the requested orientation. `ply=-2`
matches native missing/empty-value behavior; JSON additionally distinguishes an
existing empty-value record using `record_present`.

Every process verifies the complete shard size/mtime inventory and completion
marker; queries recheck each accessed shard. Row-group pruning requires exact
byte-key statistics; absent/inexact statistics cause a scan, not a false miss.
The index binds the accepted independent deep audit and its original receipt
inventory. Runtime stat checks are **not** a fresh full-byte integrity audit;
run the retained archive verifier when one is required. A moved/restored archive
needs a new index/binding after its identity is verified. Metadata cache is
bounded to 16 shards; memory and throughput depend on batch locality.

Verification evidence: `D:/chess/acceptance/chessdb-archive-query-20261125`.
The panel, independent native answers/encoder, reader results, timing report,
final adapter receipt and source/executable hashes are retained there.
