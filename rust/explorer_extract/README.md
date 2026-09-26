# explorer-extract

The explorer extract in Rust. It is exact against `python/build_pooled_stats.py`
at d79a0c7 (the explorer contract) and python-chess 1.11.2, and exists to be
fast: the Python extract spends ~37 µs per ply in pure-Python chess logic.

| Subcommand | What it does |
|---|---|
| `partials` | Drop-in for `build_pooled_stats.py --phase extract` with the explorer flags. Same file names, schemas (`large_string`), 250k-row chunks, `.tmp`-then-rename with ps last, `_DONE` sentinels, stale-chunk resume and the `_extract_params.json` lock (`producer: "rust"`). `--epd-max-ply` is required: 16 matches today's extract, 30 is B1's. |
| `month` | A month's games straight to the finished 512-bucket layout that `backfill_epd.py` and `bucket_month.py` write (EPD at every ply), plus its term monthly. Replaces extract + consolidation + EPD backfill. |
| `dump-plies` | Per-ply and per-game rows for the differential test against python-chess (`python/_test_rust_extract.py --plies-check`). |
| `selftest` | The embedded fixtures, in well under a second. Run it on any machine before real use. |

`--version` prints the crate version, git commit, build date and target features.

## Build

```powershell
$env:RUSTUP_TOOLCHAIN = "stable"            # this machine's stable is the pinned 1.98.1
$env:CARGO_TARGET_DIR = "D:\rust-target\explorer_extract"   # C: is nearly full
cargo build --release --locked
cargo test
```

- `rust-toolchain.toml` pins 1.98.1. Where `stable` already is 1.98.1, setting
  `RUSTUP_TOOLCHAIN=stable` builds with it instead of making rustup fetch a
  second copy under the pinned name.
- `.cargo/config.toml` builds with `+crt-static` (no VCRUNTIME dependency; check
  with `dumpbin /dependents`) and `target-cpu=x86-64-v3`. The binary checks for
  AVX2/BMI2/FMA/LZCNT/MOVBE at startup and exits with a message on an older CPU.
- The binary is never committed; `target/` is ignored.

## The exactness contract, and where each part is held

| Part | Rule | Tested by |
|---|---|---|
| Hash | `zobrist_hash::<Zobrist64>(EnPassantMode::PseudoLegal)` as i64 (polyglot's pawn-adjacency ep rule; `Legal` differs on a pinned ep). Start hash 5060803636482931868. | `polyglot_keys.json` (all 781 keys by polyglot index), the pinned-ep case |
| EPD | `board.epd()`: 4 fields, ep only when legal, taken before the move. Stored as a 34-byte packed position, rendered on write; equals shakmaty's `Epd::from_position(.., Legal)`. | SAN table, real-game fixtures, `dump-plies` diff |
| Tokens | `iter_san_moves`: comments, then variations (not nested), then `$\d+`; result and move-number tests on the RAW token, then `rstrip("?!")`. Python whitespace = `char::is_whitespace` + U+001C..U+001F; `\d` = Python's Unicode 14 Nd (660 code points, `src/pydigits.rs`). Counted to `max_ply + 1` so term kind is right. | `tokenizer.json`, `pyunicode.json` (every code point) |
| SAN | A port of python-chess `parse_san` over shakmaty's legal moves -- never shakmaty's own parser. Castling only via the 12-string table or `find_move` (`e1g1`/`e1h1`); `Kg1` fails; `x`,`-`,`+`,`#` unvalidated; no `P`; `=` optional; `find_move` defaults a back-rank pawn move to a queen and then fails it; pawn captures need a file; null tokens only when the regex fails, and a null move in check stops the run. | `san_table.json` (the spec's table and more), `san_games.json` (real games x token variants), `fuzz_regex.json` (1M tokens, Python's groups; the `regex` crate as a second oracle) |
| Filters | elo (NULL or < min) -> no_score (NULL; NaN is kept) -> termination (exact) -> BOT (exact, case-sensitive); band from the stored `mean_elo`. | `mini_extract.json`, `_test_rust_extract.py` |
| Chunks | Chunk k = rows [250k*k, 250k*(k+1)) READ; full chunks always written, the tail only with a row or as c000; non-final row groups must be multiples of 50k rows. | `_test_rust_extract.py` (0 / 250,000 / 250,001 / 500,000-row files) |
| partials values | ply of the key's first occurrence in the chunk; the first non-NULL EPD, through Python's per-50k-batch hash->EPD memo (so even an in-batch 64-bit collision matches); child first; W/D/B/total. | `mini_extract.json`, `_test_rust_extract.py`, T3a |
| month values | MIN over the chunks' first plies, MIN EPD, MIN child (conflicts reported), summed counts. Commutative, so thread and pass count cannot change the output. | `_test_rust_extract.py` (byte-identical at 2 threads/1 pass and 12 threads/4 passes) |

## 50 plies and the ply key

`month --max-ply N` walks and keys N plies (EPD at every ply, tokens counted to
N+1 for the term kind). `month --ply-key` adds `ply` to the ps key and
`end_ply` to the term key (a column after `reason`), so a lower cap C can be
derived later by `python/ply_cap.py`: re-aggregate ps rows at ply <= C, keep
ENDED rows with end_ply <= C, and take HORIZON at C from the ps rows at ply C
by child_hash minus the games that ENDED there at ply C. The derived HORIZON
rows carry no reason, which the merge never uses. `_test_rust_extract.py` holds
the derivation at caps 20/30/50 to direct runs; the month dir's params lock
keeps keyed and unkeyed months apart.

## Month mode's memory

Each pass walks every game but records only its bucket range. Each chunk's
per-bucket maps become sorted runs; at the end of the pass every bucket k-way
merges its runs and streams the result to parquet. So memory is the runs --
`ENTRY_BYTES` (88) per chunk-level key, no hash-table load factor -- plus the
chunks in flight. `--mem-gb` picks P from the footers' game count x
`--keys-per-game` x `--bytes-per-key`; a pass that outgrows the budget stops
with "rerun with --passes 2P". The provenance JSON records the measured bytes
per key.
