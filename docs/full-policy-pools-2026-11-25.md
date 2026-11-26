# Full policy pools: November 25, 2026

David chose complete materialized pools instead of selective explorer count lookup. This supersedes the selective broad/weak input task in the model implementation plan. Keep the previously approved five p30 populations; p50 and BRC2200 remain deferred. Prediction caching and selective continuation-value refinement are separate tasks and are still needed.

## Scope and representation

| Population | Role | Work |
|---|---|---|
| BRC 1800+ | Candidate graph, outcomes and adult opponent evidence | Preserve the mini's current 512-bucket build and receipts; final publication remains separate |
| Rapid 2200+ | Targeted opponent evidence | Reuse accepted full p30 pool |
| Classical 1800+ | Targeted opponent evidence | Reuse accepted full p30 pool |
| BRC 1000–1799 | Our out-of-book move counts | Build a complete compact p30 count pool |
| BRC all ratings | Broad opponent prior | Build a complete compact p30 count pool |

Use the accepted 2013-01 through 2026-07 explorer, full 128-bit keys, game-mean rating bands, minimum games 1, no Other Moves cutoff. OTB uses the existing accepted training-role export. No source replacement, inference parameter change or canonical activation is part of this decision.

The two new policy-only tables retain **every observed parent/move**, child identity, earliest observed ply and exact White/draw/Black/total counts. Child XOR encoding is lossless. They omit EPD strings, endings and evaluation sidecars because their consumers need move probabilities, not candidate graph reconstruction or outcome-flow accounting. They are explicitly typed policy publications, not drop-in candidate pools. This avoids arrivals redistribution and a global duplicate sort. It does not drop rare branches or restrict materialization to a current repertoire.

Physical storage is 512 parent buckets, each split into eight disjoint signed-low-half ranges. Every grouping and lookup compares both halves. Each range is sorted for Parquet pruning. A `pool.json` manifest is published only after every range is present and hash-checked. Readers refuse missing partitions and reconstruct exact child keys. They fetch from the **completed pool**, never aggregate selected positions from the explorer.

The existing native DuckDB engine performs aggregation, sorting, validation and compression; Python performs orchestration and bounded I/O. There is no Python per-row production aggregation and no new port of this existing export stage. The repertoire/model numerical code remains Rust. These are the same accepted execution components used by the mini exporter, with a smaller policy-only output contract.

## Execution and safeguards

- Maintained producer: `python/sweep/count_pool.py`; consumer: `count_pool_reader.py`, through `KeyedParquet`.
- Frozen release: `D:/chess/bin/full-policy-pools-20261125-v1`.
- Active run: `D:/chess/full-policy-pools-20261125/config-v2.json`; `status-v2.json`, `launch-v2.json`, `stdout-v2.log`, `stderr-v2.log` beside it. Config SHA256 `5962ddd95e67e47e1e2d4ade355225a62b87155e6cd80cb361cf91236aa52b7e`.
- Weak output: `E:/chess/policy-pools/weak1000-1799-p30-20261125`.
- All-ratings output: `H:/chess/policy-pools/brc-all-p30-20261125`.
- One local bucket child at a time, four native threads, 32 GB query memory, at most 64 GiB spill. D: retains 250 GiB free, E: 250 GiB, H: 500 GiB (including room for the mini's remaining collection). F: is not used.
- First compare a complete range of adult bucket 005 against its independently accepted full-schema pool using `EXCEPT ALL` in both directions.
- Then build reusable complete buckets 005, 255 and 511 for both missing populations. Estimate remaining output from the largest measured bucket with 25% margin. Continue automatically only if both destinations fit their reserves.
- Finish weak, then all-ratings. Check the reserve at each range boundary; preserve completed ranges and stop on a failed child. No source, arrival or accepted old pool deletion is authorized.
- Source provenance pins publication manifests plus a complete file inventory (paths, sizes, modification times); this is not a new full-byte hash audit of the 4.5 TB explorer. Input inventory is checked before and after each bucket range. Output bytes are SHA-256 pinned and verified before publication and on first consumer access.
- Stop by creating the active configured `queue-v2.STOP`; the active range completes first. Resume uses `config-v2.json` after removing that specific stop marker. A Windows file lock prevents duplicate coordinators of the same config. Do not run the old configuration concurrently or modify the frozen release.

## Acceptance and limitations

Small tests compare exports against an independent Python census, covering transpositions, rare single observations, ply 30 versus 31, equal low halves with different high halves, negative keys, child XOR roundtrip, invalid counts/keys, changed inputs/receipts, and missing or corrupt partitions. Consumer tests compare full lookup results and exercise the actual corrected builder using compact broad and weak pools while the legacy continuation function is patched to fail.

The adult real-data parity check and capacity pilot are required before the unbounded remainder. Their receipts and measured timing determine the ETA. Full-count completion alone does not make the Black pilot ready: adult publication, weak-policy calibration, uncertainty refinement, referee/objective audits, independent verification and the new model acceptance gates remain required.

The first publications stay at count cap 30. They do not magically provide empirical continuation evidence at plies 31–33 or 50; the corrected contract must label model-only fallback beyond the bound source. Deeper full pools are a separate later task. Do not silently use these p30 counts in a p50 recipe.

## Launch and verification record

The combined regression run passed **20 tests** in 91.58 seconds, including actual native builder runs with ordinary and compact full-count inputs. Log: `evidence/full-policy-pools-20261125/tests.log`.

The real adult parity check passed **2,986,084 rows, zero differences in either direction** against the retained full-schema bucket. The first complete weak bucket contains about 63.7 million distinct moves and roughly 1.14 GiB of compact output. These are pilot measurements, not a completed pool or final runtime estimate.

After that first bucket, the larger all-ratings pilot range measured about 202 MiB versus 146 MiB for weak. The queue paused at its existing range boundary to place the larger population on H: and weak on E:. All nine completed ranges were copied, SHA-256 checked, and given explicit relocation provenance; originals were retained. No numerical or logical recipe changed. `placement-v2.json` records this, the old `queue.STOP` remains in place, and **only config-v2 is active**. The frozen producer was unchanged. Resume launched at 17:37 PST, wrapper PID 43804; check the actual process tree rather than assuming that PID survives indefinitely.
