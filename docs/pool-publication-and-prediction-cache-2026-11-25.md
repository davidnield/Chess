# Partitioned adult pool publication and raw Maia cache

Authorized November 25 after archive retirement. Worktree branch
`codex/takeover-hardening-20261122`; running pool producers remain frozen.

## Execution plan

- [x] Inspect current bucket schemas, receipts, builder consumers and Maia worker.
- [x] Add complete, full-key, paired edge/aux publication contracts and a resumable
  per-bucket sort/verification path; refuse partial publications.
- [x] Connect partitioned input to policy readers and the Rust builder's data
  callbacks; test against the existing single-file representation.
- [x] Separate raw Maia inference from temperature/shrinkage and add a persistent,
  batched, content-addressed cache usable by own and opponent providers.
- [x] Test interruption/resume, invalidation, collisions, corrupted/missing inputs,
  temperature reuse, and full builder integration.
- [x] Run bounded real-data publication/read and real-model cache checks; record
  measured time, bytes and memory before scheduling any full publication.
- [x] Freeze tested code, record commands/evidence and update the next-step plan.

## Design constraints

The mini's raw edge and auxiliary bucket files are **not sorted**. A manifest
alone cannot make their broad row-group statistics selective. Finalization will
sort each bucket independently into a new versioned output, preserving the raw
inputs and independent acceptance evidence. This avoids a global sort, but does
require a final sorted copy; measure its size in the bounded test. Publication
requires all 512 bucket pairs, exact content receipts and independent gates.
Only bounded diagnostic inputs may omit buckets, and production readers refuse
them. Both hash halves, every observed move, flow/endings, population and depth
metadata must survive. No uncut/full-pool decision is changed.

Use the existing native DuckDB/Arrow sorting and I/O path, with Rust numerical
validation/transforms and thin Python orchestration. No Python per-row corpus
aggregation or new model implementation is introduced. Keep per-bucket memory,
spill and thread limits explicit; do not edit or automatically restart jobs.

Raw prediction identity includes the checkpoint bytes, runtime and preprocessing
code, position-only input contract, exact model-visible FEN, full position key,
legal moves, rating pair and inference settings. It excludes count pools,
temperature, pseudocounts, gates, book budgets and evaluation sources. Changes to
those downstream choices should reuse inference. Derived policy caches retain
their broader identities. Never import the older low-half-only cache implicitly.
Batch transactions preserve completed predictions across interruption; duplicate
requests share inference and the persisted first answer wins. Cached probabilities
are validated on read, and malformed/partial results cannot become cache hits.

This is preparation for the later calibrated Black pilot, not permission to
activate an uncalibrated model, publish incomplete pools, or launch the sweep.

## Implementation and operational contract

`python/sweep/partitioned_pool.py` accepts pinned `_params.json`, raw edge/aux/stats
files and source acceptance artifacts. `--bucket N` sorts and verifies one pair;
an interrupted completed file is reusable only with the matching intent embedded
in its Parquet metadata. The bucket receipt pins inputs, implementation, both
outputs, complete WDL/flow totals and the ordered parent-key digest. Rust checks
all rows for sorted, unique full keys/moves, the correct bucket, legal count
ranges and WDL conservation. The edge and auxiliary parent sets must agree.
These structural checks supplement the existing independent source/gate audits;
they do not replace a legal replay or source census for every row.

Final `--acceptance PATH` requires all 512 pairs and an acceptance document of
schema `partitioned-pool-acceptance-v1`, bound to `params_sha256` and the digest
of `inventory(output, params_sha256)`. Its `checks` must contain exactly
`term_vs_book`, `reimpl`, `conservation`, and `evals`, each true, with nonempty
pinned artifact lists under the corresponding `evidence` fields. The independent
checks must actually be run; the publisher does not generate passing evidence.
It then rehashes all outputs and writes `edges.json`, `aux.json`, their metadata,
and finally `publication.json`. A published generation cannot be rewritten with
a changed recipe or inventory. Use a new output directory for a changed pool.

The production reader requires the complete paired inventory, checks sizes at
open, and checks SHA-256 on first bucket use and when the file's size/mtime
changes. It keeps at most eight Parquet handles and prunes sorted row groups
before an exact two-half join. Cold first use therefore includes full-file
hashing; the single-bucket lookup timings below must not be mistaken for the
total cost of opening a complete pool. The builder's native collector, root
prior, auxiliary lookup and shared policy adapters accept these manifests.

`prediction_cache.py` stores complete untempered legal vectors transactionally
in batches of at most 256. The default shared file is `maia-raw-v1.sqlite` beside
the derived policy cache. Set the same explicit `model.raw_cache` path when
separate run directories should share predictions. Compatibility includes
checkpoint, runtime/package/GPU identity, inference preprocessing/protocol,
ratings, FEN and both key halves. The first persisted vector wins a concurrent
write; corrupted rows fail rather than being silently replaced. A later batch
failure preserves earlier committed batches. This is a new cache namespace;
legacy low-half caches are not imported. Temperature is applied and validated
in Rust after lookup. Policy and continuation reports expose raw-cache counters.

No opponent calibration, own-model choice, engine gate or optimizer setting was
changed. All of this remains under the existing engineering/acceptance contract.

## Verified results

Evidence is under `docs/evidence/pool-publication-and-prediction-cache-20261125/`.
The final unchanged-code regression run passed **36 Python tests** in 216.78
seconds, including the complete corrected builder with partitioned adult inputs
and compact weak/broad inputs. **30 Rust tests**, the existing randomized native
versus Python differential suite, and **3 optimizer-interface tests** also passed.
Coverage includes interrupted finalization and inference, immutable publication,
missing/corrupt inputs, full-key collisions, exact FEN identity, concurrent cache
winners, model/runtime/rating invalidation, and a temperature change without
starting the worker. Two earlier integration attempts failed provenance checks
because files were edited while tests ran; the final run used unchanged code. The initial
native test's exact floating-point equality was corrected to a 1e-15 tolerance;
the implementation was already within one rounding unit.

Real bucket **005** retained all **23,877,125 edge rows**, **20,371,938 auxiliary
rows** and **69,971,508 move observations**. Every row passed the structural and
WDL checks; both parent-set digests matched. Sixteen spread sample positions
passed legal child replay and exact raw-versus-sorted lookup comparison (22 edge
rows and 16 auxiliary rows). This is a complete structural audit plus a sampled
semantic read check, not an independent semantic census of all 44 million rows.
The raw bucket's earlier independent parity receipt is pinned too.

| Measurement | Result |
|---|---:|
| Prepare including input checks, sort, audit and receipts | 184.0 seconds |
| Reuse verified bucket, including input/output hashes | 10.2 seconds |
| Raw edge + auxiliary bytes | 1,629,790,063 |
| Sorted final edge + auxiliary bytes | 1,792,384,021 (about 10.0% larger) |
| Sample edge query, raw / sorted | 3.391 / 0.342 seconds |
| Sample auxiliary query, raw / sorted | 0.651 / 0.107 seconds |
| Sampled peak host private memory / RSS | 4.32 / 3.92 GiB |

The probe used two threads and a 4 GB DuckDB limit while other jobs were active.
Final sorted queries selected 18/365 edge groups and 16/311 auxiliary groups;
the raw files required all groups. This is not a universal 6–10x builder speedup:
broad requests prune less, cold production readers also hash bucket files, and
full-pool costs have not yet been measured. At this single bucket's rate, a serial
512-bucket finalization would take roughly **26 hours** and **855 GiB** of new
final files, plus final acceptance/hashing. Use that only as a planning estimate;
measure additional buckets and available parallelism before scheduling it. Do
not place the full output on D:, whose 250 GiB reserve must remain intact.
Recheck E:/H: against the weak/broad queues and retained raw buckets first.

There is one remaining performance question before the sweep: SHA verification
currently happens once per opened bucket **per reader instance**. A build that
touches all buckets can therefore read the whole generation for verification,
and separate consumers/processes repeat that cost. The bounded timings above
exclude this production-reader startup. The real-input pilot must measure that
cost and evaluate safe shared verification for immutable inputs; do not assume
row-group pruning alone makes the entire build equally fast. Neither a persistent
verification cache nor a persistent count-query-result cache is claimed here.

The real Maia-3 79M checkpoint (`3fc6181d...dac45c2`) was tested on 64 legal board
requests, 63 distinct positions, for rating pairs 1450/2200 and 2600/2600. Each
cold pass took **6.1–7.1 seconds including worker startup**. Reopening the cache
and changing temperature from 1.0 to 1.35 took **0.016–0.024 seconds**, with all
64 hits, no worker started and zero inferred positions. The transformed vectors
matched an independent power/normalization formula. Two rating-pair namespaces
used **196,608 bytes** after close. This tiny probe establishes correctness and
reuse, not predictive calibration, steady-state GPU throughput or total sweep
runtime. Constructor/runtime identity checks are outside those per-call times.

## Commands and remaining work

Use the repository virtualenv with `PYTHONPATH=<maintained-worktree>/python`.
The reproducible bounded commands are:

```text
python -m sweep.input_probe --config docs/evidence/pool-publication-and-prediction-cache-20261125/bucket-config.json --bucket 5 --result scratch/bucket-probe.json
python -m sweep.input_probe --config docs/evidence/pool-publication-and-prediction-cache-20261125/model-config.json --result scratch/model-probe.json
```

Code commit: `cf1068296d97b84567f406297c258f8e7cdac53f` on
`codex/takeover-hardening-20261122`. Frozen release:
`D:/chess/bin/pool-input-readiness-20261125-v1`; its `release.json` SHA-256 is
`d96ed4c324398e9aa6b08e3f554637d709f03f3874bb3b8d52a2abe08befe131`.
The 352 copied Python/native files matched their source hashes. A frozen-release
smoke check reused the real bucket receipt and a raw Maia vector without starting
a model worker. Use this release for later bounded checks; do not repoint the
active data producers to it.

The bucket command now validates and reuses its diagnostic output. The model
timing probe intentionally requires a new empty cache for its cold pass; choose
a new `raw_cache` path in a copied config to rerun it. Normal consumers accept
both cold and warm caches. The one-bucket output is at
`D:/chess/acceptance/pool-input-readiness-20261125/bucket005`; no complete
`publication.json` exists or should be synthesized there.

Next: finish and independently accept the full data publications, lock own-policy
calibration inputs, and reuse six rating-pair inference sets for the 36 transforms.
The next substantial code task is global reach-weighted uncertainty/selective
continuation refinement with persistent completed work. Referee run receipts,
recall/horizon/objective audits and the paired Black pilot remain subsequent
requirements. This milestone does not release the main sweep.

At 18:32 PST the mini had 361 new accepted buckets plus eight originals
(369/512 total), bucket 367 active; the local weak pool was processing bucket 16.
Both continued on their existing frozen releases. Broad follows weak.

## Separate archive-volume finding

During the read-only storage check, Windows reported F: (exFAT) as **Warning /
Full Repair Needed**. Its Seagate disk reported **Healthy / Online**, and actual
free space was **1,011,630,997,504 bytes (942.2 GiB)**; the sandbox's zero-free
reading was not accurate. Evidence: `archive-volume-status.json`. No repair,
dismount, deletion or new F: write was attempted. The current pools and probes
use other drives. Investigate this filesystem warning before further archive
work; preserve the archive STOP and avoid an unattended repair. The successful
archive audits remain evidence of the bytes previously checked, not proof that
the current filesystem warning is harmless.
