# Eval DB builder: pilot report (2026-09-30)

Implements the blog repo's `docs/eval-db-spec.md`: `explorer-extract evals` on branch `eval-db` (from
`rust-merge` @ c66fed3), plus the independent check `python/verify_evals.py`.

## Status

**Every gate passed; the full build is armed.**

| Gate | Result |
|---|---|
| `cargo test` | 31/31: evals 7 (identity vs python-chess on 1,551 real FENs from both datasets, the choice rules, end to end with resume after kills in every phase, the lock, determinism, a near-zero memory budget), merge 13, fixtures 9, unit 2. `_test_rust_extract.py` 39/39 and `_test_verify_book.py` 10/10 still pass with the new exe; `_test_verify_evals.py` passes a real build and fails a corrupted value and a dropped row. |
| Pilot | 13 buckets (0–7, 128, 156, 189, 200, 383), all 20 cloud + 144 fishnet files, Phase C over the same 13 book buckets. Exit 0, `_DONE`. |
| `verify_evals.py` on the pilot | ALL PASS, 13 checks (detailed below). |

Pinned:
- `D:\chess\bin\explorer-extract-0c97b27.exe`, sha256 `9812270a928390de937e8355bce01e983d41127b07480a3d0dc3a7e7b0c5ebbe`. This is the exact binary that ran the pilot. Rust sources are unchanged since 0c97b27.
- `D:\chess\bin\verify_evals_0c97b27\` holds `verify_evals.py` @ 2dead31 and `zobrist.py`.
- `D:\chess\run_eval_build.ps1`:
  - checks the exe's sha256 with .NET;
  - runs the build at `--threads 12 --mem-gb 60 --below-normal` into `D:\chess\eval_full`, with scratch in `H:\chess\eval_work`;
  - on exit 6 (free commit too low), waits 10 minutes and retries, up to 12 h;
  - then runs `verify_evals.py` on the full output;
  - logs to `D:\chess\logs\eval_build.log` and `eval_build_status.log`;
  - exits 0 only if both pass.
- `D:\chess\bin\EVAL_BUILD.READY` was written last, at 18:22 on 2026-09-30.

**The chain has not reached the build yet.** `D:\chess\run_post_merge.ps1` (pid 37112) is still in its `verify_book.py` step:
- it has been on the book-wide `--sums` query since 03:12;
- at 15:50 that process was using about 1 core and about 6 MB/s;
- that is the same single-query slowness its own docstring describes for the scan.

I did not touch it. The eval build starts when it passes.

**Update, 19:45.** The owner chose to restart verification. The old chain was stopped while `verify_book`'s scan sat at bucket 24/512:
- the sums had passed by then;
- the scan was running 700–1,000 s per bucket on one core, about 100 h to go;
- the cause is the same process-wide DuckDB decay as item 4 below.

`verify_book.py` @ 540f935 now runs each scan bucket in a fresh child process: about 15 s per bucket, `_test_verify_book.py` 10/10.

`D:\chess\run_post_merge2.ps1` replaces the chain:
- it requires the first run's digest, duplicate-key and sums PASS lines;
- it runs the owed `--scan --collisions --sample` from `D:\chess\bin\verify_book_540f935\`;
- then, as before, it waits for `_DOWNLOAD.DONE` and `EVAL_BUILD.READY` and runs `run_eval_build.ps1`.

Expected: `verify_book` about 2.5 h, then the eval build plus its verification about 5–7 h.

## The full build (2026-10-01)

**verify_book (chain v2)** passed at 2026-09-30 22:04: ALL PASS, 5 checks, 8,320 s. The scan ran about 15 s per bucket. With the first run's digests, duplicate keys and sums, the book is fully verified.

**Build.** It started at 22:04.
- Phase E took 2.6 h; Phase C took 0.8 h.
- Phase J had finished 189 of 512 buckets when Windows Update force-restarted the PC (02:44 and 02:49, KB5129195). Nothing was lost.
- `run_eval_build.ps1` was relaunched by hand at 07:29. It resumed with 323 J buckets left and finished at 09:14: exit 0, `_DONE`.
- J at full scale: about 24 s per bucket effective, at 12 threads.

**Output: `D:\chess\eval_full`.**

| | |
|---|---|
| Rows | 5,933,072,384 (196 GB): 5,484,581,495 parent, 448,490,889 child-only |
| Source | 62,752,879 cloud, 5,870,319,505 fishnet |
| Other counts | 85,430 ep-variant rows; 0 ambiguous; 0 `fishnet_disagrees` |

About 875K child-only rows per bucket, against about 14K in the pilot. The pilot's Phase C read the children of only 13 book buckets.

**verify_evals on the full DB.** These checks passed:
- **Structure:** all 512 files match the manifest (rows, bytes, sha256). In 16 sampled buckets, 185.4M rows are sorted, unique and valid.
- **Positive sample:** 110,461 rows (65,315 fishnet, 45,146 cloud). 474 ep-variant rows were found among 3M candidates and all hash correctly. Every column equals the raw recompute; the raw scans took 30 min at 19.7M rows/s.
- **Negative sample:** for 100,000 book parents without a row, no raw source carries their EPD.
- **Parents:** all 94,903 sampled parent rows are book parents.

At 13:00 the last check was still running: whether the sampled child-only hashes are book `child_hash` values. It scans `child_hash` across the whole book, about 660 GB. It runs inside the main verifier process, so it got the same process-wide DuckDB slowdown (about 1 core, 28 MB/s), projected to finish around 17:00. The owner chose to let it finish, since the DB is complete and usable meanwhile.

**Follow-up.** Move that check into a child process too, as already done for the raw scans and verify_book's scan.

**Result.** The check passed at 16:31: ALL PASS, 13 checks. The child-process version is follow-up 4 below.

## Pilot numbers

| | |
|---|---|
| Phase E (4 threads) | Cloud: 988.9M rows → 11.4M shard rows, 433 thread-s. Fishnet: 34.46B rows → 712M shard rows (13 of 512 buckets kept), 8,918 s wall. About 0.8M rows/s per thread. Peak commit 4.1 GB. |
| Phase C (4 threads) | 13 book buckets, 2.26B child rows, about 137.6M distinct per bucket, 56 s. |
| Phase J (4 workers) | 88 s per bucket (load 17 s, book stream 65 s), estimated 4.9 GB each. Peak commit for the process was 8.8 GB. |
| Output | 139,410,332 rows (10.7M per bucket), 4.59 GB (33 B/row). |
| Composition | 139.23M parent, 179,585 child-only; 1.56M cloud, 137.85M fishnet; 2,169 ep-variant rows; 0 ambiguous; 0 `fishnet_disagrees`. |
| Coverage (13 buckets) | 9.0% of book parent positions have an eval (139.2M of 1.54B). Weighted by games it is 85.4%. Ply 1: the start position, 7,130,958,849 games, all with an eval (the book's exact ply-1 total). Ply 30: 7.7% of positions, 10.6% of games. |
| Parse failures | Cloud: 7,846,898 rows (0.8%) with castling rights python-chess also rejects (e.g. `q` with the king castled), 425,175 too much material, 593 impossible check. Fishnet: 0. All skipped and counted, per the spec. |
| ep variants | 131,077 cloud and 10.68M fishnet variant hashes emitted. No source printed an ep square that the legal EPD drops. |

**Memory probe at full routing** (all 512 buckets; the largest fishnet month, 641.6M rows; `--mem-gb 20`, 3 threads):
- 524M shard rows, 13.5 GB, 25.7 B/row;
- peak commit 15.8 GB against a 10 GB buffer cap.

## Projection for the full build (`--threads 12 --mem-gb 60`)

| | Time | Space | Memory |
|---|---|---|---|
| E | ~1–1.5 h (7–9M rows/s) | ~760 GB of shards on H: | ~45 GB peak (30 GB buffer cap + overhead) |
| C | ~15–40 min (E: reads ~660 GB of `child_hash`) | ~550 GB on H: (70B hashes) | ≤ ~20 GB (16 book buckets per group) |
| J | ~1.5 h (512 × 88 s / ~9 concurrent) | ~180–190 GB on D: (~5.5B rows plus the extra child-only rows that full Phase C adds) | ≤ 48 GB gate; real use is about half the estimate |
| verify_evals | ~2–3.5 h. Sampling plus the raw scans (38 min at 15.7M rows/s) plus the child check, which scans `child_hash` across the whole book (211 s for 13 buckets in the pilot). | — | 24 GB DuckDB limit |

**Total: roughly 5–7 h after the chain releases it.**
- H: needs about 1.3 TB of its 3.3 TB free.
- D: needs about 190 GB of its 2.6 TB free.
- The tool refuses to start (exit 6) unless 64 GB of commit is free. With `verify_book` (36 GB private) gone, that holds.

## verify_evals on the pilot (ALL PASS, 13 checks, 70 min)

- **Structure.** Manifest rows, bytes and sha256 match. All 139.4M rows are strictly sorted and unique on (hash, EPD), in their bucket, with exactly one of cp/mate and valid value sets.
- **Positive sample.** 110,514 rows:
  - stratified: 45K cloud, 45K fishnet, 10K child-only, 10K ep-candidate, 527 true ep-variant rows (found by python-chess among 3M candidates), and all 3 collision-twin rows;
  - python-chess round-trips every EPD, and `zobrist_int64` gives `position_hash` (528 of them via an ep square the EPD cannot show);
  - every eval column equals DuckDB's recompute from the raw sources by EPD string match: 0 differ.
- **Negative sample.** For 100,000 book parents without an output row, no raw source row carries their EPD.
- **in_book.** All 100,423 sampled parent rows are book (`parent_hash`, `parent_epd`). All 10,091 child-only hashes are book `child_hash` values and not book `parent_hash` values.
- **Collisions.** The 6 book collision hashes in these buckets: 3 have rows, each its own row under its own EPD.

## Surprises

1. **The cloud row-order check holds.** 85,866 of 164,409,501 chosen multi-PV blocks (0.052%) have a later PV that scores better for the side to move.
   - I inspected the cases. Rows are in PV order. The exceptions are genuine Stockfish multi-PV score quirks, not a misordered file: the median shortfall is 39 cp, and they cluster in decided endgames (±600 to ±8,000 cp) and mate/cp mixes.
   - That is "about 0", not the large count the spec says would falsify first-PV, so the build proceeds.
   - The spec's three examples come out exactly: depth 46 / cp 69 / `f7g7 …`; depth 58 / knodes 491568 / `e7a7 …` / cp 0; depth 95 / mate 15.
2. **`fishnet_disagrees` is 0 in the pilot.**
   - 1,177 cloud rows have a saturated fishnet median with n_tier ≥ 5. All agree in sign with the cloud eval.
   - The old DB's sign flips came from its mate-blind cloud aggregation. The first PV now carries the mate.
   - An independent DuckDB count agrees.
   - The flag follows the old DB's `!=`, so a cloud 0 against a saturated fishnet would count.
3. **Most book positions have no eval, but most games do** (9% of positions, 85% of game-weighted mass). Fishnet only covers analysed games, and 92% of book rows are single-game.
4. **`verify_evals`'s first design was about 20× too slow.** Its fishnet scans ran at about 0.8M rows/s inside the verifier process, whether as one query over 144 files or one query and connection per file. The identical loop in a fresh process ran at about 14M rows/s.
   - That is CLAUDE.md's process-wide DuckDB decay.
   - Fix: every raw scan runs in a fresh child process (`max_tasks_per_child=1`).
5. **Two pilot restarts**, each on fresh dirs, because the lock pins the build:
   - Phase J was rewritten as a streaming merge, so memory is the shard rows rather than about 9 GB of per-position records per bucket.
   - Phase E buffers are now charged by Vec capacity rather than length, and J streams its output.
   - Both fixes matter only at full scale.
6. **NTFS case-insensitivity.** `_DONE` and a `_done/` sentinel directory collide in the output dir, so per-bucket sentinels live in `_bucket_done/`.

## Follow-ups (2026-10-01 to 10-02)

The four follow-ups are:
1. an `eval_arrays` export;
2. Stage 3 on the new DB;
3. retiring `unified_eval_db`;
4. a fast child-only check.

All four are done in code. The canonical rebuild on the new arrays, and archiving the old DB files, wait for the owner's go-ahead.

### 4. The child-only check in fresh processes (`verify_evals.py` @ 5bc7aec)

- `book_index` walks `ps/` once and holds it to each bucket's `_done` file list.
- `_member_scan` runs about 8 book buckets per fresh child process, with the target hashes passed in as a temp table.
- Timed with `--child-check-only 20000` on the full DB: **394 s, PASS**. The in-process version took about 6.5 h.
- `_test_verify_evals.py` gains a negative case: it drops the book rows that carry a child-only hash and expects the check to FAIL.
- Pinned at `D:\chess\bin\verify_evals_5bc7aec\`. `run_eval_build.ps1` now has its own `$vsha`; the old version is backed up as `run_eval_build.ps1.bak_0c97b27`.

### 1. Eval arrays from the eval DB directory (`eval_arrays.py`, `eval_arrays_build.py`)

**Format.** The on-disk format and API are unchanged:
- a globally sorted `eval_hash.npy` (int64) and `eval_cp.npy` (int16);
- `MISSING`, `open_eval_arrays` and `lookup_evals` work as before.

Every existing consumer works on the new folder.

**Fingerprint.** The `eval-db-dir-v1` fingerprint covers:
- `_DONE`;
- the sha256 of `_manifest.parquet` and `_build.meta.json`;
- a digest of the 512 bucket files;
- `source_rows`;
- the book's `_collisions.parquet` and `_book.meta.json`.

**Build.** Built at `D:\chess\eval_arrays_full` by `D:\chess\bin\eval_arrays_3af185e\`, in 6,458 s: mates 1,066 s, pass 1 3,881 s, pass 2 514 s.

| | |
|---|---|
| Entries | 6,033,226,551; 48.3 GB of hashes and 12.1 GB of evals |
| Excluded | 21 hashes (23 rows), all `book_collision`. None are `hash_ambiguous` or DB duplicates. |
| Book checkmates added | 100,154,190 at ±2000. The sign comes from ply parity, checked by python-chess replay on a 20K sample. |
| Canonical pool coverage | 26,460,667 of 26,472,943 hashes (99.95%) |

**Surprise: 98 checkmate positions carried bogus DB values.** These are fishnet classical cp values of 10–58, each with n = 1, on positions python-chess confirms are checkmate.
- The builder now reads the DB EPD of any mated hash whose DB value disagrees, and checks it with python-chess.
  - A real checkmate takes the mate value (98 overridden).
  - Anything else is excluded as `mate_hash_collision` (0 cases).
- The DB itself is unchanged. `eval_full` serves those 98 values; only the arrays correct them.

**Root cause (investigated 2026-10-02): the Lichess fishnet dump itself.** The cause is a bad window in the dump, not a bug in our builder. The raw rows behind all 98 values are:
- in `standard_rated_2016_03` (80) and `standard_rated_2016_06` (18) only;
- the final row of a game ending in checkmate: no `move`, a `cp`, and the previous row is mate-in-1 with the mating move;
- python-chess confirms all 98 are checkmate.

The score is about +25 cp from the side to move's view in 96 of the 98 (range 16–58), which is an "about level" value. It is not copied from a neighbouring row: it equals the next row's cp in only 2 of 98.

A sweep of every game-final row (`move IS NULL`) in eight months, using python-chess, shows the window:

| Month | Checkmate final rows with a cp | Stalemate final rows not 0 |
|---|---|---|
| 2016-02 | 2 | 1 |
| 2016-03 | 2,964 | 346 |
| 2016-04 | 0 | 565 |
| 2016-05 | 2 | 15 |
| 2016-06 | 711 | 10 |
| 2016-07, 2018-06, 2023-06 | 0 | 0 |

In clean months, a mated final position has no row, and a stalemate reads exactly 0. Something in the early-2016 analysis returned a small search score for positions with no legal moves. This window is not among the dataset card's known-bad months.

What reaches our data:
- **Checkmates:** 3,680 bad rows (3,677 positions). 98 are book positions, all now ±2000 in the arrays. The rest are mates after the book's ply 30, so they were never DB rows.
- **Stalemates:** 937 bad rows (793 positions). None is in the arrays, so none is a book position.
- **Other months:** no other month put a cp on any of the 100M book checkmates. A cp in any other month would have shown up among the overrides, and all 98 come from 2016-03 and 2016-06.
- **Not checked:** stalemates outside the eight months above.

For a future `explorer-extract evals` build, the clean fix is to skip any source row whose position has no legal moves, counted as its own parse reason. Shakmaty already parses every FEN.

The build validates itself before writing the meta:
- 1M sampled DB rows match the lookup;
- excluded hashes return MISSING;
- mates are ±2000.

`_test_eval_arrays.py`: 28/28 PASS. `D:\chess\eval_full` must now be kept, because the arrays verify against it.

### 2. Stage 3 on the arrays (branch `stage3-eval-full` @ 6bb9bd5, off `budget-books` d71dc23)

**Loader.** `load_evals()` accepts the legacy parquet (verbatim legacy path) or an arrays directory.
- Arrays: a dict for the DAG, plus the memmap for engine augmentation.
- An eval-DB directory is refused with the build command.
- Expected scores come from one Polars sigmoid through a lookup table.

`build_sharp_reps.py` gains `--out-dir` and `--pass1-only`, and its `.meta.json` records `eval_source`.

**Checks:**
- **(b) Loader equivalence**, old DB as parquet vs as arrays: bit-identical, 24,038,615 dict entries and 399,998,944 augmentation entries. Load takes 52 s from arrays vs 238 s from parquet.
- **(c) No-op.** The refactor on the old DB differs from the 2026-08-25 canonical pass 1 at 241 (white) and 250 (black) best moves.
  - Attribution: d71dc23 *without* the refactor gives the same white output (tolerance 0, crush wobble ≤ 7e-16). So the refactor is a no-op, and **the canonical reps no longer reproduce from current code**.
  - The flags match the canonical meta.
  - The likely cause is 2ad53d2 (shared aux/leaf valuation, 2026-09-04), the commit that also voided the pre-09-08 budget books. The two 10-01 commits only add default-off flags. Not bisected.
- **(d) A/B**, the same code (`budget-books` plus the refactor) on both sides. A = old DB (`E:\chess\repertoire\_evalnoop`, pass 1). B = new arrays (`E:\chess\repertoire\_evaldb_ab`, full build, 270.6 min).

### The A/B (pass 1, `position_stats_pooled_ge1800_2013_2026_brc`, 17,788,308 positions)

| | white A | white B | black A | black B |
|---|---|---|---|---|
| Positions with an eval | 96.9% | 100.0% | 96.9% | 100.0% |
| Our-turn positions with a best move | 8,177,227 | 8,842,253 | 8,262,180 | 8,936,181 |
| Engine-augmented recommendations | 83,439 | 216,426 | 101,235 | 270,876 |
| Best move differs (all our-turn) | | 1,027,647 (11.6%) | | 1,056,379 (11.8%) |
| Best move differs, reach-weighted (B walk / canonical walk) | | 3.19% / 2.51% | | 3.91% / 2.96% |
| … within our first 6 moves | | 1.23% | | 1.99% / 1.91% |
| Main line (30 plies) | | identical | | identical |

**Eval agreement.** On the 17,230,439 positions both DBs cover, the expected-score difference is:
- median 0.0000;
- p90 0.0009;
- p99 0.0116.

11,437 cross 0.5.

**Why decisions changed.** The decisions that changed most by reach keep their own eval: A and B have the same `eval_score` there. They move because of evals deeper in the tree. Examples:
- white after 1.e4 e5 2.Nf3 d5: Nxe5 → exd5;
- black in the Two Knights with 5.O-O: Bc5 → Nxe4.

**Scorecards** (`score_repertoire.py`, frequency-weighted):

| | white A | white B | black A | black B |
|---|---|---|---|---|
| Effectiveness | 68.70% | 68.75% | 63.34% | 63.45% |
| Soundness (value_robust) | 51.66% | 51.66% | 48.34% | 48.34% |
| Soundness (freq-weighted eval) | 57.71% | 57.83% | 54.10% | 54.12% |
| Worst-case (the gate) | 24.06% | **36.76%** | 28.06% | 27.35% |
| Coverage ply 16 | 79.27% | 79.92% | 82.31% | 82.76% |
| Booked moves / game | 10.45 | 10.57 | 9.79 | 9.84 |
| Positions reached from start | 426,944 | 429,972 | 489,704 | 492,322 |

The scorecard's "eval cover" falls slightly in B (98.2 → 97.8%, 98.0 → 97.7%). That is not missing evals: the B books reach a few more positions outside the repertoire table, and the scorecard counts those as uncovered.

**Cost.**
- Pass 1 takes 60.0 / 75.1 min (white / black) against 52.6 / 53.3 min.
- Most of the difference is augmentation lookups on the memmap: 793 s / 944 s against 111 s / 122 s, with about 2.5× as many rescues found.
- A sparse in-RAM index (every 512th hash, about 93 MB) would cut the cold binary searches. It is not built.
- Peak memory was not measured.
- The 4.8 GB of in-RAM augmentation arrays and the full-DB read are gone. Loading takes 52 s.

**Caveat: mixed provenance.** Two inputs still come from the old DB until a pool rebuild:
- the pool's aux `other_eval`;
- the t300 crush histogram, and therefore `--crush-won-cp`.

**Decision (2026-10-02).** The owner accepted the new DB. The canonical rebuild is "not yet".

### 3. Retiring `unified_eval_db` (`stage3-eval-full` @ 93b2a60)

- **Defaults:** `eval_arrays.DEFAULT_EVAL_DB` / `DEFAULT_ARRAY_DIR` now point at `D:/chess/eval_full` / `D:/chess/eval_arrays_full`. `build_sharp_reps.py`, the extract (fused winpos, `child_eval`), the budget and baseline books, and annotations all follow. `annotate.facts.EvalDB` verifies and memory-maps an arrays dir; a legacy parquet still works.
- **`winpos_reference.py`:** the winpos event SQL and the reference replay, lifted from the legacy builders. They are byte-identical, checked by comparing the generated SQL and the function body. `winpos_fused.py` and both winpos tests import it.
- **Legacy scripts:** `build_crush_winpos.py`, `build_crush_winpos_phase2.py`, `build_fishnet_eval_db.py` and `build_lichess_eval_db.py` left the repo. Copies are in the main checkout's `scratch/python/`.
- **Tests and docs:**
  - `_test_eval_arrays.py` takes its ground truth from a random sample of `bkt000` (excluded, ambiguous and mated hashes dropped): 28/28.
  - `run_tests.py`: 41/41. `_test_winpos_fused`'s real-data level ran on the new arrays: 62,276 rows identical.
  - The CLAUDE.md and README rows are updated.
- **Still waiting:**
  - the canonical rebuild (separate go-ahead);
  - archiving `unified_eval_db.parquet`, `lichess_eval_db.parquet` and `E:\chess\eval_arrays\` (separate go-ahead, after the rebuild).

**Extract cost on the new arrays (measured 2026-10-02).** Real `extract_file`, 3 concurrent workers × 300K games of 2025 Blitz, Rapid and Classical:

| | New arrays | Old arrays |
|---|---|---|
| Wall | 112–192 s | 86–154 s |
| Share of wall in `lookup_evals` | 20–26% | 7% |
| Lookups per second | 0.11–0.15M | 0.51–0.57M |

The cost comes from cold page faults: a 48 GB hash file does not stay in the page cache next to the extract's ~111 GB commit. That makes the extract about 25–30% slower.

A synthetic benchmark with keys weighted by games measured 3–12K lookups/s for a cold single worker. Fully warm it ran 0.2M/s per worker at 6 workers, against about 1M/s for the old arrays.

The untested fix is a sparse in-RAM index (every 512th hash, about 94 MB), so that each lookup touches one hash page and one cp page. Build and measure it before the next pool rebuild. Stage 3 is not affected: its lookups are one batch of 26M keys.

## Leftovers

- Deleted 2026-10-02 at the owner's request, about 1.29 TB in all:
  - `H:\chess\eval_work` (1.2 TB);
  - `H:\chess\eval_work_pilot`;
  - `D:\chess\eval_pilot` and `D:\chess\eval_pilot_bin`;
  - `D:\chess\eval_arrays_full_work` (57 GB);
  - the side rep folders `_evalnoop`, `_evalbase` and `_evaldb_ab`.

  Rerunning `run_eval_build.ps1` would recreate its scratch from nothing.
