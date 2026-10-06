# Pool from the explorer book: build report (2026-10-03)

The spec is `Chess Blog Posts/docs/pool-from-book-spec.md`, with its §8 addendum (a floor-20 variant). The builder is
`python/pool_from_book.py`, and the gates are in `python/pool_from_book_gates.py`. No consumer default
changed: `build_sharp_reps.py` and the other consumers still point at the ≥1800 pool.

**Population.** These events from the explorer book:
- Blitz, Rapid and Classical;
- elo_band 2000, 2200 and 2500 (mean Elo ≥ 2000);
- months 2013-01 to 2026-07;
- plies ≤ 20.

The evals come from `D:\chess\eval_arrays_full`. Termination reasons are pooled into `term_other_*`, and the aux metas say
`term_reasons: "pooled"`.

## Outputs

All four files are in `E:\chess\position-stats\`, each with a `.parquet.meta.json` beside it.

| file | rows |
|---|---|
| `position_stats_pooled_ge2000_2013_2026_brc_p20.parquet` (floor 50) | 11,158,142 edges |
| `position_stats_aux_pooled_ge2000_2013_2026_brc_p20.parquet` | 6,146,855 positions |
| `position_stats_pooled_ge2000_2013_2026_brc_p20_mg20.parquet` (floor 20) | 25,946,865 edges |
| `position_stats_aux_pooled_ge2000_2013_2026_brc_p20_mg20.parquet` | 14,075,324 positions |

Both outputs match the old files' arrow schemas exactly, `large_string` included. The builder checks this before it
renames a file into place.

## Gate verdicts

| gate | floor 50 | floor 20 |
|---|---|---|
| 1 · derivation vs the book's term table: all 54 slices, buckets 0 128 156 189 200 383 511 + 15 | **PASS**: 0 negative cells, **residual 0** of 4,793,801 games | not repeated: floor-independent (§8) |
| 2 · independent Polars/numpy re-implementation vs the finished files | **PASS** on 8 buckets: pool rows identical, aux doubles ≤ 3.3e-16 | **PASS** on 156 and 15 |
| 3 · conservation | **PASS** (see the note below) | **PASS** |
| 4 · evals: 10,000 random below-floor edges, 9,365 parents re-derived through `annotate.facts.EvalDB` | **PASS**: max \|diff\| 3.3e-16; invariants hold on all 6.15M rows | not in the §8 list |
| 5 · guard test + `run_tests.py` | **PASS**: 39/39 files | — |
| 6 · `--pass1-only` smoke, white and black | **PASS** | **PASS** |

**Gate 1, the term derivation:**
- The residual is exactly zero. In every (position, end_ply ≤ 20) cell, `A(x,p) − D(x,p+1)` equals the book's kind-0 term rows summed over reason, in all four
  components.
- So no parse-failure games were seen in these buckets. The "parse failures only add" allowance went unused.
- The book's pooled end_ply-0 count at the root is 2,521,483 games, all slices. Those games have no moves, so they can't be derived, and the root's
  term covers plies ≥ 1 only. For this population it is 440 games: knight round trips that end on the start
  position.

**Gate 3: the spec's identity misses a term.** `Σ pool total + Σ aux other_total = population mass` does not
hold, and cannot. The aux has one row per *pool parent*, which is `merge_aux_stats`' scope. So the below-floor moves of a position with *no*
surviving edge appear in neither file; Stage 3 never reaches such a position anyway. The gate measures that mass
from the book independently, as a third term. The identity then balances to the game:

- **Floor 50:** 9,080,548,622 kept + 323,964,221 other + 2,044,518,545 outside = 11,449,031,388.
- **Floor 20:** 9,527,318,414 + 288,466,369 + 1,633,246,605 = the same total.

The other parts of gate 3 also pass, at both floors:
- **Per parent:** for every non-root pool parent, ΣA = edges + other + term + horizon, component-wise. That is 0 mismatches of 6,146,854, and
  0 of 14,075,323 at floor 20.
- **Root:** the book's ply-1 games equal `_slices.ply1_games` at 575,635,719. The pool's root edges equal the book's departures at
  ply ≤ 20, 575,637,716, which includes 1,997 knight-shuffle returns.

**Collisions.** In this population, only 1 collision hash is a parent at ply ≤ 20. It carries 1 game and no surviving edge.

## Size against the ≥1800 cap-30 pool

| | ≥1800 cap 30 (canonical) | ≥2000 p20 floor 50 | ≥2000 p20 floor 20 |
|---|---|---|---|
| edges | 31,266,115 | 11,158,142 | 25,946,865 |
| pool parents (aux rows) | 17,788,308 | 6,146,855 | 14,075,324 |
| edges before the floor | — | 1,105,612,439 | 1,105,612,439 |
| mass at pool parents: kept | 95.71% | 96.48% | 96.97% |
| … other (below-floor) | 4.22% | 3.44% | 2.94% |
| … term | 0.062% | 0.041% | 0.045% |
| … horizon | 0.004% | 0.035% | 0.046% |
| other-bucket eval coverage | 24.4% (archived eval DB) | 95.8% | 89.7% |

The "mass at pool parents" shares are computed over the mass the aux describes: kept + other + term + horizon, at pool parents.

## Mass split by ply band

Every move at ply ≤ 20 of the population, 11.45B moves, is classified as one of three kinds:
- **kept**: along a pool edge;
- **other**: a below-floor move from a pool parent;
- **outside**: a move from a position that is not a pool parent.

Games ended at a pool parent are counted per band.

| ply | floor 50: kept / other / outside | ended at pool parent | floor 20: kept / other / outside | ended at pool parent |
|---|---|---|---|---|
| 1–5 | 99.93 / 0.05 / 0.02 % | 1,177,107 | 99.96 / 0.03 / 0.01 % | 1,180,856 |
| 6–10 | 96.48 / 1.32 / 2.20 % | 989,984 | 97.72 / 0.90 / 1.37 % | 1,077,486 |
| 11–15 | 77.55 / 4.58 / 17.87 % | 1,048,180 | 83.24 / 3.76 / 13.00 % | 1,250,811 |
| 16–20 | 42.91 / 5.40 / 51.69 % | 651,475 | 51.61 / 5.42 / 42.97 % | 874,808 |
| all | **79.31** / 2.83 / 17.86 % | 3,866,746 | **83.22** / 2.52 / 14.27 % | 4,383,961 |

**Horizon.** Games alive after ply 20 total about 566M. Of those, 3,256,594 sit at a floor-50 pool parent and 4,479,505 at a floor-20 one. Most
ply-20 positions are not pool parents.

The kept shares equal the builds' `survivor_mass` exactly, a cross-check between two separate code paths. The
spec quotes 74.2% / 79.1% from 4 sample buckets, but that figure used a different denominator. Floor 20's extra kept
mass sits almost entirely at ply 11 and beyond.

## Build cost

| | floor 50 | floor 20 |
|---|---|---|
| arrivals (32 groups of 16 source buckets) | 8.6 min | reused (`--arrivals-from`) |
| buckets (4 workers × 3 threads, 6 GB each) | ~99 min wall (two launches) | 77 min |
| finalize | 5 s | 12 s |
| per-bucket eval lookup | 130 s cold → ~22 s warm | ~22 s |

Commit read 55.8 GB mid-build, against a ~51 GB baseline that includes an unrelated crawler. The eval-array mmap is file-backed and shared, so it doesn't count toward commit.

## Smoke runs: `build_sharp_reps.py --pass1-only`

Outputs went to `E:\chess\repertoire\_pool2000_p20` and `_pool2000_p20_mg20`. The prior strength is at its default (500) in both. Memory was sampled on the
real Stage-3 child process, not the venv shim.

| | floor 50 white | floor 50 black | floor 20 white | floor 20 black |
|---|---|---|---|---|
| pass-1 wall | 12.6 min | 10.0 min | 22.2 min | 25.7 min |
| peak private (commit) | 13.4 GB | 13.1 GB | 28.8 GB | 27.9 GB |
| peak working set (incl. eval mmap) | 71.2 GB | 71.2 GB | 87.0 GB | 85.4 GB |

## Scorecards (pass-1, `score_repertoire.py`)

**These do not compare like with like.** The canonical column is a different population (≥1800) at a different cap (30). Each
rep is scored against its own pool, so the three rows estimate expected score against three different opponent
models.

| | canonical ≥1800 c30 W | floor 50 W | floor 20 W | canonical B | floor 50 B | floor 20 B |
|---|---|---|---|---|---|---|
| Effectiveness | 68.9% | 65.2% | 66.5% | 63.5% | 60.2% | 61.2% |
| Soundness (best defence) | 51.7% | 51.7% | 51.7% | 48.3% | 48.3% | 48.3% |
| Soundness (freq-wtd eval) | 57.8% | 55.6% | 56.2% | 54.0% | 52.5% | 52.8% |
| Worst-case | 40.1% | **43.1%** | 39.3% | 27.4% | **38.8%** | 34.1% |
| Coverage ply 16 | 79.7% | 68.6% | 75.1% | 82.7% | 69.1% | 70.4% |
| Booked moves / game | 10.6 | 8.9 | 9.2 | 9.8 | 8.2 | 8.4 |
| Positions reached | 427,356 | 114,728 | 200,200 | 492,029 | 131,785 | 252,578 |

## Floor 50 vs floor 20

Floor 20 does not "change little":
- Effectiveness rises 1.0–1.3 points, and ply-16 coverage rises 1.3 (black) to 6.4 (white) points.
- The book reaches 1.7–1.9× the positions.
- Worst-case falls 3.8 (white) and 4.7 (black) points.

Walking each rep forward by the pool's reach shows where the differences are:

| | white | black |
|---|---|---|
| reach mass at our decisions on a chosen 20–49-game edge | 7.8% | 11.4% |
| shared decisions, reach-weighted, that pick a different move | 8.2% | 11.1% |
| heaviest changed decision | 1.e4 c5 2.d4 cxd4: 3.Nxd4 → 3.c3 (reach 4.0%) | vs 2.f4: exf4 → d5 (reach 4.9%) |

**Prior strength.** In Stage 3, `--prior-strength 500` shrinks a leaf edge's own score to `(500·prior + n·emp)/(500+n)`.
A 20–49-game edge keeps 4–9% of its deviation, and that shrunk score is then blended 50/50 with the child's eval.
So the new edges' own results barely enter. What floor 20 adds is reach, more positions booked at ply 11–20, and
choices steered by the eval of the thin edges' children, at a much higher eval coverage than the old archived DB gave. A
lower prior strength would let those results speak, and it would be the natural next sweep. Whether 20–49-game results
*should* move a choice is a statistical judgment call this report doesn't settle. The worst-case drop is the
number to watch if it is lowered.

## Things found on the way

- **DuckDB spill collisions.** Concurrent DuckDB processes sharing one `temp_directory` can read each other's spill blocks. Gate 1, spilling beside the 4-worker build, died on a `SUM(total)` of 5.9e31,
  which an unshared rerun didn't reproduce. Fixed in `7c32ce0`: each connection spills under `<tmp>/pid<pid>`.
  - Floor-50 buckets 0–256 were built before that fix, by `aa2cb7e`, which has the same logic. Their working sets are far below the memory limit, so they shouldn't
    have spilled. Gates 2–4 and the per-parent conservation over all 6.15M parents pass on them.
  - The metas record this in `build_note`.
- **Polars can't read the book.** Polars 1.40's own reader rejects the book's parquet-rs files ("Invalid thrift: bad data"). The gates read
  them with `pq.ParquetFile(...).read()`.
- **Provenance on meta rewrites.** `--phase meta` stamps gate verdicts into the metas and now keeps the build's `git_commit` and
  `built_utc`.

## Reproduce

Run from a pinned copy (`D:\chess\bin\pool_from_book_<sha>\`), never from a worktree:

```
pool_from_book.py --events Blitz Rapid Classical --elo-bands 2000 2200 2500 --max-ply 20 --min-games 50 \
    --tag ge2000_2013_2026_brc_p20 --work H:\chess\pool_work\ge2000_2013_2026_brc_p20
pool_from_book.py ... --min-games 20 --tag ge2000_2013_2026_brc_p20_mg20 \
    --work H:\chess\pool_work\ge2000_2013_2026_brc_p20_mg20 --arrivals-from H:\chess\pool_work\ge2000_2013_2026_brc_p20
pool_from_book_gates.py {term-vs-book|reimpl|conservation|evals|mass-split} --work <work>
pool_from_book.py ... --phase meta        # stamp the gate verdicts into the metas
```
