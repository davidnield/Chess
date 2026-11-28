# Plan from 26 November 2026: ownership back with Claude

*Claude. David handed the project back on 26 Nov after
[my review of ChatGPT's 22–26 Nov work](claude-review-of-chatgpt-2026-11-26.md) and asked me to implement our
decisions and the review's recommendations. Those decisions are recorded in the review's "David's decisions"
section; this plan turns them into an order of work. Branch `claude/ownership-20261126` (from `069b0e4`), in this
worktree.*

## The goal, in David's words

One repertoire for OTB classical and for online play against players stronger than he is now. Current ratings:
1250 OTB, 1800 Lichess Rapid, 1700 Lichess Blitz. Lifetime goals: 2000 OTB and 2200 online. The openings must help
him get there and stay effective once he does. He prefers advantages that are easy to convert.

## What that means for the model

| part | decision | status |
|---|---|---|
| Opponent | `otb-ensemble-v3`: reply = λ·(OTB model, smoothed toward online) + (1−λ)·(online model), λ = 0.5 to start; v2 kept bit-identical | **code done** (26 Nov) |
| Soundness | keep the stricter gates; they keep the book sound at 2000/2200 | unchanged |
| Choosing a book | best weighted average across online-now, online 2200+, OTB < 1600 and OTB 1800–2000, among books within a small margin of the best at 2200+ | to implement in finalist scoring |
| Own play | build with a weaker-than-current own model (Maia 1200 or 1450); check finalists at 1700 and reject a book whose advantage depends on weak own play | grid supports it; calibration pending |
| Own counts | observation cap (100 / 1,000 / none) in `(c + K·q)/(n + K)`, so the model keeps a share where weak players are still in their own book | **code done** (108-transform grid) |
| In-book contamination | measure: the same position reached by a popular route vs a rare transposition | study to run |
| White activation | qualified local SF18 search for booked or gate-deciding White children within about 3 expected-score points of a gate where sources disagree on the side | to implement before any White activation |

## Order of work

1. **Code from the review: done on 26 Nov.**
   - F1, a fixed referee binding: `sweep.referee.play(referee_binding=...)`. The construction lookup is never
     consulted for a fixed referee; the role is recorded.
   - The opponent v3 mix and the own observation cap.
   - F6, the extension player term labelled as a legacy-own-model control.
   - The F7 mate-0 census (evidence; the producer is unchanged).
   - Tests: `_test_review_20261126` (6) and the affected suites.
2. **Pools.**
   - Weak finishes; broad follows automatically (`sweep.count_pool`, config-v2; weak was at bucket 456 of 512 at
     10:00 on 26 Nov).
   - Adult sorted publication, stage 1: `D:\chess\adult-publication-20261126\run_sort.bat` (release
     `pool-input-readiness-20261125-v1`, config sha `aeb76269…`), launched 08:52 on 26 Nov. About 2.4 min and
     1.8 GB per bucket, so ~20 h and ~920 GB on E:. Log `sort.log`; graceful stop: create `STOP` there.
   - Stage 2, **code done** (3ef68a5): `pool_from_book_gates --partitioned` runs the four gates on the complete
     inventory and binds each verdict to its digest; `sweep.partitioned_pool --assemble` writes the acceptance
     document only from passing, bound verdicts. Conservation checks the mass identity in all 512 buckets and the
     per-parent identity in the sampled ones. Driver `run_gates.bat` (release `partitioned-gates-20261126`), to be
     launched by hand once the sort has finished, after a commit check: reimplementation loads a whole book bucket
     (12 slices) at a time.
   - **Adult publication done 27 Nov 18:21** (`D:\chess\adult-publication-20261126\gates\acceptance.json`).
     - The sort finished at 04:21.
     - The first gate run stalled: any single DuckDB scan of all 6,144 book files in one process decayed to
       ~27 MB/s with its memory at the limit, whether it aggregated or streamed. Splitting the GROUP BY did not
       help.
     - The fix (2a8d823) reads arrivals with one fresh process per book file (~3 s each, 13 min in all), the same
       way the per-bucket mass check already worked. Release `partitioned-gates-20261127-v3`, driver
       `run_gates_v3.ps1`.
     - Results:
       - Conservation: all 512 buckets balance at 39,217,224,723 games, 0 of 162,981,823 sampled parents
         mismatch, and the root is exact.
       - Reimplementation: 8 buckets identical.
       - Term-vs-book: residual 0 over 7,969,488 endings.
       - Evals: invariants hold. It checked 0 parents by construction, because an uncut pool has no
         below-floor edges.
     - The old `run_gates.bat` misread a killed process (exit −1) as a pass; the new driver checks
       `ExitCode -ne 0`.
   - `D:\chess\restart_jobs.bat` restarts the count pools and the sort after a reboot.
   - Capacity is rechecked before each launch.
3. **Paired Black source pilot on the runnable legacy human path** (David accepted).
   - cdb2 against score334, stricter gates, B = 2000, p30.
   - Scored under **both** bindings plus fresh SF18 on the leaves carrying the most paired-difference mass. If the
     two disagree, the SF18 leaves decide.
   - Needs the adult and broad publications.
   - **Found on 26 Nov:** the legacy weak stop read `--stats` and `--weak-pool` only as single files. The adult
     pool is now partitioned and the weak pool compact, and the builder refused the compact pool off the
     corrected path, so the "runnable legacy path" was not runnable on the published inputs.
     - Fixed: `weak_stop._edges` fetches a manifest's rows by full key through `KeyedParquet`.
     - `_test_weak_stop_manifests` holds W, R and the counters identical to the single-file run.
     - Single-file inputs take the old query unchanged.
   - It also reports the OTB-weight distribution over the pilot graph's opponent reach (review F3) and real cost
     numbers.
4. **F4, prototyped on that real Black graph:** unexpanded W/R children valued at their engine eval with a
   calibrated error band ([0, 1] only with no eval), and paired difference bounds for top candidates. Measure the
   unresolved mass before building the global budget in Rust.
5. **Own-policy calibration** (108 transforms over six raw inference sets, mover-level validation) and the
   **in-book contamination study**. **Done 26 Nov** ([results](own-calibration-2026-11-26.md)).
   - Maia-3 1450/1800, temperature 1.0, K = 100 fits held-out 1000–1799 movers best, beating own 1700 and 1200 by
     about 0.02 nats per move.
   - Contamination is real but small: off-book movers drift from the counts toward Maia at heavily played
     positions.
   - Recommended own policy: that transform with observation cap 1000.
6. **λ rescoring:** the pilot graph under v2 and v3 at λ ∈ {0.3, 0.5, 0.7}, then David sets λ and the robustness
   margin.
7. **Corrected-path Black pilot** (`pilot-acceptance-v2`), then the trimmed optimizer comparison (in-model objective
   on fixed real graphs; referee only where the book changes), then the staged sweep.
   - **Screening for the extension comparison** (David, 27 Nov; idea from the SiegeChess race). **Code done 27 Nov**
     (`sweep/screening.py`, `sweep.spec --screening` for manifest v6, `_test_screening.py`). Today every one of
     the 36 queued variants (4 anchors × 9) gets 4 paired seeds: 144 referee runs, ~48 h at ~20 min per
     2,000-move book and seed.
     - **Screen:** seeds 1–2 for every variant, compared within its anchor's family. A variant is dropped when it
       trails the family leader by more than 2.33 × its 2-seed paired SE (~0.0005) plus a 0.001 tie margin.
     - **Confirm:** the survivors and the anchor's default-calibration run get fresh seeds 3–6. The reported
       paired scores use those seeds only, so nothing selected on seeds 1–2 is scored on them.
     - **Dropped variants** keep everything that comes from the build (distinct prescriptions, disagreement,
       unbooked risk, coverage, runtime), and their score is labelled as a screening result.
     - **Expected saving:** 8–16 h (96–120 runs instead of 144), depending on how many survive. It only cuts
       referee time; every variant is still built. Not used for the λ rescoring, where 4 arms save nothing.
     - **Rules:** fixed before any seed-1–2 result is read. Behind a default-off flag that is an exact no-op;
       a new manifest version (v6), because the v5 `evaluation` field is bound into its digest. Tests: the drop
       rule on synthetic families, confirm seeds disjoint from screening seeds, and the flag-off report unchanged.
8. **Before any White activation:** the gate-adjacent SF18 checks, and the confirmation statistics recomputed with
   an opening-family cluster bootstrap.
   - **Cluster bootstrap: done 26 Nov** (`scratch/chessdb_import/cluster_bootstrap.py` →
     `D:/chess/acceptance/review-20261126/cluster-bootstrap.json`; 20,000 replicates).
     - The panel has no move history, so a family is a connected component of positions that share at least T of
       64 squares.
     - The 299 positions are nearly unrelated: at T = 56–60 only 6 positions pair up, and the intervals equal the
       ordinary ones.
     - Blend minus proposal is −0.084 points:
       - at T ≥ 56 its simultaneous interval is [−0.164, −0.005];
       - grouping positions that share ≥ 52 squares (largest family 77) makes it [−0.156, +0.013], which includes
         zero.
     - Looser T chains almost everything into a few components, so no family structure is left to test.
     - Blend minus ChessDB alone (−0.246) excludes zero under every clustering.
     - Reading: the gain over the proposal is real but marginal, and depends on how related positions are treated.
       That supports carrying both rules into the book-level pilot rather than treating the blend as settled.
   - The White gate-adjacent SF18 check needs a White book under score334; it waits for White activation work.

## Standing rules carried forward

- Stockfish 18 is the reference; never switch engines without re-measuring the cp scale.
- Lifetime books stay the playing references.
- Switch rule: ≥ .005 over 4 paired seeds, beyond bootstrap uncertainty, no commonly reached move over the
  soundness limit on fresh SF18.
- New flags default off and are exact no-ops.
- Frozen releases are never edited in place. Coordinators stop before their children. Receipts are kept.
- F: is on the NAS for David's backup. No further deletion of source-derived data until a verified second copy of
  ChessDB v6 exists.
