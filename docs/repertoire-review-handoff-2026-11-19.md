# Repertoire review handoff: November 14–19, 2026

Prepared by Codex for David and Claude. **Live-state snapshot: November 19, 2026, approximately 18:02 PST.** Implementation baseline for this review: `ffb11e2e28162a4224c2e06f1806724b67424dff`, branch `codex/sweep-preparation-20261118`. This document adds no production code and does not start, stop, or change a job.

## Request to Claude

Please review the code, evidence, decisions, and remaining plan below before recommending further production builds. David wants feedback on both correctness and whether the approach serves his lifetime-repertoire goals. Distinguish a demonstrated defect, an untested boundary, a modeling limitation, and a performance improvement. Do not treat a recorded successful fixture as proof of production behavior.

For each actionable finding, provide severity, exact file/line, a reproduction or concrete counterexample, its effect on repertoire choices or evaluation, a proposed fix, and a verification test. Separate **must fix before the Black pilot**, **must fix before the main sweep**, and **can defer**. Review the existing changes first; do not repin or restart the active migration, launch the whole sweep, or promote a new canonical book as part of this review. The currently running work should continue.

Start with sections 1, 2, 11, and 12 for state and remaining questions; use the rest to audit the reasoning and implementations. The companion [evidence inventory](C:/Users/David/Documents/Chess/docs/repertoire-review-handoff-evidence-2026-11-19.json) records full commit IDs, worktree states, deployed file hashes, receipt hashes, process commands, disk observations, and the manifest's actual horizon recipes. It is a snapshot, not a live dashboard.

## 1. What is complete, active, and still only planned

The large Lichess extraction and merged explorer are complete and independently accepted. The refreshed OTB corpus now has an ancestry-preserving split and accepted full 128-bit move-count tables. The Cloud/Fishnet eval rebuild is running; its independent verification and full-key lookup are automatically chained afterward. New repertoire construction has **not** started on these final inputs. No new book from this program has replaced the canonical repertoires.

| Work | State at snapshot | Evidence or next boundary |
|---|---|---|
| Modern-month 51-ply/128-bit extraction pilots, compact format, direct-cap and downstream rehearsals | Complete | Receipts and tests described below |
| All 163 training months, January 2013–July 2026 | Complete | 4,530,190,835 raw source games; per-month archives retained |
| Compact production explorer merge | Complete | 512 buckets, 129,759,758,235 aggregate rows, about 4,503 GB decimal |
| Independent explorer acceptance | Complete, November 19 at 10:36:52 PST | All 55 verification stages passed after bounded-memory recovery |
| Eval capacity pilot | Complete at 14:02:29 PST | Eight buckets; 177,463,155 output rows, 7.40 GB |
| Full Cloud/Fishnet eval DB | **Running** | Source phase E complete; child-routing phase C had completed 8 of 47 groups |
| Independent full eval verification, then full-key lookup and terminal corrections | **Automatically queued** | Same pinned eval coordinator; conditional on preceding success |
| OTB split repair and frozen inputs | Complete | Historical role preservation, full source ancestry, conflict quarantine |
| OTB full replay, nine count exports, independent verification, actual policy-reader check | **Complete** at about 17:30 PST | Count receipt says `training_counts_ready: true` |
| Allie investigations | Completed exploratory evidence; follow-ups deferred | David chose position-only Maia because Allie's measured rollout cost was unacceptable |
| Reproducible sweep, selective policy, joint engine verification, unlimited continuation framework | Implemented and fixture-tested | Production inputs, integrations, and acceptance pilot still required |
| Shared uncut online pools, repaired-split model recheck, production evaluator binding | Planned; not running | Final eval/lookup and measured export capacity precede full pool publication |
| New Black stricter 2,000-position pilot, main books, extensions, sensitivities | Planned/manifested; not automatically executing | Acceptance gates remain in force |
| ChessDB integration | Download verified; WSL/offline read probe passed | **Not imported into this eval build**, not in its automatic queue |

### Active process, locations, and ETA

The heavy native worker was PID **16820**, started at 14:02:29 PST:

```text
D:/chess/bin/ply51-evals-rehearsed-20261115/explorer-extract.exe evals
  --book E:/chess/position-stats/brc-p51-train-201301-202607-20261115
  --cloud H:/chess/evals/cloud_2026-10-07
  --fishnet H:/chess/evals/fishnet
  --work H:/chess/eval-work-p51-20261115
  --out D:/chess/eval-db-p51-20261115
  --threads 12 --mem-gb 55 --below-normal
```

Python PIDs 37504/19752 are the recovery wrapper and its interpreter; 33952/25340 are the eval coordinator and interpreter. These are one process tree, not four separate compute jobs. No Stockfish, Allie, OTB count, or repertoire builder worker was active in the inspected process list.

Operational paths:

- Coordinator: `D:/chess/bin/ply51-evals-rehearsed-20261115/run_ply51_evals.py`.
- Overall operations directory: `D:/chess/ply51-migration-20261115`.
- Eval status/layout/capacity: its `evals/status.json`, `layout.json`, and `capacity.json`.
- Live native progress: `verification-recovery-sliced-20261119.err` in that operations directory. Normal progress is written to this file despite its `.err` suffix. The adjacent `.log` contains wrapper output.
- Accepted explorer receipt: `verification-recovery-sliced-20261119/verified.json`.
- Full eval output: `D:/chess/eval-db-p51-20261115`.
- Future full-key lookup: `D:/chess/eval-lookup-p51-20261115`.

All 164 eval-source units—20 Cloud and 144 Fishnet—had completed phase E by 17:24:10 PST. Phase C routes 512 explorer buckets in 47 groups of up to 11. The first eight groups took 215–302 seconds each and wrote approximately 40.08 GB per group. Their mean implies about **2.7 hours of C work remaining at 18:02**, with a reasonable 2.5–3.5-hour range. Native join/publication phase J follows.

Planning ranges, not deadlines: J about 5–8 hours after C; independent verification about 5–12 hours; lookup about 4–10 hours. The last two have not been measured at this full scale. The resulting estimate remains roughly **18–36 hours from this snapshot for the accepted lookup**, around midday November 20 through early November 21. Verification or I/O contention can extend it. Do not translate the small fixtures into tighter promises.

The pipeline checks receipts and propagates failures. A bare `_DONE`, an old status timestamp, or existing Parquet files is not acceptance. Reinspect the process tree and current receipts before resuming anything; do not launch a duplicate coordinator while this one is alive.

### Storage and resource policy

At the process/disk check near this snapshot, free space was approximately D: **1,627 GB**, E: **2,463 GB**, F: **3,125 GB**, H: **1,514 GB** (decimal). E: is actively shrinking as child shards accumulate. Source shards use H:, eval output/lookup and spill allowance use D:, and child shards use E: through `H:/chess/eval-work-p51-20261115/c` pointing to `E:/chess/eval-work-p51-20261115-child`.

The measured capacity forecast includes a 20% scaling margin: H: 866.6 GB, E: 2,239.0 GB, and D: 917.3 GB. The selected layout passed the existing 150 GB per-drive forecast reserve. Do not subtract the full forecast again from current free space as if none of the shards had been written. Live checks abort below the separate 80 GB emergency threshold.

David approved **80 GB for extraction** after the earlier 55 GB limit caused excessive passes, and rejected increasing it all the way to 110 GB. The active eval command's **55 GB** is a different stage's setting and was not changed by that extraction decision. These are tracked budgets, not hard limits on total process working set.

F: is now the **last-choice fallback** for eval temporary child shards, behind H:/D:/E:, selected only if necessary while retaining the reserve. Tests cover priority, insufficiency, and reserve preservation. It was not selected for this run.

### Historical jobs that must not be mistaken for current work

The old `rare-verify-v1/rs` four full-menu cp60/cp100 books have `final.json` receipts. The corresponding `lf` directory has finals for White cp60, White cp100, and Black cp100; **Black cp60 has no final receipt**, although a top-level `_DONE` exists. No worker from that chain was active. The November 14 instruction to resume `lf_chain2.bat` is historical, not an instruction to do so now.

`D:/chess/rare-verify-v1/black-030-horizon-v2/status.json` still reports queued work from November 14 at 23:54. Its old PID is gone, and no final book receipt was found. The Codex automation named “Finish the Black 0.30 pilot” is **PAUSED**. This is an inactive historical queue, not an accepted pilot or an automatic next job. Do not infer a currently active hourly monitor from the earlier conversation request; this handoff verifies the process tree and that specific paused automation, not a guaranteed ongoing monitoring service.

The old Black 0.30-centipawn pilot also has different data and gates from the new Black **stricter expected-score** pilot. Acceptance of one would not automatically accept the other.

## 2. Where to review the code and documents

The main checkout remains on `review-fixes` at `d75daa2` and has unrelated modified/untracked annotation, explorer, tournament ingestion, and puzzle work. Do not reset, switch, stage wholesale, or assume that checkout contains the new sweep implementation.

| Worktree / branch | Implementation HEAD at this snapshot | Role |
|---|---|---|
| `C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation` / `codex/sweep-preparation-20261118` | `ffb11e2` | **Primary integration and review target** |
| `.../scratch/worktrees/full-key-consumers` / `codex/full-key-consumers` | `34ace88` | November 15 downstream integration baseline |
| `.../scratch/worktrees/keys128` / `keys128` | `97da558` | Extraction, compact storage, migration orchestration |
| `.../scratch/worktrees/lookahead-floor` / `lookahead-floor` | `4d7c945` | Expected-score gates and corrected horizon/lookahead work |

The full-key-consumers and lookahead-floor worktrees have local `docs/lifetime-repertoire-plan.md` modifications; those are documentation, not a reason to discard the worktree. The sweep worktree was clean before this handoff. The evidence inventory records exact statuses and full hashes. Other sessions' annotation commits on November 14 appear in the all-branch inventory but are outside the migration/sweep changes authored in this chat.

Use the following precedence:

1. Explicit user decisions summarized here, including November 19's extension default and Maia choice.
2. [Current tuning plan in the sweep worktree](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-tuning-plan-2026-11-18.md), including its dated amendments.
3. [Current Maia manifest](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/sweep-manifest-2026-11-19-maia.json) and actual code, checking for conflicts with the plan.
4. [Progress/evidence log](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/sweep-preparation-progress.md) and [usage/contracts](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/sweep-preparation-usage.md). Later entries supersede earlier “pending” statuses.
5. Older handoffs and result reports as historical evidence.

The similarly named tuning-plan file in the **main checkout is an older, untracked copy**. The November 18 and earlier November 19 manifests remain historical records. The current manifest selects Maia and remaining-maximum extensions, but has the horizon-budget discrepancy described in section 11.

### Commit map

Claude's November 14 handoff was based on `3d99336`. Its just-committed baseline consisted of `5645037` (storage docs), `16c4284` (subset-speed merge), `512be2b` (**unsafe draft** extraction launcher), `8ae4899` (initial lookahead floor), and `008db6c` (seed overlay). Claude explicitly stated that the review findings were not yet fixed. The newer work below must be reviewed against that boundary, not attributed to the old implementation.

| Commits | Changes and review focus |
|---|---|
| `c9824ed`; `4d7c945` | Expected-score gates; route-specific horizons; complete admitted candidate menu for lookahead; source expansion; veto/evidence receipts; post-selection policy audit |
| `57412e5`, `cf0ac1e`, `94de25d` | Verified monthly publication, source/header checks, sliced termination data, pilot launch and receipts |
| `b54e3a0`, `e4514e6`, `ab64cbb` | Within-chunk full-key conflicts, pinned-source checks/empty blob repair, terminal digest and direct 30/50-cap parity |
| `8d6c5e8`, `63d1b31`, `27c68f3`, `3bfd358`, `a1aaf84` | Pilot/training coordinator, holdout audits, full-key lookup/source attribution, source-byte-bound eval resume, offline ChessDB probe and queue documentation |
| `35254b8`, `b72b0dc`, `527da1d`, `b087593`, `9daf0d8`, `8ea9420`, `97da558` | Compact book-v4 storage, logical readers, measured certification, independent monthly scheduling, recovery and terminal parity, bounded Sunday head start |
| `5292f91`, `8a9e7be`, `d9b7d51`, `655dcc0`, `34ace88` | Full-key consumers, budget and rollout keys, both Maia cache ratings, engine nominations/rare checks, F: fallback, end-to-end rehearsal fixes |
| `5452e62`, `7f751fd` | Integrated full-key score-gate builder, manifests, quality rules, extension/recall/report framework and tests |
| `e4c79fa`, `175336a` | Selective policy binding, shared verification coordinator, continuation adapter; Parquet pruning and verified-publication bindings |
| `4a6a64e`, `e518f3c`, `f84a58e`, `de6c19f` | Bounded/sliced production explorer verification, resumable receipts/heartbeat, native/compact regression evidence |
| `0749b1d` | Remaining-maximum extension default; 24 deferred metric/threshold comparisons |
| `c0f3793`, `85226a1` | Allie history-cache evidence; approved Maia evaluator and deferred Allie work |
| `76c6e42` | Bounded external sort and streamed shared-pool finalization |
| `c80f80b`, `def2b7c` | Refreshed OTB identity audit, complete ancestry reconciliation, frozen texts/splits and independent verifier |
| `c810d7e`, `ffb11e2` | Native OTB replay, resumable count exports, full independent checks, real policy-reader compatibility, accepted publication |

Some integration was applied across branches rather than preserving every side-branch SHA as an ancestor. Review code and provenance, not merely whether `git merge-base --is-ancestor` succeeds. Useful read-only comparisons are `3d99336..ffb11e2` for the integrated result and `34ace88..ffb11e2` for the sweep/OTB layer. The evidence inventory includes full commit IDs and changed file lists.

**Deployed code is separately pinned.** The active eval native executable is built from `b72b0dc`; its Python release references `655dcc0` and the verifier corrections in `34ace88`. Its executable SHA-256 is `0f79bcbb95f1055fbedc075fe2c5db5ecee9a33ec3645786bdf23c62726453c0`. The active sliced-verification recovery is a newer, separate immutable release. Do not overwrite these binaries by rebuilding in place.

The November 16 adaptive and 80 GB launchers under `D:/chess/bin/ply51-queue-adaptive-20261116` and `ply51-queue-80gb-20261116` include deployment patches. Their pin's baseline commit does not by itself describe every deployed byte. The inventory records actual hashes. Review and reconcile these patches into maintained source before a future fresh training run. They retry the recognized native memory-budget/pass-count condition using the suggested additional passes; unrelated failures must propagate. Completed month receipts remain reusable.

## 3. Goals and gate decisions

David wants one learnable repertoire per color for online and classical play, viable as he improves from approximately 1800 Lichess Rapid/1700 Blitz and provisional 1100 USCF toward 2000 online and 2000 USCF. We are comparing learning costs rather than assuming exactly 2,000 is optimal. A prescription is a distinct own-move position, counted once across transpositions, not a PGN occurrence.

The objectives remain practical continuation performance, positions that weaker own play can convert against stronger replies, and protection against objectively unsound moves or squandered advantages. Report these separately. The Lichess engine-to-score conversion is a useful optimization scale; it is **not a calibrated prediction of David's actual OTB win probability**. Draws, exact terminal outcomes, opponent-model error, and learning assumptions matter.

### Expected-score gates replace fixed centipawn loss

With centipawns from White's perspective:

```text
S(cp) = 1 / (1 + exp(-0.00368208 * cp))
White absolute check: S(child) >= White_baseline - White_tolerance
Black absolute check: S(child) <= Black_baseline + Black_tolerance
Relative check: own-side score loss against the best trusted reference <= cap
```

| Arm | White absolute tolerance | Black absolute tolerance | Relative loss cap, both colors |
|---|---:|---:|---:|
| Stricter practical | 0.050 | 0.050 | 0.060: six score percentage points |
| Permissive | 0.110 | 0.075 | 0.095: 9.5 score percentage points |

Freeze White's White-score baseline at **0.5183137990703003**, and Black's at **0.5195669627673518**, across gates, budgets, policies, and horizons. Record new pools' observed starting scores separately. The absolute tolerance is measured from this fixed baseline, **not from 0.00 cp**. Approximate absolute cutoffs are White −34.5 cp / Black +76.1 cp for stricter, and White −100.7 cp / Black +104.0 cp for permissive, all White POV. A relative percentage-point loss is not a constant cp allowance: it permits more cp loss in already strongly winning/losing positions.

The permissive decision deliberately keeps tested practical gambits, rather than promising that every gambit line is safe. Fresh 20M-node checks of the King's Gambit offer cost about 8.53 relative points and 7.60 absolute points; a tested Kieseritzky continuation through 7.exd5 cost about 10.13 absolute points. From's offer cost about 8.53 relative points and 3.73 absolute points. Earlier search depths differed, which is why tight cutoff claims require fresh checks. The 11-point White / 7.5-point Black / 9.5-point relative settings were chosen with those examples in mind.

The stricter stored-evidence audit retained the tested Alapin, Reti with 1.Nf3 d5 2.c4, Vienna Gambit routes, Scandinavian, Modern, Alekhine, and Orthodox King's Indian routes. It excludes more speculative concessions such as the tested King's/From's offers. This is route-specific evidence, not blanket certification of all named opening variations on the new database. Frozen opening reports are under `C:/Users/David/Documents/Chess/scratch/eco-relative-20261114/`.

Both absolute and relative checks still matter. Repeated individually permitted concessions can accumulate; engine evaluations can drift; finite local/lookahead checks do not prove perfect-play safety. Require coherent comparable engine evidence, final independent checks, and a separate best-defense audit. The manifest currently sets lookahead off; section 11 makes that explicit.

## 4. Extraction, compact storage, publication, and explorer recovery

### Schema and correctness decisions

David explicitly approved extracting **51 plies** to permit correctly building/filtering through game ply 50, including the next-move boundary. Keys are 128-bit throughout persistent data. Move records retain ply; termination data now retains termination kind/reason, ending ply, speed, and game-mean rating band so filtered pools can distinguish endings instead of using a globally pooled ending count.

The extraction/merge speed ordering is the native canonical subset order **Blitz, Classical, Rapid**. A source month may legitimately lack Rapid, as the inspected 2013 months do. Missing data must be distinguished from a verified zero using independent source/month/speed inventory. The older wrongly ordered 2013 pilot outputs remain size-comparison artifacts, not merge inputs.

The launcher now validates resolved disjoint paths, single-publisher ownership, source and executable identities, extraction settings, staged Parquet headers, completion manifests, and readback hashes before cleanup/publication. Recovery handles interruption around publication rename/sentinel boundaries. Tests deliberately mutate same-size files and simulate incomplete archives; an empty archive with only a completion marker must not satisfy a 51-ply request.

Native within-chunk aggregation now rejects conflicting EPD/child mappings sharing a full key before that conflict can disappear into an aggregate. Both key halves are needed for every persistent identity and join. Full-key Python/Rust bridges can use dense run-local 64-bit IDs internally, but these must never become durable board IDs.

### Compact book-v4

The merged explorer uses packed EPD rank/state data and parent-to-child XOR columns to reduce repeated physical data. Consumers obtain ordinary logical columns through `explorer_storage.relation` / `read_columns`; blindly treating raw compact Parquet fields as the old logical schema is incorrect. Published ordinary pool/lookup formats remain separate consumer interfaces.

Measured pilot size fell from about **263.453 GB to 157.880 GB**, a **40.07% reduction**. Exact comparison covered 81 files / 25,752,025 rows in three buckets, all 512 move-bucket digests, and the termination digest. A small native eval A/B produced identical 105 output rows: mean end-to-end time approximately 3.93 seconds plain versus 4.85 seconds compact, about 23% slower in that tiny test. This is neither a measured full-builder penalty nor a guarantee against a different downstream bottleneck.

Modern pilot months were March 2026, June 2024, and April 2020, with historical controls. Directly capped 30/50-ply extraction was compared with filtered 51-ply source data, including flags, parse failures, and games continuing through the horizon. The compact format was certified before production merge.

### Head start and full run

The bounded Sunday head start extracted **July of every year 2013–2026**, 14 complete months / 396,346,195 raw games, finishing November 15 around 18:41. The spread sampled different data vintages and month sizes. The coordinator schedules missing months independently, so nonconsecutive completed Julys did not complicate the Monday continuation.

All 163 January 2013–July 2026 training months are now extracted and archived in `F:/chess/explorer_brc_p51_pilot`; those immutable monthly receipts allow resuming without redoing accepted months. The merged production explorer is at `E:/chess/position-stats/brc-p51-train-201301-202607-20261115`.

The first independent production verifier exhausted its 4 GB DuckDB budget during duplicate/mapping aggregation on November 19 around 01:47. The queue correctly withheld eval publication work. Recovery first replaced high-cardinality mapping checks with exact min/max invariants and fresh sequential connections, then sliced selected signed-key ranges into 16 bounded pieces, with stage receipts and heartbeat output. All **55** independent stages passed at **10:36:52 PST**, automatically releasing the eval pilot and full build.

Explorer acceptance receipt SHA-256: `959264dc725d708e43959a0b91362af32d90a0d5524b67c0c0e2daa851f8c31a`. Bound explorer metadata SHA-256: `729801c10254906fc31c9f89b504d4c981a1f70cbbf2c6546495e477c4dde632`. Inspect the receipt for which invariants are exhaustive versus sampled; the number “55” is not a claim of independently replaying every source game.

August and September 2026 remain outside training. Their official source checksums and event/header counts were reconciled, including the local ingestion treatment of tournament-labelled games. Receipts are in the operations directory's `holdout-inputs/`. These months have already been inspected during development; do not call them a pristine final confirmation set.

### End-to-end July rehearsal

`D:/chess/ply51-migration-20261115/headstart-e2e/results.json` records extraction through pool/eval/builder/scorer, not just file-format checks. The fixture included 14 months / 221 partitions, 4,656 sampled games, 4,634 kept games, 192,858 moves, and 4,632 terminal records. It checked caps 1–50 across nine rating bands and three speeds, including 3,056 games continuing past ply 50 (913 in the 1800+ subset).

A complete July 2013 replay processed 217,505 raw games / 217,291 retained, 8,215,952 distinct rows and 216,286 terminal records; 513 re-extracted files were byte-identical. Small eval/lookup fixtures covered mate handling and off-sample full keys. Toy builder outputs had 3 White and 27 Black prescriptions against a requested 30 in the limited fixture; rollout mass conserved and did not invent early nonterminal stops. This caught missing empty-file headers and stale verifier key assumptions, fixed in `34ace88`.

It did **not** validate a full 2,000-position production book, settle solver quality, or establish production pool export/runtime capacity.

## 5. Eval DB, lookup, and ChessDB

The new eval pipeline binds checkpoint reuse to verified source payload bytes, source revision/receipts, executable/settings, and explorer generation. It writes new-generation output directories so older accepted evidence is preserved. Full-key lookup generation also handles mate/terminal corrections and source attribution. Joining only the low half is not acceptable, including in “fast” sampling or receipt code.

The active source set is the October 7 Cloud snapshot plus Fishnet. Depth and node-budget metadata must retain their source semantics: `knodes` means thousands of nominal nodes, and MultiPV totals are not independent node counts for each alternative. Source depth comparisons do not eliminate the need to verify selected cloud-dependent moves locally. Preserve stronger existing evidence when a later smaller search arrives.

After native DB construction, the coordinator runs `verify_evals.py` with 8 threads / a 24 GB DuckDB budget and independent source/key checks, then `eval_lookup128.py` with 2 threads / 4 GB. It builds all 512 lookup buckets, gathers mate corrections, and performs lookup checks. Completed lookup publication must refer to the accepted eval and explorer generations, not just find a metadata file with a matching name.

WSL2/Ubuntu was installed and used successfully for the offline ChessDB probe. The empty `LOCK` file was repaired and the complete download verified: **291 files, 1,188,168,006,841 bytes**, pinned revision `9de4573da712dbf0aaecadf62d164f7ba07e8145`. Readback log: `H:/chess/_downloads/chessdb-readback-20261115.log`.

On a panel of 1,000 positions missing from previous eval coverage, the offline probe found 931 scores (928 cp, 3 mate), with 69 absent. Among the 931, 803 matched the live/offline comparison exactly; different source dates can matter. Total probe time was 6.422 seconds including database open, with about 0.334 seconds querying. This demonstrates accessibility and useful extra coverage, not a full import benchmark, final score trust, or rollout throughput. Tablebase/zero-score/halfmove semantics still need care.

**ChessDB is not part of the currently running Cloud/Fishnet rebuild or its automatic queue.** Decide its qualified integration and measure storage/throughput in a bounded import pilot before scheduling a full pass. Do not claim all three sources are already in the accepted lookup.

## 6. Full-key consumers, horizon/lookahead fixes, and quality closure

The Python pool/uncut lookup, Rust builder bridge, budget selectors, engine nominations, rare-move verification, and rollout scoring now carry full position keys. Tests include artificial boards with identical low halves and different high halves. The in-memory Maia cache was corrected to include both self and opponent ratings, not just the board. Persistent/model cache limitations still need the review in section 11.

The November 14 horizon fix distinguishes the earliest qualifying route to a board, route-specific ply, maximum reach, and the highest-probability representative route. A board initially reached too late can be reopened through a shorter qualifying route. An edge's horizon eligibility must use a concrete route, not unrelated fields spliced from different transpositions. Repetition still requires actual history; a board DAG alone cannot determine it.

Lookahead was changed to inspect the **admitted candidate menu**, including unrecorded engine nominees and moves restored from the uncut source. Restored children are expanded through the appropriate source lookup without inventing empirical counts. Lookahead can consume the assembled opponent policy. Known surviving alternatives, empty menus, and unknown evidence are distinguished before a veto; unknown does not become proof of safety or proof of failure. Veto/evidence files are hashed into receipts and checked by the post-selection audit, including SQL-side recomputation and full-key identity.

This repairs specific implementation defects. It does not establish an unrestricted minimax guarantee or prove that the old queued Black pilot completed. The current primary sweep manifest has `lookahead_floor: 0`; review that deliberate implementation state against the safety claim before production acceptance.

### Shared engine checks instead of “rare move means rerun everything”

The new pool policy is **min_games=1**, eliminating the Other Moves bucket created solely by the historical observation cutoff. A move played once is available. However, unobserved legal moves, actual terminations, horizon boundaries, incomplete source coverage, computational pruning, and opponent-model probability outside historical support still require explicit treatment. Eliminating the frequency cutoff does not eliminate all residual mass or automatically finish quality closure in one round.

Initial selection can use hybrid cached evidence. Fresh checks are triggered by consequential uncertainty: missing scores, gate-boundary cases, inconsistent parent/child scores, competing alternatives, and selected cloud-dependent prescriptions. Requests include **reached unselected candidates**, so a dubious stored value cannot permanently hide a better alternative merely because it was not selected on the first pass. A missing `c4e5` candidate regression exercises this boundary.

The starting local verification budgets are **5M nodes**, rising to **20M** for borderline/disputed cases. Within 0.01 score units of a gate is a deeper-review signal. Search the relevant alternatives and direct child positions when necessary; do not confuse a parent PV score with independent evidence at a child. Existing higher-quality checks must survive later 5M requests. Previously fixed 20M-to-5M downgrades remain a regression target.

`sweep.verify` unions/deduplicates work across arms. `sweep.coordinator` snapshots inputs/code, shares accepted engine evidence and nominations, rebuilds every affected arm, and repeats until no consequential requests remain. Engine nominees absent from historical play do not receive fabricated observations. Reduced children use the matching parent's current epoch. A work/round cap leaves the run OPEN and resumable; it does not publish quality closure. An undersized provisional search is not a final exact-budget book.

Full graph reconstruction is still the correctness fallback. **Retained-graph/incremental correction is not implemented.** The goal is to reduce duplicated searches and cutoff-driven rounds, not to promise a fixed number of rebuilds or restore the old two-hour throughput without measurement.

## 7. Opponent population, ensemble, and Allie findings

### Accepted construction policy

The earlier OTB work found that very narrow “strong classical only” data can be too thin for rare candidate lines. We are retaining broad legal candidate discovery and smoothing opponent predictions toward more targeted online and OTB data, rather than building only on a tiny classical subpopulation. A prior rare-candidate study had about 25.8% next-reply coverage from the targeted pool plus OTB versus 99.2% from BRC1800 on 1,414 rare own alternatives. This illustrates why evaluating coverage only on already selected books is misleading; it is not a universal accuracy percentage.

The November 13 ensemble is carried forward as the primary policy, pending reproduction on the repaired OTB split. For each legal reply, let `b` be all-rating Blitz/Rapid/Classical counts, `a` all-three-speeds 1800+ counts, `r` Rapid 2200+ plus Classical 1800+ counts, and `o` eligible OTB training counts for movers below 2000. Capitals denote totals, `L` the legal move count, and `q` Maia probabilities:

```text
broad   = (b + 1/L) / (B + 1)
backup  = (a - r + 30*broad) / (A - R + 30)
online  = (r + 30*backup) / (R + 30)
support = R + 0.10*(A - R) + 0.01*(B - A)
pre_otb = (support*online + 3*q) / (support + 3)
reply   = (o + 10*pre_otb) / (O + 10)
```

Use **Maia-3 79M, position-only, input ratings 2600/2600, temperature 1.35** in this construction ensemble. Empirical smoothing strengths are 30, Maia strength is **3**, and the OTB update's prior strength is **10**. This is **not** the old Maia-30 shrinkage applied afterward. At zero empirical support, the policy reduces to Maia. Enforce subset inequalities per move (`b >= a >= r`) and subtract the overlapping targeted subset before constructing backups. Rapid and Classical components are disjoint by speed. Preserve real nonmove/ending mass outside these conditional reply vectors.

The own-move candidate menu remains broad uncut BRC1800 for both colors plus shared verified engine nominations. Opponent likelihood and own candidate eligibility serve different purposes. Construction online rating bands are **game-mean** bands from the extractor; evaluation populations use the **moving player's** rating. Do not silently treat those as the same population or convert Lichess ratings into USCF ratings.

An earlier chess.com combination study did not justify replacing the Lichess baseline; chess.com's position coverage is selected rather than a game census. No chess.com or Chessbook proprietary model component was added to the current recipe. The detailed pre-November-14 evidence is in the [OTB opponent handoff](C:/Users/David/Documents/Chess/docs/otb-opponent-reply-handoff-2026-11-13.md); close margins must now be rechecked after the split repair below.

### What the Allie tests actually showed

There were three different experiments; they should not be conflated.

**Initial direct prediction pilot:** 128 games, 512 OTB positions, equally distributed among plies 1–10, 11–20, 21–30, and 31–50, from November 2–8. It was concentrated in TWIC (120 games), with six Mechanics and two Minnesota games, spanning 28 events.

| Predictor | Brier score, lower is better | Top-1 accuracy |
|---|---:|---:|
| Maia position-only | 0.58335 | 53.32% |
| Maia with history | 0.55265 | 56.93% |
| Allie with history | 0.50352 | 60.74% |

Allie minus position-only Maia Brier was −0.07982, game-bootstrap 95% interval approximately [−0.09970, −0.06035]; versus history Maia, −0.04913 [−0.06343, −0.03597]. This supports further interest, but it is a small clustered recent panel, not comparison against the tuned empirical/OTB ensemble, not player/event-cluster confirmation, and not proof of improved repertoire performance.

**Allie backend validation:** This compared Rust and Torch implementations of Allie, not Allie versus Maia. The OTB panel contained 2,639 positions / 664 games / 33 events from November 2–11, excluding the initial 128 games. Rust's Brier difference was −0.0000496 with a 1.732x speedup. Online results were:

| Online band | Positions / games | Rust minus Torch Brier | Speedup |
|---|---:|---:|---:|
| 1000–1799 | 2,276 / 600 | +0.0000791 | 1.744x |
| 1800+ | 2,327 / 600 | +0.0000367 | 1.748x |
| 2200+ | 2,331 / 600 | +0.0002768 | 1.750x |

All passed the chosen noninferiority screen (Brier 0.002, log loss 0.005, top-1 0.005). Predictions are not bit-identical; quantization/query-order effects motivate a frozen first-writer cache. These tests used a fixed model rating/clock treatment and do not establish calibrated OTB probabilities.

**History/rollout cost pilot:** 256 games, 128 per color, using canonical books. Cold Allie rollouts took about **87 minutes White and 94 minutes Black**, versus Maia about **9.2 and 4.4 minutes** on this machine. This was the older slower Allie backend on CPU versus Maia on GPU; the backend speedup does not imply cost parity or a universal hardware ratio. The observed roughly 10–20x end-to-end cost was unacceptable to David.

Actual packed game histories occupied only 24,628 bytes, about 96 bytes/game. A history atlas took about 89 minutes and 970,752 bytes; the approximately 18.4 GB Allie model and inference dominated storage/time. Modal position-history contexts were available at only 5/4,278 White and 9/4,451 Black queried positions, with no actual replacements. Identical modal scores were therefore a fallback result, **not evidence of modal-history equivalence**. Exit-history substitution made only 3 White and 10 Black replacements; score differences of about +0.00596/−0.00312 had intervals too wide to establish equivalence at 0.002.

The direct cache audit found 8,711 history/rating contexts collapsing to 8,692 position/rating keys: only 19 entries, **0.218%**, could be removed. Eleven positions had multiple histories; their maximum probability-vector total variation was about 0.587. A modal shortcut's meaningful cost reduction and acceptable accuracy loss have not been demonstrated. Higher mean rollout scores under a different generative model do not by themselves prove the books improved.

**November 19 decision:** retain **Maia-3 79M position-only for construction and rollout evaluation**. Allie follow-ups are deferred and no longer a prerequisite. Preserve the completed evidence/caches. Keep actual chess history for repetition and exact game rules even though the neural model is position-only. Construction's 2600/2600 prior setting does not force all evaluator player panels to that rating. The earlier model-evidence JSON retains its historical pending-backend decision; the later Maia manifest and dated plan amendment supersede that field.

Evidence: [model evidence index](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/sweep-model-evidence-2026-11-19.json), `D:/chess/allie/backend-validation-20261118/comparison.json`, `D:/chess/allie/online-backend-validation-20261118/comparison.json`, and `D:/chess/allie/history-20261118/pilot01/comparison.json`.

## 8. Refreshed OTB identity repair and accepted full-key counts

The enlarged deduplicated OTB corpus changed some game identities. Reusing old numeric IDs/record offsets or hashing the new IDs into fresh folds would silently move historical training/test games across partitions. The first representative-only audit found 4,690 current families combining historical test and non-test records. That was a warning, not a complete ancestry proof.

### Complete ancestry and frozen split

The final freezer follows **all 8,484,681 archived source records** from the 4,037,682 historically selected games. Archived membership uses zero-based `rid`; published record IDs use `rid + 1`. It verifies the historical source-prefix fingerprint over the first 35,456,972 records, old representative anchors, source locators, archived identity-code/shard stamps, and complete membership. No missing ancestral record or changed source locator was found.

Historical folds 2/3/4 stay training, 1 stays validation, and 0 stays test. Families merging different roles are quarantined, including train/validation collisions. Connected split/merge ancestry preserves the role across all descendants; tests cover one-to-many cases even though none occurred in this corpus. New deterministic assignments are allowed only for genuinely new families, then must be preserved on future refreshes.

The complete all-years ancestry revealed **8,091 mixed-role families**; 4,828 meet the current eligibility rules. The larger figure supersedes the representative-only 4,690 test/non-test finding, rather than contradicting it. Only 33,427 currently eligible games have no selected historical ancestor and received a new fold.

Frozen eligible inputs cover standard-start games dated January 1, 2013–November 19, 2026, with positive ratings for both players and at least one player below 2000, under the corpus's existing `opening_analysis` clock screen. Unknown clocks and heterogeneous published rating systems remain limitations. Freeze actual resolved movetext, ratings and labels, date/source, initial FEN, and assignment—not just mutable offsets.

| Role | Frozen eligible games |
|---|---:|
| Training | 1,509,521 |
| Validation | 503,064 |
| Test | 502,273 |
| Quarantine | 4,828 |
| Total | **2,519,686** |

Accepted split directory: `D:/chess/sweep-inputs-20261119/otb-v2-20261119`. The four frozen Parquets total 905,014,062 bytes. `split.json` SHA-256 is `e5e446803b94581c84c44ecef1d11653648a19fe4ef99b00985165cd77f05536`; its independent `verified.json` SHA-256 is `dae6c7d365632b343c4418d4ef278023a9bf8a515f41a181030f594016ce4b73`.

The split receipts intentionally say `training_counts_ready: false`: they describe the pre-replay boundary and remain immutable. The later count acceptance receipt below is the authoritative “counts ready” evidence. Small provenance inputs are archived under `frozen-inputs/` so verification survives a later source-corpus refresh.

A leading-comment/blank-line PGN case initially caused an empty parsed game before the real score. The reader now skips only empty, error-free preambles; it does not excuse illegal/empty game scores. The final split smoke replay passed 256 games / 11,808 positions. That was a sample; the next producer performed full native replay.

### Full native replay and count publication

The accepted producer is pinned under `D:/chess/bin/otb-counts-20261119`. Code is `python/sweep/otb_counts.py`, `verify_otb_counts.py`, `check_otb_consumer.py`, and the standalone `rust/otb_counts_replay` library. Rust uses shakmaty 0.30.1 and the repository's SAN parsing convention; the verifier uses independent Python chess for sampled legal replay.

The first unpublished v1 attempt exposed NUL padding in native fixed-width strings; it was stopped before acceptance and replaced with explicit trimming and a new run. Do not use `otb-counts-v1-20261119` or the separate 4,096-game pilot as production data.

**Accepted counts:** `D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119`. The count-level `verified.json` has `training_counts_ready: true`, SHA-256:

```text
2c19ff37e7bfb3bc69359000cd968fd0b71e3cad3267f135373c87bcfe6be5d9
```

The producer processed 616 resumable batches of up to 4,096 games. Every frozen game has a disposition. All 4,828 quarantined games contribute zero. One training game, ID 2816742 from lumbra-otb, has illegal SAN `22. xc6`; native and Python replay both reject its entire requested prefix. It is not converted into an early terminal or repaired by guessing. Accepted games are therefore **1,509,520 training, 503,064 validation, and 502,273 test**.

The audit ledger has **122,761,289 legal moves through played ply 51**. Of these, **101,000,635** qualify by the **moving player's rating 1–1999**. Both players' valid moves remain in the ledger for game-flow audit; a below-2000 opponent does not make a 2200 player's move eligible for the published counts.

| Role / export | Distinct aggregate rows | Qualifying observations |
|---|---:|---:|
| Training, by ply/band 1–51 | 42,814,696 | 60,616,548 |
| Training, pooled through ply 30 | 17,763,076 | 36,945,738 |
| Training, pooled through ply 50 | 40,251,755 | 59,593,882 |
| Validation, by ply/band 1–51 | 14,897,056 | 20,209,870 |
| Validation, pooled through ply 30 | 6,509,568 | 12,313,808 |
| Validation, pooled through ply 50 | 14,036,746 | 19,868,483 |
| Test, by ply/band 1–51 | 14,870,273 | 20,174,217 |
| Test, pooled through ply 30 | 6,494,591 | 12,293,451 |
| Test, pooled through ply 50 | 14,009,227 | 19,834,000 |

These exports overlap; do not sum their observations as separate games. Frequency-one moves remain explicit, and repeated game visits count as separate decisions. The master retains played ply and mover bands below 1500, 1500–1799, and 1800–1999. Build new depth cuts/band unions from the master. A pooled row's `ply` is its **minimum occurrence ply**: filtering the ply-50 pooled file on that field does not reconstruct ply-30 frequencies. Exact cutoffs within a band require reaggregation from the exact-rating ledger.

Parent and child keys are signed `struct<lo:int64,hi:int64>`, counts are int64, SAN/UCI canonical, and EPD preserves the pseudo-legal en-passant convention. These are **move counts**, not fabricated WDL statistics, engine values, or terminal/horizon masses. The published files are in the accepted count directory's `counts/` subdirectory, named `training-p30.parquet`, `training-p50.parquet`, `training-by-ply-band.parquet` and equivalently for validation/test. Bind only the training export into construction; keep validation/test separate. The incidental 2000+ opponent moves in the ledger are not a representative stronger-OTB dataset.

### Verification scope and cost

Independent verification checks every frozen game disposition, identity/role/rating selection, every retained ply/full-key chain, all artifact hashes, all master and pooled aggregate counts, and missing/extra/duplicate rows. A separate Python replay checked 768 accepted games / 37,588 positions with zero mismatches. During generation, cross-checks covered 1,848 games / 89,715 positions; samples may overlap. This is not a claim that Python independently replayed every game.

`consumer-check.json` verifies exact key/count schemas for all nine files and exercises the **real** `KeyedParquet` / `frontier.vectors` reader against all six pooled files: 60 early/later boards, 811 legal SAN/UCI/full-child-key rows, count conservation, and no false hit when only the high half is changed. Checker code and runtime dependencies are hashed/archived; `environment.json` pins versions.

Nine exports occupy **10.77 GB**; resumable ledger/disposition Parquets occupy **5.76 GB**. Replay/export ran 15:29:44–16:21:11, about 51.5 minutes. Independent verification took 4,071 seconds, about 67.9 minutes, alongside the ongoing eval rebuild. It used two DuckDB threads, an 8 GB tracked budget, spill, and the 150 GB free-space check.

Remaining statistical obligations: recheck close OTB component comparisons on the repaired split; do not label previously inspected partitions fresh confirmation. The frozen game/count schema does **not** include a complete player/event identity mapping, so player/event-cluster uncertainty needs an additional frozen, identity-aligned mapping. This does not invalidate count production but limits current uncertainty claims.

## 9. Consolidated tuning experiment and implementation

### Main matrix and fixed settings

The accepted matrix is **20 distinct base books**, not 24:

- Two colors × two gate arms.
- For each combination: independently optimized 500, 1,000, and 2,000 positions; plus fixed-prescription reductions to 1,000 and 500 from that combination's 2,000 parent.
- This gives 12 independent builds and eight reductions. The two methods share the 2,000 reference.

Reduced base books must satisfy `500 subset 1000 subset 2000`, with identical moves on shared boards. Construct the 1,000 reduction first and the 500 within it. After extending, maintain the learning ladder using the **extended 2,000 master**, carrying the smaller extended book into the next rung. Count connectors and inherited prescriptions once. If a required critical continuation is absent from the master, resolve/version the master; do not silently publish a contradictory ladder. Independent arms may choose different gate-compliant moves.

Preserve base and extended artifacts. Report actual total prescriptions as well as nominal base budget: 500+800 and 1000+100 do not isolate a 500-versus-1000 learning-cost comparison.

| Main setting | Fixed value |
|---|---|
| Construction horizon | Game ply 30; retain 51-ply source for later ply-50 comparison |
| `eval_weight` | 0.75 explicitly |
| Empirical outcome prior strength | 500 where that component is active |
| Own post-book construction policy | Versioned historical 1000–1799 policy |
| Opponent policy | OTB ensemble in section 7 |
| Candidate pool | Broad BRC1800, both colors, uncut, plus shared verified engine nominees |
| Historical move cutoff | `min_games=1` on both sides |
| Model | Maia-3 79M position-only, pinned checkpoint/inference recipe |
| Independent-arm incumbent stickiness | 0 |
| Solver | White price+polish; Black DP with distinct-position matching |
| Current implementation constants | epsilon 0.0005, share floor 0.002, weak minimum games 20, grid cap 256, match inflate 1.6 |
| Lookahead flag in current manifest | **0**; do not infer the corrected lookahead is enabled |
| Engine verification | 5M / 20M; 0.01 score boundary band; no downgrade; independent final cloud checks |
| Initial extension density threshold | 0.00003 |

“Game ply 30” is not 30 additional plies after the book ends. Test the translation into the graph's boundary convention. The Rust solver is an implemented optimization method under approximations/pruning, not a proof of global repertoire optimality.

### Unlimited forcing continuations and changed default

The old canonical extension heuristic considered second-/third-best move gaps (15/25 score points), with additional old depth/reply/reach limits. David explicitly removed the **four-own-move limit** and rejected discounting tactical risk by how often historical own-color players find the move. Such players may have calculated the entire forcing sequence before entering it; entering it from memorized book does not confer that calculation.

The approved severity is `max(gap2, 0.60*gap3)` in own-side score units. A missing third legal move contributes zero; a truly forced single legal move has zero local gap but can be a necessary connector. Missing engine evidence is UNKNOWN, not zero risk. Gates remain active on extensions.

The original proposed `covered-maximum` metric could credit covering an early difficult move while overlooking an equally difficult later unbooked move. David approved changing the default on November 19 to **`remaining-maximum`**: reduce expected maximum remaining unbooked severity along a trajectory, divided by new distinct prescriptions including connectors. A connected package may need to cover both equally difficult positions before earning that reduction. This is a conservative exposure proxy, **not measured win-rate improvement**, and greedy package selection is not a global optimum certificate.

No fixed extension depth cap, minimum opponent reply frequency, or standalone 0.01% reach cutoff remains. Stop/prune only using a valid remaining-risk bound or chess termination/repetition. A low local gap cannot hide a later critical position. Beyond source ply 51, use a frozen legal model/engine fallback, report extrapolated reach and model dependence, and test stronger-defense sensitivity. Actual history prevents false repetition handling or endless revisits. A step cap checkpoints unresolved work as OPEN rather than silently imposing a chess depth cap.

Calibrate density thresholds **0.00001, 0.00003, 0.00010**, default 0.00003, on the four 2,000-position anchors. Queue the old metric at the same thresholds: **24 extension variants, reusing four base books**, not 24 additional base constructions. Use a common frozen union of explored continuations, shared engine/policy caches, separate metric/threshold checkpoints, and paired evaluation. Old covered-maximum checkpoints cannot silently resume under the new metric identity.

The current `sweep.extend` adapter performs independent **20M-node MultiPV top-three** searches, plus required-master-move checks where necessary. That is a correctness-oriented prototype, **not yet the intended inexpensive hybrid screening path**. Measure cost and add safe cache/hybrid reuse before large extension jobs. MultiPV's node budget is for the search, not 20M per alternative.

### Sensitivities and sequencing

| Family | Planned comparison | Fixed / reused controls | Additional builds if controls match |
|---|---|---|---:|
| Objective weight | 0.50, 0.75, 1.00 at independent B=1,000, both gates/colors | Primary policy, horizon 30, prior 500, weak policy and selected extension rule | 8 |
| Opponent policy | Primary ensemble vs legacy BRC1800 and BRC2200 with Maia-30, ratings 2200/2200, T=1; independent B=1,000 | Selected common weight, both gates/colors, horizon 30 | 8 |
| Construction horizon | **Plan: 30 vs 50 at independent B=2,000**, both gates/colors | Selected weight, primary policy, extension rule | 4 |

The manifest currently creates the four horizon variants at **B=1,000**, contradicting the table/approved plan. Correct this before execution and regenerate manifest IDs/dependencies; do not silently redefine the experiment. Total counts remain 20 primary + 20 sensitivity arms, but the affected identities and references differ.

First instrument whether `eval_weight` changes reachable values. It currently blends empirical and engine values in some fallback/auxiliary paths; weak/sampled stop values can override it. Do not claim it directly weights “convertibility versus objective score” everywhere. If it is effectively inactive, avoid an uninformative 12-cell experiment and propose any genuine new objective explicitly. Keep 0.75 if inconclusive.

Rescore finalists with OTB prior strengths 10 and 30 before rebuilding for that knob; rebuild only for consequential ranking changes. Keep Maia checkpoint/temperature/strength and empirical backup discounts fixed in the repertoire experiment. Tune those in separate held-out move-prediction work.

Use focused 2×2 follow-ups only for meaningful interactions (roughly 0.002 expected score with uncertainty, or a material risk/learning change). Rebuild affected 2,000 parents and regenerate ladders if a selected objective/policy changes; never attach a reduction from an incompatible parent.

### Evaluation, forgetting, and opening stability

Main evaluation uses at least **game ply 50**. If any compared extended book reaches beyond ply 40, choose the smallest even common horizon at least `max(50, deepest prescribed ply + 10)`. Evaluate base controls and all paired arms at that same horizon; finalists also get common horizon +20. A legacy ply-30 score is diagnostic. Evaluation must not truncate extension selection.

Start with **4,096 paired trajectories per book/panel**, seeds 1–4; finalists **32,768**, seeds 101–108. Reuse comparable continuation caches and counter-based common random choices. Report Monte Carlo uncertainty separately from source, population, and model misspecification. Do not infer equivalence merely from a nonsignificant difference.

Opponent panels: current Blitz movers 1600–1799 and Rapid 1800–1999; goal Blitz/Rapid 2000–2199; stronger 2200–2399 Blitz/Rapid/Classical separately; OTB 1500–1799 and 1800–1999 as main development panels, below 1500 diagnostic, and separately validated 2000–2199 OTB stress. The new under-2000 count corpus does not fulfill that last stronger panel. Own post-book policies span conservative 1000–1799, improved 1800–1999, and goal 2000–2199. Optional summaries weight OTB/Rapid/Blitz equally and development/goal halves, but preserve all component scores and own-policy conditions.

For recall, use transparent prior scenarios before trainer evidence exists: initial half-lives 2/5/10 days, learning multipliers 1.6/2.0/2.4, interval cap 365 days, 30 study presentations per color/day (15/60 for finalists), assessments at days 30/90/240 plus perfect recall, and eight schedule replicates. Include one incidental game/day split between colors and a zero-incidental-play control. Equal presentations do not mean equal minutes. Separate prior familiarity. Use reach, study opportunities, and move ambiguity where appropriate in the evaluation model; do **not** feed that recall discount into the approved engine-risk extension selection.

Track ECO code/name, explicit route/PGN, transpositions, endpoint coverage, and common-reference reach across books. At each board report eligibility, exposure, selected SAN/UCI, and provenance. Distinguish unreachable, out-of-book, gate-disqualified, missing evidence, and genuinely different prescriptions. Report conditional selection among reached/eligible arms separately from unconditional presence. Six independent primary arms per color supply headline votes; their reduced descendants are not independent supporting votes. Sensitivity families need balanced descriptive summaries, not a claim that a majority vote proves an opening objectively best.

## 10. Implemented preparation versus remaining integration

The `python/sweep/` package is usable for controlled, explicitly bound pilots. Its manifest creation and coordinator/extension entrypoints are readiness-only by default; `--execute` opts into work. A manifest state labelled “queued-waiting-for-prerequisites” is a dependency record, not an OS worker waiting to start.

### Implemented components

- **Recipes and publication contracts:** content-addressed arms and dependencies; explicit gate baselines; code/model/source/engine bindings; accepted explorer/eval/lookup receipts including all terminal corrections. Metadata existence alone is insufficient.
- **Selective opponent inference:** full legal reply vectors on demand, both key halves, source/model/rating/temperature/runtime identity, SQLite first-writer caching, timeout handling, and a frozen used-policy overlay. This avoids requiring neural predictions for every board in the multi-billion-row explorer. Dynamic and frozen-overlay parity fixtures cover both colors; true source outcomes/nonmove mass remain separate.
- **Selective Parquet access:** low-half row-group statistics prune reads, but final membership joins use both halves. Missing statistics retain the row group. A synthetic million-row benchmark showed about 4.34/5.41/19.81 ms for 1/16/128 requested keys versus 32.63/28.08/35.82 ms for a full-file join. A tiny real fixture was 4.17 versus 3.45 ms. These OS-warm microbenchmarks show the mechanism, not a guaranteed production speedup.
- **Real Maia adapter smoke:** seven legal positions produced normalized, deterministic probabilities; approximately 10.03 seconds for load/first inference and 0.047 seconds warm. This is not a bulk rollout benchmark.
- **Quality checks and coordination:** shared 5M/20M evidence, non-downgrade, unselected contenders, full-key nominations, per-epoch shared overlays, reproducible code snapshots, resumable OPEN rounds, and a separate accepted-Black-pilot prerequisite for nonpilot arms.
- **Continuation framework:** actual histories, exact automatic terminal rules, unique connector/prescription cost, connected packages, both extension metrics, unresolved-risk bounds, and checkpoints. Reduced-master holes remain explicit. The current high-node engine adapter needs cost work before scale.
- **Evaluation framework:** arbitrary common horizons, paired random choices, recall callbacks/profiles and report primitives. Choosing Maia has not yet frozen every production population/checkpoint binding.
- **Streamed pool finalization:** `pool_from_book.phase_finalize` now uses DuckDB external sort and fixed-schema Arrow batches of 65,536 rows instead of eagerly loading/sorting the entire pool in Polars. It enforces full-key auxiliary uniqueness across batches, rejects null/invalid payloads, and atomically publishes completed output while preserving an existing valid output on interruption. A million-row test at a 32 MB tracked budget spilled 94,109,696 bytes, completed in 2.406 seconds, and checked all keys/payloads. Full production export size/time remains unmeasured.

### Work not yet complete

Production pool exports and capacity measurements; complete live ensemble and weak-policy bindings; repaired-split component re-evaluation; production rollout/population integration; bounded-cost hybrid extension screening; extended-book/PGN and nested ladder publication; complete ECO route/stability reports; and a production pruning/mass-bound audit remain. Some supporting modules exist for these tasks, but an end-to-end accepted 2,000-position output is not yet evidence of completion because it has not been produced.

No retained-graph update engine, fully calibrated forgetting model, or globally optimal unlimited extension solver is claimed. Additional coding is still required; finishing the eval lookup will not automatically produce all planned books.

### Validation already recorded

The following are separate, partly overlapping suites—not additive counts of independent tests:

| Evidence | Recorded result | Scope/limitation |
|---|---|---|
| Rust builder and differential checks | 5 Rust tests; 252,000 menus, 800 reach cases, 1,000 graphs with zero recorded differential mismatches | Controlled Python/Rust parity, not full production optimality |
| Sweep preparation first suite | 25 checks: 24 passed, one opt-in Stockfish check skipped | Fixture engine/model paths explicitly distinguished |
| Later integration suite | 35 checks: 34 passed, one skipped; additional focused 22-check and 24-check passes | Overlapping selective-policy, coordinator, continuation, publication regressions |
| Native extractor regressions | 16 checks including 64/128 parity and high-half corruption | Bounded test inputs |
| Compact/headstart mapping check | All 512 fixture buckets, 192,858 rows, about 71 seconds | Fixture scope, separate from production acceptance |
| Terminal/flow regressions | 29/29 | Boundary conservation and consumer parity |
| OTB split/export checks | Ten focused tests recorded, including ancestry/split/corruption and export parity | Frozen-source correctness; not predictive-model quality |
| OTB count tests | 4 Rust and 4 focused Python tests passed | Plus full production ledger/aggregate verification and sampled independent replay |
| Full explorer acceptance | 55 stages passed | Receipt specifies exhaustive and sampled portions |
| Full OTB count acceptance | All aggregate/census checks, sample replay, and actual consumer checks passed | Exact counts accepted; population/model uncertainty remains |

Read `docs/sweep-preparation-progress.md`, `sweep-runtime-checks-2026-11-19.json`, `sweep-stream-export-check-2026-11-19.json`, and the producer receipts for precise commands and environments. This handoff task inspected existing evidence and code; it did not rerun the entire suite under the active production job.

## 11. Findings and questions requiring Claude's review

These are not an assertion that every remaining issue has been found. The first two are concrete discrepancies seen while preparing this handoff; the others separate known unfinished work from claims requiring scrutiny.

### A. Horizon sensitivity conflicts with the approved plan

[Plan line 159](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-tuning-plan-2026-11-18.md:159) says independent **B=2,000** for the 30/50 comparison. [Manifest generator around line 133](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/sweep/spec.py:133) uses `recipe(colour, gate, 1000, max_ply=50, ...)`, depending on the 1,000 anchor. The published Maia manifest contains four horizon recipes with budget 1,000; the evidence inventory reproduces them.

Before scheduling those arms, use the approved 2,000 anchor/budget, regenerate the current manifest and dependent IDs, and add a test that compares the family recipe to the approved design. No affected production books have been built, so this is still cheap to correct. Do not silently mutate historical manifests or resume mismatched output directories.

### B. Legacy Maia NPZ temporary-file reader issue remains

[convertibility_eval.py line 123](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/convertibility_eval.py:123) loads `glob("part-*.npz")`. The writer around [line 185](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/convertibility_eval.py:185) creates `f"{name}.tmp.npz"`, which also matches that glob. A stale/in-progress partial temporary ZIP can be read as a completed shard and fail loading. The earlier November 14 review raised this; the full-key work fixed the in-memory rating key but did not repair this reader/writer boundary.

Confirm whether the production rollout binding would use this legacy path or the newer SQLite cache. Fix publication outside the completed-file glob, reject temporary/incomplete artifacts, and test interruption/concurrent-reader recovery without accepting an incomplete shard. Also confirm checkpoint/model/runtime/temperature/source identity prevents incompatible disk-cache reuse. Do not dismiss the legacy problem simply because a newer construction adapter has a safer cache.

### C. Lookahead safety, historical pilot, and current defaults

Review the route-horizon and restored-menu changes in `4d7c945` as integrated, including shorter-route reopening, unknown score handling, model-only replies, high-half collisions, and receipt tampering. Confirm veto receipts bind the exact source/engine/menu/policy used and that post-selection audit actually checks all relevant prescriptions.

Explain what objective-safety claim is justified with the primary manifest's `lookahead_floor=0`. If stronger best-defense checking is required, specify whether it is a selection constraint, final audit with rebuild feedback, or an explicit sensitivity; freeze the choice before interpreting the pilot. Merely having corrected optional lookahead code does not enable it. The historical Black cp30 queue is inactive and unaccepted; do not cite it as successful production validation.

### D. Engine reference coherence and closure completeness

Attempt to reproduce 20M-to-5M overwrite through every merge/cache/seed-overlay path, including cloud-to-local transitions and MultiPV comparisons. Check mate/score bounds, parent/forced-child consistency, and evidence provenance. Verify that a previously rejected or unselected candidate can reenter after better evidence, and that changed shared evidence rebuilds all affected arms and dependent reductions.

Review whether epsilon 0.0005/share floor 0.002 or other pruning can hide a consequential candidate or invalidate an extension bound as small as 0.00001. Conservatively account for omitted mass; min_games=1 does not turn all missing replies into resolved leaves. A zero-request quality report must not merely mean the request generator failed to visit an important frontier.

### E. Ensemble semantics and repaired OTB statistics

Audit subset subtraction, zero-support behavior, legal-vector normalization, and separation of conditional replies from real ending/horizon mass. Check dynamic/frozen-overlay equivalence on real published inputs, exact full-key joins, and that no legacy Maia-30 layer is accidentally applied to the ensemble.

Use the repaired split/count receipt, not mutable numeric IDs, representative-only ancestry, provisional v1 counts, or split metadata marked counts-unready. Audit role inheritance/quarantine across future one-to-many refreshes, source-label/time-control limitations, mover rather than game-average rating selection, and exact depth aggregation. Recheck close model/pool margins and add an identity-aligned player/event map before strong cluster-generalization claims. Holdout/source leakage is a statistical issue even when all count arithmetic is exact.

### F. Extension objective and computational feasibility

Test an equally severe later move, repeated/transposed routes, connectors that cost budget without local severity, master holes, multiple branches, and unresolved opponent/model-only mass. Check that remaining-maximum bounds are valid and that bounded work remains resumable rather than declaring closure at a step cap. The proxy may be conservative or overly package-dependent; the queued old/new comparison is intended to investigate that.

Review how to replace routine full 20M top-three searches with safe cached/hybrid screening while independently verifying selected critical moves and alternatives. Measure a bounded cost pilot before attempting the 24 variants. Do not claim that removing the arbitrary four-move cap guarantees finite cheap computation in every branch.

### G. Migration/deployment and downstream compatibility

Audit compact physical-to-logical decoding, signed 128-bit keys, speed-order checks, true zero versus missing speed, ply-51 boundary flow, source-byte-bound resume, and lookup terminal corrections. Probe representative full-size consumers, not only fixture schemas. Preserve immutable live pins while reviewing.

Reconcile November 16's deployed adaptive/80 GB patches with maintained source. Review resume behavior under a legitimate partial output, metadata-only archive, stale lock, memory pass retry, disk reserve breach, source mutation, and stopped child process. Neither a generic success marker nor a successful old version's checkpoint can certify a new recipe.

The legacy [eval_arrays.py fingerprint](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/eval_arrays.py:110) still reads the old explorer's `_book.meta.json`. Do not assume older legacy acceptance jobs remain reproducible after their source explorer was deleted. New full-key lookup generations have separate provenance; preserve/archive the required old metadata or explicitly exclude those obsolete reruns. This is a legacy-path review question, not evidence that the active new pipeline is failing.

### H. Scientific interpretation and downstream publication

Confirm the fixed baseline is passed independently of any empirical outcome prior coupling; policy sensitivity should preferably change reply probabilities while holding the empirical outcome source constant. Measure whether eval_weight is active before tuning it. Distinguish engine utility, simulated score, empirical WDL, conversion difficulty, and recall cost in reports.

Check that stronger OTB/online panels and realistic own-play panels are actually available and separately frozen. Quantify Monte Carlo error, cluster/source uncertainty, and model sensitivity; do not collapse them into one narrow interval. Confirm reduced/extended nesting, common evaluation horizons, shared-randomness semantics, and opening stability denominators. A popular choice across related reductions is not several independent votes.

Finally, challenge the cost of the proposed experiment. Prefer shared pool/evidence caches and staged comparisons, but do not substitute an unmeasured runtime estimate for a feasibility test. Identify the smallest set of production checks that can meaningfully approve the first Black book.

## 12. Next work and acceptance criteria

### Order of work

1. **Let the current eval chain finish.** Recheck logs, reserves, and accepted receipts. Preserve completed source/child checkpoints and separate generations. Fix any actual failure before downstream publication; do not restart healthy work for this review.
2. **Resolve review blockers and measure pool export capacity.** Correct the horizon manifest, resolve the actual legacy rollout-cache path, and settle safety-audit semantics. Benchmark uncut shared online exports/selective batches with a disk forecast. Recipes cover BRC-all, BRC1800, BRC2200, Rapid2200, Classical1800, and weak1000–1799, each at plies 30 and 50; share underlying data and avoid unnecessary duplicate full exports.
3. **Bind accepted inputs and reproduce the policy.** Use accepted explorer/eval/lookup plus the completed OTB training counts; pin all model/runtime/engine/source identities. Recheck OTB component results and real dynamic/frozen policy behavior. Do not silently fall back to the old pool if integration is incomplete.
4. **Run one new Black stricter 2,000-position pilot.** This is the gatekeeper for the other arms, not the stale Black cp30 job. Use the intended full recipe and shared quality closure; inspect choices and safety manually with traceable PGNs.
5. **Measure and improve extension cost.** On accepted anchors, validate hybrid checks, remaining-risk bounds and master handling before broad extension calibration. Complete rollout/population bindings, recall schedules, and publication/report paths.
6. **Build the remaining independent anchors and calibrate extensions.** Compare both metrics and all three thresholds on four anchors with shared evidence. Freeze the chosen threshold/default based on paired results and learning/risk cost, retaining 0.00003 if inconclusive.
7. **Complete the main matrix and staged sensitivities.** Publish valid reduced/extended ladders; evaluate all arms under common conditions. Instrument eval_weight before its grid; then policy and corrected B=2,000 horizon comparisons, targeted interactions only when warranted.
8. **Confirm finalists and choose books deliberately.** Larger paired samples, +20 horizon, recall/time-budget sensitivity, independent engine/defense audit, coverage/learning/opening-stability reports, and a clearly designated confirmation population precede any canonical promotion.

ChessDB ingestion is a separate bounded-study decision, not a hidden dependency in steps 1–4. Allie work is deferred by user decision. Retained-graph optimization is optional performance work after the correctness path is established.

### Black pilot must demonstrate

- All bound source/code/checkpoint/engine identities are reproducible; full-key joins and canonical SAN/UCI survive actual exports.
- Exactly 2,000 distinct base prescriptions when the feasible graph supports the requested budget; any infeasibility is reported, not disguised as a complete book.
- Uncut historical candidates plus independently checked engine nominations are considered; examples formerly hidden by Other can appear without fabricated observations.
- The intended ensemble, weak-player policy, fixed baselines, and expected-score gates are actually active. Print the resolved settings, not just CLI defaults.
- Source and policy mass conserve across moves, true endings, horizon, computational pruning, and model-only replies. Check transpositions at the exact horizon and through shorter routes.
- Every consequential selected/unselected request closes under coherent evidence; stronger checks are never downgraded; independent cloud checks, veto/audit receipts, and changed-alternative rebuild feedback are complete.
- Any intended best-defense/lookahead safety contract is tested and accurately described; absence of a veto is not itself proof of perfect-play soundness.
- Interrupted/restarted runs reuse compatible work and reject changed inputs; a capped run stays OPEN.
- A real policy/evaluator smoke conserves rollout mass with history-correct endings, common horizons, and the intended player panels. Tiny fixture success alone is insufficient.
- Runtime, peak memory, disk/spill use, cache hits, new engine work, number of rounds, omitted mass, and representative opening/PGN changes are reported in an explicit **pilot acceptance report**, separate from a builder `final.json`.

The coordinator's nonpilot gate should require that report. It must not authorize the full matrix merely because one build wrote a final file.

### Planning estimates after the running chain

These are rough machine-work/implementation ranges, **not scheduled starts or promises**:

| Remaining work | Planning estimate / dependency |
|---|---|
| Shared pool materialization | Approximately 12–36 machine-hours after lookup acceptance and capacity measurement; full uncut scale unmeasured |
| Remaining integration and production pilot preparation | Roughly 2–5 working days, partly overlapping data work; depends on review findings |
| Main 20 base arms | Roughly 2–5 machine-days once prerequisites/pilot pass; closure and cache reuse must be measured |
| Sensitivities, extensions, larger confirmation | Roughly another 3–7 machine-days, staged; extension cost could dominate until improved |
| Full ChessDB import | No defensible production ETA yet; measure a bounded importer first |

Do not add all ranges mechanically: some preparation overlaps the current data job, some controls are reused, and changes may eliminate uninformative experiments. Conversely, no existing unattended coordinator will automatically perform the unfinished integration when the lookup completes.

## 13. Review navigation and safe commands

The checked Maia checkpoint is `D:/chess/maia/hf_cache/hub/models--UofTCSSLab--Maia3-79M/snapshots/a107d6ceb7b298cb04ae1da4edffe2939858b894/maia3-79m.pt`, SHA-256 `3fc6181d5db789b45a15305732148757ae74efa3e0028e81ba335b462dac45c2`, served by `D:/chess/maia/.venv/Scripts/python.exe`. The runtime evidence pins its source files and packages; production bindings must retain that identity rather than rely on a model nickname.


Primary implementation root:

```text
C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation
```

Within that root, review these groups:

| Area | Files/modules |
|---|---|
| Recipe/contract | `python/sweep/spec.py`, `prepare.py`, `publications.py`, `stages.py`; current plan and Maia manifest |
| Selective counts/policy | `python/sweep/keyed_parquet.py`, `frontier.py`, `policy.py`, `maia_policy.py`, `builder.py`, `candidates.py` |
| Shared quality closure | `python/sweep/quality.py`, `verify.py`, `coordinator.py`; full-key rare/engine verification modules |
| Continuations/learning/reports | `python/sweep/extensions.py`, `continuations.py`, `extend.py`, `ladders.py`, `rollout.py`, `recall.py`, `reports.py` |
| OTB provenance/counts | `python/sweep/audit_otb_split.py`, `freeze_otb.py`, `verify_otb_split.py`, `otb_counts.py`, `verify_otb_counts.py`, `check_otb_consumer.py`; `rust/otb_counts_replay/` |
| Data consumers | `python/explorer_storage.py`, `pool_from_book.py`, `eval_lookup128.py`, `verify_book.py`, `verify_evals.py`, full-key key/bridge modules |
| Legacy boundary checks | `python/convertibility_eval.py`, `eval_arrays.py`; historical pilot scripts/receipts |
| Migration/native source | The corresponding `keys128` worktree, `rust/explorer_extract/` and migration Python scripts; deployed pins in the evidence inventory |

Read-only Git commands from the primary implementation root:

```powershell
git status --short
git log --all --since='2026-11-14 00:00:00' --oneline
git diff --stat 3d99336..ffb11e2
git diff --stat 34ace88..ffb11e2
git show ffb11e2
git show c810d7e
git show def2b7c
```

Use the recorded usage/progress files for exact fixture test commands and environment bindings. The Python interpreter is `C:/Users/David/Documents/Chess/.venv/Scripts/python.exe`; use the intended worktree's `python` directory/module path, not the stale main checkout by accident. Bound concurrency for tests while the heavy job runs. Compile into isolated target directories, never over a running pinned executable. Do not rerun multi-terabyte acceptance jobs merely to read their results.

The companion evidence JSON hashes small receipts and deployed files and records existing bulk acceptance evidence; it did not rehash several terabytes for this handoff. Paths and process observations should be refreshed before operational action. The handoff copy in the main checkout and its committed sweep-worktree copy are intended to be byte-identical.

Please return a concise top-level verdict, prioritized reproducible findings, a list of decisions you agree/disagree with and why, and a concrete minimal validation plan for the Black pilot. State explicitly which claims are supported by the current artifacts and which still require a production run or a statistical test.
