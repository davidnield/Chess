# Repertoire project handoff to Claude: November 22–26, 2026

Prepared for David on November 26. Operational snapshot: **07:39 PST November 26**.
This consolidates changes, completed work, decisions and their reasoning since the
November 22 ownership transfer. It is a handoff and review map, not a new exhaustive
code audit. Linked reports contain detailed experiments, limitations and receipts.

## 1. Read this first

The archive, evaluation-source research, lookup repairs and mini bucket production
are finished. The corrected human-model and optimizer paths have substantial tested
implementations. **We have not produced or accepted new canonical repertoires, run
the corrected paired Black pilot, or launched the main tuning sweep.** Data completion,
engineering verification, model calibration and production acceptance are separate.

Work in:

```text
C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation
branch: codex/takeover-hardening-20261122
last implementation/evidence commit before this handoff: a46ed86
Python: C:/Users/David/Documents/Chess/.venv/Scripts/python.exe
```

The main checkout at `C:/Users/David/Documents/Chess` is a different working copy
with unrelated work. Code changes live in the maintained worktree; documentation
has been mirrored to the main checkout. Detailed-document links below point to this
maintained worktree so they resolve to the reviewed versions. Do not assume its installed Rust extension
matches the maintained worktree or a frozen production release. Do not reset either
checkout, overwrite unrelated changes, or edit code underneath an active process.

Earlier dated documents contain historical running/paused states and recommendations
subsequently superseded. This document's current-state tables and explicit decisions
below take precedence for handoff purposes. Read runtime receipts before resuming anything.

### Current state

| Work | State at snapshot | Next action |
|---|---|---|
| ChessDB menus and corrected cdb2 lookup | Complete, verified, old comparisons retained | Reuse; no import/rebuild |
| Lossless ChessDB archive and independent deep audit | Complete | Preserve archive and evidence; no conversion restart |
| Evaluation-source selection, ablations and final confirmation | Complete and reviewed | No more weight fitting on final labels |
| Native 30/30/40 score overlay | Complete, producer-verified and independently sampled | Finish pilot-ready acceptance packaging and binding |
| Mini BRC 1800 p30 bucket production | **512/512 collected**, finished 05:32 PST Nov 26 | Independent acceptance and sorted publication still needed |
| Main weak 1000–1799 p30 | **414/512 buckets**, 3,317/4,096 ranges; running | About 3 hours construction remaining at snapshot, plus publication checks |
| Main all-rating BRC p30 | Three complete pilot buckets / 24 ranges | Automatically follows weak in existing queue |
| Rapid 2200 p30, Classical 1800 p30, OTB training counts | Existing accepted exports | Recheck bindings when freezing pilot inputs |
| Corrected shared-policy/W/R/referee implementation | Opt-in engineering path implemented | Calibration, uncertainty refinement, real-data audits and acceptance |
| Rust optimizer comparison | Implemented and small-tested | Real-graph calibration and solver comparison not run |
| Black stricter paired source pilot and wider sweep | Not started | Dependencies in section 12 |

The all-rating pilot buckets took 281.3/287.4/316.7 summed range-computation seconds.
Extrapolation gives roughly **42 hours** of remaining range work after weak, before
publication/overhead; this is a three-bucket estimate, not sustained production
throughput. Do not use gaps between their receipt timestamps: the pilots were
interleaved with weak work. Adult sorted publication has a separate roughly **26-hour
serial** one-bucket extrapolation and is **not queued**. No honest end-to-end book
or full sweep ETA exists until the remaining model/input cost pilot runs.

At 07:39 PST, free space was D: 450.84 GB, E: 1,806.70 GB, H: 2,654.00 GB, all Healthy.
**F: was not mounted in Windows at that check.** David had just discussed connecting
it directly to the NAS. We have not verified where it is connected or whether a
backup has begun. This is not evidence of drive failure. The current pool jobs use
D:/E:/H:; direct archive queries need F: available. Do not recreate a missing archive.

## 2. November 22 takeover: integrity before further computation

Starting context: [Claude's handoff](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/claude-handoff-to-chatgpt-2026-11-22.md),
[our review/reproduction](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-handoff-review-for-claude-2026-11-22.md), and
[Claude's response](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/claude-response-to-chatgpt-review-2026-11-22.md).
Execution and evidence: [takeover plan](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-takeover-plan-2026-11-22.md) and
[validation findings](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-takeover-validation-2026-11-22.md).

### Fixed

- **Menu completion collided with a directory on Windows:** `_DONE` and `_done`
   are not distinct there. Added `_MENUS.COMPLETE.json` publication after checking
   all 512 position files and 512 move files against original SHA-256 receipts.
   No repeated multi-day import. Totals: 4,778,964,781 positions queried,
   2,006,286,156 found, 11,311,142,269 moves; 20 duplicate keys resolved.
- **Lookup resume reused stale output:** changed source 120 → 321 cp still resumed
   as 120, while a fresh run returned 321. New receipts bind actual source/output
   bytes, not only existence or timestamps. Same-size/same-mtime changes are tested.
- **Unsafe parallelism/coordinator guarding:** replaced shared-global metadata
   assumptions with an exclusive coordinator lock, isolated per-bucket workers,
   and verified complete publication. Do not launch independent legacy CLIs against
   the same output. Windows venv wrapper/interpreter pairs are not duplicate jobs.
- Preserved all 167 old lookup checkpoints, rebuilt a separate cdb2 generation,
   and compared every old result. **All 512 new buckets verified; all 167 comparisons
   matched.** Four-worker benchmark: four buckets in 62.5 seconds, observed maximum
   worker RSS 3.36 GB. The focused repair/acceptance tests totaled 46.
- Preserved maintained import sources and expanded evidence snapshots: the earlier
   1,336-file manifest omitted eight later OTB/entry-bias artifacts. A new 2,023-file
   snapshot checked for unlisted files. Original snapshots were not silently repaired.

Accepted baseline lookup:
`D:/chess/eval-lookup-p51-cdb2-20261122`, manifest SHA-256
`b4c90b5d923ef4a4baf2a0020ddebe31872914546be1a27207a7f4ae0777d1c6`.
Final evidence: `D:/chess/acceptance/chessdb-takeover-20261122-final-b4c90b5d923e`.

### Limitations preserved

Historical scan pin version 1 was overwritten before takeover; do not invent its
provenance. The historical engine panel recorded requested depth for older results,
not actual attained depth. SF18 remains the reference engine: Claude's November 22
`2218129` correction returned from SF19 because the tested cp scale differed by
about 1.47×. Do not mix these scales without an explicit validated conversion.

Correcting OTB ancestry did not make historical test data unused: 495,640
historical-family test games may already have influenced selection; 6,633 new-family
games are only candidates for fresh confirmation pending an exposure audit.
Freeze support-gate-300 versus ungated model comparisons before using genuinely
unexposed outcomes. Earlier shared-menu regret and gate diagnostics are not
repertoire-reach estimates or proofs against unsearched legal moves.

## 3. ChessDB: lossless archive, speed, access and source retirement

### Why a new archive

The filtered move menus/lookup cannot replace the native database: they represent
selected explorer positions and selected moves, not every original database record.
David requested an archive retaining everything, including values not useful to the
current repertoire. We tested Parquet layouts/codecs, then native streaming.

- [Lossless pilot](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-parquet-archive-pilot-2026-11-22.md): 3 M original records
   across 12 regions, original key/value round trips and multiple codecs/layouts.
- [Native benchmark](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-native-archive-benchmark-2026-11-22.md): Rust writing
   was 3.93× faster at Zstd 3 and 2.49× at Zstd 9 on the matched pilot. The specialized
   source reader remained C++/TerarkDB under WSL; claiming an all-Rust source reader
   would be inaccurate. Original bytes and both orientations' distance fields survive.
- NAS tests were only about 10–11 MB/s; F: measured 117 MB/s flushed writing and
   136 MB/s unbuffered reading/hash. David chose F: as primary archive, NAS backup later.
- Added complete range coverage, physical hashes, logical digests, immutable source/
   code/schema receipts, bounded spool, locks, reserve guards, and resumable per-shard
   export. Interruption tests retained completed shards and matched uninterrupted bytes.
- Fixed directory-enumeration races when another worker removed finished spool
   directories during scanning. Frozen releases preserved; no in-place producer edit.

[Speedup study](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-speedup-2026-11-23.md): six workers/Zstd 3 improved matched
conversion throughput **1.4696×** over four/Zstd 9, for **7.45% larger output**.
Independent four-worker deep verification improved **2.675×**, using sequential
unbuffered archive reads and bounded NVMe staging. Chosen production: six workers,
20 M-record shards, 24 GB spool cap, D: 250 GB/F: 500 GB reserves. A new v6 generation
was created because producer identity changed; old partials were not relabeled.

### Completed result

Archive: `F:/chess/chessdb-archive-20260702-v6-zstd3`.

- **63,996,865,341 records; 3,345 shards; 2,093,381,470,759 Parquet bytes.**
- Full independent audit of the completed archive passed Nov 24, taking
   26,065 seconds, about 7 h 14 min. Checks cover coverage, original bytes, decoded fields,
   physical/logical digests and receipt inventories.
- Audit: `D:/chess/chessdb-archive-independent-verification.json`.
- Archive completion-marker SHA:
   `a19025416d8f7adbb60c16ba72b6a5899907539383533c9e1ce9553ffaebc13e`.
- Keep `D:/chess/chessdb-archive.STOP`. Neither conversion nor completed deep audit
   needs restarting. The deep audit itself was not checkpoint-resumable; interrupted
   full verification restarted, unlike shard-checkpointed conversion.

Historical resume instructions: [Nov 23 archive resume](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-archive-resume-2026-11-23.md).
These are history, not instructions to rerun the finished pipeline.

### Rust archive reader made the native copies unnecessary

[Reader and retirement report](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-archive-lookup-retirement-2026-11-25.md).
Added a Rust point reader with a small receipt-bound shard index, preserving the
native key canonicalization, color/rank mirroring, side-to-move scores, both distance
fields, and empty-value versus missing distinction. Missing/corrupt files are errors.
No global re-sort or 64-billion-position index was required.

Acceptance: six Rust and two Python tests; **971 exact queries** against the pinned
C++ encoder/native answers, including missing positions, promotions and decisive
scores. Real queries touched 128 shards/207 row groups. All 971 matched again after
retirement. Timings were warm/partly cached observations, not cold full-import rates.

Frozen replacement: `D:/chess/bin/chessdb-archive-query-20261125-v1`.
Binding SHA: `1206047173e135c2b603a75d4ff576914213e82a96393e062c7b17e362b4c2cd`.
Evidence: `D:/chess/acceptance/chessdb-archive-query-20261125`.

With David's explicit approval, deleted only:

- `H:/chess/chessdb/chess-20260702/data`;
- Ubuntu `/var/lib/chessdb/chess-20260702/data`.

Each was approximately 1.188 TB. Preserved download/source metadata, tools, receipts
and Ubuntu itself; `COPY.DONE` became `COPY.RETIRED-20261125`. WSL's built-in compact
operation reduced `E:/wsl/Ubuntu/ext4.vhdx` from 1.195 TB to 6.70 GB. About **2.38 TB**
was reclaimed across H:/E:. Do not remount, recreate or restart native imports against
these retired paths. The lossless F: archive is the retained recovery authority.
Earlier [cleanup inventory](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-cleanup-inventory-2026-11-25.md) is superseded by
the retirement report for actual disposition.

## 4. Mini worker and pool speedups

David authorized the side-session-configured worker for suitable independent jobs.
It is a separate computer: Ryzen 7 8700 G, 32 GB RAM, Windows 11 Home, no NVIDIA GPU.
Use `chess-mini`, key-only SSH, pinned host verification, the existing standard-user
launcher and application checkpoints. No privilege changes or private-key copying.
Persistent jobs must go through the launcher, not SSH background children.

[Worker acceptance](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chess-mini-worker-pilot-2026-11-23.md),
[day plan/engine benchmark](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-day-plan-2026-11-23.md), and
[pool pilot](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/mini-pool-pilot-2026-11-23.md) document runtime staging and acceptance.
The SF18 matched 12-board benchmark had 2.53× throughput with four mini workers
versus one; three matched main/mini searches suggested 2.06× mini serial throughput.
These were limited workload measurements, not a universal hardware speed claim or
new reference-cache verdicts.

For pools, staged pinned user-local Python/DuckDB/Arrow dependencies. Pool aggregation
already runs in native DuckDB; Python orchestrates it. Replacing the wrapper with
Rust alone was not a justified optimization. New repertoire numerical/optimizer
code remains Rust. Only the real eval manifest was needed for min-games 1 bucket
aggregation: no below-cutoff evaluation request exists; unexpected array access
fails instead of fabricating scores.

Dedicated-mini limits increased from initial 16 GiB job/8 GB query to **24 GiB
whole-job commit / 16 GB DuckDB / four query threads / 75% CPU / six-hour attempts**.
The difference reserves room for unmanaged allocations, Arrow, OS and worker; 32 GB
physical memory does not justify a 32 GB managed-query budget. Keep 50 GiB disk reserve.

### Algorithmic improvements, with exact comparisons

[Pool speedup report](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/pool-speedup-2026-11-23.md): early release of string-bearing
edge tables, sparse ending summaries, compaction of millions of small arrival files,
and eight disjoint signed-low-half ranges for initial aggregation. Full 128-bit grouping,
flow reconciliation, negative checks and all rows/duplicate multiplicities remain.

Matched bucket 005 reference: 23,877,125 edges and 20,371,938 aux rows. All arms had
zero differences both ways and matching non-timing stats. Aggregation+compaction:
baseline 1376.89 s → best 137.267 s, **10.03×**. Real cold compaction/transport made
end-to-end cadence about 4.9 min, later 4.6 min, rather than a literal 10× end-to-end gain.
The old 10–11-day projection became roughly 40–45 h of then-remaining bucket work.
One next-bucket prefetch overlaps one compute; it never submits a second compute job.

### Transport incidents and repairs

[Transport recovery](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/pool-transport-recovery-2026-11-24.md): a PowerShell process
exited but an SSH descendant kept captured pipe handles open, defeating ordinary
timeout cleanup and causing roughly four hours idle. Added bounded process-tree
cleanup, file-backed output, explicit SSH connection/keepalive limits, submission
intent reconciliation, and truthful submitting/checkpoint statuses. A subsequent
stdin-related hang led to encoded-command delivery without SSH stdin. Transient
read-only sharing violations receive bounded retries; ambiguous Submit is never
automatically retried. Existing results/intents are reconciled first.

Current historical queue release is `D:/chess/bin/mini-pool-queue-20261124-v3`,
policy `D:/chess/bin/mini-pool-optimized-20261123-v1/policy.json`.
Pauses/resumes honored David's bedroom-noise requests. The proposed 09:30 restart
was cancelled when he requested immediate resume; no such future restart remains.
The unrelated crawler was left untouched.

### Finished raw pool, not finished publication

At **05:32:26 PST Nov 26**, all 504 assigned plus 8 original buckets completed.
The completion audit checked collected receipt hashes/identities, file lists/sizes,
stats hashes, every bucket's footer counts and no-Other contract. It did not perform
a new 833 GB payload rehash; collection-time hashes are preserved. Originals got
existence/footer/contract checks. Totals: 12,228,962,459 edges, 10,431,861,875 aux rows,
833,202,077,982 Parquet bytes. Worker is idle; do not restart its completed producer.

Outputs: `H:/chess/mini-pools/brc1800-p30/production/bNNN/work/buckets` for 504;
originals {5, 28,109,146,295,346,396,509} in
`D:/chess/sweep-inputs/brc1800-p30/work/buckets`.
See [completion evidence](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/evidence/mini-bucket-completion-20261126/verification.json)
and [full-pool report](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/full-policy-pools-2026-11-25.md).

## 5. Evaluation-source research and frozen decision

The user requested a wider source/ensemble comparison, then pooling of old and new
selection evidence, while keeping final confirmation separate. This superseded the
earlier restriction to two policies for research, not the production acceptance gate.

### Samples, exposure and depth

| Evidence | Size and role |
|---|---|
| Original Nov 13 overlap experiment | 200 positions, SF18 requested depth 22; not counted again |
| Nov 22–23 panel | 1,500 parents plus 4,030 child searches; 408 NNUE triple overlaps; exposed selection evidence |
| Nov 25 targeted deeper diagnostic | 452 old parents+490 children=942 searches; 939 qualify; 451 parent upgrades; not 942 new boards |
| Nov 25 new study | 1,000 new parents; 999 qualify; 600 NNUE triple overlaps |
| Pooled selection | 2,499 qualified distinct parents; 1,008 NNUE triple overlaps |
| Final confirmation | 300 separate triple-overlap boards; 299 qualify, 297 actual depth≥28; never pooled into fitting |

One best qualified reference per board; no downgrade from an unqualified recheck.
1,049 older rows retain unknown actual depth. Targeted disagreements are stress
tests, not unbiased prevalence samples. Related openings and approximate engine
references limit bootstrap claims.

Tested 135 fixed recipes: individual sources, nonnegative tenth-grid mixtures in
cp and expected-score space, equal thirds, median and prior policies. Primary loss:
macro mean absolute expected-score error across fixed source-quality strata, not
book reach or measured human win probability. Selected 30/30/40 on pooled selection
and locked it before opening final labels. Equal thirds was close; exact weights
are a practical frozen choice, not a discovered mathematical optimum.

Final confirmation (errors in **percentage points of expected score**):

| Rule | MAE | p95 | p99 |
|---|---:|---:|---:|
|30/30/40 score blend|0.95370|2.63330|3.79927|
|Nov 23 research proposal|1.03799|3.10574|4.98138|
|ChessDB alone|1.20019|3.48802|4.21676|

MAE improvement 8.1% vs proposal/20.5% vs ChessDB; simultaneous paired intervals
excluded zero for both. This is not an 8.1% playing-strength/win-rate improvement.
**Stricter White safety still regressed:** clear false admissions 5 versus 4 against
proposal, total 7 versus 5, unsafe shortfall 4.8243 versus 2.7164 points. Mean accuracy
does not prove gate safety. A separate 45-board selection-only White review upgraded
14 references; all 45 had actual depth≥28. It tested production arithmetic without
retuning on final labels and did not remove the general safety limitation.

Fishnet ablation: pooled triple-overlap MAE 0.94053 vs 1.05258 for 50/50 Cloud–ChessDB;
simulated ChessDB absence also favored Cloud–Fishnet over Cloud. But on **naturally
Cloud-missing** boards, mixing Fishnet worsened MAE 1.16188 vs ChessDB 0.99173. Simulated
missingness and natural missingness are different populations. Do not universally
renormalize weights or discard Fishnet everywhere. Natural ChessDB-missing behavior
still lacks independent validation; first measure its relevance before more searches.

### Approved for integration, activation pending acceptance

1. With Cloud+eligible NNUE Fishnet+ChessDB, blend White expected scores 30/30/40.
2. Without Cloud but with ChessDB, use ChessDB alone.
3. Other source combinations retain the **bound accepted cdb2 baseline** behavior.
   That baseline is not the research comparator named `proposal`.
4. Keep authoritative terminals and qualified local SF 18 overrides first; never
   downgrade an earlier stronger search. Missing evidence remains missing.
5. Preserve normalization/mates/±2,000 cp handling and the Lichess conversion;
   do not tune source scales, gates or baselines to improve these research results.
6. No additional weight search; paired book-level acceptance still required.

Detailed analysis trail:
[targeted diagnostic](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-next-steps-2026-11-25.md),
[study design](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-ensemble-study-2026-11-25.md),
[separate confirmation design](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-confirmation-plan-2026-11-25.md),
[pooling amendment](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-pooling-plan-2026-11-25.md),
[locked selection rationale](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-ensemble-selection-rationale-2026-11-25.md),
[final confirmation](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-ensemble-confirmation-results-2026-11-25.md),
[Fishnet ablation](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/chessdb-fishnet-ablation-2026-11-25.md), and
[frozen decision](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/eval-source-production-decision-2026-11-25.json).

## 6. Native score-overlay implementation

[Integration plan](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/eval-production-integration-plan-2026-11-25.md) and
[implementation/results](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/eval-production-integration-results-2026-11-25.md).

Rust `eval_policy.rs` computes source policy; Python handles bounded native
DuckDB/Arrow I/O. Added sparse float64 equivalent-cp overlay, full 128-bit joins,
base-generation binding, source/code/runtime/output receipts, corruption-safe resume,
complete-publication requirements and per-bucket reserve checks. Fixed integer
casts in related pool/extension paths and taught the independent audit reader to
compose the same overlay while retaining its own key lookup implementation.

**Completed:** `D:/chess/eval-lookup-p51-score334-20261125-v1`:
512 buckets, 1,998,220,987 sparse rows, 49,955,622,979 bytes, 9,727 seconds.
Manifest SHA:`15be7323a4af4e8360bf89a6f06652fcbbaf52764c74a9685a6a6774e2ee2f0c`.
Independent 16,893-position raw-source comparisons across 0/255/511 had max score
error 3.33e-16. This is sampled semantic verification, alongside full producer
receipts—not an independent arithmetic census of two billion rows.

Do not rematerialize it or overwrite old lookup/count metadata. Explicit candidate
bindings are implemented; final pilot-ready acceptance evidence remains to be closed.
Parent and child consumers must use the same declared provider, with local override
precedence. Microbenchmark lookup overhead is not an end-to-end builder slowdown estimate.

## 7. Human-model review: what was wrong and what changed

The Nov 25 discussion exposed inconsistencies across graph replies, stop values and
rollouts. David authorized the consolidated recommendations. Read the
[review and staged plan](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-model-review-and-implementation-plan-2026-11-25.md)
and [implementation record](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-model-implementation-2026-11-25.md).

### Intended opponent policy, unchanged calibration

Keep position-only Maia 3 79 M; opponent inputs 2600/2600, temperature 1.35,
empirical strengths 30/30, Maia strength 3, OTB strength 10, effective-support gate 300.
These ratings are model calibration inputs, not USCF conversions. Let b/a/r/o be
all-rating BRC/adult 1800+ BRC/(Rapid 2200+ plus Classical 1800+)/eligible deduplicated
under 2000 OTB training counts. Capitals are totals, L legal-move count:

```text
broad   = (b + 1/L) / (B + 1)
backup  = (a-r + 30*broad) / (A-R + 30)
online  = (r + 30*backup) / (R + 30)
support = R + .10*(A-R) + .01*(B-A)
pre_otb = online                             if support >= 300
          (support*online + 3*Maia)/(support+3) otherwise
reply   = (o + 10*pre_otb) / (O + 10)
```

One targeted game adds one observation; thirty pseudo-observations follow the
backup distribution, not that one move. OTB has weight O/(O+10), so this is not
mostly OTB everywhere. These are nested smoothing populations, not independent
samples. Backwards induction propagates values, not pseudo-games from leaves.
Do not apply another legacy Maia 30 overlay to this completed distribution.

### Reproduced problems and disposition

| Problem and impact | Implemented repair / remaining qualification |
|---|---|
|At Nweak<20 legacy code discarded all weak evidence and used raw adult counts even if fewer|New native weak+Maia blending retains observations; own calibration remains pending|
|Graph used ensemble, W/R used raw adult replies; provider closed after graph collection|Shared live batched provider across graph/W/R/native referee; off-graph support, freeze consumed union|
|Dropped sub 1% replies and missing evaluations, then renormalized|Keep legal probability mass; unknown/unexpanded utility carries propagated [0, 1] bounds|
|No-eval rollout could report equality; ancestor fallback hid uncertainty|Explicit unresolved state/bounds and exact board terminals in corrected path|
|Construction cap, count cap and actual ending/truncation semantics conflated|Separate caps; opt-in conditional-board contract; source truncation is not a move or terminal result|
|Pool metadata implicitly fixed eval source; paired arms rejected different stored CP|Explicit eval binding and per-arm score references; true counts/children must still match; shared verification union|
|Fractional overlay CP truncated on native fallback; inconsistent±1,000/±2,000 clipping|Float64 through rollout; explicit common source-normalized utility, legacy utility retained as control|
|W/R overwrites values and can make eval-weight mostly irrelevant|Effective-objective/value-origin audit still needed; do not run a meaningless weight sweep|
|Named recipes/readiness booleans did not prove consumer use|Stronger content-bound `pilot-acceptance-v2` evidence requirements|

Rust owns shared policy arithmetic, legal replay, transposition/depth handling and
W(two-ply)/R(three-ply) induction. Python supplies batched counts, neural inference
and lookup I/O. Native rollouts retain full actual rollout history/re-entry while
Maia inputs remain position-only. Endpoint histories start at that endpoint and must
not be described as full game histories.

**The corrected path is opt-in via `--continuation-config`.** Legacy commands remain
historically reproducible. This prevents an uncalibrated policy from silently becoming
production. The current scalar solve refuses unresolved W/R widths above numerical
roundoff; **global reach-weighted uncertainty budgeting and selective value refinement
are not implemented**. Exhaustive all-legal expansion at millions of boards is not
the intended final solution. Bounds concern missing computation/evidence, not a
statistical confidence interval for actual human performance.

Own-policy proposed grid: K{10, 30,100}, own Maia rating{1200, 1450, 1700}, opponent input
{1800, 2200}, temperature{1.0, 1.35}: 36 transforms using six raw inference sets.
No winning calibration selected. Validate mover-level 1000–1799 by speed/support,
document mismatch with game-mean construction bands, and reserve unexposed confirmation.
Keep weak raw/model-only/legacy-threshold controls.

Still required: conditional-board ending/clock effects, route-mixed horizon census,
long-vs-short continuation and book re-entry effects, value-origin/reach attribution,
recall integration and fixed multi-book referee/calibration receipts. Sharing policies
does not make short book-independent W/R equal a long book-dependent rollout.

## 8. Complete policy pools and reusable inference

David explicitly chose **complete pools instead of selective explorer count lookup**.
Do not revive selective count materialization. This does not forbid selective Maia
inference or the still-required selective continuation-value refinement.

| Population | Role and actual status |
|---|---|
|BRC 1800+, min 1, p30|Candidate graph/outcomes/adult opponent layer; mini raw buckets finished; publication pending|
|Rapid 2200+, p30|Targeted opponent layer; 170,549,231 move rows, existing accepted export|
|Classical 1800+, p30|Targeted opponent layer; 297,201,156 move rows, existing accepted export|
|All-rating BRC, p30|Broad empirical prior; compact full-count build queued after weak|
|BRC 1000–1799, p30|Our post-book counts; compact full-count build running|
|OTB training p30|Only training-role deduplicated counts in construction; existing corrected publication|

Lichess source period is Jan 2013–Jul 2026, game-mean rating filters, source explorer
retains 51 plies. P30 count exports do not supply empirical plies 31–33/50 by magic;
new-board tails beyond source coverage must be explicitly model-only. Full p50 and
BRC 2200 control pools are deferred. Do not substitute 1800+ for all-rating or weak.

[Full policy pool report](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/full-policy-pools-2026-11-25.md): policy-only tables retain
every observed move/count, full keys and lossless child XOR, but omit candidate EPD,
ending and eval sidecars. They are not drop-in candidate pools. 512 parent buckets × 8
signed-low ranges, sorted for pruning; complete hash-checked publication only.
Native DuckDB aggregation with thin Python orchestration. 20 regressions passed;
real reference parity 2,986,084 rows, zero differences both ways.

Weak → E:, broad → H: after capacity pilot; nine already-built ranges copied/verified
with relocation provenance, originals retained. Active `config-v2.json` only;
old `config.json` remains stopped. Both capacity pilots passed with 25%output margin.

[Partitioned publication/cache milestone](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/pool-publication-and-prediction-cache-2026-11-25.md):

- Raw adult buckets are unsorted. Added independent per-bucket edge/aux sort,
   resumable paired receipts and full 128-bit native validators. Avoid a global sort,
   but retain originals and budget a new sorted copy.
- Publication requires all 512 pairs plus independent `term_vs_book`, `reimpl`,
   `conservation`, `evals` evidence bound to exact output inventory. Structural checks
   alone do not certify source semantics. Do not manufacture true acceptance booleans.
- Bounded bucket 005: 184 s, 1.792 GB final versus 1.630 GB raw. About 26 h serial/855 GiB extra
   extrapolated, not full-run measurements. Recheck combined capacity before launch.
- Partitioned readers and builder accept paired manifests, prune row groups and
   join both halves. First bucket use hashes full files; repeated reader instances
   can repeat that cost. Full-input amortization/cold-warm benchmark remains necessary.
- Added transactional raw Maia cache before temperature/shrinkage, keyed by model
   bytes/runtime/preprocessing/FEN/full key/legal moves/ratings/inference settings.
   Downstream pool/gate/budget/temp/K changes reuse raw inference when compatible.
   Derived caches keep broader identities. Never implicitly import low-half caches.
- 36 Python+30 Rust+3 optimizer-interface checks and randomized differential checks
   passed. Real 64-request warm transforms reused predictions without inference.

Frozen engineering release:`D:/chess/bin/pool-input-readiness-20261125-v1`;
release SHA:`d96ed4c324398e9aa6b08e3f554637d709f03f3874bb3b8d52a2abe08befe131`.
It is not the running producer. No canonical activation or full finalization launch.

## 9. Rust optimizer work and retained tuning plan

David asked whether existing DP/price were the only sensible methods, approved the
staged alternative comparison, and required **all new optimization/oracle/model/
benchmark logic in Rust**. Implemented in [rust/book_builder/src/optimizer](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/rust/book_builder/src/optimizer),
standalone `optimizer-bench`, with optional pinned pure Rust `microlp0.8.0`.
Python's new path only passes the prepared native graph to the study.

Implemented controls and alternatives:

- Existing DP+distinct-position matching+polish and price+polish.
- DP package-cap diagnostics 256/512/1024; requested budgets preserved, separate 1.5 × probe.
- Deduplicated multiple starts retaining the best feasible polished book.
- Joint bounded neighborhood search, including connecting prefixes and alternatives
   outside the incumbent. Abandoned branches release budget; transposed decisions
   cost once. Accept only after whole-book budget/objective reevaluation.
- Independent exhaustive small-DAG oracle, exact uncompressed integer tree DP
   (rejects shared descendants/cycles), and bounded mixed-integer references.
- Full-key/SAN bounded-cone export with frozen boundary/parent identities, durable
   cell/neighborhood checkpoints, code/recipe drift rejection, and resource reporting.

Why: compressed DP is an approximation; transpositions share costs; greedy switches
may miss delayed benefits. Neither package-cap increases nor small exact solves prove
global optimality on the production DAG. Cycles retain the finite production cut;
local cones certify only their frozen boundary. The old price diagnostic is not a
certified global bound. MIP has no internal hard process-memory limit.

Tests: 22 native tests with optimizer feature and 22 without Python; 72 oracle/MIP
comparisons; actual cap-binding 320-branch fixture; adversarial transpositions,
fixed/unreachable choices, delayed alternatives, resume/corruption; legacy native
differential parity; 3 interface tests; 54 small standalone comparison cells.
These are engineering tests, not a solver winner or production speedup.

Next O1: representative real graph calibration, held-out cones and equal-resource
measurements. O2: both colors × both gate arms at B=2000, then independent B=500 shortlist;
B 1000 only if interactions require it. O3: paired development rollouts, full cost
accounting (portfolio includes cost of starts), freeze solver, then main sweep.
Study JSON is **unbound**, not a queue. Existing v5 manifest remains immutable.

References:[optimizer implementation](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/optimizer-implementation-2026-11-25.md),
[native guide](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/rust/book_builder/OPTIMIZERS.md),
[unbound study](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/optimizer-study-2026-11-25-v1.json),
[verification](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/optimizer-verification-2026-11-25.json),
[controlling tuning plan](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/repertoire-tuning-plan-2026-11-18.md),
[sweep progress](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/sweep-preparation-progress.md).

### Carry-forward constants, not newly changed this week

| Arm | White absolute tolerance | Black absolute tolerance | Relative score loss |
|---|---:|---:|---:|
|Stricter practical|0.050|0.050|0.060|
|Permissive|0.110|0.075|0.095|

Fixed White-score baselines: White experiments 0.5183137990703003; Black experiments
0.5195669627673518. White child score≥baseline−tolerance; Black≤baseline+tolerance.
Relative gates use our perspective versus best trusted reference. Freeze baselines
across sources/pools; the older lifetime-plan instruction to recompute them is superseded.
The current first pilot is **Black stricter score gates**, not historical 30 cp.

Primary matrix: 20 distinct base books total: both colors × both gates × {independent 500,
independent 1000, 2000 parent, fixed-parent reduced 1000, nested reduced 500}. Budget counts
distinct own prescriptions, not path occurrences. Reductions must preserve choices
and nesting, including extensions from the extended 2000 master. Track selection
frequency across variants, conditional on reach/eligibility; unavailable openings
are not votes against them. Shared book content may reuse compatible evaluations.

Extensions: default `remaining-maximum`, unlimited depth, no minimum reply-frequency
cutoff; reach × potential engine loss, with connectors charged. Severity
`max(gap2,0.60*gap3)`; own historical ease must not discount engine severity.
Thresholds {0.00001, 0.00003, 0.00010}, initial 0.00003; compare `covered-maximum` and
separate `engine-or-player` controls. A later equally difficult move remains exposed
after covering an earlier one. It is a risk proxy, not measured win-rate lift.

Retain historical eval-weight 0.75/prior 500 only for reproduction where active.
Planned 0.50/0.75/1.00 sensitivity requires the effective-objective audit first.
Later opponent controls: raw BRC 1800+/2200+ with legacy Maia 30; OTB prior 10/30 rescoring
before rebuilding. Later construction 30 vs 50 ply; shared rollout horizon at least 50
and at least 10 plies beyond deepest relevant prescription, plus finalist+20 sensitivity.
Evaluate current/goal online and OTB panels, strong-opponent stress and improved-own
play separately; do not treat online bands as USCF conversions. Retain paired seeds,
4,096 trajectories per primary seed × four seeds, finalist escalation and recall sensitivities
from the plan. Recall remains uncalibrated scenario analysis until personal evidence.

## 10. F: health, metadata ownership, cleanup and backup decisions

[F: investigation](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/f-drive-investigation-2026-11-25.md) and
[data/rebuild-cost inventory](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/f-drive-data-inventory-2026-11-25.md).

Windows initially reported ex FAT Warning/Full Repair Needed while disk hardware
reported Healthy. After read-only diagnostics and David's explicit UAC/mark-clean
approval, `chkdsk F: /markclean` finished Nov 25 19:18, exit 0, no filesystem problems.
Independent status was NotDirty/Healthy; post-operation checks matched 3,345 metadata/
receipt hashes, eight full shards/4.865 GB and 971 fixed queries. This was not a new
full surface scan. Do not repeat the old warning as current or auto-clear a future
dirty flag. F:'s current absence from Windows is separate from that resolved event.

### Source metadata now belongs with its source

David correctly challenged the production launcher's dependency on a legacy
dataset's provenance directory. [Implemented migration](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/source-inventory-migration-2026-11-25.md):

- Source-owned package at
   `D:/data/chess/standard-chess-games-compressed/_source_metadata/inventory-v1.json`;
   identical bound copies in the current F: extraction archive and D: extraction stage.
- Preserves full independent historical evidence; checks current lists/footer rows/
   sizes/mtimes. Does not silently regenerate expectations from possibly incomplete
   source files or claim full source-byte hashes.
- 163 months, 4,179 source partitions, 7,153,458,738 games across six speeds.
   Inventory SHA:`710d06201b676ad725cf81289fa49191eb25c5643322e5087c6e033e68efc2b5`.
- 659 existing metadata files unchanged; 11 tests passed including missing speeds,
   corruption, interrupted copy and resume without old provenance. Each output keeps
   its own build receipts; input identity and build provenance remain distinct.
- New standalone launcher:`D:/chess/bin/source-inventory-launcher-20261125-v1`.
   Existing resumes still use the original extractor path/binary; settings bind both.
   Historical frozen launchers were not edited. New generations require new packages.

### Authorized cleanup completed

Native raw copies/VHD retirement is in section 3. On Nov 25 David additionally asked
to delete the legacy monthly 1800+partials, superseded ChessDB tests/benchmarks, and
empty F:'s Recycle Bin. Done: 326 legacy partial files/eightbenchmark-test folders/
recycled contents. Preserved 196 small receipts/manifests, 2.08 MB, with verified copies.
Freed **807.56 GB**, leaving 1.819 TB free on F: at completion. Recursive removal's
empty-directory errors were resolved with files-first/deepest-directory removal;
all requested targets were verified absent. This did not delete archive v1/v2 small
roots, current v6, either monthly archive, accepted pools or sources.
Receipts:[cleanup evidence](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/evidence/f-drive-cleanup-20261125/after.json).

Important retained F: holdings:

| Dataset | Size | Value/recovery planning |
|---|---:|---|
|ChessDB v6|2.09 TB|Only full retained snapshot found; raw sources retired. Reacquire source then roughly 1–2 days conversion/audit; backup priority 1|
|`explorer_brc_p51_pilot`|8.03 TB|Actually full production 163-month 128-bit 51 ply BRC archive. Re-extraction+copy+readback measured 61.36 h; plan 60–72 h|
|`explorer_banded/rust_p30k`|3.99 TB|Old 64-bit 30 ply six-speed archive; dashboard consumers and Bullet/UltraBullet/Correspondence remain. Not wholly redundant; ~36–48 h rebuild allowance|

The E: all-time merged book is not a lossless backup of monthly archives. Preserve
small manifests/receipts/producer bindings immediately, ChessDB first bulk backup,
current monthly archive second. Do not infer old six-speed retirement permission
from removing the provenance dependency. The F: README's old archives-only description
is stale; concrete reader dependencies matter.

David asked about plugging the USB HDD directly into the Synology DS423+.
We verified that its USB ports/filesystem support permit NAS-local copying; this
could avoid the measured 10–11 MB/s network bottleneck.100–150 MB/s were illustrative,
not measured NAS rates. Recommended copy via DSM File Station/USB Copy, then
checksum verification; no formatting, automated move or deletion was authorized.
At handoff, F: is not mounted; no NAS backup has been verified by this session.

## 11. Active operations and safe resumption

Main coordinator:

```text
config: D:/chess/full-policy-pools-20261125/config-v2.json
SHA256: 5962ddd95e67e47e1e2d4ade355225a62b87155e6cd80cb361cf91236aa52b7e
release: D:/chess/bin/full-policy-pools-20261125-v1
status/logs: status-v2.json, stdout-v2.log, stderr-v2.log beside config
capacity/placement: capacity.json, placement-v2.json
active stop: D:/chess/full-policy-pools-20261125/queue-v2.STOP
old stop: D:/chess/full-policy-pools-20261125/queue.STOP (KEEP)
```

One bucket child, four native threads, 32 GB query budget, 64 GiB max spill;
D: 250 GiB/E: 250 GiB/H: 500 GiB reserves. **No F: pool spill.** Each completed range is
receipted/reusable; graceful stop finishes active range. Publication requires
all 512 × 8 ranges and checked hashes, not just process exit. Partial pools unusable.

Current command, for reference only—not a request to start another instance:

```powershell
& 'C:\Users\David\Documents\Chess\.venv\Scripts\python.exe' -B -u -m sweep.count_pool --config 'D:\chess\full-policy-pools-20261125\config-v2.json'
```

Run only from frozen release Python code/environment after verifying no existing
coordinator and honoring any later user stop. Last known wrapper 43804 / interpreter 26804;
inspect process ancestry instead of trusting saved PIDs. Never resume old config
alongside v2, bypass capacity holds, or kill unrelated Python processes/crawler.

Mini completion status/receipts: `D:/chess/mini-pool-20261123/queue/`.
Last logs:`queue-resume-20261125-092510.stdout.log/.stderr.log` in parent directory.
User launcher:`C:/Users/David/Downloads/chess-mini-launcher-v1/Invoke-ChessMini.ps1`.
Use Health/Status/Logs, not denied remote admin CIM or installer reruns. Future mini
work requires separately staged pinned inputs/binaries and launcher manifests.

Hourly monitor:`track-chessdb-and-mini-pool-progress`, ACTIVE, current chat.
Latest instruction acknowledges mini completion; continue tracking main weak/broad
through publication review. Do not repeat finished research/audit/mini completion
notifications. Keep later pause requests authoritative. No scheduled 09:30 restart.
Update/delete monitoring when remaining work finishes, rather than leaving stale
restart instructions. The monitor is not permission to launch the sweep or final sort.

## 12. What Claude should review and do next

Review the code and decisions; preserve active work. Recommended order:

1. **Verify current runtime state** from named status/receipts. Main continues weak
   then broad. Do not restart completed mini/ChessDB jobs. Confirm archive availability
   before direct queries; do not assume a NAS copy is complete.
2. **Close adult publication planning and acceptance.** Bind 504 collected+8 original
   sources explicitly. Recheck complete-input inventories, independent gate evidence,
   original-bucket verification and E:/H: capacity with broad output still pending.
   Use partitioned sorting, not a resurrected unconstrained global sort. Benchmark
   amortized first-use hashing/input costs. Do not mint acceptance from metadata alone.
3. **Finish lookup source acceptance packaging.** Bind completed score334 and accepted
   cdb2 control, local SF 18 precedence, White review and complete materialization evidence.
   No further blend fitting on the final 300.
4. **Lock and calibrate own policy.** Six raw neural prediction sets/36 transforms;
   preserve exposure roles and distinguish game-mean counts from mover-level validation.
   Gate 300 opponent confirmation still needs fresh-cohort exposure checks.
5. **Implement global uncertainty/selective value refinement in Rust.** Persistent
   content-bound completed work, full probability accounting, root-weighted stopping
   criteria; do not expand every legal tail indiscriminately or substitute equality.
6. **Finish empirical contracts and native referee receipts:** ending/truncation,
   horizon/repetition/transpositions, recall, book re-entry, objective-origin/sensitivity,
   fixed source/utility/paired RNG. New conditional-board utility needs ranking evidence.
7. **Bounded real-input cost pilot**, then freeze accepted corrected recipes. Keep
   independent verification of all final prescriptions and no-quality-downgrade logic.
8. **Paired Black stricter B=2000/p30 pilot**, cdb2 versus score334, identical counts,
   human models, gates/baselines, optimizer and common referee. Union required engine
   comparisons and rebuild both against shared qualified evidence. Review menus,
   budget, mass, gates, horizon, transpositions, selected moves, reach, verification
   closure, cold/warm runtime/RAM/storage and actual Maia usage.
9. **Rust optimizer O1/O2/O3**, then freeze solver and new versioned executable sweep
   manifest. Only afterward complete primary 20 arms/extensions/sensitivities. Preserve
   v5 and old results as controls rather than relabeling them.

Questions worth independent scrutiny:

- Are shared policy/value bindings actually consumed at every claimed boundary,
   particularly extension reach and the native rollout adapters?
- Is the proposed global approximation budget sufficient for ranking/gate claims,
   and does selective refinement have a practical cost on real sparse tails?
- Does the conditional-board objective improve OTB relevance without concealing
   resignation/flag dependence or route-dependent source censoring?
- Are original 8 adult buckets and 504 new buckets semantically interchangeable under
   publication despite different producer identities, with independent evidence?
- Can repeated whole-bucket hashing be amortized safely without weakening content
   identity? Is sorted-copy storage/reserve accounting correct alongside broad output?
- Is the optimizer formulation/whole-book acceptance sound for shared-cost
   transpositions and abandoned fixed branches? Do exact local certificates state
   their boundary/cycle assumptions clearly?
- Does the stricter-White false-admission regression demand additional targeted
   verification before any White activation? Do not hide it because first pilot is Black.

## 13. Code and evidence review map

| Area | Main implementation entrypoints | Milestones |
|---|---|---|
|Takeover/publication/resume|[python/chessdb_publication.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/chessdb_publication.py), `chessdb_lookup_run.py`, `chessdb_integrity.py`, `chessdb_complete.py`|`eb05705`, `fd3867f`, `61ab2e5`|
|Lossless archive|`python/chessdb_archive*.py`, `python/chessdb_import/cpp/`, Rust archive crate|`6c9843e`, `1cb15dc`, `402b440`, `18a9b5f`|
|Pool query/transport|[python/pool_from_book.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/pool_from_book.py), `mini_worker_transport.py`|`8b3edfa`, `f182abf`, `594e993`, `c3f1353`, `eedb739`|
|Native optimizers|[rust/book_builder/src/optimizer](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/rust/book_builder/src/optimizer), `src/bin/optimizer_bench.rs`, thin builder opt-in|`416732f`, `f62a3b3`|
|Engine studies|[python/chessdb_policy_validation.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/chessdb_policy_validation.py), `chessdb_ensemble_{study,confirmation,pooling}.py`|`a26917c`, `efdcfbb`, `f8b10e4`, `66f8a9a`, `7c9c5dd`, `66b677b`, `b6c0f34`|
|Score overlay|[rust/book_builder/src/eval_policy.rs](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/rust/book_builder/src/eval_policy.rs), `python/eval_policy_overlay*.py`, `eval_lookup128.py`|`d6dab65`, `aaa34c1`|
|Shared model/contracts|[rust/book_builder/src/human_policy.rs](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/rust/book_builder/src/human_policy.rs), `py_continuation.rs`, `rollout.rs`; `python/eval_binding.py`, `sweep/model_contract.py`, `continuation.py`, `human_providers.py`, `referee.py`, `verify.py`|`8f9426c`, `4caa71f`|
|Complete counts|[python/sweep/count_pool.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/sweep/count_pool.py), `count_pool_reader.py`, `keyed_parquet.py`|`2851d78`|
|Archive lookup|Rust archive query implementation; [python/chessdb_import/archive_lookup.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/chessdb_import/archive_lookup.py), `archive_query_validation.py`|`ca36446`, `0486ee1`|
|Partitioned publication/cache|`python/sweep/partitioned_pool*.py`, `prediction_cache.py`, `input_probe.py`, Rust input validation|`cf10682`, `cd755f4`|
|F: diagnostics/cleanup|Dated reports/evidence; exact-target one-use scripts|`92e575e`, `b02b736`, `2595494`, `94da645`, `3f17670`|
|Source inventory ownership|[python/source_inventory.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/source_inventory.py), `month_archive.py`, extraction wrappers|`a710ecd`|
|Mini completion audit|`docs/evidence/mini-bucket-completion-20261126/`|`a46ed86`|

Saved handoff snapshot: [runtime counts](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/evidence/claude-handoff-20261126/status.json).
The [dated commit log](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/evidence/claude-handoff-20261126/commits.txt) and
[changed-file list](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/evidence/claude-handoff-20261126/changed-files.txt) give a navigation index
for the period, including documentation-only milestones. Tests reported above were
run at those milestones; this handoff does not claim a fresh full-suite rerun against
every subsystem. Frozen releases may intentionally differ from current worktree HEAD.

No new jobs, model choices, data deletions or canonical switches are authorized
merely by receiving this document. David asked Claude to review the work and provide
feedback; preserve the active queue and its receipts while doing so.
