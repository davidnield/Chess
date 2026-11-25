# Separate ensemble confirmation panel — November 25, 2026

David approved an additional 300 independent positions with all three evaluation
sources. This is a final confirmation panel, separate from the 1,000-position
ensemble-selection holdout. The approved configuration is:

| Cloud depth | NNUE Fishnet observations | Positions |
| --- | --- | ---: |
| >=22 | One | 75 |
| >=22 | Repeated | 75 |
| <22 | One | 75 |
| <22 | Repeated | 75 |

All 300 require scored Cloud, NNUE Fishnet and ChessDB. Together with the preceding
study, this makes 1,000 fresh triple-overlap positions, including 900 modern-Fishnet
overlaps. The panels retain different roles; do not pool selection and confirmation
results to claim an independent final comparison.

## Selection and reference work

Select eight new hash buckets, disjoint from the old 1,500-parent panel and the
1,000-position study. Bucket-selection seed 202611252; reuse the tested sampler's
within-bucket thinning/ranking seed 202611251. Both are explicit in the definition.
Keep the previous >=2-observation / earliest-ply <=50 scope, full 128-bit joins,
source receipts, valid nonterminal FEN checks, and deterministic per-stratum rank.

Exclude every preceding study position as well as all older known examples already
listed in its frozen exclusions. Canonicalize irrelevant EP spelling and exclude
color mirrors. Reject duplicates and quota shortfalls rather than quietly replacing
the sampling recipe. This excludes exact-board leakage; it does not guarantee
different opening families or statistically independent engine/source errors.

Reference settings: pinned SF18, target depth 28, minimum qualified depth 26,
one thread and Hash 128 MiB per process, at most four processes, 60-second search
caps. Preserve actual depth/nodes/time and report a depth-28-only sensitivity.
Unqualified results remain unresolved. No canonical books, eval bindings, gates,
source reducers or source data are changed by this work.

The job samples now and waits for the entire preceding ensemble study to finish
before launching engines, so the three reference batches do not overlap. Estimated
incremental search cost: **30–60 minutes**, potentially around **80 minutes** if
nearly all searches hit the cap. The original jobs remain resumable and unchanged.

## Blinding and contender lock

The worker writes per-position engine outputs under **`sealed/results/`** and
publishes operational counts only. **Do not inspect those files or build a ranking
from them before locking the final contender.** This is a workflow blind, not
encrypted storage. Hashing/verifying records inside the worker does not expose them
to ensemble selection.

After the 1,000-position study finishes:

1. Review only its reports and the earlier targeted stress test. Assess paired
   error/uncertainty, tails and gate mistakes; select one final contender from the
   already frozen candidate suite. Write the rationale before opening confirmation
   scores. A small mean improvement is insufficient by itself; retaining the
   existing proposal is an allowed conclusion.
2. Run `lock-choice`. It requires completion of the preceding study, hashes its
   evidence, copies the selected recipe, and identifies the best individual source
   by the same NNUE triple-overlap macro-MAE objective. The controls are the current
   November 23 proposal and that single source. The choice cannot be overwritten.
3. Once the 300 reference searches finish, run `report`. It refuses to read sealed
   results without the locked choice. It verifies all result/input identities,
   records `OPENED.json`, and evaluates only the locked contender and controls.
4. Report expected-score errors, tails, signed bias, cp errors, gate false
   admissions/exclusions and unsafe shortfall, qualified counts, and paired
   stratified bootstrap intervals. Include ordinary intervals and simultaneous
   intervals for the at-most-two contender/control comparisons. No new tuning
   leaderboard or automatic production adoption follows this final comparison.

If no meaningful gain survives confirmation, retain the simpler established rule.
Rare failures and tiny differences may remain unresolved at N=300; the Black pilot
still verifies full blended-child relative gates, move regret and book/reach effects.

## Operating information

Maintained entry point: `python/chessdb_ensemble_confirmation.py` on
`codex/takeover-hardening-20261122`.
Frozen release: `D:/chess/bin/chessdb-ensemble-confirmation-20261125-v1`.
Output: `D:/chess/chessdb-ensemble-confirmation-20261125-v1`.
Preceding study: `D:/chess/chessdb-ensemble-study-20261125-v1`.

Started **10:08:59 PST**, wrapper PID **45668**, coordinator **44696**.
See `launch-pipeline.json`, `pipeline.stdout.log`, `pipeline.stderr.log`, `status.json`.
`definition.json` and `freeze.json` bind selection/code/runtime/source identities.
`confirmation-inputs.parquet`, `sample-summary.json` and `manifest.json` describe the
selected sample. `SEALED.COMPLETE.json` verifies completed engine outputs without
publishing comparisons. Completion state `sealed-complete` means the searches are
done, not that a contender has been confirmed.

Sampling and reference preflight passed by **10:10 PST**. Exactly 300 unique
NNUE triple-overlap positions, 75 per stratum, with zero duplicate keys/canonical
boards and zero overlap with the previous known examples. All selected FENs match
their 128-bit keys. Earliest ply range **5–41**, median **18**; historical observations
range **2–18,747**, median **20**. Buckets: **154, 98, 439, 79, 211, 94, 7, 25**.
The coordinator is healthy in `waiting-for-study`, with empty stderr and no choice,
opened marker, or analytical report. Engine results have not yet been generated.

Manifest SHA256 `aa64e675fdefac8986c9dac1e3a5cde978214e965f076322d26e3828279ccf2d`;
sample SHA256 `11ed937bdb647ff2094731ac29af23bd81b92c936e3b46119e5318e33bd4d504`.
Verification artifact: `chessdb-confirmation-verification-2026-11-25.json`.

For authorized resume, confirm no matching live coordinator, inspect the saved state,
and relaunch the **same frozen** `pipeline --out ...` with fresh hidden-process logs.
The OS lock prevents duplicate coordinators. `STOP` in the output directory prevents
new work; current searches drain. Remove it only on authorized resume. No automatic
report is chained after the engines, even when a choice has already been recorded.

```powershell
$runner = 'D:\chess\bin\chessdb-ensemble-confirmation-20261125-v1\chessdb_ensemble_confirmation.py'
$output = 'D:\chess\chessdb-ensemble-confirmation-20261125-v1'
# After reviewing the preceding study, replace both placeholders with that decision.
& 'C:\Users\David\Documents\Chess\.venv\Scripts\python.exe' -B $runner lock-choice `
  --out $output --contender SELECTED_RECIPE_NAME --rationale ABSOLUTE_RATIONALE_FILE
# Only after SEALED.COMPLETE.json exists and the choice is locked:
& 'C:\Users\David\Documents\Chess\.venv\Scripts\python.exe' -B $runner report --out $output
```

Four focused regression tests passed. They cover full-width keys and mirror
exclusions, sealed checkpoint/resume, refusal to read results before a choice,
refusal to select before the preceding study finishes, immutable choice/evidence
checks, the final limited comparison end to end, and result-corruption rejection.
The reused native sampler/search components retain their previous passing tests.
