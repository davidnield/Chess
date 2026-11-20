# Morning resume handoff for Claude — November 20, 2026

Prepared November 19 at approximately 22:30 PST, after David requested: “Let's end the current jobs” and a handoff for resuming the queue tomorrow morning. **All five processes in our eval/recovery chain are stopped. Nothing in this handoff was launched tonight.** Resume only when David invokes this handoff tomorrow. The PC was not shut down by this task.

This document supersedes the **running-job status**, not the technical evidence or decisions, in [the November 19 review handoff](repertoire-review-handoff-2026-11-19.md). Read that report for the full change history, review questions, model findings and experimental rationale. Read [the consolidated tuning plan](repertoire-tuning-plan-2026-11-18.md) and [execution usage](sweep-preparation-usage.md) in the worktree below before building books. The [overnight evidence](repertoire-morning-resume-evidence-2026-11-20.json) contains the exact 104 retained bucket identities, checks, sizes, timestamps, incomplete files and disk snapshot.

## 1. Ownership and immediate priorities

1. Check the stopped state and source/code identities. Preserve accepted explorer, OTB tables and all completed eval checkpoints.
2. Resume the unfinished eval DB join with the **same native binary and parameters**. A guarded join-only recipe is below so this can progress while you prepare verification resumption.
3. Address independent-verification resumption in a **new Python deployment**, without replacing the pinned native executable or changing its locked recipe. Then independently verify the completed DB, publish the full-key lookup and its terminal corrections, and run the existing lookup checks.
4. After final publication, measure and export shared pools, bind/recheck the intended OTB policy and evaluator, and run the **new Black stricter 2,000-position pilot**. The other books remain conditional on an explicit accepted pilot report.

The first three data stages were previously automatically chained. Stopping their parent processes disarmed that chain. All subsequent sweep work is planned/manifested, **not a currently running or self-starting OS job**. Do not assume an old batch file or automation will advance it.

The existing “Finish the Black 0.30 pilot” Codex automation is PAUSED and targets an obsolete job. Leave it paused. A separate Claude session was copying dashboard assets to `C:/Users/David/Documents/davidnield.github.io` during the stop audit; that unrelated task was deliberately untouched. This handoff is not a claim that every application or job on the PC has stopped.

## 2. Repository and immutable releases

Use this implementation checkout:

```text
C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation
branch: codex/sweep-preparation-20261118
HEAD before this handoff: 2ad11c81f9bfef038235996c7be30fa8f982f4a5
Python: C:/Users/David/Documents/Chess/.venv/Scripts/python.exe
```

The main checkout `C:/Users/David/Documents/Chess` is on `review-fixes` and contains unrelated dirty work. Do not reset it, stage everything, overwrite its implementation, or mistake it for the sweep checkout. This handoff and its evidence are copied into its `docs/` directory for convenient access; implementation and the complete plan/manifest are in the sweep worktree. Verify current `git status` before changing either checkout.

The November 19 fixes are already implemented: commit `0a3ca669b77449244549c2515961da970621b8f5` corrects the horizon comparison to independent B=2,000 and makes legacy Maia cache shard publication atomic/retry-safe. The follow-up report/evidence are committed in `2ad11c8`. Focused verification passed **25/25 tests, no skips**, including native full-key reuse. See `docs/repertoire-review-fixes-verification-2026-11-19.json`; do not repeat those changes from an older checkout. Broader model/runtime cache identity and production safety semantics remain review topics, not claims resolved by those two fixes.

Current manifest (updated 20 November by Claude, after merging `claude/review-fixes-20261119`):

```text
docs/sweep-manifest-2026-11-19-maia-v3.json
SHA-256 1c80733ee1d78dd8423b763656bb3b2b9a557a7ee159dd850f79cb67733c50f0
```

v3 adds the Black polish and the quality-request scope settings, so every arm ID changed. v2 (`...-maia-v2.json`, SHA-256 `d65c01d4…`) and the earlier `...-maia.json` remain unchanged as history, and current validation rejects both. Do not relabel old output directories as v3 arms.

Pinned running-code generation, to reuse unchanged:

```text
D:/chess/bin/ply51-evals-rehearsed-20261115
native: explorer-extract.exe
native source: b72b0dc5dd06
native SHA-256: 0f79bcbb95f1055fbedc075fe2c5db5ecee9a33ec3645786bdf23c62726453c0
pin.json: exact hashes for all 12 deployed files
```

All 12 file hashes were rechecked successfully after stopping. Never edit/rebuild this directory in place. A coordinator/verifier change needs a new versioned deployment; retain the old native bytes and compatible settings to reuse its existing checkpoints.

## 3. Exact stop and retained work

Stopped at **2026-11-19 22:16:57 PST**: Python wrappers/coordinators 37504, 19752, 33952, 25340, then native worker 16820. Process identities were checked immediately before termination. Parents were stopped first to prevent them from launching the next stage. This was a controlled process termination, **not a native drain-to-checkpoint operation**. No completed or partial data files were deleted.

Durable records:

```text
D:/chess/ply51-migration-20261115/overnight-stop-20261119/request.json
D:/chess/ply51-migration-20261115/overnight-stop-20261119/stopped.json
D:/chess/ply51-migration-20261115/overnight-stop-20261119/checkpoints.json
D:/chess/ply51-migration-20261115/evals/status.json
```

The eval status says `stopped`, explicitly naming the interrupted join and the user-requested overnight stop. **Keep the top-level operations `status.json` as `training explorer verified`**: it correctly records accepted upstream input, and the eval launcher relies on it. It does not mean the eval worker is still running.

| Stage/artifact | State retained | Resume behavior |
|---|---|---|
| Extraction and compact explorer | All 163 training months, 512 merged buckets, accepted independent verification | Reuse; no extraction/merge rerun |
| Eval E: Cloud/Fishnet source ingestion | 164/164 unit receipts | Skip completed units after identity checks |
| Eval C: route explorer child keys | 47/47 group receipts | Skip completed groups; retain E: junction |
| Eval J: match sources to explorer and publish DB buckets | **104/512 receipts**, 2,307,009,240 rows, 96,197,439,112 bytes | Skip receipted buckets; rebuild unfinished buckets |
| Independent full eval verification | Not started | See resumption defect below |
| Full 128-bit eval lookup | Not started; output directory absent | Future mate-shard and output-bucket checkpoints |
| Full OTB counts | Accepted and retained | Reuse accepted training export |
| New book sweep/Stockfish/Allie | No worker active | Only start after prerequisites |

The 104 bucket IDs are **not contiguous**. Latest completion was bucket 355 at 22:12:09 PST. Eight incomplete `.parquet.tmp` files remain: buckets 026, 103, 134, 203, 264, 323, 417 and 458. Their presence is expected. Let native resume recreate unreceipted outputs; do not promote them or delete entire work directories.

Post-stop audit checked every completed bucket's file size and Parquet footer row count against its receipt. Content SHA-256 checks passed for buckets 000, 130 and 355; this is a sample, not a rehash of all 96.2 GB. The accepted explorer receipt/metadata and OTB count receipt hashes were also checked. No eval completion `_DONE` exists yet, correctly.

Only unfinished parallel buckets must be recomputed. Typical recent bucket duration was about seven minutes, so lost **wall-clock progress is roughly one batch (6–8 minutes)**, not the hours already spent. Restart also rehashes source identities: the earlier startup took about 5m35s. Allow roughly 10–20 minutes for validation and repeated work before assessing productive resumed throughput; this is an estimate.

## 4. Paths, storage and source acceptance

```text
OPS     D:/chess/ply51-migration-20261115
BOOK    E:/chess/position-stats/brc-p51-train-201301-202607-20261115
CLOUD   H:/chess/evals/cloud_2026-10-07
FISHNET H:/chess/evals/fishnet
WORK    H:/chess/eval-work-p51-20261115
DB      D:/chess/eval-db-p51-20261115
LOOKUP  D:/chess/eval-lookup-p51-20261115
```

`WORK/c` is a junction to `E:/chess/eval-work-p51-20261115-child`. Preserve it. The locked native command is:

```text
D:/chess/bin/ply51-evals-rehearsed-20261115/explorer-extract.exe evals
  --book E:/chess/position-stats/brc-p51-train-201301-202607-20261115
  --cloud H:/chess/evals/cloud_2026-10-07
  --fishnet H:/chess/evals/fishnet
  --work H:/chess/eval-work-p51-20261115
  --out D:/chess/eval-db-p51-20261115
  --threads 12 --mem-gb 55 --below-normal
```

Keep threads, memory, sources and executable unchanged for this generation. The earlier approved 80 GB extraction setting was a different job; it is not a reason to change this 55 GB eval recipe. Use the guarded wrapper below rather than dropping its free-space protection.

Explorer acceptance is `OPS/verification-recovery-sliced-20261119/verified.json`, SHA-256 `959264dc725d708e43959a0b91362af32d90a0d5524b67c0c0e2daa851f8c31a`. Book metadata SHA-256 is `729801c10254906fc31c9f89b504d4c981a1f70cbbf2c6546495e477c4dde632`. All 55 independent explorer checks passed. The accepted dataset covers January 2013–July 2026, 4,530,190,835 raw games, with 51 extracted plies and full 128-bit identities. August/September 2026 remain separate evaluation data; do not merge them into training or describe previously inspected panels as pristine confirmation.

Free space at 22:20 PST, decimal TB: **D 1.524, E 0.918, H 1.514, F 3.125**, plus C 0.265. Recheck after reboot. Existing capacity plan in `OPS/evals/layout.json` includes a 20% proportional allowance, about 917 GB for combined final DB/lookup/spill, and a 150 GB forecast reserve. Do not subtract the entire source/child forecast again from current free space: those stages are already allocated. Runtime guard stops a child below **80 GB free** on the monitored drives. Keep both reserve policies. F: remains the user-approved **last-choice child-file fallback**, after H/D/E; it is not the selected location here and not permission for arbitrary DuckDB spill there.

Do not delete completed source shards, child shards, monthly F: archives, old canonical books, or source eval files to clean up this stop.

## 5. Morning launch: join-only, guarded, then finish the chain

Before launch, inspect process command lines for a duplicate `run_ply51_evals.py`, `resume_explorer_verification.py`, or native `explorer-extract.exe evals`; ensure none is already using these roots. Recheck pins, accepted upstream status, junction target and free space. Do not kill other Claude/session jobs. A leftover `_launcher.lock` file is normal: it is an OS-held byte-range lock, released when the process exits, not a marker to delete.

**Preferred sequence:** resume only DB construction while preparing verification receipt/checkpoint support. The following small wrapper reuses the deployed pin checks, state lock, capacity layout, child junction validation, native completion checks and 80 GB runtime guard. It does not copy/rebuild the binary and deliberately stops at a non-final status after the DB is built.

Save as a new file, for example `D:/chess/ply51-migration-20261115/resume-20261120/resume_join_only.py`. This recipe was syntax-checked during handoff preparation; it has **not been executed against the live job** tonight.

```python
import sys
from pathlib import Path

pin = Path("D:/chess/bin/ply51-evals-rehearsed-20261115")
sys.path.insert(0, str(pin))
import run_ply51_evals as r

with r.root_lock(r.STATE):
    try:
        for name, digest in r.read_json(pin / "pin.json")["files"].items():
            if r.sha(pin / name) != digest:
                raise ValueError(f"pin mismatch: {name}")
        r.wait_book()
        layout = r.read_json(r.STATE / "layout.json")
        if layout["book_meta_sha256"] != r.sha(r.BOOK / "_book.meta.json"):
            raise ValueError("capacity layout belongs to another explorer")
        drive = layout["phase_c_drive"]
        r.attach_child_folder(drive)
        r.status("building full Cloud/Fishnet eval DB", child_shards=drive,
                 resume_mode="join-only; verification still required")
        r.native(r.WORK, r.DB, drives=[Path(drive + "/")])
        r.status("eval DB built; independent verification pending",
                 db=str(r.DB), db_meta_sha256=r.sha(r.DB / "_build.meta.json"))
    except Exception as exc:
        r.status("stopped", error=str(exc))
        raise
```

Launch it hidden with fresh timestamped logs, after creating/reviewing the wrapper:

```powershell
$jobDir = 'D:\chess\ply51-migration-20261115\resume-20261120'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$job = Start-Process `
  -FilePath 'C:\Users\David\Documents\Chess\.venv\Scripts\python.exe' `
  -ArgumentList @('-X', 'utf8', '-u', "$jobDir\resume_join_only.py") `
  -WorkingDirectory $jobDir -WindowStyle Hidden `
  -RedirectStandardOutput "$jobDir\join-$stamp.log" `
  -RedirectStandardError "$jobDir\join-$stamp.err" -PassThru
$job.Id
```

Record the returned PID and actual native child PID/start time; the venv executable may spawn another interpreter. The root lock prevents a second cooperating coordinator, but still inspect for a directly launched native process. The default native E/C/J command will reuse E/C receipts; no special phase flag is needed. Confirm existing 104 receipts survive and the count begins increasing after source validation. Tail both logs; native progress normally appears in stderr. Check disk and memory and report measured throughput instead of assuming launch means success.

**After join completion:** require 512 bucket receipts, `_DONE`, `_build.meta.json`, and `_manifest.parquet`, with the original source/book/settings identity. Then run the independently verified next-stage coordinator described below. Join completion alone is not `eval DB and lookup verified` and must not unlock the sweep.

If verification-resumption changes are deferred, the unchanged full coordinator remains a functional alternative: launch the same Python executable with `-X utf8 -u D:/chess/bin/ply51-evals-rehearsed-20261115/run_ply51_evals.py`, hidden with new logs. It resumes native work, then runs verification and lookup automatically. **Choose one coordinator at a time.** After join-only exits, this full coordinator can also skip the now-complete native DB and finish the chain, but incurs startup source checks and has the repeat-verification limitation below. Do not re-run the explorer recovery wrapper or extraction launcher to resume accepted inputs.

## 6. Verification resumption defect and required safeguards

The deployed `run_ply51_evals.py` unconditionally runs `verify_evals.py` after native construction and before `eval_lookup128.py`. There is no receipt saying the independent verification already succeeded. The verifier itself has no durable internal progress checkpoints.

Consequences:

- Interruption during independent verification repeats that stage from the beginning.
- Interruption during lookup preserves its own checkpoints, **but the current coordinator first repeats the entire independent verification** on restart.
- Tonight loses no verification progress: that stage had not started.

Recommended work while J runs: add a content/provenance-bound verification-success receipt and coordinator reuse in a new immutable Python release. Prefer durable per-check/source/bucket verification units too, where the existing global checks can be preserved exactly. If internal resumability is too large to finish safely, at least implement the final successful-verification receipt before lookup, and explicitly report that an interruption during the verifier still restarts it. Never weaken a global check merely to call the stage resumable.

Receipts must bind the exact DB generation and manifests, book identity, raw source identity, verifier/native code and verification settings/samples. Reuse only after validating those dependencies. A log line, `_DONE`, file presence or mtime alone is insufficient. Publish receipts atomically only after success; retain global conservation/source-selection checks and deterministic sample identities. Reject stale or partial success. Keep the native source-selection behavior unchanged; a Python control-plane improvement must not invalidate E/C/J unnecessarily.

Minimum regression evidence: interrupt before/after receipt publication; verify failed checks never publish success; resume a partial lookup without repeating already-accepted verification; changing DB/book/source/verifier/settings invalidates reuse; corrupted or missing output/receipt fails closed; uninterrupted and resumed checks agree. Archive the tested new deployment and its hashes. Do not mutate the old pin to make it “match.” Preserve or explicitly version the publication contract consumed by `python/sweep/publications.py`.

The existing verification invocation is:

```text
PYTHON -u PIN/verify_evals.py DB --book BOOK --cloud CLOUD --fishnet FISHNET
  --threads 8 --mem 24GB --tmp-dir OPS/evals/verify-tmp
```

The lookup invocation is:

```text
PYTHON -u PIN/eval_lookup128.py --eval-db DB --book BOOK --out LOOKUP
```

These are schematic paths from section 4; run through a guarded coordinator, not an unmonitored shell chain. Lookup preserves per-book-bucket mate shards and completed sorted lookup buckets. Its source hashing and rebuilding in-memory mate indexes still take time on resume. Unfinished output buckets repeat. Require all 512 lookup entries and the existing `sample_lookup()` comparisons, including terminal corrections. Only after real success publish the final `eval DB and lookup verified` status with DB/lookup identities. Do not fabricate that status to satisfy pool readiness.

## 7. Queue after publication, including work that is not yet implemented

### Accepted OTB inputs — do not rebuild them

```text
D:/chess/sweep-inputs-20261119/otb-v2-20261119
D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119
```

Count acceptance is the second directory's `verified.json`, SHA-256 `2c19ff37e7bfb3bc69359000cd968fd0b71e3cad3267f135373c87bcfe6be5d9`. It says `training_counts_ready: true`. Older split receipts intentionally say false because they precede replay; do not edit them. Accepted legal game counts: training 1,509,520; validation 503,064; test 502,273. All 4,828 quarantined games and one illegal training game contribute zero to published counts. The ledger has 122,761,289 moves through ply 51, of which 101,000,635 satisfy **mover rating below 2000**. Nine aggregate exports occupy 10.77 GB, plus 5.76 GB replay/disposition files. Full count verification and a real full-key policy-reader check passed.

Use `counts/training-p30.parquet` for the main construction recipe, and the training p50 export for horizon sensitivity. Never merge validation/test into construction. The master retains ply/band; pooled minimum ply cannot reconstruct shallower counts. Counts retain frequency-one moves. Recheck close OTB-component prediction results on the repaired split; player/event-cluster uncertainty still requires an identity-aligned frozen mapping. This under-2000 dataset does not validate a 2000–2199 OTB stress model.

### Shared pools, policy and evaluator

1. Bind accepted explorer/eval/lookup receipts through `sweep.prepare` and run a small accepted pool export/capacity pilot. Twelve recipe slots cover BRC-all, BRC1800, BRC2200, Rapid2200, Classical1800 and weak1000–1799 at plies 30 and 50. Share data where possible instead of blindly materializing twelve huge duplicates. Measure storage/throughput before full uncut exports.
2. Preserve min_games=1 and uncut observed moves for both colors. Actual terminations, horizon/pruning mass and model-only replies remain separate; removing cutoff-generated Other does not remove those categories.
3. Bind the selective OTB policy to accepted training counts and real source/model/runtime identities. Reproduce ensemble checks and source nesting. Position-only Maia-3 **79M** remains the approved model. Allie adoption/history follow-ups are deferred: measured rollout cost was unacceptable to David. Do not silently substitute Allie or apply legacy Maia-30 after the new ensemble.
4. Bind weak-player continuation and rollout panels separately from game-mean construction pools. Validate actual mover populations, legal move mass, terminal outcomes, full keys and leaf scoring. Finish real rollout/population bindings, recall schedules and route-aware ECO/report publication. Fixtures are not evidence of production throughput or final book quality.

Primary ensemble: BRC-all broad prior; BRC1800 backup; Rapid2200 + Classical1800 target; eligible OTB-under2000 training update. Empirical smoothing strengths 30, backup support discounts 0.10 and 0.01, Maia strength 3 at ratings 2600/2600 and temperature 1.35, OTB prior strength 10. Exact formula/config contract is in the tuning plan and usage guide. Recheck it rather than relabeling an old pool.

### Black pilot, then staged sweep

The gatekeeper is **Black stricter expected-score B=2,000**, not the historical Black cp0.30 queue. It must demonstrate:

- Correct full-key graph/horizon behavior, source coverage, uncut own/reply menus and conserved mass.
- Actual intended policy, fixed root baseline, absolute/relative gates and verified engine nominations; no silent old-policy fallback.
- Verification of consequential reached alternatives as well as chosen moves, no stronger-evidence downgrade, independent final cloud-move checks and closure after corrected evidence.
- Resume/clean-rebuild agreement, correct distinct-position budgeting or an explicit infeasibility report, concrete PGNs/choices, and a real rollout smoke.
- Runtime, memory, spill/disk, cache use, new searches by reason, rebuild rounds and omitted mass in a separate **accepted pilot report**. A builder `final.json` alone is insufficient to release the remaining arms.

The manifest keeps lookahead floor **off**. Integrated lookahead tests do not establish an unrestricted “never below floor” guarantee. Resolve the safety/audit semantics in the review handoff before claiming that guarantee or changing the recipe. Existing correction uses full rebuilds; retained-graph incremental updating is a planned optimization, not implemented production capability.

After pilot acceptance, build the four independent 2,000-position color/gate anchors, measure extension cost and calibrate both extension metrics, then complete the primary matrix and staged sensitivities. Preserve canonical books throughout; promotion requires final evidence and a separate decision.

## 8. Locked experiment decisions to preserve

| Setting | Approved recipe |
|---|---|
| Stricter gates | Absolute 0.050 both colors; relative loss 0.060 |
| Permissive gates | Absolute White 0.110, Black 0.075; relative loss 0.095 both |
| Fixed White-score root baselines | White arms 0.5183137990703003; Black arms 0.5195669627673518 |
| Score conversion | S(cp)=1/(1+exp(-0.00368208*cp)); absolute baseline is not reset by arm/pool |
| Primary matrix | 20 base arms: 2 colors × 2 gates × independent 500/1000/2000 plus reduced 500/1000 from matching 2000 parent |
| Base horizon / objective | 30 game plies; eval-weight 0.75; legacy empirical prior 500 where active |
| Candidate source | Shared broad uncut BRC1800 menu, both colors, plus verified engine nominations |
| Our unaided construction policy | Historical 1000–1799, versioned and held constant |
| Local evidence | 5M baseline, 20M borderline/disputed; one-score-point review margin; retain stronger compatible results |
| Extension default | **remaining-maximum** risk metric, severity max(second-best gap, 0.6 × third-best gap), reach-weighted benefit per additional distinct prescription including connectors |
| Extension bounds | No four-move/depth cap; no own-player move-frequency/recall discount; valid remaining-risk bounds must cover later hard moves |
| Extension comparison | Both metrics × thresholds 0.00001/0.00003/0.0001 × four anchors = 24 variants; current default 0.00003 |
| Weight sensitivity | 0.50/0.75/1.00 at independent B=1,000, crossed with colors/gates; up to eight new arms; first verify knob actually changes relevant values |
| Policy sensitivity | Primary ensemble vs legacy BRC1800 and BRC2200 Maia-30, at B=1,000; up to eight new arms with shared candidates/evidence |
| Horizon sensitivity | 30 vs 50 at independent **B=2,000**, colors/gates crossed; four additional arms with matching 30-ply controls |

Reduced books must nest 500 inside 1000 inside 2000, preserve moves, and retain consistency after extensions against the extended master. Count connectors and inherited extensions honestly. The current extension prototype's 20M MultiPV searches need a bounded cost pilot/hybrid screening before 24 large runs.

Use common rollout horizons at least game ply 50 and at least ten plies beyond the deepest selected prescription, rounded to even; finalists also common horizon +20. Start with 4,096 paired trajectories across seeds 1–4, confirm frozen finalists with 32,768 across seeds 101–108. Preserve separate online/OTB/strong-opponent panels and own-player strength sensitivities. Recall is an evaluation scenario, not an extension discount: 30 presentations per color/day initially, 15/60 on finalists, approved low/central/high retention assumptions plus perfect recall. The plan contains exact schedules and panel weights.

Report ECO name/code/move order, reach, exact-move stability, eligible/reachable denominators and uncertainty across independent recipe families. Reduced copies are not independent votes. Do not optimize solely by one aggregate score or call fixture/model uncertainty a proven human improvement.

## 9. Deferred work, ETAs and morning report

**Do not restart** `lf_chain.bat`, `lf_chain2.bat`, the obsolete White 1.0 job, or `rare-verify-v1/black-030-horizon-v2`. Historical `_DONE` markers can be misleading; some old LF runs have no accepted final. Those are controls/history, not the new sweep queue.

ChessDB's roughly 1.188 TB snapshot and WSL/offline read probe are verified, but ChessDB has **not** been imported into this Cloud/Fishnet generation. It needs a separately measured bounded import/source-policy study and an explicit versioned integration plan. Do not add it halfway through this generation or treat it as a hidden sweep prerequisite. Allie work remains deferred. Canonical learned books remain unchanged.

From restart, allowing for current measured join rate:

| Remaining stage | Planning estimate | Checkpoint qualification |
|---|---|---|
| Finish eval J, 408 buckets | About 6–7 hours | Completed bucket receipts reusable; startup source checks add minutes |
| Independent eval verification | Roughly 5–12 hours | Unmeasured full-run estimate; currently restarts if interrupted; add receipts/checkpoints as above |
| Full-key lookup + checks | Roughly 4–10 hours | Mate-shard/output-bucket resume; current parent needlessly repeats prior verification |
| Combined remaining data chain | Roughly 15–30 active hours | Not a completion promise; downtime shifts wall-clock ETA |
| Shared uncut pools | Prior rough 12–36 machine-hours | Capacity/throughput pilot still required before trusting this |
| Integration/pilot/sweep | No reliable fresh completion ETA yet | Prior planning: 2–5 working days integration and 2–5 machine-days main bases after acceptance; extension/evaluation add work |

Report tomorrow: exact deployed revision/hash and worker PIDs; log paths; checkpoint count and measured progress; reserves/free space; whether the verifier-resume defect is fixed or explicitly deferred; which next stages are actually armed; any failed acceptance gate. Keep the progress log and this handoff's successor current. If another shutdown is requested, stop coordinators before children, preserve receipts, and record the exact interrupted stage without erasing accepted upstream status.
