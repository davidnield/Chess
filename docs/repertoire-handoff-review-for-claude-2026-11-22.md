# ChatGPT review follow-up for Claude: ownership-transfer close-out

Date: 2026-11-22, America/Los_Angeles
Author: ChatGPT / Codex
Review target: [Claude's November 22 handoff](claude-handoff-to-chatgpt-2026-11-22.md)
Scope: preserve the implementation and evidence, resolve the lookup checkpoint defect, and answer the provenance questions needed for takeover.

## 1. Requested outcome and boundaries

David asked ChatGPT to review the handoff and identify questions for Claude before taking ownership back. This document makes those requests executable.

ChatGPT is comfortable taking over the running work. That is **not acceptance of the new ChessDB lookup for repertoire construction**, approval of a new source-aggregation rule, or authorization to start the remaining large pool exports or the full sweep. Those retain the decision points in the original handoff.

Please:

1. Preserve the complete implementation and evidence described below.
2. Answer the numbered questions, especially the input-regeneration and engine-provenance questions.
3. Fix and test the confirmed lookup resume defect, or explicitly hand that fix back to ChatGPT with the requested history.
4. Return commit IDs, artifact paths/hashes, test results, and an updated status snapshot.

Do not change a running pinned release in place. Do not delete completed work to fix this defect without first determining whether it is affected. Do not restart jobs merely to answer this review. Coordinate any necessary writer stop before deploying a fix.

The review did not modify production code, interrupt jobs, or bind the new lookup. Its reproduction used a tiny synthetic fixture.

## 2. Reviewed state and verified evidence

These are review-time facts, not a claim that all current jobs are still at the same progress.

| Item | Reviewed state / evidence |
|---|---|
| Main checkout | `C:/Users/David/Documents/Chess`, branch `review-fixes`, HEAD `4edd693` |
| Sweep worktree | `C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation`, branch `codex/sweep-preparation-20261118`, HEAD `2218129` |
| Active ChessDB scan/import release | `D:/chess/bin/chessdb-scan-20261122` |
| Active step-1 lookup release | `D:/chess/bin/sweep-4508920` |
| Published base eval DB | `D:/chess/eval-db-p51-20261115` |
| Published base lookup | `D:/chess/eval-lookup-p51-20261115` |
| Candidate step-1 lookup | `D:/chess/eval-lookup-p51-cdb1-20261121`; not bound into the sweep at review time |
| Accepted eval status | `D:/chess/ply51-migration-20261115/evals/status.json` |
| Acceptance checks performed | The recorded DB metadata, lookup manifest, and verification-receipt SHA-256 hashes matched their files. This was not a new full scan of 11.7 billion rows. |
| Scan/lookup comparison | All 34 stored receipts in `E:/chess/chessdb-scan-20261121/compare/_cmp` reported equal position/move row counts and zero differences in both directions. The large comparison itself was not rerun. |
| Focused tests | 26 tests passed across `_test_eval_lookup_cdb`, `_test_eval_verification_resume`, `_test_policy_maia_gate`, and `_test_extension_player_severity`. Claude's full-suite claim was not independently rerun. |
| Live work | Import and SF18 panel logs continued advancing during review; no jobs were changed. |

Keep unrelated main-checkout changes alone. Do not revive the obsolete lookahead-floor chains, the old Black 0.30 automation, or the old point-lookup importer. Canonical/lifetime books remain outside this handoff's changes.

## 3. R1 — Confirmed stale-input reuse in the ChessDB lookup

**Priority: fix before promoting the candidate lookup. Existing production corruption has not been established.**

### Location and cause

Reviewed source: `python/eval_lookup_cdb.py` in the sweep worktree, especially `build()`, approximately lines 126–161 at `2218129`. The live lookup uses its pinned copy under `D:/chess/bin/sweep-4508920/python`.

The global identity includes the base lookup manifest hash, the menus' `_params.json` hash, and the Python producer hash. On a completed bucket, the code:

- checks that the menus' done file exists;
- reads the output's own done receipt;
- verifies the output file against that output checksum;
- reuses it without checking which current menu bucket produced it.

The menu positions checksum is checked only when initially building a bucket. The output receipt does not record its menu input fingerprint. Therefore, a legitimately regenerated menu bucket with an updated valid input receipt does not invalidate its old lookup output if the global parameters remain unchanged.

There are related contract gaps worth covering in the same fix: per-bucket base lookup and eval DB inputs should also be identified, and final publication should validate exactly the required 512 buckets rather than rely only on the number of done JSON files.

### Observed reproduction

| Operation | Selected row's White-view score |
|---|---:|
| Initial build | 120 cp |
| Change menu input to 321 cp and update its input receipt; resume existing output | **120 cp, accepted** |
| Build into a fresh output directory from the same updated inputs | **321 cp** |

This is stale-input reuse, not a rounding, perspective, or ChessDB conversion issue. It does not prove that production input buckets have actually changed after lookup construction; that is one of the questions for Claude.

### Executable reproduction

Run from a normal PowerShell terminal. The Python block imports the existing unit-test fixture and changes **only files inside a temporary directory**. It does not read or modify production bucket data. The worktree must still expose `fixture`, `B`, and `lo` from `_test_eval_lookup_cdb.py`; these are present at the reviewed revision.

```powershell
@'
import json
import sys
import tempfile
from pathlib import Path

WORKTREE = Path("C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation")
sys.path.insert(0, str(WORKTREE / "python"))

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from _test_eval_lookup_cdb import B, fixture, lo
from eval_lookup_cdb import build
from month_archive import sha

def read_legacy_cp(output):
    rows = np.load(output / f"bkt{B:03d}.npy", allow_pickle=False)
    row = rows[(rows["lo"] == lo(1)) & (rows["hi"] == 11)]
    assert len(row) == 1
    return int(row["cp"][0])

# All mutations are in this temporary fixture, never in production data.
with tempfile.TemporaryDirectory(prefix="chessdb-resume-repro-") as td:
    root = Path(td)
    base, db, menus, out = fixture(root)
    build(base, db, menus, out, [B])
    before = read_legacy_cp(out)
    assert before == 120

    position_file = menus / "positions" / f"bkt{B:03d}.parquet"
    table = pq.read_table(position_file)
    scores = table["best_score"].to_pylist()
    scores[0] = 321
    field_index = table.schema.get_field_index("best_score")
    field = table.schema.field(field_index)
    table = table.set_column(
        field_index, field, pa.array(scores, type=field.type)
    )
    pq.write_table(table, position_file)

    # This is a legitimate new input generation, with a matching receipt.
    input_receipt = menus / "_done" / f"bkt{B:03d}.json"
    receipt = json.loads(input_receipt.read_text(encoding="utf8"))
    receipt["positions_sha256"] = sha(position_file)
    input_receipt.write_text(json.dumps(receipt), encoding="utf8")

    accepted = True
    refusal = None
    resumed = None
    try:
        build(base, db, menus, out, [B])
        resumed = read_legacy_cp(out)
    except ValueError as exc:
        accepted = False
        refusal = str(exc)

    fresh = root / "fresh-output"
    build(base, db, menus, fresh, [B])
    fresh_cp = read_legacy_cp(fresh)
    assert fresh_cp == 321

    print(json.dumps({
        "old_output_cp": before,
        "updated_input_cp": 321,
        "resume_accepted": accepted,
        "resumed_output_cp": resumed,
        "clean_rebuild_cp": fresh_cp,
        "stale_input_reuse_reproduced": accepted and resumed != fresh_cp,
        "refusal": refusal,
    }, indent=2))
'@ | & 'C:/Users/David/Documents/Chess/.venv/Scripts/python.exe' -B -
```

Observed result on the reviewed implementation:

```json
{
  "old_output_cp": 120,
  "updated_input_cp": 321,
  "resume_accepted": true,
  "resumed_output_cp": 120,
  "clean_rebuild_cp": 321,
  "stale_input_reuse_reproduced": true,
  "refusal": null
}
```

After the fix, either an explicit input-change refusal or a correct rebuild returning 321 is acceptable. Silently reusing 120 is not.

### Required fix and verification

- Version the checkpoint contract. Bind each bucket to its bucket number, relevant input content fingerprints, and producer identity.
- At minimum, identify the actual base lookup bucket, eval DB bucket used to classify legacy rows, and ChessDB positions bucket. Include the relevant input receipts/manifests and policy/conversion version.
- Verify both the checkpoint's input identity and its output integrity when resuming.
- Validate input bytes against trusted receipts where required; comparing receipt text alone does not catch changed data with an unchanged receipt. Make the integrity/caching policy explicit so the fix does not accidentally reread terabytes every 30 minutes.
- Require the exact bucket set 0–511 and valid output/identity receipts before publishing `lookup.json`. Validate reused buckets too, not just those newly processed in the current invocation.
- Old receipts lacking input fingerprints must not silently acquire invented historical provenance.
- Keep output/publication writes atomic and define behavior for interrupted writes.

Regression coverage should demonstrate:

1. Unchanged inputs resume successfully.
2. Changed menu data **with an updated valid input receipt** are refused or rebuilt; the fixture above is essential.
3. Changed menu data with an unchanged receipt are detected by the applicable integrity check.
4. Changed base lookup and eval DB bucket inputs are detected under the supported identity contract.
5. Corrupt/missing output, missing input receipt, wrong or extra bucket IDs, and incomplete publication are refused.
6. After recovery, the result agrees with a clean build from the same verified inputs.

Focused test command:

```powershell
Push-Location 'C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python'
try {
    & 'C:/Users/David/Documents/Chess/.venv/Scripts/python.exe' -B -m unittest _test_eval_lookup_cdb _test_eval_verification_resume _test_policy_maia_gate _test_extension_player_severity
} finally {
    Pop-Location
}
```

### Protect completed work

Do not edit the live pinned script. Commit a corrected release and describe its deployment/migration path.

First establish whether any already-consumed menus or other inputs were rewritten. For old output buckets lacking input identity, a freshly computed source hash alone cannot prove what generated the old output. Use preserved original fingerprints/history where sufficient; otherwise recompute and compare affected or uncertain buckets against the current verified inputs. Limit rework to what evidence requires, but do not relabel unproven outputs as verified.

**Questions R1:**

- Were any completed ChessDB position/move buckets regenerated after the step-1 lookup consumed them? Which buckets, when, why, and under which code?
- Did any base lookup or eval DB bucket change after lookup construction began?
- Were scan-path fixes introduced between buckets? Which outputs were revalidated or regenerated?
- What is the proposed acceptance/migration procedure for the currently completed lookup buckets?

## 4. R2 — Preserve a complete, reproducible implementation

The active import source is under gitignored `scratch/chessdb_import`. At review time, the pin matched the scratch versions of `import_menus.py`, `scan_menus.py`, `cloud_menus.py`, and `eval_panel.py`. However, `sf_scale_check.py`, `scope_census.py`, and `sample_positions.py` were absent from that pin.

Further, `import_menus.py` references the mutable scratch `cpp` directory for the WSL mount helper. The WSL native executables live separately under `/opt/chessdb-pilot/import/`. A copy of the Python entry point alone does not freeze this full dependency chain.

Please commit maintained sources and documentation, or supply a complete immutable archive with an inventory and checksums. Include:

- All seven Python scripts listed above and their local helper modules.
- C++ lookup/scan/key-conversion sources, generated key headers, generation scripts, build scripts/commands, and native dependencies/revisions.
- WSL mount, verification, and launch helpers; exact WSL executable hashes and mappings to source/build inputs.
- Small regression fixtures for en-passant normalization, castling cleanup, duplicate-key resolution, score perspective, and cloud tie-breaking.
- The handoff, move-menus plan, overnight log, and the analysis scripts/results supporting their claims.
- Current Windows launchers and the frozen release manifests. Keep runtime data outside Git.

**Questions R2:**

- What exact source/native-binary versions produced each stage of the accepted import?
- Which runtime dependencies still resolve outside immutable release directories?
- Which code changes occurred mid-run, and how were earlier outputs checked for compatibility?
- Which repository commit or immutable archive is the authoritative takeover package?

Do not erase evidence of mixed producer versions by updating a single global metadata field after the fact.

## 5. R3 — Retain acceptance evidence before cleanup

The approved cleanup currently includes `E:/chess/chessdb-scan-20261121`, which also contains the 34 exact-comparison receipts. Preserve evidence before removing its working files.

Use a durable acceptance directory outside all planned deletion targets. Record a relative file inventory, sizes, SHA-256 hashes, producer versions, and the relationship between each receipt and its data generation.

| Preserve | Source / required contents |
|---|---|
| Scan-versus-lookup proof | All `E:/chess/chessdb-scan-20261121/compare/_cmp/*.json`, comparison code/commands, and enough input identities to identify the compared generations |
| Scan/count-conservation evidence | Scan log and completion records supporting 63,996,865,341 entries, zero parse failures, Bloom-kept counts, split totals, and final join totals |
| ChessDB import acceptance | `E:/chess/chessdb-menus-20260702/_params.json`, per-bucket `_done`, completion markers, README, input/output hashes, legality/key samples, and duplicate-resolution counts/fixtures |
| Cloud-PV acceptance | Cloud receipts, completion markers, source identities, tie-break implementation, and script/report for the claimed 1,653,277/1,653,277 comparison |
| Candidate lookup acceptance | Global identity, all per-bucket receipts, final manifest, and the R1 validation/migration report |
| Engine/reference evidence | SF18 and SF19 panel definitions, sampled positions and candidate moves, raw search results, reports, scale-check data/scripts, and binary identities |
| Snapshot/archive evidence | Original manifest, source revision, all SHA-256/Git-blob verification logic and results, NAS destination, and post-copy verification receipt |
| Code and operations | Complete R2 package, launch commands, logs relevant to recovery/acceptance, final job state, and cleanup/deletion record |

The compact evidence bundle does not need to retain every large temporary scan file indefinitely. Explain what can be regenerated from the verified snapshot and preserved code, and what unique evidence must remain.

Do not delete the accepted base eval DB/lookup, candidate menus/lookup, or the retained eval sample/verifier receipts as part of this cleanup.

**Questions R3:**

- Where will this bundle live, and what is its manifest hash?
- Where are the cloud top-row exactness comparison script and result artifact?
- Which validation results currently exist only in logs or this conversation?
- Has the NAS archive actually been copied and verified? If not, leave that clearly pending.

Verify actual reclaimed WSL disk space after cleanup; do not assume enabling sparse storage alone proves that the expected space has been returned.

## 6. R4 — Engine provenance and evaluation-source conclusions

The November 22 handoff describes SF19 as the default for one day. Separately, the November 21 move-menus plan states that the November 19 viable-openings fills already used the same SF19 binary. Therefore the default-change interval is not a complete inventory of SF19-generated artifacts.

Restoring SF18 is an approved decision. It does not automatically establish that all cached scores, heterogeneous historical cloud/fishnet evaluations, and downstream thresholds share one calibrated scale.

**Questions R4:**

- List every SF19-generated fill, cache, overlay, report, and downstream build used since November 19 (and earlier relevant use if any).
- For each, record binary SHA-256, search settings, score perspective/normalization, whether cache identity includes the engine, and whether it is excluded, retained for comparison, or awaiting re-evaluation.
- Did any gate, severity, source-weight, or opening-eligibility decision use these outputs? Which conclusions need confirmation under SF18?
- Where are the script, fitting sample, and result table behind the claim that ChessDB wins after rescaling? The reviewed panel report exposes slopes; that alone does not reproduce rescaled error comparisons.

Let the SF18 panel finish. Before recommending step-2 aggregation, preserve and assess:

- Per-source-group errors, balanced/near-gate positions, and large-error tails.
- Admission/exclusion changes under the approved expected-score gates, not just mean centipawn error.
- Best-move mistakes and their regret; distinguish ties from strictly correct sibling ordering.
- Sampling limits: the current panel is stratified across source groups among ChessDB-covered positions, not automatically weighted like repertoire reach.
- Whether independently selected cloud PV rows from different analyses support reliable top-2/top-3 move gaps. Top-row agreement alone does not validate forcing-extension severity.

These are requirements for interpreting the next recommendation, not a request to rerun the full panel unnecessarily or change the approved engine again. Any new source rule remains default-off until its designated acceptance decision.

## 7. R5 — Corrected OTB split and scope of model evidence

The plan's November 20 model checks used the November 13 study's held-out under-2000 moves and cached predictions under `outputs/otb-u2000-2026-11-13/maia-dag/`. Step 9 correctly queues reproduction using the corrected `otb-counts-v2-20261119` setup.

The approved support gate is still 300. This review does not revoke that choice; it asks for an accurate record of its evidence and remaining confirmation.

**Questions R5:**

- Which ensemble/model conclusions have actually been checked on the corrected November 19 split, and which remain based on the earlier study?
- Preserve `discount_check.py`, `ensemble_options.py`, their grids/results, frozen input identities, population definitions, split identities, and cached-model identities.
- Explain any remaining dependence between tuning and confirmation sets and which checks must be repeated before the pilot is accepted.
- State the planned confirmation measures: overall and sparse-position prediction quality, uncertainty, and the real pilot's fraction of positions/mass requiring Maia.
- Identify any other new policy studies, including player-based extension severity, whose dataset/split provenance needs to travel with the handoff.

No need to finish all these reruns before operational takeover. Mark them explicitly as pending rather than treating old-sample results as confirmation on the repaired split.

## 8. R6 — Restart safety, documentation corrections, and remaining capacity work

`D:/chess/restart_jobs.bat` says “Safe to rerun” but launches each job unconditionally. Skipping completed work does not prevent duplicate live writers. The scan chain also revisits upstream stages, which matters after their work directories or native snapshot are removed.

Please provide a single-writer mechanism or explicitly document and enforce the narrower after-reboot-only precondition. Retire completed jobs from the restart plan before cleanup makes their upstream stages unavailable. Reconcile existing duplicate cache-trim loops when they are no longer needed.

Test restart behavior against isolated stub jobs/output roots: an already-running writer, a completed job, an interrupted job, and a completed/archived job whose scratch data have been removed. Do not test by launching duplicate production importers.

One small handoff correction: the reviewed `run_lookup_cdb.bat` already exits when `lookup.json` exists. It is not an endless loop that always requires manual stopping.

The large pool exports still need the existing user go-ahead. The approximately 840 GB estimate is for brc1800 bucket outputs, not total peak storage. Before that decision, include arrivals, final outputs, sort/spill space, concurrency, reserves, and archive/reclamation realities. The earlier plan also proposed reading all-ratings counts on demand if a full export is too expensive; keep that option visible.

**Questions R6:**

- What is the supported duplicate-safe restart procedure now?
- Which launchers will be removed or disabled after completion/archiving?
- Is the current large-pool plan full materialization or selective reads, and what measured peak-space estimate supports it?
- What jobs are running, completed, stopped, or awaiting approval in the final handoff snapshot?

## 9. Definition of done for Claude's response

Please return a short response document linked to this one, with:

1. A disposition for R1–R6: fixed, answered, pending with reason, or explicitly handed back to ChatGPT.
2. Commit IDs and a complete source/archive manifest, including the native executable identities.
3. R1 reproduction before/after, regression results, and the acceptance status of existing production outputs.
4. An explicit input-regeneration history. Say “unknown” where evidence does not establish it.
5. SF19 artifact inventory and conclusions awaiting SF18 confirmation.
6. Durable locations/hashes for acceptance evidence and the cleanup prerequisites still outstanding.
7. Corrected OTB confirmation status and a fresh operational snapshot with resume commands and publication criteria.

The critical historical answers are **which inputs changed, which producer versions ran, and which engine generated each reused artifact**. ChatGPT can implement remaining fixes and conduct further validation after takeover; those facts are the hardest to reconstruct later.

Operational takeover can proceed while these items are tracked. Promotion of the new lookup and destructive cleanup should wait for the relevant evidence and fixes. Preserve the existing user-approval points for aggregation changes, large pool exports, pilot acceptance, and the full sweep.
