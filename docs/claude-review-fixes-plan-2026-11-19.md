# Plan: repertoire builder, from 20 November 2026

*Claude. First written 19 November (the review fixes, now done; history at the end). Rewritten 20
November, when David handed the project back to me with ChatGPT's morning handoff
(`docs/repertoire-morning-resume-handoff-2026-11-20.md`).*

## How the work is ordered

The machine time is a single chain:

1. **Eval DB join.** About 6–7 hours. 104 of 512 buckets are done.
2. **Independent eval verification.** 5–12 hours, unmeasured at full scale.
3. **Full-key lookup.** 4–10 hours.
4. **Shared pools.**
5. **The Black pilot.**

Each depends on the one before. Everything else is code or small checks fitted into the gaps. The
order below keeps the chain moving and does the code work while it runs:

- **The verifier-resume receipt** must land before step 2 starts, so an interruption there doesn't
  cost hours.
- **The branch merge and v3 manifest** must land before any sweep binding.
- **The request census** must land before the pilot, because it decides whether the pilot can close.

Two corrections to ChatGPT's handoff, both from my 19 November work:

- **The current manifest is v3, not v2:**
  `docs/sweep-manifest-2026-11-19-maia-v3.json`, on branch `claude/review-fixes-20261119`
  (`142ad3a`, `acc418f`).
- **The sweep checkout needs that branch merged in.** Codex's branch has one later commit
  (`4092ccd`: the stop record and this handoff). Only `docs/sweep-preparation-progress.md` overlaps
  between the two, and both sides just appended entries.

## The queue

| # | when | task | done when | status |
|---|---|---|---|---|
| 1 | morning, before any launch | **Pre-flight.** Re-read the stop record (`D:/chess/ply51-migration-20261115/overnight-stop-20261119/`): no `run_ply51_evals.py`, `resume_explorer_verification.py` or native `explorer-extract.exe evals` running; the 12 pin hashes in `D:/chess/bin/ply51-evals-rehearsed-20261115/pin.json` match; the explorer acceptance receipt is `959264dc…`; the `H:/chess/eval-work-p51-20261115/c` junction points to E:; free space checked (at 22:20 it was D 1.524 TB, E 0.918, H 1.514, F 3.125); 104 J receipts present. | All checks recorded in a fresh status note | done |
| 2 | morning | **Resume the eval join (J) only**, with the guarded `resume_join_only.py` wrapper from the handoff (§5): the same pinned binary, sources, 12 threads, 55 GB, and the 80 GB runtime guard. Launch hidden with timestamped logs. Record the wrapper and native PIDs. Watch the first 20–30 minutes: source rehash, then the receipt count rising above 104. | J receipts increasing at roughly the old rate (about 7 minutes per bucket batch) | running |
| 3 | while J runs | **Merge `claude/review-fixes-20261119` into `codex/sweep-preparation-20261118`.** Resolve the progress-log append, rerun `_test_review_fixes_1119`, `_test_sweep_coordinator` and `_test_sweep_preparation`, and make v3 the only current manifest in the docs, including the morning handoff's references. | Merged branch with the tests green; v2 marked archived | todo |
| 4 | while J runs | **The verifier-resume receipt** (handoff §6), in a **new** versioned Python deployment next to the old pin, which is not modified. A success receipt for `verify_evals.py` binds the DB generation and manifests, the book, raw sources, verifier and native code, and the settings. The coordinator reuses it only after re-validating those, so a lookup interruption no longer repeats verification. Per-check checkpoints inside the verifier are added only where the global checks stay exact; otherwise that is reported as deferred. Tests cover interruption before and after the receipt, a failed check publishing nothing, a stale DB/book/source/settings/code identity refusing reuse, a corrupt or missing receipt failing closed, and resumed checks matching uninterrupted ones. | The new release is archived with hashes and tests pass; the old pin is untouched | todo |
| 5 | while J runs, at below-normal priority | **Full `python/run_tests.py`** on the merged branch, with `EXPLORER_EXTRACT_EXE` set to a current binary. Its Rust-extract and `verify_book` tests are I/O heavy, so I watch J's bucket rate and pause the suite if it slows J by more than about 10%. | Suite result recorded; any failure explained as code or environment | todo |
| 6 | after J: 512 receipts, `_DONE`, `_build.meta.json` and `_manifest.parquet`, with the original identity | **Independent verification, then the lookup**, through the step-4 coordinator: `verify_evals.py` (8 threads, 24 GB), then `eval_lookup128.py` with all 512 lookup buckets and `sample_lookup()` including terminal corrections. Only then is `eval DB and lookup verified` published. | Accepted lookup receipt | todo |
| 7 | during the lookup (light machine load) | **Request census on a real Black graph.** Build Black stricter at 2,000 with `--verification-policy quality` on the existing 64-bit 2000+ pool and eval arrays, as a cost probe only (not a pilot book). Report round-0 requests by reason, deferred and below-threshold mass, empty-menu positions by cause, and estimated engine time. About 35 minutes and 40 GB commit. If requests exceed what one round should search, tune `min_reach`/`max_parents` before the pilot. | Census numbers in the progress log | todo |
| 8 | after step 6 | **Shared pool capacity pilot, then the exports** (handoff §7). `sweep.prepare` binds the accepted explorer, eval and lookup receipts. Export one bounded pool first to measure size and time against free space (**E: has only 0.918 TB free**, so place outputs deliberately). Then the 12 recipes (BRC-all, BRC1800, BRC2200, Rapid2200, Classical1800, weak1000–1799, each at p30 and p50), sharing data where possible. `min_games = 1`; endings, horizon and model-only mass kept separate. | Each pool's meta passes `sweep.bindings` for its recipe | todo |
| 9 | after step 8 | **Bind the policy and evaluator.** The selective OTB policy goes on the accepted training counts (`otb-counts-v2-20261119`, `training-p30`, and p50 for the horizon arm) with Maia-3 79M position-only at 2600/2600, temperature 1.35, and pinned model and runtime identities. Reproduce the ensemble checks, then the weak-player continuation and rollout panels. | Preflight passes for the pilot arm on real bindings | todo |
| 10 | after step 9 | **The Black stricter 2,000-position pilot** (v3 manifest), closed under the step-7 settings. Its accepted pilot report shows: resolved settings including `--polish`, request counts by reason, deferred and unresolved mass, rounds, runtime, memory, disk, PGNs of the top decisions, rare and engine prescriptions, a fresh best-reply check at the top booked positions, and a real rollout smoke against the lifetime Black book. **Stop for David's review.** | Accepted pilot report | todo |
| 11 | after David accepts the pilot | **Staged sweep:** the four 2,000-position anchors; an extension cost pilot, then the calibration (24 variants); the main matrix; then the sensitivities. | Per the tuning plan's release gates | todo |
| 12 | before the opponent-policy sensitivity only | **Control overlays:** a generator for `brc1800-maia30` and `brc2200-maia30` that writes `reply_pool` into each overlay's metadata. Preflight refuses those 8 arms until it exists, by design. | Preflight passes for the policy arms | todo |
| 13 | optional, when the machine would otherwise be idle | **Polish on/off referee A/B** on one Black book (cut menu, 0.30 or 0.60). About 25 minutes to build and 2.5 hours to score. Not a blocker: polish stays in the recipe either way. | Paired difference recorded | todo |

**Decisions for David, unchanged from the review:**

- whether the construction policy should be OTB-dominated where OTB data is plentiful;
- whether a permissive-arm winner (which re-admits the Blackmar-Diemer) needs a fresh
  best-defence pass;
- whether an empty menu should end the book.

None of these blocks steps 1–9. The pilot report (step 10) includes online-panel scores so the
first can be judged.

**Do not touch:** `lf_chain.bat`, `lf_chain2.bat`, `rare-verify-v1/black-030-horizon-v2`, the paused "Finish the Black 0.30 pilot" automation, the unrelated dashboard-copy session, ChessDB import (a separate study), Allie, or the canonical books.

**Before any shutdown:** stop coordinators before their children, keep every receipt, and record the interrupted stage without changing the accepted upstream status (handoff §9).

## Progress log

- *20 Nov.* Plan rewritten from ChatGPT's morning handoff. At writing, nothing in the eval chain was running; free space was D 1.524 TB, E 0.918, H 1.514, F 3.125, C 0.264.
- *20 Nov, 08:42.* Pre-flight passed (record `D:/chess/ply51-migration-20261115/resume-20261120/preflight-20261120.json`): no duplicate eval processes; 12/12 pin hashes; explorer receipt `959264dc…`, book meta `729801c1…`, OTB counts `2c19ff37…`; junction to E:; 104 J receipts and 8 `.tmp`; free space unchanged. J resumed with `resume_join_only.py` in that folder: wrapper PID 10856, interpreter 19324, native `explorer-extract.exe` 29952 (started 08:42:55); logs `join-20261120-084253.{log,err}`.

---

## History: the 19 November review fixes (done)

Findings: `docs/claude-review-of-codex-2026-11-19.md`. Branch `claude/review-fixes-20261119`
(worktree `scratch/worktrees/review-fixes-1119`), starting at Codex's `2ad11c8`. Committed:
`142ad3a` (code and tests), `acc418f` (docs).

| # | finding | change | status |
|---|---|---|---|
| 1 | Black solver dropped polish | `match_inflate` back to 1.5; Black solver `dp+match-distinct+polish`; `builder.command` adds `--polish` for Black; manifest v3 | done |
| 2 | Parent eval above every child empties the menu with no request | `parent-above-children` request; the inconsistency check is symmetric; gate semantics unchanged | done |
| 3 | Quality frontier unbounded | Requests come from the book's policy reach, thresholded (`min_reach` 0.0005), ranked and capped (`max_parents` 2,000), with accounting | done |
| 4 | Unverified rare reference cuts a rival at an unbooked node | `unverified-rare-reference` request (`rare_trust` 20), measured against the trusted children | done |
| 5 | Recipe labels not bound to data | `sweep.bindings.check_arm` in preflight; the builder cap refusal for full-key pools; `build_weak_edges.py` cap and key-width checks; control overlays must declare `reply_pool` | done |
| 6 | Ensemble constants hardcoded | Read from the recipe; identical at the approved values | done |
| 7 | Endings-only and model-only replies | Pseudo-continuation; model-only reply accounting; frontier cache implementation 3 | done |

Tests on 19 November:

- the new `_test_review_fixes_1119.py`, 9 tests;
- 51 passed (1 opt-in skip) across the sweep, full-key, gate and horizon suites;
- `_test_rare_verify`, `_test_book_builder_rs`, `_test_budget_eval_loss`, `_test_opponent_prior`,
  `_test_lookahead_floor`, `_test_pool128` and `_test_docs_paths` all pass.

The checks deferred to 20 November are now queue steps 5 (full suite), 7 (census) and 13 (polish
A/B).
