# Hand-off from Claude to ChatGPT, 22 November 2026

*Written 22 Nov ~12:00 PST by Claude, because David is close to his weekly Claude usage limit (it resets
Wednesday 25 Nov). It covers everything since ChatGPT's last hand-off to Claude,
[the 20 November morning resume hand-off](repertoire-morning-resume-handoff-2026-11-20.md): what was done,
changed and decided; what is running and queued right now; and what is planned. Nothing here needs to be
restarted: every running job is detached and resumable, and the ones that finish on their own are listed with
what to check when they do.*

**Read in this order:**

1. This document.
2. [The plan and its progress log](claude-review-fixes-plan-2026-11-19.md): the 13-step queue, with every step
   through 21 Nov in detail.
3. [The move-menus plan](move-menus-plan-2026-11-21.md): the ChessDB and cloud-PV import, with its own status
   log.
4. [The overnight log](overnight-log-2026-11-22.md): 22 Nov hour by hour, every decision taken while David slept
   and the ones he approved in the morning.
5. `CLAUDE.md` in the sweep worktree. It has two new paragraphs: ChessDB step 1, and the Stockfish rule.

---

## 1. Three things to know before touching anything

1. **The engine is Stockfish 18 again, and must not be switched without re-measuring the centipawn scale.**
   - Stockfish 19 was every tool's default for one day (21 Nov).
   - On 22 Nov the validation panel showed SF19 scores positions **~1.47× more extreme than SF18 at the same
     depth**. That is provisional, from 51 positions; the full rerun is in progress.
   - SF18 sits on the scale of everything the project measures in centipawns: the eval DB's cloud and fishnet
     scores, the fitted eval-to-score curve, the 30/60 cp soundness limits, the critical-move gap and the
     extension severities.
   - David approved going back. Sweep worktree `2218129` and main checkout `4edd693`.
   - `python/engine_path.STOCKFISH` now names `D:/chess/bin/stockfish-18/stockfish-windows-x86-64-avx2.exe`. It is
     a byte-identical copy of the WinGet SF18 (sha256 `c86215fa…`), so every sha-pinned cache is valid again.
2. **The ChessDB import is finishing on a new code path.** It is a single full scan, not point lookups. That path
   passed a hard tolerance-0 comparison against the old lookup path on 34 buckets. Do **not** start the old
   importer (`D:\chess\chessdb-import\run_import.bat`). It stopped at 34 buckets on purpose and is only the
   fallback.
3. **After a reboot,** run `D:\chess\restart_jobs.bat` and nothing else. Every job in it skips finished work.

---

## 2. Code: where it lives

| what | where | state |
|---|---|---|
| Sweep / pipeline | `C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation`, branch `codex/sweep-preparation-20261118` | HEAD `2218129`, **local only (no remote)**. Commits since your hand-off: `909d8f8` (merge of claude/review-fixes-20261119), `441fe59` (verifier-resume receipt), `7d811ab` (`mm_hash` fix + `_test_bound_names`), `2dd3a63` (otb-ensemble-v2 support gate, manifest v4), `b16354b`/`b880bbb` (plan logs), `8660145` (extension player severity, manifest v5), `4508920` (ChessDB step-1 eval lookup), `ab5b78a` (SF19 everywhere, superseded), `2218129` (back to SF18). |
| Main checkout | `C:/Users/David/Documents/Chess`, branch `review-fixes` | 3 commits ahead of origin, not pushed: `2c644aa` (trainer critical flags), `5baab55` (SF19, superseded), `4edd693` (back to SF18). Its uncommitted `README.md`, `python/explorer_policy_value.py`, `python/repertoire_explorer.py` changes predate this work: leave them. |
| ChessDB / cloud import code | `scratch/chessdb_import/` (gitignored), pinned copy `D:/chess/bin/chessdb-scan-20261122` (+ `cpp/`, binary sha in `cdb_scan.binary.sha256`) | `import_menus.py` (lookup path), `scan_menus.py` (scan path), `cloud_menus.py`, `eval_panel.py`, `sf_scale_check.py`, `scope_census.py`, `sample_positions.py`; C++ in `cpp/` (`cdb_lookup.cpp`, `cdb_scan.cpp`, `fen_keys.cpp`, generated `z128_keys.h`/`z128_pos.h`). WSL binaries in `/opt/chessdb-pilot/import/`. |
| Frozen sweep code for pools | `D:/chess/bin/sweep-2dd3a63` | used by the step-8 pool exports and the probe |
| Step-1 eval lookup code | `D:/chess/bin/sweep-4508920` | used by `run_lookup_cdb.bat` |

Current sweep manifest: **v5** `docs/sweep-manifest-2026-11-21-v5.json` (v4 and v3 are refused by `spec.validate`).

---

## 3. What was done, changed and decided since 20 November

### 20 November (steps 1-7 of the plan; details in its progress log)

- **The eval chain was resumed and finished.**
  - Pre-flight passed. The join resumed with your `resume_join_only.py` and completed at 14:01.
  - The new verifier-resume release `D:/chess/bin/ply51-evals-resume-20261120` (`441fe59`) publishes a receipt
    only after `verify_evals.py` passes, and reuses it only when the identity is unchanged. The old pin is
    untouched.
  - The lookup's mate pass and bucket pass decayed in-process. Fresh processes finished them (`mate_fill.py`,
    `lookup_fill.py` in `D:/chess/ply51-migration-20261115/resume-20261120/`).
  - **Published 21 Nov 07:13:** status "eval DB and lookup verified", 11,715,795,322 full-key scores. DB
    `D:/chess/eval-db-p51-20261115`, lookup `D:/chess/eval-lookup-p51-20261115`.
- **Branches merged.** `claude/review-fixes-20261119` went into the sweep branch (`909d8f8`).
- **Full suite:** 107/108; the one failure was an opt-in script, which now skips cleanly. Later 111/111 with
  `EXPLORER_EXTRACT_EXE` set to the pinned exe. The default-path exe is an old Oct-30 build without 128-bit
  support, so tests fail without that variable.
- **Census bug fixed.** `8a9e7be` had dropped the `mm_hash` binding; `7d811ab` restores it and adds
  `_test_bound_names.py`.
- **Step 7, the request census** on a real Black stricter 2,000 graph (`D:/chess/census-20261120`):
  - 1,475 parents requested in round 0, 8,590 moves.
  - 7 empty menus, all from the parent reference.
  - 4.8–20 h of engine time.
  - Pilot keeps `min_reach` .0005 and `max_parents` 2,000.
  - Recheck the count on the new lookup at pilot round 0.
- **Opponent-ensemble checks.**
  - The low-rated discount is fine and the approved recipe stands.
  - Dropping Maia is not viable. Dropping the all-ratings pool is slightly worse.
  - **David approved** `otb-ensemble-v2` = the approved weights + `maia_support_gate` 300 (`2dd3a63`, manifest v4).
- **David decided step-8 scope:** the five p30 pools only (brc-all, brc1800, rapid2200, classical1800,
  weak1000-1799). brc2200-p30 waits for the policy sensitivity, and every p50 waits for a horizon decision.

### 21 November

- **Deleted, with David's OK, after publication:**
  - the eval work dirs on E: and H: (~1.7 TB);
  - later in the day, `pool_work`, `uncut-own-v1`, the older cloud snapshot (`H:\chess\evals\cloud`), the p51
    pilot book and stale DuckDB spills (`D:\chess\_housekeeping_2026-11-21\delete.bat` and its log).
  - Kept: `H:\chess\eval-work-p51-20261115-sample` and the verifier receipt and checkpoint.
- **Step 8 pools: two done, the big three not started.**
  - `make_bindings.py` wrote `D:/chess/sweep-inputs/bindings.json`.
  - **classical1800-p30 done:** all 4 gates pass; pool 22 GB, 297M edges.
  - **rapid2200-p30 done:** all gates pass.
  - The big three (brc-all, brc1800, weak1000-1799) were **not** launched, for lack of space and sizing. A
    **brc1800-p30 probe** was approved and started 20:35 (§4).
- **Extension player severity** (`8660145`, manifest v5).
  - `sweep.extend --severity engine-or-player` takes max(engine gaps, our 1000-1799 players' expected loss). It
    may only raise severity, never lower it.
  - Default `engine` is an exact no-op.
  - v5 adds 12 player-severity runs to the extension comparison.
- **Entry-bias study** (`D:/chess/entry-bias-20261121/report.txt`).
  - There is no general "they chose the tactic, so they know it" effect.
  - There is a modest effect at non-recapture and very forcing follow-ups, which the engine term covers through
    the max.
  - **David accepted:** no correction, no lowering variant; the v5 comparison decides.
- **Trainer critical-move flags** (`2c644aa`, `python/trainer_app/critical.py`).
  - A card is critical when its book move is the only good move (other engine top-3 moves ≥ .15 worse) and not a
    recapture. It gets a banner and its priority × `critical_boost` (default 2).
  - White has 118 of 520 cards, Black 90 of 527.
  - Rerun `python -m trainer_app.critical` after every pack rebuild. The flags were built from the SF18 cache.
- **David declined a custom GPU move model.** Weeks to months of training aren't worth it. The Maia/Allie ranking
  check was dropped too.
- **ChessDB move-menu import approved.** David removed ChessDB from the do-not-touch list.
  - Scope: book positions with **≥ 2 games** (~4.78B).
  - It also imports the Lichess cloud dataset's unused multi-PV rows.
  - The current `eval_cp` is untouched. The plan is `docs/move-menus-plan-2026-11-21.md`.
- **ChessDB aggregation, step 1** (David). `python/eval_lookup_cdb.py` (`4508920`) builds a *new* lookup
  generation:
  - it fills positions that have no score;
  - it replaces scores that come only from pre-2021 fishnet;
  - new source codes: 4 for a gap fill, 5 for a replaced legacy score.
  - Output `D:/chess/eval-lookup-p51-cdb1-20261121`. Using it in builds is a separate, default-off sweep decision.
  - **Step 2** (ChessDB leading over 2021+ fishnet, and so on) waits for the validation panel (§4).
- **WSL Ubuntu was moved twice** with `wsl --manage Ubuntu --move` (no admin): C: → H: on 21 Nov, then
  **H: → E:\wsl\Ubuntu** at 22:47–23:00. H: is a USB SSD and capped the lookups at its 32-command queue.
  - The distro holds the ChessDB reader (`/opt/chessdb-pilot`) and a verified native copy of the snapshot
    (`/var/lib/chessdb`, 1.1 TB inside a 1,113 GB vhdx).
- **SF19 everywhere** (`ab5b78a`, `5baab55`). Superseded on 22 Nov (§1).

### 22 November

- **ChessDB import switched to a full scan** (David: "why not both", meaning the disk move and the scan).
  - Measured: a point lookup costs ~200 µs of CPU, and the old export's EPD pass was 90% of its time.
  - The new path, `scan_menus.py chain`:
    1. keys-only export of all 512 buckets;
    2. one scan of the snapshot (`cdb_scan`): every entry in both orientations, our 128-bit key computed in C++,
       an 8 GiB Bloom filter of the 4,778,964,781 book keys;
    3. split by bucket;
    4. **comparison gate**: the 34 lookup-made buckets rebuilt and compared at tolerance 0, or stop;
    5. join of the other 478 into `E:\chess\chessdb-menus-20260702`, through the lookup path's own output code.
  - The scan read **63,996,865,341 entries** (exactly the snapshot's count) in 4.4 h, with 0 parse failures, and
    kept 2,228,572,202 rows. The split's rows add up exactly to that.
  - **The gate passed at 10:48: all 34 buckets identical,** positions and moves, EXCEPT ALL both ways.
- **Exactness rules in the scan path.** Each one mimics what the lookup's EPD probe would have hit; each is
  documented in the code headers and in `E:\chess\chessdb-menus-20260702\README.md`.
  - **En passant:**
    - Our key hashes an ep square when the capture is pseudo-legal; the book's EPD writes it only when legal.
    - Python-chess decides each affected row (variant rows: keep only if the restored capture is illegal; FEN-ep
      rows: keep only if legal).
    - The old importer's sample check stopped on such a position at bucket 28 on 21 Nov; `check_sample` now
      accepts exactly that case (`ep_lossy`).
  - **Castling:** entries whose castling field differs from the cleaned rights are dropped. ChessDB holds ~1B such
    orientations, mostly Chess960.
  - **Side to move** is taken from the book's first ply (odd = White).
  - **Duplicates:** ChessDB sometimes holds one position under two encodings (first seen in bucket 40, 9 so far).
    The join asks the lookup itself (`resolve_by_lookup`: book EPD → `cdb_lookup`) and keeps the scan row equal to
    its answer; anything else stops the bucket. Done records count them as `duplicate_keys_resolved_by_lookup`.
- **Lichess cloud PVs: done.**
  - `E:\chess\chessdb-menus-20260702\cloud_moves`: 52,933,853 positions, 133,450,747 moves.
  - Each move is scored by the deepest cloud analysis with it as a PV's first move. Values are cp/mate in
    **White's view**, unlike ChessDB's side-to-move scores.
  - It matches the eval DB's chosen cloud row on **1,653,277 of 1,653,277** positions checked, after a tie-break
    fix (the superseded first build was deleted with David's OK).
  - Stale-castling FENs are dropped and illegal-ep FENs mapped to both keys, as the eval DB does. David approved
    keeping that rule.
  - Code: `cloud_menus.py` + `cpp/fen_keys.cpp`.
- **Step-2 validation panel, run 1 (SF19 depth 24).**
  - 1,500 positions: 6 source groups × 250, from buckets 2, 6, …, 30. The groups are deep cloud, shallow cloud,
    NNUE once, NNUE repeated, pre-NNUE and gap.
  - Searches: each position at depth 24, plus the child after each of ChessDB's top-3 moves at depth 23.
  - Report: `E:\chess\eval-panel-20261122\report_d24.md`.
  - **Its raw means are scale-skewed.** SF19-on-source slope: cloud ~1.45, ChessDB ~1.35, fishnet ~1.1–1.2. That is
    what exposed the SF19 problem.
  - On medians and after rescaling, **ChessDB was the most accurate source in every group** (median error 8–12.5
    cp).
  - ChessDB's rank-1 vs rank-2 move order agrees with the engine at 84.6% of positions.
- **David approved (~10:20):**
  1. back to SF18 everywhere;
  2. rerun the panel against SF18 and decide step 2 on that;
  3. keep dropping stale-castling cloud FENs;
  4. delete the superseded cloud build;
  5. after the import, archive ChessDB to the NAS and delete the local copies;
  6. then the brc1800 export, which still needs his explicit go-ahead.
  - The planned SF19 rare-move re-search is **cancelled**.

---

## 4. Running now (snapshot 22 Nov 11:55)

All jobs were launched detached (`Start-Process … -WindowStyle Hidden`). Commit was 64.8 of 135.9 GB. Free space:
C 0.23 TB, D 0.74, E 1.12, F 2.84, H 2.09.

| job | launcher (PID at 11:55) | progress / ETA | log | when it finishes, check |
|---|---|---|---|---|
| **ChessDB menus fill-in** (scan chain, join phase) | `D:\chess\chessdb-import\run_scan_chain.bat` (cmd 7212, python 11256 + join workers) | 195 of 512 buckets; ~2.7/min → **~14:00** | `D:\chess\chessdb-import\import.log` (`convert N: … [scan]`), console `scan.out` | `chain finished` in the log; 512 files in `E:\chess\chessdb-menus-20260702\_done`; `_DONE` written; every scan bucket ~42.0% found. A `STOP` or Traceback means read `scan.out`; rerunning the bat resumes. |
| **Step-1 eval lookup** (ChessDB gap fills) | `D:\chess\chessdb-import\run_lookup_cdb.bat` (cmd 42820; python 35560), loops every 30 min | follows finished menu buckets; last pass 11:11 | `D:\chess\chessdb-import\lookup_cdb.log` | `lookup.json` in `D:\chess\eval-lookup-p51-cdb1-20261121` (written only when all 512 buckets exist), expected ~30 min after the menus finish. Then stop the loop (it is a `while` loop in the bat). |
| **Validation panel, SF18 rerun** | `D:\chess\chessdb-import\run_eval_panel_sf18.bat` (cmd 37544, python 32684) | 2,400 of 5,260 searches → **~14:00** | `D:\chess\chessdb-import\panel.out` | `sf18 panel done exit 0`; report `E:\chess\eval-panel-20261122\report_sf18_d24.md` (median, balanced-position and all-position errors, scale table, per-move errors, sibling order). |
| **brc1800-p30 probe** (pool sizing) | `D:\chess\sweep-inputs\brc1800_probe.bat` (cmd 10020, python 9488) | arrivals done; buckets 6 of 8 (5 28 109 146 295 346 done; 396, 509 left; ~75 min each while contended) → ~13:30 | `D:\chess\sweep-inputs\brc1800-probe.log` | `probe buckets done`; files in `D:\chess\sweep-inputs\brc1800-p30\work\buckets\` (`bNNN.edges/.aux/.stats`). |
| WSL page-cache trim | `D:\chess\chessdb-import\wsl_cache_trim.bat` (two loops are running, wsl 44828 and 10352: harmless duplicates) | every 3 min | — | Stop both once nothing uses WSL any more. They keep the WSL VM's Linux file cache from holding tens of GB of Windows commit. |

`D:\chess\restart_jobs.bat` relaunches exactly these: trim, scan chain, lookup loop, probe, SF18 panel.

---

## 5. Queued: what to do as each job finishes (David has approved all except where noted)

1. **Menus finish (~14:00).**
   - Confirm 512 done records, `_DONE`, and the README in `E:\chess\chessdb-menus-20260702`.
   - Sum the done records: positions, found, moves, `duplicate_keys_resolved_by_lookup`.
   - Add a closing entry to the move-menus plan's status log.
2. **Step-1 lookup finishes (~14:30).**
   - Check its `lookup.json`.
   - The expected totals are ~1.05B gap fills and ~112M legacy replacements, about +9% rows.
   - It is **not** bound into any sweep run; `bindings.json` still pins the published 2026-11-15 lookup.
3. **SF18 panel finishes (~14:00). Then write David a step-2 recommendation.** He decides it.
   - Read `report_sf18_d24.md` for: whether ChessDB is the most accurate source per group on SF18's scale; whether
     the 13 Nov rules (cloud+NNUE mean for deep cloud, a median of three under shallow cloud, 2:1 ChessDB:NNUE
     without cloud, ChessDB alone for pre-NNUE and gaps) beat each single source; and per-move rank errors.
   - Expected direction: ChessDB leads wherever there is no cloud eval, and a cloud+ChessDB blend where there is.
   - Whatever is chosen enters builds only as a **default-off** sweep setting, adopted through the switch rule.
     `eval_lookup_cdb.py` is the place for a step-2 generation.
4. **ChessDB archive, once 1–2 are done** (approved).
   1. Copy `H:\chess\chessdb\chess-20260702` (1.19 TB) to the NAS, `\\DyerNAS\data\chess` per the plan; this
      session did not test the share's reachability.
   2. Verify it against `manifest.tsv`. The 12 small files are Git blob sha1s, see `cpp/verify_small.sh`.
   3. Then delete the H: copy.
   4. Delete the native copy inside WSL (`/var/lib/chessdb`), then `wsl --manage Ubuntu --set-sparse true` to
      shrink the vhdx on E:.
   5. Delete the scan's working files `E:\chess\chessdb-scan-20261121` (units 206 GB, split, keys ~160 GB,
      cloud_work, compare).
   - Record each deletion in the drive README (`E:\chess\README.md`, `H:\chess\README.md`).
5. **Probe finishes (~13:30). Report final sizing to David.**
   - Per bucket so far: 23.9M edges, 20.4M parents, 1.31 GB of edges + 0.32 GB of aux.
   - × 512 ≈ 12.2B edges and ~840 GB of bucket outputs; arrivals were ~12.4B rows.
   - Timings are upper bounds: everything shared the CPU.
   - Then **the full brc1800 / brc-all / weak1000-1799 p30 exports need David's go-ahead.** Place their workspace
     on E: after step 4 frees space.
   - `sweep.prepare` reuses the probe's arrivals and buckets.

---

## 6. Planned (the 13-step plan, from step 8)

| step | what | state |
|---|---|---|
| 8 | Shared pools (five p30 pools) | classical1800 and rapid2200 done; brc1800 probe nearly done; the big three wait for space and David |
| 9 | Bind the policy and evaluator: `otb-ensemble-v2` (support gate 300) on `otb-counts-v2-20261119` training-p30, Maia-3 79M position-only 2600/2600 T 1.35; reproduce the ensemble checks and the continuation/rollout panels | todo, after step 8 |
| 10 | **Black stricter 2,000-position pilot** (manifest v5), closed under the census settings. Its accepted pilot report shows: resolved settings including `--polish`, request counts by reason, deferred and unresolved mass, rounds, runtime, memory, disk, PGNs of the top decisions, rare and engine prescriptions, a fresh best-reply check at the top booked positions, and a real rollout smoke against the lifetime Black book. **Stop for David's review.** | todo |
| 11 | Staged sweep: the four 2,000-position anchors, an extension cost pilot, then the calibration (the v5 extension comparison: 36 runs), the main matrix, the sensitivities | after David accepts the pilot |
| 12 | Control overlays `brc1800-maia30` / `brc2200-maia30` with `reply_pool` metadata | before the opponent-policy sensitivity only |
| 13 | Optional polish on/off referee A/B on one Black book | when idle |

Also planned:
- **Step 2 of the ChessDB aggregation**, after the SF18 panel (§5.3).
- Possibly a **step-1/step-2 lookup sweep setting**, default off.
- The pilot (step 10) can run on either lookup. David has not decided whether it uses the ChessDB-augmented one.

**Decisions still open for David:**
- whether the construction policy should be OTB-dominated where OTB data is plentiful;
- whether a permissive-arm winner (which re-admits the Blackmar-Diemer) needs a fresh best-defence pass;
- whether an empty menu should end the book;
- the step-2 aggregation rule;
- the go-ahead for the big-three pool exports.

---

## 7. Rules and lessons that still apply

**Do not touch:**
- `lf_chain.bat`, `lf_chain2.bat`, `rare-verify-v1/black-030-horizon-v2`;
- the paused "Finish the Black 0.30 pilot" automation;
- the unrelated dashboard-copy session, Allie, the canonical/lifetime books;
- the old pin `D:/chess/bin/ply51-evals-rehearsed-20261115`;
- `H:\chess\eval-work-p51-20261115-sample` and the verifier receipt and checkpoint (unless David says so).

Keep the top-level ops status "training explorer verified".

**General rules:**
- **Switch rule** for any book swap: ≥ .005 over 4 paired seeds, beyond bootstrap uncertainty, and no commonly
  reached move over the soundness limit on fresh SF (SF18).
- **Before any shutdown:** stop coordinators before their children, keep every receipt, and record the interrupted
  stage. Never fabricate statuses.
- **Course text is copyrighted:** keep it local. David's email address is private.

**WSL:**
- Call `wsl` from PowerShell, not Git Bash, because MSYS mangles paths.
- The `/mnt/chessdb-ro` bind is lost when WSL idles. `scratch/chessdb_import/cpp/mount.sh` restores it, and the
  tools call it first.
- `pkill -f <pattern>` inside `bash -c` matches and kills its own shell; use `pkill -x` or a `[x]` bracket.
- The WSL VM's file cache counts against Windows commit, hence the trim loop.

**Engines:** never switch the Stockfish binary without re-measuring the cp scale against the eval DB's sources
(`eval_panel.py --engine`, `sf_scale_check.py`).

**ChessDB scores:**
- side-to-move view, ChessDB's backprop scale;
- |s| ≥ 15000 is a mate or tablebase claim, and the −30001 child code means 0;
- map with `eval_lookup_cdb.chessdb_white_cp` before comparing with `eval_cp`;
- cloud values in `cloud_moves` are White's view.

**DuckDB in-process decay** (CLAUDE.md) bit the lookup twice; give heavy per-bucket queries their own process.
Every scan-path stage already does.

**The cloud dataset is far bigger than the plan assumed:** 15–24M distinct FENs per file, 20 files. Process it per
file.
