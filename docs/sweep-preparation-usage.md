# Sweep preparation implementation

Branch: codex/sweep-preparation-20261118. Worktree: C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation.

This branch integrates codex/full-key-consumers (34ace88) with lookahead-floor (4d7c945). Production work uses immutable deployment snapshots. The November 19 recovery uses its separately pinned verifier and resumes the original eval launcher after verification; the Allie investigations have finished. Nothing in this preparation launches a repertoire sweep or replaces a canonical book.

## What is usable now

- The native builder combines full-key I/O, expected-score gates, route-horizon corrections, complete source-move collection and auditable lookahead. Lookahead proofs and veto lists persist both hash halves. Internal run-local IDs are never published as position identities.
- python/sweep/spec.py generates the 20 primary arms and 20 staged follow-up arms. It fixes White-view baselines, score gates, min_games=1, weight, solver settings and dependency identities. The 2,000-position master is counted once per gate/color. Reduced 1,000 comes from that master, then reduced 500 comes from the reduced 1,000.
- python/sweep/builder.py writes an independent source header with no inherited prescriptions and produces explicit builder commands from bound inputs. This avoids using a legacy choice menu to seed independent arms. The full-key end-to-end fixture tests the empty header.
- --allowed-book is a hard restriction after engine admission, before lookahead and optimization. --incumbent remains a separate soft preference. The parent is hashed; resume rejects a changed parent. Gates still apply, so a changed engine generation can require a new master rather than silently preserving a failing move.
- --verification-policy quality uses stored scores provisionally without a frequency-based trust cutoff. Final prescribed moves require independent evidence; moves within 0.01 score of a boundary require the deeper tier. Legacy frequency verification remains available for historical reproducibility; --rare-trust-floor separates that legacy threshold from inclusion.
- Schema-3 opponent overlays contain a complete legal probability vector from the approved OTB ensemble. They preserve empirical outcome counts and real nonmove mass. --opponent-pseudogames must be zero for these overlays. The publisher and reader replay every move and check its child key, pool/aux receipts and normalization. Controls retain the legacy schema-2 Maia-30 policy.
- python/sweep/quality.py unions requested comparison menus across arms, distinguishes both hash halves, and preserves deeper compatible evidence. The existing verifier's base/deep caches remain the actual engine-search implementation.
- python/sweep/extensions.py implements the approved severity, expected maximum exposure, distinct-prescription/connector cost, and conservative unresolved-frontier bound. The November 19 continuation driver uses this accounting; see the new execution commands below.
- python/sweep/rollout.py supports full-key books, arbitrary common horizons, actual histories, paired random draws, per-position recall, and exact terminal outcomes through batched model/leaf callbacks.
- python/sweep/recall.py supports arbitrary book sizes, full-key cards, equal practice budgets, low/central/high retention scenarios, exact supplied familiarity, and optional game retrievals. These are scenario assumptions, not measured learner performance.
- python/sweep/reports.py checks ladder inheritance, constructs evaluator identities/common horizons, and reports exact-move stability with eligible/reachable/unknown denominators. Reduced copies do not count as independent votes.

The manifest preserves the builder's current lookahead default (off). The integrated implementation is tested with lookahead on as well. Changing the production lookahead setting requires a versioned recipe; it is not silently added as another experimental difference. Weak-policy fallback at 20 observations and the existing solver controls are explicitly frozen, not newly tuned.

## Generate and inspect the manifest

From this worktree's python directory:

    C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.spec --out ../docs/sweep-manifest-2026-11-21-v5.json

This refuses to overwrite a different manifest. Repeating the same recipe is harmless. v3 is current (it adds the Black polish and the quality-request scope). Its predecessor v2 corrected the four horizon comparisons to independent 2,000-position books paired with their matching ply-30 masters. Weight/policy sensitivities remain at 1,000. The older Maia manifest is archived unchanged; its incorrect horizon design now fails readiness validation. No old output directory should be relabelled or resumed as a corrected horizon arm.

    C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.prepare --manifest ../docs/sweep-manifest-2026-11-21-v5.json --workspace D:/chess/sweep-inputs

With no source bindings this prints a waiting-for-publication report and starts nothing.

## Prepare a shared pool after publication

A bindings JSON supplies code (this worktree or its immutable deployment snapshot), python, explorer, eval_lookup and artifacts, plus publications={explorer: COMPLETED_STATUS_PATH, eval_lookup: COMPLETED_STATUS_PATH}. Each artifact is a path plus its SHA-256. Pin the published explorer _book.meta.json and matching lookup.json, their verification receipts, and any other source identity needed for the deployment. The bindings validator requires completed status values, matching source generations, terminal corrections and all 512 lookup bucket entries. Include the eval DB _build.meta.json and the explorer recovery verification_receipt when present among the pinned artifacts. These checks do not rehash the whole multi-terabyte dataset; the producer/verification receipts and lookup manifests carry that identity. The tool adds hashes of its Python implementation.

Use --pool brc1800-p30 and --bindings PATH to inspect one concrete command. Only adding --execute runs it. The work directory is persistent: interrupted arrivals/buckets resume through pool_from_book.py. A failed process or validator never writes a successful receipt. Resume checks the recipe and every listed artifact. Outputs are restricted to the designated workspace.

The validator runs the existing term-vs-book, reimplementation, conservation and evaluation gates on their documented sampled buckets, with low concurrency. These sampled cross-checks supplement the per-bucket producer checks; they are not described as an independent census. The accepted gate report is part of the receipt.

Construction bands are archive game-mean bands. The lowest archived band is 0, not the explorer UI label 400. Mover-rating evaluation panels and the historical weak policy must be bound separately and must not be inferred from these construction exports.

## Remaining work before production use

1. Bind the published explorer/eval generations and run a small accepted pool export to measure throughput/storage before the shared exports.
2. Freeze the deduplicated OTB training/evaluation split and materialize the on-demand reached-position ensemble inputs. The formula, cache and overlay consumer are implemented; the selective count/model adapter is connected; its production paths and performance still need a pilot. Never fall back silently to the old reply pool.
3. Pilot the implemented reached-candidate discovery and joint verification/rebuild coordinator. Retained-graph correction is not implemented; full rebuilding remains available and its cost must be measured.
4. Run the Black stricter 2,000-position pilot. Require full-key identity, complete menus, conserved mass, gate/lookahead audit as applicable, quality closure, no evidence downgrade, and clean-rebuild agreement before the remaining primary base books.
5. Calibrate the implemented continuation driver with the approved remaining-maximum default and run the queued old-metric comparison. Add hybrid screening and measure engine/policy costs before large runs; finish extended-book/ladder publication.
6. Bind the evaluator to the validated model/backend and population panels; connect training profiles and common leaf scoring. Build route-aware ECO annotation/report exports from a frozen opening-name source. Allie adoption remains a separate evidence decision.

No claim of incremental speedup, extension completeness, or production book quality is made from the fixture results.

## Validation

The native release build uses D:/rust-target/sweep-preparation, with two build jobs. It does not replace other worktrees' extensions. Cargo's five tests and the native/Python differential suite pass, including 252,000 gate-menu cases, 800 reach graphs and 1,000 assembled graph cases.

The combined targeted regression command runs _test_sweep_preparation, _test_horizon_lookahead, _test_full_key_book and _test_full_key_verification. It ran 25 tests: 24 passed and one optional real-Stockfish smoke was skipped. Separate expected-score gate, rare-verification, opponent-prior and lookahead reference suites pass. Tests include nonempty full-key veto receipts, independent empty-source builds, hard parent restrictions, mate outcomes, score-boundary reviews, cache identity, stale receipt rejection and count-independent evidence decisions.

Temporary test logs are in scratch/preparation-tests. Production-scale performance and final input coverage remain unmeasured.


## Selective OTB configuration and builder use

`--opponent-policy-config CONFIG.json` is an alternative to a static `--opponent-prior`. It requires full keys, min_games=1, the normal aux sidecar, and zero additional pseudogames.

The config has schema=1; recipe exactly equal to `sweep.spec.POLICY`; sources mapping broad/adult/rapid/classical/otb to frozen uncut full-key pooled Parquets; aux equal to the adult pool's aux; cache naming a shared SQLite file; model containing python and checkpoint; and artifacts listing path/SHA-256 for every source, aux, checkpoint and model Python executable. Model timeout_seconds is optional (600 by default). Adult must equal the builder's outcome stats. Rapid and Classical are disjoint targeted components; the per-move vectors enforce broad >= adult >= targeted.

The cache also fingerprints installed Maia source files and runtime versions without loading the model. The actual server is lazy. Each book freezes `PREFIX.opponent.parquet` and its `.json` sidecar, including materialization/query/model timing and support counts. The resumed build must retain these files and the policy config. Never substitute a different source generation into an existing cache recipe.

## Joint verification and construction

From the worktree's python directory:

    C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.verify --books BOOK_LIST.json --cache SEARCH_CACHE --out REQUESTS.json --plan-only

BOOK_LIST is a JSON list of objects with book and colour. Plans include selected prescriptions plus saved reached-candidate quality requests. Remove --plan-only and use a new Parquet output path to execute one shared verification pass; pass --prior for the common evidence actually applied to all books. Never treat that pass alone as a finished comparison: changed evidence requires rebuilding all affected books.

    C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.coordinator --config JOINT.json --out RUN_DIRECTORY

This is readiness-only unless --execute is added. The config contains schema=1, arms=[{arm: MANIFEST_ARM, inputs: {...}}], artifacts, searches, optional seed_overlay, optional python/threads, and a pinned pilot_report before any nonpilot run. Inputs bind source, stats, aux, weak and exactly one of opponent_policy_config/opponent_overlay. Pin the bound files, existing metadata sidecars, policy source artifacts, lookup.json, the builder Python executable and Stockfish executable. Engine evidence is a single shared seed_overlay. Reduced arms require their dependency in the same epoch and automatically use that epoch's parent book. Independently verified engine choices absent from the counts are added as shared nominations for the next epoch; empirical counts remain unchanged.

The coordinator records immutable code snapshots, build/verify/audit receipts, rounds.json, status.json and final.json. Its default eight-round cap is an operational work limit; increasing --max-rounds resumes the same recipe. A recipe/code change requires a new output directory, but compatible position-search and prediction caches remain reusable. A quality-closed final.json is not the pilot acceptance report and does not authorize the main matrix by itself.

## Bounded work on unlimited extensions

    C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.extend --book CLOSED_BOOK.parquet --policy-config CONFIG.json --engine-cache CACHE --out EXTENSION_RUN

Default is read-only readiness. --execute --steps 1000 performs bounded work, saves checkpoint.json, and reports whether the unexplored exposure is bounded below the threshold. Run the same command again to continue. No book-depth limit or nonzero opponent-move cutoff is introduced by --steps. --allowed-book constrains the prescribed moves to the extended master; an uncovered required position is reported as a master hole.

The default metric is remaining-maximum, approved November 19. --metric covered-maximum runs the earlier metric as a comparison control; use a separate output directory for each metric and threshold. The November 19 manifest queues both metrics on all four 2,000-position anchors at all three calibration thresholds (24 extension variants, reusing the same four base books). This deferred stage waits for verified inputs, an accepted Black pilot, closed anchors and the extension cost pilot. Use shared evidence/caches and a common continuation union for comparison. The November 18 manifest remains the historical record. The complete engine/policy history is retained in checkpoint.json and its hash appears in the report. The adapter is deliberately not a production speed claim: its independent 20M MultiPV top-three searches still need hybrid screening and cost calibration. A bounded result creates extensions.json, preserving base and additional prescriptions separately; automatic PGN/ladder publication is not wired in.

`--severity engine-or-player` (2026-11-21) scores each analysed own position as max(engine gaps, our players' expected loss). The player counts come from the candidate pool's explorer book, read by full key (`sweep/own_moves.py`: the recipe's `own_policy` population, every ply, at least `weak_min_games` games). Moves the engine ranked keep its 20M scores, other moves are scored from the eval lookup, and unevaluated moves count as no loss. The player term can only raise severity: the data understates our loss inside a sequence, because players there mostly chose the tactic themselves. The default `engine` is an exact no-op. The v5 manifest queues the variant at the default metric on the four anchors and three thresholds (12 runs, role `player-severity`); its control is the matching `default-calibration` run. The report's `player_severity` block counts positions analysed, positions with enough games and positions the term raised.

Both entrypoints are ready for controlled pilots, not an instruction to launch the production matrix now.


## Selective lookup benchmark and model evidence

From the Python directory, run `python -m sweep.benchmark_counts --source POOL.parquet --out RESULT.json`. This compares exact full-key results with a complete-file join and records row-group selection and timings. It does not rebuild a pool. Defaults are 16 requested keys and three alternating-order repetitions. Results are OS-warm; measure production batches before deriving an ETA.

The selective count adapter now prunes row groups using existing low-half statistics, performs the final join on both halves, and retains groups whose statistics are missing. Frozen overlay sidecars include count_lookup metrics. The real Maia adapter/checkpoint smoke and microbenchmarks are in `docs/sweep-runtime-checks-2026-11-19.json`.

Allie backend/history results and frozen evidence paths are in `docs/sweep-model-evidence-2026-11-19.json`; interpretation and limitations are in the third-pass progress entry. Rust passed the backend quality screen, but modal-history substitution has not passed equivalence. This does not change the calibrated Maia construction prior.


## OTB split preflight after a corpus refresh

Run from the pinned code's python directory:

    python -m sweep.audit_otb_split --database D:/data/chess/tournament-games-deduplicated/tournament-games-deduplicated.duckdb --source-manifest D:/data/chess/tournament-games-deduplicated/manifest.json --historical-games C:/Users/David/Documents/Chess/outputs/otb-u2000-2026-11-13/games.parquet --out D:/chess/sweep-inputs-20261119/otb-audit

This is a read-only corpus audit. Outputs are provisional identity candidates, a quarantine list and a hashed diagnostic report; they are **not training counts or an accepted split receipt**. Reuse checks both source generation and output hashes. A changed source/code requires another output directory. The November 19 run found 4,690 current families crossing the old test/non-test boundary. Complete ancestry reconciliation before freezing and replaying training inputs. Preserve old assignments; do not rehash changed numeric game IDs and silently reshuffle the holdout.

Shared-pool finalization now externally sorts with the configured DuckDB budget/spill directory and streams Arrow batches. It no longer requires enough RAM for the entire pool plus sort. Ensure adequate temporary disk space before a production export; the bounded smoke is not a full-size capacity forecast.


## Freeze and verify the corrected OTB split (November 19)

The representative audit above is diagnostic. `sweep.freeze_otb` follows **every archived source record** in each historical game to its current deduplicated identity. The archived membership uses zero-based `rid`; published record IDs use `rid + 1`. Before accepting ancestry, the freezer verifies the historical source-prefix fingerprint, archived identity-code/shard stamp, every old representative anchor, complete source-record membership and unchanged source locators. The source corpus is opened read-only.

From this worktree's Python directory:

```powershell
python -m sweep.freeze_otb `
  --database D:/data/chess/tournament-games-deduplicated/tournament-games-deduplicated.duckdb `
  --source-manifest D:/data/chess/tournament-games-deduplicated/manifest.json `
  --historical-games C:/Users/David/Documents/Chess/outputs/otb-u2000-2026-11-13/games.parquet `
  --membership D:/data/chess/tournament-games-deduplicated/_work/membership.parquet `
  --index D:/data/chess/tournament-games-deduplicated/_work/index.duckdb `
  --shards D:/data/chess/tournament-games-deduplicated/_work/shards.json `
  --identity-code D:/data/chess/tournament-games-deduplicated/rebuild/tournament_identity.py `
  --identity-stamp D:/data/chess/tournament-games-deduplicated/_work/identity-version.txt `
  --as-of 2026-11-19 `
  --out D:/chess/sweep-inputs-20261119/otb-v2-20261119

python -m sweep.verify_otb_split `
  --split D:/chess/sweep-inputs-20261119/otb-v2-20261119/split.json
```

Historical folds 2/3/4 remain training, 1 validation, and 0 test. A current game combining different historical roles is quarantined. All descendants connected through a historical game's split/merge ancestry inherit the same role, including quarantine. A merge of two training folds stays training. Only a genuinely new eligible family gets a new deterministic fold; its assignment must be preserved during future migrations.

`games.parquet` freezes the **current resolved movetext**, ratings and rating-system labels, standard starting position, dates, source and assignments. It selects eligible standard-start games dated 2013-01-01 through the explicit as-of date, with positive ratings for both players and at least one player below 2000, under the corpus's existing `opening_analysis` clock screen. This keeps both ratings for replay: the OTB under-2000 construction counts must select **the moving player's rating below 2000**, not count both players merely because one qualifies. Unknown clocks and mixed published rating systems remain disclosed limitations.

`ancestry.parquet`, `lineage.parquet` and `historical-assignments.parquet` retain the reconciliation evidence, including old games outside the current date eligibility. `split.json` pins the source generation, code and outputs. A changed input/code cannot reuse a completed freeze directory. The verifier archives the small provenance inputs under `frozen-inputs/`, rechecks the complete ancestral-record census and partition rules from exported files, checks output hashes, and smoke-replays a deterministic sample through 50 plies. `verified.json` pins those results. Neither receipt asserts that all game scores were legally replayed: both say `training_counts_ready: false`.

The next producer must replay the frozen texts into full 128-bit position/move counts, separate training from validation/test, exclude quarantine, handle leading comment-only PGN preambles without losing the actual game, report malformed games/prefixes explicitly, retain mover-rating and ply eligibility, and check conservation before publishing counts. Production policy bindings must reference those verified training-count outputs, never this game's table or the old diagnostic candidates. Recheck close OTB prediction comparisons on the reconciled partitions; these previously inspected partitions are development/test partitions, not a new untouched confirmation set.


## Full 128-bit OTB counts (November 19)

The corrected production run uses immutable code in `D:/chess/bin/otb-counts-20261119/` and outputs under `D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119/`. The earlier `otb-counts-v1-20261119` directory was stopped before publication because a stored-file test found native string padding; it is not an accepted source. The `otb-counts-pilot-20261119` directory is a 4,096-game engineering fixture, also not a production source.

The hidden launcher `D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119/launch.cmd` runs replay/export followed by independent verification and propagates failure. Read `status.json` and `run.log`. Restart the same launcher to resume an unfinished run; do not use it to start a competing worker. A completion claim requires `verified.json` with `training_counts_ready: true` and matching artifact hashes, not merely the presence of Parquet files.

For explicit stage execution, from the pinned release's `python` directory:

```powershell
C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.otb_counts `
  --split-verified D:/chess/sweep-inputs-20261119/otb-v2-20261119/verified.json `
  --dll D:/chess/bin/otb-counts-20261119/otb_counts_replay.dll `
  --out D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119 --phase all

C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.verify_otb_counts `
  --root D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119
```

`--phase replay` and `--phase aggregate` are available separately. Replay commits each 4,096-game batch with hashes, allowing an unfinished batch to be redone without losing completed ones. Changed source/code/binary hashes require another output directory. Replay uses a separate Rust library pinned to shakmaty 0.30.1 and the repository's python-chess-compatible SAN parser. It does not modify or relaunch the explorer/eval executable. DuckDB export/verification uses two threads, an 8 GB tracked-memory budget, disk spill and a 150 GB free-space check before work; these are not a hard OS working-set limit.

For each of `training`, `validation`, and `test`, `counts/` contains:

- `ROLE-by-ply-band.parquet`: exact counts by full parent and child keys, canonical SAN/UCI, parent EPD, **played ply 1–51**, and mover rating band (positive below 1500, 1500–1799, 1800–1999).
- `ROLE-p30.parquet`: summed over qualifying mover bands and played plies 1–30, one row per position/move.
- `ROLE-p50.parquet`: the corresponding sum through played ply 50.

The extra 51st-ply records remain in the master table. Build other depth cuts and unions of those rating bands from the master, not by filtering the minimum-ply field of an already pooled table. A new threshold inside a stored band (for example 1600) requires reaggregating the audit ledger by its exact mover_elo; it does not require replaying the PGNs again. `parent_hash` and `child_hash` are signed `struct<lo: int64, hi: int64>` values; **every join uses both halves**. Parent EPD preserves pseudo-legal en-passant state consistently with the Polyglot/shakmaty key convention. Count every observed move, including frequency one; there is no Other Moves bucket. Repeated visits within a game remain separate observed decisions, distinguished by ply in the audit ledger.

Only `training-p30.parquet` (or the explicitly selected depth's training export) supplies the OTB component of the construction policy. The policy adapter consumes parent/child keys, canonical SAN and total counts. These files contain **move observations**, not invented win/draw/loss results, engine evaluations or terminal/horizon masses; the existing outcome pool still supplies those quantities. Validation and test exports remain separate for prediction checks. Retain the frozen game's rating-system labels and clock-screen limitations.

`chunks/*.edges.parquet` keeps game IDs, both key halves, ply, actual mover rating and full move/position text for auditing. Both players' valid moves are retained there for complete game-flow checks; publication filters the moving player's rating to 1–1999. Consequently its incidental 2000+ opponent moves are not a representative stronger-OTB population. `chunks/*.games.parquet` gives every frozen row a disposition. Quarantined games are never replayed. A syntax, illegal-move, null-move or empty-score error within the requested prefix excludes that entire game's prefix and is counted explicitly; an error after ply 51 does not affect this prefix. Such exclusions are not game endings.

The verifier checks every frozen game's identity/role/disposition, every retained ply and full-key chain, actual mover-rating selection, and **every aggregate count** against the game ledger. It also checks pooled move uniqueness and full-key/EPD consistency, compares sampled legal replays with independent Python chess, and records native/Python crosschecks performed throughout extraction. A complete aggregation check is not a claim that a second chess implementation replayed every source game. `verified.json` pins final outputs, input/code provenance, counts, exclusions and sample coverage. Keep the game ledger for game/source-clustered checks and additional verification. Player/event clustering additionally requires an identity-aligned, frozen player/event mapping; those fields are not present in this count ledger.


After full acceptance, the current worktree also provides `python -m sweep.check_otb_consumer --root D:/chess/sweep-inputs-20261119/otb-counts-v2-20261119` (run from its `python` directory). This additional check verifies exact signed 128-bit key and integer-count schemas on all nine files, then uses the real selective policy reader on all six pooled exports. It checks legal SAN/UCI and full child keys, includes sampled later positions, and rejects a deliberately wrong high half paired with the starting position's real low half. It writes `consumer-check.json` only after the count receipt is accepted. The runtime versions and release manifest are recorded separately in `environment.json`.
