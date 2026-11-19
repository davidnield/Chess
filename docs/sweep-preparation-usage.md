# Sweep preparation implementation

Branch: codex/sweep-preparation-20261118. Worktree: C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation.

This branch integrates codex/full-key-consumers (34ace88) with lookahead-floor (4d7c945). The running migration and Allie investigations use their existing pinned snapshots. Nothing in this preparation launches a repertoire sweep or replaces a canonical book.

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

    C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.spec --out ../docs/sweep-manifest-2026-11-18.json

This refuses to overwrite a different manifest. Repeating the same recipe is harmless.

    C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -m sweep.prepare --manifest ../docs/sweep-manifest-2026-11-18.json --workspace D:/chess/sweep-inputs

With no source bindings this prints a waiting-for-publication report and starts nothing.

## Prepare a shared pool after publication

A bindings JSON supplies code (this worktree or its immutable deployment snapshot), python, explorer, eval_lookup and artifacts. Each artifact is a path plus its SHA-256. Pin the published explorer _book.meta.json and matching lookup.json, their verification receipts, and any other source identity needed for the deployment. The tool adds hashes of its Python implementation.

Use --pool brc1800-p30 and --bindings PATH to inspect one concrete command. Only adding --execute runs it. The work directory is persistent: interrupted arrivals/buckets resume through pool_from_book.py. A failed process or validator never writes a successful receipt. Resume checks the recipe and every listed artifact. Outputs are restricted to the designated workspace.

The validator runs the existing term-vs-book, reimplementation, conservation and evaluation gates on their documented sampled buckets, with low concurrency. These sampled cross-checks supplement the per-bucket producer checks; they are not described as an independent census. The accepted gate report is part of the receipt.

Construction bands are archive game-mean bands. The lowest archived band is 0, not the explorer UI label 400. Mover-rating evaluation panels and the historical weak policy must be bound separately and must not be inferred from these construction exports.

## Remaining work before production use

1. Bind the published explorer/eval generations and run a small accepted pool export to measure throughput/storage before the shared exports.
2. Freeze the deduplicated OTB training/evaluation split and materialize the on-demand reached-position ensemble inputs. The formula, cache and overlay consumer are implemented; the selective count/model adapter is connected; its production paths and performance still need a pilot. Never fall back silently to the old reply pool.
3. Pilot the implemented reached-candidate discovery and joint verification/rebuild coordinator. Retained-graph correction is not implemented; full rebuilding remains available and its cost must be measured.
4. Run the Black stricter 2,000-position pilot. Require full-key identity, complete menus, conserved mass, gate/lookahead audit as applicable, quality closure, no evidence downgrade, and clean-rebuild agreement before the remaining primary base books.
5. Calibrate the implemented continuation driver and resolve the metric issue described in the progress log. Add hybrid screening and measure engine/policy costs before large runs; finish extended-book/ladder publication.
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

The default metric remains the approved covered maximum. --metric remaining-maximum enables the proposed alternative described in the progress log; use a separate output directory for a different metric. The complete engine/policy history is retained in checkpoint.json and its hash appears in the report. The adapter is deliberately not a production speed claim: its independent 20M MultiPV top-three searches still need hybrid screening and cost calibration. A bounded result creates extensions.json, preserving base and additional prescriptions separately; automatic PGN/ladder publication is not wired in.

Both entrypoints are ready for controlled pilots, not an instruction to launch the production matrix now.
