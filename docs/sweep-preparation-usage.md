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
- python/sweep/extensions.py implements the approved severity, expected maximum exposure, distinct-prescription/connector cost, and conservative unresolved-frontier bound. This is accounting, not the production continuation-search driver. It does not replace extend_book.py yet.
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
2. Freeze the deduplicated OTB training/evaluation split and materialize the on-demand reached-position ensemble inputs. The formula, cache and overlay consumer are implemented; the production count/model adapter and selective refresh coordinator still need connecting. Never fall back silently to the old reply pool.
3. Complete uncertain unselected-candidate discovery and the joint verification/correction coordinator. Selected-prescription quality review and request deduplication are implemented. Retained-graph correction is not implemented; full rebuilding remains available and its cost must be measured.
4. Run the Black stricter 2,000-position pilot. Require full-key identity, complete menus, conserved mass, gate/lookahead audit as applicable, quality closure, no evidence downgrade, and clean-rebuild agreement before the remaining primary base books.
5. Finish the unlimited production continuation expansion/package selection driver. It must supply disjoint history-aware routes, verified lower-ranked moves, gate-compliant prescriptions, complete policy fallbacks and a valid unresolved-risk bound. The tested accounting kernel alone cannot certify a finished extended book.
6. Bind the evaluator to the validated model/backend and population panels; connect training profiles and common leaf scoring. Build route-aware ECO annotation/report exports from a frozen opening-name source. Allie adoption remains a separate evidence decision.

No claim of incremental speedup, extension completeness, or production book quality is made from the fixture results.

## Validation

The native release build uses D:/rust-target/sweep-preparation, with two build jobs. It does not replace other worktrees' extensions. Cargo's five tests and the native/Python differential suite pass, including 252,000 gate-menu cases, 800 reach graphs and 1,000 assembled graph cases.

The combined targeted regression command runs _test_sweep_preparation, _test_horizon_lookahead, _test_full_key_book and _test_full_key_verification. It passed 25 tests with one optional real-Stockfish smoke skipped. Separate expected-score gate, rare-verification, opponent-prior and lookahead reference suites pass. Tests include nonempty full-key veto receipts, independent empty-source builds, hard parent restrictions, mate outcomes, score-boundary reviews, cache identity, stale receipt rejection and count-independent evidence decisions.

Temporary test logs are in scratch/preparation-tests. Production-scale performance and final input coverage remain unmeasured.
