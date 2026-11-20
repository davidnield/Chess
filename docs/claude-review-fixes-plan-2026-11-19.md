# Plan: fixing the 19 November review findings

*Claude, 19 November 2026, evening.*

The findings are in `docs/claude-review-of-codex-2026-11-19.md`. The work happens on branch
`claude/review-fixes-20261119` (worktree `scratch/worktrees/review-fixes-1119`), which starts at
Codex's `2ad11c8`. Nothing is merged, pushed, rebuilt in Rust, or run on production data.

**Tonight's limit:** tests under 20 minutes of machine time. Anything heavier (a real-graph request
census, a polish A/B on the referee) is listed for tomorrow.

## Tasks

| # | finding | change | test tonight | status |
|---|---|---|---|---|
| 1 | Black solver dropped polish | `spec.FIXED`: `match_inflate` back to 1.5 (the validated value); Black solver label `dp+match-distinct+polish`; `builder.command` adds `--polish` for Black. New manifest `v3`, because every arm ID changes. | spec, builder and manifest tests | done |
| 2 | Parent eval above every child empties the menu, with no request | `sweep.candidates.requests`: a `parent-above-children` request whenever the parent's own-side score exceeds the best evaluated child by more than the band and the parent has no 5M+ verification. The search then nominates the engine's best move, which joins the menu as an engine candidate next round. The inconsistency check becomes symmetric. Gate semantics are unchanged (no Rust rebuild). | the reproduction as a regression test, both colours | done |
| 3 | Quality frontier unbounded | Requests are computed after the book is chosen, from the book's **policy reach** (booked positions and the positions where it stops), not the whole graph's optimistic reach. They are kept above `--quality-min-reach`, ranked by reach and capped at `--quality-max-parents` per round. The meta records what was requested, deferred beyond the cap, below the threshold, and off-policy. Recipe fields: `verification.min_reach`, `verification.max_parents`. | unit tests on the selection and accounting; a builder fixture | done |
| 4 | Unverified rare reference cuts a rival at an unbooked node | `requests`: an `unverified-rare-reference` request when the best-scoring child has fewer than `rare_trust` games and no 5M+ verification, and some move it cuts would pass against the best trusted child. Totals are carried in the quality edges. | regression test | done |
| 5 | Recipe labels not bound to data | `coordinator.preflight` checks each arm's bound files against the recipe: the candidate pool's `population`/`max_ply`/`min_games`/`key_bits` against `pool_recipes()[candidate_pool]`; the weak pool against `weak1000-1799-p{max_ply}`; for a selective policy, its sources against `brc-all`, `brc1800`, `rapid2200`, `classical1800` at `p{max_ply}`; for a control overlay, its source pool against `brc{rating}-p{max_ply}`. The builder refuses `--max-ply` above the pool's recorded cap. `build_weak_edges.py` requires `--book` and `--max-ply`, filters `ply <= max_ply`, refuses a 128-bit book (it joins on one key half) and writes a meta. | preflight refusal tests for each counterexample; the builder cap refusal | done |
| 6 | Ensemble constants hardcoded | `policy.ensemble` takes `empirical_strength`, `maia_strength`, `backup_discount` and `broad_discount` from the recipe; `frontier` passes them. Defaults equal today's numbers, so output is unchanged. | existing ensemble tests plus a changed-constant test | done |
| 7 | Endings-only positions lose their replies; model-only replies are uncounted | `frontier.rows`: when the outcome pool has no continuation but endings exist and another source observed moves, use one pseudo-continuation rather than zero. Count model-only replies (no observation in any source) and their probability mass in the frontier stats. The builder meta reports the reach of model-only replies whose child has no eval. | unit tests | done |
| 8 | Documentation | Review doc status table; CLAUDE.md sweep notes; the sweep progress log | – | done |

## Not changed (decisions, or tomorrow's work)

- **The OTB-dominant construction policy, the permissive arm and the empty-menu stop rule** are
  modeling decisions for David. They are written up in the review and unchanged here.
- **Lookahead stays off** in the manifest. The proposed final best-reply audit is a design
  proposal, not part of this change.
- **Tomorrow's checks, each needing more than 20 minutes:**
  - the real-graph request census under the new cap;
  - the polish on/off referee A/B on one Black book;
  - a full `run_tests.py`.

## Progress log

- **Tasks 1–7 done** in `scratch/worktrees/review-fixes-1119` (committed on 19 November: see the commit log).
- **Two refinements made while testing:**
  - The control-policy check now requires the overlay to declare `reply_pool`, because a
    schema-2 overlay's `source_pool` is the outcome pool, which must equal `--stats`.
  - The rare-reference rule measures "would pass" against the trusted children only, because the
    parent's stored score often agrees with an inflated thin child (the 10 November pattern).
- **Extra item:** the selective policy cache identity moved to implementation 3, so cached
  answers made under the old continuation-mass rule are not reused.
- **Builder cap refusal:** applies to full-key pools. Historical 64-bit recipes (`--max-ply 40`
  over ply-30 pools) only warn, so the October books stay reproducible.
- **Manifest:** `docs/sweep-manifest-2026-11-19-maia-v3.json` is the current one. All arm IDs
  changed.
- **Tests run** (under 2 minutes of machine time in all):
  - new: `_test_review_fixes_1119.py`, 9 tests;
  - `_test_maia_cache`, `_test_sweep_preparation`, `_test_sweep_coordinator` (with the new
    binding refusals), `_test_full_key_book` (with the cap refusal), `_test_sweep_execution`,
    `_test_horizon_lookahead`, `_test_expected_score_gates` and `_test_full_key_verification`:
    51 passed, 1 opt-in skip;
  - script tests: `_test_rare_verify`, `_test_book_builder_rs`, `_test_budget_eval_loss`,
    `_test_opponent_prior`, `_test_lookahead_floor`, `_test_pool128`, `_test_docs_paths`: all
    pass.
- **Still for tomorrow** (over 20 minutes each, or production-data steps):
  1. the round-0 request census on a real Black graph with the new scope;
  2. the polish on/off referee A/B;
  3. a full `run_tests.py`.
- **Before the opponent-policy sensitivity runs** (not needed for the Black pilot or the 20 main
  arms): the generator for the `brc1800-maia30` and `brc2200-maia30` control overlays must write
  `reply_pool` into each overlay's metadata. Neither the generator nor the overlays exist yet.
  Until then, preflight refuses those 8 arms ("does not declare its reply_pool"), by design.
