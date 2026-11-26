# Repertoire model review and implementation plan

Date: November 25, 2026. Prepared for David and the next implementation session.

The overall approach remains appropriate: choose a budgeted repertoire by backwards induction, model human replies, value our weaker play after preparation ends, and enforce independent engine constraints. The immediate problem is inconsistent implementation across those components. Finish the model and evaluation contracts before spending time on the full tuning matrix. Keep the approved opponent ensemble, engine-source ensemble, gate settings, Rust optimizers and reusable data work.

This review covers construction, reply prediction, stop values, rollout evaluation, source evaluations, verification, input bindings, horizon handling, extensions and the queued tuning work. It compares the maintained implementation with today's explanations. It does not certify every project subsystem or reassess the trainer, puzzle miner or unrelated crawler.

Reviewed baseline: `7076faa149ecefa509a71854e3e81b28b1f5fbb0`, branch `codex/takeover-hardening-20261122`, worktree `C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation`. New reproductions and this plan do not change production behavior. Numerical examples below are controlled fixtures, not measured repertoire performance.

## Decisions to preserve

| Component | Current decision and recommendation |
|---|---|
| Main construction | Budget counts distinct prescribed positions; position-only model with full 128-bit identity; observed own-move menu from BRC 1800+ at min-games 1, plus shared engine nominations. |
| Main opponent policy | Keep `otb-ensemble-v2`, Maia-3 79M position-only, ratings 2600/2600, temperature 1.35, empirical strengths 30/30, Maia strength 3, OTB strength 10, online support gate 300. These Maia ratings are calibrated prediction settings. |
| Our intended play after book | Retain the weaker-player objective. Replace the unconditional threshold fallback; calibrate its new prior separately from the opponent model. |
| Engine-source policy | Eligible Cloud / NNUE Fishnet / ChessDB = 30/30/40 in White expected-score space. No Cloud plus available ChessDB uses ChessDB alone. Other missing-source cases retain the pinned accepted step-1 lookup behavior. No universal weight renormalization. |
| Engine evidence | Pinned Stockfish 18; qualified independent evidence overrides cached sources. Preserve stronger searches, complete comparison menus, and independent verification of final prescriptions. No further broad source-weight search. |
| First production pilot | Black, stricter score gates, 2,000 prescribed positions, ply-30 construction; paired evaluation-source comparison with the same corrected human models and incumbent Black optimizer. This is the current stricter-score pilot, not a return to the historical 0.30-pawn relative gate. |
| Optimizer work | Existing DP/matching/polish and price/polish remain controls. Native portfolio, exact bounded references, joint neighborhoods and curve-cap sensitivities are implemented but still need real-data comparison. |
| Main tuning matrix | Both gate arms, budgets 500/1,000/2,000, independent versus fixed-prescription reductions; extensions and choice-stability reports remain in the consolidated tuning plan. |
| Extensions | Keep remaining-maximum risk, unlimited sequence length, reach times engine-loss severity, connectors charged, and the queued covered-maximum comparison. Historical ease must not reduce the approved engine severity. |
| Runtime | Implement new numerical policy, continuation, bounds and simulation work in Rust. Python may remain the I/O/launch layer and the existing neural inference bridge; reference implementations can support tests. Do not replace native simulation with the Python rollout prototype. |

The gate settings remain:

| Arm | White absolute tolerance | Black absolute tolerance | Relative score loss |
|---|---:|---:|---:|
| Stricter practical | 0.050 | 0.050 | 0.060 |
| Permissive | 0.110 | 0.075 | 0.095 |

Absolute gates use the frozen White-score baselines 0.5183137990703003 for White experiments and 0.5195669627673518 for Black experiments. White requires child White score at least baseline minus tolerance; Black requires child White score at most baseline plus tolerance. Do not recompute these baselines when changing sources or pools. The relative gate compares the trusted reference and child from our perspective. Keep source-score normalization separate from rollout leaf clipping.

## Corrections and qualifications to today's explanations

1. **The new model is not already producing the canonical books.** The BRC-plus-Maia-30 path, the implemented opt-in OTB graph policy, and the intended next pilot are different states. Neither extra pool availability nor a configuration label proves a consumer uses that pool.
2. **The new policy is not mostly OTB at every position.** OTB observations have weight O/(O+10). With no OTB counts it is entirely the online/Maia estimate; ten observations make it half OTB.
3. **Thirty does not mean thirty games added to the one observed move.** At the targeted-online layer, probability is `(target_count[m] + 30 * backup[m]) / (target_total + 30)`. The one game supplies one observation; the prior spreads thirty across legal moves. The 60/30/10 fallback example with one B observation gives 58.06/32.26/9.68 percent before Maia/OTB.
4. **The empirical hierarchy is nested smoothing, not independent samples.** Targeted counts are subtracted from the intermediate adult counts; the all-rating distribution still includes observations found in the narrower pools. Do not describe the layers as independent evidence or interpret their totals as a literal effective sample size.
5. **The discounts control Maia's weight.** Support is `R + .10*(A-R) + .01*(B-A)`. This is not a direct 100:10:1 weighted sum of move counts. Maia is skipped at support >=300; below that its weight is 3/(support+3). If there is only one online game anywhere, Maia receives 75% before the OTB update.
6. **Counts do not propagate backwards as new games.** Each board has its own conditional reply model. Backwards induction propagates values, weighting by local probabilities. Large early-position counts improve those early probabilities, not the certainty of a thin deep leaf. The optimizer currently uses point estimates rather than propagated statistical confidence intervals.
7. **The own-side threshold is per position and unconditional.** Nweak=19 and Nadult=1 discards all nineteen weak observations in favor of the one adult observation. Nadult=0 still does not restore the nineteen weak observations. The reproductions confirm this.
8. **The short continuation is not the full rollout evaluator.** W uses our move, their reply, then engine score. R uses their reply, then W. Its opponent probabilities are raw adult frequencies. The legacy referee uses counts plus Maia; the new generic sweep rollout accepts callbacks but has no automatically bound production ensemble.
9. **The online construction bands are game-mean ratings.** They are not censuses of movers individually rated 1000–1799 or 1800+. OTB construction uses the approved mover filter. Label this difference and measure it when calibrating our policy; do not silently call the construction weak pool a mover-level dataset.
10. **A 0.75 eval-weight label does not mean every final value is 75% engine plus 25% historical outcome.** W/R replacement overrides ordinary initial leaf values where available. The legacy blend remains in defaults, uncovered paths and auxiliary terms. Its effective contribution must be measured before interpreting a sweep of this flag.
11. **The full source overlay does not imply end-to-end float preservation.** Its lookup and tested builder consumers preserve fractional scores, but the legacy native rollout's carried ancestor CP is still integer. A directly evaluated final leaf retains its float; the truncation affects carried/fallback evaluations.
12. **The score is a utility proxy.** The logistic engine conversion is the project's chosen score scale, not a calibrated win probability for David versus every target population. Report it as modeled expected-score utility and distinguish it from actual wins, move-prediction accuracy and engine soundness.

The exact opponent recipe is in [policy.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/sweep/policy.py) and [spec.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/sweep/spec.py). At each board it smooths all-rating BRC with one uniform observation, smooths other-1800+ counts toward that distribution with 30, smooths Rapid2200+ plus Classical1800+ toward that result with 30, selectively blends Maia with strength 3, then applies the OTB update with prior strength 10. Completed ensemble probabilities must not receive another Maia-30 layer.

## Findings requiring implementation

P1 means resolve before the intended pilot or its acceptance. P2 means resolve before the affected comparison/publication. A documented approximation is not necessarily a code defect; the plan distinguishes the two.

### R1 Preserve weak observations and model our side consistently

**P1, reproduced.** [weak_stop.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/weak_stop.py) switches to adult frequencies below twenty weak observations without checking adult support. The fixture gives W=0.20 with nineteen weak observations and one contrary adult observation, but W=0.80 after the twentieth identical weak observation. With nineteen weak observations and no adult observations it falls back to the current-board engine value 0.50 instead of using the available weak continuation.

Replace this switch with a normalized weak-policy provider. Recommended candidate form: `Q(m) = (n_weak(m) + K_own*q_weak_maia(m))/(N_weak+K_own)`. Retain every weak observation; at zero count use the separately calibrated weak Maia prior. Do not use the opponent's 2600/2600 settings as a shortcut. Adult frequencies may remain an explicit diagnostic control, not an automatic substitution. Retain the legacy implementation under an explicit historical recipe for reproducibility.

Freeze a small calibration protocol before selecting K or Maia settings. Suggested engineering comparison: K in {10,30,100}; Maia own rating in {1200,1450,1700}, opponent input in {1800,2200}, temperature in {1.0,1.35}. This requires six raw rating-pair inference sets; temperature and K variants reuse predictions. Compare raw weak counts where supported, model-only, and the legacy threshold rule. K=30 is a starting candidate, not a newly approved production constant. Choose on development data, then confirm once on an unexposed cohort.

Use mover-level 1000–1799 online validation, with separate time-control and support strata. Quantify the difference from the game-mean construction counts. Do not force an immediate full re-extraction to mover bands unless the measured mismatch warrants it.

### R2 Share opponent probabilities across book boundaries

**P1, confirmed call-path mismatch.** [build_budget_books.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/python/build_budget_books.py) publishes/closes `FrontierOverlay` after graph collection, then calls `weak_stop_values` without a policy adapter. W/R queries raw adult counts. In the fixture, raw replies give W=0.90; a specified 10/90 ensemble over continuations valued 0.90/0.10 would give 0.18. This is a demonstration of sensitivity, not a measured book error.

Create one frozen, batched policy interface used by graph collection, W/R, model-matched rollouts and extension reach. It must serve newly reached off-graph boards and return complete legal conditional probabilities, source support, identity and separately typed continuation/ending/censoring accounting. Keep the provider alive through stop construction, or reopen a compatible query provider; do not silently fall back when a static overlay lacks a queried board. Freeze the union of all probabilities actually used after construction finishes.

The matched referee must use the same recipe. Additional empirical/strong-opponent validation panels may intentionally use different distributions, but must be labeled external validation. Merely copying an ensemble name into metadata is insufficient.

### R3 Keep probability mass when pruning or evaluations are missing

**P1, reproduced.** `_shares` drops moves below 1% and renormalizes. W/R then filters unevaluated continuations and renormalizes again. A 0.9% losing reply disappears from a fixture whose other reply wins: the result becomes 1.000 instead of 0.991. With half the reply mass evaluated at 0.80 and half missing, the routine returns 0.80. Without further evidence the corresponding model-value interval is [0.40,0.90].

The legacy referee additionally maps a leaf with no evaluation, no ancestor value and no SF fill to equality, while reporting `no_eval_mass`. It must not count such a point estimate as accepted evidence. This branch was reproduced at 0.50; default SF fill can avoid it, so it is not a claim that every normal evaluation encounters it.

Keep complete legal probability mass. Separate search pruning from the human model: an unexpanded reply still needs a value contribution or a reported unresolved interval. Batch child lookup; spend local evaluation or continuation work where root-reach-weighted uncertainty could affect selection. Conservative [0,1] bounds apply to unknown model utility, not to an asserted confidence interval for real human performance. Track the aggregate approximation budget, not a per-edge epsilon whose errors can accumulate without limit. Refuse acceptance when unresolved mass could reverse a claimed comparison or conceal a gate-critical choice.

Do not expand every legal move several plies at millions of boards without a cost benchmark. Cache, deduplicate, bound and selectively refine. Short-model truncation error and missing-evidence uncertainty must be reported separately.

### R4 Separate construction depth from continuation evidence and game endings

**P1 semantic contract; impact requires route-specific tests and a real-board census.** Pool metadata and `check_arm` currently require opponent and weak pool caps to match construction depth. The p30 exports contain moves through ply 30; the short tail may ask for later moves. A new deep board can therefore lose empirical support because of an export boundary rather than genuine rarity. The underlying explorer already contains 51 plies.

The ensemble frontier also sums actual ending categories and extraction-horizon mass into `nonmove`. Native graph auxiliary values score horizon mass with historical outcomes; extension `reply` treats all nonmove mass as stop probability. The legacy rollout generally simulates legal moves and actual board outcomes rather than that same empirical stopping process. These differences require an explicit shared rule, especially when one board occurs both early and at the source horizon. Passing the existing transposition collector regression does not establish that these auxiliary semantics agree.

Recommended contract: distinguish (a) actual chess terminals, (b) observed game endings such as resignation/flag, (c) data truncation, and (d) computational stopping. Source truncation is not an opponent decision or a win/loss event. Never absorb its eventual game result as a fabricated terminal payoff. Keep original counts/flow receipts intact while implementing the consumer rule. Determine whether and how empirical non-board endings contribute to the objective; report clock-flag dependence separately for the OTB goal. Do not silently remove actual ending information or assume its hazard transfers unchanged to a counterfactual repertoire.

Bind candidate depth, count-source depth, continuation horizon and evaluation horizon separately. Prefer selectively queried broader-ply evidence for tails, with an explicit source-cap contract and the same filters across nested populations. Beyond available source coverage, identify model-only prediction rather than claiming an empirical zero. Test the same full key reached at different route plies, exact cap boundaries, repetitions and unbooked connectors. Changes to this contract require new derived-policy/stop identities, not re-extraction of the accepted source book.

### R5 Make the paired evaluation-source pilot executable

**P1, confirmed binding gaps and a reproduced request-union rejection.** `ConsumerKeys.for_pool` binds the lookup from the candidate pool's historical metadata. The builder has no separate accepted evaluation-provider override. An artifact list mentioning the new overlay does not change that lookup. Reusing accepted count tables requires an explicit consumer binding, not changing their original metadata or rebuilding their counts.

`sweep.verify.combine` also demands identical menu dictionaries at a shared parent, including `stored_cp`. Two valid source-policy arms with identical counts/children but stored scores 40 versus 60 are rejected with `books use different candidate counts/evidence at a shared parent`. This is appropriate for identical-source arms, but blocks the intended different-source comparison. The coordinator's first-pilot preflight is designed for one manifest pilot arm, not an arbitrary pair with duplicate arm IDs.

Implement an evaluation binding containing the full provider manifest, fallback identity, local evidence identity and acceptance receipt. Propagate it through builder, gates, own-move helpers, stops, extensions, audits and caches. Preserve count-source provenance independently. Add a paired-pilot driver or explicit scenario identities that keep the two evaluation bindings distinct while sharing counts, human models, solver, gates and independent-search epochs.

Request union must require equal board identity, legal move/child identity and true counts, but retain per-arm stored scores. Close the comparison sets against every arm's references, union the independently required moves, and rebuild both arms on the same monotone verified overlay. Do not merely ignore unequal score dictionaries or pick one arm's values. Freeze one common referee for both books; evaluating each book with its own construction-source provider would confound the comparison.

### R6 Preserve score precision and define one leaf utility contract

**P2 for fractional fallback precision; P1 for the pilot's explicit valuation contract.** The new sparse source overlay is float64, but `convertibility_eval.rollouts` casts lookup/start CP to int64 and the Rust rollout stores `Option<i64>` CP on return. A real native one-ply fixture carries 12.75 as 12. A final leaf found directly in the lookup keeps the float; missing-leaf ancestor fallback does not.

`EvalSource(boundary=False)` and `uniform_score` also differ in clipping. At +2000 cp the ordinary stop helper returns 0.99936684 and the horizon helper returns 0.97544744 because the latter clips at +1000. That is an established legacy difference, not a failure of the new source arithmetic. It can matter to the convertibility objective in strongly winning positions.

Use float64 (or typed expected score) end to end, an explicit missing state, and exact game outcomes for actual terminals. Recommended new continuation/referee utility: preserve the source policy's approved normalization, then use the common logistic score without an additional hidden +1000 cap; keep the old clipped utility as a named comparison control. Gates and the selected source weights remain unchanged. Before adoption compare ranking effects and terminal/large-advantage cases. Every new utility identity must invalidate derived scores while preserving raw independent analyses.

### R7 Clarify what the optimizer is actually optimizing

**P1 objective audit before the main sweep.** `Core.apply_stop_values` replaces node and out-of-graph edge values with W/R when available. Those W/R values do not take `eval_weight` or the empirical score prior. A fixture confirms both initial values are overwritten. Therefore the planned 0.50/0.75/1.00 eval-weight sweep may principally measure missing-stop/auxiliary behavior rather than a clean tradeoff between empirical performance and engine value.

Instrument every final value by origin: exact terminal, short continuation, sampled continuation, direct engine, legacy empirical blend, auxiliary ending/censoring term, and unresolved. Measure reach-weighted contribution and finite-difference sensitivity to each advertised parameter. Preserve 0.75 for historical reproduction; do not spend full builds on a parameter until its intended effect is demonstrated.

For the new primary objective, prefer modeled continuation utility with the soundness gates as explicit constraints. Do not add a second raw outcome blend merely to keep an old knob meaningful. If a deliberate empirical/value mixture is retained, define its location and calibration separately, without moving the frozen gate baselines.

The short continuation remains an approximation to the long referee. It can miss later tactical difficulty and book re-entry: W/R is book-independent, whereas the referee follows a prescription again if a trajectory returns to one. Quantify these effects on selected endpoint/transition panels. Do not claim exact builder/referee value equality merely because they share policy functions. Selective longer continuation or a calibrated continuation-value approximation is a later, measured response if the short proxy changes rankings materially; no full-game simulation at every graph node is required upfront.

### R8 Complete the production evaluator rather than relying on a prototype

**P1 before the paired rollout result is accepted.** `sweep/rollout.py` is a useful full-key callback/reference implementation, but simulates boards in Python. The existing native referee takes legacy count-plus-Maia blends; it does not automatically consume `otb-ensemble-v2`. `sampled_stop.py`'s sampler CLI still opens legacy eval arrays and does not supply the full-key adapter, even though the builder has full-key stop dumps. Do not route the new pipeline through that CLI unchanged.

Bind the new own/opponent providers to the Rust rollout engine. Preserve paired random streams, both key halves, full actual rollout history for terminal/repetition rules, position-only model inputs, exact terminal outcomes, recall behavior and book re-entry. Freeze horizon and model/runtime/temperature/count identities. Either port the sampled-stop producer to that contract with a full-key round trip, or explicitly exclude it from the first pilot; its old implementation is not a prerequisite to use by default.

Use model-matched rollouts for construction consistency and separately frozen external populations to assess generalization. Identical recipes in builder and evaluator do not constitute independent empirical validation. Keep OTB/online holdout roles and previous exposure information intact.

### R9 Close gaps in publication and readiness checks

**P1 for first-pilot acceptance, P2 for later extensions.** Existing tests cover many important local contracts, but the selective-policy integration test patches `weak_stop_values`, and the builder/referee parity fixture uses synthetic compatible distributions. Neither would detect the current production-policy mismatch. `pilot_acceptance` checks keys, menus, gates, lookahead, quality closure, monotone evidence and clean rebuild, but has no own-policy, continuation-policy, horizon-source or evaluator-parity checks.

Require evidence-backed receipts for the new contracts, including tests that intentionally make raw and ensemble distributions disagree. Add missing-mass, source-binding, full-key sampling, float-fallback, runtime and cache-hit/miss comparisons. Actual canonical publication must remain separate from pilot readiness. Keep existing safeguards instead of replacing their requirements with a single broad boolean.

### R10 Avoid unnecessary data rebuilding while preserving provenance

**P2 performance and capacity decision before another large export or global sort.** The mini is building only BRC1800-p30. Rapid2200-p30, Classical1800-p30 and the accepted OTB training counts already exist. Full all-rating and weak exports do not. The current policy adapter requires them as pool Parquets, but the approved plan already permits selective broad reads from the explorer. `BookOwnMoves` supplies a starting point for full-key reads, though it is a single-board/all-plies extension helper, not a complete batched production adapter.

Benchmark a batched Rust-backed reader and persistent cache for missing broad/weak inputs against a bounded export. Validate against explicit aggregation on real representative buckets. Include zero-count proofs, expected-partition inventory, both key halves, ply filters, source hashes, read amplification and cold/warm performance. Missing files must not silently mean zero games. Benchmark early widely scattered keys as well as deep concentrated batches. Persist queried counts independently of the resulting model probabilities.

Before launching a multi-terabyte global pool sort, compare consuming the accepted 512 bucket outputs through a partitioned publication/reader. Each bucket must retain sorting, schema, key-partition checks and independent flow validation. Existing consumers assume particular Parquet layouts; add an adapter and parity tests before considering the bucket set published. This option may avoid a large duplicate output and sort, but its cost is unmeasured. It does not authorize deleting arrivals, originals or accepted outputs.

## Implementation sequence

The review and plan are complete. Items below are queued recommendations, not changes already applied. Existing authorization to finish the data and source-policy work remains in force. No blanket approval prompt is needed for that work.

| Stage | Work | Completion evidence | Dependencies and status |
|---|---|---|---|
| 0 | Freeze this review, reproductions and current state; update controlling plans | Source hashes, probe results, focused tests, explicit outstanding items | Complete with this document |
| 1 | Define versioned policy/value/data-depth contracts; add explicit evaluation binding and paired source scenarios | Refusal tests for missing/wrong bindings; both evaluation arms see different provider values over identical count files | Next; can proceed while mini runs |
| 2 | Implement shared native own/opponent policy kernels and batched providers; retain legacy controls | Native versus independent formula tests; all legal mass; no double Maia; nineteen/twenty and zero-count cases; cache identity tests | After stage 1; weak parameter selection remains separate |
| 3 | Connect W/R, off-graph boards and native referee; preserve mass and fractional values; separate horizons/endings | End-to-end unequal-policy fixtures, missing/tail bounds, full-key collisions, no hidden rounding, route/cap/terminal tests, resume parity | After stage 2; first key correctness milestone |
| 4 | Calibrate own policy and run targeted continuation/model audits | Locked development/confirmation protocol; Brier/log-loss/support results; short versus long endpoint audit; effective-objective report | May develop fixture/inference tooling alongside stages 2–3; requires accepted validation inputs |
| 5 | Complete and independently publish required data access and evaluation artifacts | Full overlay receipt; accepted adult counts; verified selective or exported broad/weak counts; pinned targeted/OTB training counts; capacity benchmark | Existing jobs continue; do not wait for them to start stages 1–3 |
| 6 | Freeze new pilot recipes and run a bounded end-to-end cost/correctness pilot | Real-board W/R/referee checks; warm/cold cost; selected Maia share; count-query amplification; no unexplained early stop/missing mass | Requires stages 1–5; does not launch six books |
| 7 | Run paired Black stricter B=2,000 p30 source-policy pilot | Common search-evidence epochs; menu/gate/closure audits; same independent referee; changed choices/PGNs; all release gates below | Full acceptance prerequisite before the other books |
| 8 | Run native optimizer comparison and staged main tuning matrix | Shared frozen graph, equal-resource results, independent validation and stability records | After pilot acceptance |

Stages 1–3 should be coherent changes with meaningful integration tests, not separate patches that temporarily advertise the whole system as ready. Use a fresh immutable release after them; never change the code underneath running jobs. The real-data cost pilot determines job ETAs and whether selective access is sufficient. There is not yet a measured basis for an honest total rebuild ETA.

### Tests required before production construction

- Weak support 0/1/19/20/21 with adult support 0/1/large: retained weak data, continuous blending, normalized legal probabilities.
- One targeted game distributes the thirty pseudo-observations according to fallback; zero online support uses Maia; support 299/300 behavior is explicit; OTB update 0/1/10/90; all nested counts valid or rejected.
- The same opponent board queried in graph, W, R, rollout and extension returns the same conditional vector under the same contract. Static overlay miss fails or uses the pinned provider, never raw adult fallback.
- Both colors; identical low half and different high halves; castling/en-passant reconstruction; transpositions at different plies; actual checkmate/stalemate and repetition behavior.
- Probability conservation with sub-1% replies, model-only moves, missing engine children and actual endings. Dropped mass is never renormalized out of the modeled distribution.
- Construction cap 30 with continuation requests at plies 30–33, then rollout cap 50; same board at early/cap plies; source truncation is distinct from game termination. Document evidence behavior beyond ply 51.
- Fractional CP through lookup, native rollout, last-ancestor fallback, serialization and final scoring; terminal values remain exact; absent evidence never masquerades as a neutral measured score.
- Paired engine-source arms with identical real counts but disagreeing stored CP: both build, all necessary comparisons are unioned, stronger local evidence never downgrades, and both rebuild on the common overlay.
- Frozen policy/cache/output identities change when model settings, source depth, value normalization, lookup binding or code changes; raw neural predictions and independent engine searches remain reusable when their own contracts are compatible.
- Interrupt/resume and clean-rebuild parity for count caches, inference batches, stop-value batches, verifier outputs and pilot reports.

### Real-data validation before broad tuning

Stratify prediction and continuation checks by color, ply 0–9/10–19/20–29/30–39/40–50, weak/target/OTB support 0/1–4/5–19/20–99/100+, time control, and representative openings. Report both natural frequency weighting and a deliberately sparse/adversarial panel. Do not blend the two into a claimed population rate.

Use full legal-move Brier/log loss and top-choice accuracy with game/family clustering; player/tournament clustering is an additional sensitivity where available. Preserve the corrected OTB family split. Historical OTB panels influenced earlier choices, so do not relabel them fresh. Confirm the exposure status of the new-family cohort before using it as independent confirmation. Never tune on the already opened final engine-source confirmation set.

For book-level work, keep the existing paired seeds and evaluation sizes (4,096 trajectories on each of four primary seeds; finalist expansion as specified in the tuning plan). Report Monte Carlo uncertainty separately from data/model uncertainty. A higher in-model score alone cannot establish an improvement. Add sensitivity to opponent population, own-play strength, missing-data treatment and continuation depth using rescoring first; rebuild only when a consequential ranking change justifies it.

Measure the number and reach of model-only boards, Maia calls and cache hits, empirical support distribution, unresolved value mass, actual endpoints, book re-entry, weak moves per game, late-game horizon reach, and wall time/RAM/storage. Record source-policy eligibility and independent verification coverage for all final prescriptions, including low-reach ones. The known stricter-White audit concern must remain visible even when the first construction pilot is Black.

### Pilot release requirements

Existing gates remain mandatory: full keys, complete historical menus plus shared nominations, mass conservation, parent/child gates, horizon/lookahead audit, exact prescribed-position budget, independent-verification closure, monotone evidence and clean rebuild parity.

Add these independently evidenced requirements before releasing the other books:

1. Explicit own/opponent providers are consumed at every claimed location.
2. Sparse weak data is retained; the selected own-policy calibration and its uncertainty are recorded.
3. Missing and computationally truncated probability mass is accounted for with acceptable comparison bounds.
4. Construction, source and evaluation horizons and nonmove semantics are distinct and tested.
5. Both source-policy arms consume their declared evaluation providers, including local override precedence.
6. Referee settings, leaf source and paired random streams are fixed across the comparison; fractional scores survive.
7. Effective objective terms and active hyperparameters are reported.
8. Real input access, inference and continuation costs fit the measured resource budget; no missing input is relabeled as another pool.
9. Receipts bind those checks and the implementation actually used, not only descriptive recipe labels.

## Data and operational state

- The lossless ChessDB archive and its independent audit are complete. The source studies, pooled analysis and final confirmation are complete and reviewed. Preserve their evidence and original sources; this plan authorizes no deletion or repeat research batch.
- The full score334 candidate overlay finished during this review: 512 buckets, 1,998,220,987 sparse rows, 49,955,622,979 bytes (about 46.5 GiB), 9,727 seconds (about 2h42m). Its producer status is verified. Manifest SHA256: `15be7323a4af4e8360bf89a6f06652fcbbaf52764c74a9685a6a6774e2ee2f0c`.
- The prescribed independent sample of buckets 0/255/511 passed against raw source SQL: 16,893 positions, maximum score error 3.33e-16. This is a sampled semantic check of the full published generation, not an independent arithmetic census of two billion rows. Full per-bucket materialization receipts remain part of acceptance. Pilot-ready publication evidence and the consumer bindings still need completion; canonical bindings are unchanged.
- The mini BRC1800-p30 bucket queue continues. Its bucket completion does not alone publish a globally usable pool. Preserve all accepted receipts and the eight original buckets. No new global merge, broad export or weak export was launched by this review.
- Rapid2200-p30 and Classical1800-p30 have existing validated exports. Accepted OTB training p30 counts are available; validation/test counts stay out of construction. A later binding step must recheck the required content hashes.
- Broad BRC and weak counts are still missing from the current shared-input exports. Decide selective versus full materialization after a bounded cost pilot; do not imply they are queued on the mini.
- Keep the 250 GiB evaluation reserve, mini resource limits and existing no-F-pool-spill rule. Include the overlay's 46.5 GiB in merge capacity. Do not modify frozen releases, unrelated main-checkout changes or the crawler.

## Evidence and verification performed

Reproductions: [probe.py](evidence/repertoire-model-review-20261125/probe.py) and [results.json](evidence/repertoire-model-review-20261125/results.json). They exercise the current functions with small local Parquets and a real native rollout; no GPU or Stockfish process is required. Source hashes and the script hash are recorded. To reproduce on the reviewed code, copy the script into `scratch/model-contract-review-20261125/probe.py` in the maintained worktree and run it with the repository virtualenv; it writes only to that scratch directory. Arbitrary node IDs in the stop fixtures isolate numerical behavior; legal chess/full-key behavior is covered by the separate regressions.

| Check run during this review | Result |
|---|---|
| Policy support gate, selective execution, evaluation overlay and publication suites | 20 tests passed |
| Horizon/lookahead, full-key verification and full-key builder/referee suites | 14 discovered, 13 passed, one opt-in installed-Stockfish smoke skipped |
| Weak fallback, raw-opponent mismatch, missing/tail mass, pseudocount, no-eval leaf, fractional native fallback, utility scale, paired source-menu and stop replacement probes | All reproduction assertions passed, confirming the reported current behaviors |
| Independent sample of the completed full overlay | 16,893 source comparisons passed, max discrepancy 3.33e-16 |

This targeted verification is not the full repository suite and does not establish production book quality. The passing existing tests are useful regression protection, but their fixtures do not close the integration gaps above.

## Progress tracking

- [x] Trace current graph, policy, stop, referee, value, binding and verification paths.
- [x] Reconcile today's explanations with the implementation and frozen decisions.
- [x] Reproduce critical behavior and run focused existing tests.
- [x] Independently sample the newly completed full evaluation overlay.
- [x] Save this review and link it from the controlling plans.
- [ ] Implement stages 1–3 and extend the acceptance contract.
- [ ] Complete own-model calibration, effective-objective and continuation audits.
- [ ] Publish accepted data access/bindings and measure the bounded real-data pilot.
- [ ] Run and accept the paired Black production pilot.
- [ ] Proceed to the native optimizer and main repertoire sweep.

The next coding step is stage 1: establish explicit, versioned policy/value/provider contracts and paired evaluation-source bindings, followed immediately by the shared-policy and continuation work. This addresses the boundary inconsistencies together instead of adding another isolated fallback rule.
