# Consolidated lifetime repertoire tuning plan

Decision date November 18, 2026. This is the current experiment specification. It supersedes the older six-book centipawn sweep and the four-book-only score-gate plan. The historical reports remain evidence. The user approved the additional recommended numerical settings and staged tests on November 18. The constants below are the adopted starting recipe, with changes permitted only through the documented calibration and sensitivity stages.

## Goal and experimental units

Choose one repertoire per color that works now and through the user's 2000 online and 2000 USCF goals, with acceptable learning effort, strong-opponent safety, and useful middlegames when recall fails. Report practical expected score, conversion, objective quality and learning burden separately.

A base budget counts distinct positions with a prescribed own move. An extended book counts the union of base and extension prescriptions, deduplicated across transpositions. Additional forced or connecting prescriptions count toward the extended total.

A build arm is a specified color, gate pair, base budget, construction method, objective weight, opponent policy, horizon and extension configuration. Repeated evaluations of one book are replicates of that arm. Identical resulting books share a content ID and can share evaluations.

## Approved primary comparison

| Gate arm | White absolute tolerance | Black absolute tolerance | Relative score-loss cap for both |
|---|---:|---:|---:|
| Stricter practical | 0.050 | 0.050 | 0.060 |
| Permissive | 0.110 | 0.075 | 0.095 |

Absolute tolerances use a fixed White-score baseline for each color's experiments. The approved explicit constants are the previously measured White baseline 0.5183137990703003 and Black baseline 0.5195669627673518. Freeze these across gates, budgets, policies and horizons to isolate their effects. This intentionally overrides recomputing the gate baseline from each newly exported pool. Record the current pools' observed root scores separately.

White requires S(child) >= baseline - tolerance. Black requires S(child) <= baseline + tolerance. S(cp) = 1/(1 + exp(-0.00368208*cp)) with cp from White's perspective. The relative cap is the engine score loss against the best trusted reference, from the repertoire side. The stricter absolute cutoffs are approximately -34.5 cp for White and +76.1 cp for Black. Permissive cutoffs are approximately -100.7 and +104.0 cp. Code defaults must not determine these arguments implicitly.

| Base budget per color | Independently optimized | Reduced with prescriptions fixed to its 2,000-position parent |
|---|---|---|
| 500 | Yes | Yes |
| 1,000 | Yes | Yes |
| 2,000 | Shared parent | Same reference |

There are 10 distinct base arms per color, 20 total. The two construction methods coincide at 2,000. Independently optimized books can change opening choices. Reduced base books must satisfy 500 subset 1,000 subset 2,000, with identical moves on common boards. Derive the 1,000 subset first and the 500 subset within it. Each gate setting has its own parent.

Every arm receives the same selected extension policy. Preserve both base and extended artifacts. Compare at nominal base budget and at actual total prescribed positions. Report unmatched total costs directly; a comparison of 500+800 versus 1,000+100 cannot isolate the base budget alone.

The reduced learning ladder must also preserve prescriptions after extension. Use the extended 2,000-position master as the permitted prescription set and carry the smaller extended book into the next rung. Report mandatory inherited extensions and their cost separately from newly selected extensions. If a needed critical continuation is absent from the master, resolve and version the master before publishing the ladder. An extension becoming part of a larger base is counted once. Independent arms receive unconstrained, gate-compliant extensions.

## Fixed main-run settings

| Parameter | Approved constant for the primary 20 arms |
|---|---|
| Base construction horizon | 30 plies, with the boundary counted explicitly; retain 51-ply source data for later 50-ply comparison |
| Objective CLI eval-weight | 0.75 explicitly, matching the verified historical build receipt |
| Legacy empirical outcome prior strength | 500 where that component remains active |
| Our out-of-book construction policy | Historical 1000–1799 weak-player policy, using the same versioned stop/continuation implementation throughout |
| Opponent construction policy | The November 13 OTB ensemble detailed below |
| Candidate discovery | Shared broad uncut all-three-speeds 1800+ own-move menu for both colors, plus the same verified engine nominations |
| Own-move and opponent-reply historical cutoff | min_games=1 for observed moves; use stored evaluations for initial selection and trigger fresh checks by consequential evaluation uncertainty |
| Maia checkpoint | Maia-3 79M, position-only, immutable checkpoint and inference recipe |
| Incumbent stickiness for independent arms | Off; measure switching cost afterward |
| Solver | Existing validated Rust price+polish for White and DP with distinct-position matching for Black; freeze solver settings within color |
| Gates on extensions and inherited moves | On, same configuration as the arm |
| Neural or recall discount in extension selection | None |
| Fixed extension depth limit | None |
| Minimum opponent reply frequency for extensions | None |
| Extension threshold before calibration | 0.00003 exposure units per additional distinct prescription |
| Primary numeric evaluation | Lichess score conversion; actual game terminals use exact outcomes |

Horizon means game plies, not additional plies after the book ends. Translate it to the implementation's graph-boundary convention and test the translation. The preceding approved four-book comparison forms the four parent anchors of this design.

Keep the existing verified computational pruning initially, with its actual settings in the manifest. Audit omitted reachable mass at the lowest extension threshold and on at least one stricter and one permissive 500-position case with tighter pruning. The earlier base epsilon of 0.0005 must not silently exclude extension branches with reach around 0.0001. Extension expansion needs its own certified bounds. Computational pruning is an approximation-control check, not a repertoire preference.

The old pool-specific empirical score priors and the absolute gate baseline share one parameter in some code paths. Record that coupling; if reply-policy variants also alter outcome data, label the comparison accordingly. Prefer a shared empirical outcome source in the policy sensitivity experiment.

## Approved uncut counts and evaluation-quality verification — November 18, 2026

Use min_games=1 for the construction pools and keep every observed move. Eliminate the Other bucket generated solely by a per-move observation cutoff. Preserve mass accounting for actual terminations, horizon boundaries, incomplete source coverage, computational pruning and model-assigned unobserved legal replies. All observed moves being explicit does not imply that the full legal reply distribution has empirical support.

Historical frequency and engine evidence quality are separate. A move observed once can have strong engine analysis. A common move can have missing, inconsistent or outdated analysis. Use the stored hybrid evaluations for initial selection, with provenance and analysis quality attached, rather than sending every selected rare move to Stockfish.

The current legacy builder couples rare_floor to min_games when rare verification is enabled. Decouple move inclusion from verification policy explicitly. Setting min_games=1 must not accidentally suppress verification of missing or consequentially uncertain evaluations. Low game count alone is no longer a fresh-engine-search trigger. Sparse opponent counts continue to receive the approved probability smoothing.

Fresh searches are targeted at:
- Missing or unusable evidence affecting an eligible candidate or selected prescription.
- Inconsistent parent/child or competing-move scores that could alter admission or selection.
- Decisions near an absolute or relative gate, using the shared boundary-review rule.
- Selected tactical extensions whose second-best/third-best gaps need independent confirmation.
- Cloud-derived final prescriptions requiring the user's agreed independent local verification.

Apply the same evaluation-quality policy to all gates and budgets. Record why each search was requested and the quality of reused evidence. A rare stored evaluation can be used provisionally without being treated as final independent verification. Reuse an existing qualifying independent search; preserve stronger evidence and exact position identity.

The intended workflow is:
1. Load the shared candidate graph and best available compatible evaluation overlay, then select provisional books.
2. Batch the consequential verification requests across relevant sweep arms, deduplicating parent/move comparisons and retaining the required alternatives for each comparison.
3. Incorporate results into a versioned shared evaluation cache. Identify changed gates, candidates, continuation values and budget allocations.
4. Recompute affected dependencies and re-solve using the retained graph where supported. Changes in eligibility can require graph expansion and new topological work. Verify incremental results against a clean rebuild on representative fixtures and a production pilot before relying on incremental updates.
5. Recheck newly selected prescriptions and any affected extension continuations. Continue until the selected book passes the agreed verification criteria. Reuse the prepared graph for repeated solves whenever possible.
6. Freeze final evidence for each comparison, revalidate all affected books against that evidence, and record book/overlay receipts. Shared cache growth alone must not silently change an already reported comparison.

This targets an initial cached-evaluation selection followed by batched corrections. There is no promised fixed number of verification passes. Full graph reload/rebuild remains available when structural changes or implementation constraints require it. Repeated full rebuilds for one or two changed positions are a performance problem to address.

The historical White 0.60 round requests were 461 initially, then 36, 12, 7, 2, 2, 2, 0. The initial round included 300 proactive frontier disputes; the later rounds were rare prescriptions and disputed gates. Black 0.60 requested 434, 21, 7, 1, 0. These results explain the old 5–9-round behavior and must not be assumed to be the normal cost of the new verification policy. Evidence is in D:/chess/rare-verify-v1/rs/white-cp60/rounds.json and the matching black-cp60 file.

The Black stricter pilot must report initial solve time, fresh searches by reason, cache hits, newly selected unverified positions, gate changes, number of correction solves, full reloads, final size and verification closure. Confirm that changing only historical move frequency cannot change the engine-evidence trust decision. Check conservation after removal of cutoff-generated Other mass, and test the one-observation/high-quality-eval and common-move/uncertain-eval cases.

## Opponent policy and population sensitivity

Primary construction uses the tested OTB ensemble. Let b be all-rating Blitz/Rapid/Classical counts, a all-three-speeds 1800+ counts, r Rapid 2200+ plus Classical 1800+ counts, and o eligible deduplicated under-2000 OTB counts from 2013 onward. Capitals denote reply totals, L legal moves and q Maia's legal reply probabilities.

```text
broad       = (b + 1/L) / (B + 1)
backup      = (a - r + 30*broad) / (A - R + 30)
online      = (r + 30*backup) / (R + 30)
support     = R + 0.10*(A - R) + 0.01*(B - A)
pre_otb     = (support*online + 3*q) / (support + 3)
reply       = (o + 10*pre_otb) / (O + 10)
```

Use Maia input ratings 2600/2600 and temperature 1.35 in this ensemble, as selected by the OTB study. The empirical smoothing strengths are 30, the Maia strength is 3 and the OTB-update prior strength is 10. A legacy Maia-30 overlay must not be applied afterward. At zero support this returns Maia. Keep actual terminations and horizon mass separate from conditional reply probabilities.

Before the main build, reproduce the ensemble checks and one Black stricter 2,000-position pilot. If integration is incomplete, complete it and report the dependency. The planned main policy must not silently fall back to the old pool.

After the main comparison, test these three opponent policies at independent B=1,000, crossing both gate arms and both colors, with a common selected objective weight and horizon 30.

1. The primary OTB ensemble above.
2. All-three-speeds Lichess 1800+ with the existing position-only Maia-30 recipe.
3. All-three-speeds Lichess 2200+ with the same legacy Maia-30 recipe.

For controls 2 and 3, use fixed Maia ratings 2200/2200, temperature 1.0, and K=30. These are explicit comparison settings, not inferred mappings to USCF strength. Keep candidate menus and engine evidence shared so policy coverage does not restrict our alternatives. Reuse all existing cells whose complete recipe matches. This is 12 cells, up to eight additional books when the primary reference cells already exist.

Initially evaluate OTB-update strengths 10 and 30 by rescoring finalists, holding all other ensemble settings fixed. Rebuild only if a consequential ranking change appears. Keep Maia checkpoint, temperatures, strength 3, and empirical backup discounts fixed in repertoire experiments. Any later tuning of them belongs to a held-out move-prediction study.

## Extension calibration

Approved behavior is unlimited sequence depth, reach times potential engine loss, whole-continuation assessment, and no own-player historical accuracy or memorability discount.

The approved initial local severity is
```text
gap2 = S(best) - S(second best)
gap3 = S(best) - S(third best)
severity = max(gap2, 0.60*gap3)
```
Scores are from our perspective. The factor 0.60 preserves the relative scaling of the canonical 15/25-point warning thresholds. Keep those old thresholds visible as diagnostics, rather than mandatory eligibility cutoffs. A missing third legal move contributes zero to gap3; a single legal move has no local choice gap but may be a necessary connector. Missing engine analysis is unknown and triggers analysis; it is not a zero gap.

The approved default, changed by David on November 19, is `remaining-maximum`: the expected maximum severity still exposed at or after the first unbooked own-move position on each trajectory. A connected package earns the reduction in this remaining exposure, divided by the number of new distinct prescriptions including connectors. Thus covering an earlier equally difficult move cannot hide an uncovered later move; a package may need to cover both to earn credit. Use the arm's opponent probabilities and prescribed own moves. This remains a conservative exposure proxy, not an estimated win-rate lift. Report per-position reach, both gaps and residual unbooked risk. Keep the previous `covered-maximum` metric as an explicit comparison control.

Test thresholds 0.00001, 0.00003 and 0.00010 exposure units per added position. Lower values permit more extensions. At reach 0.001, severity 0.20 and five added prescriptions, a previously uncovered package has density 0.00004. It passes the first two thresholds. This illustration assumes the package removes the route's remaining 0.20 severity; leaving a later equally severe move uncovered would not earn that reduction.

Calibrate on all four 2,000-position anchors, yielding 12 default-metric extension variants from four base books. Queue the same four anchors and three thresholds under the previous covered-maximum metric as 12 additional controls: 24 extension variants in total, with no additional base books. Keep the severity definition, gates, opponent policy and engine checks fixed. Start with 0.00003. Change it only if validation shows a clear cost/performance tradeoff; if inconclusive, retain 0.00003. Use a 0.002 expected-score difference as a practical comparison scale, with uncertainty, rather than treating the exposure proxy as measured performance. Freeze the selected threshold before extending the remaining main arms.

Use whole-continuation searches and valid remaining-risk bounds. Low local gap alone cannot stop the search before a later critical position. Remove the independent 0.01% reach cutoff as a mandatory stopping rule. Prune only when the remaining exposure cannot justify additional prescriptions under the chosen threshold, or chess termination/repetition resolves the route. Handle board transpositions and repetition history correctly; raw revisits must not create endless chains or duplicated benefit.

Beyond historical coverage, use the frozen legal-move policy's documented fallback, including Maia at zero counts, plus engine verification. Mark source support and extrapolated reach in reports. Beyond 51 plies, broader-board counts if available are also identified as such. Report a sensitivity using stronger defense probabilities on these continuations. A source-data boundary must not silently become a four-move or 50-ply extension cap.

For engine evidence, use the cached hybrid data for screening, followed by independent local checks of selected cloud-dependent and tactically critical prescriptions. The initial production verification recipe remains 5M nodes and 20M for borderline/disputed cases. Use the higher-quality recorded result when one already exists. A margin of 0.01 in score units (one percentage point) to a gate or inconsistent parent/child results flags deeper review. Extend verification if a consequential dispute remains unresolved. For lower-ranked gap signals, verify the competing alternatives as well as the prescribed move.

## Objective-weight experiment

The inspected build uses eval-weight 0.75, prior strength 500. In the current implementation this weight blends empirical outcomes and engine score in some fallback and auxiliary values. The weak-stop and sampled-stop paths can replace those values. It is not automatically a direct weight on convertibility versus exit evaluation.

Test eval-weight 0.50, 0.75 and 1.00 at independent B=1,000, crossing both gate settings and both colors. Keep primary opponent policy, horizon 30, extension threshold, weak-player policy, empirical prior 500 and all verification settings fixed. There are 12 cells, four reused from the main matrix and eight extra builds if the same baseline recipe applies.

First instrument a representative pilot to show which reachable values change at each setting, and their reach. If sampled values override the knob almost everywhere, document that result and avoid a meaningless 12-cell sweep. A separate direct blend of exit evaluation and human-continuation score would be a new objective and requires an explicit proposal, rather than silently reinterpreting eval-weight.

Evaluate every weight with the same external score and opponent scenarios. Report raw engine exit quality and human-continuation score separately. Keep both gate settings in this sweep because tighter gates can change how much objective weighting matters. Do not re-tune the evaluator to favor the construction weight.

Choose a shared weight across colors unless a consistent practically meaningful interaction supports separate choices. When differences are unresolved, retain 0.75. Rebuild the relevant 2,000-position parents and regenerate reduced ladders if the selected weight changes; do not splice a reduced child from an old objective into a new parent.

## Horizon sensitivity and follow-up interactions

Test construction horizons 30 and 50 plies on independent B=2,000 books, crossing both gate arms and both colors, after selecting the objective weight. Hold the primary opponent policy and extension rule fixed. There are eight cells, with the four horizon-30 references reused when their recipe matches.

Use identical evaluation horizons across compared books. Main evaluation is at game ply 50, accompanied by a legacy ply-30 diagnostic. Before each comparison, if any relevant extended book reaches beyond ply 40, set the common evaluation horizon to the smallest even ply >= max(50, deepest selected prescription ply + 10). This gives every extension at least ten plies of common downstream evaluation. Use that same horizon for base controls and all paired arms. Record additional uncertainty from model-only continuations beyond data coverage. Also score finalists at common horizon +20 to detect ranking changes near the evaluation boundary. Horizon choice for evaluation must never truncate extension construction.

If pool, weight or horizon changes reverse the gate ordering or materially alter common moves, run a focused 2-by-2 interaction using only the two implicated settings at B=1,000. A practically consequential reversal is about 0.002 expected score or a clearly different risk/learning profile. Keep uncertain but potentially important cases for more paired evaluation. Extend to B=2,000 for the final contenders.

## Evaluation and learning settings

Use the same opponent panels and random streams across every pair, regardless of its construction policy. Report every panel separately.

| Scenario | Opponent model target |
|---|---|
| Current online Blitz | Mover rating 1600–1799, Blitz |
| Current online Rapid | Mover rating 1800–1999, Rapid |
| Goal online Blitz and Rapid | Mover rating 2000–2199, separate speeds |
| Strong online stress | Mover rating 2200–2399, separate Blitz, Rapid and Classical |
| OTB development | Published mover ratings 1500–1799 and 1800–1999, separate |
| OTB current-strength diagnostic | Positive published ratings below 1500 |
| OTB stronger stress | Published mover ratings 2000–2199, requiring its own data/model validation |

Construction Lichess cutoffs use game-mean rating. Evaluation mover bands are explicitly different populations. Never treat a Lichess cutoff as a USCF conversion. The under-2000 OTB ensemble's validation does not establish accuracy on 2000–2199 OTB opponents.

Use three own-player post-book scenarios, 1000–1799 as the conservative construction-matched condition, 1800–1999 as improved play, and 2000–2199 as a goal sensitivity. Hold these policies identical across candidate books. Validate empirical/model fallbacks before reporting a scenario.

The main report is a vector of outcomes. For an optional ranking summary, give OTB, Rapid and Blitz equal one-third weights, split equally between development and goal opponent scenarios within each environment. For OTB, the two summary panels are 1500–1799 and 1800–1999. For Rapid they are 1800–1999 and 2000–2199. For Blitz they are 1600–1799 and 2000–2199. Use the conservative and improved own-player scenarios as separately reported rankings. These are evaluation scenarios for current and improving opposition, rather than a claimed USCF equivalence. Strong stress panels remain separate. Use equal color weighting only for a combined repertoire summary.

Start scoring at 4,096 paired trajectories per book per panel, across seeds 1–4. Confirm finalists with 32,768 paired trajectories across seeds 101–108. Use matched counter-based draws keyed by scenario, seed, game and ply where supported. Adaptively expand development samples if uncertainty could change selection; reserve confirmation samples for frozen configurations. Cache common continuations. Distinguish Monte Carlo uncertainty from empirical/model uncertainty.

Recall sensitivity is an evaluation axis, excluded from extension selection. Retain the earlier scenario parameters as planning assumptions:
- Low, central and high initial half-lives of 2, 5 and 10 days.
- Successful-retrieval multipliers 1.6, 2.0 and 2.4, maximum half-life 365 days.
- Measurements at 30, 90 and 240 days, plus perfect recall.
- Primary study allocation 30 card presentations per color daily; sensitivities 15 and 60 on finalists.
- Primary game exposure averages one game per day total, split equally by color. Also report zero incidental game reinforcement.
- Eight simulated learner schedules per retention scenario, shared between books.

A presentation count is an effort proxy. Record elapsed review time when real training data exist. Report equal-presentation comparisons explicitly and avoid asserting equal minutes. Teach base and extension prescriptions from the same total allowance. From-scratch and actual documented prior familiarity are separate scenarios.

Report score loss on lapses, reach-weighted recall, fraction introduced, reviews required, and learned-move conflicts when moving from 500 to 1,000 to 2,000. Use imperfect recall in evaluation while preserving the approved engine-only extension priority.

## Opening and prescription stability across books

Produce these outputs for every completed book, including intermediate calibration variants. Label the experimental family so plentiful sensitivity runs cannot dominate the headline.

1. Opening presence and opening reach. Replay named ECO routes and detect transpositions by verified position identity. Record code, name, representative move order, whether our choices permit the route, whether its endpoint is covered by the book, and its probability under a common reference opponent policy. A catalog name beginning in the prefix of another must not be treated as a disjoint event.
2. Exact prescriptions. For each verified position key and legal UCI move, record SAN, representative PGN, first-reach ply, base/extension/inherited provenance, eligibility, objective evaluation, reference reach and model support.
3. Conditional choice stability. Among independently optimized books that reach and prescribe at a board where the move is eligible, report how many choose it. Also report the unconditional presence rate across books, coverage/reach denominator, and eligibility rate. Separate out-of-book, unreachable, gate-disqualified, missing evidence, and an explicitly different choice.
4. Family-balanced summaries. The primary independent evidence is six books per color, two gate settings times independent budgets 500/1,000/2,000. The four reduced books per color measure retention and learning progression. They do not cast independent votes. Report gate, budget, weight, policy and horizon stability separately. Any composite family-balanced index is descriptive sensitivity evidence, never a probability of correctness.
5. Exposure and switching. Report exact and reach-weighted overlap, disagreement at common boards, opening-family changes, total reach of controversial decisions, and conflict cost from the current canonical and between budget rungs.
6. Engine and data robustness. Attach gate margin, verified evaluation, outcome support and recurrence across game-resampled models where available. A universally copied or forced choice is labeled separately from a repeated choice among viable alternatives.

For early decisions, reach weighting shows practical importance. Conditional statistics preserve rare branches. For example, a move selected in all three independently optimized books that cover a rare board is recorded as 3/3 among covering books and 3/6 across primary independent books, with reach and eligibility alongside. It is not reported as unanimous across all books.

Suggested descriptive flags are "stable across all six independent main arms", "gate-dependent", "budget-dependent", "pool-dependent", "extension-only" and "inherited". Show the counts and denominators with every flag. Stability increases confidence in sensitivity to tested assumptions; shared engine or human-policy errors can persist across all arms.

## Evidence controls and promotion

Freeze source snapshots, code, model revisions, numerical settings, evaluator recipes and all gate baselines in build receipts. Use training data for construction, development data for tuning, and fresh confirmation data for final selection. Previously inspected August/September 2026 data and published OTB confirmation sets are development evidence now. Use genuinely unexamined partitions/new deduplicated games where available; otherwise disclose the lack of a fresh test. Split OTB copies by deduplicated game identity and assess repeat-player clustering.

Include engine expected score at book exit, common-horizon human-continuation score, objective floor violations, best-defense checks, reach of deteriorated positions, rare-reply coverage, final unique size, recall results, and runtime/cache cost. Exact mate and draw outcomes remain exact. Engine-converted scores are model utilities, not calibrated guarantees of OTB win probability.

Use paired uncertainty intervals and practical scale 0.002 expected score in development. A confidence interval containing zero does not establish equivalence. Report tradeoffs and retain close contenders. Freeze the finalist configuration before confirmation. Apply existing canonical promotion checks and require a reviewable result before switching learned prescriptions.

## Execution order and deliverables

1. Finish and verify the explorer/eval migration, integrate the intended Rust score gates and OTB policy, export min-games-1 inputs, decouple count-based inclusion from evaluation-quality verification, and run the Black stricter pilot. Verify horizon/transposition behavior, complete candidate menus, provenance, no evaluation-quality downgrade, and retained-graph correction parity.
2. Build the four independent 2,000-position anchors at primary settings.
3. Calibrate extension threshold on those anchors, then freeze the rule.
4. Complete the primary 20 base arms and their extensions, with the nested reduced ladders.
5. Score the matrix and publish the opening/prescription stability tables.
6. Run the objective-weight cross, opponent-policy cross and horizon cross on the stated smaller designs. Reuse exact reference cells.
7. Run focused interactions and refine paired evaluation where needed. Rebuild parents and ladders if selected settings change.
8. Confirm finalists, report actual learning cost and switching cost, and prepare the chosen learning ladder.

Required planned artifacts are an experiment manifest, immutable per-book receipts, base and extended PGNs/position tables, evaluation summaries with uncertainty, opening-selection and exact-prescription stability tables, learning/switching reports, and runtime/cache measurements. These artifacts are planned deliverables; this document does not claim that the new sweep or extension implementation has run.

## Production preparation and queue status — November 18, 2026

At 08:56 Pacific, all 163 monthly archives were complete. The merged explorer had 111/512 buckets published and a reported remaining ETA of 16.82 hours. The separate eval worker was waiting for the verified explorer.

The deployed training launcher queues extraction, archival, merge and explorer verification. The deployed eval worker queues the production capacity pilot, Cloud/Fishnet eval DB generation, independent source verification, the 128-bit lookup and terminal corrections. The current eval worker does not import ChessDB. Any requirement for ChessDB is a separately tracked integration task and must be identified in eval-source provenance.

The following production inputs are required by this tuning plan and are not queued by those launchers. The November 15 end-to-end rehearsal proves a small export/build/score path, rather than full production readiness.

| Required input | Planned derivation | Queue status at this check |
|---|---|---|
| Broad online reply counts | All-rating Blitz/Rapid/Classical, training months January 2013–July 2026 | Production export pending |
| Broad candidates and 1800+ control | Uncut all-three-speeds 1800+ counts, both colors | Production export pending |
| 2200+ policy control | Uncut all-three-speeds 2200+ counts | Production export pending |
| Targeted online OTB component | Rapid 2200+ plus Classical 1800+, retaining components for overlap subtraction | Production export pending |
| Weak and improved own-play counts | 1000–1799, 1800–1999, 2000–2199 bands under the appropriate training/evaluation rating semantics | Production export pending |
| OTB policy training data | Frozen deduplicated 2013-onward eligible games, training partitions and 128-bit positions | Snapshot/split audit and compatible production tables pending |
| Evaluation population panels | Speed-specific and mover-rating-specific online panels, plus OTB scenarios and disjoint evaluation data | Production preparation pending |
| Versioned policy cache | Legal probability vectors for the approved ensemble and controls, keyed by position/model/recipe | Integration and population of reachable cache pending |

Prepare the 30-ply main inputs first. Build the 50-ply sensitivity inputs after the primary input pilot, reusing the same merged explorer. Preserve ply in shared count caches when practical. Export multiple needed pool slices per bucket in a shared read where supported, then validate each separately. Do not rescan the multi-terabyte explorer once per repertoire. Hash both key halves in every consumer. Attach eval lookups to the prepared counts with explicit coverage checks.

Online construction counts use the archived game-mean rating slices. Mover-rating evaluation panels require the game/mover data sources and cannot be recovered simply by filtering that mean-rating archive. Freeze training/tuning/confirmation partitions before materializing OTB or online evaluation tables.

Add an explicit downstream preparation coordinator after the verified explorer and lookup publications. Receipt-check each shared input before starting dependent builds. This is an outstanding implementation step; this plan edit does not deploy or queue that coordinator.

## Timing budget approved for planning

These are provisional engineering estimates on this PC, using cache sharing and staged comparisons. Full production 128-bit/OTB/extension throughput has not yet been benchmarked. At November 18 08:56 Pacific the live merge estimate is about 17 hours remaining, with additional final verification.

| Stage | Planning allowance | Evidence or uncertainty |
|---|---|---|
| Remaining merge and explorer verification | 17–24 hours | Live merger estimate, still subject to I/O and final checks |
| Eval DB, independent verification and lookup | 12–48 hours after explorer verification | Full source scaling remains unmeasured; replace estimate after eight-bucket production pilot |
| Shared pool exports and policy/evaluation tables | 12–36 machine-hours after implementation is ready | Requires production timing and storage checks; separate from source extraction |
| Consumer integration, extension implementation, and production pilot | 2–5 working days, partly overlapping infrastructure | The unlimited continuation policy and full new experiment are implementation work |
| Primary 20-book comparison, extensions and initial evaluation | 2–5 days after prerequisites | Reuse engine/Maia work and reduced-parent solves; measure targeted correction costs in the pilot |
| Weight/policy/horizon sensitivities and finalist confirmation | 3–7 further days | About 20 additional base builds before any conditional interactions/rebuilds, plus evaluation |

Allow roughly two to four weeks from this point for the complete staged study, including integration and confirmation, with useful primary results earlier. This is a broad planning range, not a measured completion promise. Historical Rust build receipts show roughly 20–60 minutes per solve. The old 5–9 build/verify rounds were driven largely by the frequency-based verification policy. Do not multiply that historical round count by every new book in the runtime forecast. The approved approach uses stored evaluations, shared quality-based verification and retained-graph corrections. Keep the broad planning allowance provisionally until the pilot measures the new search and correction cost; no specific speedup has yet been demonstrated. Pool exports, integration, extension searches and evaluation remain separate costs.

Update the forecast after the production eval pilot, first shared pool export, first closed Black stricter 2,000-position book, and first fully verified extension run. Record CPU-hours separately from elapsed time and avoid assuming all phases can run concurrently at full speed.

## Source records

- [OTB policy recipe and evidence](otb-opponent-reply-handoff-2026-11-13.md)
- [Maia checkpoint study](maia3-position-only-otb-2026-11-13.md)
- [Earlier plan and recall pilot](../scratch/worktrees/full-key-consumers/docs/lifetime-repertoire-plan.md)
- [Opening gate audit](../scratch/eco-relative-20261114/05-06-opening-check.md)
- [Gambit verification](../scratch/eco-relative-20261114/king-and-from-gates.md)


## Allie history approximation and resource study — November 18, 2026

The user approved a bounded evaluation of history-aware Allie and explicitly requested both incremental runtime/storage versus a position-only model and the quality/cost tradeoff from substituting the most common history. This is an evaluator experiment. It does not replace the approved position-level builder or its OTB ensemble.

Compare four arms using both frozen canonical 2,000-decision books:

| Arm | Context used after the first own out-of-book move |
|---|---|
| Maia position-only | Current board only |
| Allie sampled history | The actual root-to-current simulated move sequence |
| Allie common exit history | Replace the prefix at the book exit with the estimated modal legal prefix; then retain the actual rollout suffix |
| Allie common position history | Substitute the estimated modal legal history at each subsequent neural query |

Before the first own out-of-book move, all arms use the same reference reply policy and paired random draws. Follow prescribed moves on book reentry. A board/ply can have multiple histories: sampling complete games from the initial position supplies correctly weighted histories under that reference policy without enumerating every path. The modal tables are conditioned on the book, position and ply. They are estimated from independent reference simulations and frozen before scoring; no evaluation trajectory may update them. They are not claimed to be exact population modes.

A missing modal entry falls back to actual history with a full-history prediction-cache key. Report availability, replacement, tie, support and fallback rates. High fallback can make a shortcut appear accurate without actually testing much approximation. Assess this coverage before interpreting similarity. The entire-history substitute must replay legally to the identical board and ply. The exit-prefix substitute must preserve the actual suffix and likewise reach the actual board. Repetition and terminal outcomes are determined from the actual sampled game, not from the substitute neural input.

Initial sizing: 1,024 independent opening trajectories and 64 independent full Allie discovery trajectories per color; then 32 games per seed on four paired seeds per color and arm (128 games/color/arm), horizon 50, plus identical cache-hot replays. Referee each distinct nonterminal leaf with the same Stockfish executable and 500,000-node budget, clearing its hash between positions. This is a throughput and disagreement pilot, not enough by design to establish equivalence or choose a production model. Expand after measuring variance and shortcut coverage. If the shortcut is promising, compare at matched query positions and across additional frozen books to test whether it changes repertoire rankings.

The pilot uses available legacy August/September 2026 BRC mover counts through ply 30, K=30, 1450 own/2000 opponent model ratings, temperature 1 and unknown clocks/time control. These constants isolate engineering cost and approximation behavior; they do not supersede the approved OTB model recipe. Beyond the count data's coverage, predictions increasingly rely on the neural prior. Retuning/calibrating Allie and testing the final OTB ensemble remain separate prerequisites for adoption.

Measure and report:

- Wall time for startup, independent-history discovery/table construction, cold probability-cache scoring, hot replay and engine refereeing. Model inference CPU time is also recorded. Shared count caches/model weights mean “cold” refers to prediction caches, not a cold operating system.
- One-time weights/environment storage separately from persistent modal tables, full-history probability caches, compressed game histories and diagnostic artifacts. Record actual bytes, entries and key/value sizes; use compact move encoding and a lossless shared-prefix trie to avoid mistaking verbose JSON overhead for an unavoidable history cost.
- Actual unique history contexts versus unique positions and substituted contexts, cache hit/miss rates, model memory, and total extra cost of each shortcut including building its lookup. Account for amortization over repeated book comparisons; do not count shared inputs twice.
- Paired score differences and Monte Carlo uncertainty, modal coverage/support, and score/ranking changes in follow-up books. A nonsignificant difference is not equivalence. Use the existing 0.002 expected-score practical scale only with adequate uncertainty bounds; a small all-identical sample is not proof of zero effect.
- Separate the cost of Allie itself from history handling. This hardware runs Maia on the RTX 3070 Ti and Allie on CPU because Allie's int8 weights exceed GPU memory. Switching to one history does not remove Allie's weights or guarantee faster inference: replacing prefixes may reduce internal prefix-cache reuse.

Implementation: scratch/allie-history-20261118. Outputs: D:/chess/allie/history-20261118. The full pilot must follow unit tests and an end-to-end smoke test; using Allie's native CPU backend additionally requires same-query probability parity against its reference backend. Limit Allie to two low-priority CPU threads while the production merge runs. Preserve per-stage receipts, immutable atlas checksum, exact replay parity, cache identity and restart points. Do not modify the production migration, builder, canonical books or selected OTB policy as part of this experiment.

Backend check at 16:30 Pacific: 64 matched inputs took 78.84 seconds with the reference int8 backend and 43.64 seconds with native Rust (1.81x faster), but maximum move-probability change was 0.01531 and mean total variation was 0.00586, exceeding the predeclared parity limits. Use reference Torch for the controlled history pilot. These differences do not prove native predictions are worse; native accuracy/calibration requires a separate comparison before treating it as an interchangeable implementation. The reference-backend cost estimate is consequently not a claim about Allie's lowest achievable production cost.


## Native Allie backend quality validation — November 18, 2026

The user explicitly accepts changed predictions if practical predictive quality is nearly unchanged in exchange for the observed roughly 1.8x inference speedup. The original strict numerical-parity failure does not rule out native adoption. Queue a separate validation immediately after the current history/cost study finishes successfully. Keep the current study on its frozen reference backend.

The primary frozen panel has 2,639 decisions by under-2000 movers in 664 deduplicated OTB games dated November 2–11, 2026. It excludes every game from the prior 128-game/512-position pilot. It contains 33 events, six entirely new relative to the pilot, and positions in four strata: plies 1–10 (664), 11–20 (664), 21–30 (661), and 31–50 (650). Events/players can overlap earlier evidence. Use the same Allie 2.1 weights, int8 quantization, temperature 1, unknown clocks/time control and fixed 2200/2200 model ratings for both backends. These ratings are frozen validation inputs, not a calibration choice.

Add up to 1,024 unique query contexts from the completed full-history rollout arm, stratified by book color, 1450/2000 mover strength and ply band. Measure probability changes and K=30 mixture attenuation on those inputs. There are no human target moves on simulated contexts, so do not call their probability agreement an accuracy result.

Compare proper Brier score and log loss, top-move accuracy, probability-change tails, entropy change and subgroup results by ply, mover rating, color, source, event and check status. Inspect the largest regressions on observed human moves. Paired game and event resampling provide uncertainty; they do not capture all repeat-player/source dependence. Mean signed differences across all move probabilities are necessarily zero and cannot prove harmless random noise. Numerical differences may be deterministic.

Predeclare practical noninferiority screening margins:

| Metric | Largest tolerated native deterioration |
|---|---|
| Multiclass Brier score | 0.002 absolute |
| Mean log loss | 0.005 nats per move |
| Top-move accuracy | 0.5 percentage point |

Require upper 95% bounds from both game and event resampling below the loss margins; require the game-resampled accuracy lower bound above the accuracy-loss margin, with a conservative guard for no observed changed classifications. Review subgroup/outlier and reproducibility diagnostics separately before recommending a switch. Passing a screen is not automatic production adoption, and nonsignificance is not equivalence.

Use the same loaded weights and two low-priority CPU threads, alternate reference-first/native-first across blocks of up to 64 matched queries, reset both prefix caches at block boundaries, and report timing by first backend. Run repeat queries with fresh caches to distinguish backend differences from within-backend repeatability and input-order effects. Save each completed backend/block so interruption loses at most one incomplete block; never reuse a result with changed inputs, source code, native binary or settings.

Implementation: scratch/allie-backend-validation-20261118. Outputs: D:/chess/allie/backend-validation-20261118. The queue verifies the current history-study receipt, requires its successful final comparison, and records a blocked state if the dependency fails. It must not modify that active study or production migration. The queued worker writes comparison.json plus queue/status receipts. Validate the metric and completion gates before launching the waiter.


## Online rating-pool backend validation — November 18, 2026

Extend the native/reference comparison to the online populations used for continuation play and stronger opponents: movers rated 1000–1799, 1800+ and 2200+. Membership uses the moving player's rating, not the game's mean. The 1800+ and 2200+ pools overlap and do not count as independent evidence. Preserve the separate OTB results.

Use September 2026 source games. Allie's public documentation says its training includes August 2026, so the combined August/September evaluation tables are unsuitable as an untouched model test. September is beyond that stated training range, but has already been used in this project's development and Allie's published evaluations; label it development validation, not pristine confirmation. Source: https://github.com/y0mingzhang/allie#data and https://github.com/y0mingzhang/allie#comparison .

Frozen panel: 200 eligible games per rating pool and speed, with one eligible mover selected per game and one actual decision per available ply band (1–10, 11–20, 21–30, 31–50). This gives 1,800 pool/game memberships across 1,799 distinct games and 6,934 positions. Sampling uses a deterministic bottom-hash aggregate over the full September eligible-game headers, then reconstructs histories from the original movetext. Bots, abandoned/rules-infraction games, missing scores/ratings and zero-move games are excluded. Validate legal histories before inference.

| Mover pool | Blitz positions | Rapid positions | Classical positions | Total |
|---|---:|---:|---:|---:|
| 1000–1799 | 775 | 760 | 741 | 2,276 |
| 1800+ | 779 | 776 | 772 | 2,327 |
| 2200+ | 784 | 774 | 773 | 2,331 |

Supply the real game prefix and actual white/black ratings to both backends, at temperature 1 with unknown clocks/time control, matching the current clockless inference setting. The existing rollout-context diagnostics separately cover the fixed representative 1450/2000 inputs. Do not tune ratings or temperature on this backend comparison.

Report each pool separately with the same proper-score/accuracy margins, paired game/date uncertainty, depth and time-control slices, probability tails, repeatability and matched-block timing. Equal games per time control prevent Blitz from hiding Rapid/Classical behavior. Also report a descriptive reweighting by each pool's eligible source-game counts per speed; this does not convert a depth-stratified game sample into a move-visit census. No combined accuracy average across overlapping rating pools. Material regressions in one relevant population require investigation even if another population improves.

The frozen panels are reusable for the later Allie-versus-Maia comparison and calibration. This queued job specifically validates native versus reference Allie; it does not automatically switch the deployed model/backend.

Implementation: scratch/allie-online-validation-20261118. Output: D:/chess/allie/online-backend-validation-20261118. Run after the OTB backend validation completes successfully, using two low-priority CPU threads. Preserve the current history pilot and OTB queue receipts. Rating-boundary/overlap tests, real-panel report checks, completion-gate checks and replay of all 6,934 histories passed before scheduling. The independent queue must propagate a failed/blocked dependency instead of launching on an incomplete report.


## Decision, November 19: remaining-risk default; comparison queued

David explicitly approved changing the default to `remaining-maximum` while retaining a comparison with `covered-maximum`. The new default measures the reduction in maximum remaining unbooked severity. Both versions retain continuous connectors, full-key distinct-position cost, the same engine gates and no recall discount or fixed depth cap. The default threshold remains 0.00003.

The versioned machine-readable queue is `sweep-manifest-2026-11-19.json`, under `extension_comparison`. Preserve `sweep-manifest-2026-11-18.json` as the historical recipe; it is not the current default. The queue has 24 extension variants: both metrics x three thresholds (0.00001, 0.00003, 0.00010) x four independent 2,000-position anchors (both colors and both gate settings). Its prerequisites are verified inputs, the accepted Black pilot, four independently verified anchor books and an accepted extension-cost pilot. This is a queued experiment stage, not a newly launched background extraction or extension worker.

Hold the base books, gates, severity formula, opponent probabilities and independent tactical evidence fixed. Reuse shared engine/policy caches, give each metric/threshold its own checkpoint/output identity, and compare selection on a common frozen union of explored continuations so unequal exploration is not mistaken for a metric effect. Incomplete exposure bounds remain OPEN. Report added distinct positions, prescription disagreements, coverage of later difficult moves, remaining risk, reach/support, paired rollout score at a common horizon, runtime and cache reuse. Retain the new default unless the comparison supports a further explicitly documented change.

The default affects new extension runs. Existing covered-maximum checkpoints retain their old metric identity and must not silently resume as remaining-maximum; an explicit old-metric control or a fresh run is required. Base-book choices and caches are unaffected by the extension-default decision. See the progress log for the regression and the outstanding hybrid-screening/convergence work.
