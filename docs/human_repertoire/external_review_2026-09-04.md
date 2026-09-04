**External review of the repertoire builder — 4 September 2026**

Reviewed branch `budget-books`, commit `f249c4d`, using the hand-off at `scratch/handoff_for_external_review.md` and `CLAUDE.md` as guides. The tracked working tree was clean. I inspected the extraction, hashing, eval ingestion, Stage 3, budget planner, baseline, replay and scorecard code, relevant tests, and selected saved run metadata. Trainer coverage was limited to reading selected code and running its existing tests; this is not an exhaustive UI or application-security audit.

The central idea is worth pursuing: choosing moves and deciding where preparation ends belong in the same optimisation. Explicitly modelling human replies, preserving missing outcome mass, and distinguishing storage cost from expected recall are all sound design choices. The current numerical comparisons need stronger validation before being interpreted as improved playing strength. Several implementation discrepancies are large enough in small counterexamples to exceed the reported improvements.

**Validation performed.** All 37 existing test files passed, in 127.1 seconds. I added a separate, disposable [reproduction script](C:/Users/David/Documents/Chess/scratch/python/external_review_repros.py) that imports production functions and demonstrates nine current behaviours. Its [saved output](C:/Users/David/Documents/Chess/scratch/external_review_repros_results.txt) confirms all nine. These comprise seven implementation-defect examples, one optimisation counterexample, and one metric-definition counterexample; they are not estimates of production impact. No production code or datasets were changed, and no full build was launched.

Run the probes from the repository:

```powershell
.venv\Scripts\python.exe scratch\python\external_review_repros.py
```

The probes assert the current problematic behaviour. After a fix, invert the relevant assertions and promote the case into a maintained regression test.

**Findings to address first**

1. **[P1] Aux handling has defects in both engines, and they evaluate different objectives.** At our nodes, budget `build_graph` derives the stopping value from surviving outgoing edges and never reads the aux row. Stage 3 also blends in opponent resignations before our move. At opponent nodes, the budget builder uses raw `other_*` outcomes, whereas Stage 3 blends the OTHER bucket with `other_eval_mean`. These are differences in the objective being evaluated, not budget-allocation effects.

   With matched parameters, a one-edge fixture with 100 continuations scoring 0.6 and 100 opponent resignations gives Stage 3 **0.8** and the budget builder **0.6**. A second fixture gives Stage 3 **0.5** and budget **0.75**, in White-score units, solely because of the missing OTHER eval blend. Those magnitudes are deliberately simple counterexamples, not measured biases in the full book.

   Stage 3 has a separate denominator error in that resignation blend: `cont` counts only surviving outgoing edges. It omits games that continued through below-floor OTHER moves. In reproduction 9, 100 surviving continuations plus 900 OTHER continuations and 100 opponent resignations still produce **0.8**. With the same prepared continuation worth 0.6, the arrival-weighted mixture is **(1000 × 0.6 + 100) / 1100 = 0.63636**. We need the full continuing population in the denominator, even though our policy replaces its historical move choices. This otherwise exaggerates resignations precisely at thin nodes. Horizon mass also needs an explicit denominator treatment.

   Move the shared leaf and aux valuation into one implementation. Define both stopping and prepared continuation on the same arrival population. Add parity tests covering both colours, all aux buckets, partial eval coverage, horizon options and a fixed policy. “Fixed-policy at full capacity equals Stage 3” also needs qualifications: stopping can rationally beat the source's forced continuation, and the two modules currently handle cycles differently.

   Evidence: [budget our-node construction](C:/Users/David/Documents/Chess/python/budget_core.py:446), [budget opponent aux](C:/Users/David/Documents/Chess/python/budget_core.py:559), [Stage 3 resignation blend](C:/Users/David/Documents/Chess/python/stage3_backwards_induction.py:1477), [Stage 3 OTHER blend](C:/Users/David/Documents/Chess/python/stage3_backwards_induction.py:1548). Reproductions 2, 3 and 9.

2. **[P1] The CLI's frontier collection can permanently discard positions above its own reach threshold.** `collect_subgraph_edges` traverses by levels and refuses to update a child once it has been fetched. A transposition may first arrive through a short, unlikely path and later through a longer, much likelier path. The later probability is ignored; descendants pruned on the first visit are never restored. Running the correct max-product calculation on the already-collected graph cannot recover the missing edges.

   The fixture first reaches a position with probability 0.1, later with 0.9. A downstream 20% reply has correct maximum-path reach **0.18**, but its parent rows are missing at epsilon **0.05**.

   Separate “edges fetched” from “reach settled.” Cache fetched adjacency, then re-propagate every improvement in reach, or use a correctly ordered priority queue with a well-defined horizon state. Also distinguish maximum path probability from total probability of reaching a transposed position: the former is **not** an upper bound on the latter when several opponent move orders converge. For a fixed policy, sum the mutually exclusive path mass; use a demonstrably conservative bound during policy-free pruning.

   Evidence: [collector](C:/Users/David/Documents/Chess/python/build_budget_books.py:127), [separate reach calculation](C:/Users/David/Documents/Chess/python/budget_core.py:331). Reproduction 4.

3. **[P1 when crush is enabled] The curve and extracted policy optimise different objectives.** Curves ignore crush; extraction chooses by `value + bonus`, then rejects the chosen move if its value fails to beat stopping. This can return an empty book despite a profitable alternative.

   Example: stop = **0.50**; Solid = **0.55**, bonus 0; Sharp = **0.49**, bonus 0.10. At budget 1, the curve promises **0.55**, but extraction selects Sharp, rejects it, and returns **0.50**. There are **no fused atoms**. Even when the sharp candidate clears stopping, the extracted value can fall below the value used to allocate its ancestors' budgets.

   The current small-budget ladder explicitly uses crush weight **0**, so this particular defect does **not** explain its measured `dp − fixdp` gains. It does block extending that result to the sharp recipe unchanged.

   Choose one Bellman-consistent objective, preserve its selected action and allocations, and separately report the resulting expected score. If crush is only a tie-break, restrict it to an explicitly tolerated score window and measure the root concession. If score must never fall below stopping, filter eligible continuations before applying the tie-break. That local fix alone does not repair the ancestor objective mismatch.

   Evidence: [curve construction](C:/Users/David/Documents/Chess/python/budget_core.py:772), [extraction argmax and stop test](C:/Users/David/Documents/Chess/python/budget_core.py:1017). Reproduction 1.

4. **[P1 for experiment integrity] The edge-crush cache has no input identity.** The default filename is shared by builds in the same output directory, and an existing cache is accepted without checking the histogram, training years, parent set or relevant configuration. It was built only for one subgraph's parents. Reusing it can silently omit newly reachable edges or use the wrong year's statistics, including potential holdout contamination if a later-year cache is reused in a training-only build.

   The probe builds a cache from a 10% event source, requests the same cache from a 90% source, and still receives **10%**. This is a possible leakage route, not evidence that a particular saved comparison leaked.

   Fingerprint the histogram, parent universe, window and schema; fail on mismatch or namespace the cache by that identity. Audit the output-existence skips in the other long-running drivers too. Atomic rename ensures completeness, not freshness. The eval-array fingerprinting is a useful local pattern to generalise. Also, `load_edge_imm` currently returns the raw rate, while Stage 3 applies the crush prior; restore parity if this is meant to mirror Stage 3.

   Evidence: [cache skip](C:/Users/David/Documents/Chess/python/build_budget_books.py:156), [shared default filename](C:/Users/David/Documents/Chess/python/build_budget_books.py:363). Reproduction 7.

5. **[P2] Replay can label an out-of-book final move as an in-book game ending.** Membership is checked before the next SAN token. If the last move enters a missing position, no next token triggers that check, and the remaining book receives `game_end` with a missing exit value. In the probe, `e4 e5` is `game_end`; appending `Nf3` changes the exit at the same position to `opp_out_of_book`.

   Decide membership immediately after each played move, and record game termination separately. If terminal results deliberately take precedence, name that broader category explicitly; it no longer means “game ended inside the book.” This matters because the review relies heavily on the composition of `game_end`.

   Evidence: [finalisation loop](C:/Users/David/Documents/Chess/python/replay_holdout.py:300). Reproduction 5.

6. **[P2] Budget exports retain the source policy's soundness metrics.** The output recalculates `value` and changes `best_move`, but copies `value_worst`, `value_robust` and `crush_potential` from the unconstrained source. `score_repertoire` then presents those fields as properties of the budget book. At least `value_worst` depends directly on the moves and stopping decisions, so this can give a changed book an inherited assessment it did not earn.

   Re-evaluate the extracted policy, including its fallback after preparation ends. Until then, rename these fields as source diagnostics or leave the policy-dependent output fields null. The position's own `eval_score` can remain a position property. This finding is based on direct code inspection, rather than an end-to-end CLI probe.

   Evidence: [budget output rows](C:/Users/David/Documents/Chess/python/build_budget_books.py:490), [scorecard root metrics](C:/Users/David/Documents/Chess/python/score_repertoire.py:98).

**What the budget algorithm does and does not establish**

Keeping the pre-fusion points is useful, but it does not make the allocation exact. Density ordering is exact for divisible concave resource allocation, and for discrete concavity with unit marginal increments. Your compressed cost-k atoms are indivisible packages. The step function used by `eval` is not a discretely concave value function: its marginal gains include zeros followed by a jump.

Reproduction 6 isolates this without any missing child values. Two branches have probability 0.5 each. One offers cost 2, gain 0.6; the other cost 3, gain 0.8. At budget 3, density ordering buys the first package, giving **0.30** at the parent; the exact allocation buys the second, giving **0.40**. The child step values are already exact. Retaining additional child points cannot fix that allocation choice by itself.

Consequently:

- The hand-off's observed association between fusion and errors is useful diagnostic evidence, not a proof that repairing evaluation alone removes all suboptimality.
- `exact_opp_curve` is exact only over the child curves supplied to it. If those curves already lost achievable policies, it cannot recover them.
- The linear interpolation of a concave majorant and the implemented flat steps serve different purposes. Interpolation can be an upper bound under an appropriate relaxation; flat steps can represent feasible packages on a tree. Neither is automatically a certified bound for the final shared-policy, cyclic graph.
- Atom coarsening, horizon truncation, cycle handling, the reach collector, and the optional crush mismatch introduce additional discrepancies. A bound on one straddling atom does not bound all of them.
- Requesting a larger maximum budget can fuse away earlier points and change small-budget answers. The code documents this for `--match-distinct`, but the same issue affects comparing `--budget 20` with a single build requesting both 20 and 1000. Separate graph/cap changes from budget effects.

**My recommended optimisation formulation**

First make an exact, small benchmark with crush disabled and a fixed leaf/aux model. For a tree with path costs, store the complete values for budgets 0 through B, preserve decision backpointers, and perform integer max-plus convolution at opponent nodes. Clip each table to attainable subtree cost. This is straightforward to validate; the worst-case opponent work is quadratic in B per merge, so “B is only 1000” does not make it cheap on two million nodes. Start at the 1–20 ladder on controlled subgraphs.

For the actual shared-position budget, use a mixed-integer model on a reduced, acyclic graph. Here is an exact formulation for the pure-score version of the stated model:

- Give each our-position/move pair a binary variable `x[u,m]`, with `sum_m x[u,m] <= 1`.
- No selected move means stop, with value `L[u]`. A selected move makes `V[u]` equal its child value (or its leaf value).
- Opponent values obey `V[v] = constant[v] + sum_i p[i] V[child_i]`.
- Impose `sum_{u,m} x[u,m] <= B` and maximise `V[root]`.

Use indicator constraints, or correctly bounded big-M equalities, for the conditional our-node equations. Values in [0,1] give tight bounds. Incorporate termination mass as affine branch values once its semantics are fixed. A transposed position shares one decision variable and costs once. The solver's incumbent is a feasible book; its best bound quantifies how far from optimal it could be. A time-limited solve is still informative.

This is exact for the finite graph and specified stop model, not for chess or an unknown human population. It is not a proposal to give a solver all 17 million positions. Start with small high-reach subgraphs and use the existing planner as a candidate solution. Measure solvability before expanding.

There is a reason shared-cost optimisation feels difficult. A simple set-cover reduction fits this model: an opponent root selects an element; our element-node chooses one containing set; a free deterministic hop reaches the shared set-node; preparing that node gives payoff 1. Achieving root value 1 with budget `number_of_elements + k` requires covering every element using at most k shared set-nodes. Thus even a shallow version encodes an NP-hard problem. A general polynomial exact algorithm is not a reasonable expectation.

Lagrangian penalties are useful for warm starts and supported points on a frontier, but varying a multiplier can skip valid intermediate integer budgets. Bisection does not remove that gap. Nor is `--match-distinct` an exact optimiser when its distinct-count function is nonmonotone. Report the best feasible book with **at most B** actual cards, and never call an overshoot an equal-budget result. For practical approximation, improve entire prerequisite bundles and shared continuations with exact marginal re-evaluation; single-node greedy has no general diminishing-returns guarantee in the presence of trap prerequisites.

**The 817-decision result is not evidence of saturation**

The saved [1,000-budget metadata](E:/chess/repertoire/_budget/b1000_white_b1000.parquet.meta.json) records:

| Quantity | Saved value |
|---|---:|
| Requested budget | 1,000 |
| Distinct booked decisions | 817 |
| Root curve capacity | 1,000 |
| Root curve value | 0.615932 |
| Realised model value | 0.615395 |
| Cycle nodes flattened | 1,759 |

In the matching [400-budget output](E:/chess/repertoire/_budget/b1000_white_b400.parquet.meta.json), curve value is 0.606100 and realised value 0.607986. The model is still buying additional value between those requests. Capacity 1,000 is also the imposed build cap; it tells us nothing about the unconstrained capacity beyond it.

Furthermore, `spent_paths` increments once per booked node in the deduplicated traversal; it is numerically the distinct count, as an existing test explicitly acknowledges. It is not an independent audit of the curve's path charges. Rename it, and report requested allocation, atom cost allocated, allocation lost to coarsening/stopping, and distinct storage separately.

The realised value below the curve in the 1,000-budget artifact is already a reason to audit the claimed achievable-value contract on realistic transposition/cycle graphs. Its cause is not identified by this review.

For a saturation experiment, fix the graph, candidate set, leaf model and optimiser accuracy. Vary B and plot realised value against **actual distinct cards**, with solver/approximation bounds. Separately relax epsilon, share floor, horizon and atom cap one at a time. Track aggregate probability mass excluded at each frontier. Only call it gain exhaustion after these checks stop moving the curve.

**The graph and stopping assumptions need an explicit contract**

A graph keyed solely by the Polyglot position hash is not a DAG. Ordinary knight repetitions create cycles, and both modules already have cycle handling. Stage 3 iterates strongly connected components; the budget builder flattens selected nodes and breaks traversal dependencies. These procedures need not value the same policy identically.

Position identity also omits repetition history and the halfmove clock. Even a correctly implemented hash is therefore not a complete game state. This is a recognised [graph-history interaction problem](https://webdocs.cs.ualberta.ca/~mmueller/ghi.html). For the small exact benchmark, use a finite horizon and an explicit repetition policy. A state such as `(position, ply)` makes the horizon acyclic but does not by itself encode repetition claims; add history where relevant. Share the learning decision across occurrences of the same flashcard position if that is the intended product.

The current stop value assumes that preparation ends permanently and the player thereafter follows the population's policy. That is a modelling choice. A person may leave the book, reach a familiar transposition later, and resume preparation. Initially keep the absorbing-stop simplification, label it, and evaluate whether allowing re-entry changes conclusions. Ultimately a personal fallback and measured recall probability from the trainer will be more relevant than a generic population fallback.

**A cleaner empirical evaluation**

The central problem is selection on actions and players, not merely selection on exit reason. A rare prescribed move selects the people who chose it; deeper agreement selects them again. Adjusting the distribution of `game_end` cannot correct that. `faithful_pct` is also not pure repertoire coverage: an empty or root-stopping book can have nearly every game counted faithful, while a deeper book creates more opportunities for our-side deviation. Measure coverage under a policy that actually follows our prescribed moves, with opponent behaviour estimated separately.

Likewise, comparing the last in-book parent's unconditional value with outcomes conditional on a particular exit is not a calibration test of the same estimand. Split-by-reason tables remain useful diagnostics, but each prediction must condition on the corresponding action/exit event before that table can be interpreted as calibration.

I would use the following evaluation sequence:

1. Freeze recipes and train them on a historical window. Fit opponent distributions and fallback outcomes on a separate validation window. Evaluate all fixed books with the **same** validation evaluator, rating/time-control mix, horizon and terminal treatment. This is model-based out-of-sample evaluation, not a causal guarantee. Use several rolling windows. A repeatedly inspected 2025 set is a development set; reserve another untouched period for the final comparison.
2. Condition on our rating, opponent rating and their difference, time control, date and available player history. The present average-rating threshold admits, for example, a 2200–1400 pairing. That need not resemble the equal-strength opposition the repertoire is intended for. The cleaned source already retains individual ratings, game identity and date, so this information has not all been lost upstream.
3. If adding importance weighting, weight the actual sequence of prescribed **moves**, not exit labels. Sequential doubly robust estimators combine an outcome model with action-probability corrections; they are a relevant tool here. They require overlap and identification assumptions, and long deterministic prefixes can produce extreme weights. Report effective sample size `(sum w)^2 / sum(w^2)`, clipping sensitivity and unsupported regions. See [Jiang and Li](https://arxiv.org/abs/1511.03722).
4. Opening expertise, preparation and player style may remain unobserved confounders after controlling for rating. Doubly robust does not mean robust to arbitrary hidden confounding. Use sensitivity analysis or bounded claims, as motivated by [Namkoong et al.](https://arxiv.org/abs/2003.05623), and treat personal prospective results as a later validation layer.
5. Use paired comparisons on the same evaluation games/positions, block uncertainty by time and player where possible, and measure the whole selection pipeline on manageable samples. A fixed SD divided by square-root of game count does not account for player dependence, model fitting or recipe selection.

A common evaluation distribution also lets you distinguish *different openings* from *different depth*. On shared our-turn positions, compute reach-weighted move disagreement; separately measure the mass where one book stops and the other continues. Report the first disagreement depth and root-move frequencies. Show a few high-impact changed branches with their continuation cost, stopping value and value at each budget. ECO labels alone miss transpositions and meaningful changes within one named opening. A `dp − fixdp` value gap proves improvement within the evaluated model; it does not by itself prove a different opening family.

**A better definition of sharp but sound**

I would keep expected points as the main performance metric and report win/draw/loss separately. If wins are more valuable than draws for your use case, specify that utility directly; a drawish equal score and a volatile equal score then differ by design.

The winpos implementation measures the next **above-threshold observation**, not necessarily a fresh crossing. Reproduction 8 passes three successive +400 evaluations to `game_events`; earlier edges receive events even though the game was already above +300. That contradicts the stronger comment that an already-winning game must not credit the move, although it matches the implemented “first later qualifying position” rule. This is a definition issue to resolve, not evidence that the extractor fails to match its SQL oracle.

There is a related probability issue in `imm + (1-imm) * gamma * child_crush`: the immediate window overlaps the child's future-event window. Multiplying by `(1-imm)` is a valid hazard decomposition only if the continuation probability is conditional on the event **not** occurring in the immediate window. The present aggregate continuation has no such conditioning. Repeatedly observing a won position or the same imminent win can reward it more than once.

A coherent alternative is a finite-horizon first-hit objective: probability of reaching a decisive advantage before a decisive disadvantage, optionally discounted by time to that first hit. Make winning and losing threshold states absorbing for this metric, and propagate it under the **budgeted** policy. If a transient engine spike is undesirable, define a sustained advantage or a later conversion outcome explicitly. Store observation coverage and distinguish an unknown eval from evidence of no crossing.

For “sharpness” as practical difficulty, also report the expected engine loss in opponents' human replies, the number/probability of adequate defences, and the analogous difficulty on our side after a forgotten move. Tune those separately from soundness. A useful repertoire makes opponents find hard moves while giving us follow-ups we can actually remember and play.

The gates are useful safeguards, but the all-gated fallback drops the constraint and selects by the full utility key. It is not necessarily choosing the objectively least-bad move. Engine augmentation helps, but does not turn the fallback into a safety guarantee. Record fallback reason and gate violation; if the position itself is defensible but no candidate is certified, stop or obtain a consistent engine analysis. If every legal continuation is losing, represent that state honestly and choose according to an explicit best-defence fallback. Propagate an audit of the actual selected policy.

**Calibration and data changes I would make**

- Fit the centipawn-to-outcome mapping on your target population. Lichess's published coefficient was fitted using 75,000 positions from 2300+ rated rapid games, excluding forfeits and very short results; it is not a calibration study for your pooled population. This is documented in the [original Lichess change](https://github.com/lichess-org/lila/pull/11148). Verify calibration against `P(win) + 0.5 P(draw)` before treating the sigmoid as interchangeable with empirical expected points.
- Preserve engine version, depth, source, mate status and analysis date where possible. Version matters in addition to depth: [Stockfish 15.1 changed its reported evaluation convention](https://stockfishchess.org/blog/2022/stockfish-15-1/). For high-reach moves near a gate, analyse parent and all relevant children with one engine and consistent resources. A universal 0.02 margin cannot characterise all those disagreements.
- Use a hierarchical W/D/L model with rating and time-control effects and tune shrinkage by validation. Treating a draw as half a success gives a useful shrinkage mean, but it is not literally a Binomial likelihood for a three-outcome game. Dirichlet W/D/L counts make uncertainty and draw preference explicit.
- Add recency weighting or rolling windows, then measure sensitivity across time controls and strength bands. More old data can reduce variance while increasing bias. Maximising over millions of noisy alternatives also produces winner's-curse effects that a fixed pseudocount alone does not quantify.
- Define terminal handling consistently. Actual checkmate/stalemate should have exact rule outcomes. Voluntary resignations, agreements and clock results reflect human behaviour. Stage 3 includes our-node wins while excluding our-node losses/draws as deviations; that is a counterfactual assumption that should be tested, not treated as a neutral accounting identity. Preparation can prevent premature resignation, but cannot prevent a checkmate already on the board.
- Preserve inexpensive monthly/stratified sufficient statistics and manifests after extraction, so later recency or rating questions do not demand another full replay. Keep enough provenance to distinguish new code, stale artifacts and results produced with old semantics.

**Documentation and engineering follow-through**

The hand-off is already stale in several material places. The locked recipe uses crush prior **5000** with a **zero** baseline, not 20,000 toward the slice mean. Its eval gate reads the reached position's engine evaluation, which differs from tracing only recorded opponent replies. The graph is cyclic. Stage 3 already contains the our-turn resignation blend, although whether saved artifacts include the required extraction data is a separate question. The README's pawn-unit gate description also needs correction. `budget_core` mentions a CLI `--exact-alloc` flag that the current parser does not expose.

Consolidate configuration into a versioned resolved recipe shared by build, evaluation and export. Save code revision, dependency versions, input fingerprints, all resolved flags and schema/metric versions. Make resume gates verify those identities. Pin a tested environment as well as listing permissible dependency ranges.

Extend tests around externally meaningful contracts: budget feasibility, reconstruction of promised values, consistent gate guarantees, matching objective across methods, invariance to unrelated requested budgets, and valid mass accounting. Preserve existing characterization tests, but do not mistake a test that locks in an approximation for evidence of correctness. The random budget oracle currently generates trees with zero crush; broaden it deliberately to shared nodes, cycles, aux mass and the actual enabled objective. Keep scale tests separate and cheap enough that the ordinary suite remains usable.

**Recommended next sequence**

1. Repair aux parity and frontier collection; correct replay exits, cache identity and exported metric provenance. Keep the budget comparison's crush weight at zero initially.
2. Build an exact reduced-graph benchmark and independent policy evaluator. Validate the 1–20 ladder against actual unique storage budgets and solver/DP bounds, using the same frozen graph and terminal model.
3. Recompute the four-arm comparison on a common held-out evaluator with rolling windows and uncertainty. Inspect the highest-impact policy changes, rather than relying only on a root scalar.
4. Implement a coherent budget-dependent sharpness objective and a selected-policy soundness audit. Then test sensitivity to rating, recency, fallback skill and recall.
5. Expand to larger budgets only after allocation error and evaluator uncertainty are small relative to the claimed improvement. Treat the 817-card result as a diagnostic to explain, not a finding about the limits of opening preparation.
