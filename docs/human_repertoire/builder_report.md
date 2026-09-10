# Bounded multi-branch repertoire builder

The reusable selection loop is working: it generates sound candidate moves, searches connected preparation packages under a position budget, recomputes encounter frequency and recall, compares all books with the same evaluator, and exports annotated legal PGNs.

This run produces 9 provisional White books after **1. e4**, across budgets **10, 25, 50**. They are research outputs, not a validated replacement for the existing repertoire. The lookahead ends after move 4 by Black; unexpanded positions remain explicitly unresolved.

To inspect the highest-scoring book found at each budget under the common human evaluator: [budget 10: 10 cards](builder_books/best_available_10.pgn), [budget 25: 25 cards](builder_books/best_available_25.pgn), [budget 50: 48 cards](builder_books/best_available_50.pgn). These copies preserve the originating method in their PGN headers; no search results were changed after comparison.

## Books and common evaluation

**Nested PGNs for study import:** [10 positions](builder_books/nested/best_available_10.pgn), [25 positions](builder_books/nested/best_available_25.pgn), [48 positions](builder_books/nested/best_available_50.pgn). Each file contains one game with nested variations and preserved STUDY comments. Original per-card exports remain available above.

Each percentage is conditional on reaching the starting position after 1. e4. The score is discounted probability of reaching +300 cp before -300 cp within the horizon, on paths with enough known evaluation. The interval adds unresolved first-hit mass to that lower bound; it is a conservative model-coverage bound, **not a statistical confidence interval or win rate**. Scores average Blitz and Rapid equally.

| Budget | Selection method / PGN | Actual positions | Known early advantage | Coverage bound | Observed first disadvantage |
|---:|---|---:|---:|---:|---:|
| 10 | [Existing-reference projection](builder_books/reference_10.pgn) | 10 | 0.120% | 0.12%–51.71% | 0.923% |
| 10 | [Perfect-recall selection](builder_books/perfect_selection_10.pgn) | 10 | 0.415% | 0.42%–69.20% | 1.827% |
| 10 | [Human-recall selection](builder_books/human_10.pgn) | 10 | 0.402% | 0.40%–65.19% | 1.873% |
| 25 | [Existing-reference projection](builder_books/reference_25.pgn) | 25 | 0.117% | 0.12%–49.48% | 1.499% |
| 25 | [Perfect-recall selection](builder_books/perfect_selection_25.pgn) | 25 | 0.439% | 0.44%–58.76% | 1.643% |
| 25 | [Human-recall selection](builder_books/human_25.pgn) | 25 | 0.439% | 0.44%–58.78% | 1.656% |
| 50 | [Existing-reference projection](builder_books/reference_50.pgn) | 50 | 0.138% | 0.14%–44.37% | 0.992% |
| 50 | [Perfect-recall selection](builder_books/perfect_selection_50.pgn) | 37 | 0.440% | 0.44%–57.22% | 2.277% |
| 50 | [Human-recall selection](builder_books/human_50.pgn) | 48 | 0.441% | 0.44%–56.92% | 2.281% |

With no studied cards, the same model gives 0.069% known early advantage and a 0.07%–89.64% coverage bound. Adding cards is optional; an allowed budget is a ceiling, not a target to fill. Observed disadvantage also has missing-path uncertainty and is not a certified risk bound.

The human-selected score moves from 0.402% with 10 cards to 0.441% with 48 cards. Compare this small covered-mass difference with the much wider coverage bounds before interpreting the additional study cost. The run demonstrates automatic allocation across branches; it does not resolve a whole-book ranking.

A useful search limitation appeared: at budget 10, perfect_selection found the highest-scoring book under the common human evaluator. The best-available exports use those books. Different search paths can reach different local solutions; these comparisons do not isolate recall assumptions as the sole cause of score differences.

The reference retains canonical move choices, ordered by baseline fallback encounter frequency to fit each budget. This adaptation is not a fresh run of the original unrestricted optimiser. Its existing learnability settings are preserved in the source metadata; it is not described as a perfect-recall method. Both searches can start from this reference seed. The perfect-recall arm changes **selection** only: every exported book is finally scored using the same human recall model.

The optimiser maximises the known early-advantage lower bound, breaking ties by fewer positions. It can substitute moves, add connected follow-ups and remove/replace prescriptions. Search stops at the declared time/evaluation cap or a local plateau; none of these results proves a global optimum. Wider coverage could change the rankings.

## Branch allocation and selected moves

Counts below use one representative legal route per card. A transposed position still costs one card and can be recognised through other routes.

| Budget | Human-book positions by Black first reply | Changes from reference | Human vs perfect-selection prescriptions differing |
|---:|---|---|---:|
| 10 | 1...c5: 2, 1...c6: 1, 1...d5: 2, 1...e5: 3, 1...e6: 1, 1...g6: 1 | 1 substituted, 3 added, 3 omitted | 4 |
| 25 | 1...Nc6: 1, 1...Nf6: 1, 1...b6: 2, 1...c5: 5, 1...c6: 3, 1...d5: 6, 1...e5: 3, 1...e6: 3, 1...g6: 1 | 5 substituted, 10 added, 10 omitted | 2 |
| 50 | 1...Nc6: 1, 1...Nf6: 1, 1...b6: 2, 1...c5: 8, 1...c6: 7, 1...d5: 7, 1...d6: 2, 1...e5: 12, 1...e6: 6, 1...g6: 2 | 8 substituted, 16 added, 18 omitted | 12 |

The human-selected book with budget 50 contains the following prescriptions. Full routes are shown to make side and move number unambiguous. Recall percentages are model assumptions applied to candidate-dependent exposure, not measured memory.

| Full route through prescribed White move | Predicted recall, Blitz / Rapid |
|---|---|
| 1. e4 Nc6 2. Nf3 | 68.0% / 68.0% |
| 1. e4 Nf6 2. e5 | 83.7% / 83.7% |
| 1. e4 b6 2. d4 | 70.8% / 70.8% |
| 1. e4 c5 2. d4 | 99.9% / 99.9% |
| 1. e4 c6 2. Nf3 | 98.9% / 98.9% |
| 1. e4 d5 2. exd5 | 98.6% / 98.6% |
| 1. e4 d6 2. d4 | 95.0% / 95.0% |
| 1. e4 e5 2. Bc4 | 99.9% / 99.9% |
| 1. e4 e6 2. d4 | 99.3% / 99.3% |
| 1. e4 g6 2. d4 | 90.8% / 90.8% |
| 1. e4 b6 2. d4 Bb7 3. Nc3 | 52.2% / 53.1% |
| 1. e4 c5 2. d4 Nc6 3. d5 | 59.2% / 59.2% |
| 1. e4 c5 2. d4 cxd4 3. Nf3 | 99.8% / 99.8% |
| 1. e4 c5 2. d4 e6 3. Nf3 | 75.2% / 75.2% |
| 1. e4 c6 2. Nf3 d5 3. e5 | 98.7% / 98.7% |
| 1. e4 c6 2. d4 d5 3. e5 | 0.0% / 0.0% |
| 1. e4 d5 2. exd5 Nf6 3. Nc3 | 86.9% / 86.9% |
| 1. e4 d5 2. exd5 Qxd5 3. Nc3 | 96.2% / 96.2% |
| 1. e4 d6 2. d4 Nf6 3. Nc3 | 84.4% / 84.5% |
| 1. e4 e5 2. Bc4 Nc6 3. Qf3 | 99.0% / 99.0% |
| 1. e4 e5 2. Bc4 Nf6 3. d4 | 99.1% / 99.1% |
| 1. e4 e5 2. Nc3 Nf6 3. Nf3 | 0.0% / 0.1% |
| 1. e4 e5 2. Nf3 Nc6 3. Bc4 | 97.5% / 97.5% |
| 1. e4 e6 2. d4 d5 3. Nc3 | 98.9% / 98.9% |
| 1. e4 c5 2. d4 cxd4 3. Nf3 Nc6 4. c3 | 99.4% / 99.4% |
| 1. e4 c5 2. d4 cxd4 3. Nf3 Nf6 4. e5 | 66.4% / 66.4% |
| 1. e4 c5 2. d4 cxd4 3. Nf3 d6 4. Qxd4 | 90.2% / 90.2% |
| 1. e4 c5 2. d4 e6 3. Nf3 cxd4 4. Qxd4 | 90.6% / 90.7% |
| 1. e4 c6 2. Bc4 d5 3. exd5 cxd5 4. Bb3 | 0.2% / 0.1% |
| 1. e4 c6 2. Nf3 d5 3. e5 Bf5 4. d4 | 88.8% / 88.8% |
| 1. e4 c6 2. Nf3 d5 3. e5 Bg4 4. h3 | 94.7% / 94.8% |
| 1. e4 c6 2. Nf3 d5 3. exd5 cxd5 4. Bb5+ | 0.6% / 0.5% |
| 1. e4 d5 2. exd5 Nf6 3. Nc3 Nxd5 4. Nxd5 | 78.7% / 79.6% |
| 1. e4 d5 2. exd5 Nf6 3. Nf3 Nxd5 4. d4 | 0.0% / 0.0% |
| 1. e4 d5 2. exd5 Qxd5 3. Nc3 Qa5 4. Nf3 | 81.6% / 81.5% |
| 1. e4 d5 2. exd5 Qxd5 3. Nc3 Qd8 4. Nf3 | 74.4% / 74.4% |
| 1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Bxc6 | 97.0% / 97.0% |
| 1. e4 e5 2. Nf3 Nc6 3. Bc4 Be7 4. d4 | 93.2% / 92.5% |
| 1. e4 e5 2. Nf3 Nc6 3. Bc4 Nf6 4. d4 | 90.5% / 89.3% |
| 1. e4 e5 2. Nf3 Nc6 3. Bc4 d6 4. d4 | 51.7% / 39.1% |
| 1. e4 e5 2. Nf3 Nc6 3. Bc4 h6 4. d4 | 93.2% / 92.5% |
| 1. e4 e5 2. Nf3 Nc6 3. d3 Nf6 4. c3 | 20.1% / 6.4% |
| 1. e4 e5 2. Nf3 Nc6 3. d4 exd4 4. Bc4 | 62.8% / 23.2% |
| 1. e4 e6 2. d4 d5 3. Nc3 Nf6 4. e5 | 93.1% / 93.1% |
| 1. e4 e6 2. d4 d5 3. Nc3 dxe4 4. Nxe4 | 79.2% / 79.2% |
| 1. e4 e6 2. d4 d5 3. e5 c5 4. Nf3 | 0.2% / 0.2% |
| 1. e4 e6 2. d4 d5 3. exd5 exd5 4. Nc3 | 0.2% / 0.1% |
| 1. e4 g6 2. d4 Bg7 3. Nc3 d6 4. Bf4 | 2.9% / 3.5% |

PGNs contain one game per distinct study card. **STUDY** comments mark prescribed moves; unmarked White moves provide a route to the card and are not additional prescriptions. Alternative move orders can therefore appear without adding to the budget.

## Scope, evidence and limitations

- Candidate pool: 77 positions / 170 moves after gates, with 75 eligible canonical prescriptions. Exclusions: {'gate_not_audited': 55, 'soundness_gate': 15}.
- Graph: 11,855 positions per time-control policy; 12,532 edges. Preparation used 422 new Maia inferences in its completing session (124.2 seconds).
- Engine gates: 256 positions audited at 100,000 nodes / up to 2 seconds each. Every candidate requires both parent and child in this audit, child ≥−100 cp, and loss ≤150 cp. This is bounded engine evidence, not a deep soundness certificate.
- Independent verification passed for 18 time-control/book cases and 24,538 legal graph edges, including empirical mass accounting, path-sum scores, exposure, budgets, candidate gates and PGN reconstruction.
- Opponents use the authoritative pooled mean-Elo≥1800 BRC position counts. This pool is not a both-players≥1800/rating-gap≤200 filtered sample. Blitz and Rapid labels distinguish White fallback models, not separate opponent datasets; Classical uses the declared Rapid transfer and is not an independent third validation arm.
- White fallback is Maia-2 at 1400, conditioned on a 2000-rated opponent. Per-all-games entry probability 10% is an explicit provisional scenario. The familiar e4 e5 Nf3 Nc6 tabiya has strength .99; no universal familiarity boost was assigned to the initial board.
- Recall combines per-game frequency, distance from a declared familiar tabiya and absolute ply. It is recomputed for every proposed book. These settings need personal calibration; the shallow horizon barely exercises the deeper-memory decay.
- Known earlier evaluations are overlaid from the canonical arrays; candidate parents/children use this run's fresh engine audit. Mixed engine budgets and incomplete graph expansion limit outcome interpretation. Computational cutoffs are not evidence that empirical opponent data is sparse.
- Horizon W/D/L uses the existing neutral placeholder. These outputs do not establish acceptable full-game performance or causal improvement in playing strength.

## Search effort

| Method | Budget | New book evaluations | Seconds | Stop |
|---|---:|---:|---:|---|
| human | 10 | 693 | 71.3 | local_plateau |
| human | 25 | 1200 | 118.9 | local_plateau |
| human | 50 | 1800 | 167.8 | evaluation_cap |
| perfect_selection | 10 | 826 | 67.2 | local_plateau |
| perfect_selection | 25 | 1033 | 66.5 | local_plateau |
| perfect_selection | 50 | 1070 | 61.8 | local_plateau |

For the budget-50 human book, mean route mass by frontier reason: earlier_layer: 0.000%, empirical_absent: 0.001%, empirical_tail: 0.028%, expansion_cap: 58.317%, horizon: 41.396%. These are route-exit masses, not additional uncertainty to add to the bounds above.

## Reproduction and decisions

Configuration and prospective limits: [builder_config.json](builder_config.json), [builder_plan.md](builder_plan.md). Run instructions: [research README](../../research/human_repertoire/README.md). Tracked [results and recall details](builder_books/summary.json) and [source manifest](builder_books/source_manifest.json) preserve the delivered experiment. Larger input snapshots, engine audit and checkpoints live under `research/human_repertoire/_artifacts/builder/e4_v1/`.

Decisions made for this delivery: use a bounded e4 region across multiple replies; use 10/25/50 position ceilings; compare against a canonical projection and a distinct perfect-selection ablation; optimise the common covered-mass objective; retain provisional books at resource caps; and report unresolved coverage instead of opening another single-move investigation. The delivered capability now answers which cards the current model selects across branches, rather than only whether one extra card helps.

The main next development constraint is coverage-aware continuation allocation for competing books, especially beyond the fourth move. That work can now operate through this builder and report whole-book tradeoffs; it need not restart another chain of isolated Be3 experiments.
