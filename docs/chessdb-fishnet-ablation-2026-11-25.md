# Where Fishnet helps — November 25, 2026

David asked whether Fishnet adds value when another source is available, including
50/50 Cloud–ChessDB against 30/30/40, and Cloud alone against 50/50 Cloud–Fishnet
with ChessDB artificially withheld. **Fishnet helps mean accuracy on the triple-
overlap population, including both requested comparisons. Its benefit depends on
coverage; it does not justify using the same fallback for every missing source.**

No additional Stockfish searches were run. Existing selections and production
bindings remain unchanged. The independent final 300-position panel was **not
read or rescored for these new contrasts**. Its earlier three-rule confirmation
remains intact; the following are exploratory analyses of already exposed selection
data, with older/newer panels reported separately.

## Matched comparisons

Values are mean absolute expected-score error in percentage points, giving equal
weight to source-quality strata. Lower is better. Three-source overlaps use four
strata: deep/shallow Cloud × once/repeated NNUE Fishnet. Naturally missing-Cloud
positions use equal once/repeated weights. These are different populations and
their aggregate levels must not be compared as if sampled identically.

| Situation | N | Without Fishnet | With Fishnet | Effect of including Fishnet |
| --- | ---: | ---: | ---: | --- |
| All three available: 50/50 Cloud–ChessDB versus 30/30/40 | 1,008 | 1.05258 | **0.94053** | 10.6% lower mean error |
| Simulate missing ChessDB: Cloud versus 50/50 Cloud–Fishnet | 1,008 | 1.36186 | **1.05850** | 22.3% lower |
| Simulate missing Cloud: ChessDB versus 3/7 Fishnet + 4/7 ChessDB | 1,008 | 1.18541 | **1.02897** | 13.2% lower |
| Cloud actually missing: ChessDB versus 3/7 Fishnet + 4/7 ChessDB | 699 | **0.99173** | 1.16188 | 17.2% higher |

The last two rows are the key distinction. Removing a source from a matched board
answers a useful ablation question, but it does not reproduce the distribution
of boards where that source never supplied an evaluation. Cloud availability
appears informative about evaluation quality; this analysis does not establish
why, and we should not assert a causal explanation from these samples.

The same directions appear on the new selection panel alone:

| Situation | N | Without Fishnet | With Fishnet |
| --- | ---: | ---: | ---: |
| Three-source blend versus 50/50 Cloud–ChessDB | 600 | 1.02439 | **0.94721** |
| Simulated missing ChessDB | 600 | 1.34718 | **1.08804** |
| Simulated missing Cloud | 600 | 1.16796 | **1.06623** |
| Actually missing Cloud | 199 | **1.04347** | 1.16797 |

For the first two requested comparisons, Fishnet improves mean error in **all four
new overlap strata**. These are descriptive subgroup results, not four independent
confirmation tests. Keeping only new references at actual depth >=28 preserves
both improvements (595 positions): 0.93883 versus 1.01936 for three-source versus
50/50 Cloud–ChessDB; 1.07573 versus 1.33458 for Cloud–Fishnet versus Cloud alone.

## Other no-Fishnet controls and uncertainty

The original frozen 135-recipe grid already included pairwise blends and individual
sources. This analysis makes the Fishnet contrasts explicit and adds exact removal
of Fishnet while preserving the other sources' 3:4 ratio.

- Three-source versus 3/7 Cloud + 4/7 ChessDB: **0.94053 versus 1.04046** pooled;
  **0.94721 versus 1.01207** on the newer 600.
- Select the best no-Fishnet rule on the older panel alone, including cp-space
  and score-space grid options: **30% Cloud + 70% ChessDB in score space**.
  On the newer panel it scores **1.01870**, versus three-source **0.94721**.

These controls support Fishnet's contribution beyond the user's 50/50 example.
A lower-quality individual predictor can still help a blend when its errors
differ from the others. Descriptively, pooled signed-score-error correlations
are 0.051 Cloud/Fishnet, 0.269 Cloud/ChessDB and 0.313 Fishnet/ChessDB. Low
Cloud/Fishnet error correlation is consistent with useful complementary information;
it is not proof that source errors are independent.

Paired stratified bootstrap: 10,000 repetitions, seed 202611253. Differences
below are with-Fishnet minus without-Fishnet MAE; negative favors Fishnet.
Intervals are adjusted within each panel for six stated contrasts. They do not
cover selection over the entire prior grid or every subgroup/depth sensitivity.
All these new analyses are exploratory, and the bootstrap does not capture
opening-family dependence or systematic engine-reference error.

| Contrast, newer panel | Difference | Ordinary 95% interval | Six-contrast simultaneous interval |
| --- | ---: | ---: | ---: |
| Three-source vs 50/50 Cloud–ChessDB | -0.07718 | [-0.12826, -0.02289] | [-0.14713, -0.00105] |
| Three-source vs ratio-preserving removal | -0.06486 | [-0.11517, -0.01286] | [-0.13066, +0.00268] |
| Three-source vs older-selected no-Fishnet rule | -0.07149 | [-0.12017, -0.02082] | [-0.13707, -0.00126] |
| Cloud–Fishnet vs Cloud, simulated no ChessDB | -0.25914 | [-0.34546, -0.16897] | [-0.37454, -0.13780] |
| Fishnet–ChessDB vs ChessDB, simulated no Cloud | -0.10173 | [-0.17182, -0.03101] | [-0.19714, -0.00508] |
| Fishnet–ChessDB vs ChessDB, actual no Cloud | +0.12450 | [-0.01466, +0.27135] | [-0.05753, +0.32488] |

The actual-no-Cloud disadvantage is clearer pooled: +0.17015, simultaneous
interval [+0.06960, +0.27718]. Its newer-only interval includes zero, so the
direction replicates but that smaller sample is inconclusive by itself. Several
other adjusted endpoints are close to zero; do not overstate precision.

## Tails and gates

Mean accuracy is not the only criterion:

- On the 1,008 overlaps, three-source p99 error is **3.88952**, worse than
  50/50 Cloud–ChessDB **3.67614** (and ratio-preserving removal **3.50074**).
  Conversely, its clear false admissions are lower than 50/50 Cloud–ChessDB in
  every gate arm: permissive White **5 vs 11**, permissive Black **14 vs 24**,
  stricter White **22 vs 29**, stricter Black **17 vs 28**. False exclusions rise.
- Cloud–Fishnet improves pooled p99 versus Cloud alone (**4.66587 vs 4.89247**)
  and reduces clear false admissions in every arm. More false exclusions remain
  a tradeoff, not a guarantee of uniformly better decisions.
- In naturally missing-Cloud positions, Fishnet–ChessDB worsens p99
  (**5.79165 vs ChessDB 4.09415**) as well as mean error, yet decreases clear
  false admissions at both stricter floors: White **7 vs 15**, Black **7 vs 10**.
  Thus "ChessDB is better without Cloud" refers to accuracy/tails, not every
  safety metric. Retaining Fishnet as disagreement evidence may still be useful.

These prospective gate counts are measured on shared positions, not selected
book moves or game failure rates. Actual relative-gate decisions and reach effects
still require the book pilot.

## Recommendation and execution scope

1. Keep Fishnet in consideration when all three sources are available. Removing
   it everywhere is contradicted by these matched comparisons.
2. Keep ChessDB-only as the current candidate point estimate when Cloud is actually
   missing, pending book-level safety validation. Preserve Fishnet values for
   disagreement/verification rather than deleting their information. The current
   study does not validate a new disagreement-triggered verification threshold.
3. Cloud–Fishnet is promising when ChessDB is unavailable, but test **genuinely
   ChessDB-missing** positions before treating the simulated result as validated
   production behavior. First measure whether this population is material and
   how many such positions occur in the retained builder graph; do not automatically
   launch another large engine batch merely to test an unused fallback.
4. Keep complete-case accuracy, fallback quality, gate safety and book-level
   performance separate in acceptance. Do not retune on the already opened final
   300, silently replace the locked recipe, or change production from this analysis.

This follow-up adds no new engine jobs and does not alter the running mini pool.
The potential future naturally-missing coverage audit is documented, not launched
as a new bulk database scan or independent confirmation sample.

## Reproduction and artifacts

One-off source (intentionally in gitignored scratch, not a maintained entrypoint):
`scratch/fishnet-ablation-20261125/analyze.py` in the maintained worktree.
It reads the verified pooled selection artifact and existing old/pooled metrics;
it does not open final-confirmation results. `summary.json` binds code/input/output
hashes and recipes. Outputs: `metrics.csv`, `gates.csv`, `paired.csv`,
`error-correlations.csv` in the same scratch directory. Copies of numeric results
and the receipt are retained under `docs/evidence/fishnet-ablation-20261125/`.

Checks passed: original complete-case means/tails/counts reproduced to 1e-12;
zero-weight source erasure leaves predictions unchanged; all compared rows are
qualified, distinct, NNUE-eligible and complete for the included sources. Reference
depth, source-quality strata and old/new separation are preserved. No additional
Stockfish work, no production adoption, and no changes to frozen study releases.
