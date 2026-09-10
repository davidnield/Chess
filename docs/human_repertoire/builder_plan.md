# Bounded repertoire generator: first vertical slice

Deliver one configurable workflow for a White research region after 1.e4,
covering multiple empirical Black replies. Generate candidates automatically,
search packages under budgets 10/25/50, compare a reference projection and a
perfect-recall selection ablation, and export all resulting books. The scope
and resources are fixed by builder_config.json before real scoring. Do not
require every selected move or whole-book ranking to be statistically resolved.

Use existing mean-Elo>=1800 pooled BRC counts for opponents, Maia-1400 for White,
2000 opponent conditioning, and equal weighting of Blitz/Rapid in selection.
Classical remains a declared Rapid transfer, not separate empirical validation.
The configured per-all-games entry .1 is illustrative, not a personal estimate.
Only the previously declared tabiya after e4 e5 Nf3 Nc6 has familiarity .99.
The initial board/e4 boundary does not acquire a universal familiarity boost.
Recompute candidate-dependent exposure and contextual recall for each book.

The first graph runs seven plies after e4, through Black's fourth move. Expand
up to 96 positions per ply using a common envelope: max Maia probabilities and
probability one for the canonical reference move on White turns; empirical
probabilities on Black turns. This is a computation-allocation envelope only.
Retain all legal Maia and stored empirical edges; unexpanded/absent positions,
empirical tails and earlier-layer transitions are explicit frontiers. Query in
batches of 64 with the existing read-only guards: <=40M rows, <=400 groups,
32 MiB source row groups, <=4 GiB decoded per source/batch, <=5000 retained rows,
240 seconds/batch. Bound the whole phase by config and resume cached batches.

Project the canonical White repertoire at expanded White positions. Record its
metadata, file identity and footer hash; do not reconstruct or modify production
Stage 3. Select up to 80 candidate positions by the common envelope. At each,
consider the reference move and highest-Maia legal alternatives, up to three
moves. Resolve all candidate parents and those children under a frozen base
engine audit (cap 256 targets); never relax the -100 cp floor/150 cp loss gate.
If targets exceed the cap, retain only candidates whose parent and child both
received this run's audit; record incomplete gate coverage instead of widening the cap.
No globally best engine-move or deep soundness claim follows from this audit.

Reference budget adaptation is a reproducible projection: retain eligible
canonical prescriptions ordered by baseline fallback encounter frequency, with
position hash as tie-breaker, up to each budget. It preserves the canonical move
choice; it is not the original unrestricted optimiser or a perfect-recall model.
Use the same candidate pool and common graph for all comparisons. Keep the
reference seed available to both searches so the selected search objective is
at least as good as that seed on the frozen graph.

The heuristic search evaluates singleton additions, connected path packages,
move substitutions and bounded removals/replacements. Candidate-dependent recall
is recalculated after every change. Use cached actual book evaluations and a
packet-gain screening heuristic; caps/time termination return the best feasible
provisional book found, not an optimality claim. A packet may cross a barrier
where no singleton is beneficial; tests must demonstrate this. Report search
effort, stop reasons and actual costs, which may be below the allowed budget.

Select by mean known discounted first-advantage mass, then fewer positions on
ties. Preserve first +/-300 cp, discount .99, distinct-position cost and gates.
Unknown scores/tails are never treated as neutral known outcomes. The objective
is a common conservative covered-mass criterion, not calibrated win probability.
All final reference, perfect-selection and human-selection books are evaluated
with the same full contextual evaluator, reporting early advantage/disadvantage,
coverage intervals, recall and cost. Horizon W/D/L remains a placeholder; do not
use it to claim an acceptable full-game performance guarantee.

Compile the layered-DAG score calculation to make repeated search practical;
retain the generic contextual evaluator as a correctness reference, with a
fallback for transpositions of differing depths. Independently check tiny-book
search, package interactions, mass, shared costs and contextual score parity.
For real selected books, verify every legal/probability edge, candidate gate,
budget, export reconstruction and score parity with independent path sums.
Use generic data/search modules with no historical experiment-manifest chain.
Outputs go only to _artifacts/builder/<run_name>/; tracked reports/PGNs record
the delivered books and limitations. A fixed script can resume preparation,
engine, search and verification after interruption.
