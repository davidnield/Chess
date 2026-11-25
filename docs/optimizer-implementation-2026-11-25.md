# Rust optimizer implementation — November 25, 2026

The code portion of the approved optimizer study is implemented on
`codex/takeover-hardening-20261122`, in the maintained worktree
`C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation`.
Small native and interface tests pass. Representative real-data calibration, solver
selection and production book comparisons have **not** run. Data jobs and the monitor
were left paused, as requested. No canonical book or frozen release was replaced.

The controlling experiment design remains
[the consolidated tuning plan](repertoire-tuning-plan-2026-11-18.md).
Build/run instructions and API details are in
[the native optimizer guide](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/rust/book_builder/OPTIMIZERS.md).

## What is implemented

All new optimization, exact-reference modeling, graph extraction, fixture generation,
checkpoint validation and benchmark computation live in Rust. The optional backend is
the pinned, pure-Rust `microlp 0.8.0` mixed-integer solver. The standalone
`optimizer-bench` executable builds without Python. The existing Python builder has
only a thin opt-in invocation path after it prepares the native graph and stop values.

| Component | Implementation and important contract |
|---|---|
| DP control | Existing DP plus distinct-position matching and polish; requested-budget curves remain authoritative, with a separate 1.5x probe set. Cap diagnostics at 256/512/1,024 record binding counts, merged packages and root budget values. |
| Price control | Existing price search and polish, preserving its source-policy/optimistic-reach seed recipe. Absent or gated-out source moves are excluded from the seed and counted. |
| Multiple starts | Deduplicate independent DP/price starts, polish each and retain the best feasible book. The native API also accepts additional admitted starts. |
| Joint neighborhood search | Discover alternatives outside the incumbent book, include connecting prefixes and jointly optimize bounded sets. Fixed choices remain fixed when reached. Abandoned branches can release budget; transposed prescriptions are charged once. Accept improvements only after whole-book re-evaluation. |
| Exact references | Independent exhaustive small-DAG enumeration; full integer-budget tree DP without package compression; bounded mixed-integer models. Tree DP refuses shared descendants and cycles. |
| Real-subgraph export | Export bounded cones from the resident Rust graph with full 128-bit identities, SAN dictionary, parent graph digest, boundary-policy digest, limits and frozen boundary values. No full graph JSON/Python copy is needed. |
| Runner and resume | Immutable recipe/input/code identities; digest-checked completed cells and individual neighborhood steps; exclusive writer lock; wall/CPU time, process peak working set and dependency accounting. |
| Portable results | Actual SAN names and both key halves accompany every prescription. Content hashes are independent of run-local position IDs and SAN codes. Identical content is available for later evaluation/stability deduplication. |

The native comparison borrows the builder's fully prepared graph, so all methods use
the same candidate menu, gate decisions, probabilities, stop values and horizon cuts.
It runs before ordinary DP curve construction, avoiding an additional unused curve
set. Existing production entry points and default solver choices retain their behavior.

`python/build_budget_books.py` accepts the three opt-in flags
`--optimizer-study-config`, `--optimizer-study-receipts` and `--optimizer-study-out`.
It validates the configuration through Rust, requires a matching single budget and
128-bit keys, rejects fixed-parent/incumbent and early-dump modes, and exits after the
native study. It does not publish production book parquets. Use a fresh output prefix
for graph-preparation evidence. The source policy, graph, engine overlay, reply model,
stop values and horizon/cut receipts must be supplied explicitly. Hash syntax checks
do not substitute for the existing pipeline's acceptance of the underlying artifacts.

## Correctness and limitations

The optimization model counts **distinct reached own-side prescriptions**. It does
not independently charge each path through a transposition. Whole-book verification
removes unreachable choices and checks the actual budget and objective. Fixed choices
outside a neighborhood can disappear when an opening switch makes them unreachable;
they cannot change at a position that remains reached.
Reachability indicators use whether an edge has positive probability, rather than
its probability magnitude, so an extremely rare branch cannot evade a fixed choice
through solver feasibility tolerance. Actual probabilities still weight the objective.

Cycles preserve the production evaluator's existing fixed topological-cut/static
fallback convention. Exactness refers to that finite admitted model, not to a new
repetition model or to all legal chess moves. A cone's optimum/bound is local to its
frozen boundary. The model reports optimal, limited and resource-skipped statuses
separately; the old price diagnostic called an upper bound is not used as a certified
bound for this DAG problem.

MIP feasibility/integrality tolerances are 1e-9, pruning epsilon is 1e-10, and the
rounded-policy objective must agree within 1e-7. Budget-relaxed backwards-induction
values provide valid node ceilings. A larger forcing-chain test initially hit the
time limit with generic [0,1] value bounds; these tighter ceilings resolved that case.
This is evidence for the formulation change on the test, not a production speed claim.

Initial neighborhood settings are provisional: width 12, 8 discovery rounds and
2,000 discovery nodes; each bounded solve allows 2 seconds, 10,000 branch nodes,
2,000 active graph nodes and 20,000 variables. Discovery is heuristic and is not a
proof that unexplored alternatives cannot improve the book. There is no internal
hard process-memory limit in microlp; use measured resource settings and worker limits
before increasing scope. Full multimillion-node MIP is not required.

Completed cells survive interruption. Neighborhood search also resumes after each
completed neighborhood. An interrupted DP/price cell or current solver neighborhood
restarts; in-memory branch trees are not checkpointed. A stale writer lock requires
checking that its recorded process no longer owns the run before removal. Changed
input, recipe, code or SAN mapping requires a new directory. Corrupt checkpoints fail
closed instead of being silently treated as cache misses.

Per-cell wall/CPU figures are incremental: portfolio must be charged for its DP and
price starts, and neighborhood search also for its portfolio. Peak working set is a
process-lifetime high-water mark, not isolated memory attributable to one method.
Graph-loading/validation costs and interrupted work must be included in end-to-end
comparisons. The builder logs graph-preparation wall time; its CPU/memory costs need
the launcher monitor. Equal-resource measurements and paired rollouts remain O1/O3 work.

## Verification performed

| Check | Result |
|---|---|
| Rust library tests with optional optimizer feature | 22 passed. |
| Rust library tests with Python entirely disabled | 22 passed. |
| Independent DAG oracle vs MIP | 24 deterministic 10-node DAGs at budgets 1, 3 and 5; 72 comparisons agreed and completed with optimal status. |
| Adversarial fixtures | Shared-cost diamond; delayed alternatives; 25-decision chain; fixed abandoned branches freeing budget for a new line; zero/duplicate reply edges; cycles; malformed graphs; resource limits. |
| Actual cap binding | 320-branch tree exercises the 256-package cap; 512 does not bind. Feasible results checked against exact tree allocation. |
| Resume and identity | Complete-cell reuse; neighborhood interruption/resume; corruption, recipe drift and changed SAN mapping rejected. |
| Existing Python/Rust differential suite | Numerical/graph/DP/price/polish/columnar/sampled sections passed bit-for-bit. Its final collector check initially encountered sandbox temporary-directory permissions; rerunning that check with workspace TEMP/TMP passed: 100 pools, 2,142 rows, zero differences. |
| New thin-interface checks | 3 passed: resident-graph compare/resume, full-key/SAN exports, source filtering, boundary export, missing registry/dictionary rejection and strict config validation. Includes two positions with equal low halves and different high halves. |
| Standalone release smoke | Seven cases, 54 method/reference cells. Results agree with exact references on these deliberately small fixtures. A second invocation reuses completed receipts and reproduces the same summary. |
| Launcher syntax/help | Python compilation and opt-in argument visibility passed. No production pool was loaded or job resumed for this check. |

Native tests additionally verify that portfolios retain the best feasible start and
that a large neighborhood can discover an unselected delayed-payoff alternative.
Fixture generation reserves separate seeds 1000–1011 for later calibration checks;
the small smoke run does not consume them. These checks do not replace representative
OTB/Lichess graph calibration or an end-to-end production pilot.

The standalone release and extension use separate build target directories because
switching feature combinations in one Windows target directory caused an import-library
linker collision. Both builds succeeded with separate targets. The extension is
installed only in this worktree; frozen release binaries and the main checkout's
installed extension were preserved.

## Artifacts and next execution steps

[optimizer-study-2026-11-25-v1.json](optimizer-study-2026-11-25-v1.json) is a
Rust-generated **unbound study specification**, not an executing production queue.
It covers both colors, both approved gate arms and B=2,000/B=500, with the cap study
separate. B=500 methods are candidates for shortlisting, not a decision to run every
combination. It binds the existing v5 recipe by SHA256 and carries the fixed gate
baselines. v5 remains unchanged:
`36f3e3dbd2c5bb2d639bb0f03610f21c50b56d06b47d22f378dffcac102bb613`.

Local native smoke receipts/summaries are retained under this worktree's
`scratch/optimizer-native-smoke-20261125-*` directories. They are small development
artifacts, not input acceptance receipts or production books.
The final checked build's [verification record](optimizer-verification-2026-11-25.json)
contains executable/extension/input-summary hashes, and its
[native smoke summary](optimizer-smoke-2026-11-25.json) records all 54 cells.
The receipt points to `scratch/optimizer-native-smoke-20261125-final-v2`; earlier
development runs used earlier binary identities and are not silently resumed as this build.

Next, after the data jobs finish and inputs pass acceptance:

1. Bind the common graph/evidence/policy/stop/horizon receipts and close the Black
   stricter pilot under the existing quality gates.
2. Export representative cones across the planned depth, reach, support and
   transposition strata. Measure actual gaps, time and memory; calibrate neighborhood
   and solver limits, including held-out cases. Select the practical settings.
3. Publish a bound executable study manifest using those accepted inputs and settings.
   Run the four B=2,000 contexts, then B=500 shortlist checks and any justified B=1,000
   follow-up. Revalidate all affected contenders if shared engine evidence changes.
4. Use paired development rollouts, account for full compute and learning burden,
   choose/freeze the solver, and update the primary sweep's generator/validator/release
   gates with the selected recipe. Reserve fresh confirmation for frozen finalists.

No optimizer winner, production resource cap, full-book ETA or practical playing-strength
gain is established by this implementation work.
