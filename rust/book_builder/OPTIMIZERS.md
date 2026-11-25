# Native optimizer study

Opt-in implementation of the November 25 tuning-plan amendment. All new search,
modeling, fixtures, exact oracles and benchmark computation are Rust. This does not
change production defaults, select a winner, or resume data jobs.

## Build and small checks

From this crate:

```powershell
cargo test --locked --no-default-features --features optimizer --lib
cargo build --release --locked --no-default-features --features optimizer --bin optimizer-bench --target-dir target-native
target-native/release/optimizer-bench.exe config NEW-config.json
target-native/release/optimizer-bench.exe fixtures NEW-fixtures
target-native/release/optimizer-bench.exe run NEW-fixtures/diamond.json NEW-results NEW-config.json
target-native/release/optimizer-bench.exe smoke NEW-smoke-results
target-native/release/optimizer-bench.exe plan NEW-plan.json
```

The standalone binary neither links Python nor requires its interpreter. The normal
extension keeps its default `python` feature. `build.ps1 -Optimizer` adds the optional
extension methods; `-Test` also runs existing parity checks. Preserve frozen releases.
Use separate target directories for standalone and extension builds to avoid Windows
import-library collisions. The generated fixtures include a 25-decision delayed chain,
a 320-branch tree that binds the 256-package cap, development seeds 0–11 and separately
reserved calibration seeds 1000–1011. The small smoke command does not consume the latter.

The plan command writes an unbound study specification, not a running queue or a
replacement for main manifest v5. Accepted inputs and calibrated production resource
settings remain prerequisites. The B=500 list contains methods to shortlist.

## Methods

- DP: native DP, authoritative requested-budget curves plus separate inflated probe
  curves, distinct-position matching and polish. Diagnose caps 256/512/1,024.
  Profiles include cap binding/merge counts and root values at every budget.
- Price: existing native search and its built-in starts. A supplied source policy
  raises optimistic reach using summed policy reach, matching the existing recipe.
- Portfolio: polish independent DP and price starts, deduplicate and retain the best
  feasible book. Arbitrary additional admitted starts are supported in Rust.
- Large-neighborhood search: discover admitted alternatives, retain connecting
  prefixes, unlock related positions and solve the joint subproblem. Outside choices
  are fixed **when reached**; abandoned branches release budget, while shared nodes
  remain charged once. Accept only after whole-book verification. Discovery limits
  are heuristic exploration limits, not certified pruning bounds.
- Exact references: independent exhaustive enumeration/value calculation on tiny
  DAGs, full max-plus budget tables on trees, and bounded mixed-integer optimization.
  Tree DP rejects shared descendants/cycles and limits table size and operations.

The model backend is pinned to **microlp 0.8.0**, an Apache-2.0 pure-Rust solver:
<https://docs.rs/microlp/0.8.0/microlp/>. Binary moves, continuous values and reachability
constraints handle distinct/shared/releasable costs. Report limited, feasible and
optimal statuses separately. Numerical proof means within tolerances, not exact
arithmetic: feasibility/integrality 1e-9, pruning epsilon 1e-10, and a rounded-policy
objective check of 1e-7. Always independently re-evaluate the selected book.
Node bounds use valid budget-relaxed backwards-induction ceilings. The legacy price
search's reported upper bound is deliberately not used as a DAG optimality certificate.
Reachability is Boolean at integer solutions, including arbitrarily rare positive
branches; actual reply probabilities weight the objective, not fixed-choice enforcement.

Initial settings are **small calibration defaults**, not accepted production settings.
Time, branch-node, active-node and variable limits bound solves. There is no claimed
hard process-RAM cap inside microlp. Measure memory and apply launcher memory limits
before increasing scope. Full multimillion-node MIP is not a prerequisite.

## Inputs and integration

All methods borrow the same admitted graph, probabilities and stop values, with
stickiness off. Cycles retain the existing fixed topological-cut/static fallback
convention; this is not a new chess repetition model. Data/engine quality closure
and independent cloud-move verification remain prerequisites for promotion.
Optimality refers only to this finite model.

Bounded snapshots encode full keys as 32 hex digits and preserve node/edge order,
move codes, reply probabilities, terminal values, reach, source prescriptions and
provenance. Loading rejects duplicate keys/moves, missing children, invalid mass,
bad links/CSR, nonfinite values and unknown schema. Never serialize an aliased
run-local i64 handle as a full position identity.

`snapshot::extract` creates a bounded calibration cone with alternatives and
transpositions. Outside children receive frozen baseline-policy values; original
cycle cuts remain terminal. Its receipt records graph/policy hashes and limits.
Its optimum is local to that boundary, not a bound on the whole repertoire.

`RsGraph.optimizer_compare` runs directly on the resident graph, without a second
Python/JSON graph. Supply config/provenance JSON, an output directory,
source-key/source-move arrays and the graph's SAN dictionary. The constructor retains
its original full-key ID registry; a graph without that registry is rejected. Required
SHA256 receipt bindings:
`candidate-graph`, `engine-overlay`, `opponent-policy`, `stop-values`,
`horizon-and-cuts`, `price-source-policy`. These do not replace independent receipt
acceptance by the calling pipeline. The graph fingerprint streams columns and
decodes full keys through the registry, including colliding low halves. Returned
results include portable full-key prescriptions, SAN names and content hashes. The
unconstrained source policy may be a superset: absent or inadmissible source moves are
ignored for the price-reach seed, with explicit counts, matching legacy policy reach.
Search starts and finished prescriptions still require admitted moves.

The existing `python/build_budget_books.py` launcher has an opt-in, study-only exit:
add `--optimizer-study-config CONFIG --optimizer-study-receipts RECEIPTS
--optimizer-study-out DIRECTORY` to the usual full-key graph preparation command,
using one `--budget` matching the config and a fresh `--out-prefix` for graph evidence.
This calls the Rust runner after policy/stop-value preparation and **before** building
ordinary DP curves. It rejects fixed-parent/incumbent and early-dump modes, validates
config in Rust, writes separate study artifacts and does not emit production books.
Its `--method`, matching and polish flags do not select study methods; the native
comparison uses the explicit baseline recipes above. Receipt hashes must refer to
accepted immutable inputs; syntactically valid hashes alone do not establish acceptance.
It logs graph-preparation wall time separately; CPU/peak memory for that phase still
need the launcher monitor for end-to-end comparisons.

`RsGraph.optimizer_snapshot(root, base_keys, base_sans, max_nodes, max_depth,
output_file, move_names)` exports a bounded cone through the original ID registry.
The boundary policy must use admitted graph moves. The SAN dictionary and parent/
boundary receipts travel with the snapshot. No full graph copy is made. Select real
roots across the plan's depth/reach/support/transposition strata after data acceptance.

`RsGraph.optimizer_polish_starts` accepts independent starts and returns the existing
book-result format. Legacy entry points retain their signatures and behavior.

## Resume and measurements

Identity binds graph/provenance, source policy, configuration, compiled-source
fingerprint, solver version and tolerances. The CLI also binds executable SHA256.
Changed recipes/binaries need a new directory. An exclusive `RUN.lock` prevents
concurrent writers. After a crash, verify the recorded PID no longer owns the
directory before removing a stale lock; locks are never stolen automatically.

Completed cells and neighborhood steps are flushed and atomically published with
payload digests. Resume rechecks identities, admitted moves, budgets, values and
bounds; corruption fails rather than being silently recomputed. Completed cells
and LNS neighborhoods survive. An interrupted DP/price solve or current MIP
neighborhood restarts; in-memory solver branch trees are not persisted.

Reports include wall/CPU time and Windows process peak working set. The latter is
a **process-lifetime high-water mark**, not an isolated per-method peak. Unsupported
OS counters are null. Use separate workers/sampling for strict memory comparisons.
Incremental cell times exclude reuse: charge DP + price + portfolio overhead to
portfolio, then add LNS work for neighborhood search. Add graph loading/validation
costs from the caller for end-to-end reporting. Preserve checkpoint timing evidence;
a resumed invocation's elapsed time is not the total work performed.

Representative O1 calibration, production resource selection, accepted data binding
and paired rollouts remain subsequent stages. Small tests do not establish
full-book optimality or production speed.
