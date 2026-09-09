# First empirical exposure-recall comparison

Implement the accepted curve r(f)=f^2/(f^2+.001^2), where f is probability of
encountering the position at least once per game. Merge transpositions and
count repeat visits once. Solve acyclic graphs forward; cyclic examples use
two-start damped fixed-point checks, fail on nonconvergence/disagreement, and
do not claim a uniqueness proof from two starts.

Use the existing 2013-2026 ge1800 BRC positions and aux tables read-only. The
canonical pooler documents average rating >=1800, not both players >=1800/gap
<=200. No source-generation sidecar was found; record file identities and footer
hashes, validate every copied chess edge/count, and describe cohort provenance
accordingly. The completed lookup copied 440 edges and 31 aux rows. Unsorted
row groups required increasing cumulative decoded-byte guard from 2 to 4 GiB
per file after measuring the failure; retain a 32 MiB source row-group bound,
5,000 retained rows and 240-second time guard. Actual extraction took 7.7 sec.

Bound the first real comparison to three plies after e4 e5 Nf3 Nc6. This lets
us enumerate every retained empirical opponent reply and every legal Maia
fallback move, measure exact card exposure and score a short early horizon
without another Monte Carlo benchmark. It is not a full opening recommendation.
Use Maia-1400 for White, with the existing explicit opponent-conditioning
scenario 2000. Opponents use empirical stored move counts only. OTHER and
horizon mass become explicit unresolved frontier edges, never model guesses
or silent renormalization. Known ending counts become absorbing W/D/L hazards.
Other White leaf-decision positions without copied aux data have a declared
zero ending-hazard assumption; this cannot change exposure of the studied
positions reached before those decisions, but limits outcome interpretation.

Entry frequencies .1, .01 and .001 are explicit per-all-games scenarios for
this opening boundary, including color/participation. They are not inferred
from population position popularity and are not estimates of the user's play.
Future whole-repertoire use must obtain them from upstream candidate play or
observed exposure. These scenarios make the denominator and dependency visible.

Four fixed books: d4 only, plus alternatively Nxd4 after exd4, Nc3 after Nf6,
or Nc3 after d6. Count one/two distinct studied positions beyond the common
opening boundary. Compare frequency recall with fixed .85 recall on the same
graph and inputs; do not call the original methodology inferior. Preserve
first +/-300 cp and discount .99, child floor -100 cp and max loss 150 cp.
Horizon W/D/L remains an explicitly neutral placeholder, not a validated
performance criterion. Report first-hit uncertainty and do not rank overlapping
intervals. No outcome-dependent sample expansion or card replacement.

Use read-only canonical eval mmap lookups (at most 35,000 keys) and compatible
previous base/deeper engine records for the book gates. No new engine run is
required for this first exact comparison. Canonical mixed-budget scores are
common to all books and explicitly distinct from uniform engine-budget tests.
Cap graph at 35,000 nodes, 100,000 edges, 900 inference calls, 360 preparation
seconds and 200,000 evaluator states. Check resource headroom before inference;
existing workloads and canonical artifacts remain untouched.

Verify curve anchors, exact transposition sums, upstream recall effects,
per-game entry scaling, repeated-visit semantics, ambiguous cycles, all legal
Maia probabilities, empirical count/aux conservation, both-budget gates, and
independent tiny path enumeration. Report exact exposure/recall for every card
and the bounded real comparison, with input/output/code fingerprints.
