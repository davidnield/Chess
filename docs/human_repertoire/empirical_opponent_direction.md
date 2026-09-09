# Use the full empirical opponent distributions

The user clarified that the project already has a very large >=1800 positions
dataset and questioned replacing its opponent move distributions with Maia.
The correct default is empirical opponent behavior wherever support is adequate.
Maia-1400 remains the user's chosen policy for their own unremembered moves.

The recent research runner did not use the full production positions counts.
Its shared-position opponent prior was fitted to 2,250 January games in the
M3 research sample (6,750 games across fitting, tuning and testing), with
move-token smoothing at sparse/unseen positions. Its demonstrated weaknesses
are not evidence that the full empirical database is sparse at those positions.
The prototype's bounded sample was initially useful for testing infrastructure;
continuing to model around its sparsity instead of connecting the production
counts was a research-priority detour.

The Maia-2000 opponent run is a sensitivity diagnostic only. Its 768 paths
and bounded engine coverage completed before this clarification; final
comparison/reporting had not run. Preserve its artifacts as diagnostic evidence,
but do not promote Maia to the default opponent or launch further opponent-model
experiments ahead of the empirical connection.

Next priority: identify the authoritative positions/aux artifacts and read a
bounded, fingerprinted snapshot for this opening experiment. Inspect manifests,
schema, depth/truncation rules and filtered-out move mass before constructing
probabilities. Preserve all accounted continuation mass; do not silently
renormalize stored popular moves after discarding the tail. Sufficient-data
positions should be driven by observed move counts. Sparse/unseen positions
may use a separately declared prior/fallback, with support and fallback use
reported explicitly. Do not hard-code a support threshold without justification.

Verify cohort semantics: the production pooler's documented default is mean
player rating >=1800 with pooled Blitz/Rapid/Classical, while M3 used both
players >=1800 and gap <=200. Do not claim those filters or separate time-control
distributions exist in a pooled artifact unless its provenance supports them.
This calls for honest configuration and provenance, not a full-data rebuild.

Reuse the existing evaluator, Maia-1400 own fallback, study costs, recall,
Stockfish soundness and exact-prefix sampling. A small input/output workload
does not require restricting the underlying empirical evidence to a tiny
training sample. Original data and processes remain read-only/untouched.
