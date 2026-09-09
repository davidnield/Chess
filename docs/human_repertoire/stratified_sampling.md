# Exact prefix weights for rare study positions

The ordinary equal-budget panel encountered the ...Nf6 card zero times and
the ...d6 card twice. Increasing every trajectory count equally would spend
most additional work on ...exd4. The new sampler conditions deliberately on
early branches while preserving their probability in the final estimate.

Enumerate the complete first-two-ply distribution under the common root card.
At a learned node with recall r, action probability is
`r * I(action == book move) + (1-r) * fallback(action)`. Multiply by the
probability of reaching the node and surviving any ending hazard. Terminal,
repetition and horizon arrival rules take precedence over a new ending hazard,
as in the existing evaluator. Early-ending mass remains part of the partition.

Partition prefixes into d4/exd4, d4/Nf6, d4/d6 and the remainder. Let w_s be
the sum of model probabilities in stratum s. Sample a prefix within its stratum
proportional to its exact probability, then continue using the original model.
The remainder includes every other move sequence; no first move is forcibly
set to d4. Prefix distributions must match across compared books. If an extra
card can affect the enumerated prefix, that equality check fails rather than
applying incorrect common weights.

Continuation carries the original visited positions and ply count. Thus a
later return can trigger repetition or recognise a learned position, and an
advantage first reached in the prefix keeps its original discount. The current
implementation requires `reentry=True`; it refuses unsupported active-book
history semantics rather than silently approximating them.

For n_s paired samples in each stratum, let D_si be candidate utility minus
reference utility. The full-path effect estimate is
`sum_s w_s * mean_i(D_si)`. Its Monte Carlo variance estimate is
`sum_s w_s^2 * sample_variance_i(D_si) / n_s`. Sharing random uniforms between
books within each stratum preserves the paired variance reduction. Different
strata use distinct seed labels. Reproducibility uses fixed SHA-derived uniforms;
uncertainty calculations assume those pseudo-random streams represent
independent sampling across replicates and strata.

Because discounted first-advantage utility is in [0,1], D is in [-1,1]. A
conservative Hoeffding margin is
`sqrt(2 * log(2/alpha) * sum_s(w_s^2/n_s))`, clipped to the feasible effect
range when forming bounds. Four primary comparisons use alpha .05/4 for
family-wise 95% sampling coverage. These are conditional on the frozen model,
not bounds on real human benefit. Zero observed variance is not proof of no
effect. A large conditional improvement on a 2% branch is still weighted 2%.

The independent tiny-book path oracle checks complete prefix enumeration at
multiple recalls. Further tests compare sampled suffix outcomes to that oracle,
including position-specific recall, shared positions and re-entry. Separate
fixtures check mass conservation, horizon/ending precedence, repetition across
the prefix boundary, rejection of unsupported semantics and rare-branch weights.

The real preparation exposed a serialization edge case: Polyglot hashes an
adjacent pawn's en-passant file even when capturing would expose its king,
whereas default legal-only FEN omits that file. `HashedBoard` uses X-FEN
en-passant serialization by default. It preserves the repository's existing
hash and the exact legal move set, including across board copies. Use it as
the initial board for new Maia-backed trajectory runs. Earlier frozen modules
and successful artifacts are unchanged; the failing preparation stopped before
engine scoring, then reran after a tested correction without altering its seed
or candidate packages.

Code: [sampler](../../research/human_repertoire/stratified.py),
[board adapter](../../research/human_repertoire/hashed_board.py),
[tests](../../research/human_repertoire/test_stratified.py),
[experiment protocol](maia_stratified_plan.md).
