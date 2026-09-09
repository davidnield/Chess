# Focused value of the 5.Be3 card

Keep exd4_Nxd4 versus sequence3, the same eight-ply horizon, empirical opponents,
Maia-1400, soundness gates and contextual recall model. Their only prescription
difference is White's 5.Be3 after 1.e4 e5 2.Nf3 Nc6 3.d4 exd4 4.Nxd4 Bc5.
Freeze inherited arrival/recall scenarios and gates; do not refit recall or pick
a new card. This focused experiment conditions on first arriving at that card
position at relative ply four. Compute the local intervention and scale its
contrast by the inherited no-hit arrival probability. Earlier unresolved routes
remain a separate limitation, not silently canceled as if they cannot reach
the card later. Report a recommendation as conditional on this boundary model.

Evaluate four remaining plies from the card position. Subsequent White moves
use Maia fallback in both arms; no 6.c3 prescription. Expand every legal root
move and every stored Black reply after it (at most 64 first-layer parents).
At White's sixth move, expand up to 256 positions; at Black's sixth move up to
1024. Rank by an outcome-blind envelope of the absolute one-card probability
change: abs(I(move=Be3)-Maia(move)), max across Blitz/Rapid; thereafter empirical
opponents and max Maia probabilities. Retain all moves; unexpanded, absent-data,
empirical tails and earlier-layer transitions are explicit frontiers. Do not
renormalize them. Common signed flow at the same frontier may cancel because
both policies thereafter are the same fallback policy with the same remaining
horizon. Track first-hit/unknown status so incompatible histories never cancel.

Use signed interval dynamic programming: known first hits fix discounted payoff;
an unknown score or pre-horizon cutoff has a [0,remaining-discount] payoff range.
At the root compare deterministic Be3 to full Maia fallback, cancel their common
Be3 component, then multiply by the inherited recall. Also report ordinary arm
intervals and explicitly separate fixed-boundary conclusions from the unresolved
full opening model. No W/D/L conversion claim from neutral horizon outcomes.

Bound new data to at most 24 batches of 64 queried parents, using the previous
4 GiB per-file, 32 MiB row-group, 5000-row and 240-second extraction guards.
Reuse verified inputs and cache each batch. At most 60,000 graph positions,
150,000 edges, 512 new Maia calls, 240 preparation seconds per resumable session.
Check memory/workloads; one-thread inference, canonical sources read-only. The
raised graph cap serves the deeper branch coverage identified in the prior run;
no larger population/model build is authorized by this experiment.

Look up existing verified eval arrays and freeze up to 128 highest-envelope
missing scores for a single-thread base engine audit (100k nodes/2 seconds,
Hash 32 MiB, no tablebases, 180-second session guard). Inherit existing compatible
base scores and the already-passed parent/Be3 child deeper gates. No adaptive
new card or outcome-driven topology change. Stop after this fixed expansion and
audit, or if a guard prevents completion; retain caches rather than widen limits.

Test signed interval cancellation against exhaustive tiny graphs, nonzero lower
bounds, unknown-before-hit semantics, shared transpositions and remaining-ply
separation. Independently verify all legal empirical/Maia edges, exact mass,
six scaled scenario contrasts and non-cancelled arm evaluator bounds. Record
output/code hashes, cutoff reduction, source work and a clear conclusion.
