# Familiar tabiya and depth interaction: prospective plan

The user rejects 3.6% recall for White's 4.Nc3 in
1.e4 e5 2.Nf3 Nc6 3.d4 Nf6 4.Nc3: the shallow move follows a familiar tabiya.
Use this qualitative personal calibration constraint, not as measured recall.
Preserve the original frequency-only implementation and frozen experiments.

Add an explicit profile of familiar tabiyas. The position after 2...Nc6 has
declared familiarity .99 as a configurable interpretation of the user's
statement. Do not infer personal tabiyas from population counts, add the
initial board as a universal familiar anchor, or hard-code a particular move.
Other anchors require explicit profile entries. Maia-1400 remains fallback;
opponents remain empirical. The model applies only to studied prescriptions.

At a familiar anchor, strength is max(declared familiarity, frequency recall
of its per-all-games exposure). Carry the most recent such anchor along each
route. Its local support is strength * 2^(-max(0,distance-2)/4), where distance
is in plies. Multiply by absolute-depth attenuation
2^(-max(0,absolute decision ply-8)/8). Default contextual boost ceiling is .98.
Decision ply counts completed half-moves before the move to be remembered:
4.Nc3 is ply 6; this root is ply 4. Combine with frequency recall as
r = r_frequency + (1-r_frequency) * contextual_boost.

For transpositions, use arrival-mass-weighted local support and depth factors
together, then one position-level recall probability. This models familiarity
of the position pooled across routes, rather than route-specific recall at
each visit. Keep the most recent declared anchor per route, not a global
shortest-path distance. Exposure follows upstream recall under that same
position-level policy. Acyclic graphs admit a topological solution; reject
cycles explicitly in this first extension rather than use an unverified
feedback approximation. Respect endings and finite horizon. Charge distinct
positions once, with no new study discount for anchors.

The chosen defaults should predict at least 95% recall for the stated 4.Nc3
example when the opening is entered once per 100 games. This is a calibration
check, not independent evidence that 95% is the true rate. Tests must show
boost decay with distance and absolute ply, no boost for an unfamiliar early
branch, exact merging of routes with different anchors/depths, upstream recall
effects, zero-entry handling, absent-anchor parity with the old curve, and
rejection of unsupported cyclic graphs. Mark all numerical results as model
predictions and preserve sensitivity to the assumed strength.

Reuse the frozen empirical graphs and original 24 scenario/book results.
Recompute the 24 cases with the new model and compare recall, exposure and
the same early-advantage objective, horizon, gates and study costs. Run the
4.Nc3 calibration case with boost ceilings .90, .95 and .98 as sensitivity.
Independently enumerate paths using the solved recall, reconstruct exposure
and contextual support, and compare all evaluator metrics. No new data scan,
Maia inference, engine evaluation or production change is needed. Outputs
stay in the ignored `_artifacts/context_recall/` directory, with hashes and
tracked report/results. This task does not extend the real outcome horizon.
