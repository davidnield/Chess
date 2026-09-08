# Bounded study decision using Maia-1400

Freeze this plan/config and tested runner before generating paths. Compare the
unchanged M4/M6 prescriptions: one distinct-position card for 3.d4 versus that
card plus 4.Nxd4 after 3...exd4. This is the marginal value of a second card,
not a global optimizer or an equal-budget comparison of every opening choice.

Use the shared-token policy as an unchanged model reference and Maia-1400 Blitz
and Rapid for our unprepared moves. Opponent moves remain the shared-token
policy. Maia's opponent-conditioning input is a fixed **2000 scenario**, not a
claim about the user's opponent pool. Retain the user's self=1400 preference.
Classical uses the same Rapid policy, so it is the same modeled scenario, not
independent Classical evidence; do not triple-count it as a new simulation.

Cross all three policies with recall .5/.85/1.0 and two books. Generate exactly
32 paired paths per arm (576 total), using common index/ply/channel uniforms
across policies, books and recall settings. Reuse the existing sampler/evaluator.
The twelve-ply horizon, reentry, third-visit repetition draw, empirical ending
hazards and frozen terminal WDL assumptions remain M6's. Stop interpretation
at model predictions: terminal WDL estimates are not measured human outcomes.

Primary outcome is the discounted first occurrence of a +300-cp advantage
before a -300-cp disadvantage, discount .99 per ply. Preserve separate downside
rates. Book prescriptions must pass the old soundness floor -100 cp and
maximum loss 150 cp. Study cost is one unit per distinct prescribed position;
recall is an explicit scenario, not fitted retention. Keep M6's other evaluator
thresholds in the output; do not claim risk/points population feasibility from
this small conditional simulation.

Freeze all paths and a union of visited positions plus book-gate positions
before new engine work. At most 7,500 base positions; first four indices per
arm are a fixed deeper subset, at most 950 positions. Reuse verified M4-M6
engine records at matching budgets only. New work uses the same Stockfish
binary, one thread/32-MiB hash/no tablebases, 100k nodes/1 second base and 400k
nodes/2 seconds deeper. Require complete coverage; no dropping expensive paths.
Sampling guard 360 seconds; engine session guard 900 seconds. Save partial
engine results on guard failure and label comparison incomplete. No source
extraction, canonical eval scans, new model fitting or new book search.

Report each arm, paired two-minus-one early advantage and downside differences,
paired MC standard errors and the existing conservative Hoeffding intervals.
Keep deeper-subset results separate and report all nine policy/recall settings.
Small-sample intervals may be uninformative; do not increase sample size after
seeing results or claim a universal book ranking.

Also report exact local fallback probability of Nxd4 at the extra-card position.
The local recalled/fallback mixture assigns it r+(1-r)*q under two cards versus
q under one card. Their action-distribution total variation is r*(1-q). Since
the only prescription difference is this position, this bounds the change in
any [0,1] path outcome under coupling at its first encounter when the position
can occur at most once before termination. A repeated encounter could accumulate
differences: verify that caveat separately, and do not use a single-encounter
bound as a general global bound. The local quantity alone measures opportunity
for the extra card to change a decision, not its engine benefit.

Verify original-game-independent legal replay of all traces, regenerate path
decisions against frozen policy tables, preserve opponent probabilities, and
reconstruct scores/gates/contrasts. Record runtime and all source/model/config
fingerprints. The completed report must answer the concrete study question and
identify what remains unknown, rather than launching another predictor project.
