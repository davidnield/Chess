# Fixed equal-budget Maia comparison

Freeze before generating new paths or scoring new positions. Retain the
Maia-1400 preference, separate shared-position opponent policy, fixed Maia
opponent-conditioning input 2000, horizon 12, threshold +/-300 cp, discount
0.99, repetition/terminal semantics and study/soundness constraints of the
completed Maia comparison. No fitting, new human sample or canonical writes.

All three packages cost exactly two distinct positions: prescribe 3.d4 after
1.e4 e5 2.Nf3 Nc6, then either 4.Nxd4 after 3...exd4 (reference), 4.Nc3 after
3...Nf6, or 4.Nc3 after 3...d6. These are fixed illustrative alternative uses
of one follow-up card, not a comprehensive or engine-selected candidate set.
The root card is included in every package; all remaining continuation moves
use the fallback policy. No later continuation is silently treated as learned.
This explicitly tests whether a two-card package suffices under fallback; it
does not assume every sound move can safely end a preparation sequence.

Primary recall is 0.85, already included in the prior sensitivity experiment.
Use Maia Blitz and Rapid separately; Classical is the identical Rapid transfer
scenario and is not counted again. Generate 64 fresh paired indices per arm
using the new fixed seed: 384 paths total. No sample reuse from the earlier
comparison; reuse only fingerprinted compatible engine evaluations. First
eight indices form the nested deeper subset. No adaptive expansion or deletion.

Both alternatives are compared to exd4_Nxd4 with common random numbers. Use
the existing evaluator and paired contrasts, plus first-disadvantage and
uncalibrated expected-points diagnostics. Family-wise 95% conservative
Hoeffding bounds use Bonferroni across four primary contrasts (two candidates
x two policies). A candidate failing either prescribed-move gate at either
engine budget is reported ineligible, never replaced after seeing outcomes.
The gates remain child >= -100 cp and child >= parent -150 cp.

Bound work at 6,000 base positions, 1,000 deeper, 360 sampling seconds and
900 engine seconds with one thread, Hash 32, no Syzygy. Complete every frozen
path or report an incomplete run. Validate equal distinct-position cost,
required root inclusion, legal prescriptions and target union before scoring.
Replay all final paths from frozen probabilities and independently reconstruct
first-hit utility, paired effects, soundness and engine coverage. Report local
fallback probability, branch probability and observed encounters to explain
where study can change behavior. These are model predictions, not personalised
human benefit or proof of safety. Do not rank books when uncertainty prevents it.
