# Milestone 3 pre-analysis plan: shared fallback and date spread

Frozen before new game extraction and model scoring. Continue the user's target:
both individual ratings >=1800, rating gap <=200, Blitz/Rapid/Classical. Use the
same exact `1.e4 e5 2.Nf3 Nc6` root and twelve additional plies. Observed endings
remain in game counts; only actual decisions enter move prediction.

## Sample

Use January 2024 for fitting, February for tuning, March for a final test. Earlier
milestones examined different January/February frames; March has not informed the
research choices. There is no claim of independence between development studies.
Inspect only Parquet footers to choose three spread frames per event/month. With
at least three row groups, take 65,536-row prefixes at the 1/6, 1/2 and 5/6 row-group
indices. With fewer groups, center disjoint windows at those row quantiles.
Freeze exact frames and footer/source fingerprints before reading game moves.

Hash-rank at most 250 eligible games per frame, without selecting on the recorded
result beyond requiring completion. Maximum 2,250 games/month; 6,750 total.
Equal event/frame caps define an intentionally balanced convenience frame, not
the platform's population mixture. Report actual dates and counts. No replacement
of failed PGN replays. Abort on overlapping frames or selected game IDs. Track
shared players; temporal game separation is not player independence.

Projection remains limited to the ten existing cohort columns. At most 4 million
rows decoded (plus at most one partial batch per selected row group), 512 MiB
projected uncompressed per row group, 8,192 rows per batch and one reader thread.
The examined frame is 1,769,472 rows. Footer inventory is capped at 256 files per
stratum. No engine-array scan, engine run, neural training or production CLI.

## Fixed comparison

Fit a deliberately small, engine-free **shared move-token prior** using January
only. A token comprises moving piece type, destination square from the mover's
perspective (Black ranks reflected), capture flag, promotion piece, and castling
flag. Pool both sides and all three events. Count tokens on recorded decisions.
Give each legal move weight `(training token count + 1)^temperature`, normalize
over that position's legal moves. Distinct legal moves sharing a token each get
that weight. No dependence on final outcomes, engine scores or held-out moves.

Combine with January's exact-position move counts:
`p(move | position) = (count + alpha * prior) / (position_count + alpha)`.
The shared counts describe this opening population's moves; they do not model
personal recall or engine quality. The prior transfers habits across positions,
but ignores tactical context and can be confidently wrong.

Compare uniform legal, the milestone-2 position model (alpha=1, uniform prior),
a tuned position/uniform model, a token-only model, and a tuned position/token
model. Tune alpha in {0.1,1,5,20}; token exponent in {0.5,1}. Select on February's
mean decision log loss only, deterministic lower-alpha/lower-exponent ties.
Freeze parameters and January fitted-count hashes before opening March games for
scoring. Do not refit on February or adjust the grid after March results.

Primary final endpoint: paired March log-loss difference, position/token minus
tuned position/uniform, with lower better. Also report Brier, uniform baseline,
per-event, our/opponent turn, and unseen-position performance. Report the mass
assigned to moves absent at each training position on the actual March positions;
this is a one-step diagnostic, not a counterfactual path frontier probability.
Use a 500-resample shared-player-component bootstrap of paired game aggregates,
preserving decision weighting; report largest component and a conditional 95%
interval. It excludes training, tuning, frame and model uncertainty.

The shared model passes this bounded predictive check if its paired interval is
entirely below zero, every event's point log-loss difference is below zero, and
unseen-position log loss improves. These are a research progression rule, not a
causal effectiveness claim or a repertoire recommendation. If it fails, preserve
the result and investigate the failure before adding complexity.

## Scope and follow-through

Add independent hand-calculated probability tests, symmetry/legal-move tests,
sampling-boundary tests and a tuning/test separation check. Run the research
suite before extraction/scoring, save config/code/input hashes and fitted counts,
and report all fixed comparisons. This changes the continuation assumption only;
leave the book optimiser, recall model, soundness gates and existing snapshots
unchanged. The existing early-advantage objective remains the eventual goal.

Even a predictive pass does not complete unseen continuation expansion or provide
consistent engine coverage. The next step would be bounded integration into the
common evaluator and an influential-position engine audit before book rankings.
Do not silently replace unknown branches by predicted outcomes or call reduced
one-step loss an improvement in human results.
