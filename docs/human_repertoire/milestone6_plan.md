# Milestone 6: complete engine coverage of a paired path panel

Freeze this protocol and the implementation before generating the panel. This
is a bounded coverage/cost and variance experiment under frozen models, not new
human validation, policy tuning or a repertoire recommendation.

## Population, policies and sample

Preserve the >=1800 Blitz/Rapid/Classical population, both individual ratings
at or above the threshold and gap <=200. Use milestone 3's frozen January counts and
February-selected shared prior, pooled across these time controls. Compare the
fixed one-card `3.d4` and two-card `3.d4` / `3...exd4 4.Nxd4` plans after
`1.e4 e5 2.Nf3 Nc6`. Card costs remain 1 and 2 distinct positions.

Use the existing full legal sampler with twelve additional plies, independent
.85 recall and re-entry, +/-300 cp competing first-hit markers, .99 discount,
third position occurrence, board terminals and the documented human-ending
hazard. Preserve empirical/prior endpoint W/D/L. The utility remains early
advantage subject to the existing soundness and performance constraints.

Generate 64 paired indices per arm with a fresh fixed seed. The same random
channels couple plans; no index is accepted/rejected based on its path or score.
The first 16 indices form a predeclared deeper-budget subset. This subset is
nested, not an independent replication. Freeze all paths before new engine work.

## Complete position coverage and bounded work

Audit every visited nonterminal position at the base budget, including positions
after the first known hit, identical paired paths, and known canonical positions.
This simple complete-coverage rule allows full per-arm metrics and avoids outcome
dependent engine allocation. Exact rule terminals use exact cp markers.

The absolute arrival bound is 2*64*13 = 1,664, with at most four additional
prepared parents/children for soundness gates: base cap 1,668. The deeper subset
has at most 2*16*13+4 = 420 positions. These caps do not discard trajectories.

Use the same Stockfish binary and single-thread/32 MiB options as M4/M5. Reuse
prior engine records only at the matching budget and binary identity: base
100,000 nodes/1 second; deeper 400,000 nodes/2 seconds. Do not substitute deeper
scores into the base map. Reused FENs must match the position and cannot involve
fifty-move-rule counter differences in this short opening horizon. New requests
clear hash/game state and retain the last completed non-bound score as before.

Select gate positions first, then the first encounter of each position in index,
plan-name and ply order. Remove exact terminals and matching cached records.
Freeze both target lists before scores. The source database is used only for
the labelled pre-audit diagnostic and never controls target selection. All
source reads are sparse and read-only, with identity checks across M5/M6.

The sampling guard is 60 seconds. New engine work has a 900-second session guard
between requests, with the existing per-request timeout, bounded target counts
and fixed per-search caps. Save progress every 16 requests. If a guard interrupts
work, preserve the entire panel and partial records and report missing-score
intervals explicitly; never claim complete coverage or drop incomplete paths.
Resume only with unchanged identities. No canonical data, original process or
production artifact may be modified.

## Comparisons and Monte Carlo error

Primary: all 64 pairs under the complete homogeneous base-budget map. Also show
canonical source-only and inherited M4/M5 mixed-overlay coverage on the same
paths. The diagnostic budget curve adds the first 0/64/128/256/512/1024/1668 new
base targets in their frozen order, clipping to actual work and including the
actual completed total. Its starting map contains matching prior base records;
it does not contain canonical scores or deeper replacements. Report per-arm
missing-evaluation fractions and first-hit interval widths and the paired
contrast interval. The curve is descriptive, with no data-driven stopping rule.

On the same fixed 16-pair subset, compare complete base and complete deeper maps.
Show position-level cp/threshold changes and path-level first-hit/contrast changes.
Do not mix this subset's deeper evidence into a nominally homogeneous 64-pair
deeper result. Check all prepared gates separately at both budgets.

Reuse the existing sample mean, Monte Carlo SE, missing-information endpoints
and per-comparison 95% Hoeffding outer envelopes. The primary estimand is the
two-minus-one difference in discounted first advantage. Identical position
sequences have zero first-hit difference. Complete coverage should collapse
the sample observation interval to a point; it does not remove simulation or
engine error. At N=64 the paired Hoeffding margin alone is about 33.95 percentage
points. Report that limitation even if a marker mean favors one plan. The N=16
subset has still wider simulation uncertainty.

Simulation error conditions on frozen human-move assumptions and the fixed
engine-budget mapping, treating per-position search results as deterministic
measurements. Time-cap or search instability and human generalisation are not
included in those envelopes. This selected opening is not a claim about the
entire >=1800 population. Do not recommend a book from this small panel.

## Verification and continuation

Test target completeness, deterministic order, matching-budget cache reuse,
terminal handling, missing-position rejection, paired exact scoring and gates.
Replay every path independently, reconstruct scalar metrics without the sampler
scorer, and verify snapshot/config/code/binary identities and all selected
search limits. Preserve all metrics, engine work and sensitivity results in
tracked compact summaries and ignored full artifacts.

Use the result to decide whether coverage cost, Monte Carlo precision, engine
stability or human-policy extrapolation is the next binding limitation. Any
larger/tuned experiment requires a fresh frozen sample; do not silently grow
this panel or begin a broad optimisation.
