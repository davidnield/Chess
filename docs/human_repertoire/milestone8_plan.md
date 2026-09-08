# Milestone 8: bounded board-aware fallback benchmark

Freeze protocol/code before development. Preserve >=1800 individual ratings,
rating gap <=200 and Blitz/Rapid/Classical, the same opening root and twelve-ply
window. No book optimisation, recall changes or production integration.

## Candidate and development

Keep the frozen M3 shared-token prior/counts as the reference. The candidate
multiplies its legal prior q by exp(beta*w.f), then renormalises over all legal
moves. Exact-position posterior counts and alpha remain the M3 settings.
Beta=0 must reproduce the reference exactly. No engine score, human game result,
March case or April observation is a training feature.

The ten bounded board features are: gives check; castles; captured material/9;
moving piece attacked by an enemy pawn after the move; attacked by a cheaper
enemy piece; attacked and undefended; defended after moving; promotion;
moving piece attacked before moving; maximum attacked/undefended friendly
non-king material after moving /9. Material values P/N/B/R/Q = 1/3/3/5/9.
These are geometric attack features, including pinned attackers, not legal
static-exchange analysis or an engine. They can miss tactics and overstate risk.
White/Black features must be symmetric. Every legal move retains positive mass.

Select 200 games per event from each frozen January/February snapshot by fixed
hash (600 train, 600 development). Existing M3 token/count estimates stay common
to both models; only the new coefficients use the smaller January panel.
Fit conditional softmax coefficients against January moves with fixed L2=.01,
150 full-batch gradient steps and learning rate .5/sqrt(1+step/20), starting at
zero. No convergence-based extension. Use prior-only likelihood for these
coefficients; development evaluates the final count-plus-prior posterior.
Select beta from [0,.5,1,2] by February decision-weighted log loss, ties favoring
smaller beta. Freeze coefficients, all development scores, sample IDs, config
and code before opening April game rows. No further tuning after this point.

## Untouched April test

Reuse the bounded M3 date-spread extractor in a separate module instance whose
output paths are confined to M8. Do not alter earlier frozen modules/globals.
Use April 2024, three 65,536-row convenience frames per event and at most 100
selected games per frame: at most 900 games. Read only projected columns and
at most two million decoded rows, with existing row-group/512-MiB projection
bounds. Footer discovery may precede model freezing, but extraction must not.
April is new to this research's human-policy training and validation. Verify
no selected game IDs overlap January, February or March snapshots.

Report both models' decision-weighted log loss and Brier score overall, by
event and by zero/positive January position count. Reuse paired player-component
bootstrap for game-level log-loss differences. These are conditional diagnostics
on the convenience frame, not population or causal estimates. Report every
predeclared comparison even if beta=0 wins or the candidate fails.

## Fresh tactical calibration

Select 16 cases per event/support stratum from April using the M7 game/decision
hash design, 96 cases. Keep each actual human move and draw one move per model
with common case-specific uniforms in sorted-SAN order. Freeze these draws
before engine work. Both model draws start at the same actual parent board.

Audit parent plus human/reference/candidate children: at most 384 positions at
100,000 nodes/1 second. The first two cases per stratum form a fixed nested
12-case sensitivity subset, at most 48 positions at 400,000 nodes/2 seconds.
Reuse matching-budget engine records only with frozen binary/FEN identity;
never substitute deeper scores into the base map. Use the existing one-thread,
32-MiB, no-tablebase reader with complete non-bound scores and a 600-second
session guard. No case may be dropped for missing scores or budget pressure.

Report each model versus actual moves and candidate versus reference on
>=150/300-cp mover-oriented loss and the mean capped at 1,000 cp. Use the same
balanced strata and paired player-component resampling as M7. Keep the nested
depth comparison separate. There is no claim of learned retention or free-running
rollout validity from these human-reached one-step observations.

## Resource bounds, checks and decision

Feature construction/evaluation phases have 360-second guards, at most 20,000
cached positions and 700,000 legal rows per panel. Run numerical training with
one BLAS thread; use a separate isolated venv and artifact paths. Existing
canonical files and other processes remain untouched.

Test feature symmetry and tactical edge cases, probability conservation,
zero-strength baseline parity and the objective gradient against finite
differences. Verify fresh-cohort identity and train/test order, independently
replay calibration boards/draws, reconstruct scores and verify complete engine
coverage. Record compact tracked provenance and full ignored snapshots.

A candidate is promising only if fresh overall/event/unseen likelihood improves
and its tactical excess over humans decreases; report uncertainty and failures
without relaxing this interpretation. This cheap candidate is a benchmark,
not a presumed adequate human policy. A negative result is informative. Any
further revision requires new development decisions and new held-out evaluation;
do not silently expand or tune against April.
