# Milestone 2: documented cohort and longer-horizon validation

Pre-analysis plan, 2026-09-05 UTC. Continue on `codex/human-repertoire` from
`9cffaca`; no baseline update or production pipeline changes.

Question: with documented player filters and a longer horizon, is early advantage
observed often enough, and are move/outcome predictions supported well enough, to
make the recall comparison informative? A negative adequacy result is acceptable.

Population: user specified rated Blitz, Rapid and Classical with ratings >=1800.
Retain the stated absolute rating difference <=200; both individual ratings must
clear 1800. Use completed
results; exact move-order prefix `1.e4 e5 2.Nf3 Nc6`. Do not select games by result,
engine evaluation, future advantage, later agreement with a book, or termination
class. Preserve endings, including flags, as observed human events for audit.

Take exactly the first 262,144 source rows of row group 0 in `part-0.parquet` of
January and February 2024, separately for each of Blitz, Rapid and Classical.
This is a reproducible convenience frame, **not** a
random sample of either month. Audit actual dates, ratings and time controls.
Cap each decoded projected row group at 512 MiB and 1,000,000 rows; use batches of
8,192, one thread. Keep at most 2,000 root-matching games per month via a fixed
SHA-256 rank of game ID across the three strata. Report each time control
separately. Pooled weights represent eligible games in these equal-sized source
frames, not the full BRC population. Filter ratings and prefix before legal replay; snapshot
only bounded selected inputs. Reject malformed records and report every reason.

January is the training/development cohort. February is a separate validation
cohort, not an untouched final test; do not tune on its outcomes. Freeze extraction
recipe and identities. Duplicate game IDs cannot cross cohorts. Player overlap is
reported; game-level independence must not be inferred from distinct IDs.

Replay at most 12 plies after the root. Reuse this checkout's signed Polyglot hash
and verified read-only engine arrays. No engine jobs or source re-extractions.
Retain graph mass at terminal/censored/unknown continuations and quantify unresolved
first-hit probability. Existing mixed-version engine values remain a limitation.

Fit simple position-frequency and shrunk W/D/L models on January only. Check
February move log loss/Brier and support; check per-game root-branch outcome
predictions. Report modest baselines and sampling uncertainty without claiming
causal validity or calibrated personal recall. Candidate books are tiny and use
training-derived positions/moves; the frozen original reference remains `3.d4`.
All fixed plans share an evaluator within each cohort. Separate train-optimal
plans, their validation predictions, and any hindsight diagnostics. Compare
independent recall with an explicitly correlated good-session/bad-session mixture
of the same mean recall, without fitting a new learning model.

Deliver extraction and modelling code, meaningful tests, a frozen config and
provenance, local results, and a concise adequacy report. Keep all generated files
under `_artifacts/m2/`. Do not expand source caps, horizons or model jobs merely to
obtain a positive result. Personal recommendations remain outside this step.
