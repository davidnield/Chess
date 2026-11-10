# Lifetime repertoire experiment plan

Agreed November 9, 2026, on `codex/practical-repertoire`. Keep this page current when a decision changes. Consult the local run receipts for progress.

## Objective and fixed settings

Choose one book per color, each with 2,000 distinct positions at which we prescribe a move. It should be learnable now (1800 Lichess Rapid, 1700 Blitz, provisional 1100 USCF) and useful through at least 2000 online and 2000 USCF classical. Compare practical expected score after book exit, ease of conversion with weaker human play, and objective safety against strong replies.

| Setting | Decision |
|---|---|
| Opponent move model | Use 30 Maia pseudogames, applied once to a position's reply distribution. |
| Absolute gate | Keep the expected-score margin at 0.10 from the construction pool's prior. In the current pools the effective floors are about −0.90 pawns for White and −1.32 for Black, from our side. Record the prior and implied floors when changing pools. |
| Relative gates | Sweep the engine loss cap at 30, 60, and 100 centipawns for each color. Hold the separate relative expected-score margin at 0.10 and the parent allowance at 0.02. Select the cap per color. |
| Current construction pools | Lichess Blitz, Rapid, and Classical, January 2013–July 2026, game mean rating 1800+ for White and 2000+ for Black. Opponent prediction studies that use mover ratings label them separately. |

The 100 cp arm lets the Blackmar–Diemer and From’s Gambit compete alongside the King’s Gambit, Danish, Schliemann and Albin. Their inclusion in a final book depends on selection and validation. The two-color screening builder uses a weak-player stop model and historical pool counts. Its paired reference evaluator has a legacy Maia weight of 100. The separate population evaluator uses 30. Compare books within each evaluator.

## Experiments and decisions

1. **Gate screen.** Build and verify all six 2,000-position books using the current construction pools. Compare each color’s books against its canonical book at ply 30 with paired seeds. Look at the opening choices and positions displaced as well as score differences. Carry close contenders into the pool test.
2. **Population rescore.** Freeze each color's three books once all three pass the footprint check. Score them across current and goal Blitz, Rapid and slower environments with both current and stronger future play on our side. The development design gives the three speeds equal standing and reports online 2200+ Classical separately as a stress test. OTB move prediction is an independent completed experiment. Published tournament ratings are a proxy for the USCF goal.
3. **Construction-pool comparison.** Rebuild competitive gate settings with the full historical 1800+ all-speed control, the 2200+ all-speed challenger, and a candidate that borrows from strong Blitz/Rapid where 2200+ Classical evidence is sparse. Fit or validate the borrowing rule on fresh data before fixing it. The last candidate needs rare Classical replies. The observed crossover near 100 local observations is a hypothesis to test. Keep Chess.com 1600+ Blitz/Rapid for an independent check on important reply frequencies.
4. **Final selection.** Compare finalists against canonical at matched budgets, with paired ply-30 scores, stronger opponents, rapid/classical results, and game-fold uncertainty. Inspect common changed moves, practical book exits, and sensitivity to repeat players. Independently verify selected cloud moves and borderline decisions locally with Stockfish. Use the existing promotion criteria before replacing canonical books. [Validation and promotion workflow](practical-repertoire.md#compare-candidate-books)

## Where to resume

At **7:36 p.m. Pacific on November 9**, Black 30 cp, White 30 cp and Black 60 cp had successful build receipts and exactly 2,000 decisions each. White 60 cp was running. The 100 cp arms were queued. Black's completed books shared 971 identical prescriptions. No paired screen scores were available. The next decision is which caps to advance after scoring. The new construction-pool builds are a subsequent experiment.

These links point to local artifacts in the practical-repertoire worktree:

| Question | Evidence |
|---|---|
| Which gate build is active or complete? | [Current stage](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/status.json), [successful stage receipts](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/stages), [logs](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/logs). After a restart, check the log and live process before relaunching a coordinator. |
| Is population scoring waiting or running? | [Progress](../outputs/rebuild-2026-11-06/population-transfer-v1/progress.json) and [design](../outputs/rebuild-2026-11-06/population-transfer-v1/design.json). “Waiting for frozen books” refers to repertoire scoring. The [prediction scores](../outputs/rebuild-2026-11-06/population-transfer-v1/validation.json) already exist. |
| Why these pool candidates? | On 316,018 OTB decisions by 1500–2199 movers, the reconstructed 1800+ all-speed pool scored 0.5477 Brier, removing Blitz scored 0.5593, and 2200+ all-speed scored 0.5270 (lower is better). Raw 2200+ Classical was competitive with abundant local data and weaker at sparse positions. [Time-control and cutoff comparison](../outputs/rebuild-2026-11-06/population-transfer-v1/lichess-speed-cutoffs/results.json) |
| Why retain Chess.com separately? | Summing both historical exports scored 0.5539 versus 0.5475 for the native Lichess export alone on the same OTB decisions. [Merge comparison](../outputs/rebuild-2026-11-06/population-transfer-v1/chesscom-1600/merged-summary.json) |
