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

## Learning and recall experiment

Evaluate every frozen book under perfect recall and under the same study time or review budget. Start with the 30 and 60 cp books while the 100 cp builds run. Keep this experiment outside the build and population-scoring coordinators.

1. **Cold start and switching today.** Compare learning from scratch with transfer of documented practice on the exact board and prescribed move. Track conflicting prescriptions separately. Count distinct positions across transpositions. Estimate exposure using probability of reaching the position while following the repertoire. Record ply, time since practice and plausible alternatives. Keep natural move choice and memory as separate quantities.
2. **Sensitivity before fitting.** Run identical practice schedules and several explicit retention assumptions on each book. Report the fraction introduced, recall weighted by reference book visits, and depth and frequency breakdowns at matched study milestones. These are scenario estimates. Validate them against first-attempt, delayed trainer answers as those accrue. Historical game deviations have a selected denominator. Reconstruct all eligible game decisions before using game adherence as evidence.
3. **Playing strength.** At our turn combine retrieval of the prescribed move with the existing unaided human policy after a miss. The unaided policy can still choose the prescribed move or an equally good alternative. Allow later transpositions back into book. Recompute the distribution of positions reached. Rescore at ply 30 using paired draws across books, both current and improved self-play, and the agreed opponent and speed panels. Keep perfect recall as a control. Report expected score, score lost to lapses, and frequently reached positions where a lapse is costly. Carry uncertainty in the retention assumptions as well as sampling uncertainty through the rankings.
4. **Personal calibration if rankings depend on recall.** Sample roughly 48 finalist positions covering common and rare lines, early and deep decisions and plausible alternatives. Include a representative sample and identify any deliberately oversampled high-impact cards. Assign only one prescription per board, take an unaided baseline, teach for equal time, and test first responses after approximately two days and two weeks. Record review dates, correctness, latency, and intervening exposures. Use new cards or separate groups for delays if the earlier test would refresh the later one. Validate on held-out positions or lines before using a fitted model to rank books.
5. **Builder decision.** First measure whether plausible recall profiles change rankings or expose substantial losses. If so, test allocating the finite study budget by reach times the expected-score gain of knowing a move, including downstream consequences. Compare that candidate against the frozen finalists at the same 2,000 positions and study budget. Keep the objective gates in force.

### First equal-practice pilot

Four 2,000-card books entered the pilot on November 9. The trainer contains 40 explicit reviews on 33 boards. Recorded practice matches the move on 3 White 30 cp cards, 5 White 60 cp cards, and 6 cards in each Black book. The review log points to the trainer card and does not store a move snapshot per attempt. The switching scenario uses that card's stored prescription. Treat this as a lower-information estimate of prior practice.

Each simulated learner gets 30 card presentations and one training game **with that color** per day. A user splitting that budget between colors would reach the same practice totals on a different timescale. New cards are introduced in order of reference-book reach. Reviews favor frequently encountered cards that are likely to be forgotten. Opponent frequencies for training come from the historical construction pool. Training-game reinforcement stops at the first missed or uncovered move and counts a retrieval at most once per card per day. Games used to evaluate playing strength continue through such misses. For the initial sensitivity analysis, the three assumed initial half-lives are 2, 5 and 10 days. Successful spaced retrieval multiplies them by 1.6, 2.0 and 2.4 respectively, up to one year. A failed drill gives feedback and resets the card to the initial half-life. Fit those parameters when there is enough personal evidence.

Eight simulated schedules per book estimate recall at days 0, 30, 90 and 240 from scratch and with transfer. Under the central from-scratch assumptions, day-90 recall weighted by visits during perfect book play was 71.4% and 71.8% for White 30 and 60 cp, and 65.8% and 66.5% for Black. Each book had introduced 920 cards at that point. In White 30 cp, recall was 93.6% on visits to cards first reached at plies 0–7, 51.3% at plies 8–15, and 23.7% at plies 16–29. Two White 60 cp moves that have no historical reply deliver checkmate. One of its 2,000 cards had zero reference reach through ply 30. [Book inputs and matched history](../outputs/rebuild-2026-11-06/recall-pilot/design.json) · [Study summary](../outputs/rebuild-2026-11-06/recall-pilot/study-summary.csv) · [Depth and frequency](../outputs/rebuild-2026-11-06/recall-pilot/day90-by-depth-and-frequency.csv)

The first playing-strength pilot uses the current Rapid scenario, with the existing current self policy, borrowed opponent policy and 30 Maia pseudogames. It draws complete trajectories from the starting position, with fresh retrieval decisions on every visit, paired across gates and retention scenarios. It compares perfect recall, central retention at days 30/90/240, and low/high retention at day 90. Missing engine leaves use one local depth-10 Stockfish worker and a separate persistent cache. Each of eight checkpoint blocks contains 32 games per book and scenario. Use paired block intervals for differences. These intervals cover Monte Carlo and simulated schedule variation within the chosen scenario. Additional opponents, game-fold uncertainty and controlled comparisons at high-impact disagreements belong to finalist evaluation.

The completed from-scratch Rapid pilot contains 6,144 trajectories. Scores are expected game points expressed as percentages. In the central 90-day scenario White 60 minus White 30 was −1.55 percentage points (approximate paired 95% interval −4.85 to +1.75) and Black 60 minus Black 30 was −0.44 (−2.68 to +1.80). Across the four books the estimated loss from perfect recall to this scenario ranged from 6.99 to 12.63 points. The current sample supports measuring the effect of recall more strongly than it supports choosing a relative gate. At the familiar White choice after `1.e4 e5 2.Nf3 Nc6`, the model gives both `3.Nc3` (30 cp) and `3.Bc4` (60 cp) more than 99% central day-90 recall. Check disagreements further down those branches, including where an unaided alternative is costly. [Scores versus study effort](../outputs/rebuild-2026-11-06/recall-pilot/rapid-current/scores.csv) · [Paired differences](../outputs/rebuild-2026-11-06/recall-pilot/rapid-current/paired.csv)

A second panel completed 3,072 trajectories with no preparation, existing review evidence on day zero, and 90 days of practice after switching. With no preparation both gates produced the same score in every matched game of the same color, as required. Crediting the available training history at day zero produced no detectable benefit in this sample. The central estimates versus no preparation were −0.87 and −1.23 points for White, with both paired intervals spanning zero, and zero for both Black books. This measures only evidence captured by explicit drills. Recovering the denominator from historical game opportunities is the next step for estimating existing familiarity. [Switching scores](../outputs/rebuild-2026-11-06/recall-pilot/rapid-switch/scores.csv) · [Paired switching differences](../outputs/rebuild-2026-11-06/recall-pilot/rapid-switch/paired.csv)

To resume from this worktree, use the project's Python interpreter and the absolute paths below so the input fingerprint matches the original run. Reuse the saved profiles when resuming a scoring job.

```powershell
$pilot=(Resolve-Path 'outputs/rebuild-2026-11-06/recall-pilot').Path
$models=(Resolve-Path 'outputs/rebuild-2026-11-06/population-transfer-v1/models/rapid-now').Path
python python/experiments/score_repertoire_recall.py --profiles $pilot --models $models --out "$pilot/rapid-current"
python python/experiments/summarize_repertoire_recall.py $pilot --scores "$pilot/rapid-current"
python python/experiments/score_repertoire_recall.py --profiles $pilot --models $models --out "$pilot/rapid-switch" --scenarios scratch-central-0 switch-central-0 switch-central-90
python python/experiments/summarize_repertoire_recall.py $pilot --scores "$pilot/rapid-switch"
```

The practice model can be regenerated separately with `python/experiments/repertoire_recall.py`, the saved `books.json` manifest, `--trainer` pointing to the main checkout's trainer database, and `--as-of 2026-11-09`. For the 100 cp books, extract historical opponent counts at their new opponent nodes before adding them. Use a fresh output directory for a changed panel.

## Restore all observed choices on our turn

Treat the 30/60/100 cp sweep underway as the historical-cutoff baseline. Before selecting a gate, repeat the comparison with every observed own move available at each explored decision, for both White and Black, including boards newly reachable through restored moves. Use the matching uncut archive slices, combine transpositions, cache lookups across gates, and apply the same engine gates to the expanded choice set. Keep opponent frequencies, Other mass and the 30-pseudogame opponent-policy experiment distinct from our choice of moves. Compare frozen books at 2,000 decisions and rescore them under the same referee. Recheck high-impact changes independently with Stockfish. An engine nomination can still add a move with zero historical observations.

The Black cutoff audit found 28 observations of Petroff 9...f5, 12 of 9...O-O and one of 9...Bf5. Restoring castling changes the only eligible 30 cp move from 9...f5 to 9...O-O. Across 2,000 Black decisions, 37 prescriptions in the 30 cp book and 28 in the 60 cp book lose eligibility when all observed alternatives compete. [Audit](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/floor-audit/candidate-cutoff.json)

Implementation and tests are on `codex/uncut-move-lookup` in the sibling `uncut-move-lookup` worktree. The November 9 evening hold is after baseline Black 100 cp and before White 100 cp. Hold full uncut builds until the next session. Each cache covers the initial White board and/or children of named opponent replies, including replies in a supplied overlay. Its preparation is independent of the current books, gate and budget. The builder uses it during frontier expansion and budget optimization. A novel observed move can still fail an engine gate or lead to an opponent position with too little historical support for further construction.

Use the exact source for each baseline. Black uses the 2000+ banded archive and White uses the original 1800+ monthly consolidations. The separately reconstructed 1800+ banded pool disagrees with the native White pool even at the root. Each lookup checks retained move identities and W/D/L against the old pool and recovered W/D/L against Other. Completed months or archive buckets survive an interrupted cache build. The final merge can be restarted from those parts. The book solver still requires a complete run for a publishable book.

On November 9 the new reader reproduced the Petroff counts and gate menus above. The Black preflight measured 7.1–11.0 seconds per bucket on three of 512 equally sized buckets. The final White streaming query took 18.4 seconds for 1.37 GB and 111.7 seconds for 10.22 GB, out of 753.57 GB across 163 months. Estimated input stages are 1.2 hours for Black and 2.4 hours for White. Full aggregation, verification and disk spill are additional and have not yet been measured. Allow roughly 1½–3 hours for the completed Black cache and 4–8 hours for White. Store the cache and DuckDB temporary data on a high-free-space disk.

Tomorrow, start with Black 30 cp, check the Petroff gate and record expanded graph size, memory, elapsed solve time, distinct count and practical score. Run White 30 cp and then 60 and 100 cp in each color, revising the time budget from the first complete solves. Compare to the frozen cutoff baseline at the same cap using the same population, stop model, evaluation weight and referee. Include reach-weighted prescription changes, missing-evaluation coverage and fresh Stockfish checks of the largest losses.

The recommended next experiment skips the old White 100 cp build. Five frozen baseline books provide matched controls, including two White caps and the permissive Black cap. Regenerate all six books with the expanded menus. White 100 cp can be compared with the other corrected White caps. If a same-cap White before/after estimate becomes important, its archived 60 cp recipe with `--max-eval-loss 100` reconstructs the omitted control. Keep old and corrected populations of books in separate scoring manifests.

In the old White 30 and 60 cp books, 230 and 265 of the 2,000 decisions respectively had just one named historical move and positive Other mass. Across all decisions, 1,940 and 1,950 respectively had positive Other mass. These are exposure counts. The number of additional moves that will pass the gate requires the native lookup and a new search.

Before timing the permissive corrected books, test reusing the graph’s topological order across the budget probes in a single solve. At 23:58 Pacific on November 9, a nonblocking sample of Black 100 cp found extra probe 61 of 64 inside `topo_down`, sorting the same graph again. `match_distinct` changes the charged budget while its graph stays fixed. Compare the charged budget, selected moves and realized value against the original extractor, including a graph with cycles. Any shared order belongs to one concrete graph. The current screen reads existing engine arrays and does not produce new Stockfish or Maia predictions.

From the `uncut-move-lookup` checkout, set `$side` and `$gate` for the desired book. The example reads the original population and solver arguments, uses fresh output paths, and reuses the cache across gate values.

```powershell
$side = 'black'
$gate = 30
$baseline = '../practical-repertoire/outputs/rebuild-2026-11-06/cp-screen-030-060-100'
$templateGate = if ($side -eq 'white' -and $gate -eq 100) { 60 } else { $gate }
$receipt = Get-Content "$baseline/stages/$side-cp$templateGate.json" -Raw | ConvertFrom-Json
$exe = $receipt.command[0]
$first = [array]::IndexOf($receipt.command, '--rep')
$buildArgs = [string[]]($receipt.command[$first..($receipt.command.Count - 1)] | Where-Object { $_ -ne '--force' })
$buildArgs[[array]::IndexOf($buildArgs, '--max-eval-loss') + 1] = [string]$gate
$pool = $buildArgs[[array]::IndexOf($buildArgs, '--stats') + 1]
$aux = $buildArgs[[array]::IndexOf($buildArgs, '--aux-stats') + 1]
$work = "D:/chess/uncut-own-v1/$side"
$cache = "$work/cache"
$buildArgs[[array]::IndexOf($buildArgs, '--out-prefix') + 1] = "$work/cp$gate/book"
$buildArgs[[array]::IndexOf($buildArgs, '--tmp-dir') + 1] = "$work/cp$gate/tmp"
& $exe python/uncut_own_moves.py --stats $pool --aux-stats $aux --perspective $side --out $cache
if ($LASTEXITCODE -ne 0) { throw 'Cache preparation failed' }
& $exe python/build_budget_books.py @buildArgs --uncut-own-moves $cache
if ($LASTEXITCODE -ne 0) { throw 'Book build failed' }
```

## Where to resume

On November 10 the Black nomination sampler and baseline population scorer resumed here. The corrected six-book sweep and its paired referee run from the sibling `codex/uncut-move-lookup` checkout. The restart commands, Petroff count check and 2200+ archive prerequisite are in [the active worktree's plan](../../uncut-move-lookup/docs/lifetime-repertoire-plan.md#november-10-restart). Build status is in `D:/chess/uncut-own-v1/status.json` and referee status in `D:/chess/uncut-own-v1/referee/status.json`. The cutoff screen stays held before White 100 cp.

After the corrected six-book gate sweep, the first additional pool build is Black 60 cp on all-speed 2200+. Pair it with corrected Black 60 cp on 2000+ under the same referee and 2,000-position budget. On 131,355 White OTB decisions after the opening move, the Brier scores for 1800+, 2000+ and 2200+ were 0.5163, 0.5020 and 0.4918 respectively. Keep White at 1800+ and Black at 2000+ for the initial gate sweep. Record each pool’s implied absolute floor and reply coverage when interpreting a difference. [OTB results by side](../outputs/rebuild-2026-11-06/population-transfer-v1/lichess-speed-cutoffs/by-side-all-speeds.json)

Baseline Black 100 cp finished November 10 at 00:06:13 Pacific after 4 h 23 min 17 s, with exactly 2,000 distinct decisions. The coordinator is held before White 100 cp. Five cutoff baselines have successful receipts. The 100 cp Black book shares 1,260 exact prescriptions with 60 cp, 576 with 30 cp and 557 with canonical. On the 2000+ historical White reply distribution, its book-exit engine score is 61.23% versus 60.36% for 60 cp. The probability of reaching a booked move evaluated below −1.00 for Black rises from 1.01% to 8.84% using stored values. The new Petroff 3...c6 is reached in 15.18% of games and falls from −0.98 stored to −1.14 at two million nodes. Substituting that single evaluation raises the 100 cp exposure to 16.69%. Keep the separate ply-30 human-continuation scores as the conversion test. [Comparison and reach](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/book-comparison/black-100-comparison.json) · [Opening PGN](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/book-comparison/black-100-opening-comparison.pgn)

These links point to local artifacts in the practical-repertoire worktree:

| Question | Evidence |
|---|---|
| Which gate build is active or complete? | [Current stage](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/status.json), [successful stage receipts](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/stages), [logs](../outputs/rebuild-2026-11-06/cp-screen-030-060-100/logs). After a restart, check the log and live process before relaunching a coordinator. |
| Is population scoring waiting or running? | [Progress](../outputs/rebuild-2026-11-06/population-transfer-v1/progress.json) and [design](../outputs/rebuild-2026-11-06/population-transfer-v1/design.json). “Waiting for frozen books” refers to repertoire scoring. The [prediction scores](../outputs/rebuild-2026-11-06/population-transfer-v1/validation.json) already exist. |
| Why these pool candidates? | On 316,018 OTB decisions by 1500–2199 movers, the reconstructed 1800+ all-speed pool scored 0.5477 Brier, removing Blitz scored 0.5593, and 2200+ all-speed scored 0.5270 (lower is better). Raw 2200+ Classical was competitive with abundant local data and weaker at sparse positions. [Time-control and cutoff comparison](../outputs/rebuild-2026-11-06/population-transfer-v1/lichess-speed-cutoffs/results.json) |
| Why retain Chess.com separately? | Summing both historical exports scored 0.5539 versus 0.5475 for the native Lichess export alone on the same OTB decisions. [Merge comparison](../outputs/rebuild-2026-11-06/population-transfer-v1/chesscom-1600/merged-summary.json) |
