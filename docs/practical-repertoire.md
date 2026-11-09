# Practical repertoire experiments

Run these commands from the checkout containing this branch with the project Python interpreter. In a linked worktree, the original checkout's virtual environment also works. Write candidates and experiment results to a dedicated output directory.

## Nominate moves before selection

For hybrid nomination, run `python python/hybrid_candidates.py --targets targets.parquet --colour black --out hybrid.parquet` (also supports White). The default database is the merged `D:/chess/eval_full`, built from the raw cloud and fishnet downloads on H:. An exact hash and EPD match can supply the legal first cloud PV move at depth 22 or higher. The default minimum knodes is zero. Ambiguous hashes and decisive fishnet disagreements go to local Stockfish. Pass `--keep-local FILE` to reuse completed local searches from the same target file, engine, node budget and thread count.

The rebuild reconciles transpositions before each graph that consumes proposals. For unverified incoming scores it prefers a local nomination, then the greatest depth, then the greatest node count. Equal-quality scores use their median. A spread over 30 cp between incoming edges from different parents triggers a fresh 30M-node search from the shared child. The proposal overlay records the positional score separately from each incoming search and its move-specific verified loss.

The feedback loop searches selected cloud moves locally at 15M nodes and feeds those scores back into selection. The publication audit reuses those searches. The JSON audit records the number selected, checked, over the book's loss limit and unstable. A fresh post-selection audit of the frozen finalists is a separate step.

1. Run the intended `build_budget_books.py` recipe with `--max-eval-loss CP --dump-engine-targets targets.parquet`. This exports reachable decisions where the recorded choices fail the loss limit, have unknown evaluations or leave no candidate. It exits before optimizing a book.
2. Run Stockfish on those positions. For a bounded trial, `--limit N` selects the N highest optimistic reach weights. Inspect the number left outside the search.
3. Repeat the original builder command with `--engine-candidates proposals.parquet`, using a fresh candidate output prefix. For sampled-stop or sampled-other builds, regenerate the graph dumps and sample tables with that proposal file first. The builder checks its content fingerprint since proposals can change reachable continuations. The search evaluates recorded and nominated moves under the same soundness rule. If the new move has recorded opponent replies, the builder can prepare below it. A new move without reply data is valued as a leaf.
4. Validate and score the resulting deliverable. The target list describes the initial graph. Another pre-selection pass with the proposals supplied can discover problems newly reachable through them.

```powershell
python python/engine_candidates.py --targets targets.parquet --colour black --nodes 10000000 --limit 100 --out proposals.parquet
```

Proposal caches are tied to the input file, color, engine binary, search budget and thread count. Supply a new cache path for different settings. In the bounded Queen's Gambit example the original pool contribution was `7...Bc5`, and Stockfish nominated `7...Nh6` at three million nodes. Nominating a move lets the optimizer compare it with stopping and spending the card elsewhere.

## November 9 Black opponent-policy challenger

After a reboot, start the November 8 coordinator below and run this second command
from the same worktree in a separate terminal.

```powershell
C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -u python/experiments/run_opponent_prior_challenger.py
```

The challenger waits for the broad Black selection and reuses its verified nominations.
It applies 30 Maia pseudogames at 15,235 White-to-move positions drawn from the
canonical and provisional books' decisions and alternatives. At these positions,
all historical replies have individual successors. The rest of the graph uses
the original empirical policy. Both graph expansion and move selection use
the blended probabilities. True game counts still determine empirical outcome
estimates. The experiment tests naming rare replies and adding the prior together.

Under `outputs/rebuild-2026-11-06`, the `opponent-prior-pilot` directory holds
`design.json`, `results.json`, `matched-results.json`, and the reusable policy cache.
The separate `opponent-prior-challenger/status.json` records the active stage or
dependency. Its verified book, per-scenario comparisons against both references,
and stage receipts stay in that directory. All seven comparisons run to ply 30.

### Expanded historical replies, K=0 versus K=30

The follow-up uses the same frozen Black decision inventory for both books. It
looks up individual replies and their actual win/draw/loss counts in the archive,
including positions where every reply falls below 20 games. A frontier lookup
can introduce one of those positions even if the main pool returns no rows.
Parents with zero historical replies cannot define an empirical K=0 policy and
are excluded from both arms. Both books preserve the observed probability of
game endings and horizon observations. K only changes the choice of reply given
that someone plays a move.

The November 9 inventory contains 16,736 positions. Of these, 16,680 have
historical replies, including 1,445 with no retained move. These newly eligible
positions account for 44,910 historical replies. The most frequent such position
had 103 replies spread over 14 moves, with 18 games for the most popular move.

Run this coordinator alongside the two commands for the earlier comparisons
after a reboot. It can resume the extraction from any completed bucket. It
waits for the original Black comparison before selecting the two books. If a
verification changes a nomination, both final books must use the corrected
proposal set. Each book gets its own stop calibration and 2,000-decision audit.

```powershell
C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -u python/experiments/run_expanded_reply_comparison.py
```

Outputs are under `outputs/rebuild-2026-11-06/expanded-opponent-comparison`.
`status.json` identifies the current stage. After scoring, each scenario has
`paired-vs-BASE.json` for the comparison with the broad book and
`paired-vs-EXP0.json` for the incremental effect of K=30.

### November 9 root-based search pilot

The pilot starts from the initial board and takes two sets of six first moves by
historical frequency rank. The main set uses ranks 1, 2, 3, 5, 7 and 10. The
second uses 4, 6, 8, 9, 11 and 13. It follows popular and lower-ranked routes
under K=0 and K=30. The canonical and provisional books play no role in
eligibility. An audit starts at plies 1, 5, 9 and 11 wherever the sampled route
has a Black candidate. Each audited position considers all legal Black moves
with cached evaluations passing the builder's absolute, relative and 30 cp gates.

Each subtree reserves one study position for the first Black move and can
prepare zero, one or three further Black answers after White replies. The
pilot conditions on a White move being played. If there are no historical
replies, both arms use the parent evaluation. Once preparation ends, the
model takes one empirical weak Black move and uses the cached Lichess-curve
evaluation. With fewer than 20 weak games it uses the position's own score,
with 0.5 as the fallback for a missing evaluation. These local budgets and
leaf values define the reference used for this pilot.

The five-game pool supplies named reply counts and the remaining historical
mass. Maia supplies the full legal distribution at every reached White decision
in the pilot. The selective solver computes the unaided Black score for each
reply. Until it evaluates a prepared answer, its possible improvement is at
most one minus the unaided score. With B positions available including the
first Black move, only B−1 of these gains can enter the candidate's upper bound.
The solver also bounds what it can gain or lose from the remaining unnamed
historical mass. It refines competitive candidates across both K values and all
budgets until the upper bound on the optimum exceeds the selected policy's
lower bound by at most 0.001 expected points per game. The archive and deeper
response queries are shared across those comparisons.

The first implementation used a wider bound and achieved 1.58× end-to-end on
the main set. A budget-aware version reduced the response searches but read
the archive for all 81 unresolved parents and achieved 1.16×. The final version
orders work by potential improvement per estimated cost. It prices one archive
lookup at twenty response lookups for scheduling. This affects the order of
work, while the stopping condition retains the same bound. The second opening
set was first scored after this scheduling rule was fixed.

| Final run | Subtrees | K/budget cases | Same first move | Same whole local book | Selective / full including shared setup | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Main set | 22 | 132 | 132 | 132 | 18.48 s / 35.34 s | 1.91× |
| Second set | 20 | 120 | 120 | 116 | 19.34 s / 49.14 s | 2.54× |

The complete reference recovered every unresolved historical reply distribution
and evaluated every positive-probability response in the bounded trees. It ran
second on the main set and first on the second set. Both methods saw identical
common inputs per set. The main set reused its cached Maia predictions from
development. Of the 169 White positions on the second set, 168 needed fresh
predictions. These are single-machine elapsed times with other chess work active.

Summing both sets gave 37.82 s selective versus 84.48 s exhaustive (2.23×).
A replay with solve order reversed on each set and all Maia predictions cached
reproduced every per-case decision and score. It took 36.08 s versus 65.94 s
(1.83×). The 2× end-to-end target was met on the first combined run only.

The selective runs performed 52 raw parent lookups and 1,276 deeper response
evaluations in total. Full expansion performed 148 and 5,820 respectively.
All 165 cases with a competing Black move and a lead exceeding 0.001 agreed on
the first move. The largest loss from a different follow-up was 0.000111 points
per game, or 0.0111 percentage points, measured against full expansion at that
subtree. For example, after `1.c4 Nf6 2.Nc3 e5 3.g3`, both methods chose
`3...d5` and prepared for `4.cxd5` and `4.Bg2`. The exhaustive book also used
its last available position on `4.e3 d4`. Across the 42 distinct roots, 30 were
absent from both earlier books' booked Black decisions.

The next integration test should use a common study budget across openings,
charge a transposed position once, and apply the production ply-30 sampled
stop model. The pilot's small-tree error bound and timings apply to the local
objective above.

The two run directories are under `outputs/rebuild-2026-11-06/root-search-pilot`.
Each has `design.json`, `result.json` and a per-case `cases.json`. To reproduce
from this worktree, use fresh output directories and the project's interpreter.

```powershell
$pilot = 'outputs/rebuild-2026-11-06/root-search-pilot'
$python = 'C:/Users/David/Documents/Chess/.venv/Scripts/python.exe'
& $python -X utf8 python/experiments/root_search_pilot.py --out "$pilot/replay-main" --policy-cache $pilot --panel main
& $python -X utf8 python/experiments/root_search_pilot.py --out "$pilot/replay-second" --policy-cache $pilot --panel second --full-first
```

## November 8 nomination and tuning experiment

Resume with the project interpreter from this worktree:

```powershell
C:/Users/David/Documents/Chess/.venv/Scripts/python.exe -u python/run_nomination_comparison.py
```

The run is under `outputs/rebuild-2026-11-06/nomination-trial`. The original Black
stage receipts and sampler checkpoints remain under its parent directory. The coordinator
checks the Windows run locks, reuses completed stages, and resumes broad Black after a
shutdown. Start one coordinator. Its queue takes precedence over the old two-color
driver. Read `design.json`, `queue.json`, the current stage's status and log, and
`comparison-black/decision.json` for the design, schedule and decision.

* Both Black arms use the same frozen solver, 2,000 decisions, loss limit, mover populations,
  ply-30 horizon, stop/other sampling and cloud feedback. If the DP skips 2,000, use its
  existing polish on the affected arm and check the resulting count.
* The targeted arm starts from the pre-nomination graph, requests every eligible cloud PV
  at discovered failure targets, then builds its own provisional book. It requests a local
  nomination for uncovered booked failures and missing/borderline evaluations (within 15 cp
  of the loss limit). The cumulative off-book allowance is 128 uncovered failure targets with
  optimistic reach at least .001 that can arise after a different recorded book-side move and
  an opponent reply. Requests are frozen by hash and EPD before replay. Only raw 10M local
  results for those requests may enter from broad Black. Missing requests are searched.
  Selected cloud moves get the ordinary independent feedback check. Calibration and subsequent
  feedback never borrow broad's book or verified overlays.
* Score both frozen arms, canonical, FREE and the historical 2,001-decision ENGINE reference
  together on four common seeds. Separately score 16 shared game-fold replicates at seed 1.
  Report the four-seed mean difference, a one-sided three-degree-of-freedom rollout allowance,
  and empirical fold perturbations centered on that seed. The resulting bounds are a development
  heuristic. Sixteen replicates give limited precision at the tails. Compare nominal engine work,
  actual stage times and estimated cold hours at 1,428 local nominations/hour. The cloud part
  write span approximates disk lookup time; search receipts after restarts omit earlier uptime.
* Freeze a fresh safety audit at 30M nodes (60M retry for instability) of commonly reached
  changes, flagged/borderline decisions and 32 controls. Audit both moves on the same board.
  Reach here is expected visits before book exit under the recorded opponent pool, over 30 plies.
  Common means at least .001 expected visits/game. Record severity and reach for each check.
* Use targeted as the development default if its lower bound is at least -.002 expected score,
  cold time improves, and the audit finds no additional common risk or unresolved common
  instability. A clear score loss or additional confirmed common failure favors broad. An
  inconclusive result permits a provisional targeted White and keeps its exhaustive queue
  deferred. Changing the canonical books still requires the separate promotion and holdout tests.

For quick parameter changes use `python/screen_repertoires.py --one black relative-tight`
(also accepts `white`). The one-factor screen varies the absolute and relative score gates
independently from .10 to .06 and .14, with the source repertoire held fixed for each color.
It uses the inexpensive weak stop model, followed by two paired seeds and one rollout for
ranking. The best recipe and baseline get freshly calibrated stop/other tables at the
shortlisted Maia weights and four paired seeds. The human evaluator stays fixed at k=100.
Elapsed time for a complete grid is separate from a single warm screening build.

Compare 2000+ at floors 5, 20 and 50 and compare 1800+ and 2000+ at floor 50. The two
rating pools were constructed by different historical producers, so interpret the latter
as an operational pool comparison. `rebucket_pool.py` moves discarded edges and their
covered expected scores into the aux and verifies per-parent count conservation.
`tune_maia_shrinkage.py` uses game folds 0–47 for counts (scaled to the full table size)
and 48–63 for observed moves at sampled canonical positions, using only August–September.
It saves move-weighted and position-weighted log loss plus actual mass removed by the
production probability cutoff. October and November are reserved.

## Recall and difficulty

`convertibility_eval.py --recall 0.85` remembers a prescribed move with probability 0.85 on each visit. On a miss, the player uses the usual weak-player policy and may find the prescribed move anyway. The result's book-move count measures remembered decisions. The default is perfect recall. The exact walk splits probability at covered nodes. Sampled walks use a separate random draw for remembering. Later arrivals can re-enter the book.

Add `--exits-out PATH` to write one parquet per book and seed containing the first unaided decision and its unconditional probability. The probabilities can sum to less than one when games reach the horizon or finish inside the book. Pass all partitions for the selected book, including empty ones, to the difficulty calculation. It also accepts a single saved boundary-study parquet containing hash, ply, EPD and mass.

```powershell
python python/exit_difficulty.py --exits E:/chess/repertoire/_align/L28.parquet --colour white --our-moves E:/chess/holdout_explorer/movers1000_white_f64_2026_08_09 --opp-moves E:/chess/holdout_explorer/movers1800_black_f64_2026_08_09 --top 24 --decisions 3 --games 32 --threshold 0.10 --out outputs/L28-risk.parquet
```

At each selected exit, the tool plays through the next three unaided decisions or the absolute horizon, whichever comes first. The risk column estimates the probability that at least one of those moves costs 0.10 of uniform engine utility. It also reports the mean worst loss, sampled decisions, per-exit Monte Carlo uncertainty, and the covered root mass. Compare books using both score and risk, keeping the covered mass in view. Model ratings, tables and engine settings should match the scenario being studied.

## Compare candidate books

For a wholesale switch, retain the November 4 decision rule: at least +.005 modeled expected score against the same-color canonical 2,000-decision book over four paired seeds, a gain clearly above game-fold bootstrap uncertainty, and a fresh Stockfish check of commonly reached moves. Compare the unrestricted finalists for both colors and the previous engine-nominated Black finalist. Check each canonical seed before reusing previous scores. Freeze the books and decision rule before scoring the reserved month. A narrow repair to the incumbent needs its own paired test and an agreed tolerance for deterioration before seeing the result.

`practical_scenarios.py` runs one color at a time. Use the same seeds for each book in a scenario and keep an incumbent in every comparison. It reports the paired gain and its Monte Carlo standard error alongside the number of prescribed decisions, changes from the incumbent, and optional safety audit fields.

```powershell
python python/practical_scenarios.py plan --book L28=E:/chess/repertoire/final/lifetime_white_2026-10-28.parquet --book FREE=E:/chess/repertoire/_candidates/cand2free_white_b2000.parquet --base L28 --colour white --out outputs/white-comparison
```

Use `--scenario` repeatedly to select conditions. `baseline` uses 1000–1799 movers against 1800+, and `recall85` varies only recall. `future` uses 1800–1999 movers on our side, and `strong-opponent` uses 2200+ movers on theirs. `blitz`, `rapid` and `classical` each require tables filtered to that Lichess event on both sides. Online classical is a proxy for slower play.

`plan` prints the exact commands and identifies missing cohort tables. `prepare` builds the requested missing August–September development tables. `score` verifies population settings and completed months, then runs the comparisons. Table extraction and full ply-30 runs can be lengthy. Each scenario writes scores and exit partitions under its own directory. `comparison.csv` contains one row per book and completed scenario. Pass `--safety LABEL=validator.json` for each available exact-file audit. Blank safety fields mean an audit was not supplied. Paired seed errors measure rollout uncertainty. Data uncertainty and testing on reserved months require separate runs.

To include the ease-of-play measure directly in `comparison.csv`, add `--difficulty-exits 24 --difficulty-games 32` to the score command. The result reports total first-exit mass, the mass selected for evaluation and the conditional risk over that selection. A covered position with a missed recall can therefore qualify for practice even when the book contains it.

## Practice and review

```powershell
python python/exit_practice.py deck --colour white --exits L28=E:/chess/repertoire/_align/L28.parquet --book L28=E:/chess/repertoire/final/lifetime_white_2026-10-28.parquet --exits EMP=E:/chess/repertoire/_align/EMP2000w.parquet --book EMP=E:/chess/repertoire/_budget_emp/emp_white_b2000.parquet --out outputs/white-deck
python python/exit_practice.py review-games --player "Your PGN player name" --pgn tournament.pgn --book white=E:/chess/repertoire/final/lifetime_white_2026-10-28_ext_c10.parquet --book black=E:/chess/repertoire/final/lifetime_black_2026-10-24_ext_c10.parquet --out outputs/tournament-review.csv
```

The deck weights exits by their frequency within each source, gives each source equal total weight, and reserves up to half the deck for positions where the books disagree. It samples distinct boards. Each PGN opens directly at the exercise position. The second randomized session comes seven days later and swaps the assigned time control for each position. Record your first move, time, plan, result, and first serious error in `observations.csv`. Reveal the source and recommendations in `private/key.json` after both sessions. Use a fresh directory for each experiment.

The game-review worksheet identifies the first move that departed from a prescribed line or the first position outside the book. It preserves clock and elapsed-move comments when present. Enter whether the practical cause was recall, lack of a plan, tactics or something else, and record the first serious error separately. An uncovered position can be played well.

After practicing, `python python/exit_practice.py results --deck outputs/white-deck --out outputs/white-results.csv` joins completed observations to the private source key, checks the first moves on their boards, and expresses each game result from your side.
