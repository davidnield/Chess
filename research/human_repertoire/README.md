# Human repertoire prototype

Finite-horizon evaluator, exact small-book search, independent complete-path
oracle, synthetic fixtures, and a bounded real chess adapter. Read
[the model contract](../../docs/human_repertoire/MODEL.md) and
[the experiment report](../../docs/human_repertoire/milestone1_report.md).

From PowerShell on this machine:

```powershell
Set-Location C:/Users/David/Documents/Chess-human-repertoire
uv --cache-dir research/human_repertoire/_artifacts/uv-cache venv research/human_repertoire/_artifacts/venv --python 3.11.15
uv --cache-dir research/human_repertoire/_artifacts/uv-cache pip install --python research/human_repertoire/_artifacts/venv/Scripts/python.exe -r research/human_repertoire/requirements.txt
$researchPython = 'C:/Users/David/Documents/Chess-human-repertoire/research/human_repertoire/_artifacts/venv/Scripts/python.exe'
& $researchPython -B -m unittest research.human_repertoire.test_model -v
& $researchPython -B -m research.human_repertoire.experiments --synthetic-only
```

The synthetic evaluator and test suite use only Python's standard library;
the three pinned dependencies support the real adapter. `-B` prevents bytecode
caches outside the artifact directory. No production CLI is invoked.

The following step reads external chess files; first inspect current commit
headroom and active jobs. It caps each stats/reference input at 8 MiB/100,000 rows,
the graph at 500 nodes, and uses sparse read-only engine mmap lookups. Never
substitute a full pooled stats file for the small input and raise the cap casually.

```powershell
Get-CimInstance Win32_OperatingSystem | Select-Object TotalVirtualMemorySize,FreeVirtualMemory
Get-CimInstance Win32_Process | Where-Object { $_.Name -match 'python|duckdb|stockfish' } | Select-Object ProcessId,Name,WorkingSetSize,CommandLine
& $researchPython -B -m research.human_repertoire.extract_real
& $researchPython -B -m unittest research.human_repertoire.test_model research.human_repertoire.test_real -v
& $researchPython -B -m research.human_repertoire.experiments
```

The experiment runner only reads frozen worktree snapshots. Extraction re-runs
verify source/config/adapter identity and snapshot hashes, then skip; they fail on
drift. Preserve an old snapshot under another artifact directory before explicitly
starting a revised experiment. Do not edit frozen snapshot files to bypass checks.
One adapter test intentionally rechecks external input identities for this guard.
Without real inputs, use the synthetic commands; real-data tests skip when no
snapshot exists. Synthetic defaults need no personalisation answer.

Configuration is `docs/human_repertoire/experiment.json`; every row of the saved
grid includes a fully resolved `Config`, including all overrides. Search is exact
only over the supplied candidates and raises at 200,000 books. This run requires
at most 112 books per real optimisation; it is not designed for large repertoires.

Outputs under `research/human_repertoire/_artifacts/`:

| Path | Contents |
|---|---|
| `runs/synthetic.json` | Synthetic-only results, serialised fixtures, all arms |
| `runs/milestone1.json` | Full grids, resolved configs, selected books, W/D/L, first-hit and exit metrics, sensitivities, code/dependency fingerprints |
| `real/graph.json` | 79-position graph, FENs, candidate moves, reference projection |
| `real/{stats_rows,aux_rows,reference_rows,eval_rows}.json` | Bounded raw inputs needed to audit the model |
| `real/provenance.json` | Source identities, schemas, snapshot hashes, limitations |
| `resource_check.json` | Captured pre-extraction memory/workload check |
| `test_results.txt` | Final recorded test output |

Code boundaries: `model.py` is the evaluator/search; `oracle.py` deliberately
duplicates the mathematical specification independently; `fixtures.py` generates
the small examples; `extract_real.py` reuses only this checkout's `zobrist` and
read-only `eval_arrays` helpers; `experiments.py` compares arms. No trainer, data
writer, production optimiser or engine-analysis process is imported or launched.

## Milestone 2: documented BRC cohort

The next completed step uses both players rated >=1800 across Blitz, Rapid and
Classical (rating gap <=200). It takes a fixed source-row frame from January and
February 2024, preserves at most 2,000 opening games per month, and extends the
horizon to twelve plies after the same root. See the tracked pre-analysis plan,
`docs/human_repertoire/cohort_config.json`, and the milestone-2 report.

After the resource check above, run with the same `$researchPython`:

```powershell
& $researchPython -B -m unittest research.human_repertoire.test_cohort -v
& $researchPython -B -m research.human_repertoire.cohort
& $researchPython -B -m unittest research.human_repertoire.test_model research.human_repertoire.test_real research.human_repertoire.test_cohort research.human_repertoire.test_empirical -v
& $researchPython -B -m research.human_repertoire.validate_cohort
```

`cohort` is the only new command reading external data. It processes exactly
262,144 rows from row group 0 of each of six declared files, caps projected
row-group size at 512 MiB, uses one thread and bounded batches, and performs sparse
read-only engine lookups. It samples at most 2,000 matching records per stratum by
fixed game-ID hash rank, then selects at most 2,000 across the three strata by the
same rank. Selection precedes outcome/eval analysis. This convenience frame is not
a population-random monthly sample. Identities and full snapshot hashes guard reuse;
changing source/config/extractor versions requires a separately preserved snapshot.

`validate_cohort` reads only frozen local files. It chooses candidate books on
January, checks February move/outcome predictions, compares frozen books on common
cohort models, reports BRC strata separately, and tests session-correlated recall.
The driver caps candidate enumeration at 1,024 books (40 used), core evaluation at
200,000 states per book, and checks a 240-second deadline before uncached book
evaluations. Diagnostics and final output also take time after those evaluations;
the deadline is not a whole-process hard timeout. No scalable-optimiser claim is made.

Additional outputs are all in `_artifacts/m2/`:

| File | Contents |
|---|---|
| `cohort_manifest.json` | Frozen frame, source fingerprints, sampling/rejection audit, schemas, engine identity, player overlap |
| `train_games.json`, `validation_games.json` | Selected game metadata, replayed opening moves, hashes and FENs |
| `evals.json` | Frozen bounded engine lookup results, with missing values explicit |
| `train_graph.json`, `validation_graph.json` | Models shared by all plans in each cohort, study candidates and support audit |
| `results.json` | Resolved configs, fixed books, observations, validation diagnostics, first-hit bounds, source/code hashes |
| `decision_diagnostics.json`, `outcome_diagnostics.json` | Per-game audit quantities for the reported predictive checks |
| `test_results.txt` | Recorded 31-test verification output |

The pre-extraction system check is `_artifacts/resource_check_m2.json`. Original
milestone-1 snapshots and results remain separate and unchanged.

## Milestone 3: shared fallback prediction across dates

The [fixed plan](../../docs/human_repertoire/milestone3_plan.md) and
[config](../../docs/human_repertoire/milestone3_config.json) define three spread
frames per event/month, at most 2,250 games each for January fitting, February
tuning and March testing. The population and twelve-ply opening window match
milestone 2. Exact frames and footer fingerprints are tracked separately.

After checking memory headroom and active workloads, use the isolated interpreter:

```powershell
& $researchPython -B -m unittest research.human_repertoire.test_model research.human_repertoire.test_real research.human_repertoire.test_cohort research.human_repertoire.test_empirical research.human_repertoire.test_shared_fallback -v
& $researchPython -B -m research.human_repertoire.date_cohort --discover
& $researchPython -B -m research.human_repertoire.date_cohort --extract
& $researchPython -B -m research.human_repertoire.shared_fallback --tune
& $researchPython -B -m research.human_repertoire.shared_fallback --test
& $researchPython -B -m research.human_repertoire.verify_shared_fallback
```

Discovery reads Parquet footers only. Extraction caps decoded rows at four million
plus batch rounding, projected row-group bytes at 512 MiB and selected games at
250 per 65,536-row frame. No engine lookup or process is needed for this check.
The 360-second extraction guard is checked before each row-group read; prediction
checks its 360-second guard before each game. These are not hard process timeouts.
Scoring permits at most 30,000 decisions and 50,000 cached positions per split.

Tuning reads January/February only and freezes the selected settings and January
counts. Final scoring verifies the frozen identity before opening March games.
All model families are evaluated on the same recorded decisions; final results
are predictive checks, not counterfactual outcomes or repertoire recommendations.
Reuse verifies identity and skips completed stages. Preserve the old artifact
directory before an explicitly revised experiment; do not bypass a drift error.

Additional outputs under `_artifacts/m3/`:

| File | Contents |
|---|---|
| `frames.json` | Frozen row windows, footer dates, source inventory and config |
| `cohort_manifest.json` | Source/code identities, frame audits, snapshot hashes and player overlap |
| `{train,tune,test}_games.json` | Replayed bounded game snapshots |
| `fitted_counts.json` | January exact-position/UCI and shared-token counts |
| `tuning_scores.json` | Every fixed grid setting's February diagnostics |
| `frozen_model.json` | Selected parameters and fitted/tuning/plan/code hashes |
| `test_results.json` | All final comparisons, paired differences and component bootstrap |
| `test_game_scores.json` | Paired per-game sufficient statistics for audit |
| `verification.json` | Snapshot/selection checks, score reconstruction and parity with the unchanged milestone-2 diagnostic |
| `preflight_tests.txt` | Recorded 40-test verification before real extraction |

The system check is `_artifacts/resource_check_m3.json`. Code remains separate
from the existing evaluator/optimiser and earlier frozen experiments.
See the [completed report](../../docs/human_repertoire/milestone3_report.md) for
the final predictive pass and the remaining graph/engine-coverage limitations.

## Milestone 4: common graph and bounded engine audit

The fixed protocol is `docs/human_repertoire/milestone4_plan.md`, with resource
caps and search limits in `milestone4_config.json`. It uses only frozen January
training games/counts and the February-selected priors. No new game extraction
or model fitting occurs. The existing source projection and two-card plan are
compared at recall .85; this is not a new optimisation run.

After checking commit headroom and active workloads, run:

```powershell
& $researchPython -B -m unittest research.human_repertoire.test_model research.human_repertoire.test_real research.human_repertoire.test_cohort research.human_repertoire.test_empirical research.human_repertoire.test_shared_fallback research.human_repertoire.test_common_graph -v
& $researchPython -B -m research.human_repertoire.continuation_audit --prepare
& $researchPython -B -m research.human_repertoire.continuation_audit --engine
& $researchPython -B -m research.human_repertoire.continuation_audit --compare
& $researchPython -B -m research.human_repertoire.verify_continuation
```

Preparation creates the common observed graph and a bounded extension of 64
missing edges, with two further plies at unsupported children. Both policies use
identical explicit branches and keep omitted probability in aggregate frontiers.
It checks parity with the unchanged previous builder, verifies the external eval
arrays with `adopt=False`, and freezes sparse lookups, engine identity and targets.
No external database, repertoire or cache is written.

The engine phase uses the already installed Stockfish binary with one thread and
32 MiB hash. Initial searches cover at most 256 positions at 100,000 nodes/1 second;
32 fixed targets get 400,000 nodes/2 seconds. Both limits apply; actual work and
depth are recorded. Hash and game state are reset for each position. A 420-second
guard applies before requests, with a separate 10-second protocol timeout.
The last completed non-bound score is retained within each cap, with interrupted
final iterations and total work recorded separately. Partial progress is saved
every 16 positions and can resume only with matching
identities. Only a complete audit can enter the comparison.

Comparison reads isolated snapshots and uses the existing exact evaluator for
all fixed plans, policies, graph variants and evaluation maps. Source-only,
mixed-source overlay and homogeneous audited-only evidence are labelled
separately. Uncertified prepared moves are reported without relaxing gates.
The 360-second guard is checked between evaluations; each evaluation retains its
200,000-history-state cap. The independent path oracle checks real predictions
at a bounded six-ply horizon.

Outputs under `_artifacts/m4/`:

| File | Contents |
|---|---|
| `preparation.json` | Code/config/source identities, parity check, graph counts, books and engine identity |
| `templates.json`, `graphs.json` | Shared legal topology and both frozen probability kernels |
| `source_evals.json` | Sparse frozen canonical lookups with explicit missing values |
| `engine_selection.json` | Fixed target/deeper lists, mandatory gates and reach priorities |
| `engine_audit.json` | Resumable isolated UCI results, actual limits/work and provenance |
| `results.json` | Fixed-plan model predictions, observation bounds, gates and score disagreements |
| `preflight_tests.txt` | Recorded 49-test result before real preparation |
| `final_tests.txt` | Recorded 50-test result including first-gap accounting |
| `verification.json` | First-gap partitions, gate stability, engine work and frozen-hash checks |

The resource check is `_artifacts/resource_check_m4.json`. Do not bypass drift
checks, increase frozen work caps silently, or interpret model predictions as
observed benefits from studying a book.

The [completed report](../../docs/human_repertoire/milestone4_report.md) identifies
aggregate omitted moves as the dominant observation gap. The verifier also
checks identical snapshot hashes against the preserved initial preparation in
`_artifacts/m4_before_score_stream_fix/`; retain that small experiment snapshot
when moving the research artifacts. The intermediate UCI trace and preparation
are in `_artifacts/m4_before_completed_iteration_fix/`. These do not contain a
completed alternative experiment or altered graph/target selection.

## Milestone 5: full legal trajectories with Monte Carlo error

The frozen [protocol](../../docs/human_repertoire/milestone5_plan.md) and
`milestone5_config.json` retain the two policies, three fixed books and existing
evaluator assumptions. No fitting, new game extraction or optimisation occurs.
The sampler generates legal children on demand without an omitted-move frontier.
It uses the same random draws across plans, with distinct pilot/control/final
streams. Source scores do not influence trajectory generation.

After checking system headroom and active workloads, run in this worktree:

```powershell
& $researchPython -B -m unittest discover -s research/human_repertoire -t . -p 'test_*.py'
& $researchPython -B -m research.human_repertoire.rollout_experiment --pilot
& $researchPython -B -m research.human_repertoire.rollout_experiment --engine
& $researchPython -B -m research.human_repertoire.rollout_experiment --final
& $researchPython -B -m research.human_repertoire.verify_rollout
```

The pilot uses 256 trajectories per arm and selects at most 512 additional
missing-evaluation targets, with 32 deeper checks. The installed Stockfish binary
must match milestone 4 before its audited scores can be reused. Search uses one
thread and 32 MiB hash, 100,000 nodes/1 second initially and 400,000 nodes/2 seconds
for the deeper subset. The engine phase has a 600-second guard between requests
and the existing per-request timeout. Pilot targets and completed engine scores
freeze before the independent 1,024-trajectory-per-arm final sample.

Sampling phases have a 300-second guard. Distinct-position caps are 20,000 for
the pilot and 80,000 for the final sample, above their maximum possible arrival
counts. The final phase first checks 4,096 control trajectories per arm against
the unchanged milestone-4 exact graph metrics. Source lookups remain sparse and
read-only, with source identity checks before/after and across sampling phases.

Missing-evaluation intervals and Monte Carlo error are separate. The primary
paired contrast is two cards minus one under the shared prior and mixed-source
audited overlay. Identical position sequences have exactly zero first-hit
difference despite missing scores. The reported Hoeffding envelopes condition
on the frozen model and evidence maps, not human-performance or engine accuracy.

Outputs under `_artifacts/m5/`:

| File | Contents |
|------|----------|
| `pilot_manifest.json` | Frozen protocol/code/input hashes, source identity, gates and engine identity |
| `{pilot,final}_traces.json` | All legal moves, position sequences, stop reasons and endpoint W/D/L |
| `{pilot,final}_fens.json`, `{pilot,final}_source_evals.json` | Visited positions and sparse source scores |
| `engine_selection.json`, `engine_audit.json` | Pilot-only target ranking and completed bounded searches |
| `control.json` | Every sampled metric versus the prior exact result |
| `results.json` | All 18 model/evidence/plan summaries, Monte Carlo errors and paired contrasts |
| `verification.json` | Complete legal replay, independent per-path score reconstruction and engine work |

The resource check is `_artifacts/resource_check_m5.json`. Reuse validates frozen
hashes and skips completed work; do not bypass drift checks. See the
[completed report](../../docs/human_repertoire/milestone5_report.md) for results
and the next bounded priority.

## Milestone 6: complete paired-panel engine coverage

The frozen [protocol](../../docs/human_repertoire/milestone6_plan.md) and config
use 64 coupled one-card/two-card paths under the shared prior, with a fixed
16-pair subset for deeper-budget sensitivity. The existing legal sampler,
recall, horizon, endpoints and outcome assumptions remain unchanged.

After checking headroom and active workloads, use the isolated interpreter:

```powershell
& $researchPython -B -m unittest discover -s research/human_repertoire -t . -p 'test_*.py'
& $researchPython -B -m research.human_repertoire.paired_panel --prepare
& $researchPython -B -m research.human_repertoire.paired_panel --engine
& $researchPython -B -m research.human_repertoire.paired_panel --compare
& $researchPython -B -m research.human_repertoire.verify_panel
```

Preparation freezes every path and both complete-position target lists before
new scores. Matching cached engine records are copied in bounded form; base
100,000-node/1-second records and deeper 400,000-node/2-second records remain
separate. The installed binary must match M4/M5. Canonical scores only enter
labelled diagnostics and never select targets. No source artifact is written.

At most 1,664 positions can be visited; four additional mandatory gate positions
give an absolute base cap of 1,668. The deeper subset/gate cap is 420. Every
visited nonterminal position is covered, including identical paths and arrivals
after a first hit. Sampling has a 60-second guard. Engine work uses one thread,
32 MiB hash and a 900-second session guard between requests, plus the existing
per-request timeout. Partial progress is saved every 16 searches. A stopped
audit keeps all paths; `--compare` writes explicit `partial_results.json` until
the audit is complete. Resume only with matching identities.

The final comparison uses a homogeneous base-budget map for all 64 pairs and
two complete budget maps on the same fixed 16-pair subset. A predeclared budget
curve shows missing-information intervals as ordered targets are added. The
independent verifier checks legal replay, complete position sets, cache budgets,
engine principal variations, source identities, all metrics and the curve.

Outputs under `_artifacts/m6/`:

| File | Contents |
|------|----------|
| `panel_manifest.json` | Frozen code/config/input identities, gates, search options and target counts |
| `panel_traces.json`, `panel_fens.json` | Every paired trajectory and visited FEN |
| `engine_fens.json`, `engine_selection.json` | Full base/deeper requirements, reused/new targets and gate positions |
| `source_evals.json`, `inherited_overlay.json` | Labelled pre-audit evidence on the fixed panel |
| `inherited_records.json`, `engine_audit.json` | Matching cached and new base/deeper engine records |
| `results.json` | Full and subset comparisons, budget curve, gates and Monte Carlo uncertainty |
| `verification.json` | Independent reconstruction, complete coverage, actual engine work and depth sensitivity |

The resource check is `_artifacts/resource_check_m6.json`. The
[completed report](../../docs/human_repertoire/milestone6_report.md) separates
resolved coverage from the remaining simulation and human-model limitations.

## Milestone 7: human/model engine-loss calibration

The frozen [protocol](../../docs/human_repertoire/milestone7_plan.md) and config
select 32 actual March decisions per time-control/support stratum, 192 total.
One case per game/support pair is selected with independent fixed hashes for
the within-game decision and game selection. One frozen-prior move is sampled
at each actual parent board. No March tuning, new extraction or book optimisation
occurs. This is retrospective use of the existing final cohort.

After checking headroom and active workloads, use the isolated interpreter:

```powershell
& $researchPython -B -m unittest discover -s research/human_repertoire -t . -p 'test_*.py'
& $researchPython -B -m research.human_repertoire.calibration_experiment --prepare
& $researchPython -B -m research.human_repertoire.calibration_experiment --engine
& $researchPython -B -m research.human_repertoire.calibration_experiment --compare
& $researchPython -B -m research.human_repertoire.verify_calibration
```

All cases/draws/targets freeze before scores. Parent and human/model children
receive a homogeneous 100,000-node/1-second audit, at most 576 positions. The
first four cases per stratum form a fixed 24-case deeper subset, at most 72
positions at 400,000 nodes/2 seconds. Matching M4-M6 records are reused by budget.
The binary and one-thread/32 MiB options must match previous audits. There is no
canonical eval lookup. Preparation caps 2,250 games / 30,000 decisions / 60 seconds;
engine work has a 600-second session guard and existing per-request timeouts.
Partial scores are saved every 16 requests. No complete comparison is claimed
until every case has coverage; do not drop cases or bypass identity checks.

Loss uses the mover's perspective, clips apparent improvements at zero, and
reports >=150/300-cp markers plus a mean capped at 1,000 cp. Balanced stratum
means and paired connected-player bootstrap intervals retain cross-stratum
dependence. Empty-stratum bootstrap draws make the affected interval unavailable.
Intervals remain approximate diagnostic summaries, not population guarantees.

Outputs under `_artifacts/m7/`:

| File | Contents |
|------|----------|
| `case_manifest.json` | Frozen identities, counts, engine options and snapshot hashes |
| `cases.json`, `selection_frame.json` | Selected human/model decisions and complete eligible-frame counts |
| `fens.json`, `engine_selection.json` | Parent/child FENs and frozen base/deeper target lists |
| `inherited_records.json`, `engine_audit.json` | Matching cached and new search records |
| `scored_cases.json`, `results.json` | Per-case losses, balanced summaries, resampling and subset sensitivity |
| `verification.json` | Original-game replay, regenerated draws, independent losses, PVs and hash checks |

Resource check: `_artifacts/resource_check_m7.json`. The
[completed report](../../docs/human_repertoire/milestone7_report.md) identifies
unsupported-position tactical calibration as a limitation of the current prior.

## Milestone 8: board-aware benchmark

The [frozen plan](../../docs/human_repertoire/milestone8_plan.md) adds ten geometric
board features to the existing legal token prior. Counts and alpha remain common.
January fits coefficients; February selects correction strength; April is opened
only after model freezing. Existing M3/M7 modules are reused in separate module
instances with outputs mapped to M8; their normal globals and files are unchanged.

Run these phases in order with the isolated venv and one BLAS thread:

```powershell
$env:OPENBLAS_NUM_THREADS='1'
$env:OMP_NUM_THREADS='1'
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.board_benchmark develop
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.board_benchmark holdout
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.board_benchmark test
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.board_benchmark prepare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.board_benchmark engine
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.board_benchmark compare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.verify_board_benchmark
```

Use a fresh shell for thread limits so other processes are unaffected. Do not
change frozen code/config or bypass hash checks to reuse outputs for a revision.
Development artifacts include coefficient/feature identity, training arrays,
sample IDs and every beta's scores. `cohort/` holds April frame metadata and
games. `test_results.json` and `test_game_scores.json` hold likelihood evidence.
`calibration/` holds the 96 paired cases, complete base/deeper engine records,
summaries and scalar scores. In candidate-versus-reference summaries only,
the reused `human` field means reference draw and `model` means candidate draw.
`verification.json` records weight reproduction, replay and independent checks.

The [completed report](../../docs/human_repertoire/milestone8_report.md) finds
better likelihood but an unresolved tactical mismatch at unseen positions.
Resource checks are `_artifacts/resource_check_m8*.json`. Next work should audit
residual failures before choosing a stronger candidate; April cannot be called
fresh validation for a revision informed by these results.

## Milestone 9: retrospective residual audit

```powershell
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.residual_audit
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.verify_residual_audit
```

Uses only frozen M8 cases and engine records. Outputs under `_artifacts/m9/`
are `residual_cases.json`, `results.json`, `verification.json` and
`severe_cases.pgn` (all 22 severe draws, actual-human alternatives and engine
continuations). No new engine requests or canonical scans occur. The six-ply
legal same-square capture game permits optional stopping even in check; it is
a material feature, not a forced tactical evaluator. Nodes cap at 20,000 per
child and exhaustion fails visibly without dropping cases.

See the [report](../../docs/human_repertoire/milestone9_report.md) for observed
blind spots, false-positive signals and the specified next candidate. The
next step is a January-only cost pilot and a prospective protocol; this audit
does not fit a replacement or validate one on fresh data.

## Milestone 10: cost pilot and prospective protocol

```powershell
$env:OPENBLAS_NUM_THREADS='1'
$env:OMP_NUM_THREADS='1'
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.exchange_prior
```

This command runs only the fixed 1,000-alternative January pilot and writes
`_artifacts/m10/pilot.json` and `pilot_rows.json`. It does not fit coefficients
or open May. `exchange_prior.features` returns thirteen features and actual
exchange-node count. The pilot records Windows process memory and preserves
failed-run metadata if a declared bound is hit.

The [status](../../docs/human_repertoire/milestone10_status.md) records the passed
pilot and 92-test suite. The [prospective plan](../../docs/human_repertoire/milestone10_plan.md)
defines the full benchmark, which still requires a new tested runner. Do not
invoke the frozen ten-weight M8 fitter with thirteen-dimensional arrays.

## Maia-2 fallback integration

`maia_policy.MaiaPolicy` provides full legal UCI probabilities with self rating
1400 and caller-supplied opponent rating. `MaiaFallbackKernels` matches the
existing evaluator's `get(key, fen)` interface, uses Maia only on our turns and
delegates opponent turns to the existing supplied policy. Select `maia1400` as
the evaluator model name. No frozen evaluator module was edited.

Use `_artifacts/maia2/venv/Scripts/python.exe`, not the original research venv,
for `python -B -m research.human_repertoire.maia_smoke`. The latter verifies both
real checkpoints and writes full probabilities for the 96 frozen diagnostic
parents. The [integration report](../../docs/human_repertoire/maia_integration.md)
documents setup, semantics, provenance and transfer limits. No training or
May data extraction is required. Ordinary contract tests remain runnable in
the original research environment without installing torch or maia2 there.

## Maia study-card comparison

```powershell
$env:OPENBLAS_NUM_THREADS='1'
$env:OMP_NUM_THREADS='1'
& research/human_repertoire/_artifacts/maia2/venv/Scripts/python.exe -B -m research.human_repertoire.maia_comparison prepare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.maia_comparison engine
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.maia_comparison compare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.verify_maia_comparison
```

The Maia environment also needs `pyarrow==24.0.0` for existing adapters.
Preparation verifies the frozen identity on reuse; do not overwrite the Maia
integration verification snapshot it fingerprints. Outputs are exclusively
under `_artifacts/maia_compare/`. The [report](../../docs/human_repertoire/maia_comparison_report.md)
records the completed 576-path comparison, bounded engine work, all nine
contexts and unresolved book ranking. The independent verifier uses frozen
probabilities and needs neither checkpoint loading nor fresh engine calls.

## Equal-budget Maia packages

```powershell
& research/human_repertoire/_artifacts/maia2/venv/Scripts/python.exe -B -m research.human_repertoire.maia_equal prepare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.maia_equal engine
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.maia_equal compare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.verify_maia_equal
```

The frozen [plan](../../docs/human_repertoire/maia_equal_plan.md) compares three
two-card packages on 384 fresh paths, with the existing opponent policy and
Maia fallback. All outputs are under `_artifacts/maia_equal/`; inherited engine
records are fingerprinted and read-only. Four primary contrasts use Bonferroni
Hoeffding bounds. Ineligible packages remain reported rather than being replaced.

## Branch-stratified follow-up comparison

```powershell
& research/human_repertoire/_artifacts/maia2/venv/Scripts/python.exe -B -m research.human_repertoire.maia_stratified prepare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.maia_stratified engine
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.maia_stratified compare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.verify_maia_stratified
```

Read the [method](../../docs/human_repertoire/stratified_sampling.md) and
[prospective plan](../../docs/human_repertoire/maia_stratified_plan.md).
The resolved config inherits the equal-budget config but uses 32 replicates
per stratum, four deeper replicates, a 2,000-prefix cap and a new seed; it is
embedded in the frozen manifest. Outputs stay under `_artifacts/maia_stratified/`.
No frozen earlier runner is modified. New Maia chess backends should initialise
with `HashedBoard` so pinned en-passant state survives FEN serialization and
retains its Polyglot key. This changes serialization, not legal move support.

## Empirical opponents and exposure recall

The [report](../../docs/human_repertoire/exposure_report.md) records the completed
exact comparison. All outputs stay under `_artifacts/exposure/`. Preparation
uses existing read-only source tables and eval arrays, with bounded streaming
and Maia inference; check shared workload headroom before preparing new inputs.
Frozen snapshots can be compared and verified without loading Maia or engines.

```powershell
& research/human_repertoire/_artifacts/maia2/venv/Scripts/python.exe -B -m research.human_repertoire.exposure_real prepare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.exposure_real compare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.verify_exposure
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m unittest discover -s research/human_repertoire -t . -p 'test_*.py'
```

## Familiar-tabiya recall comparison

The [contextual recall report](../../docs/human_repertoire/context_recall_report.md)
adds the user's familiar opening context and depth decay. Reuses frozen empirical
snapshots without data scanning, Maia loading or engines. New outputs stay under
`_artifacts/context_recall/`; the original exposure experiment is preserved.

```powershell
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.context_comparison compare
& research/human_repertoire/_artifacts/venv/Scripts/python.exe -B -m research.human_repertoire.verify_context
```
