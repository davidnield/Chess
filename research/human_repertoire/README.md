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
