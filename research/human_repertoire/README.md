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
