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
