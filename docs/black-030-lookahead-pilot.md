# Black 0.30 pilot after the horizon and lookahead repairs

Use the `lookahead-floor` worktree. The historical control is the floor-five own-move book at `D:\chess\rare-verify-v1\black-cp30-o5\round-03\book_b2000.parquet`. Its relative gate is 30 centipawns, its absolute tolerance is 0.10 expected score, its pool is 2000+ Blitz/Rapid/Classical, and its last Black decision can be at ply 29. The 0.095 relative score gate and Black's 0.075 absolute tolerance belong to the later lifetime experiment.

## What changed

- The collector expands improved arrivals at the layer where they actually arrive. The graph also reopens an already-reached board on a shorter qualifying route. The maximum route probability, earliest qualifying ply, and ply of the highest-probability route answer three different questions. In the original five-ply reproduction, X has reach 1, earliest ply 3 and most likely ply 5. Its forced reply reaches Y at ply 4 with probability 0.2.
- Collection follows a replayed source move far enough to fetch available replies. The original counts stay on the observed moves. Both pilot arms use the same source-child evaluation lookup, with verified scores taking priority. Final native assembly admits the recorded, proposed and restored moves under the original gates. Lookahead filters that menu after admission. It uses the assembled opponent probabilities, including a configured prior. An alternate move order can expose more analysis of a shared position. Each forcing sequence still has to fit within the ply allowance of the decision using it.
- A dead decision has either an empty admitted menu or a failure on every admitted action. Unknown scores can keep an action unresolved. A veto needs a known passing alternative that survives propagation. A void leaves the choice to the existing builder. The report counts empty menus and unresolved menus separately. The post-build policy audit follows the moves actually prescribed in the budget book and identifies reachable below-floor prescriptions and incomplete continuations.
- Every lookahead round stores its final book, metadata, rare-dispute list, pre-veto menu/reply evidence, veto JSON and independent SQL audit in the build receipt. The validator checks the receipted digests and recomputes the ranks and vetoes. A copied base/extended book needs its original evidence beside the base file. Old lookahead results can be read historically but lack this evidence.

## Preflight and run

On 14 November the historical template receipt, floor-five lookup recipe, applied Stockfish overlay, eval manifest and control book matched the handoff. The source, stats, aux and weak pool paths existed. The new native extension built locally has SHA-256 `65f50f749d20b9d46201e13b7f2ede8576929582d7b80bb0dbea5ea3f6c6b9c0`. Git tracks its Rust sources. The coordinator snapshots and hashes the extension bytes it actually loads.

| Input | SHA-256 |
| --- | --- |
| Historical template, stages/black-cp30.json | `30e75edd38dcd9f2069a16dfe4a361b7635a6df6ef9a426803282e4c3ddb15ba` |
| cache-own5 recipe | `9c0dc90870e642911cf6cc1fa0e788e20a23427c5183f435469046bf62516903` |
| Round 02 overlay used to build control round 03 | `88b0451147e9170226053f5821b109213205e237c25e87c65e5c422b5e65e4b5` |
| D:\chess\eval_arrays_full\eval_arrays.meta.json | `3adca850a165dc24394588f935a40f8e4fde5d094439cc02d2ca0bc1d896431a` |
| Historical control book | `c7ab6e2dcfd6d0baaea80a9926feaa1af25d0ca75d7db0eab4b89e03432c554f` |

Run on a quiet machine from PowerShell. The first arm measures the combined horizon and source-handling changes against the frozen control. The second arm isolates the incremental effect of lookahead. Both start from the same verified overlay, use the same old eval generation, and share the Black Stockfish search cache. Keep the historical opponent model in both arms. Test a compatible frozen opponent prior separately if evaluating the finalist recipe.

```powershell
$repo = 'C:\Users\David\Documents\Chess'
$work = Join-Path $repo 'scratch\worktrees\lookahead-floor'
$py = Join-Path $repo '.venv\Scripts\python.exe'
$runner = Join-Path $work 'python\run_rare_book.py'
$runs = 'D:\chess\rare-verify-v1'
$pilot = Join-Path $runs 'black-030-horizon-v2'
$common = @(
    '--colour', 'black', '--cap', '30',
    '--baseline', (Join-Path $repo 'scratch\worktrees\practical-repertoire\outputs\rebuild-2026-11-06\cp-screen-030-060-100'),
    '--lookup', 'D:\chess\uncut-own-v1\black\cache-own5',
    '--searches', (Join-Path $runs 'black\search-cache'),
    '--seed-overlay', (Join-Path $runs 'black-cp30-o5\round-02\overlay.parquet'),
    '--max-rounds', '20', '--build-arg=--rs-core'
)
& $py -X utf8 -u $runner @common --out (Join-Path $pilot 'off')
if ($LASTEXITCODE -ne 0) { throw 'Black control failed' }
& $py -X utf8 -u $runner @common --out (Join-Path $pilot 'on') --build-arg=--lookahead-floor=99
if ($LASTEXITCODE -ne 0) { throw 'Black lookahead failed' }
```

Rerunning the same command validates and reuses intact rounds. Keep the historical template, lookup, eval arrays and cache in place during the comparison. Each arm is closed by its own `final.json` and the referenced receipts.

## Before expanding to the other books

1. Confirm 2,000 distinct legal Black prescriptions in each final book, the final applied overlay digest, closure of the booked verification requests, and the high-reach unbooked disputes left by the configured frontier limit.
2. Compare the archived control to the corrected off arm, then corrected off to on. Report changed prescriptions, entries and exits, reach of the changes, subgraph size, root value, build and verification time, peak commit and disk cost. The first comparison includes both horizon and source corrections.
3. In the on arm inspect `book_b2000.parquet.lookahead-audit.json`. Expect 2,000 decisions checked. A missing, ambiguous or vetoed prescription, a rank disagreement, or a veto-list mismatch fails the round. Review the witnesses, empty menus, voids and incomplete continuations, prioritizing the highest-reach examples. The reported reach is the best single route in the **pre-veto graph**. Summed reach across decisions is an exposure indicator and can exceed one.
4. Run the independent legal/gate and Stockfish best-defence checks. Use the frozen code snapshot for the relevant book. One example for the on arm follows. The restricted best-defence walk adds a fresh engine reply to the pool replies and evaluates the worst reachable leaf. Replay high-reach witnesses and examine any consequential floor breaches before a promotion decision.
5. Compare closed books using the pinned referee under the same opponents, time controls and paired seeds. The old lookahead-chain results use a different implementation. Production runtime, memory, number of changed decisions and strength remain empirical pilot outcomes.

```powershell
$closed = Get-Content -Raw (Join-Path $pilot 'on\final.json') | ConvertFrom-Json
$validator = Join-Path $closed.code_snapshot 'validate_book.py'
& $py -X utf8 $validator --book $closed.book --colour black --nodes 5000000 --best-defence --defence-nodes 1000000 --out (Join-Path $pilot 'on\validation')
if ($LASTEXITCODE -ne 0) { throw 'Black validation failed' }
```

The focused regression suite is `python\_test_horizon_lookahead.py`. It runs through native graph assembly, Python and Rust collection, budget selection, evidence publication, independent audit and coordinator resume on small fixtures. The Rust/Python differential suite is `python\_test_book_builder_rs.py`.
