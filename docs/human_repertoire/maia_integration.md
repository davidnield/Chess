# Maia-2 integration: user-selected 1400 fallback

Completed 2026-09-08. Maia-2 is installed in a separate artifact-local environment,
both released checkpoints are downloaded and hash-verified, and the legal move
policy adapter works with the research evaluator's kernel interface. No model
training, engine search, canonical scan or May extraction was needed.

## User preference and semantics

The user selected **1400** despite a higher actual rating, to favor fallback
moves that are easy or intuitive rather than requiring calculation or familiarity.
This is a deliberate modeling preference, not an estimate of the user's strength.
Maia's 1400 input maps to its 1400-1499 category. Lower-rated move prediction is
a proxy for this preference, not a measured intuitiveness/effort score or proof
of conservative tactical play. It can include lower-rated mistakes.

Use 1400 only for **our unprepared/failure-to-recall policy**. The caller supplies
the actual opponent rating to Maia's separate opponent-conditioning input.
`MaiaFallbackKernels` delegates opponent turns to an explicitly supplied existing
opponent kernel; it does not downgrade the opponent to 1400. Our recalled book
move and the probability of recalling it remain the evaluator's responsibility.
The original empirical cohort remains both ratings >=1800, gap <=200, across
Blitz/Rapid/Classical. Its population was not changed by this preference.

## Implementation

- `maia_policy.py`: lazy heavy imports, pinned versions and checkpoint validation,
  full legal probabilities, stable sorted-SAN sampling, and evaluator adapter.
- `maia_smoke.py`: actual-checkpoint verification and bounded diagnostic export.
- `test_maia_policy.py`: lightweight contract tests in the original research venv.
- [Configuration](maia_config.json): Maia-2 0.11.0, PyTorch 2.8.0+cpu, one CPU
  thread, self rating 1400, temperature 1 and full support (no top-p truncation).

Blitz uses the Blitz checkpoint; Rapid uses Rapid. Classical uses Rapid as an
explicit transfer assumption. No claim of dedicated Classical training is made.
No engine filter, top-move substitution, retraining or softmax-strength tuning
is added. Maia's value head is not used for Stockfish soundness or book utility.

The upstream public inference display rounds probabilities to four decimals.
The adapter uses its preprocessing/model output and computes a float64 masked
softmax over legal actions, preserving small probabilities. It checks exact
legal support, positivity, finite values and unit mass. Black positions use
upstream mirroring and are mapped back to actual UCI moves. Invalid boards fail;
automatic terminal boards return an empty policy without loading a model.

The implementation follows the inspected official
[inference code](https://github.com/CSSLab/maia2/blob/main/maia2/inference.py) and
[checkpoint loader](https://github.com/CSSLab/maia2/blob/main/maia2/model.py).
The loader uses tensor-only loading and the upstream published SHA-256 values.
Installed source-file hashes, checkpoint hashes and every dependency version
are recorded in [verification/provenance](maia_integration_verification.json).
Each checkpoint is 279,704,570 bytes and each model has 23,297,486 parameters.

## Verification and measured cost

All **98 research tests pass**. New contracts test supported time controls,
rejection of incomplete/invalid probability mass, automatic terminal behavior,
non-argmax sampling, opponent-policy preservation and position/cache guards.

Actual checkpoint tests make **112 probability checks** against upstream's
rounded display and mirrored boards. The fixtures include promotion, castling
and en passant for both colors. Three terminal fixtures cover mate, stalemate
and insufficient material. The 1400-1499 bucket mapping is checked explicitly.

The smoke run then computes complete move distributions on all 96 frozen M8
April diagnostic parents, 32 per event, at self=1400 and the actual opponent's
rating. This phase takes **3.77 seconds**; model loading, fixtures and diagnostic
work together take **8.84 seconds**, excluding package/checkpoint downloads.
The existing independent Chess.com crawl remained running and untouched.

| Diagnostic group | Cases | Mean log loss against recorded >=1800 moves |
|---|---:|---:|
| Blitz | 32 | 1.532788 |
| Rapid | 32 | 1.440795 |
| Classical, Rapid checkpoint transfer | 32 | 1.671717 |

These are retrospective integration diagnostics on the balanced M8 case sample,
not the 900-game likelihood cohort. They are not a new held-out performance
claim or a fit of 1400 Maia to >=1800 behavior. They must not be compared directly
to the earlier 900-game means. The rating preference is not to be optimized away
merely to improve likelihood on stronger players. No tactical-loss rate or study
benefit has yet been estimated for Maia in this project.

## Running and using it

Run from the research worktree using the separate Maia interpreter:

```powershell
$env:OPENBLAS_NUM_THREADS='1'
$env:OMP_NUM_THREADS='1'
& research/human_repertoire/_artifacts/maia2/venv/Scripts/python.exe -B -m research.human_repertoire.maia_smoke
```

Direct use (run under that interpreter):

```python
import chess
from research.human_repertoire.maia_policy import MaiaPolicy, MaiaFallbackKernels

policy = MaiaPolicy()
probabilities = policy.probabilities(chess.Board(), 'Rapid', opponent_rating=2000)
move = policy.sample(chess.Board(), 'Rapid', opponent_rating=2000, u=0.42)
# Self rating defaults to the configured 1400. Opponent rating here is an example.

# For an existing ChessBackend with its original opponent kernel:
# backend.kernels = MaiaFallbackKernels(
#     policy, 'Rapid', chess.WHITE, 2000, backend.kernels, 'shared_position')
# Evaluate using the policy name 'maia1400'. Existing recall/book logic is reused.
```

The Maia environment contains inference dependencies only; the original research
venv and its frozen package versions were not changed. A future full trajectory
runner may require additional research imports in the Maia environment; do not
import the original checkout's mutable modules or broaden dependencies silently.
The actual-checkpoint smoke does not claim a completed trajectory/book comparison.

All large/local outputs are ignored under `_artifacts/maia2/`: `venv/`,
`uv-cache/`, `models/`, `requirements-lock.txt`, `diagnostic_probabilities.json`
and `integration_verification.json`. Resource evidence is
`_artifacts/resource_check_maia.json`. The two checkpoints were downloaded using
official asset URLs with gdown cookies disabled, then verified against the
published hashes before loading. Normal adapter use requires those verified
local files and does not silently download missing weights.

## Next step tied to the project goal

Use this explicit 1400 fallback in a bounded repertoire comparison with the
existing opponent model, Stockfish soundness, study budget and recall scenarios.
Include enough paired tactical checks to expose gross adapter/model mismatch,
but do not start another open-ended move-predictor development project. Report
whether the study recommendation changes and whether that conclusion survives
fallback/recall sensitivity. This integration supplies the policy component;
it does not establish measured ease of recall or an actual human benefit.
