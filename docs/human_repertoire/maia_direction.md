# Direction checkpoint: use a pretrained human policy

2026-09-08. Following the user's suggestion to use Maia, pause the custom
thirteen-feature benchmark. M10's cost pilot remains a valid completed result;
no new candidate was fitted and no May rows were opened. The unfinished,
untested runner draft is preserved under ignored
`_artifacts/m10/paused_exchange_benchmark.py`; it was not run or committed as a
supported implementation. The earlier prospective M10 plan is historical and
must not silently become the protocol for a different model.

## Why this changes the priority

The project aims to choose sound opening preparation that produces early
advantage under limited study and imperfect recall. A competitive human-move
predictor is a component, not the project's intended contribution. Our small
models established interpretable controls and exposed a serious extrapolation
problem. Continuing feature engineering without an established external policy
baseline risks spending effort on the wrong subproblem. Maia should have been
considered earlier.

Keep the existing evaluators, oracle tests, calibration and provenance work.
Replace the next custom-model experiment with a bounded pretrained-Maia
integration and calibration, then return to a concrete repertoire comparison.
Do not require a perfect population-level human model before asking whether a
study recommendation is robust to plausible fallback/recall assumptions.

## Verified upstream facts and implications

Sources inspected on 2026-09-08; pin exact code and checkpoint revisions before
execution rather than relying on moving repository branches.

- [Maia-2's official repository](https://github.com/CSSLab/maia2) documents
  pretrained Rapid and Blitz models, CPU inference and a position API taking
  FEN plus both ratings and returning move probabilities. It identifies the
  released training range as May 2018 through November 2023. The repository
  labels its code MIT. **Inference:** Maia-2 is a practical first comparison for
  our existing 2024 data, subject to verifying the specific checkpoint's origin.
- [Maia-2 rating mapping](https://github.com/CSSLab/maia2/blob/main/maia2/utils.py)
  separates 1800-1899 and 1900-1999 but groups all ratings >=2000 together.
  Our >=1800 cohort is supported by inputs, but exact high-rating differentiation
  must not be claimed. A Rapid checkpoint on Classical games is a declared
  transfer assumption, not native Classical validation.
- [Maia-3's repository](https://github.com/CSSLab/maia3) offers 5M/23M/79M
  variants and identifies 5M as the initial CPU option. Its code is labeled
  AGPL-3.0; weight terms need explicit artifact-level recording before any
  redistribution. No legal compatibility conclusion is made here.
- The [Maia3-5M model card](https://huggingface.co/UofTCSSLab/Maia3-5M) reports
  training on January 2023-July 2025 Lichess games. **Inference:** 2024 is not
  temporally held out for that checkpoint. It can supply diagnostics, but a
  fresh evaluation needs verified nonoverlap or data beyond its training period.
- [Maia-3 inference code](https://github.com/CSSLab/maia3/blob/main/maia3/uci.py)
  masks illegal actions and computes policy probabilities, accepts both players'
  ratings, and can use reconstructed history. Its WDL-derived GUI score must
  not substitute for our Stockfish soundness/loss evaluation. Full policy output,
  rather than a best-move-only UCI result, is the relevant integration interface.

## Bounded next action

Start with a pinned Maia-2 inference smoke test in a separate artifact-local
environment/cache. Inspect dependencies, checkpoint loading, size and provenance
first; check current resource headroom before any substantive inference. Load
released weights only; no training or fine-tuning. Do not change the existing
research environment or original checkout's environment.

Use the Blitz model for Blitz and Rapid for Rapid. Initially declare Rapid on
Classical as transfer and report it separately. Pass side-to-move/opponent
ratings correctly, record >=2000 bucketing, and test all-legal normalized move
probabilities, color orientation, promotions and terminal boards. Draw from the
distribution; do not turn Maia into an argmax or engine-filtered policy. Keep
Stockfish for objective soundness and the existing recall/study mechanism for
preparation. A recalled book move and an unprepared-policy draw remain distinct.

First score a small frozen diagnostic sample to establish that the adapter
works and to measure cost. Then freeze a Maia-specific comparison protocol,
checkpoint/config identities and a defensible held-out cohort before making
performance claims. Existing M3/M8 outputs remain reference model predictions.
Do not call pretrained training-overlap data fresh merely because our own
research had not opened it.

After this bounded calibration, return to a concrete study-package decision
under common utility, horizon, soundness and study constraints. Compare the
original-method reference and recall-aware alternatives, and report which
recommendations survive fallback/recall sensitivity. Maia predicts ordinary
human moves, not personal memory failure or study effort; these remain explicit
assumptions or later personalization inputs. Another open-ended predictor
development loop is not the default next step.

Status: upstream feasibility checked and direction recorded; Maia has not yet
been installed, downloaded or evaluated locally.

**Update:** [the integration is complete](maia_integration.md). The user selected
self rating 1400 as an ease/intuitiveness proxy despite a higher actual rating.
Opponent conditioning retains the supplied actual rating; opponent policy is
not downgraded. The above pre-integration status is historical. Both checkpoints
are installed and verified, and the 96-case integration diagnostic passes.
