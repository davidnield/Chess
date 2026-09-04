# Human repertoire research: starter brief

This brief starts a parallel research effort within the existing Chess repository. It captures the user's intent and the proposed methodology so a new task can work independently of the review conversation.

## User intent

The user has spent a year building a data-driven opening repertoire builder with Claude. They want to explore a more human-centred formulation while preserving that work and allowing Claude to continue developing it independently. The user has explicitly deferred bug-fixing discussion in favour of exploring the methodology. The alternative is a research hypothesis to test, not an assumed replacement for the current approach.

The product remains a memorisation-efficient, practically dangerous opening repertoire: obtain a meaningful advantage early, remain objectively defensible, and offer follow-ups the player can actually learn and execute.

## Workspace arrangement

Use a permanent Git worktree of the existing repository, on a dedicated branch such as `codex/human-repertoire`. A proposed location is `C:/Users/David/Documents/Chess-human-repertoire`. These are proposed names, not resources already created by this brief.

The existing checkout at `C:/Users/David/Documents/Chess` belongs to the current development effort. At the review checkpoint it was on `budget-books`, commit `f249c4d`, with a clean tracked working tree. Use that commit as the initial reproducible reference unless the user chooses a later checkpoint. Check that it resolves before setup, and record the full commit ID in the research project. Do not change the original checkout's branch or files while creating the new worktree.

Work in the research worktree. Keep the initial implementation in its own module, for example `research/human_repertoire/`, and durable design/decision notes under a tracked documentation directory. Reuse suitable existing functions from the research checkout, through small adapters. Do not import mutable Python modules from the original checkout by an absolute path.

Use a separate virtual environment, configuration, logs, caches and generated outputs. Existing scripts contain absolute output paths: inspect these before invoking any CLI. A different worktree does not isolate external files, shared drives, processes or machine memory.

Existing large datasets can be inputs. Open them read-only; do not overwrite, rename, rebuild or remove them. Create small, experiment-specific snapshots of the data actually used, with manifests recording sources, training periods, schema and relevant fingerprints. If an input changes, detect it and invalidate the experiment rather than quietly accepting a moving baseline. Avoid copying terabytes or scanning an entire dataset merely to produce a fingerprint.

Keep initial generated artifacts under the research workspace, in a deliberately ignored output directory. Changing to larger external output storage should be a separate, concrete setup choice. No canonical output filename should be reused by the research code.

Read the worktree's `CLAUDE.md` for data paths, operational constraints and validation conventions. Full builds take days and are not part of the initial scope. Before any substantial data scan or model job, check available commit memory and active workload; small synthetic work should proceed independently. Do not interrupt or reconfigure Claude's running processes. Merely launching two jobs in separate folders does not make them safe to run concurrently.

## Context to preserve

The following files are in the original checkout's gitignored `scratch` directory and will not arrive through a normal Git worktree checkout. Read them at these locations and copy selected documents into the research worktree's tracked documentation, with their origin recorded:

- `C:/Users/David/Documents/Chess/scratch/handoff_for_external_review.md`: original project hand-off.
- `C:/Users/David/Documents/Chess/scratch/external_review_2026-09-04.md`: external review, including a critique of the optimisation and evaluation methodology.
- This file: `C:/Users/David/Documents/Chess/scratch/human_repertoire_start_here.md`.

The review's reproduction script and results are available if needed, but reproducing or fixing every listed defect is not this research task's objective. Where a defect affects a reused component, isolate it with a small adapter or an independently checked implementation inside the research worktree, and record the difference. Keep an unchanged reference to the original method for fair comparisons.

## Proposed research question

Does modelling the player's unprepared moves and imperfect recall change which sound opening preparation is worth learning, and does it improve predictions or recommendations relative to the current perfect-recall/card-budget model?

The main proposed changes are:

1. Separate engine soundness, human move probabilities and human outcome predictions.
2. Model both sides' behaviour. For the prepared player, mix the prescribed move with an unprepared move distribution using an explicit recall probability. Unlearned positions use the unprepared policy. Permit recognising a learned position after an unfamiliar move order where the chosen formulation supports it.
3. Optimise preparation packages, including required follow-ups, under a study budget. Begin with distinct-position counts and simple recall scenarios; move to measured learning/maintenance effort only when data support that refinement.
4. Make the objective explicit. The user's stated preference is early meaningful advantage subject to soundness, acceptable overall performance and limited study burden. Report early advantage, early disadvantage, W/D/L, expected points and study cost separately. Do not invent a complicated weighted sum before testing simpler formulations.
5. Model early advantage as a first-hit event with a defined horizon; evaluate conversion separately. Define terminal and repetition handling explicitly.
6. Test robustness to plausible changes in opponent strength and familiarity. Simulated improvement is a model prediction, not evidence of a causal improvement in human playing strength.

Retain what is useful from the existing project: game/statistics infrastructure, tested position hashing, empirical opening distributions, engine evaluation access, selected book formats and potentially the trainer. Do not begin with a full ingest, a rewrite of Stage 3, a new UI, or custom neural-model training.

## First milestone: demonstrate the modelling difference cheaply

Deliver a small runnable prototype and a short experiment report:

- Define an explicit player profile, opponent profile, horizon, study budget and utility. Keep choices configurable. Ask the user for their actual rating/time control and practical learning preferences when those become necessary for personalisation; synthetic work can proceed before that answer.
- Implement a minimal policy evaluator with exact game transitions, an unprepared human policy, a prepared move table and configurable recall. Start with transparent synthetic probabilities. Document assumptions about independence of recall events and whether preparation can resume after a deviation.
- Include a demanding line whose payoff depends on several remembered decisions and an easier alternative. Show when budget and recall change the preferred choice. Demonstrate that the result follows from the objective, not an arbitrary opening preference.
- On tiny finite graphs, enumerate feasible books as an independent oracle. Test probability conservation, budget feasibility, transposition cost and evaluation/reconstruction consistency. Allow returns to learned positions only with explicit finite-horizon/repetition semantics.
- Compare the same candidate sets under perfect recall, imperfect recall, and a small set of fallback-skill assumptions. Use common probability and value inputs to attribute differences to the proposed model.
- Add a small real opening example using existing data after the synthetic evaluator is correct. Read a bounded extract into the research workspace. Use the existing repertoire as one candidate/reference rather than assuming its values are an independent evaluator.
- Record predicted benefits, sensitivity, uncertainty, study burden and the highest-impact changed decisions. A model that cannot yet distinguish the alternatives credibly is a legitimate research result.

The deliverables should include a documented command, small fixtures, a resolved experiment config, source provenance, tests and a concise report. Keep enough state in tracked files that subsequent tasks can continue without this conversation.

## Later milestones, conditional on the first result

Validate a human move model on a held-out sample for the intended population. Empirical probabilities remain useful in common positions; model predictions can provide a prior/fallback in sparse positions. Maia-3 is one candidate to benchmark, not a dependency that must be installed immediately: https://github.com/CSSLab/maia3 . Verify current interfaces and resource requirements before integrating it.

Next, add selective search over preparation packages, with exact solutions on small instances for comparison. Evaluate distinct learning costs and retention scenarios before attempting an enormous optimisation problem. Use the trainer's recall and timing information through a read-only snapshot if authorised and relevant; do not share its writable SQLite database with the existing app.

Separate training, tuning and final evaluation periods. Validate move probabilities and outcome predictions before trusting simulated repertoire uplift. Evaluate all methods on a common setup, and use prospective human learning evidence later to test whether the recommended material is learnable and useful.

## Working alongside the original effort

Keep changes modular and make small commits on the research branch. Record the original baseline commit and the identity of comparison artifacts. Integrate useful changes from Claude at deliberate checkpoints, using selected commits or an explicit merge, and rerun relevant checks. Do not automatically follow the tip of the other branch or merge research changes into it.

If the research later becomes a separate product, it can be extracted into its own repository then. Early on, shared history and the ability to compare or selectively transfer code are advantages.

## Suggested initial task message

You are working on the parallel human-repertoire research effort. Read this starter brief and the referenced project context, verify that you are in the dedicated research worktree, and implement the first milestone. Preserve the existing pipeline and Claude's checkout. Start with a small transparent evaluator and exact synthetic examples showing how fallback play and imperfect recall affect the best study plan. Reuse existing infrastructure selectively, keep experiment inputs and outputs isolated and reproducible, and report findings as hypotheses or model predictions until independently validated. Do not launch full-data builds. Document decisions and progress in tracked files so the effort can continue across tasks.
