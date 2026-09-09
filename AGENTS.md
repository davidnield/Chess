# Research worktree instructions

## Purpose and starting context

This checkout is the user's permanent human-repertoire research worktree. Read `README_RESEARCH.md`, `docs/human_repertoire/human_repertoire_start_here.md`, and `CLAUDE.md`. The objective is a memorisation-efficient, practically dangerous repertoire: choose sound preparation packages under a study budget while accounting for human fallback and imperfect recall. The initial prototype and bounded-example milestone is complete. Do not keep repeating it through increasingly narrow single-move experiments. Do not replace the research objective with broad bug fixing or a redesign of the production pipeline.

The original methodology is a reference to compare against, not a presumed inferior answer. Label synthetic demonstrations and simulated results as model predictions. Preserve the stated preference for early advantage under soundness and study constraints; make assumptions and utility explicit.

## Prioritisation and completion

- Follow the active priority at the top of `README_RESEARCH.md`. Older reports' "next steps" are historical context, not a queue of mandatory follow-ups. The current reset and next deliverable are in `docs/human_repertoire/research_priorities.md`.
- Prioritise a reusable end-to-end research capability: candidate generation, selective package search, common evaluation, and inspectable repertoire export. A run should help answer which positions and follow-ups to study at a given budget across multiple opponent branches.
- Treat completed opening examples, including 5.Be3, as regression fixtures. Further work on one card needs a concrete connection to a limitation of the reusable builder or an explicit user request. Do not default to chasing every remaining uncertainty in that example.
- Before a substantial phase, state the capability or project-level decision it enables, its bounded completion criterion, and what evidence would change the next priority. Prefer a runnable vertical slice over another isolated benchmark when the necessary components already exist.
- Keep research rigor, but distinguish implementation correctness from empirical certainty. Test new search/evaluation machinery on independent tiny instances. Report unresolved rankings, coverage limits and assumption sensitivity without requiring a positive result or a narrow confidence interval before broader prototype development can proceed.
- Set resource and search limits prospectively. At a cap, retain the best feasible provisional result and its limitations, or report that no feasible result was found. Do not silently discard uncertainty, silently exceed caps, or turn every cap into an automatic new experiment.
- Measure progress by generated usable books, budget tradeoffs, meaningful differences from the reference, coverage and computational cost. Test counts and numbers of reports are verification evidence, not the primary measure of project progress.
- Preserve prior frozen experiments. Build the active workflow from reusable adapters and stable snapshots instead of making each new run depend on an ever-longer chain of historical experiment runners. Avoid a broad refactor solely to tidy history.
- Continue routine authorized priorities autonomously. Ask only for genuinely necessary missing decisions; use explicit provisional scenarios when personalization can wait. Do not ask for approval merely to move from a completed small experiment to the next authorized implementation step.

## Accepted modeling direction

- Existing large empirical position/move distributions drive opponent behavior. A small research sample's sparsity is not evidence of missing data in the authoritative source. Preserve omitted mass explicitly; do not substitute Maia for opponents as a new default.
- Maia-2 at rating 1400 models the user's fallback. Use the configured time-control policies and disclose Classical transfer and cohort limitations.
- Keep encounter frequency, ply/depth and declared familiar tabiyas in the configurable recall model. Treat current calibration as assumptions, not measured personal memory. Recompute candidate-dependent exposure/recall during search where supported; do not freeze one book's recall across all alternatives and call it equivalent.
- Preserve early advantage, soundness and distinct-position study cost. Compare methods with a common evaluator and candidate scope; distinguish the original-method reference from a perfect-recall ablation. Placeholder horizon W/D/L is not a validated performance guarantee.

## Isolation

- Work in `C:/Users/David/Documents/Chess-human-repertoire`, branch `codex/human-repertoire`. Leave the original checkout, branch and running processes alone.
- Reuse code from this checkout. Do not import mutable modules by absolute path from `C:/Users/David/Documents/Chess`.
- Put new implementation under `research/human_repertoire/`. Keep durable design, decisions, status and experiment reports in `docs/human_repertoire/`.
- Use a separate local virtual environment. Existing data/eval files are inputs only. Keep outputs, temporary data, caches and spill in this worktree's ignored artifact directory; inspect absolute defaults before running existing scripts.
- Do not modify the original trainer database or canonical data/repertoire artifacts. Copy only bounded inputs needed for the experiment, with provenance and fingerprints.
- Small synthetic work can proceed independently. Check current system commit headroom and active workloads before substantial scans or model jobs. Full-data builds, long re-extractions and unbounded memory use remain outside the standing research scope; use the explicit full-build approval rule in `CLAUDE.md` if scope later changes.

## Research quality and continuity

- Define fallback policy, recall, horizon, terminal/repetition semantics and study cost. Demonstrate effects with small interpretable examples before introducing complex learning models.
- Use independent tiny-book enumeration to check the optimiser/evaluator, with meaningful tests for mass conservation, distinct-position budget, shared positions, and reconstruction.
- Use a common evaluator and explicit configs for method comparisons. Distinguish changed model assumptions from changed optimisation machinery.
- Ask for personalisation inputs when they become necessary; synthetic and infrastructure work should continue using explicit configurable assumptions.
- Record progress and decisions in tracked files and make focused commits on the research branch. Never merge back into the original branch automatically.
