# Research worktree instructions

## Purpose and starting context

This checkout is the user's permanent human-repertoire research worktree. Read `README_RESEARCH.md`, `docs/human_repertoire/human_repertoire_start_here.md`, and `CLAUDE.md`. The first milestone is a working prototype, tests and an experiment report about fallback play and imperfect recall, followed by a bounded real example. Do not replace that milestone with a broad bug-fixing effort or a redesign of the production pipeline.

The original methodology is a reference to compare against, not a presumed inferior answer. Label synthetic demonstrations and simulated results as model predictions. Preserve the stated preference for early advantage under soundness and study constraints; make assumptions and utility explicit.

## Isolation

- Work in `C:/Users/David/Documents/Chess-human-repertoire`, branch `codex/human-repertoire`. Leave the original checkout, branch and running processes alone.
- Reuse code from this checkout. Do not import mutable modules by absolute path from `C:/Users/David/Documents/Chess`.
- Put new implementation under `research/human_repertoire/`. Keep durable design, decisions, status and experiment reports in `docs/human_repertoire/`.
- Use a separate local virtual environment. Existing data/eval files are inputs only. Keep outputs, temporary data, caches and spill in this worktree's ignored artifact directory; inspect absolute defaults before running existing scripts.
- Do not modify the original trainer database or canonical data/repertoire artifacts. Copy only bounded inputs needed for the experiment, with provenance and fingerprints.
- Small synthetic work can proceed independently. Check current system commit headroom and active workloads before substantial scans or model jobs. Full-data builds, long re-extractions and unbounded memory use are outside the initial milestone.

## Research quality and continuity

- Define fallback policy, recall, horizon, terminal/repetition semantics and study cost. Demonstrate effects with small interpretable examples before introducing complex learning models.
- Use independent tiny-book enumeration to check the optimiser/evaluator, with meaningful tests for mass conservation, distinct-position budget, shared positions, and reconstruction.
- Use a common evaluator and explicit configs for method comparisons. Distinguish changed model assumptions from changed optimisation machinery.
- Ask for personalisation inputs when they become necessary; synthetic and infrastructure work should continue using explicit configurable assumptions.
- Record progress and decisions in tracked files and make focused commits on the research branch. Never merge back into the original branch automatically.
