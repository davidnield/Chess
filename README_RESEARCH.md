# Human repertoire research

This is the permanent research worktree on `codex/human-repertoire`, based on original commit `f249c4dced5935619500514e26fd1ddb0dea78a1`.

Start with [the research brief](docs/human_repertoire/human_repertoire_start_here.md). Its proposed workspace has now been created here. Context documents are copied into this worktree; their original paths and SHA-256 hashes are in [bootstrap.json](docs/human_repertoire/bootstrap.json).

The first milestone tests whether fallback play and imperfect recall change the best preparation plan. Implement a transparent small evaluator, an independent exhaustive oracle, and a bounded real opening comparison. Keep the initial code in `research/human_repertoire/`, notes and reports in `docs/human_repertoire/`, and generated data in `research/human_repertoire/_artifacts/` (ignored).

Claude's existing project continues in `C:/Users/David/Documents/Chess`. This worktree has its own files and branch. External datasets, processes and memory remain shared, so isolate outputs and bound workloads.

Milestone 1 is complete: [findings and limitations](docs/human_repertoire/milestone1_report.md),
[model contract](docs/human_repertoire/MODEL.md), and
[runnable commands](research/human_repertoire/README.md). The synthetic examples
show recall/budget-dependent book changes. The bounded real example cannot
distinguish early advantage and does not justify changing the user's repertoire.

Milestone 2 is also complete: [documented BRC cohort and validation report](docs/human_repertoire/milestone2_report.md).
It uses 4,000 games with both players rated >=1800 across Blitz, Rapid and
Classical (rating gap <=200), a twelve-ply horizon, and separate January/February
2024 cohorts. The move model has useful predictive signal against a weak baseline,
but sparse continuation support leaves book comparisons unresolved. Next work
should improve and validate fallback prediction and engine coverage on a sample
spread across dates before expanding optimisation or recommending a repertoire.
