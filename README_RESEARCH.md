# Human repertoire research

This is the permanent research worktree on `codex/human-repertoire`, based on original commit `f249c4dced5935619500514e26fd1ddb0dea78a1`.

Start with [the research brief](docs/human_repertoire/human_repertoire_start_here.md). Its proposed workspace has now been created here. Context documents are copied into this worktree; their original paths and SHA-256 hashes are in [bootstrap.json](docs/human_repertoire/bootstrap.json).

The first milestone tests whether fallback play and imperfect recall change the best preparation plan. Implement a transparent small evaluator, an independent exhaustive oracle, and a bounded real opening comparison. Keep the initial code in `research/human_repertoire/`, notes and reports in `docs/human_repertoire/`, and generated data in `research/human_repertoire/_artifacts/` (ignored).

Claude's existing project continues in `C:/Users/David/Documents/Chess`. This worktree has its own files and branch. External datasets, processes and memory remain shared, so isolate outputs and bound workloads.
