# Priority reset: build the repertoire-selection loop

The user requested this reassessment after repeated follow-ups on 5.Be3.
This document overrides historical "next priority" paragraphs in experiment
reports. The starter brief and isolation requirements remain in force.

## Assessment

The brief asks whether fallback and imperfect recall change which preparation
packages are worth learning under a budget. Its later milestone explicitly says
to add selective search over preparation packages. `CLAUDE.md` describes a
repertoire-building product and supplies operational constraints; it does not
require resolving every local chess recommendation before a research builder
can be developed.

The initial prototype is complete. We now have empirical opponent adapters,
Maia-1400 fallback, configurable contextual recall, soundness checks, exact tiny
oracles, bounded graph expansion, uncertainty accounting and PGN export. These
are useful assets. However, recent real experiments choose books by hand and
evaluate them. The tiny `model.optimise` exhaustively enumerates books with its
existing fixed recall configuration; it is not a practical selector integrating
the later candidate-dependent exposure and contextual-recall model.

The important gap is automated allocation of study effort across alternative
positions and connected follow-ups. More upstream certainty about 5.Be3 has
diminishing value for closing that gap. The previous proposed next step is
therefore deferred. Be3 remains a conditional result and regression fixture;
none of its historical claims is strengthened by this reprioritisation.

## Next deliverable

A single runnable, bounded research workflow that accepts an opening region,
player/recall profile and study budget, then generates and evaluates candidate
books and exports the selected prescriptions as annotated PGNs.

Use a bounded White-after-1.e4 region spanning several empirically common Black
replies as the initial research scenario, not as a newly inferred personal
opening preference. Start from reusable existing adapters and read-only source
snapshots. Include a bounded projection of the existing repertoire among the
candidates, with provenance, plus sound alternatives. Do not restrict the next
comparison to the seven manually chosen Scotch packages.

Run several study budgets (provisional targets: 10, 25 and 50 distinct positions)
against the same candidate scope. If a resource bound cannot support a target,
report the feasible scope explicitly rather than claiming budget coverage that
was not achieved. Bound graph expansion, candidate-book evaluations, inference
and engine work in config before launching the real run.

Selective search must consider connected additions and substitutions, so a
useful sequence need not consist of individually profitable one-card additions.
Recompute exposure and recall for each proposed book using the current supported
graph semantics. Use exact enumeration on tiny instances to check the machinery;
a bounded heuristic on the real region must disclose that it is not globally
optimal. Existing soundness gates and shared-position costs remain enforced.

Compare (1) the existing-method projection, with its budget adaptation disclosed,
(2) a perfect-recall selection ablation on the same candidates, and (3) the human
model's selection. Score all three with the common human evaluator. Report the
tradeoff between study cost, early advantage, early disadvantage, coverage and
runtime. Preserve uncertainty and label provisional books accordingly. Do not
treat neutral horizon outcomes as evidence of acceptable whole-game performance.

## Completion and scope

Completion means the runnable workflow produces independently checkable books
at the supported budgets across several branches, demonstrates how selection
changes (or does not change) under the human assumptions, and records baseline
comparisons, PGNs, resource cost and limitations. It does not require proving
every selected move individually or establishing real-world human uplift.

The first implementation step is wiring candidate-book search to the existing
contextual evaluator and package operations, then checking that integration on
tiny graphs. Follow with the bounded real region in the same delivery effort;
do not promote each internal check into another user-facing milestone.

Defer custom fallback training, further Be3-specific expansion, trainer/UI work,
production pipeline changes and full-data builds. Address those only when the
working selection loop reveals a consequential limitation or the user explicitly
changes scope. This reassessment changes priorities and instructions; it does
not claim that the new builder has already been implemented.
