# Bounded empirical follow-through comparison

Freeze these choices before outcome scoring. Extend the shared boundary after
1.e4 e5 2.Nf3 Nc6 from three to eight plies, through Black's sixth move.
Keep empirical pooled mean-Elo>=1800 BRC opponents, Maia-1400 own fallback,
2000 opponent-conditioning scenario, contextual recall (.99 root familiarity,
default depth/distance decay), entry scenarios .1/.01/.001, first +/-300 cp,
.99 discount, child floor -100 cp and maximum engine loss 150 cp.

Keep all four original books: 3.d4 alone and three alternative fourth-move
cards. Add breadth4: 3.d4 plus Nxd4 after ...exd4 and Nc3 after each of ...Nf6
and ...d6. Compare at equal four-position cost with sequence4:
1.e4 e5 2.Nf3 Nc6 3.d4 exd4 4.Nxd4 Bc5 5.Be3 Qf6 6.c3.
Also include sequence3, omitting the 6.c3 card, to isolate the fifth/sixth-move
increments. These are fixed illustrative Scotch packages, not an exhaustive
opening search. Export full PGNs with prescribed White moves annotated.
Do not replace a card after seeing outcomes. Report ineligible packages if
either base or deeper engine gates fail.

Bound topology by expanding at most 64 decision positions per ply, with all
candidate-line prefixes forced into that limit. Rank remaining positions by
an optimistic arrival envelope shared across every book and Blitz/Rapid:
for each own edge use max(both Maia probabilities,1 if prescribed by any book),
and for opponents use empirical probability. This envelope is for computation
selection only, not a normalized behavioral policy. Sum transposition arrivals.
All legal own moves and all stored opponent moves are retained as graph edges;
unexpanded nodes and empirical OTHER/horizon mass remain explicit frontiers.
Endings are absorbing empirical W/D/L hazards. Never silently renormalize tails.
Absent empirical parents become unresolved frontiers; no Maia opponent fallback.

The context solver currently requires a DAG. Transpositions to already earlier
layers become explicit cutoff frontiers, rather than being relabeled as new
study positions. Same-layer arrivals merge by board hash and cost once. This
means the result is exact on the bounded graph, not the complete opening tree.
Card exposures exclude arrivals that might re-enter after a cutoff and must
be described as truncated-model predictions. Report cutoff and missing-score
uncertainty, and do not claim a full-model ranking from overlapping bounds.

Read source tables in bounded row-group projections, at most 64 requested
parents per layer, 5,000 retained move rows and 4 GiB cumulative decoding per
source per layer, with 32 MiB source row-group and 240-second layer guards.
Reuse existing verified rows/policies where available. At most 8 layers,
35,000 graph nodes, 100,000 edges, 600 new Maia calls, 600 preparation seconds.
Fingerprint sources before and after extraction; cache each layer atomically
to resume interruptions. Use the isolated Maia environment and one CPU thread.
Check shared memory/workloads first; leave canonical sources/processes alone.

Read existing canonical eval arrays with verification, at most 35,000 positions.
Freeze a bounded engine selection: all candidate parent/child gate positions,
plus up to 64 highest-envelope missing-score positions. Base: 100k nodes or
2 seconds per position; deeper: 500k nodes or 5 seconds for gate positions only.
One thread, Hash 32 MiB, no tablebases, total engine session guard 240 seconds;
cache partial records for resumption. Existing base records can be reused only
with provenance; gate checks use this experiment's paired base/deeper budgets.

Use the common evaluator and contextual solver, cap each at 2 million states,
and compare seven fixed books over six policy/entry contexts (42 cases if
all eligible). Report conditional and per-all-games discounted early advantage,
coverage intervals, disadvantage, recall, distinct cost, and incremental/equal
budget contrasts. W/D/L at computational/horizon boundaries is a neutral
placeholder, not a game-result forecast. No superiority claim solely from point
estimates. Independent path reconstruction must verify probability mass, legal
hashes, empirical probabilities, shared costs, gates and contextual exposure.
Record the remaining uncertainty if the bounded scope cannot select a package.
