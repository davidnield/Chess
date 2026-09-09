# Does deeper preparation earn its study cost?

The extended comparison now distinguishes the packages on the evaluated
branches. **5.Be3 after ...Bc5 is the most promising added card in this fixed
candidate set.** A four-card sequence has a higher observed early-advantage
score than four cards spread across third-move replies. However, computational
cutoffs leave too much continuation unresolved to recommend one package over
the other confidently. This is a bounded model result, not evidence of improved
human playing strength.

All seven packages pass both engine soundness checks. All **126 tests** pass;
independent verification covers **42 cases, 19,368 legal chess edges and 1,206
scalar checks**. The experiment and its reviewable [full PGNs](followthrough_packages.pgn)
are complete. You play **White**; comments identify every studied move.

## What was compared

The common boundary is after 1.e4 e5 2.Nf3 Nc6. Evaluation now continues eight
plies, through Black's sixth move. The seven fixed books are:

| Package | Studied positions beyond the boundary |
|---|---:|
| 3.d4 only | 1 |
| 3.d4 and 4.Nxd4 after ...exd4 | 2 |
| 3.d4 and 4.Nc3 after ...Nf6 | 2 |
| 3.d4 and 4.Nc3 after ...d6 | 2 |
| Breadth: 3.d4 plus all three fourth-move responses | 4 |
| Sequence through 5.Be3 | 3 |
| Sequence through 6.c3 | 4 |

The full sequence is:

```pgn
1. e4 e5 2. Nf3 Nc6 3. d4 exd4 4. Nxd4 Bc5 5. Be3 Qf6 6. c3 *
```

Study cost counts distinct prescribed positions, so the last two sequence
cards apply only after their specific opponent replies. The common first two
moves are outside this experiment's charged budget. These are illustrative
fixed packages, not the output of an exhaustive opening search. They were
specified in the [plan](followthrough_plan.md) before outcome scoring; none was
replaced after seeing its results.

## Results at one opening entry per 100 games

The following numbers are **discounted early-advantage scores conditional on
entering the opening**, expressed as percentages. They count first reaching
+300 cp before -300 cp, discounted by .99 per ply. They are observed covered
mass, not win rates or estimates with negligible missing continuation.

| Package | Cost | Blitz observed score | Rapid observed score |
|---|---:|---:|---:|
| 3.d4 only | 1 | 1.532% | 1.782% |
| Add 4.Nxd4 after ...exd4 | 2 | 1.564% | 1.860% |
| Add 4.Nc3 after ...Nf6 | 2 | 1.517% | 1.775% |
| Add 4.Nc3 after ...d6 | 2 | 1.560% | 1.810% |
| Breadth across three replies | 4 | 1.578% | 1.882% |
| Sequence through 5.Be3 | 3 | 2.347% | 2.408% |
| Sequence through 6.c3 | 4 | 2.336% | 2.400% |

Adding **5.Be3** to the d4/Nxd4 package increases the observed score by
**0.784 percentage points in Blitz and 0.548 in Rapid**. Adding **6.c3** changes
it by -0.011 and -0.008 points respectively. Its apparent role is defensive:
observed early-disadvantage mass falls from 1.376% to 0.949% in Blitz and from
1.455% to 1.087% in Rapid. Those disadvantage observations also omit unresolved
continuations; they do not prove a reduction in total practical risk.

At equal four-position cost, the sequence exceeds breadth by **0.758 points
in Blitz and 0.518 in Rapid on covered mass**. The same direction holds at
opening-entry scenarios of once per 10 and once per 1,000 games. This consistency
is sensitivity within one model and topology, not an independent validation.
To express the once-per-100 scenario as an all-games contribution, multiply
conditional values and differences by .01.

The revised recall model remains active. At the once-per-100 entry scenario,
5.Be3 is encountered about once per 338 games overall and has roughly 96.8%
predicted recall. The 6.c3 position is encountered about once per 589 games
with Blitz fallback or 563 with Rapid, with predicted recall 84.7% and 85.8%.
These are truncated-model exposure predictions with the previously declared
familiar-tabiya profile; they are not measured personal retention.

## Why this does not yet select a repertoire

The finite graph expands only 64 decision positions per ply, preserving all
candidate-line prefixes and retaining other moves as explicit cutoff frontiers.
At the central entry scenario, **38.9–58.3%** of conditional play reaches a
frontier before the horizon, depending on package/policy. Those branches could
materially change the ranking.

For the four-card sequence, the Blitz early-advantage coverage interval is
[2.336%, 40.188%], versus [1.578%, 40.917%] for breadth. The resulting
sequence-minus-breadth interval is approximately **[-38.58, +38.61] percentage
points**. Rapid is similarly unresolved at about [-37.31, +37.37] points.
These are conservative coverage intervals, not confidence intervals.

The source of uncertainty is now clear. In the four-card sequence, almost all
early cutoff mass comes from the **computational expansion cap**: 39.669% in
Blitz and 38.861% in Rapid. Empirical OTHER/horizon tails account for only
0.0241% and 0.0248%; earlier-layer cutoffs account for less than 0.0004%.
Missing engine-score mass is also small, about 0.18%/0.17% in that package.
The large empirical dataset is supplying opponent probabilities successfully;
this result is not evidence that it lacks common-position coverage.

The largest portion of the sequence's cutoff mass occurs immediately after
White's sixth move: 17.75% Blitz and 16.58% Rapid. The
[cutoff diagnostics](followthrough_cutoffs.json) identify specific omitted
positions and representative move orders, enabling targeted continuation work.
For example, continuations after 3.d4 exd4 4.Nxd4 g6 and after
3.d4 exd4 4.Nxd4 Bc5 5.Be3 d6 6.Nxc6 remain unexpanded.

Coverage intervals hold the solved bounded-graph recall policy fixed. They do
not include uncertainty from changing recall when omitted paths re-enter
studied positions. Such arrivals are excluded from this graph's exposure;
therefore neither the observed scores nor their intervals are a certified
bound for a fully expanded, self-consistent memory model. Computational/horizon
W/D/L uses neutral placeholders and cannot be read as game-outcome forecasts.

My recommendation is to treat **the 5.Be3 follow-up as the leading candidate
for further validation**, without selecting the four-card package for training
yet. There is no demonstrated early-advantage reason to spend the additional
card on 6.c3 in this bounded result, although its defensive role deserves
examination. A firm study recommendation would overstate this evidence.

## Data, controls and reproducibility

Opponent probabilities use pooled 2013–2026 mean-Elo>=1800 BRC position counts,
with source/cohort caveats inherited from the [empirical report](exposure_report.md).
Both Maia-1400 time-control policies use the same opponent counts; Classical
continues to use Rapid as an explicit transfer assumption. Maia opponent-rating
conditioning remains the declared 2000 scenario. Recall, engine thresholds,
discount and study cost are common across methods. The depth extension changes
topology and adds a frozen engine audit, so differences from earlier reports
must not be attributed to horizon alone.

Each event graph contains 9,461 nodes and 9,896 edges. Selection uses an
optimistic probability envelope over the union of books and both Maia policies
solely to allocate computation. Actual evaluation uses the normalized empirical
and Maia policies, with no discarded-move renormalization. Same-layer board
transpositions merge; a transition to an already earlier layer becomes an
explicit frontier, rather than a new copy of a studied position. The contextual
solver and independent verifier operate exactly on this acyclic bounded graph.

Preparation took **62.4 seconds**, including six bounded projected lookups and
**256 new Maia calls**. Existing score inputs supplied 7,364 of 9,248 real
position scores before the new audit. The first lookup attempt hit the 4 GiB
decoded-byte guard. Removing the source FEN text from the projection resolved
that without raising limits: requested FENs are checked against parent hashes,
and every copied move is legally replayed and checked against its child hash.
Those FENs are query provenance, not copied source EPDs. Initial cache files
were preserved under attempt-specific names; the correction preceded scoring.

The separate engine audit took **33.3 seconds** for 76 base targets and 12 deeper
gate targets, with one thread, 32 MiB hash, no tablebases, and the frozen
100k-node/2-second and 500k-node/5-second limits. All seven packages satisfy
child >=-100 cp and loss <=150 cp at both budgets. All PVs legally replay.
Shared resource headroom was checked; canonical datasets, the original checkout
and existing workloads were not modified.

Protocol/runner freeze: `1e17edf`; bounded projection correction: `a077bfd`.
The [manifest](followthrough_manifest.json), [results](followthrough_results.json),
and [verification](followthrough_verification.json) record code/input fingerprints,
all 42 scenario results, contrasts and independent checks. Large snapshots and
the per-position engine records remain in `_artifacts/followthrough/`.

The next useful step is reducing continuation uncertainty around the leading
5.Be3 comparison, especially the omitted sixth-move replies, while retaining
these books and the common evaluator. Another recall-model or opponent-model
benchmark would not address the limitation measured here.
