# Empirical opponents and encounter-based recall

The accepted recall model is implemented and verified. Opponent moves in this
experiment use the existing large positions tables; our failed recall uses
Maia-1400. Exposure follows the candidate book, including upstream failed
recall, and merges transpositions. Repeated visits count once per game.

These are model predictions, not measured personal recall or playing improvement.
The curve is r(f) = f² / (f² + .001²): roughly 99%, 50%, and 1% recall at
one encounter per 100, 1,000, and 10,000 games respectively.

## What the real example shows

The boundary is after 1.e4 e5 2.Nf3 Nc6. Each book prescribes 3.d4 and
optionally one White follow-up. Entries below are explicit frequencies per
**all games**, including color participation, rather than estimates of the
user's opening choices. Blitz fallback gives these predicted recall rates:

| Opening entry | 3.d4 | Nxd4 after …exd4 | Nc3 after …Nf6 | Nc3 after …d6 |
|---|---:|---:|---:|---:|
| Once per 10 games | 99.99% | 99.99% | 79.13% | 94.05% |
| Once per 100 games | 99.01% | 98.75% | 3.60% | 13.47% |
| Once per 1,000 games | 50.00% | 22.92% | 0.014% | 0.058% |

At the middle entry scenario, Nc3 after …Nf6 is encountered about once per
5,176 games. A uniform 85% recall assumption substantially overstates recall
there. Rapid fallback gives 3.59% for that card. At the rarest entry scenario,
Nxd4 recall is 18.83% with Rapid versus 22.92% with Blitz: different fallback
choices upstream change downstream exposure even with identical opponents.

The exact comparison covers four books, three entry frequencies and two Maia
time-control policies: 24 cases. Classical uses the Rapid policy as an explicit
transfer assumption. Opponent counts are pooled across Blitz/Rapid/Classical;
these are not separate empirical time-control estimates. Maia's opponent-rating
conditioning is the declared 2000 scenario; the user's fallback rating is 1400.

## Study allocation and limits

An independently enumerated synthetic one-card test demonstrates a changed
choice. A common position occurs in 1% of games and preparation produces an
advantage after a 6% opponent error; a rare position occurs in 0.1% of games
and preparation immediately produces advantage. With zero successful fallback,
85% flat recall prefers the rare card. Frequency-based recall prefers the
common card. Both use the same graph, one-position budget, soundness gates,
three-ply horizon and .99 discount. This is a constructed model prediction,
not evidence that common cards always deserve priority.

The real horizon ends three plies after the boundary, immediately after the
White follow-up. Within each scenario, all four books have the same observed
discounted early-advantage mass, and their missing-score bounds overlap.
For Blitz at one opening entry per 100 games, the d4-only conditional interval
is [0.3141%, 0.3316%], versus [0.2798%, 0.4289%] with flat recall. These are
coverage bounds, not statistical confidence intervals or a resolved improvement.
They are conditional on entering the opening; all-games contributions multiply
by entry frequency. No real package ranking follows from this short horizon.

The objective remains early first arrival at +/-300 cp, discounted by .99,
with child floor -100 cp and maximum engine loss 150 cp. The books pass the
inherited base and deeper engine gates. Study cost is one or two distinct
positions beyond the shared boundary. Horizon W/D/L uses a neutral placeholder,
so the reported points and conversion fields must not be interpreted as actual
game-outcome forecasts. Missing evaluations remain explicit uncertainty.

## Data and verification

A read-only snapshot contains 440 edges and 31 auxiliary rows from the existing
2013–2026 ge1800 BRC tables. The boundary has 102,196,214 stored move counts;
after 3.d4 there are 19,500,052. This replaces the small research prior for
these opponent positions. Counts represent pooled position occurrences, not
distinct personal games or an estimate of the opening-entry denominator.
The canonical pooler uses **mean Elo >=1800**, which differs from the earlier
research sample requiring both players >=1800 and a gap <=200. No source
generation sidecar was found; source file identities and footer hashes are
recorded without claiming stronger cohort provenance.

Stored move probabilities, omitted OTHER/horizon mass, and ending hazards are
accounted for together. Unknown continuation stays unresolved rather than being
silently dropped or replaced by Maia. White decision positions without copied
auxiliary data assume zero ending hazard; this limits outcome interpretation
but does not change arrival exposure at the studied positions.

The bounded streaming extraction took 7.7 seconds; preparation took 62.5 seconds
with 706 Maia inferences. Each policy graph has 8,468 nodes. Existing canonical
eval arrays supplied scores for 4,083 of 8,443 real positions, supplemented by
compatible previous engine records. Scores have mixed evaluation budgets and
are shared by all candidate books. No fresh engine job or data build ran.
Source row groups were streamed under a 32 MiB source-size guard; cumulative
decoded bytes were approximately 3.43 GB for move stats and 0.88 GB for aux.
The 4 GiB per-file decoded-byte guard is not a resident-memory allocation.

All **117 research tests pass**. Independent verification checks **21,408 legal
graph edges** and **1,092 scalar identities** across all 24 cases, including
first-encounter exposure, path-evaluator parity, probability conservation and
soundness. DAG exposure is solved exactly. Cyclic graphs use two-start damped
checks and reject disagreement or nonconvergence; agreement is not a uniqueness
proof. Test discovery requires the repository package root (`-t .`).

Frozen implementation/plan commit: `5f7226e`. The [manifest](exposure_manifest.json)
records input/code fingerprints and book definitions; [source provenance](exposure_sources.json)
records the bounded extraction and source identities; [results](exposure_results.json)
preserve every scenario and [verification](exposure_verification.json) binds the
result hash to the independent verifier. Full snapshots remain in the ignored
research artifact directory. The [prospective plan](exposure_plan.md) was frozen
before inference and scoring.

## Next priority

Extend a bounded common continuation beyond these follow-up moves so preparation
has time to affect the early-advantage objective, retaining exact card exposure
and explicit unresolved tails. Compare the same books and soundness constraints
before expanding optimisation. Continue entry-frequency sensitivity until an
upstream candidate repertoire supplies the per-all-games denominator; population
position counts alone cannot substitute for that denominator. This is the next
step toward actionable repertoire selection, rather than another fallback-model
benchmark.
