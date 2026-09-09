# Familiar context and depth in recall

The revised model predicts **97.1% recall** for the user's studied White move
4.Nc3 after 1.e4 e5 2.Nf3 Nc6 3.d4 Nf6, at the same opening-entry scenario
that previously produced 3.6%. It accounts for an explicitly familiar tabiya,
distance from that tabiya, absolute ply, and encounter frequency. The original
frequency-only solver and results are preserved as a comparison.

This is a calibration to the user's statement that they would remember this
shallow response. It is not a measured 97.1% success rate or independent
validation of a memory model. The strength and decay assumptions are exposed
in configuration and the experiment records sensitivity to them.

## Decisions and model

The personal profile marks the position after **2...Nc6** as familiar, with a
configurable strength floor of .99. A known tabiya can remain familiar despite
infrequent recent use; this prior is distinct from its current encounter rate.
We do not infer personal familiarity from population counts. The initial
position is not automatically an anchor, so an unfamiliar opening beginning
1...b6 does not inherit a universal early-game boost. If that opening becomes
familiar, its tabiya can be explicitly added to the profile. The implementation
contains no special rule for Nc3 or for any particular prescribed move.

For frequency f, the original base remains B(f)=f²/(f²+.001²). The most recent
declared tabiya on each route supplies strength A=max(profile strength,B(anchor
frequency)). Contextual support at a decision is:

```
support = A * 2^(-max(0, distance_plies - 2) / 4)
            * 2^(-max(0, absolute_decision_ply - 8) / 8)
recall = B(f) + (1 - B(f)) * 0.98 * mean_support
```

Plies count half-moves already played: the decision to play 4.Nc3 is ply 6.
The first two plies from an anchor receive its full local support; thereafter
that support halves every four plies. Depth attenuation starts after ply 8
and halves every eight further plies. These constants implement a substantial
shallow boost and a fading rare-branch benefit; they are assumptions, not
empirically estimated psychological constants. The profile and .98 ceiling
were frozen before the real comparison in commit `dedc239` and the
[prospective plan](context_recall_plan.md).

At transpositions, mean support is weighted by actual arrival mass, with each
route's own depth and most recent anchor. A rare familiar route cannot give
all arrivals its full boost. The resulting recall is a position-level policy
pooled across routes, not recall that changes depending on the route taken
in the current game. Exposure then follows that policy, including upstream
recall failures. A new declared anchor replaces the earlier one on its route.

The current solver is exact on acyclic graphs, where a position is visited at
most once on a path. Cycles are rejected explicitly. Finite horizon and ending
hazards are retained. Missing/zero-frequency positions have no contextual
arrival mass. With no anchors or a zero ceiling, results reduce to the old
frequency model. Costs still count distinct studied positions; an anchor is
a familiarity input, not a free learned prescription. Only studied moves
receive recall; Maia-1400 supplies unprepared and forgotten-move choices.

## Real comparison

Opening entry remains an explicit per-all-games assumption, not inferred
personal behavior. At once per 100 games, the Blitz results are:

| Studied White move | Frequency-only recall | Contextual recall |
|---|---:|---:|
| 3.d4 | 99.01% | 99.97% |
| 4.Nxd4 after 3...exd4 | 98.75% | 99.96% |
| 4.Nc3 after 3...Nf6 | 3.60% | 97.14% |
| 4.Nc3 after 3...d6 | 13.47% | 97.44% |

Rapid produces almost identical contextual rates at this entry scenario.
At once per 1,000 games, 4.Nc3 after ...Nf6 still receives about 97.02% recall
because of the explicit stable-familiarity prior; frequency alone gives 0.014%
in Blitz. This is a deliberate implication of representing an already familiar
tabiya, not evidence of measured retention over long periods without practice.

Sensitivity for 4.Nc3 at once per 100 games gives **89.51%, 94.28%, 97.14%**
recall with contextual ceilings .90, .95, .98. These are alternative model
settings, not confidence bounds. The default satisfies the predeclared >=95%
interpretation of the user's qualitative calibration; the exact rate is not
identified by that statement.

The controlled illustration below holds the *position's own* encounter
frequency at once per 5,000 games, with tabiya strength .99, and moves farther
from an anchor at ply 4. These are synthetic calculations, not real sampled
chess lines or a forecast for 1...b6:

| Decision | Plies from tabiya | Predicted recall |
|---|---:|---:|
| White move 4 (ply 6) | 2 | 97.13% |
| White move 6 (ply 10) | 6 | 43.07% |
| White move 8 (ply 14) | 10 | 17.71% |
| White move 12 (ply 22) | 18 | 5.58% |
| Same rarity, no familiar anchor | — | 3.85% |

All 24 real cases use the same empirical opponent graphs, legal Maia fallback
probabilities, engine scores, candidate books and gates as the earlier
[exposure comparison](exposure_report.md). Blitz/Rapid/Classical cohort and
transfer assumptions are unchanged. No new source scan, inference or engine
evaluation ran. There is no claim of separate Classical validation.

The real early-advantage horizon is still only three plies beyond the boundary.
All four books retain the same observed early-advantage mass within each
scenario, and missing-score bounds overlap. This comparison therefore corrects
personal recall assumptions without establishing a best repertoire package.
Neutral horizon W/D/L remains a placeholder, not a game-result forecast.

## Verification and remaining work

All **123 research tests pass**, including six new tests for calibration,
upstream flow, zero/no-anchor parity, distance/depth decay, route-weighted
transpositions, finite horizon, ending hazards, anchor reset, distinct cost,
invalid profiles, state caps and cycle rejection. The independent verifier
reconstructs complete paths without the contextual solver or its support
function and checks **822 scalar identities across 30 cases**: 24 comparisons
and six sensitivity cases. It also compares the common outcome evaluator
against the independent path oracle and checks probability conservation.

The [full results](context_recall_results.json) include per-card exposure,
recall components, input/code hashes and the original frequency results.
The [verification record](context_recall_verification.json) binds the successful
checks to the result and verifier hashes. Larger graph/policy snapshots stay
in the ignored artifact directory and remain unchanged.

The boost applies to all studied shallow responses near a familiar anchor;
it does not yet distinguish simple associations from individually confusing
moves. No elapsed-time forgetting, review schedule or measured recall history
has been fitted. The next repertoire-selection priority remains extending a
bounded common continuation past these follow-ups, using this revised profile
and sensitivity scenarios, so the studied decisions can affect the objective.
