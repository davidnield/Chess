# Focused recommendation: add 5.Be3

**Under the existing opening-entry and recall assumptions, add the 5.Be3 card
to the d4/Nxd4 package.** The focused comparison now has a positive pessimistic
benefit bound in Blitz and Rapid, across all three entry/recall scenarios.
The targeted expansion and engine audit reduced local comparison uncertainty
by **91.4% in Blitz and 89.9% in Rapid**, using the same estimator on both graphs.

You play White. The added card is:

```pgn
1. e4 e5 2. Nf3 Nc6 3. d4 exd4 4. Nxd4 Bc5 5. Be3 *
```

The baseline learns 3.d4 and 4.Nxd4; the proposed package also learns 5.Be3.
Everything afterward uses Maia-1400 fallback, including White's sixth move.
No 6.c3 card was added. The [two annotated PGNs](be3_focus_packages.pgn) identify
the studied moves and their costs: two versus three distinct positions beyond
the shared opening prefix.

This is a resolved **local model recommendation**, conditional on the inherited
arrival and recall model. The uncertainty in reaching the position through
previously unexpanded earlier routes remains unresolved. It is not a proof of
the complete repertoire's benefit from move one, nor measured human improvement.

## Result and interpretation

The objective is unchanged: first reaching +300 cp before -300 cp by Black's
sixth move, with .99 discount per ply. The table reports the increase in that
discounted score from adding the card, in percentage points **conditional on
entering the opening after 2...Nc6**. It scales the local effect by the inherited
clean arrival probability and recall. These are conservative model coverage
intervals, not statistical confidence intervals or win-rate improvements.

| Opening entry per all games | Blitz gain | Rapid gain |
|---|---:|---:|
| Once per 10 games | +0.279 to +1.006 pp | +0.091 to +0.705 pp |
| Once per 100 games | +0.270 to +0.974 pp | +0.088 to +0.683 pp |
| Once per 1,000 games | +0.195 to +0.701 pp | +0.064 to +0.494 pp |

All six lower bounds exceed zero. At the central scenario, the frozen model
reaches the 5.Be3 decision on about 29.6% of games that enter the opening and
predicts 96.8% recall there. At the rarest opening-entry scenario recall is
about 71%; at the most frequent it is about 99.97%. To obtain the contribution
per all games, multiply a conditional interval by its entry frequency. For
example, the central Blitz interval becomes +0.00270 to +0.00974 percentage
points across all games. These small absolute numbers reflect the explicit
once-per-100 entry assumption; they are not estimates of the user's schedule.

At the decision itself, before multiplying by recall, deterministic Be3 versus
Maia fallback has a local score difference of **+0.982 to +3.538 pp in Blitz**
and **+0.321 to +2.480 pp in Rapid**. This does not mean Be3 is the engine's
unique best move, or the best possible extra card. It establishes benefit
against the specified fallback within the fixed candidate comparison.

## What removed the uncertainty

The previous broad graph spent computation across seven books. This run focuses
on the four plies remaining after reaching the Be3 decision. It expands all 42
legal fifth-move alternatives, every stored Black reply following those moves,
all 131 resulting White sixth-move positions, and the 1,024 most influential
Black sixth-move decision positions. Unsupported or unexpanded continuations
remain explicit frontiers. No omitted-move renormalization or Maia opponent
fallback was introduced.

The comparison also cancels shared continuation flow. Learning a card changes
the root move distribution by r * (I[Be3] - Maia), where r is recall. Both arms
then use the same fallback/opponent policies, so matching states with matching
remaining horizons have the same unknown continuation value. Their opposite
probability contributions can cancel. Unknown scores absorb flow before further
propagation; a path with an unknown prior first hit is never merged with a clean
history. Known first hits have fixed discounted payoff. Remaining uncertainty
is assigned pessimistic/optimistic payoff bounds according to its sign.

This is a change in evaluation machinery and coverage, not in the objective or
memory model. Applying this same signed estimator to the old graph still leaves
the local difference at [-16.49,+13.30] pp Blitz and [-12.37,+9.04] pp Rapid.
The new bounds above demonstrate that the expansion and targeted score audit
were necessary; shared-flow cancellation alone did not settle the old result.

At the central entry scenario, the separate prepared/fallback arm bounds overlap
slightly in Rapid. The paired difference is nevertheless positive because the
same unresolved continuation cannot simultaneously take two incompatible values
for identical states in the two arms. The independent verifier reconstructs
the shared uncertainty coefficients from complete paths.

## Limits of the recommendation

We preserve the previously verified arrival and recall values at relative ply
four. This is deliberate isolation of the additional card's continuation effect,
not an assertion that all earlier routes have been expanded. About 20% of the
opening's conditional mass enters a previously unresolved earlier frontier that
could potentially affect later card arrival. A conservative allowance for those
routes widens the central opening-level intervals to approximately
[-19.72,+20.96] pp Blitz and [-20.16,+20.93] pp Rapid. Those intervals remain
unresolved and are intentionally reported rather than canceled away.

That allowance is coarse: it does not prove that all of those earlier positions
can reach the Be3 card. It prevents extrapolating a local positive result into
an unsupported full-opening claim. Nor does it model recalibration of recall
if full-graph exposure changes. The documented conclusion is therefore to add
Be3 **within the fixed boundary model**; a globally certified repertoire ranking
still needs earlier-route reachability/exposure work.

The population remains the existing pooled mean-Elo>=1800 BRC table, with the
same provenance caveats as the [prior report](followthrough_report.md). Blitz
and Rapid use separate Maia-1400 fallback policies and shared empirical opponent
counts; Classical remains Rapid transfer. Opponent-rating conditioning is the
declared 2000 scenario. This is not a newly measured personal memory model or
a validated prediction of game W/D/L; neutral horizon outcomes are not used
to claim conversion gains.

## Work, controls and verification

The protocol and code were frozen in `4f244f3` before extraction/scoring; see the
[plan](be3_focus_plan.md). Each focused graph has 6,291 nodes and 6,730 edges,
representing 5,810 real positions plus explicit frontiers. Preparation took
78.4 seconds, with 19 bounded empirical query batches and 192 new Maia calls.
The prior 4 GiB decoded-byte and 32 MiB source row-group guards were retained.
The fixed caps were not widened after outcomes. Existing verified eval sources
covered 2,789 real positions; the frozen new audit covered the 128 highest-envelope
missing scores. Many residual unscored positions have very small probability.

The single-thread audit took 26.3 seconds, using 100k nodes or two seconds per
position, Hash 32 MiB and no tablebases. Both books retain their prior passing
base/deeper soundness gates. The usage-limit interruption occurred after graph
preparation; resumption reused that graph and ran the pending engine phase.
No canonical data, original checkout or existing user process was changed.

All **132 research tests pass**, including six new interval/cancellation tests.
Independent verification checks **12,498 legal chess edges** and **100 numerical
identities across six scenarios**, including separate-arm probability mass,
shared uncertainty coefficients, inherited prescriptions/recall/exposure and
global discount/entry scaling. All new engine PVs legally replay.

The [manifest](be3_focus_manifest.json), [full results](be3_focus_results.json),
[verification](be3_focus_verification.json), and
[old/new uncertainty comparison](be3_focus_diagnostics.json) preserve the outcome
and input/code hashes. Full graph, policy, source and engine snapshots remain in
the ignored `_artifacts/be3_focus/` directory. The fixed experiment's stopping
rule is satisfied: local benefit is resolved; broader upstream uncertainty is
explicitly bounded rather than prompting an unbounded expansion.
