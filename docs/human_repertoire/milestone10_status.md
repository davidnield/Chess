# M10 status: cost pilot passed; prospective protocol established

**Direction update:** the custom benchmark is paused following the user's Maia
suggestion. The [Maia checkpoint](maia_direction.md) is the current next priority.
The below pilot results remain valid; the previously planned custom development
and May experiment have not run.

The next bounded step is complete: the thirteen-feature implementation and
January cost pilot are tested, and the [prospective protocol](milestone10_plan.md)
and [configuration](milestone10_config.json) are established. **The full M10
benchmark remains pending.** No new weights have been fitted, no February
selection performed, and no May rows opened during this step.

The feature implementation preserves M8's first ten coordinates and appends
legal exchange exposure, exposure of the just-moved piece, and legal checking
reply count. An en-passant reply correctly identifies the pawn's actual square.
The exchange search reuses the bounded M9 abstraction without modifying it.

The fixed pilot processes the first 1,000 legal alternatives in the same January
panel order used by M8, with sorted SAN ordering and no deduplication. Results:

| Quantity | Observed | Limit |
|---|---:|---:|
| Legal alternatives | 1,000 | 1,000 |
| Timed feature loop | 0.727 seconds | 30 seconds |
| Exchange nodes | 5,903 | 20,000 per child |
| Maximum child nodes | 40 | 20,000 |
| Peak process working set | 70.76 MiB | Recorded, no separate RSS cap |
| Maximum sampled private memory | 49.88 MiB | 256 MiB |

Process memory is measured with the Windows process-memory API. The recorded
working-set peak includes input loading; the timed loop excludes it. Memory is
sampled every 50 alternatives, not continuously. The OS peak pagefile measure
is also recorded in the [pilot artifact](milestone10_pilot.json).

The first 1,000 alternatives cover only **three games and 29 distinct FENs**,
all at the start of the fixed panel order. They are not representative of all
600 games, all events, late positions or rare expensive exchanges. No linear
runtime extrapolation is treated as a resource guarantee. The next full phase
must retain its own runtime, aggregate-node, position and memory caps.

All **92 research tests pass**, including three new tests of feature symmetry,
M8-coordinate preservation, normalized feature ranges, en passant, defended
queen exchange loss and legal check counts. A separate replay reproduces the
exact ordering and all 1,000 feature vectors/node counts; its
[verification record](milestone10_pilot_verification.json) fingerprints the
pilot and implementation. Earlier frozen model fingerprints still validate.

The protocol keeps frozen M3 and M8 policies as references, specifies fresh May
likelihood and 96-case paired tactical calibration, and requires complete
engine coverage before any tactical comparison. Its relative progression rule
requires evidence of lower unseen severe-loss rate against M8 as well as
likelihood improvement. Secondary metrics cannot replace that rule after May.

Next priority: implement and test the M10 runner, including thirteen-dimensional
fitting, reference parity and all resource/freeze guards, then commit it before
development. Check current system headroom and active workloads before the full
phase. Fit on January, select on February, freeze the resulting model, and only
then proceed to May. No original checkout, canonical artifact, existing policy,
recall law or repertoire optimizer was changed by this step.
