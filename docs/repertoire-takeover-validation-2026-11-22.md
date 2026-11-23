# Takeover validation findings — 2026-11-22

Implementation: codex/takeover-hardening-20261122. Publication/lookup fix commit eb05705.
Runtime artifacts: D:/chess/takeover-20261122. This report changes no repertoire, gate, model, or source weights.

## Integrity and runtime

All 512 menu position files and all 512 move files passed their original SHA-256 receipts. Publication is now
_MENUS.COMPLETE.json; the original _DONE name collided with _done on Windows. The 512-bucket import was retained.
The old lookup loop was stopped with 167 completed checkpoints preserved. V2 fingerprints actual inputs and
outputs, uses an exclusive coordinator lock and fresh per-bucket workers, and compares every available original
bucket. Forty-three relevant tests passed, including same-size/same-mtime corruption and process-lock recovery.
Eight real benchmark buckets match the originals byte-for-byte. Four workers completed four in 62.5 seconds,
peaking at 3.36 GB RSS per worker. The full candidate rebuild is running; publication and final comparisons are
still pending. Its initial ETA is 2–2.5 hours; early progress includes eight fast revalidated benchmark buckets.

## Evaluation conclusions

The SF18 panel confirms the original response's correction: blends improve average accuracy in cloud groups,
while ChessDB improves the no-cloud NNUE groups. These are stratified diagnostics, not reach-weighted book scores.

Among the 1,314 positions with two searched candidates, strict ordering agreed in 1,061 and disagreed in 169;
84 had a tie in one or both sources. Strict agreement among non-ties is 86.3%, rather than counting ties as wins.
Within the searched candidate menus, choosing ChessDB's first move loses an average 1.73 cp / 0.139 expected-score
points against the best SF18-scored candidate; the 95th percentile is 12 cp / 0.982 points, and the largest
observed score regret is 4.316 points. This does not bound regret against unsearched legal moves.

Using the same top-three menu and max(parent reference, best child) under each source, 185 of 4,030 stricter-arm
candidate decisions were admitted by ChessDB but rejected by the SF18 reference; 35 flipped the other way.
For the permissive arm those counts are 62 and 25. These are diagnostic gate disagreements, not errors proven
against perfect play, and not counts of moves that the eventual book will select. Fresh gate-boundary and final
prescription verification remains necessary.

Cloud top-two gaps had mean error 1.148 points and 95th-percentile error 3.430 points on 234 comparable shared
move sets. Sixty of those sets mix depth or knodes. Equal depth/knodes does not prove a common search either,
because analysis identity is not preserved in this menu schema. Keep independently verified move comparisons
for critical extensions; do not treat a top-PV equality check as certification of all sibling gaps.

The diagnostic output directory validation-v2 contains the CSVs, references, source identities and detailed report.
The partial validation-v1 output was abandoned after an int8 mate-score conversion overflow; v2 casts to float
before scaling and completed successfully. It is not an alternate accepted result.

## OTB evidence and frozen next comparison

Corrected test roles contain 495,640 preserved-ancestry games and 6,633 new-family games. Earlier study code uses
historical split=0 for both tuning and confirmation. Repairing lineage prevents train/test crossing but does not
make those old observations unused. No claim of a newly untouched test set is made.

Before examining new-family outcomes, freeze: approved otb-ensemble-v2 weights, Maia-3 79M position-only
2600/2600 at temperature 1.35, and a paired support-gate-300 versus ungated comparison. Measure Brier and log-loss,
overall and sparse-support strata (<20 target games, including 100+ broad-only games), with family/game and board
clustering explicitly reported. Require an exposure audit before calling the 6,633 games fresh; otherwise use
an independently reserved later cohort. Report ancestry-preserved results as repeat validation separately.
Do not tune on the new confirmation cohort. Measure the actual pilot's Maia call fraction after graph selection.

This production-policy comparison still depends on complete accepted policy inputs; the current diagnostic
stage does not silently substitute the November 13 sample or retune the approved support gate.

## Pool capacity

Arrivals were measured at 227.65 GB in 9,177,335 files. Eight bucket outputs project to about 834.53 GB for all 512.
Retaining arrivals, all buckets, and a similarly sized final pool requires about 1.90 TB before external-sort spill,
filesystem overhead, free-space reserve, or the other two large pools. The previous 4.5-times trial multiplier
suggests about 3.76 TB as a conservative scenario, not a measured bound. Finalization performs a global external
sort and keeps the bucket inputs. Do not approve a full export from the bucket-size number alone.

Measure spill on a bounded finalization sample or use selective all-ratings reads before scheduling all three
large pools. Existing pool bindings remain on the accepted base lookup. No large pool export or cleanup was launched.


## Bounded finalization measurement

A two-bucket trial used the exact frozen pool writer, 2 threads, 2 GB memory and a 64 GB spill cap.
It correctly exported 47,759,281 edge rows and 40,747,691 auxiliary rows in 169 seconds, with final files
of 2.815 GB and 0.774 GB. Peak pilot output-plus-spill occupancy was 9.677 GB; peak RSS was 2.254 GB.
The final writer produced files larger than the corresponding bucket inputs. Scaling final output size
from this sample suggests approximately 919 GB for the full pool, making arrivals + buckets + final output
about 1.98 TB before spill/reserves. Scaling the measured temporary footprint suggests roughly 3.54 TB
including arrivals and bucket inputs at the peak; larger merges can add sort passes, so this is not a bound.
The two pilot outputs are marked BOUNDED-BENCHMARK-NOT-A-POOL and are not connected to construction inputs.

Current conclusion: do not start the three full exports on the available space as if each only needs 840 GB.
First reclaim verified/archive-backed space or prefer selective broad-pool counts after parity checks.
The candidate lookup and all existing pool bindings are independent of this decision.
