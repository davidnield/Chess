# Plan from 26 November 2026: ownership back with Claude

*Claude. David handed the project back on 26 Nov after
[my review of ChatGPT's 22–26 Nov work](claude-review-of-chatgpt-2026-11-26.md) and asked me to implement our
decisions and the review's recommendations. Those decisions are recorded in the review's "David's decisions"
section; this plan turns them into an order of work. Branch `claude/ownership-20261126` (from `069b0e4`), in this
worktree.*

## The goal, in David's words

One repertoire for OTB classical and for online play against players stronger than he is now. Current ratings:
1250 OTB, 1800 Lichess Rapid, 1700 Lichess Blitz. Lifetime goals: 2000 OTB and 2200 online. The openings must help
him get there and stay effective once he does. He prefers advantages that are easy to convert.

## What that means for the model

| part | decision | status |
|---|---|---|
| Opponent | `otb-ensemble-v3`: reply = λ·(OTB model, smoothed toward online) + (1−λ)·(online model), λ = 0.5 to start; v2 kept bit-identical | **code done** (26 Nov) |
| Soundness | keep the stricter gates; they keep the book sound at 2000/2200 | unchanged |
| Choosing a book | best weighted average across online-now, online 2200+, OTB < 1600 and OTB 1800–2000, among books within a small margin of the best at 2200+ | to implement in finalist scoring |
| Own play | build with a weaker-than-current own model (Maia 1200 or 1450); check finalists at 1700 and reject a book whose advantage depends on weak own play | grid supports it; calibration pending |
| Own counts | observation cap (100 / 1,000 / none) in `(c + K·q)/(n + K)`, so the model keeps a share where weak players are still in their own book | **code done** (108-transform grid) |
| In-book contamination | measure: the same position reached by a popular route vs a rare transposition | study to run |
| White activation | qualified local SF18 search for booked or gate-deciding White children within about 3 expected-score points of a gate where sources disagree on the side | to implement before any White activation |

## Order of work

1. **Code from the review: done on 26 Nov.**
   - F1, a fixed referee binding: `sweep.referee.play(referee_binding=...)`. The construction lookup is never
     consulted for a fixed referee; the role is recorded.
   - The opponent v3 mix and the own observation cap.
   - F6, the extension player term labelled as a legacy-own-model control.
   - The F7 mate-0 census (evidence; the producer is unchanged).
   - Tests: `_test_review_20261126` (6) and the affected suites.
2. **Pools.**
   - Weak finishes; broad follows automatically.
   - Adult sorted publication (512 buckets, about 26 h serial, ~855 GiB on E:) and the four independent gates.
   - Capacity is rechecked before each launch.
3. **Paired Black source pilot on the runnable legacy human path** (David accepted).
   - cdb2 against score334, stricter gates, B = 2000, p30.
   - Scored under **both** bindings plus fresh SF18 on the leaves carrying the most paired-difference mass. If the
     two disagree, the SF18 leaves decide.
   - Needs the adult and broad publications.
   - It also reports the OTB-weight distribution over the pilot graph's opponent reach (review F3) and real cost
     numbers.
4. **F4, prototyped on that real Black graph:** unexpanded W/R children valued at their engine eval with a
   calibrated error band ([0, 1] only with no eval), and paired difference bounds for top candidates. Measure the
   unresolved mass before building the global budget in Rust.
5. **Own-policy calibration** (108 transforms over six raw inference sets, mover-level validation) and the
   **in-book contamination study**.
6. **λ rescoring:** the pilot graph under v2 and v3 at λ ∈ {0.3, 0.5, 0.7}, then David sets λ and the robustness
   margin.
7. **Corrected-path Black pilot** (`pilot-acceptance-v2`), then the trimmed optimizer comparison (in-model objective
   on fixed real graphs; referee only where the book changes), then the staged sweep.
8. **Before any White activation:** the gate-adjacent SF18 checks, and the confirmation statistics recomputed with
   an opening-family cluster bootstrap.

## Standing rules carried forward

- Stockfish 18 is the reference; never switch engines without re-measuring the cp scale.
- Lifetime books stay the playing references.
- Switch rule: ≥ .005 over 4 paired seeds, beyond bootstrap uncertainty, no commonly reached move over the
  soundness limit on fresh SF18.
- New flags default off and are exact no-ops.
- Frozen releases are never edited in place. Coordinators stop before their children. Receipts are kept.
- F: is on the NAS for David's backup. No further deletion of source-derived data until a verified second copy of
  ChessDB v6 exists.
