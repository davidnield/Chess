# Next-stage readiness and recommendations — 2026-11-23

The full lossless ChessDB archive is now running from the tested v5 release on the main PC. The already completed candidate lookup/menu acceptance remains complete. The next bottlenecks are evaluation-source adoption, the large pool workspace, and confirmation of the opponent model on correctly separated data. This note makes the choices concrete; it changes no production source, book or cohort binding.

## Evaluation-source proposal

Keep the verified step-1 lookup as the control. A reasonable **default-off step-2 candidate**, restricted initially to ChessDB-covered positions, is:

- Deep cloud: retain the November 13 cloud/NNUE mean where NNUE is available, otherwise cloud.
- Shallow cloud: retain the November 13 median of cloud, NNUE and ChessDB, with that rule's documented missing-source fallback.
- No cloud: use ChessDB alone, including NNUE-only positions, rather than mixing it 2:1 with fishnet.
- Positions outside the tested coverage retain the existing source policy. Preserve terminal mates and the project's SF18 scale and expected-score gates.

This is a proposal to test, not a new global default. The saved handoff reserves the aggregation choice for David; the candidate would subsequently need the normal paired-book acceptance process. No claim that this diagnostic establishes practical repertoire improvement.

### Paired comparison on the existing panel

The exact same 1,500 positions are used for every policy (250 in each of six strata). Step 1 is the current source except ChessDB fills gaps and replaces pre-NNUE; the proposal changes cloud aggregation and NNUE-only positions as above. Thus this comparison avoids comparing one source on 1,250 rows against another on 1,500. All scores use the existing project conversion and +/-2000 cp clipping.

Errors are **expected-score percentage points**, not win-rate improvement. The overall figure equally weights the six source groups; it is not weighted by repertoire reach or population source prevalence.

| Group | Step-1 mean error | Proposed mean error | Step-1 99th-percentile error | Proposed 99th-percentile error |
|---|---:|---:|---:|---:|
| cloud_deep | 1.282 | 0.932 | 4.363 | 3.780 |
| cloud_shallow | 1.413 | 0.950 | 4.656 | 3.745 |
| nnue_once | 2.158 | 0.957 | 11.981 | 3.191 |
| nnue_repeated | 1.797 | 1.117 | 10.383 | 4.425 |
| pre_nnue | 1.019 | 1.019 | 3.569 | 3.569 |
| gap | 0.956 | 0.956 | 3.899 | 3.899 |
| all_equal_group_weights | 1.437 | 0.989 | 9.189 | 3.914 |

Overall paired mean-error reduction is 0.449 points, with a stratified paired bootstrap interval of 0.379–0.525 points (4,000 resamples; seed 20261123). Relative to the entire November 13 rule, reduction is 0.051 points, interval 0.022–0.079. These are conditional diagnostic intervals on this panel. The no-cloud proposal was informed by this panel, so these observations are **not an independent confirmation test** of the newly selected proposal. SF18 depth-24 references also have search uncertainty.

### Gate consequences are not uniformly favorable

Absolute-floor classification against the same SF18 parent references, 1,500 boards per color/setting:

| Gate/color | Step-1 false admissions | Proposed false admissions | Step-1 false exclusions | Proposed false exclusions |
|---|---:|---:|---:|---:|
| stricter, white | 29 | 36 | 20 | 10 |
| stricter, black | 45 | 32 | 14 | 5 |
| permissive, white | 17 | 14 | 8 | 4 |
| permissive, black | 38 | 19 | 11 | 9 |

The stricter White floor admits seven additional objectively-below-floor positions relative to these reference scores, even as total error and exclusions improve. These are parent-floor diagnostics for each prospective repertoire color, not a distribution of actual White/Black reach. They do not establish full relative-gate behavior in the transposition graph. A sensible next validation is a fixed, targeted deeper-search panel of admission flips and high-impact candidate disagreements, followed by the real pilot's reached positions. Keep near-boundary fresh verification and no-quality-downgrade rules. Do not loosen the already chosen gates to make the new source appear better.

Artifacts: D:/chess/day-20261123/source-policy (definition, paired inputs, source errors, bootstrap differences and gate tables). The producer script is retained beside the other daily artifacts before final handoff. No new holdout outcomes were inspected.

## Pool capacity and mini-PC allocation

The existing brc1800 probe retains 227.65 GB of arrivals across 9,177,335 files. Eight completed bucket outputs remain reusable. The two-bucket finalization pilot wrote 3.589 GB of final data, with 9.677 GB peak output plus spill and ~2.25 GB RSS under 2 GB DuckDB memory / two threads. Linear projections imply roughly 835 GB bucket outputs, 919 GB final outputs, and about **3.54 TB combined peak** including arrivals and scaled spill. The full global sort can require extra merge passes; 3.54 TB is not a hard upper bound.

- Main currently has D: ~507 GB free, E: ~1.125 TB, H: ~2.295 TB. None alone has room for that peak plus a useful reserve. F: is the archive destination and is excluded from pool sort/spill work.
- Mini has ~3.89 TB free. After a 50 GiB reserve, the projected 3.54 TB peak leaves only about **0.30 TB** headroom. This is enough to justify a bounded aggregation/finalization pilot, not enough evidence to promise the full global export will fit. Other pools' retained outputs cannot be ignored.
- A payload-only transfer of 227.65 GB at the measured ~104 MB/s is about 36.5 minutes. Copying nine million tiny files separately will be much slower. A mini pilot should use independently hashed per-bucket bundles, preserve full source/recipe receipts and produce isolated outputs. The current arrivals exist only for brc1800; they cannot be relabeled as brc-all or weak1000-1799.
- The mini currently lacks a validated Python/DuckDB/PyArrow workload environment. Native Stockfish and Rust audit jobs need none; Python-based pool jobs require a separate pinned user-local runtime and dependency smoke test before staging large inputs. No administrator change is needed or justified.
- Recommended sequence: finish the archive and its independent acceptance, then reassess actual reclaimable space; in parallel prepare a **one-bucket mini pool pilot** with a pinned runtime and bundled inputs if that becomes the chosen route. Keep input, bucket, final and spill storage separately budgeted. Do not queue all three large exports at once. The saved handoff retains a full-export decision after these checks; no full pool export was launched today.

The archive itself stays on the main PC because its native TerarkDB source and read-only WSL reader are local. Independent Stockfish batches with tiny position inputs are the mini's best immediate role; today's benchmark measures that role without adopting any engine output into the repertoire caches.

## Opponent-model and book prerequisites

The corrected OTB split contains historical families already exposed to earlier choices and a much smaller new-family cohort. Freeze the gate-300 versus ungated comparison and sparse-support metrics before examining that cohort, and establish exposure before labeling it fresh. Building the necessary pools and binding their exact training inputs precede the production ensemble/continuation checks. Actual pilot-graph Maia usage and decision mass cannot be inferred from a generic position sample.

After those dependencies, the next book remains the Black stricter 2,000-position pilot, including its verification closure, cost/resource report, PGNs, fresh best-reply checks and rollout smoke. The rest of the sweep still follows pilot acceptance. Neither the mini's availability nor the archive completion bypasses these checks.
