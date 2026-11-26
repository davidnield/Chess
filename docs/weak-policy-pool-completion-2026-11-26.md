# Weak-player policy pool published: November 26, 2026

Operational addendum after the review snapshot `069b0e4`; no implementation,
parameter, acceptance criterion, or frozen producer was changed.

The BRC 1000–1799 full p30 policy pool finished range construction at 11:12:49 PST
and published `pool.json` at **11:36:18 PST**. The existing coordinator automatically
continued to the all-ratings pool. This is completion of an input dataset, not
acceptance of the corrected human model or a repertoire.

## Published input

- Directory: `E:/chess/policy-pools/weak1000-1799-p30-20261125`.
- Manifest SHA-256: `f08b894687646fd89d93891538fa880f9d3e573bd94bf475a347dc2227758d1f`.
- Recipe identity: `24ae51624bb6eadf3663ee7ebce1af64f4a92bbb9bf15fb3007a6829e9da9e8c`.
- All **512 buckets / 4,096 ranges**, **32,607,828,391 move rows**,
  **625,454,897,158 Parquet bytes**.
- Summed move observations: **84,813,059,313**. These are position/move counts,
  not a count of distinct games or players.
- Full 128-bit keys, minimum games 1, all observed moves through ply 30.
- Blitz/Rapid/Classical, game-mean rating bands 1000/1200/1400/1600, training
  months January 2013–July 2026. Child XOR is lossless. This policy-only format
  omits EPD, ending and evaluation sidecars; it is not a candidate-graph pool.

## Completion review

At 11:55 PST the read-only review independently checked:

- The expected config SHA, source identity, recipe identity, population and caps.
- All 167 pinned producer files against their stored hashes.
- Manifest/sidecar agreement and exact 512-by-8 file coverage without duplicate names.
- All range receipt identities, boundaries, record equality, file sizes, Parquet
  footer row counts and full-key schema; arithmetic consistency of recorded masses.
- An unchanged final manifest hash at the end of the review.

All passed. The frozen producer's publication procedure also rechecked source
inventories and SHA-256-hashed every completed Parquet payload before writing the
completion manifest. The monitor did **not** repeat a second 625 GB payload hash
pass or a full independent source aggregation. Prior regression and real-range
parity evidence remains separate; this is not a new semantic census of all rows.

Reproducible check: [verify.py](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/evidence/weak-pool-completion-20261126/verify.py).
Result: [verification.json](C:/Users/David/Documents/Chess/scratch/worktrees/sweep-preparation/docs/evidence/weak-pool-completion-20261126/verification.json).

## Remaining active work

At approximately 11:56 PST the all-ratings pool had seven complete buckets: three
retained pilots and four new production buckets. Intervals between the new production
buckets averaged 285.8 seconds, implying **about 40 hours** of remaining construction,
plus publication checks. This is still an early estimate. Pilot timestamp gaps are
excluded because their computation was interleaved with weak work.

The coordinator remains `config-v2.json`, unchanged, with one expected child and
empty stderr. D:/E:/H: were Healthy, with approximately 450 GB / 1.552 TB / 2.648 TB
free at the check, above the existing reserves. No restart or fix was required.

Continue the existing monitor for the all-ratings build. Do not repeat weak or mini
completion audits by default, restart finished producers, or launch adult sorted
publication. Adult publication/independent acceptance, model calibration, uncertainty
refinement, empirical audits and paired Black acceptance remain pending.
