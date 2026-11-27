# Own-policy calibration: 26 November 2026

*Claude. Plan step 5 ([plan](claude-plan-2026-11-26.md)). Driver `python/sweep/own_calibration.py`, release
`D:/chess/bin/own-calibration-20261126-v2` (b4a725f; the panel stage ran under v1, 07089d2, and the same
`own_calibration.py`). Outputs in `D:/chess/own-calibration-20261126/run`: `report.json`, `transforms.csv`,
`offbook-cap.json` and each stage's receipt.*

## Question

Which of the 108 own transforms best predicts the move a 1000–1799 **mover** actually plays at their own turn in
held-out games? Each transform combines the training weak counts (game-mean bands 1000–1799, Jan 2013–Jul 2026)
with Maia-3 as (c·s + K·q)/(n·s + K). Its parameters are Maia's (own, opponent) ratings, a temperature, the
strength K and the observation cap.

The selection rule was fixed in the module header before any score was computed.

## Panel

- **Games:** 58,937 held-out Blitz/Rapid/Classical games, about 10,000 per (month, event). 2026-08 is
  development and 2026-09 confirmation. BOT games are excluded.
- **Moves:** 1,193,778 moves by movers rated 1000–1799 (median 1493), at plies 1–30.
- **Boards:** 833,013 distinct boards. 370k of them (44%) have no training games.
- **Families:** 129 opening families, used for the bootstrap.

## Result

**Chosen: Maia-3 at 1450/1800, temperature 1.0, K = 100.** Development selection NLL is 1.5455 nats per move
(positions with < 1,000 training games), and confirmation NLL is 1.5469 (328,805 moves).

Chosen minus each alternative on the confirmation month, with an opening-family bootstrap (negative means the
chosen transform fits better):

| alternative | difference | 95% interval |
|---|---:|---|
| best own 1700 (1700/2200, t 1.0, K 100) | −0.0209 | [−0.0259, −0.0167] |
| best own 1200 (1200/2200, t 1.0, K 30, cap 100) | −0.0190 | [−0.0211, −0.0168] |
| same with cap 100 | −0.0005 | [−0.0007, −0.0004] |
| same with cap 1000 | 0 | identical below 1,000 games, by construction |

- **Own rating:** 1450 fits the 1000–1799 movers clearly better than 1200 or 1700.
- **Temperature:** 1.0 beats 1.35.
- **Strength:** K = 100 beats 30 and 10.
- **Opponent rating:** 1800 beats 2200.

## Contamination (David's in-book concern)

A mover is "off book" once an earlier own move in the game had training share < 5%.

**At the same positions** (7,028 positions with ≥ 20 training games, played by both groups):
- Off-book movers play the training-modal move 1.1 points less often: [−9.1, +6.0].
- The counts predict off-book movers worse by 0.16 nats: [−0.06, +0.43].
- Both point in the expected direction, but neither interval excludes zero (96 families).

**Where the cap acts** (`offbook-cap.json`; cap minus no cap, negative = cap fits better):

| | n 100–999 | n ≥ 1,000 |
|---|---|---|
| cap 100, on book | +0.0068 / +0.0055 | +0.0130 / +0.0132 |
| cap 100, off book | +0.0031 / +0.0028 | +0.0097 / +0.0085 |
| cap 1000, on book | 0 | +0.0001 / +0.0001 (zero inside both intervals) |
| cap 1000, off book | 0 | **−0.0011 / −0.0012** (both intervals exclude zero) |

(development / confirmation.)

Reading:
- Off-book movers do drift from the counts toward Maia at heavily played positions. That is the contamination
  David described: the counts there partly record players still in their own book.
- The effect is small.
- Cap 1000 captures it without cost elsewhere. Cap 100 over-corrects.
- Our side is off book by construction once the repertoire ends, so the off-book row is the relevant one.

## Recommendation

**Own policy for the corrected path: Maia-3 1450/1800, temperature 1.0, K = 100, observation cap 1000.**
- It is the selected transform plus the cap that fits off-book movers better.
- It sits within David's decision to model our side below his current level. Maia 1450 also fits real
  1000–1799 movers best, so the weaker-than-current choice costs no fit.
- Finalists are still to be checked at 1700, as agreed.

This is a diagnostic, not a promotion: the contract value is set when the corrected-path pilot recipe is frozen.

Limits:
- The panel weights moves as real games produce them, not by repertoire exposure.
- The off-book flag is defined from the counts, so it also marks idiosyncratic players.
- 2026-08/09 had been used for earlier selections, though never for this model.
