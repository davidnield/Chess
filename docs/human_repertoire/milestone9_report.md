# Milestone 9: residual tactical failures and the next candidate

This is a **retrospective diagnostic**, completed 2026-09-08, of all 96 frozen
M8 April cases. It adds no engine searches, source scans, model fitting or fresh
validation claim. April is already consumed evidence. The candidate's 22 severe
draws are simulated policy actions, not actual human fallback observations.

## What the failures show

The candidate and reference both make severe (>=150-cp) draws in 14 cases.
The candidate resolves 10 reference failures but introduces 8 others; 64 cases
are nonsevere for both. Thus the aggregate reduction from 24 to 22 conceals
substantial turnover. Twenty-one candidate failures occur at January-unseen
positions, and one at a seen position.

Fifteen of the 22 candidate failures have a frozen engine continuation beginning
with a capture. Three begin with check (these categories can overlap). These
are descriptive PV patterns, not proofs that an immediate capture explains the
entire engine loss. Quiet continuations and checking sequences also matter.

The new diagnostic enumerates the opponent's immediate legal captures, then
up to five additional legal captures on the same square. Each side maximizes
material gain and may stop. Material is P=1, N/B=3, R=5, Q=9; capture-promotions
include promoted material and en passant removes its actual victim. This is a
bounded exchange feature, not a tactical search: stopping is allowed even in
check; noncapture evasions, captures elsewhere, compensation and threats are
excluded; six capture plies truncate longer exchanges. No engine score enters
the feature.

| Diagnostic | Severe candidate draws (22) | Other candidate draws (74) | Actual human moves (96) |
|---|---:|---:|---:|
| Opponent exchange gain >=1 pawn | 16 | 18 | 16 |
| Opponent exchange gain >=3 pawns | 7 | 2 | 6 |
| Nonpawn capture without an immediate legal recapture | 4 | 2 | 5 |

All 96 actual human moves are below the severe engine-loss threshold in M8,
yet several have positive exchange signals. These signals therefore cannot
serve as hard tactical labels or automatic move exclusions. Existing material
imbalance, compensation, intermezzos and the restricted exchange tree matter.

The current M8 features mark the moving piece as geometrically hanging in only
3/22 severe cases and any friendly piece as hanging in 9/22. Thirteen severe
cases have neither a flagged hanging friendly piece nor an undefended moving
piece under that feature definition. Legal exchange information distinguishes
some defended-but-losing material that the geometric flags miss.

In 8/22 severe cases the candidate move receives a larger multiplicative
board-feature correction than the human move. In the other 14 it receives a
smaller correction but is still sampled. This separates a feature/ranking
problem from insufficient suppression of bad moves; it does not justify
increasing correction strength after inspecting April. M8's stronger beta=2
already lost to beta=1 on February likelihood.

## Concrete examples

All examples use the original board and the existing base-budget child PV.
The [annotated PGN](../../research/human_repertoire/_artifacts/m9/severe_cases.pgn)
contains all 22 positions, candidate and actual-human variations, and up to
eight engine plies per variation. The PGN's candidate line is explicitly a
model draw. Full FENs and descriptive fields are in the ignored residual ledger.

| Case | Actual human move | Candidate draw | Candidate loss | Existing engine continuation after draw |
|---|---|---|---:|---|
| GxsIAsKg:10 | Bxd4 | Qe2 | 591 cp | Nxe2+ Nxe2 Bxf3 gxf3 Bxe3 fxe3 |
| QDbt4uEl:5 | Qxg2 | f6 | 1,002 cp | fxg5 fxe5 Bd3 Ne7 O-O Ng6 |
| areLQStJ:8 | Bxd5 | O-O | 516 cp | dxc4 Qh5+ g6 Qe2 Qd7 Nc3 |
| yEVFGXck:11 | O-O | f5 | 248 cp | Qh5+ Kf8 Ng6+ hxg6 Qxh8+ Kf7 |
| kv3qa3tE:9 | Nf3+ | Ng6 | 583 cp | e5 Qb6 Qxb6 axb6 f4 N8e7 |

**GxsIAsKg:** Qe2 permits a knight to take the queen, followed by a knight
recapture. Defense does not make that trade safe. M8's maximum hanging-material
feature is zero; the legal exchange diagnostic finds a six-pawn gain for the
opponent. The human move also has a positive three-pawn exchange signal, so
relative context and learning still matter.

**QDbt4uEl:** f6 leaves a queen capturable by a pawn. The exchange diagnostic
finds an eight-pawn gain while the geometric hanging-material feature is zero.
This illustrates the same value-of-defender problem without a knight capture.

**areLQStJ:** castling leaves a bishop available to dxc4 without an immediate
legal recapture. Both the existing hanging-material feature and the new
three-pawn exchange signal notice exposure. The candidate already gives this
move a smaller correction than the human move; feature presence alone does not
eliminate the policy's probability of choosing it.

**yEVFGXck:** f5 permits a checking sequence beginning Qh5+ and Ng6+ that leads
to Qxh8+. Both the geometric hanging signal and immediate capture-exchange gain
are zero. A capture-only addition cannot cover this case.

**kv3qa3tE:** Ng6 has zero immediate exchange gain against it, yet the frozen
engine line begins with the quiet e5 and later exchanges queens. The actual
human move is Nf3+. The scalar material diagnostic misses the large engine-loss
contrast. This audit does not assign a complete tactical explanation to the
position from the short PV alone.

## One next candidate to test

Specify **M8 plus legal exchange and check-response features**, retaining the
same token prior and January exact-position posterior counts. Append three
features after each legal candidate move:

1. Opponent's best positive six-capture-ply exchange gain, clipped at 9 and /9.
2. The same best gain restricted to captures of the piece that just moved,
   identified by its destination square, clipped at 9 and /9.
3. Number of immediate legal checking replies, capped at 8 and /8.

Fit all thirteen coefficients jointly on the same hashed 600 January games,
using M8's fixed regularized objective and update schedule. Retain the same
four correction strengths and February selection rule, evaluated with the
unchanged position-count posterior. Keep M3 and frozen M8 as named references.
Never train coefficients or select correction strength using April loss labels.
These design choices are informed by April and must be described that way.

This is one bounded, interpretable next benchmark, not a claim that three more
features solve the problem. Legal checks provide threat information but do not
identify their consequences. The fixed-square exchange abstraction remains
limited; a probability model must learn how much to trust each signal.

Before fitting, run a separate cost pilot on the first 1,000 legal alternatives
in the already-hashed January panel: one thread, 30-second guard, at most 20,000
exchange nodes per child and no engine. Record elapsed time, node counts and
memory. If a bound is hit, stop and revise the development plan explicitly;
do not drop expensive positions. A successful small pilot permits a bounded
full phase, not an assumption of constant cost on every board.

Proposed full-phase caps retain M8's 20,000 cached positions, 700,000 legal rows
and 360-second phase guard, adding a five-million aggregate exchange-node cap.
Check current memory/workloads before starting that phase. Preserve all legal
move probabilities; add no tactical veto and no hard threshold learned from the
96 April cases. Freeze implementation, protocol, coefficients and February
selection before opening **May 2024** rows. May has not been opened for this
audit. A prospective protocol must define the May sample and paired likelihood/
loss-calibration criteria before its extraction. No larger book comparison is
authorized by these descriptive feature counts.

## Reproducibility and status

New code lives in `residual_audit.py`, `test_residual_audit.py` and
`verify_residual_audit.py`; M1-M8 files and frozen identities are unchanged.
All **89 research tests pass**. New tests cover defended-queen exchanges,
illegal pinned captures, en passant, capture-promotion, node-cap restoration,
illegal PV rejection and an independent copy-tree enumeration oracle.

The audit traverses only 1,187 exchange-tree nodes across 192 child boards.
Verification checks all 96 cases, replays 1,516 PV plies, recomputes exposure and
aggregate counts, and round-trips all 22 annotated PGN cases. The oracle checks
the declared restricted material game, not its adequacy as a chess evaluator.
Tracked [results](milestone9_results.json) and [verification](milestone9_verification.json)
identify frozen inputs and code hashes. Full outputs remain in `_artifacts/m9/`.

The residual audit is complete. Next priority is the January-only cost pilot
and prospective protocol for the specified thirteen-feature candidate. April's
retrospective results remain diagnostics, and personal recall, study cost and
counterfactual rollout validity remain unmeasured.
