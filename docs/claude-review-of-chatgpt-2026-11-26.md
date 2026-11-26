# Claude's review of ChatGPT's repertoire work, 22–26 November 2026

*Claude, 26 Nov ~08:30 PST.*

- **Reviewed:** `codex/takeover-hardening-20261122`, range `2218129..069b0e4` (50 commits), in
  `scratch/worktrees/sweep-preparation`. Everything started from
  [the consolidated hand-off](../scratch/worktrees/sweep-preparation/docs/chatgpt-handoff-to-claude-2026-11-26.md).
- **Nothing was changed:** no job, frozen release, branch or data was modified. Every check below was a
  read-only query or a code reading.
- **Live state at review time:** the weak 1000–1799 count pool was **running** (bucket 429 of 512 at 08:15). The
  all-ratings pool is queued behind it. F: was **not mounted**.

## Verdict

**The engineering is careful, and most of what I checked is correct.**
- Every arithmetic core I read matches its documented formula and fails closed on bad input: the opponent
  policy, the own-player blend, the 30/30/40 source policy, the continuation solver and the optimizer's
  whole-book evaluation.
- The R1 resume defect from my hand-off is properly fixed.
- The 8 original adult buckets and the 504 new ones are interchangeable (§3.3).

**Three things, though, stand between the current state and a meaningful paired Black pilot. None of them is a
test failure:**

1. **The referee cannot yet score paired arms with one common lookup** (F1, F2). As written, each arm's book
   would be judged by its own construction source, the confound the plan itself forbids.
2. **The corrected human-model path cannot run on a real graph** (F4). Any reply below the expansion threshold
   leaves an unresolved interval, the builder refuses every unresolved root, and the uncertainty budget that would
   resolve this is design-only. The hand-off documents this, but its step order understates it: it is a hard
   prerequisite, not a refinement.
3. **On the main-line opening tree, the opponent model is almost entirely under-2000 OTB players** (F3). 18,590
   positions, nearly all at plies 2–16, give OTB counts ≥ 90% weight. That is a consequential modelling choice. It
   was approved as a recipe, but its size was never measured or shown to David. (Precisely: 16,054 of the 18,590,
   86%, are at plies 1–16.)

A recoverability risk also needs attention before anything else is deleted (F8). The only retained full ChessDB
snapshot and the only 8 TB monthly archive both sit on F:, which is currently unmounted, and there is no verified
second copy.

---

## David's decisions on this review (26 Nov, after reading it)

- **F: is unmounted on purpose.** David connected it directly to the NAS to back everything up, and will reconnect
  it afterwards. Verify the NAS copy against the archive's own shard/receipt hashes, not file sizes, before relying
  on it.
- **Goals:** one repertoire for OTB classical and for online play against stronger players. Current ratings: 1250
  OTB, 1800 Lichess Rapid, 1700 Lichess Blitz. Lifetime goals: 2000 OTB, 2200 online. The openings must help him
  get there and stay effective after he does.
- **Opponent model (replaces the F3 open question; David agreed):**
  1. **Explicit population mix** instead of OTB data overriding online data as counts grow:
     `reply = λ·OTB-model (smoothed toward online) + (1−λ)·online-model`, starting λ ≈ 0.5. Rescore existing graphs
     before any rebuild.
  2. **Keep the stricter soundness gates.** They keep the openings effective at 2000/2200; the 23 Oct
     opponent-strength test showed trap books decay against 2200+.
  3. **Choose by robustness.** Score candidates against online at his current level, online 2200+, OTB under 1600,
     and OTB 1800–2000. Pick the best weighted average among books within a small margin of the best book at the
     2200+ level.
- **Own-play model (David's view, adopted):** build with a **weaker-than-current** own model (Maia 1200 or 1450),
  because he wants advantages that are easy to convert. Check finalists with the 1700 model, and reject only books
  whose advantage *depends* on weak own play. David's second concern is that own-move counts include players who are
  still in their own book, which makes post-book play look optimistic. Address it by:
  - **capping the effective own count** in `(c + K·q)/(n + K)`, so Maia keeps a share at popular positions. Add
    caps 100 / 1,000 / none to the 36-transform calibration.
  - **measuring the contamination directly** with the entry-bias machinery: at the same position, compare move
    quality for players who arrived by the popular route against those who arrived by a rare transposition. Discount
    the counts further only if the gap is material.
- **Eval-source question first (David accepted):** fix the referee (F1/F2), then run the paired Black
  cdb2-vs-score334 pilot on the **legacy, runnable human path**, scored under both bindings plus SF18 on the
  decisive leaves. Decide the source from that. Prototype F4 in parallel, and recheck the source decision on the
  corrected-path pilot later.
- **White activation (David accepted):** before any White book on score334 is activated, every booked or
  gate-deciding child within about 3 expected-score points of a gate, where the sources disagree about the side of the
  line, gets a qualified local SF18 search.

## 1. Findings

Severity:
- **P1:** blocks the stated next milestone, or risks a wrong repertoire conclusion.
- **P2:** should be fixed before wider use.
- **P3:** latent or hygiene.

| # | Sev | Area | Finding | Blocks |
|---|---|---|---|---|
| F1 | P1 | Referee | The only non-test path scores rollouts with the arm's own construction binding; the coordinator's common referee binding is pinned but never reaches `play` | Paired-pilot **acceptance** |
| F2 | P1 | Referee design | No referee is specified for cdb2 vs score334. Either construction source as referee favours its own arm | Paired-pilot **interpretation** |
| F3 | P1 (decision) | Opponent model | OTB weight O/(O+10) ≥ 0.9 at 18,590 positions, 86% of them at plies 1–16, covering 39% of OTB visits; the 1800+ online layers are almost ignored there | Pilot **interpretation**; wider sweep |
| F4 | P1 | Corrected human path | The builder refuses any unresolved W/R root; `min_expand > 0` makes unresolved roots near-universal on real graphs; the uncertainty budget is unimplemented | Any corrected-path book, including the pilot |
| F5 | P2 | Eval-source evidence | The confirmation gain over the proposal is small and only marginally significant under an independence assumption; stricter-White safety regressed | White activation |
| F6 | P2 | Extensions | The `engine-or-player` severity still uses legacy raw weak counts with a min-games threshold, not the corrected own policy | Only the 12 player-severity control runs |
| F7 | P3 | Source policy | `normalize` maps `mate == 0` to 0 cp, but the eval DB's mate 0 means the side to move is mated (no occurrences found) | Nothing now |
| F8 | P1 (ops) | Recoverability | ChessDB v6 (2.09 TB) is the only full snapshot after the raw copies were retired; the 8.03 TB monthly archive is the only copy; both on one USB HDD, unmounted, no verified backup | Further deletions; long-run safety |

### F1. The referee path cannot use a common evaluation binding (P1)

**Where:** `python/sweep/referee.py:16-19, 38`.

- When no `lookup` is passed, `play` takes `binding = policies.keys.binding` and `lookup = policies.keys.scores`.
  That is the **arm's** construction binding. The `ConsumerKeys` for an arm is opened on that arm's
  `--eval-binding` (`build_budget_books.py:674`).
- Passing any other lookup sets `binding=None` and labels the result `test_lookup_override=True`. So a legitimate
  common referee would be reported as a test override.
- `sweep/scenarios.py:30` requires `referee_eval_binding`, and `sweep/coordinator.py:105` pins it. But no
  production code calls `sweep.referee.play`: a repository-wide grep finds only tests.

**Evidence:** code reading. Scoring under another lookup is technically possible, because IDs decode through
`keys.halves(ids)`, but no API supports it as a non-test.

**Impact:** a paired cdb2/score334 pilot driven through this API would evaluate each book's leaves with its own
source. That inflates whichever arm's construction values agree with its own leaf values, which is the confound the
plan's §R-request-union text explicitly forbids.

**Fix:**
- Give `play` an explicit `referee_binding` argument, separate from `policies.keys`. Open its lookup and score by
  `keys.halves(ids)`, then record the referee binding sha in the result.
- Remove the "test" label from that path.
- Test that two arms with different construction bindings, played with the same referee binding, get identical
  leaf scores for identical leaf boards. Also test that the result records the referee binding, not the arm's.

### F2. No referee is chosen for the source pilot (P1, decision)

Even with F1 fixed, the choice of referee decides the answer:
- **A cdb2 referee** rewards books whose leaves cdb2 likes.
- **A score334 referee** rewards score334's own books. With the same leaves and the same values, it would partly
  measure agreement with itself.

**Recommendation:**
1. Score both books under **both** bindings, and report the ranking only if it agrees.
2. Also run a fresh **SF18-qualified check of the decisive leaves**: the leaves carrying the largest paired-difference
   mass, a few hundred positions, a few hours of mini time.
3. If the two lookups disagree on the ranking, the SF18-checked leaves decide.

This keeps the referee independent of both arms at bounded cost.

### F3. The opponent model is OTB-dominated on the main lines (P1, decision with evidence)

**Where:** `rust/book_builder/src/human_policy.rs:54`, `reply = (o + 10·pre)/(O + 10)`, with OTB strength 10.

**Evidence** (read-only query of `otb-counts-v2-20261119/counts/training-p30.parquet`):

| plies | positions with O ≥ 90 (OTB weight ≥ 0.9) | with O ≥ 10 (weight ≥ 0.5) |
|---|---:|---:|
| 1–4 | 361 | 737 |
| 5–8 | 4,007 | 14,757 |
| 9–12 | 6,801 | 46,493 |
| 13–16 | 4,885 | 57,926 |
| all | **18,590** (39% of OTB visits) | 178,967 |

The start position has O = 1,238,580.

**Impact:**
- At exactly the positions where the repertoire's main decisions are made, the opponent reply distribution is
  more than 90% the frequencies of the eligible under-2000 OTB training players. The 1800+, Rapid 2200+ and
  Classical 1800+ layers, often with millions of games, contribute under 10%.
- The repertoire will therefore be tuned to a weaker OTB population's main-line choices, even for online play
  against 1800+ opponents.
- That may be what David wants, since his OTB section is U1500. But he has never seen this magnitude, and the open
  decision "OTB-dominated where OTB data is plentiful?" is still listed as undecided in the plan.

**Fix and verification:**
- Report the OTB-weight distribution over the actual pilot graph's opponent reach (not over all positions), with
  a few main-line examples.
- Rescore the paired pilot under OTB strength 10 against, say, 300 and 3,000. The planned "OTB prior 10/30" sensitivity
  is too narrow to show the effect, because both values give OTB > 0.97 weight wherever O is in the thousands.
- Let David decide between one book for both populations and separate online/OTB targets before the main sweep.

### F4. The corrected human path is not yet runnable on real data (P1, documented but understated)

**Where:**
- `rust/book_builder/src/py_continuation.rs:88`: any edge with probability below `min_expand`, or past
  `max_states`, becomes an unexpanded `None` child with bounds [0, 1].
- `python/sweep/continuation.py:35`: only roots with width ≤ 1e-12 enter W and R.
- `python/build_budget_books.py:1296`: the builder raises if any root is unresolved.

**Impact:**
- On a real B=2000 graph with roughly a million stop roots and ~30 legal moves per ply, exhaustive expansion is
  infeasible, so a non-zero threshold will leave width at essentially every root.
- The refusal is correct and safe, but it means **no corrected-path book can be built** until the global budget and
  selective refinement exist.
- The hand-off's step order puts them before the pilot, which is right. They should be called the critical path.

**Design concerns for that work:**
1. **[0, 1] is the wrong bound for an unexpanded child that has an engine eval.** Most unexpanded W/R children are
   ordinary positions with a lookup score. Valuing them at their eval, with a calibrated error band, collapses most
   width without expansion. Keep [0, 1] for children with no eval at all. The final panel's p95 absolute error is
   about 2.6 expected-score points, which gives a starting band to validate.
2. **Selection depends on differences between siblings, not on absolute widths.** Two candidates whose
   continuations share unexpanded subtrees have correlated uncertainty. Root-weighted absolute widths will request
   far more refinement than ranking needs. Compute paired (difference) bounds for the decisions that matter: the top
   candidates, and gate-adjacent children.
3. **Prototype the cost on a real Black graph** before building the full budgeting machinery. Measure how much
   unresolved reach mass remains with eval-valued leaves at `min_expand` = 1e-3 and 1e-4.

### F5. The eval-source evidence is real but modest; White needs more (P2)

From [the confirmation results](../scratch/worktrees/sweep-preparation/docs/chessdb-ensemble-confirmation-results-2026-11-25.md):

- **The gain over the proposal is small and only just significant.** The blend's mean error is 0.084
  expected-score points below the proposal's (8.1%). Its simultaneous interval, [−0.166, −0.006], barely excludes
  zero. That is an ordinary paired bootstrap over 299 positions, and it doesn't cluster by opening family, so
  related positions count as independent. A cluster bootstrap could plausibly include zero.
- **The advantage over ChessDB alone is robust.**
- **Stricter White got worse on safety:** clear false admissions 5 against 4, total 7 against 5, unsafe shortfall
  4.82 against 2.72. These are small counts, but in the wrong direction on the metric gates care about.
- **The "ChessDB alone when there is no cloud" rule wasn't confirmed by this panel.** It rests on selection-phase
  evidence only; the final panel is all triple-overlap. That evidence is consistent: my 22 Nov SF18 panel showed
  13.2 against fishnet's 37.4 mean error in the NNUE-once group.

**Recommendation:**
- Use score334 for the **Black** pilot as planned.
- Before any White activation:
  1. Rerun the confirmation statistics with an opening-family cluster bootstrap.
  2. Require a qualified local SF18 search for every booked or gate-deciding White child whose blended score is
     within about 3 points (≈ the p95 error) of a gate and whose sources disagree in sign relative to the gate.
- Say "small mean-error improvement" rather than "8.1%" in decision documents.

### F6. Extension player severity uses the legacy own-count model (P2)

**Where:** `python/sweep/extend.py:116, 142`. `BookOwnMoves(..., own_policy=FIXED["own_policy"])` with
`player_min_games=FIXED["weak_min_games"]`. These are raw weak counts with a threshold, not the corrected
`OwnProvider` (weak+Maia, continuous, no abrupt cutoff).

**Impact:** only the 12 `engine-or-player` control runs, because the default is engine severity. Those runs would
measure the legacy own model, not the corrected one.

**Fix:** build the player term from the same `OwnProvider` and pin its identity in the extension contract. Or
explicitly label those runs as legacy-own-model controls.

### F7. Mate 0 is mapped to 0 cp (P3, latent)

**Where:** `rust/book_builder/src/eval_policy.rs:29`. In the eval DB, `mate = 0` means the side to move is mated,
which should be ±2000 by side.

**Evidence:** buckets 0, 255 and 511 of the eval DB contain no `cloud_mate = 0`, `fishnet_mate = 0` or `mate = 0`
rows, and terminal rows take precedence. So it never fires today.

**Fix:** reject `mate == 0` from a source as malformed, or assert it absent at overlay build time. Add a unit test.

### F8. Single copies on an unmounted USB disk (P1, operational)

**Evidence:**
- [The hand-off](../scratch/worktrees/sweep-preparation/docs/chatgpt-handoff-to-claude-2026-11-26.md) §3 and §10:
  ChessDB v6 is "the only full retained snapshot", now that both raw copies were retired on 25 Nov.
  `explorer_brc_p51_pilot`, 8.03 TB, is the full 163-month 128-bit archive. Rebuilding it takes about 60–72 h, and
  re-acquiring and converting ChessDB about 1–2 days plus a download.
- F: is unmounted at review time. No NAS copy has been verified. F: had a dirty-flag event on 25 Nov, resolved as
  a filesystem flag, not a hardware fault.

**Impact:** one drive failure costs days of rebuilding. ChessDB in particular depends on its upstream snapshot
staying available.

**Fix:**
- No further deletions of any source-derived data until a second, verified copy of ChessDB v6 exists. Then copy the
  monthly archive.
- The DSM USB-copy plan with checksum verification is sensible. Verify against the archive's own receipts.

---

## 2. What I checked and endorse

- **Lookup R1 fix** (`chessdb_lookup_run.py`): each bucket receipt binds the base-bucket sha, the eval-DB bucket sha
  (from the manifest), the menus receipt and positions sha, and the output fingerprint. The old outputs were compared
  byte-for-byte (167 of 167). This is the fix I proposed, done properly. The `_MENUS.COMPLETE.json` marker fixes my
  `_DONE`/`_done` collision.
- **Opponent policy:** native `human_policy::opponent` and Python `ensemble_reference` match term for term. Vectors
  span all legal moves (`frontier.vectors`). Nesting b ≥ a ≥ r is checked per move and fails closed. The gate
  condition matches the doc (Maia below support 300; Maia alone with no online and no OTB data).
- **Own policy:** `(c + K·q)/(n + K)` is continuous in n, as intended; the legacy n < 20 cliff is gone. Counts are
  replayed and child keys checked (`human_providers.OwnProvider`).
- **Source policy** (`eval_policy.rs`):
  - blend in expected-score space;
  - ChessDB side-to-move orientation and decisive-claim capping;
  - legacy-tier fishnet excluded;
  - no renormalisation;
  - missing evidence stays missing;
  - terminal precedence;
  - mirror symmetry tested.
- **Binding flow:**
  - the builder records the bound lookup root and sha in book meta (`build_budget_books.py:1601`);
  - extensions and rare-verify rebuild `ConsumerKeys` from it, so parents and children see the same scores;
  - the continuation lookup applies local verified overrides first.
- **Adult pool compatibility, 8 originals and 504 mini buckets:**
  - the mini manifest's `pool_code_sha256` `228e229c…` equals the frozen
    `D:/chess/bin/sweep-2dd3a63/python/pool_from_book.py`, so the speed-ups live in the driver;
  - the recipe is identical (book, events, bands, cap, minimum games, lookup `70bc73fc…`);
  - the only lookup-derived columns are `other_eval_*`, which are empty under the no-Other contract at minimum
    games 1;
  - with bucket 005's exact parity, the 512 buckets are semantically one generation.
  - Publication still needs the independent gates, as planned.
- **Raw Maia cache identity:** it covers the checkpoint, interpreter, runtime and GPU, every `maia3` package source,
  `maia_server.py`, `maia_batch.py`, `zobrist128.py`, the ratings, the `infer_raw` protocol and the batch size.
  Applying temperature after the cache is exact on masked probabilities. I first suspected a preprocessing gap;
  there isn't one.
- **Optimizer:**
  - `View::evaluate` prices a book by its root-reachable region, so abandoned branches free budget and transpositions
    cost once;
  - `exact::mip` re-evaluates its book and refuses an objective mismatch (`exact.rs:408-410`);
  - neighbourhood acceptance uses those values;
  - the unbudgeted bound is a valid upper bound for the given Core.
  - Local-optimality claims are scoped to frozen boundaries, as stated.
- **Continuation solver:** `depth` is plies *remaining*, so the ascending sort is a correct bottom-up order.
  Terminals are exact. Refusing unresolved roots is the right fail-closed choice (F4 is about what comes next).

Not reviewed in depth: the archive writer and auditor internals, the Rust archive query beyond its documented
971-query parity, the mini transport scripts, the source-inventory migration, the count-pool SQL and
partitioned-publication validators. I relied on their reports and receipts.

## 3. Claims the evidence does not support

- That 30/30/40 is safer, or better for the repertoire: only complete-case mean and tail accuracy was shown. Gate
  safety on White got worse (F5).
- That the gain over the proposal is robust: it is marginal, with an independence assumption (F5).
- That the ChessDB-alone fallback is confirmed: it rests on selection-phase evidence only.
- That the corrected path is close to pilot-ready: it cannot run on real data until F4 is solved.
- That "a common referee" is in place: only its binding is pinned (F1).
- Nothing in the record supports OTB-dominated main lines as the right opponent for online play (F3). It may be
  right for OTB.

## 4. Recommended order of work

1. **Back up ChessDB v6 and verify the copy (F8).** It doesn't compete with the pool jobs for CPU.
2. **Let weak, then broad, finish. Then do the adult sorted publication with the independent gates**, as planned.
   Capacity: D: has 420 GB free against a 250 GiB reserve, so the sorted adult copy (~855 GiB) belongs on E: or H:.
3. **Fix the referee (F1) and decide its protocol (F2).** That's small code: a day including tests.
4. **Decouple the eval-source question from the human model.** The cdb2-vs-score334 comparison mostly doesn't
   depend on F4. Run the paired **Black** source pilot on the **runnable legacy human path**, with the F1/F2 common
   referee. That answers the source question within days instead of waiting on the uncertainty work.
5. **Run an F4 cost prototype on a real Black graph:** eval-valued unexpanded leaves with error bands, paired
   difference bounds for top candidates, measured unresolved mass. Only then build the global budgeting machinery
   in Rust.
6. **Calibrate the own policy** (the 36 transforms) with mover-level validation, then the **OTB-strength rescoring
   sensitivity (F3)**, then a David decision on the opponent target.
7. **Corrected-path Black pilot**, accepted under `pilot-acceptance-v2`.
8. **Optimizer study, trimmed:**
   - compare solvers on the **in-model objective** over fixed real graphs; it is deterministic and needs no
     referee, since all solvers optimise the same objective;
   - send only solver choices that change the book materially to paired referee rollouts.
   - That is likely most of O2 at a fraction of the cost.
9. **Main sweep.** Revisit whether all 20 base arms plus 36 extension variants are needed to pick one book David
   will learn. A staged design (one colour at a time, prune by paired rollouts at each stage) would answer the
   practical question sooner.

## 5. Questions

**For ChatGPT:**
1. How was the common referee for the paired pilot meant to reach `sweep.referee.play`? Which binding: cdb2,
   score334 or a third (F1, F2)?
2. Was OTB strength 10 chosen knowing it gives ≥ 90% OTB weight at about 18,600 main-line positions? What evidence
   supports under-2000 OTB frequencies as the opponent for David's online games (F3)?
3. For W/R children below `min_expand`: is the intended treatment [0, 1], or an eval-based value with an error
   model? Has unresolved mass been measured on any real graph (F4)?
4. Is the first paired Black pilot planned on the corrected human path or the legacy one? If corrected, what is the
   realistic date given F4?
5. Was a cluster (opening-family) bootstrap considered for the confirmation comparison (F5)?
6. Should the player-severity extension controls use the corrected own policy (F6)?

**For David:**
1. Backing up the ChessDB archive and the monthly archive to the NAS now, before any further cleanup: yes?
2. The opponent target. One book tuned mostly to under-2000 OTB players on the main lines, or more weight on the
   1800+ online data for online play? See F3's table.
3. Answering the eval-source question first, on the legacy human model (days), while the corrected human model is
   finished separately (weeks)?
4. White activation needs targeted local SF18 checks at gate-adjacent children first (F5). OK to make that a
   requirement?
