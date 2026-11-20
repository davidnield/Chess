# Review of Codex's November 14–19 work

*Claude, 19 November 2026. Responds to `docs/repertoire-review-handoff-2026-11-19.md`.*

**Scope.** I reviewed the code on `codex/sweep-preparation-20261118` at `0a3ca66`, concentrating on the
parts that decide which moves a book contains:

- the recipe spec (`python/sweep/spec.py`) and build command (`python/sweep/builder.py`);
- the coordinator (`python/sweep/coordinator.py`);
- the expected-score gates (`python/budget_core.py` and `rust/book_builder/src/gates.rs`);
- quality-closure request generation (`python/sweep/candidates.py`, `quality.py`, `verify.py`,
  and `rare_verify.plan`);
- the opponent ensemble (`python/sweep/policy.py`, `frontier.py`);
- the horizon fix (`rust/book_builder/src/reach.rs`);
- the extension accounting kernel (`python/sweep/extensions.py`).

I did not audit the extraction, compact storage, OTB freeze or eval-DB code beyond reading the
handoff's claims. Those have production acceptance receipts, and I have no reason to doubt them.

Reproduction scripts are in `C:\Users\David\Documents\Chess\scratch\review-20261119\`. They ran
against the live eval arrays and pools with 2 threads and a 4 GB budget, beside the running eval
job.

## Status of the fixes (19 November, evening)

Branch `claude/review-fixes-20261119`; details in `docs/claude-review-fixes-plan-2026-11-19.md`.

| finding | status |
|---|---|
| 1 Quality closure unbounded | Fixed: the book's policy reach, a threshold, ranking, a per-round cap, and accounting for what is left. A real-graph census is still pending. |
| 2 Parent eval empties the menu | Fixed via the new `parent-above-children` request; gate semantics unchanged. |
| 3 Black polish | Fixed: polish and match-inflate 1.5 are back in the recipe (manifest v3). |
| 4 Recipe-to-data binding | Fixed: `sweep.bindings.check_arm` in preflight, the builder's cap refusal for full-key pools, and `build_weak_edges.py` cap and key-width checks. |
| 5 Ensemble constants | Fixed: read from the recipe; identical output at the approved values. |
| 6 Policy edge cases | Fixed: endings-only pseudo-continuation, model-only reply accounting, and a bumped cache identity. |
| 7 Rare reference at unbooked nodes | Fixed via the new `unverified-rare-reference` request. |
| Modeling and decision points | Unchanged; for David. |

## Verdict

**The data side is in good shape.** The archive, eval and OTB-count work looks careful and
well-receipted.

**The construction side is not ready for the Black pilot.** Two of the findings below would stop
the pilot meeting its own acceptance criteria:

1. As written, quality closure would request engine searches at about **two to three million**
   positions in round 0. It cannot close.
2. The new relative gate silently makes some positions unbookable. That happens exactly where the
   engine thinks a strong unplayed move exists, and no request is ever raised for them.

Neither appears in fixture tests, because fixtures are a few dozen positions.

**The experiment framework also doesn't bind recipes to data.** The recipe says "ply 50" or
"BRC2200 control", but nothing checks that the bound files are those. That doesn't block the
pilot, which is bound by hand, but it must be fixed before the matrix.

## Must fix before the Black pilot

### 1. Quality closure requests a search at nearly every graph position (demonstrated, high)

- **Where:**
  - `python/build_budget_books.py:1052-1064` passes **all** own-turn edges of the collected frame
    and the graph's optimistic reach to `sweep.candidates.requests`.
  - `python/sweep/candidates.py:26-45` emits a request for every parent with `reach > 0` that has
    a child without a stored eval (`missing-candidate-evidence`) or near a gate.
  - `python/rare_verify.py:487-493` merges every one of those rows into the verification plan,
    with no reach threshold, ranking or batch limit. The old frontier cap
    (`--frontier-reach 0.002 --frontier 300`) applies only to rare-move disputes.
- **Measured:** on the uncut White own-move table
  (`D:\chess\uncut-own-v1\white\cache\graph.parquet`, a 1-in-997 sample of White-to-move
  parents), **75.6% of parents have at least one observed move whose child has no stored eval**.
  The rate is 83.1% at parents with 1,000+ games, with a mean of 2.2–2.4 such moves per parent.
  The White full-menu build's graph had 3.24 million own nodes, so round 0 would request roughly
  2.4 million parents. At even 3 seconds per 5M-node search, that is months of engine time.
  - Script: `scratch\review-20261119\missing_child_evals.py`.
  - It uses the old eval arrays. The new Cloud/Fishnet DB draws on the same sources, so I expect
    similar coverage. Recheck on the new lookup.
- **Effect:** the pilot either never closes, or someone caps it ad hoc and the cap decides the
  book. Most requested positions are ones the book will never book or reach with meaningful
  probability.
- **Fix:**
  - Make a request **decision-relevant**: the booked prescription; parents whose booking decision
    could change; and children that could plausibly beat the selected or stop value (a missing
    child is unknown, not automatically worth a search).
  - Rank by reach × possible gain and cap each round explicitly, reporting what was left as
    unresolved mass.
  - Treat a missing-eval child as excluded with explicit unknown-mass reporting, rather than as a
    mandatory search.
- **Test:** a real Black graph's round-0 request count and estimated engine time, printed in the
  pilot report before any searching. Require it to be under a stated budget, such as a few
  thousand parents.

### 2. The parent's own stored eval can empty the menu, and nothing asks about it (reproduced, high)

- **Where:**
  - `rust/book_builder/src/gates.rs:313-330` (`score_candidates`): the relative-gate reference
    includes `own` (the parent position's stored eval), and "an empty menu ends the book at this
    node".
  - Python reference: `python/budget_core.py` `score_candidates`, the same logic.
- **Reproduction** (`scratch\review-20261119\repro_parent_ref.py`):
  - White to move, parent stored at +150, recorded children +60 and +40. The own-side score gap
    is 0.633 − 0.555 = 0.078, above the 0.060 stricter cap.
  - The menu with the parent in the reference is `[]`. Without the parent eval it is
    `['Nf3', 'd4']`.
  - `sweep.candidates.requests` returns no request.
- **Why no request:** the inconsistency check (`candidates.py:40`) fires only when the best
  *child* is above the *parent*, never the reverse. The boundary checks don't fire either,
  because every move is well beyond the cap.
- **How often** (`parent_gap_rate.py`; positions with 200+ games in the 1800+ pool, current
  arrays):

  | side to move | parent above best child by > 0.06 | by > 0.095 |
  |---|---|---|
  | White | 0.75% of positions | 0.21% |
  | Black | 0.78% of positions | 0.31% |

  The 99.9th percentile gap is 0.12–0.14. Under the uncut menu more children exist, so the rate
  should be somewhat lower, but the mechanism is the same.
- **Why it matters for the repertoire:** a parent stored well above every recorded child usually
  means the engine's best move was **never played**, which is exactly where a strong move exists.
  The new rule turns those positions into forced stops, and the unplayed move is never nominated,
  because nothing requests the position. (A parent from a different, deeper source is the other
  cause.)
- **Fix:**
  - The parent eval should not be able to remove every recorded move on its own. Either use it as
    a reference only when it agrees with its children, or when the engine has verified it.
  - Whenever parent minus best child exceeds the band, raise a request: a nomination search for
    the engine's best move plus the recorded moves.
  - Make the inconsistency check symmetric.
- **Test:** the reproduction above, in both colours, asserting a non-empty menu or a request, and
  that a nominated move enters after verification.

### 3. The Black solver drops `--polish` from the validated recipe (demonstrated, medium; a decision)

- **Where:** `python/sweep/builder.py:33-54` builds Black as `--method dp --match-distinct` with
  no `--polish`, and `--match-inflate` is now the default 1.6.
- **The validated control** (`D:\chess\rare-verify-v1\black-cp30-o5\round-03\build.receipt.json`)
  used `--method dp --match-distinct --match-inflate 1.5 --polish`. The 12 November "DP stays for
  Black" decision compared price + polish against that recipe.
- **Effect:**
  - Polish is a coordinate ascent that is never below the DP book in-model. Dropping it gives up
    in-model value for free.
  - The pilot would no longer be comparable with B30o5 or with the reasoning behind keeping DP.
- **Fix:** add `--polish` for Black, or record a deliberate decision not to.
- **Test:** the pilot's resolved-settings printout (an acceptance criterion already) must show the
  solver flags, and match a named validated recipe.

### 4. Recipe labels are not bound to the data they name (demonstrated by inspection, high for the matrix, medium for the pilot)

- **Where:**
  - `python/sweep/coordinator.py:49-96` (`preflight`) checks that every bound file is pinned and
    that the arm is in the manifest.
  - It never compares the arm's recipe with what the files are. Nothing in `python/` reads
    `candidate_pool`, `own_policy` or the control policy's name (`grep` finds them only in
    `spec.py`).
  - `build_budget_books.py` never reads the pool's own cap, so `--max-ply 50` over a ply-30 pool
    runs without complaint.
- **Counterexamples:**
  - The horizon-50 arm bound to `brc1800-p30` builds a ply-30 book labelled "horizon 50".
  - Both opponent-policy controls (`brc1800-maia30`, `brc2200-maia30`) bound to the same overlay
    are two identical builds with different names. The recipe dicts differ only in `name`.
  - A weak-player input built by `build_weak_edges.py` sums every ply of the 51-ply archive
    (finding 9 of the last review, still open: the script has no cap, and its default `--book` is
    the deleted E: book). The "1000–1799 own policy at ply 30" would then silently include late
    observations.
- **Fix:**
  - Every pool, overlay and weak table carries machine-readable metadata: population, bands,
    speeds, cap, min_games and key width.
  - `preflight` checks each arm's recipe fields against them: `candidate_pool` against stats,
    `max_ply` against the pool cap, the policy name against the overlay's population, and
    `own_policy` against the weak pool.
  - The builder refuses `--max-ply` above the pool's recorded cap.
- **Test:** each counterexample above as a preflight refusal.

## Fix before the main sweep

### 5. The ensemble's tunable constants are hardcoded (low)

`policy.ensemble` (`python/sweep/policy.py:16-37`) hardcodes 30, 3, 0.10 and 0.01. `POLICY` in
`spec.py` records `empirical_strength`, `maia_strength`, `backup_discount` and `broad_discount`,
and they enter the recipe hash, but they are never passed in (only `otb_strength` is). Today the
values agree. A future recipe change would alter the hash and not the behaviour. **Fix:** pass
them from the recipe.

### 6. Two edge cases in the selective opponent policy (untested boundaries, low)

- **Endings-only positions lose their replies** (`frontier.py:170`). When the 1800+ outcome pool
  has only endings at a position (`observed == 0`, `nonmove > 0`), `move_mass` is 0. Every reply
  then gets zero mass and is dropped, even if broad or OTB players continued. The ensemble's reply
  distribution is discarded at exactly the positions where it is the only data. Use a small
  explicit continuation mass instead, as the zero-zero case already does.
- **Model-only replies are valued at a placeholder.** They carry `white_score_avg = 0.5` with zero
  observations, so their value comes from whatever stop or eval fallback applies. Mostly these are
  moves nobody plays, often blunders. Report the reach of model-only replies whose child has no
  eval. Those are unknowns, not 0.5.

### 7. Rare-move inflation is handled only where the book books the node (untested boundary, medium)

With `--verification-policy quality`, `rare_floor` is 0 (`build_budget_books.py:845`). So every
stored score, including a move played once, can set the relative-gate reference: the 10 November
pathology, where stored scores of rare moves read about 88 cp high on average.

The selected prescription is always verified, and `comparison_set` then re-admits rivals the
fresh reference allows, so booked nodes self-correct. **An unbooked node whose only good rival
was cut by an inflated rare reference is never requested.** That can decide whether the node is
booked at all.

**Fix:** under uncut menus, keep the November 11 "unverified rare moves do not bind" rule. At
minimum, request the node when the reference child has under N games and no verification.

## Modeling and decision points

- **The opponent the book is tuned against is mostly sub-2000 OTB players.**
  - In the ensemble, OTB is the outermost layer with prior strength 10. Wherever OTB has more
    than a few hundred observations (every main line for many moves), the construction policy is
    essentially the OTB under-2000 movers' distribution. The online 1800+/2200+ layers matter
    only where OTB is thin.
  - That may be what David wants for classical OTB, but it is a different population from his
    online Rapid and Blitz opponents, and a weaker one than the 1800+ pools the October books used.
  - The October opponent-strength test found trap books decay against stronger players. The gates
    protect soundness, not robustness to stronger replies.
  - **Recommend:** a pilot sensitivity scoring the Black pilot book under the online
    1800–1999 / 2000–2199 panels as well as OTB, before freezing this as the primary policy.
- **The permissive arm re-admits the Blackmar-Diemer trunk.** 1.d4 d5 2.e4 at −64 cp: S = 0.441
  against the permissive floor of 0.408, and a relative loss of 0.079 against 0.095. David's
  switch rule bars a commonly reached move over the soundness limit. Keep the arm, but state that
  a permissive winner must still pass a fresh best-defence check before it can be chosen.
- **An empty menu now ends the book, instead of keeping the least-bad move.** This means the book
  never prescribes a move below the absolute floor (good). It also means it stops exactly where
  the player most needs help. Report how much reach ends that way. Finding 2 is a separate case,
  where good positions are stopped by mistake.
- **What lookahead off justifies.** With `lookahead_floor = 0`, the defensible claim is: *every
  prescribed move passes the absolute and relative gates on the evidence used (stored, or verified
  at 5M/20M).* It is not a claim about the opponent's best defence after that move.
  - Lookahead remains a sensitivity until its known gaps are closed.
  - The safety check I'd adopt is a **final audit with feedback**: a fresh best-reply search at
    booked positions above a reach threshold, rebuilding when one fails. It is cheap at 2,000
    prescriptions, and it is the check David's switch rule actually asks for.
- **The scale of the program against the effects we've measured.** Every comparison since
  October has moved the referee by .001–.004: the cut against the full menu, the gate limits, the
  sampled stop, and the Black limits. None cleared .005.
  - The matrix is 20 base books, 16 sensitivity arms and 24 extension variants, at roughly 5–12
    machine-days plus closure.
  - Prefer the staged path in the next section. Only expand the matrix where the pilot shows an
    effect bigger than seed and bootstrap noise.

## Decisions I agree with

- **Ply 51 archive and 128-bit keys throughout.** The boundary argument is right and the receipts
  support it.
- **Expected-score gates measured from a fixed baseline,** rather than fixed centipawns. Better
  behaved in winning and losing positions.
- **`min_games = 1` with explicit unknown mass.** Right in principle, but it moves cost into
  verification (finding 1).
- **Position-only Maia for rollouts.** Allie's 10–20× cost is not justified by the evidence so
  far.
- **The OTB ancestry-preserving split and quarantine.** Correct and well verified.
- **Remaining-maximum as the extension default.** The kernel matches the stated rule; I found no
  error in `residual_exposure`. Finite cost remains the open question.
- **The horizon fix.** Shorter arrivals now reopen a node, and the test pins the review's graph.

## Decisions I'd revisit

- **The OTB-dominant construction policy** (above).
- **Dropping polish for Black** (finding 3).
- **Making the matrix a prerequisite for choosing a book** (scale, above).

## Minimal validation plan for the Black pilot

1. **Fix findings 1–3 first.** Add at least a pilot-local check that the bound pool, weak table
   and policy match the recipe (finding 4).
2. **Dry-run graph and request census before any engine work:**
   - node counts and the reach distribution;
   - the round-0 request count by reason, with estimated search time;
   - the count and reach of empty-menu positions, split by cause: absolute floor, parent
     reference, or no eval.
3. **Build once with stored evidence and inspect it:**
   - the top 25 decisions by reach, with PGN routes;
   - every prescription against a rare reference;
   - the root value against the lifetime Black book under the same model.
4. **Close with a capped, ranked request budget.** Report what remains open rather than claiming
   closure.
5. **Final audit:** a fresh 20M best-reply check at the top 200 booked positions by reach, plus
   the gate audit of every prescription.
6. **Score on the new referee against the lifetime Black book** (`lifetime_black_2026-10-24`):
   - same seeds, OTB and online panels, at horizon 50;
   - the game-fold bootstrap on the difference;
   - the switch rule as before (≥ .005, clear of uncertainty, no unsound common move).
7. **Only then build the White counterpart, then the matrix.**

## What the artifacts support, and what still needs a run

| Supported now | Needs a production run or a statistical test |
|---|---|
| Archive, merge and OTB counts are internally consistent (acceptance receipts) | The quality loop closes at real scale |
| Python and Rust builder parity on fixtures and the earlier six real books | The ensemble improves the book against David's actual opponents |
| The horizon and Maia-cache fixes behave on their regression tests | Any arm differs from another by more than noise |
| Gate arithmetic and baselines match the stated design | The lookahead or extension rules change reached risk materially |

