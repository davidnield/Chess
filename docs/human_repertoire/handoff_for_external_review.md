# Chess opening repertoire builder — project hand-off

I'm looking for outside ideas on this project. Below is a complete description:
what it does, the model it optimises, what's been measured, and the problems I
haven't solved. Sections 7 and 8 are where I most want fresh thinking.

Everything here is measured unless marked as an assumption. Numbers are from the
current build.

---

## 1. What it is

An empirical chess opening repertoire builder. It ingests the Lichess monthly
game dumps, extracts every position-move edge from the games, aggregates them
into per-position move statistics, then runs backwards induction over the
position graph to pick one move at every position I can reach.

The product is a **memorisation-efficient "sharp" repertoire** for ≥1800 pooled
Blitz/Rapid/Classical: reach a winning position as early as possible without
being outright losing against best play, with a Stockfish eval overlay gating
out lines that only score well because opponents keep falling for them.

Scale of the current build:

| quantity | value |
|---|---|
| raw Lichess PGN ingested | 4.8 TB |
| after cleaning + recompression | 1.03 TB |
| rated games used | 1.33 billion (≥1800 average rating, Blitz/Rapid/Classical, 2013–present) |
| position-move edges | 31.27 M |
| distinct positions | 17.79 M |
| Stockfish eval DB | 400.0 M positions |
| booked moves in the unbounded repertoire | ~6.98 M (White), ~7.05 M (Black) |

It's a solo hobby project on one Windows box. Python 3.11 (uv-managed venv),
DuckDB + Polars + PyArrow + python-chess + NumPy. No cluster, no cloud.

---

## 2. Data pipeline

```
Lichess monthly PGN dumps
   │  process_pgn_parquets.py     clean + compress to Hive-partitioned parquet
   ▼                              ("recipe v3": zstd-19, strips [%clk]/[%eval])
source parquets (year=/month=/event=)
   │  build_pooled_stats.py --phase extract   filter by elo, replay once,
   │                                          emit pre-aggregated partials
   │  build_pooled_stats.py --phase merge     two-phase external GROUP BY
   ▼
position_stats_*.parquet        per-edge win/draw/loss counts
position_stats_aux_*.parquet    per-position: the mass those edges CAN'T see
crush_hist_*.parquet            "winning position reached early" histogram
   │  build_sharp_reps.py        two-pass driver around the engine below
   │    └─ stage3_backwards_induction.py
   ▼
repertoire_{white,black}_sharp.parquet
   │
   ├─ score_repertoire.py         effectiveness / soundness / coverage scorecard
   ├─ replay_holdout.py           out-of-sample replay against held-out games
   ├─ repertoire_explorer.py      browse the tree
   └─ trainer_app/                local web app: FSRS drills, deviation review
```

Every position is keyed on an **incremental Zobrist hash** (`zobrist.py`),
differentially tested against python-chess over millions of plies. The graph is
a DAG, not a tree — transpositions converge, which is load-bearing for both the
valuation (a position is valued once) and the budget work (section 4).

The `aux` sidecar matters more than it looks: some games record no reply at all
at a given position because the game *ended* there (resignation, mate, clock).
Dropping those would quietly delete exactly the games where the opponent
collapsed, which is the opposite of noise. They're retained as per-position
"unseen mass" and folded back in at each step.

---

## 3. The valuation model

Scores are expected points in [0,1] (win 1, draw 0.5, loss 0), always from the
perspective of the side whose book is being built.

**Backwards induction in topological order (Kahn's algorithm on the DAG):**

- **Our turn:** take the max. We choose, so the position is worth our best option.
- **Their turn:** average over their *empirical* move distribution, weighted by
  observed frequency. We don't choose, and this is a repertoire for playing
  humans, so it deliberately does *not* assume they find the strongest reply.

Because opponent nodes use the empirical distribution rather than optimal play,
White- and Black-perspective values do **not** sum to 1. Each is "expected score
if I play optimally and my opponent plays like a typical player at this rating".

**Two values propagate per node:**

- `value` (mean) — vs the average empirical opponent. The primary objective.
- `value_robust` — along the opponent's *best* reply (the critical line). Drives
  the refutation gate.

**Leaf values** use a Beta-Binomial posterior mean rather than the raw empirical
score:

```
smoothed = (k · mu_slice + score_avg · n) / (k + n)      k = 500 (--prior-strength)
```

`mu_slice` is the slice's start-position white score (measured 0.5183 on the
2019–25 pool). Leaves optionally blend a Stockfish eval at `--eval-weight 0.5`,
with centipawns → expected score via Lichess's own accuracy model.

**Selection at our turn** maximises a key that is *not* the propagated value:

```
score = sign · value + crush_weight        · crush_rate
                     + decisiveness_weight · (1 − draw_rate)
                     + error_weight        · opponent_error
                     + forcing_weight      · forcingness
                     + cover_weight        · coverage_efficiency
                     − memo_weight         · memorisation_cost
```

Note the split: selection is by *key*, propagation is of *value*. The extra
terms shape which move is chosen without contaminating the value that flows
upward.

The terms:

- **`crush_rate`** — the primary sharpness driver. Fraction of games through the
  edge where our side reaches a decisive advantage early (mate/resignation, or
  eval ≥ +300cp achieved). Empirical-Bayes shrunk toward the slice-mean crush
  with pseudocount 20,000 games, so thin edges can't manufacture a crush bonus.
- **`coverage_efficiency`** — covered opponent-decision *depth* (reach-weighted)
  per memorised prepared *branch*. Rewards lines that keep opponent mass on
  prepared rails with few things to learn; penalises fan-out.
- **`memorisation_cost`** — propagated value-at-stake-if-you-forget.
- `decisiveness`, `forcingness` — legacy knobs, largely superseded by crush.

**Two gates** restrict eligibility (locked recipe in `build_sharp_reps.py`):

- **Absolute / refutation gate** — `--robustness-floor 0.1 --gate-metric eval`.
  A move is eligible only if its value along the opponent's best defence stays
  within 0.1 expected-score of the slice prior. Drops lines refuted by best
  defence (1...g5) while keeping ones that hold (Blackmar-Diemer).
- **Relative gate** — `--gate-rel-floor 0.1 --gate-rel-baseline own-eval
  --gate-rel-own-margin 0.02`. Rejects a candidate conceding >0.1 expected-score
  versus the position's own eval, even when it clears the absolute floor. This
  stops the book choosing a move that drops us from objectively winning to dead
  equal purely because humans misplay the sequel.

If *every* candidate is gated, the gate is dropped for that node (sparse-tail
fallback) — otherwise thin deep nodes would empty out.

> **Discrepancy to flag:** the repo README describes the two gates as defaulting
> to "-1.0 for the side to move", which reads as pawns. No such flag or default
> exists in the code; the locked recipe is 0.1/0.1 in *expected-score* units
> (≈110cp near equality). The README prose needs reconciling with
> `build_sharp_reps.py`.

---

## 4. The budget extension (what I'm working on now)

**The problem.** The model above books a move at every reachable gated node —
about 7 million per colour. Nobody learns 7 million moves. Every learning cost
already in the model (`memo_cost`, crush γ, learnability δ) is **reach-weighted**:
it prices *expected recall*, so a rare deep trap is nearly free. A hard budget of
moves-to-learn is a **storage** cost — each learned move counts once regardless
of how often you reach it. The two diverge exactly on rare deep lines.

The naive fix is to build the full book and truncate by reach. But that finds the
best 20 moves *of a book designed for someone who knows all 7 million*. If you
only have room for 20, you might want different openings entirely — ones that pay
off immediately, not the first 20 moves of a system whose point arrives on move 15.

Worked example (illustrative numbers):

```
  Solid  →  0.54, and knowing more barely helps
  Trap   →  0.47 if you stop there
            0.72 if you also know the next three moves
```

At budget 1, Solid wins. At budget 4, Trap wins. Truncating the 4-move Trap book
to 1 move gives 0.47 — the worst of the three. **The budget has to be inside the
selection, not applied afterwards.**

**A measured finding that motivated this.** `--memo-weight` is documented as "the
Lagrange multiplier of a memo budget". It isn't one, on the size axis. Sweeping it
0 → 2.0 leaves the footprint *exactly* invariant (6,977,911 White / 7,048,109
Black at every weight, over the identical position set) while `best_move` differs
at 13.84% of nodes. It's a **vocabulary** regulariser — it changes *which* move is
learned, never *how many* — because Stage 3 books every gated node unconditionally.
There is no "stop here" option in its our-node branch.

**The method.** Replace each node's scalar value with a **value-vs-budget curve**,
built bottom-up in the same topological style:

```
our node u:       V_b(u) = max( L_node(u),  max_m V_{child_m}(b−1) )
opponent node v:  V_b(v) = max over allocations Σb_i = b of Σ_i p_i · V_{b_i}(child_i)
                           + constant terms (below-floor replies, aux buckets)
```

`L_node(u)` is the "stop here and wing it" value — play on unprepared, at the
population's move mix. `V_b = max(L_node, …)` keeps curves monotone, so booking
a move never loses value.

A curve is stored as `(base, atoms)` where atoms are `(cost, gain)` increments
sorted by density `gain/cost` non-increasing — a **concave step hull**.

- The our-node `max` **convexifies** adjacent increasing-density increments into
  single atoms. That merge *is* chain compression: a trap prefix (tiny gains)
  fuses with its payoff step into one cost-k atom whose density is the trap's
  true amortised rate. This is what lets the planner "see" a trap worth entering.
- The opponent-node allocation is a greedy density merge across children — the
  exact sup-convolution for concave inputs (standard exchange argument).

Budget unit: **one per our-turn decision** — one position where you have to
remember what to play, one flashcard. Opponent moves are free.

**Four arms are compared**, in increasing order of what they're allowed to choose:

| arm | what it may choose | role |
|---|---|---|
| `trunc` | nothing — source book's move everywhere, cut by compound reach | the existing method; the floor |
| `greedy` | myopic one-step gains, no chain atoms | can't see a trap two moves deep |
| `fixdp` | optimal *stopping* only — source's move everywhere, DP picks where to stop | isolates "spend the budget better" |
| `dp` | joint stopping **and** policy | `dp − fixdp` is the novel claim |

---

## 5. How claims get validated

A four-tier ladder, cheapest first; a change may not reach a tier without passing
the ones below it.

| tier | cost | what only this tier catches |
|---|---|---|
| 1 · `run_tests.py` (37 files) | seconds | logic, contracts, schema drift |
| 2 · real CLI over one slice | minutes | real movetext, real schemas, A/B equivalence |
| 3 · single-month prototype | hours | accounting identities, peak-RAM and wall-clock projections |
| 4 · full build | days | the answer itself — needs explicit go-ahead |

Two rules that make it load-bearing:

- **A new flag defaults to off and must be an exact no-op** — rebuild without it
  and diff at tolerance 0.
- **Measure across scales, never at a single slice.** One-slice numbers have
  flipped a verdict here twice: the same change measured +19%, then +34.6%, then
  +13.1% depending on which slice was used.

**Out-of-sample scoring** is `replay_holdout.py`: replay held-out games (2025)
against a book in lockstep and record how each game left it. Exit reasons:

| exit | meaning |
|---|---|
| `our_deviation` | the player played something other than the book move (99.87% of exits) |
| `opp_out_of_book` | opponent left the prepared set — a *coverage* exit |
| `book_end` | ran out of book — a *budget* exit |
| `game_end` | the game itself ended inside the book |

---

## 6. What's been measured

**In-sample, small-budget ladder (budgets 1–20, ≤2024 pool):** `dp` beats `fixdp`
at every budget, +0.0077 at b=1 widening to +0.0234 at b=20. So joint
stopping-plus-policy does beat pure optimal stopping. This has **not yet been
confirmed out-of-sample** — that run is in flight.

**Book size saturates.** Asking for 1000 decisions returns **817**, at ε=0.0025
over a subgraph of 2.15 M nodes / 880 K our-nodes, peak 27.65 GB. Marginal gain
appears to vanish somewhere near 800 decisions. I don't yet know whether that's
genuine gain exhaustion, the reach floor, or the share floor binding — and it
matters, because the plan's 4,000 and 40,000-move rungs may be meaningless.

**`game_end` is mostly signal, not noise.** I initially treated it as
contamination and dropped it. That was wrong. Measured composition: only ply 1–2
is majority-clock (53.1%); ply 3+ is dominated by resignations and mates scoring
at or above pool, and the deep remainder scores **0.8145** — the highest-scoring
population in the entire replay, and precisely the crush payoff the repertoire
exists to produce. Now split by (reason, depth, termination class) instead.

**A parity artifact drove an apparent colour gap.** An abandonment before White's
first move is a White loss and a Black win regardless of position. That single
effect accounts for essentially the whole gap between White's raw Δ (+0.015) and
Black's (+0.145).

**A selection confound in holdout deltas.** Across sweep configs, White's
correlation between "faithful%" (share of the replay that stayed in book) and raw
delta is **+0.899**. Removing `game_end` exits drops it to **−0.095**. So a
config can look good largely by keeping games in book longer, independent of move
quality.

**`cover_eff` has a thin-data bias.** It's `covered_depth / (1 + branches)` over
*recorded* replies. The pool drops edges under 50 games and survivors renormalise
to 100%, so a thin node reads as a narrow efficient rail: one surviving 55-game
reply scores 0.500 while a well-sampled 90/10 node scores 0.333. It structurally
rewards rarity.

**A known value bug, not yet fixed.** Backward induction currently deletes games
that ended in resignation/mate at a node (~7% of mass), and those games score
**0.90** versus 0.51 for the rest. Net effect: winning lines are understated by
about **0.027**. Fixing it needs a re-extract; the intermediate partials are gone.

---

## 7. Open problems — where I'd most like ideas

**7.1 The concave-hull fusion gap is the sole source of DP suboptimality.**

Measured on 2,400 (graph, budget) instances from 400 random DAGs, against a
brute-force optimum:

| curves | n | suboptimal | mean gap | worst |
|---|---|---|---|---|
| **with a fused (cost>1) atom** | 969 | **70 (7.2%)** | +0.0020 | **+0.114** |
| with none | 1431 | **0 (0.0%)** | 0 | 0 |

Perfect separation. Chain compression buys the ability to see traps, and pays for
it with a hull that under-states what a book is worth — so the *allocation* is
chosen against understated curves. The worst case (+0.114) and even the mean
(+0.002) are the same order as the effect being measured (+0.0077 … +0.0234).

My candidate fix: retain the pre-fusion monotone point set alongside the hull, use
it for `eval(b)` so the value is exact, and keep the hull only for the
density-ordered allocation at opponent nodes. **Is there a better formulation?**
This is tree/DAG knapsack with a concavity relaxation, and I suspect there's
established work I'm reinventing badly. Lagrangian relaxation with a bisected
multiplier? Exact DP over a bounded budget axis, given budgets ≤ ~1000?

**7.2 Transposition cost is charged per path, and I don't know how to fix it
properly.**

The same position reachable by two move orders is charged twice even though you'd
learn it once. Transposition share is 7–13% of nodes, so this isn't negligible.
Current mitigation is a bisection (`--match-distinct`) that grows the path-charged
budget until the *distinct* count hits the target — a hack, and I found the
distinct count isn't even monotone in the charged budget (one run bracketed 30 and
32 while 31 booked fewer than 20).

The honest version is: select a budget-limited set of nodes on a DAG where cost is
counted once per node but value flows along paths. That feels genuinely hard —
possibly NP-hard. **Is there a tractable exact formulation, or a principled
approximation with a bound?**

**7.3 Does the budget actually change the openings, or just the depth?**

The whole thesis is that a 20-move book should be *different openings*, not the
same tree cut shorter. I have in-sample evidence (`dp` > `fixdp`) but I haven't
yet characterised *how* the books differ qualitatively. **What's the right way to
measure "these are different repertoires" rather than "these are the same
repertoire at different depths"?**

**7.4 The out-of-sample comparison is confounded and I'm not sure it's fixable.**

Given 7's `faithful%` confound: comparing two books on held-out games rewards the
book that keeps games in book longer, which is partly a coverage property rather
than a quality one. Per-book calibration is meaningful; cross-book calibration
isn't. **Is there a cleaner estimator?** Something like an inverse-propensity or
doubly-robust correction over exit reasons?

**7.5 Is "crush" the right sharpness objective at all?**

Currently: fraction of games reaching a decisive advantage early. It's a
result-based proxy that conflates "the position is winning" with "this opponent
population collapses here". The eval overlay exists to stop the second from
dominating, but the two are entangled by construction. **Is there a better
formalisation of "sharp but sound"?**

**7.6 Why does the curve saturate at ~817 decisions?**

See section 6. If genuine, it's a substantive result about opening preparation —
there may be a real ceiling on how much memorisation helps, and locating it would
be more interesting than the budget machinery itself. If it's an artifact of the
reach floor, the whole large-budget arm of the project needs re-scoping.

---

## 8. Things I might be wrong about

Worth challenging directly:

- **The opponent model.** Empirical move frequencies, pooled over ≥1800. No
  opponent-strength conditioning, no adaptation, no assumption they've prepared
  against me. Reasonable for the use case, but it means the book is optimised
  against the *average* opponent and I have no measure of variance across
  opponents.
- **Pooling all of 2013–2025.** Opening fashion moves. A 2014 game and a 2025 game
  count equally. There's no recency weighting anywhere.
- **One rating band.** "≥1800 average" pools a 1800 and a 2600 player together.
- **Score, not win rate.** Draws count 0.5, so a solid drawish line and a
  wild 50/50 line score identically. The `decisiveness` and `crush` terms are
  meant to break that tie, but the base objective doesn't.
- **The eval overlay uses cloud/fishnet Stockfish evals of varying depth.** Parent
  and child positions may be evaluated at different depths, which is why a 0.02
  "own-margin" slack exists in the relative gate. That slack is a fudge factor.

---

## 9. Repo map

Working directory: `C:\Users\David\Documents\Chess` (Windows, git, branch
`budget-books`).

| path | what |
|---|---|
| **`CLAUDE.md`** | **the technical companion — invariants, contracts, data-path table, and hard-won failure modes. Read this first.** |
| `README.md` | public front door; the conceptual explanation |
| `python/zobrist.py` | the position hash every stage keys on |
| `python/process_pgn_parquets.py` | ingest + compression (recipe v3) |
| `python/build_pooled_stats.py` | canonical extract + aggregate |
| `python/winpos_fused.py` | "winning position reached early", fused into the replay |
| `python/eval_arrays.py` | eval DB as mmap'd arrays workers share |
| `python/stage3_backwards_induction.py` | **the valuation engine** — header docstring is the authoritative spec |
| `python/build_sharp_reps.py` | **the locked recipe**, as a two-pass driver |
| `python/build_baseline_books.py` | reach-pruned books — the truncation baseline |
| `python/budget_core.py` | **the value-vs-budget curve library** — header explains the method |
| `python/build_budget_books.py` | the budget CLI |
| `python/replay_holdout.py` | out-of-sample lockstep replay |
| `python/score_repertoire.py` | effectiveness / soundness / coverage scorecard |
| `python/plan_consistency_report.py` | how habitual the ideas are across variations |
| `python/repertoire_explorer.py` | browsing UI |
| `python/trainer_app/` | local web app: FSRS drills, deviation review |
| `python/run_tests.py`, `python/_test_*.py` | 37 standalone pass/fail test scripts |
| `scratch/` | gitignored: probes, drivers for completed runs, analysis one-offs |
| `archive/` | the original R implementation this replaced |

Conventions worth knowing:

- **Every script's header docstring is the authoritative CLI.** Remembered flags
  are not trusted.
- Tests import the production logic rather than reimplementing it.
- Everything is resumable: atomic writes (`.tmp` then rename), existence skip
  gates, checkpointed expensive intermediates. Long runs are *assumed* to die.

Data lives on local drives, not in the repo (several TB). `CLAUDE.md` has the
full path table.

---

## 10. Machine and operational constraints

One Windows 11 box. These shape what's feasible more than anything else:

- **The ceiling is commit charge, not RAM** — ~214 GB commit limit. A 3-worker
  extract holds ~111 GB of it; anything else must be budgeted against what's
  *left*.
- **Stage 3 peaks at ~64.7 GB**; the budget DP at ~27.65 GB.
- **Windows native-allocator degradation** — long-lived processes doing repeated
  multi-GB alloc/free cycles either segfault (PyArrow, `0xC0000005`, no traceback)
  or silently decay 3–5× in throughput (DuckDB, consecutive large GROUP BYs, no
  error). Mitigations: periodic pool release; per-task process isolation.
- **Polars `sink_parquet()` on >10 GB inputs silently produces no output file**,
  no exception. `collect(streaming=True)` crashes on >10 GB.
- **A DuckDB memory_limit set too *high* can hang the box** — Windows trims the
  working set in favour of file cache and the process crawls on page faults with
  plenty of free RAM. The fix is a *lower* limit.
- Processes attached to a session die when it ends. Long runs go through a
  detached `.bat` logging to file.

Full runs take **days**. That's why the validation ladder exists — a wrong
`child_hash` corrupts every downstream join and nothing fails loudly.
