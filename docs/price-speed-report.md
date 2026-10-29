# Price search speed: the compiled engine

Brief: `docs/handoff_price_speed.md`. Branch `claude/jolly-shannon-8orkcb` (off `main` at 56f92b6).
All of this ran on synthetic graphs (`_test_budget_dag.big_dag`) in a cloud container: 4 cores,
Python 3.11 venv from `requirements.txt` (duckdb 1.5.6, numpy 2.4.6), single-threaded. No pools,
eval arrays or books were touched. The real-graph check is for the owner's PC (last section).

## Verdict

- **Same books, bit for bit.** With the production flags, every booked move, float, sweep
  entry, polish statistic and dict insertion order is identical to the old code. No tolerance
  is needed anywhere: no summation order changed. The suite pins this
  (`_test_budget_speed.py`), and so do the scale runs' fingerprints at 10k, 100k and 1M nodes.
- **Speed with the production flags** (`--price-rounds 1 --price-select best --price-reprice
  max --price-seed source --price-dag-mode polish`): **3.2x at 1M nodes** (50.0 → 15.4 s at target
  400, 91.7 → 28.0 s at 2,000) and 3.0–4.3x at 100k. With `--price-rounds 4` it is **5.3–5.8x
  at 1M** (180.6 → 31.1 s, 267.0 → 50.1 s) and 5.5–6.2x at 100k. By part, at 1M:
  - the solves are 8–9x faster;
  - polish is 3.3–3.8x faster;
  - ordering the graph is 1.2–1.6x faster, its time now including the one-off compile.
- **The time was mostly polish, not the bisection** (profile below). The owner's own figures
  point the same way. Four rounds add 66–109 solves for ten-odd minutes, so a solve costs
  roughly 5–9 s at 4.7M nodes, and the 11 solves of a one-round build are 1–2 minutes of the 65.
  The rest is polish (the build log's `dag polish:` line gives seconds per start) or happens
  before the search (graph build, stop model). The new `price search time` log line splits
  it on the next run.
- **No numba or Cython**: pure numpy gets the solves to 8–12x, and the remaining time is
  Python-level polish logic, which a JIT would not reach without a rewrite.

## 1. Profile: where the time went

Production flags, the old code, `big_dag` graphs. Times are seconds, single-threaded.

| nodes | target | total | price_book solves | topo_up | polish | of which the region evaluator |
|---|---|---|---|---|---|---|
| 10k | 400 | 1.5 | 0.3 (30 solves) | 0.1 | 1.1 | 0.7 (406 calls) |
| 100k | 400 | 6.0 | 2.5 (16) | 0.7 | 2.7 | 0.7 (308) |
| 100k | 2,000 | 19.7 | 5.2 (30) | 0.8 | 13.7 | 9.9 (654) |
| 1M | 400 | 52.3 | 25.8 (15) | 9.3 | 17.0 | 1.0 (441) |
| 1M | 2,000 | 97.9 | 18.7 (11) | 10.8 | 68.2 | 27.1 (1,697) |

(10k at 2,000 takes 0.1 s: the graph holds only about 1,000 positions worth booking.)

- **Solves** (`price_book`): O(graph) dict walks, 1.2–1.7 s each at 1M nodes. They are
  19–49% of the time, and more with `--price-rounds 4` (about 40–120 solves).
- **Polish** is 33–73%. Inside one 1M/2,000 polish start (64 s under cProfile):
  - the region evaluator `_book_region`: 27.6 s, 857 calls. It is a full re-evaluation per
    verified proposal, about 4.5k nodes each.
  - line tables: 23.9 s. That is 228k top-level `line()` calls rebuilt every step for every
    frontier position, plus 248k book-free `line_free` tables (the cone exploration).
  - `_static_value`: 4.4 s for 1.3M calls of a pure function.
  - `_cheapest`: 2.6 s for 617k calls.
- **topo_up**: about 10 s at 1M. Its `_adjacency` also builds a parents map that topo_up never
  reads.
- **Cyclic GC**: a full collection rescans every container in the process. The graph is
  2.5M of them at 1M nodes, so a collection takes about 3 s, and the search triggers several.
- **Fixed per-solve cost.** Every `price_book` call builds V, T, choice and R dicts over the
  whole graph, and every `polish_book` and `book_result` call rebuilds a `{hash: position}`
  dict of the whole graph.

## 2. What changed

### `python/budget_compiled.py`: a `CompiledGraph`, built once per search

Node index = position in `topo_up`'s order. Our-node candidates and opponent replies are
CSR arrays. An edge whose value is fixed reads a constant slot past N in the value arrays,
so every edge is one gather. Fixed means either out of the subgraph (its leaf value) or a
cut back edge (the child's static value, from the same Python expression). A node's level
is one more than its highest forward child's. Within each level, nodes are sorted by
degree, so column j of the jagged layout touches exactly the first `cnt_j` nodes.

- `price_book` → `solve` + `reach`:
  - **Bottom-up, level by level.** An opponent node's value is the same left fold
    `((const_base + p0*v0) + p1*v1) + ...`: one IEEE multiply and one add per edge, column by
    column. numpy does not fuse or reassociate them.
  - **Candidate choice.** An our node keeps the first candidate with the strictly greatest
    key, as `key > best[0]` does. The key includes the stickiness bonus and respects
    `fixed_policy`.
  - **Top-down reach** sums each node's incoming reach in the dict walk's order: parents by
    descending order position, each parent's edges in list order. Contributions through
    cut back edges come after, from a short Python loop.
  - **The result dict** is materialised from the reached nodes only. It has the same keys,
    floats and insertion orders, `reach_policy`'s included.
- `policy_reach` and `policy_value` (with `detail`) run on the same sweeps. `played` is a
  frontier BFS (it follows cut back edges, as the DFS does).
- Graphs the arrays cannot hold compile with `ok=False`, and every call falls back to the
  dict code. That means an in-subgraph child that is not a node, on which the dict walk
  raises `KeyError`, or a root outside the graph.

### `topo_up`: the same Tarjan on integer ids, about 2x faster

The roots come in the same set-iteration order and the children in the same list order, so
the order and the cut set are identical. The old function is kept as `_topo_up_dict`.

### `polish_book(compiled=...)`: the same steps, faster

- **Region evaluator on indices** (`budget_compiled.region`): the same heap traversal, sums
  and dict orders, using a per-node adjacency cache built on first touch.
- **Exact incremental verification** (`quick`). A proposal changes a few moves. The verifier:
  1. re-derives membership below the changed moves (old and new edges, parents first);
  2. re-values children-first, with the same expressions, only the nodes whose inputs
     changed;
  3. stops wherever a recomputed value is bitwise unchanged.

  Every other node keeps the region's floats, which a full pass would recompute
  identically.
- **Incremental rebuild of the accepted region** (`advance`). It re-sums reach for the nodes
  below the change, from the same parents in the same order. It rebuilds the dicts in the
  evaluator's insertion orders: W in discovery order (the first parent popped, then its
  edge order), T ascending, booked and child in pop order.
- **Line tables are kept across steps** and reused while every input they read is
  bitwise unchanged. Those inputs are the children's region values, which replies are
  booked, and the free-line memo entries. Each step stamps the positions whose value,
  membership or booked status changed, taken from `advance`'s delta.
- **Free-line tables are shared between the polish starts of one search.**
  - **Reusing a longer table is exact.** A table built for more moves serves fewer: entries
    `j <= K` of a K'-table equal the K-table's, values, moves and aliasing. The max-plus
    fills entry j only from pairs `j1 + j2 = j`, visited in the same order. The "at most j"
    pass reads only entries `<= j`, and the depth cap never binds.
  - **The floor is handled by an interval.** Each entry carries the interval of floors that
    make the same `reach >= floor` decisions over its cone, and serves another start only
    inside that interval.
  - **Gain.** At 1M/400 the second and third starts drop from about 2.8 s to under 0.5 s.
- **Smaller exact skips.**
  - `_static_value` is memoised.
  - The max-plus loop skips iterations that cannot update anything (`j1 + j2 > K`).
  - `_cheapest`'s plain-prefix case memoises the same `sum`/`tuple` per length.
  - polish clears its memos on return (`line_free`'s self-reference would otherwise hold
    them until a GC pass).

### `price_search(engine=..., compiled=...)`

- `engine='auto'` (the default) compiles graphs of at least `PRICE_COMPILED_MIN_NODES`
  (5,000) nodes. Below that it uses the dict walks, because a compiled solve carries about
  0.4 ms of numpy overhead; that is 30x a dict solve on the tests' 20-node graphs and breaks
  even near 3k nodes.
- `'compiled'` forces the arrays and `'legacy'` is the old code throughout, `_topo_up_dict`
  included.
- `compiled=` passes a graph already built, shared across budgets.
- The cyclic GC is paused for the search (restored after, also on error), except under
  `'legacy'`.
- `info['timing']` and a new log line split the time into order/compile, solves and the DAG
  mode.

### `build_budget_books.py`

- **`--price-engine {auto,compiled,legacy}`** (default auto) is recorded in the meta as
  `price_engine`.
- **One compile per run.** The graph is compiled once, after the stop model, and the source
  seed is computed once. Both are shared by every `--budget`. The old code ran `topo_up` and
  a dict reach walk for the seed at every budget, then `topo_up` again inside the search.
- **`--evaluate-after`** reuses the compiled graph.

### Not done, and why

- **No warm-started bisection or reused lambda brackets.** They would change the sweep,
  and the gate is the same sweep.
- **No numba.** It would mean a new compiled dependency on the owner's Windows box (numba's
  numpy-version support lags), for a part that is no longer the bottleneck.
- **No single pass for topo_up and compile.** Merging them would save topo_up's own
  adjacency pass, about 1 s per run at 1M nodes. The rest of the 7 s is the compile's read
  of every graph object, which is memory-bound, plus its numpy sorts. That is worth doing
  only if the real-graph timing line shows order/compile is large.

## 3. Exactness evidence

`python/_test_budget_speed.py` (in `run_tests.py`; 93–110 s here):

| check | what | result |
|---|---|---|
| T | `topo_up` = `_topo_up_dict` (order and cut) on every graph of both test files plus 300 random graphs full of repetition cycles | 789 graphs (274 with cycles cut), 0 differ |
| K | `price_book(compiled=)` = the dict walk on every key, over 7 lambdas × optimistic/seeded reach × with/without `fixed_policy` × with/without an incumbent bonus | 27,384 solves, 0 differ |
| K | `policy_reach`, `policy_value(detail=True)` on random books (non-candidate moves and outside positions included) | 1,956 books, 0 differ |
| S | `price_search` compiled = legacy (result dict, sweep, bound, polish stats) for **every graph and budget** of `_test_budget_dag.py` (300-DAG report, checks E, N/P, hand cases) and `_test_budget_price.py` under the production polish build, and under all 7 flag sets those tests use (defaults; rounds 3 + reprice max; production rounds 4 with dag_mode none / fixpoint / polish / fixpoint+polish) on every price-test graph, E/N/P, the hand cases and the first 60 report DAGs | 4,393 + 1,477 searches, 0 differ |
| P | `polish_book(compiled=)` = `polish_book()` from the price book and from nothing, with and without `fixed_policy` | 873 polishes, 0 differ |
| F | a dangling child compiles `ok=False`; the calls are the dict code's (same `KeyError`) | pass |
| G | the GC state is restored after the search, also after it raises | pass |

During S and P, `budget_compiled.CHECK_ADVANCE` re-evaluates every incrementally rebuilt
polish region in full and fails on any difference: dicts and their order, parents lists,
floats, the change stamps. Comparisons are bitwise (0.0 ≠ −0.0) with dicts compared in
order. `--full` runs all seven flag sets on every report DAG (10,591 searches, 0 differ,
about 5 minutes).

Beyond the suite (scratch runs, all 0 differences):

| run | searches |
|---|---|
| random DAGs at three transposition rates, all flag sets and `fixed_policy` | about 9,800 |
| `big_dag` graphs of 3k–30k nodes (rounds 1, rounds 4, fixpoint+polish) | 384 |
| the build_budget_books sequence (one compile, one seed, several budgets, an incumbent) at 6k and 40k nodes | 12 |
| the scale runs below (fingerprints at 10k, 100k, 1M; one and four rounds) | 10 |

A summation-order change would have shown up as a float difference in the solves' values or
reach, and none occurred.

## 4. Scaling

`python/_test_budget_speed.py --scale 10000,100000,1000000 400,2000 [--rounds N]`. Each
(engine, size) runs in its own process. Peak = the process's max RSS; the column in brackets
is the peak after building the graph and seed, before the search.

Production flags, **one round** (the 65-minute build):

| nodes | target | old s | new s | × | solves | same | old: order / solves / polish | new: order+compile / solves / polish | peak GB old / new (graph alone) |
|---|---|---|---|---|---|---|---|---|---|
| 10k | 400 | 1.4 | 0.5 | 2.7 | 30 | yes | 0.0 / 0.3 / 1.0 | 0.1 / 0.1 / 0.4 | 0.08 / 0.08 (0.08) |
| 10k | 2,000 | 0.1 | 0.1 | — | 2 | yes | 0.1 / 0.0 / 0.1 | 0.0 / 0.0 / 0.0 | 0.08 / 0.08 (0.08) |
| 100k | 400 | 5.4 | 1.8 | 3.0 | 16 | yes | 0.6 / 2.2 / 2.6 | 0.6 / 0.2 / 1.0 | 0.22 / 0.22 (0.22) |
| 100k | 2,000 | 17.3 | 4.0 | 4.3 | 30 | yes | 0.9 / 5.2 / 11.2 | 0.6 / 0.5 / 3.0 | 0.22 / 0.22 (0.22) |
| 1M | 400 | 50.0 | 15.4 | 3.2 | 15 | yes | 9.5 / 23.8 / 16.7 | 7.6 / 2.6 / 5.1 | 1.50 / 1.46 (1.46) |
| 1M | 2,000 | 91.7 | 28.0 | 3.3 | 11 | yes | 10.6 / 17.9 / 63.2 | 6.8 / 2.0 / 19.1 | 1.50 / 1.46 (1.46) |

The same with **four rounds** (the 75-minute build):

| nodes | target | old s | new s | × | solves | same | old: order / solves / polish | new: order+compile / solves / polish | peak GB old / new (graph alone) |
|---|---|---|---|---|---|---|---|---|---|
| 100k | 400 | 13.1 | 2.4 | 5.5 | 64 | yes | 0.6 / 9.9 / 2.6 | 0.6 / 0.8 / 1.0 | 0.22 / 0.22 (0.22) |
| 100k | 2,000 | 32.6 | 5.3 | 6.2 | 120 | yes | 0.9 / 20.1 / 11.6 | 0.6 / 2.0 / 2.6 | 0.23 / 0.25 (0.22) |
| 1M | 400 | 180.6 | 31.1 | 5.8 | 105 | yes | 9.0 / 156.2 / 15.2 | 7.4 / 18.9 / 4.6 | 1.50 / 1.46 (1.46) |
| 1M | 2,000 | 267.0 | 50.1 | 5.3 | 101 | yes | 10.1 / 157.9 / 98.7 | 6.4 / 17.4 / 26.0 | 1.52 / 1.46 (1.46) |

"same" means the two engines' fingerprints match: booked moves in order, value bits, sweep
bits, visited order, realized bits and polish stats.

- **The trend holds across scales.** The speed-up rises from 10k to 100k and holds from
  100k to 1M. Per solve it is 10–12x at 100k and 8–9x at 1M (cache effects). Polish
  gains 2.6–4.5x at 100k and 3.3–3.8x at 1M.
- **The compile is paid once per search here** (each row is one search). In
  `build_budget_books` it is paid once per run, for every budget.
- **What's left.** At 1M the compile is about half of a target-400 search. Polish is two
  thirds of a target-2,000 one, mostly the first start's cone exploration and the per-step
  proposal bookkeeping, both Python.
- **4.7M nodes, from these rates.** A compiled solve should take about 1 s, against the
  roughly 5–9 s the owner's figures imply for the old one. The compile should take 35 s or
  more (real graphs have more edges per node than these), once per run.

Peak memory: the compiled search never raised the process peak above the old one. At 1M
nodes it peaked at 1.46 GB against 1.50–1.52 GB, where building the graph alone peaked at
1.46 GB. The worst case was 100k/2,000 with four rounds: 0.25 against 0.23 GB (+20 MB).

Measured directly with tracemalloc at 1M nodes (2.28M edges):
- the `CompiledGraph` holds 252 MB, and compiling it peaks at 436 MB;
- a compiled solve allocates 63–79 MB transiently, against 123–145 MB for a dict solve;
- the `{hash: position}` dict that the old polish and `book_result` rebuilt on every call is
  70 MB.

## 5. For the owner: the real graphs

1. **Rebuild the White b2000 book twice from this checkout and diff at tolerance 0.** Use the
   usual command once with `--price-engine legacy` and once without (auto), into different
   `--out-prefix`es, for example `..._legacy` and `..._new`. Then:

   ```powershell
   .venv\Scripts\python.exe -c "import json,polars as pl,sys; a,b=sys.argv[1:3]; print('parquet equal:', pl.read_parquet(a).equals(pl.read_parquet(b))); ja,jb=(json.load(open(x+'.meta.json')) for x in (a,b)); drop=lambda m:{k:v for k,v in m.items() if k not in ('built','peak_commit_gb','price_engine','price')}; print('meta equal (bar time/engine):', drop(ja)==drop(jb)); pa,pb=ja['price'],jb['price']; print('sweep equal:', pa['sweep']==pb['sweep'], ' value equal:', pa['value']==pb['value'])" E:\chess\repertoire\_budget\X_legacy_b2000.parquet E:\chess\repertoire\_budget\X_new_b2000.parquet
   ```

   Expect `True` three times. `pl.DataFrame.equals` compares row order and values exactly.
   If the polish stats in `price.polish` differ in anything but `seconds`, the engines
   diverged.
2. **Read the new timing line** (`price search time (compiled): order …, solves …, dag mode …`)
   and the per-start polish seconds. It says what the 65 minutes are now.
   - **If the DAG mode is still most of it**, the next lever is polish's per-step
     bookkeeping and the first start's cone exploration. Both scale with the frontier, which
     real opponent nodes, with 10+ replies above the share floor, make far larger than the
     synthetic ones.
   - **If order/compile is large**, merge topo_up's and compile's passes (section 2).
3. **Check `peak_commit_gb` in both metas.** The compiled graph holds integer and float
   arrays over every node and edge, plus one `{hash: position}` dict (which polish and
   `book_result` used to rebuild per call). In exchange, the dict solves' per-call V/T/R
   dicts are gone. Here the compiled search's peak never exceeded the legacy one (section 4).
   At 4.7M nodes, scaling those sizes (about 77 bytes held per node or edge, 133 at
   the compile peak) with about 4 edges per node gives roughly 2 GB held and a 3.3 GB compile
   peak. The White build peaks at about 51 GB against a commit limit of about 136 GB. In
   exchange, a dict solve's own transient (about 0.7 GB at that size) and polish's per-start
   position dicts are gone.
4. **If polish ever raises `AssertionError: polish_book: incremental verification …`**, the
   incremental evaluator and a full rebuild disagreed on that graph. Rerun with
   `--price-engine legacy` (the old code, unchanged) and keep the graph inputs: that is a
   bug to report, not a result to trust.

Tests: `python run_tests.py`: **69/69 pass** here, `_test_budget_speed.py` included. The
data-dependent tests skip without local data:

- `_test_docs_paths` (drive entries)
- `_test_epd_max_ply`, `_test_epd_memo`, `_test_extract_accounting`,
  `_test_extract_child_hash`, `_test_extract_equivalence`, `_test_zobrist_incremental`
  (source parquets)
- `_test_eval_arrays` (eval arrays)
- `_test_merge_child_hash` (pooled stats)
- `_test_replay_holdout_guard` (the F: tree)
- `_test_rust_extract`, `_test_verify_book`, `_test_verify_evals` (the Rust exe)
- `_test_stage3_load_evals` (the real eval DB)
- `_test_trainer_m5` (the training pack)

No budget test skips. requirements.txt still pins `duckdb<2`.
