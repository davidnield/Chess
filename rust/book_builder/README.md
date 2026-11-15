# book_builder: the budget-book builder's core in Rust

Phase 3 of `docs/builder-port-and-128bit-plan-2026-11-12.md` (local, untracked). The Python
builder (`python/budget_core.py`, `budget_compiled.py`, `build_budget_books.py`) stays the
reference. This crate replaces it stage by stage, and each stage must give the same floats.
Since 2026-11-12 `build_budget_books.py` runs only on this crate (no Python fallback); the
Python stages stay as the tests' reference.

## Build

```powershell
pwsh rust/book_builder/build.ps1          # release build, installs python/book_builder_rs.pyd
pwsh rust/book_builder/build.ps1 -Test    # also cargo test and python/_test_book_builder_rs.py
```

- **Build output:** goes to `D:\rust-target\book_builder`, because C: is nearly full.
- **The module is not committed:** `python/*.pyd` is gitignored. The differential test skips
  when the module is missing, as `_test_rust_extract.py` does without the explorer exe.
- **Linking:** PyO3 0.30 builds against the stable ABI from Python 3.11 on (`abi3-py311`).
  `cargo test` builds without the `extension-module` feature, so it links libpython, and
  `build.ps1` puts the interpreter's directory on PATH for that.

## Exactness contract

### Production engine gates

`python/build_budget_books.py` defaults to the expected-score policy recorded in the [lifetime plan](../../docs/lifetime-repertoire-plan.md#engine-gate-decision-november-14-2026). `--robustness-floor` sets the allowed deficit below the construction prior from our side. `--max-score-loss` sets a relative tolerance on the 0–1 scale. For example, `0.095` means 9.5 percentage points. The Rust collector and final menu use the parent and eligible trusted siblings as references. Rare unverified moves can enter provisionally and trigger verification. A missing evaluation or a failed gate excludes an inherited move as well. If every move fails, the book ends there. Opponent replies retain their empirical weights.

`run_rare_book.py` takes the non-gate recipe from the 60 cp template by default (`--template-cap` selects another). Verified overlays can be reused because they store the parent and child cp, which are converted separately for each new policy. The book metadata records `max_score_loss`, `robustness_floor`, `slice_prior`, and `absolute_score_floor`. For historical builds, `--max-eval-loss CP` and the driver's `--cap CP` select the centipawn rule. The builder's `--legacy-gates` reproduces the historical uncapped rule.

### Numerical parity

Every exported function equals the Python it names, bit for bit. The test
`python/_test_book_builder_rs.py` compares floats by their bits, so NaN, -0.0 and the last ulp
all count. These rules keep that true:

- **Operations:** use the same IEEE operations in the same order, and never rearrange
  algebraically.
- **Sums:** Python 3.11's `sum()` adds left to right from integer 0, which a running
  `acc += x` from 0.0 reproduces. numpy's `cumsum` is a sequential fold as well.
  - Python 3.12 changed float `sum()` to compensated summation. Moving the venv to 3.12
    changes the reference, and this crate with it.
- **Transcendentals:** `exp` and `ln` come from the same C runtime that CPython uses on this
  machine (MSVC UCRT).
  - `cp_to_expected_score` is checked on every int16 against both `math.exp` and the polars
    expression the eval loaders use.
  - A different platform or C runtime must re-run that check before it trusts a book.
- **Missing values:** a Python `None` evaluation is NaN here; a real evaluation is never NaN.
- **0/0:** where Python would raise `ZeroDivisionError`, the port refuses the input. One case
  is prior strength 0 with a zero-game edge. It must not return NaN instead.
- **Orders:** every dict comes back in Python's insertion order. Python's float comparisons
  inside tuples and heaps are IEEE (`-0.0 == 0.0`, then the next field decides), and keys
  compare as Python ints.
  - CPython's set iteration order is not reimplemented. `budget_rs.set_order` rebuilds
    `list(set(g.our) | set(g.opp))` from the two key lists, and `topo_up`'s Tarjan roots and
    the DP's queues start in it.
- **Identity:** polish tests Python object identity (`b is prev`, `sub[j] is not sub[j - 1]`).
  Line-table entries are `Rc`s compared with `Rc::ptr_eq`.

## Ported so far

Moves are interned as integer codes. For graph assembly they are assigned in sorted SAN order,
so comparing codes orders moves the way Python's string comparison does. Each move also has a
separate code without its check suffix (its bare code).

| Rust (`src/`) | Python reference |
|---|---|
| `value.rs` | `cp_to_expected_score`, `ws_to_cp`, `smoothed_score`, `effective_eval_weight`, `leaf_edge_value`, `leaf_node_value`, `aux_row_buckets`, `aux_opp_mix` (horizon mode `empirical`) |
| `gates.rs` | `passes_abs_gate`, `rel_gate_keep`, `eval_loss_keep`, `gate_candidates`, `final_candidates` (candidate cap 0). python-chess still resolves the source move's child in Python; only its eval enters here. |
| `reach.rs` | `optimistic_reach` and `route_plies`. The heap pops by (reach, push counter), exactly as `heapq` does. |
| `graph.rs` | `build_graph`, into compressed sparse rows |
| `core.rs` | the graph after assembly (`RsGraph`): `topo_up` (Tarjan and the cycle cut), `topo_down`, `stop_model_positions`, `apply_stop_values`, `_static_value` |
| `dp.rs` | the curve engine: `Curve`, `curve_from_points`, `cap_curve`, `merge_weighted`, `build_curves`, `extract_book`, `match_distinct` |
| `polish.rs` | `_book_region`, `book_result`, `polish_book`: the dict path's semantics, stickiness included, and the compiled path's loop bounds, held to both |
| `price.rs` | `price_book`, `policy_reach`, `price_search` (one pricing round, the selection rule, polish from three starts) |
| `collect.rs` | `collect_subgraph_edges`' walk and `GateAdmit`, with an opponent prior's reach shares when one is given. DuckDB still scans the pool. The gates' eval cache follows `GateAdmit`: a hash is known once a level's prefetch has looked it up, or once the engine candidates gave it a fresh eval. |

| `rollout.rs` | `convertibility_eval.rollouts`' games, for the referee and the stop/other samplers (always; `rollouts_py` is the Python reference). This one is NOT held to Python bit for bit (user, 2026-11-12: sampler work need only replicate the tool conceptually). Still, `_test_convertibility_eval.py` R2 gets identical games on the referee's fixtures. On real data, 500 positions of Black's stop dump gave the Python sampler's table bit for bit, in 63 s against Python's 606 s (225 s with a warm Maia cache). |

The Python side lives in `python/budget_rs.py`. The columnar path (`frame_inputs`, `run_csr`,
`GraphView`, `rs_graph_from_out`) builds the graph's input arrays straight from the collector's
frame. The graph then lives in Rust, and no `edges_by_parent`, eval, aux or source-row dict is
built. The sampled stop and other tables run on the `RsGraph` (`stop_dump`, `add_const_base`,
`terminal_outcomes`). The opponent prior (`--opponent-prior`) reaches the walk as each row's
`reach_share` and the graph as the frame's `reply_mass` / `additional_reply` columns;
`--dump-engine-targets` reads the graph view. Every builder option now runs here.

## Checks

The synthetic checks in `python/_test_book_builder_rs.py`:

| check | on | result |
|---|---|---|
| G | 216,000 node solves | equal; with `CP_EPS` planted as 0, 561 differ |
| R | 800 graphs | equal; with the heap's tie-break reversed, 381 differ |
| B | 1,000 assembled graphs | equal |
| D | 436 graphs from the DP, price and speed tests, plus cyclic graphs, big DAGs and assembled graphs (4,604 extractions, 136 with cycles cut, 174 sticky) | equal; with extraction's `>` planted as `>=`, 348 extractions and 923 matches differ |
| P | 2,820 polishes (183 trimmed, 915 improved, 438 sticky), 2,382 also on Python's compiled path | equal; with rebook's length limit tightened, 111 differ |
| S | 4,740 price searches on 494 graphs | equal |
| C | 277 graphs, half with opponent-prior rows: columnar inputs, graph view, the `RsGraph` from the Rust output | equal |
| T | 120 graphs: the stop and other dumps, the other table's application, finished-game outcomes | equal |
| W | 100 random pool files (65 with an opponent prior): the collector's frame, its input arrays, the engine-target rows | equal |

Two planted errors went undetected, because the random graphs never tie exactly:
- a hull merge on equal densities;
- the "at most j moves" pass aliasing on equal values.

Real graphs, assembly only (`D:/chess/scope-checks/rs_graph_realcheck.py`; both identical):

| graph | nodes | Python `build_graph` | Rust | converting in / out |
|---|---|---|---|---|
| Black 0.30, floor-5 rare round 0 | 1.79M | 96.6 s | 16.6 s | 31.9 s / 10.4 s |
| White 0.30, full-menu round 0 | 3.00M | 232.3 s | 43.2 s | 89.2 s / 26.6 s |

The conversions are the dict path's. The columnar path replaces both.

The collector, Python against Rust on the same inputs (`D:/chess/scope-checks/rs_collect_realcheck.py`;
the edge frames identical at tolerance 0; times with other checks running):

| recipe | edges | Python | Rust |
|---|---|---|---|
| Black 0.30 floor-5 rare round 0 | 6.48M | 617 s | 466 s |
| the same, round 1 (engine candidates) | 6.48M | 595 s | 407 s |
| White 0.30 full-menu round 0 | 19.76M | 1,019 s | 648 s |

The rest is DuckDB rescanning the pool at every level.

Whole books with `--rs-core` (`D:/chess/scope-checks/run_rs_core_check.bat`, against the
Python builder's books): Black 0.30 DP, Black 0.30 price and Black 0.30 floor-5 rare round 0
are identical, `rare.json` and meta included (timings aside). The columnar path, end to end
at epsilon 0.01 with engine candidates (`run_rs_small.bat`), is identical for DP and price.

All of it at full size (`run_rs_core_check2.bat`: the columnar path, the Rust collector,
everything after; quiet machine). Six books are identical to the Python builder's, `rare.json`
and meta included (timings aside). Python's times are from the simplify check and the profile.

| book | edges | Python: time, peak commit | `--rs-core`: time, peak commit |
|---|---|---|---|
| Black 0.30 DP | 4.27M | 628 s, 24.1 GB | 332 s, 10.1 GB |
| Black 0.30 price | 4.27M | | 352 s, 8.8 GB |
| Black 0.30 floor-5 rare round 0 | 6.48M | | 434 s, 10.5 GB |
| the same, round 1 (engine candidates) | 6.48M | | 476 s, 10.2 GB |
| Black 0.60 floor-5 rare round 0 | 15.48M | 3,834 s, 63.7 GB (profiled) | 790 s, 24.9 GB |
| White 0.30 full-menu rare round 0 | 19.76M | 1,607 s, 67.8 GB | 1,024 s, 25.7 GB |

What is left is mostly the collector's DuckDB scans (122-437 s) and the weak-player stop
model's DuckDB joins.
