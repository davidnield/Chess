# book_builder: the budget-book builder's core in Rust

Phase 3 of `docs/builder-port-and-128bit-plan-2026-11-12.md` (local, untracked). The Python
builder (`python/budget_core.py`, `budget_compiled.py`, `build_budget_books.py`) stays the
reference. This crate replaces it stage by stage, and each stage must give the same floats.

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

Every exported function equals the Python it names, bit for bit. The test
`python/_test_book_builder_rs.py` compares floats by their bits, so NaN, -0.0 and the last ulp
all count. Five rules keep that true:

- **Operations:** use the same IEEE operations in the same order, and never rearrange
  algebraically.
- **Sums:** Python 3.11's `sum()` adds left to right from integer 0, which a running
  `acc += x` from 0.0 reproduces.
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

## Ported so far

| Rust (`src/`) | Python reference |
|---|---|
| `value.rs` | `cp_to_expected_score`, `ws_to_cp`, `smoothed_score`, `effective_eval_weight`, `leaf_edge_value`, `leaf_node_value`, `aux_row_buckets`, `aux_opp_mix` (horizon mode `empirical`) |
| `gates.rs` | `passes_abs_gate`, `rel_gate_keep`, `eval_loss_keep`, `gate_candidates`, `final_candidates` (candidate cap 0). The source move's child is still resolved in Python with python-chess; only its eval enters here. |

Moves are interned as integer codes, assigned in sorted SAN order, so comparing codes orders
moves the way Python's string comparison does. A separate code identifies each move without
its check suffix.

The gate test has been checked against a deliberately planted error: setting `CP_EPS` to 0
made 561 of 216,000 node solves differ.

**Next:** `optimistic_reach` and `route_plies`, then
`build_graph` itself into compressed sparse rows. Keys are a (hi, lo) pair of 64-bit
integers, with hi 0 on today's data.
