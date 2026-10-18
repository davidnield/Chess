"""weak_stop.py and budget_core.apply_stop_values (2026-10-18).

FIXTURE (white book; evals white-perspective, stubbed):

  x0 (our turn)  weak pool: m1 -> A 80 games, m2 -> B 20      main pool: 50 / 50
  A  (their turn) main pool: r1 -> A1 (eval 0.6), r2 -> A2 (eval 0.4)  -> R1(A) = 0.5
  B  (their turn) main pool: r3 -> B1 (eval 0.8)                       -> R1(B) = 0.8
  x1 (our turn)  weak pool: 5 games only -> falls back to the main pool (m1/m2 50/50)
  x2 (our turn)  no moves anywhere, eval 0.7 -> the engine alone
  x3 (our turn)  nothing at all -> no value
  y0 (their turn) main pool: s -> x0 (100%)

  A  W(x0) = 0.8*0.5 + 0.2*0.8 = 0.56 (weak frequencies, not the pool's 0.65)
  B  W(x1) = 0.65 via the main pool's frequencies (fallback counted)
  C  W(x2) = 0.7 (engine only); x3 has no entry
  D  R(y0) = W(x0)
  E  black perspective flips every value (1 - white score)
  F  apply_stop_values rewrites the stop value, the leaf after our last move, a
     reply leaving the subgraph and a sub-floor reply's share of const_base, and
     re-applies the aux crack blend; positions with no value keep the default.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
from stage3_backwards_induction import aux_our_blend  # noqa: E402
from weak_stop import MISSING, EvalSource, weak_stop_values  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


X0, X1, X2, X3, A, B, A1, A2, B1, Y0 = range(3001, 3011)
WEAK = [(X0, "m1", A, 80), (X0, "m2", B, 20), (X1, "m1", A, 3), (X1, "m2", B, 2)]
MAIN = [(X0, "m1", A, 50), (X0, "m2", B, 50), (X1, "m1", A, 50), (X1, "m2", B, 50),
        (A, "r1", A1, 100), (A, "r2", A2, 100), (B, "r3", B1, 100), (Y0, "s", X0, 100)]
WHITE_ES = {A1: 0.6, A2: 0.4, B1: 0.8, X2: 0.7, A: 0.5, B: 0.8, X0: 0.55, X1: 0.55}


def cp_of(es: float) -> int:
    from stage3_backwards_induction import LICHESS_CP_SCALE
    return int(round(np.log(es / (1 - es)) / LICHESS_CP_SCALE))


def lookup(keys):
    return np.array([cp_of(WHITE_ES[k]) if k in WHITE_ES else MISSING
                     for k in np.asarray(keys).tolist()], dtype=np.int64)


def write(path: Path, rows) -> None:
    pl.DataFrame({"parent_hash": [r[0] for r in rows], "move_san": [r[1] for r in rows],
                  "child_hash": [r[2] for r in rows], "total": [r[3] for r in rows]}
                 ).write_parquet(path)


def near(a, b, tol=2e-3) -> bool:
    return a is not None and abs(a - b) < tol


def main() -> int:
    here = Path(__file__).parent
    wp, mp = here / "_tmp_weak_pool.parquet", here / "_tmp_main_pool.parquet"
    write(wp, WEAK)
    write(mp, MAIN)
    try:
        con = duckdb.connect()
        W, R, c = weak_stop_values(con, [X0, X1, X2, X3], [Y0], pool=mp.as_posix(),
                                   weak_pool=wp.as_posix(),
                                   evals=EvalSource(True, lookup), log=lambda m: None)
        Wb, Rb, _ = weak_stop_values(con, [X0], [Y0], pool=mp.as_posix(),
                                     weak_pool=wp.as_posix(),
                                     evals=EvalSource(False, lookup), log=lambda m: None)
        con.close()
    finally:
        wp.unlink(missing_ok=True)
        mp.unlink(missing_ok=True)

    check("A: W(x0) uses the weak frequencies (0.56)", near(W.get(X0), 0.56),
          f"{W.get(X0)}")
    check("B: W(x1) falls back to the main pool's frequencies (0.65)",
          near(W.get(X1), 0.65) and c["x_fallback_pool"] >= 1, f"{W.get(X1)}, {dict(c)}")
    check("C: W(x2) is the engine alone; x3 has no value",
          near(W.get(X2), 0.7) and X3 not in W, f"{W.get(X2)}, x3 {X3 in W}")
    check("D: R(y0) = W(x0)", near(R.get(Y0), W.get(X0) or -1), f"{R.get(Y0)}")
    check("E: black flips: W(x0) = 1 - 0.56", near(Wb.get(X0), 0.44), f"{Wb.get(X0)}")

    # F: a hand-built graph
    g = bc.Graph(root=1, our={}, opp={}, epd={}, ply={}, reach={})
    g.our[1] = bc.OurNode(l_node=0.60, cands=[("a", 2, True, 0.5), ("b", 9, False, 0.51)])
    g.our[5] = bc.OurNode(l_node=0.58, crack=(90.0, 10.0))
    g.opp[2] = bc.OppNode(const_base=0.1 * 0.40, kids=[(0.5, 5, True, 0.6),
                                                      (0.4, 6, False, 0.62)],
                          floor=[(0.1, 7, 0.40)])
    Wg = {1: 0.55, 5: 0.50, 6: 0.57, 7: 0.30}
    Rg = {9: 0.48}
    cnt = bc.apply_stop_values(g, Wg, Rg)
    check("F: stop value replaced", g.our[1].l_node == 0.55)
    check("F: crack blend re-applied to the new stop value",
          abs(g.our[5].l_node - aux_our_blend(0.50, 90.0, 10.0, 1.0)) < 1e-12,
          f"{g.our[5].l_node}")
    check("F: leaf after our last move uses R; in-subgraph candidate untouched",
          g.our[1].cands == [("a", 2, True, 0.5), ("b", 9, False, 0.48)])
    check("F: a reply leaving the subgraph uses W; an in-subgraph one is untouched",
          g.opp[2].kids == [(0.5, 5, True, 0.6), (0.4, 6, False, 0.57)])
    check("F: a sub-floor reply's share of const_base is revalued",
          abs(g.opp[2].const_base - 0.1 * 0.30) < 1e-12, f"{g.opp[2].const_base}")
    check("F: counts", cnt == {"our_l": 2, "cand_leaf": 1, "opp_leaf": 1, "opp_floor": 1},
          f"{cnt}")
    xs, ys = bc.stop_model_positions(g)
    check("F: stop_model_positions finds every out-of-book position",
          xs == {1, 5, 6, 7} and ys == {9}, f"{xs}, {ys}")

    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
