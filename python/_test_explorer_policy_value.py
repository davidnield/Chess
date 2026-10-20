"""explorer_policy_value.py on a hand-built explorer dir (2026-10-20).

Two months of ps rows plus a term table over a small tree, values worked by hand:

  R (start) -e4-> A (10: 5W 2D 3B)          R -d4-> X (5, split over both months)
  A -e5-> B (6: 3W 1D 2B)   A -c5-> C (3: 1W 1D 1B)   ENDED at A: 1 (1W)
  B -Nf3+-> D (4: 3W 0D 1B) B -Bc4-> E (2: 0W 1D 1B)
  D -Nc6-> F (2: 2W)        D -d6-> G (1: 0W 0D 1B)   HORIZON at D: 1 (1W)
  F -Bc4-> H (2: 2W)

  A  White book {R: e4, B: Nf3, F: Bb5}: Bb5 was never played (F's mean, 1.0), so
     V(D) = (2*1 + 0 + 1)/4 = 0.75 = V(B); V(A) = (6*0.75 + 1.5 + 1)/10 = 0.70.
     The book's "Nf3" matches the explorer's "Nf3+".
  B  its exits: left 0.30 + 0.15, ended 0.10, cap 0.15, unplayed 0.30 (sum 1),
     and 1.6 book moves played per game (R, then B in 60% of games)
  C  Black book {A: e5}: V(B) = (1 + 1.5)/6 from Black's side, V(R) =
     (10 * V(B) + 2.5)/15 with d4's games summed over the two months
  D  a White book without a first move has no value
  E  --shrink 4: B (Nf3 played by 4 of 6) -> 0.5*0.75 + 0.5*3.5/6; A = (6*V(B) + 2.5)/10;
     R (e4 by 10 of 15) -> 10/14*V(A) + 4/14*8.5/15
  F  --months 2026_8 reads August alone: d4 has 3 games, V(R) = (10*V(B) + 1.5)/13
"""
from __future__ import annotations

import math
import sys
import tempfile
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import explorer_policy_value as epv  # noqa: E402

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    if not ok:
        FAIL += 1


R = epv.ROOT_HASH
A, B, C, D, E, F, G, H, X = (1000 + i for i in range(9))


def ps_rows(rows):
    return pl.DataFrame(
        [{"parent_hash": p, "move_san": s, "event": "Blitz", "elo_band": 1800,
          "parent_epd": "x", "child_hash": c, "child_eval": None, "ply": 1,
          "white_wins": w, "draws": d, "black_wins": b, "total": w + d + b}
         for p, s, c, w, d, b in rows],
        schema={"parent_hash": pl.Int64, "move_san": pl.Utf8, "event": pl.Utf8,
                "elo_band": pl.Int32, "parent_epd": pl.Utf8, "child_hash": pl.Int64,
                "child_eval": pl.Int16, "ply": pl.Int32, "white_wins": pl.Int64,
                "draws": pl.Int64, "black_wins": pl.Int64, "total": pl.Int64})


def build(root: Path) -> None:
    m8 = root / "month=2026_8" / "bkt=0"
    m9 = root / "month=2026_9" / "bkt=1"
    for d in (m8, m9, root / "_term"):
        d.mkdir(parents=True)
    ps_rows([(R, "e4", A, 5, 2, 3), (R, "d4", X, 1, 1, 1),
             (A, "e5", B, 3, 1, 2), (A, "c5", C, 1, 1, 1),
             (B, "Nf3+", D, 3, 0, 1), (B, "Bc4", E, 0, 1, 1),
             (D, "Nc6", F, 2, 0, 0), (D, "d6", G, 0, 0, 1),
             (F, "Bc4", H, 2, 0, 0)]).write_parquet(m8 / "part-0000.parquet")
    ps_rows([(R, "d4", X, 1, 0, 1)]).write_parquet(m9 / "part-0000.parquet")
    pl.DataFrame({"position_hash": [A, D], "kind": [epv.TERM_ENDED, epv.TERM_HORIZON],
                  "reason": [0, 0], "white_wins": [1, 1], "draws": [0, 0],
                  "black_wins": [0, 0], "total": [1, 1]},
                 schema={"position_hash": pl.Int64, "kind": pl.Int32, "reason": pl.Int32,
                         "white_wins": pl.Int64, "draws": pl.Int64, "black_wins": pl.Int64,
                         "total": pl.Int64}
                 ).write_parquet(root / "_term" / "year=2026_month=8.term.parquet")


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        build(root)
        white = {R: "e4", B: "Nf3", F: "Bb5"}
        black = {A: "e5"}
        nofirst = {B: "Nf3"}
        H = epv.HoldoutIndex(root, [white, black, nofirst], threads=2, mem_gb=1,
                             log=lambda *_: None)
        w = epv.policy_value(white, "white", H)
        check("A  White book value 0.70 (unplayed Bb5 at F's mean, Nf3 = Nf3+)",
              w is not None and abs(w["value"] - 0.70) < 1e-12, f"{w and w['value']}")
        want = {"mass_left": 0.45, "mass_ended": 0.10, "mass_cap": 0.15,
                "mass_unplayed": 0.30, "mass_cycle": 0.0}
        ok = w is not None and all(abs(w[k] - v) < 1e-12 for k, v in want.items()) \
            and abs(w["moves_played"] - 1.6) < 1e-12 and math.isfinite(w["se"]) and w["se"] > 0
        check("B  exits by kind sum to 1; 1.6 book moves played per game", ok,
              f"{w and {k: round(w[k], 4) for k in want}}, played {w and w['moves_played']}")
        b = epv.policy_value(black, "black", H)
        vb = 2.5 / 6
        check("C  Black book value, d4 summed over both months",
              b is not None and abs(b["value"] - (10 * vb + 2.5) / 15) < 1e-12,
              f"{b and b['value']} vs {(10 * vb + 2.5) / 15}")
        check("D  a White book without a first move has no value",
              epv.policy_value(nofirst, "white", H) is None)
        s4 = epv.policy_value(white, "white", H, shrink=4)
        vb = 0.5 * 0.75 + 0.5 * 3.5 / 6
        va = (6 * vb + 2.5) / 10
        want = 10 / 14 * va + 4 / 14 * 8.5 / 15
        check("E  --shrink pulls thin book moves toward the node mean",
              s4 is not None and abs(s4["value"] - want) < 1e-12
              and abs(sum(s4[f"mass_{k}"] for k in epv.KINDS) - 1) < 1e-12,
              f"{s4 and s4['value']} vs {want}")
        H8 = epv.HoldoutIndex(root, [black], threads=2, mem_gb=1, log=lambda *_: None,
                              months=["2026_8"])
        b8 = epv.policy_value(black, "black", H8)
        check("F  --months reads one month", b8 is not None
              and abs(b8["value"] - (10 * 2.5 / 6 + 1.5) / 13) < 1e-12, f"{b8 and b8['value']}")
    print("ALL PASS" if FAIL == 0 else f"{FAIL} FAILED")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
