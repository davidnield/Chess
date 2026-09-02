"""The aux sidecar's merge SQL says exactly what it should.

Hand-built partials with known answers, run through the real
merge_position_stats + merge_aux_stats, because the risk here is in the SQL, not
in the Python: a mis-scoped join or a mean taken over the wrong denominator
produces a perfectly well-formed table that is quietly wrong.

Covers the three things that would matter and are easy to get subtly wrong:
  * scope — aux carries a row for every position that is a PARENT in
    position_stats, and for no other. A position whose edges all fell below the
    floor is not a node in Stage 3's graph; an aux row for it could never be read.
  * the bucket's evaluation — games-weighted over the EVAL-COVERED edges only,
    with cp converted to expected score PER EDGE before averaging (the mean of a
    sigmoid is not the sigmoid of a mean), and min/max spanning it.
  * termination reasons — Normal / Time forfeit / other stay separable, and
    HORIZON stays out of all of them.

Run: .venv/Scripts/python.exe python/_test_aux_stats.py
"""
from __future__ import annotations

import math
import shutil
import sys
import tempfile
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import (TERM_ABANDON, TERM_ENDED, TERM_FLAG, TERM_HORIZON,
                                TERM_NORMAL, consolidate_monthly, merge_aux_stats,
                                merge_position_stats)
from stage3_backwards_induction import LICHESS_CP_SCALE

MIN_GAMES = 50
TOL = 1e-12
_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def es(cp: float) -> float:
    return 1.0 / (1.0 + math.exp(-LICHESS_CP_SCALE * cp))


# (parent, san, child, child_eval, w, d, b, total)
PS_ROWS = [
    # P1: two survivors + three below-floor (one with no eval at all)
    (1001, "e4", 2001, 30, 60, 20, 20, 100),
    (1001, "d4", 2002, -10, 25, 15, 20, 60),
    (1001, "a3", 2003, -200, 1, 0, 2, 3),
    (1001, "h3", 2004, 100, 1, 1, 0, 2),
    (1001, "g4", 2005, None, 2, 1, 2, 5),
    # P2: survivor only -> no bucket at all
    (1002, "Nf3", 2006, 50, 40, 10, 30, 80),
    # P3: every edge below the floor -> P3 is NOT a parent in position_stats,
    #     so it must not appear in aux even though it has a bucket and terms.
    (1003, "b4", 2007, 0, 1, 0, 1, 2),
    (1003, "c4", 2008, 0, 1, 0, 0, 1),
]

# (position, kind, reason, w, d, b, total)
TERM_ROWS = [
    (1001, TERM_ENDED, TERM_NORMAL, 5, 1, 1, 7),
    (1001, TERM_ENDED, TERM_FLAG, 0, 0, 3, 3),
    (1001, TERM_ENDED, TERM_ABANDON, 1, 0, 0, 1),
    (1001, TERM_HORIZON, TERM_NORMAL, 6, 2, 3, 11),
    (1001, TERM_HORIZON, TERM_FLAG, 1, 0, 0, 1),
    (1002, TERM_ENDED, TERM_NORMAL, 2, 0, 0, 2),
    (1003, TERM_ENDED, TERM_NORMAL, 9, 9, 9, 27),   # dropped: P3 is not a parent
    (4004, TERM_ENDED, TERM_NORMAL, 4, 0, 0, 4),    # dropped: never a parent
]


def main() -> None:
    print("=" * 70)
    print("AUX SIDECAR MERGE")
    print("=" * 70)
    tmp = Path(tempfile.mkdtemp(prefix="aux_stats_"))
    try:
        pdir = tmp / "partials"
        pdir.mkdir(parents=True)
        stem = "year=2025_month=1_event=Blitz_part-0"
        # The whole fixture sits in ONE (event, elo_band) cell. position-stats is
        # keyed on those two as well now, so a cell-per-row fixture would change
        # what min_games means here; keeping it constant leaves every expected
        # number below exactly as it was when the key was (parent_hash, move_san).
        pl.DataFrame(
            {"parent_hash": [r[0] for r in PS_ROWS],
             "move_san": [r[1] for r in PS_ROWS],
             "event": ["Blitz" for _ in PS_ROWS],
             "elo_band": [1800 for _ in PS_ROWS],
             "parent_epd": [f"epd{r[0]}" for r in PS_ROWS],
             "child_hash": [r[2] for r in PS_ROWS],
             "child_eval": [r[3] for r in PS_ROWS],
             "ply": [1 for _ in PS_ROWS],
             "white_wins": [r[4] for r in PS_ROWS],
             "draws": [r[5] for r in PS_ROWS],
             "black_wins": [r[6] for r in PS_ROWS],
             "total": [r[7] for r in PS_ROWS]},
            schema={"parent_hash": pl.Int64, "move_san": pl.Utf8,
                    "event": pl.Utf8, "elo_band": pl.Int64,
                    "parent_epd": pl.Utf8, "child_hash": pl.Int64,
                    "child_eval": pl.Int32, "ply": pl.Int32,
                    "white_wins": pl.Int64, "draws": pl.Int64,
                    "black_wins": pl.Int64, "total": pl.Int64},
        ).write_parquet(pdir / f"{stem}.ps.parquet")
        pl.DataFrame(
            {"position_hash": [r[0] for r in TERM_ROWS],
             "kind": [r[1] for r in TERM_ROWS],
             "reason": [r[2] for r in TERM_ROWS],
             "white_wins": [r[3] for r in TERM_ROWS],
             "draws": [r[4] for r in TERM_ROWS],
             "black_wins": [r[5] for r in TERM_ROWS],
             "total": [r[6] for r in TERM_ROWS]},
            schema={"position_hash": pl.Int64, "kind": pl.Int32, "reason": pl.Int32,
                    "white_wins": pl.Int64, "draws": pl.Int64,
                    "black_wins": pl.Int64, "total": pl.Int64},
        ).write_parquet(pdir / f"{stem}.term.parquet")

        mdir = consolidate_monthly(pdir, 2, "2GB", tmp, ("ps", "term"))
        ps_out = tmp / "position_stats_pooled_t.parquet"
        aux_out = tmp / "position_stats_aux_pooled_t.parquet"
        merge_position_stats(mdir, pdir, ps_out, MIN_GAMES, 2, "2GB", tmp)
        merge_aux_stats(pdir, ps_out.with_name(ps_out.stem + "_other_buckets"),
                        ps_out, aux_out, 2, "2GB", tmp)

        ps = pl.read_parquet(ps_out)
        aux = pl.read_parquet(aux_out).sort("position_hash")

        check(sorted(ps["parent_hash"].unique().to_list()) == [1001, 1002],
              f"position_stats keeps only >= {MIN_GAMES}-game edges "
              f"(parents {sorted(ps['parent_hash'].unique().to_list())})")
        check(aux["position_hash"].to_list() == [1001, 1002],
              f"aux is scoped to position_stats parents "
              f"(got {aux['position_hash'].to_list()})")

        r = aux.filter(pl.col("position_hash") == 1001).to_dicts()[0]

        check(r["other_total"] == 10 and r["other_edges"] == 3,
              f"bucket collapses the 3 below-floor edges, mass 10 "
              f"(got {r['other_edges']} edges, {r['other_total']})")
        check((r["other_white_wins"], r["other_draws"], r["other_black_wins"])
              == (4, 2, 4),
              f"bucket W/D/B summed: {(r['other_white_wins'], r['other_draws'], r['other_black_wins'])}")

        want_mean = (3 * es(-200) + 2 * es(100)) / 5      # g4 has no eval: excluded
        check(abs(r["other_eval_mean"] - want_mean) < TOL,
              f"eval mean is games-weighted over COVERED edges only "
              f"({r['other_eval_mean']:.12f} vs {want_mean:.12f})")
        naive = (3 * es(-200) + 2 * es(100) + 5 * 0.5) / 10
        check(abs(r["other_eval_mean"] - naive) > 1e-6,
              "uncovered mass is not silently valued at 0.5")
        check(abs(r["other_eval_min"] - es(-200)) < TOL
              and abs(r["other_eval_max"] - es(100)) < TOL,
              f"min/max span the bucket ({r['other_eval_min']:.6f} .. "
              f"{r['other_eval_max']:.6f})")
        check(abs(r["other_eval_cov"] - 0.5) < TOL,
              f"eval coverage is 5/10 (got {r['other_eval_cov']})")
        check(r["other_eval_min"] - TOL <= r["other_eval_mean"]
              <= r["other_eval_max"] + TOL,
              "min <= mean <= max (within float tolerance)")

        check((r["term_normal_total"], r["term_flag_total"], r["term_other_total"])
              == (7, 3, 1),
              f"terminations split by reason: "
              f"{(r['term_normal_total'], r['term_flag_total'], r['term_other_total'])}")
        check(r["horizon_total"] == 12,
              f"horizon aggregates across reasons and stays out of term_* "
              f"(got {r['horizon_total']})")
        check((r["term_normal_white_wins"], r["term_normal_draws"],
               r["term_normal_black_wins"]) == (5, 1, 1),
              "termination W/D/B carried per reason (never imputed from side to move)")

        r2 = aux.filter(pl.col("position_hash") == 1002).to_dicts()[0]
        check(r2["other_total"] == 0 and r2["other_edges"] == 0
              and r2["other_eval_mean"] is None,
              "a position with no below-floor edge gets a zeroed, NULL-eval bucket")
        check(r2["term_normal_total"] == 2 and r2["horizon_total"] == 0,
              "term still recorded where there is no bucket")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
