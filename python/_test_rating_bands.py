"""rating_bands reproduces lila-openingexplorer's RatingGroup exactly.

The value of this module is fidelity to someone else's source, so the test is
written as a transcription check, not a sanity check: every boundary is pinned
on BOTH sides, and the two upstream quirks are asserted as behaviour rather than
described in a comment where they can rot.

If lichess changes select_avg, this test is what should fail -- so the expected
values below are written as literals, deliberately not derived from the same
tables the implementation uses.

Run: .venv/Scripts/python.exe python/_test_rating_bands.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from rating_bands import (LICHESS_BOUNDS, LICHESS_FILTER_BOUNDS,
                          lichess_group_expr, lichess_rating_group,
                          mean_elo_floored)

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def main() -> int:
    print("=" * 70)
    print("rating_bands == lila-openingexplorer RatingGroup::select_avg")
    print("=" * 70)

    # Every boundary, both sides. Transcribed from src/model/lichess.rs.
    edges = [
        (999, 0), (1000, 1000),
        (1199, 1000), (1200, 1200),
        (1399, 1200), (1400, 1400),
        (1599, 1400), (1600, 1600),
        (1799, 1600), (1800, 1800),
        (1999, 1800), (2000, 2000),
        (2199, 2000), (2200, 2200),
        (2499, 2200), (2500, 2500),
        (2799, 2500), (2800, 2500),   # collapsed: see quirk (b)
    ]
    bad = [(a, want, lichess_rating_group(a)) for a, want in edges
           if lichess_rating_group(a) != want]
    check(not bad, f"{len(edges)} boundary points map correctly (collapsed)")
    for a, want, got in bad[:6]:
        print(f"        avg={a} want={want} got={got}")

    # Quirk (a): 2800 is never produced, in either mode.
    produced = {lichess_rating_group(a, collapse_top=False)
                for a in range(0, 4001)}
    check(2800 not in produced,
          "Group2800 is unreachable (select_avg jumps 2500 -> 3200)")
    check(2800 not in LICHESS_BOUNDS, "LICHESS_BOUNDS omits the dead 2800 group")

    # Quirk (b): uncollapsed keeps 3200 for >= 2800, collapsed folds >= 2500.
    check(lichess_rating_group(2800, collapse_top=False) == 3200
          and lichess_rating_group(2799, collapse_top=False) == 2500,
          "uncollapsed: >=2800 -> 3200, 2500..2799 -> 2500")
    check(all(lichess_rating_group(a) == 2500 for a in (2500, 2799, 2800, 3999)),
          "collapsed: everything >=2500 -> 2500 (the explorer's filter)")

    check(LICHESS_FILTER_BOUNDS ==
          (0, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2500),
          "nine user-visible filter groups")

    # midpoint is a FLOOR, not a round -- the one place we differ from our own
    # mean_elo column, and only visible on odd sums.
    check(mean_elo_floored(1799, 1800) == 1799 and mean_elo_floored(1800, 1801) == 1800,
          "mean_elo_floored floors odd sums (1799/1800 -> 1799, not 1800)")
    check(lichess_rating_group(mean_elo_floored(1799, 1800)) == 1600
          and lichess_rating_group(round((1799 + 1800) / 2)) == 1800,
          "the floor-vs-round difference can cross a boundary (1600 vs 1800)")

    # The vectorised form must agree with the scalar one everywhere, not just
    # at the points a human thought to check.
    rng = list(range(0, 4001))
    for collapse in (True, False):
        got = (pl.DataFrame({"mean_elo": rng})
               .select(lichess_group_expr(collapse_top=collapse))
               ["rating_group"].to_list())
        want = [lichess_rating_group(a, collapse_top=collapse) for a in rng]
        n_bad = sum(1 for g, w in zip(got, want) if g != w)
        check(n_bad == 0,
              f"polars expr == scalar over 0..4000 (collapse_top={collapse})")

    # Our pool floor sits exactly on a group edge, so no band straddles it.
    check(lichess_rating_group(1800) == 1800 and lichess_rating_group(1799) == 1600,
          "mean_elo>=1800 pool floor lands exactly on a Lichess boundary")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print()
    print(f"{'ALL PASS' if not n_fail else f'{n_fail} FAILED'} "
          f"({len(_checks)} checks)")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
