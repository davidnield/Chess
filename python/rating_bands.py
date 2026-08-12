"""Lichess opening-explorer rating groups, reproduced exactly.

Our derived schema stores `elo_band` as `floor(mean_elo / 100) * 100` -- fixed
100-wide bands of our own invention. Lichess's opening explorer bins the same
quantity differently, and since the whole point of a popularity census is to be
comparable to the explorer people actually use, this module reproduces THEIR
definition rather than ours.

Source: lichess-org/lila-openingexplorer @ master, read 2026-08-12.

  src/model/lichess.rs
      fn select(mover_rating, opponent_rating) -> RatingGroup {
          RatingGroup::select_avg(midpoint(mover_rating, opponent_rating))
      }
  src/util.rs
      pub fn midpoint(a: u16, b: u16) -> u16 { ((a as u32 + b as u32) / 2) as u16 }

So the basis is the FLOORED INTEGER AVERAGE OF BOTH PLAYERS' RATINGS -- the same
quantity as our `mean_elo`, and order-independent (the mover/opponent split does
not survive the average). Only the bucketing differs: theirs is variable width.

Two quirks, both verified in source, both reproduced here rather than tidied
away -- the point is to match their counts, not to improve on them:

  (a) Group2800 IS UNREACHABLE. It is declared in the enum and included in
      RatingGroup::ALL, but select_avg's final `else` returns Group3200 for any
      average >= 2800, so the indexer never writes it. The upstream comment
      "// TODO: Tweak rating groups for better top game selection" sits on that
      enum line. `LICHESS_BOUNDS` therefore omits 2800.

  (b) THE QUERY LAYER COLLAPSES THE TOP END. src/api/query.rs does
      `min(rating_group, RatingGroup::Group2500)` before testing membership, so
      for FILTERING everything >= 2500 is one bucket; 2800/3200 exist only to
      feed top_group() for top-game selection. That is why `collapse_top`
      defaults to True: it is what the explorer's rating filter actually does,
      and it yields the nine user-visible groups.

ROUNDING: they floor, `process_pgn_parquets.py` rounds (`.round(0)` before the
Int16 cast). The two differ by one point on odd rating sums, which only changes
a group assignment for a game sitting exactly on a boundary. `mean_elo_floored`
is here so a consumer that needs their counts exactly can recompute from the
raw ratings instead of inheriting our rounding by accident. Anything working
from the stored `mean_elo` column carries our rounding -- fine for our own
analysis, not bit-comparable to lichess.org.

Our pool floor of mean_elo >= 1800 lands exactly on a Lichess boundary, so the
corpus spans their 1800 / 2000 / 2200 / 2500+ groups with nothing straddling.
"""
from __future__ import annotations

import polars as pl

# Lower bounds of the groups select_avg can actually return, in order. 2800 is
# absent on purpose -- see quirk (a). 3200 is the label of the open-ended
# >= 2800 group, not its lower bound; upstream names it that way and we keep the
# name so a value here is greppable against their source.
LICHESS_BOUNDS: tuple[int, ...] = (
    0, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2500, 3200,
)

# What the explorer's rating filter exposes, after quirk (b) folds 2500/3200
# together. Nine groups. This is the set a UI should offer.
LICHESS_FILTER_BOUNDS: tuple[int, ...] = LICHESS_BOUNDS[:-1]

# Where each group starts, as select_avg tests them. Kept separate from the
# labels above because the >= 2800 group is labelled 3200.
_EDGES: tuple[tuple[int, int], ...] = (
    (1000, 0), (1200, 1000), (1400, 1200), (1600, 1400), (1800, 1600),
    (2000, 1800), (2200, 2000), (2500, 2200), (2800, 2500),
)
_TOP = 3200


def mean_elo_floored(white_elo: int, black_elo: int) -> int:
    """Lichess's `midpoint`: integer average, floored. Not our `.round(0)`."""
    return (int(white_elo) + int(black_elo)) // 2


def lichess_rating_group(avg_elo: int, *, collapse_top: bool = True) -> int:
    """Group label for a game whose two ratings average `avg_elo`.

    Returns the group's lower bound (0, 1000, ... 2500), except the open-ended
    top group which upstream labels 3200. With `collapse_top` (the default, and
    what the explorer's filter does) everything >= 2500 returns 2500.
    """
    avg = int(avg_elo)
    for upper, label in _EDGES:
        if avg < upper:
            return label
    return 2500 if collapse_top else _TOP


def lichess_group_expr(col: str | pl.Expr = "mean_elo", *,
                       collapse_top: bool = True) -> pl.Expr:
    """Vectorised `lichess_rating_group` for a column of averages.

    Same result as the scalar form on every input -- _test_rating_bands.py
    asserts that across the full rating range rather than trusting it.
    """
    e = pl.col(col) if isinstance(col, str) else col
    top = 2500 if collapse_top else _TOP
    out = pl.lit(top, dtype=pl.Int32)
    # Built inside out so the lowest group is tested first once evaluated.
    for upper, label in reversed(_EDGES):
        out = pl.when(e < upper).then(pl.lit(label, dtype=pl.Int32)).otherwise(out)
    return out.alias("rating_group")
