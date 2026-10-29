"""Weak-us stop model for the budget DP (2026-10-18): out of book, WE play like a
weaker population while the opponents still play like the pool.

budget_core's default stop value (leaf_node_value) is the pool's own results from
a position: once the book ends we play on like the pool (1800+), scored by how the
pool's games went. Here, from an our-turn position x that is out of book:

    W(x)  = sum over our moves m of q_weak(m | x) * R1(x_m)
    R1(y) = sum over replies r of p_pool(r | y) * E(y_r)

q_weak comes from a weaker pool (e.g. Blitz/Rapid/Classical games with mean Elo
1000-1799, pool_from_book bands 1000..1600), p_pool from the main pool, and E is the
engine eval through the uniform Lichess curve, our perspective -- or, with `curves`
(build_budget_books --weak-eval-curve), through curves fitted on real games for the
rating pairing at hand (fit_eval_curve.py): one where we (the weaker side) are to
move, one where they are. One full move of
play-out (our weak move, their reply), then the engine: the depth that keeps the
per-position cost to a pair of joins, since a book can leave millions of positions.

Every out-of-book valuation in the DP must use the same model, or the DP is biased
toward whichever place the book can end that the cheaper model flatters (e.g. ending
right after our move, valued by the engine, versus at our turn, valued after a weak
move). So the DP gets two functions:

    W(x)  for an our-turn position x out of book (stop values; opponent replies
          that leave the subgraph or fall under the share floor)
    R(y)  = sum over replies r of p_pool(r | y) * W(y_r), for an opponent-turn
          position y reached by our book's LAST move (the book ends after it)

Fallbacks, each counted: fewer than `min_weak` weak games at x -> the main pool's
move frequencies; no recorded replies at y_m -> E(y_m); nothing evaluable -> E(x);
nothing at all -> no entry (the caller keeps its default value). Moves and replies
under `min_share` are dropped and the rest renormalised.
"""
from __future__ import annotations

import math
from collections import Counter

import numpy as np
import polars as pl
import pyarrow as pa

from duck_compat import duck_pl
from stage3_backwards_induction import LICHESS_CP_SCALE

MISSING = -32768
EVAL_CLIP = 1000          # fit_eval_curve.CLIP: the fitted curves' cp domain


def _edges(con, path: str, keys: np.ndarray, name: str) -> pl.DataFrame:
    """(parent, child, n) for every recorded move out of `keys`, summed over rows."""
    if not len(keys):
        return pl.DataFrame(schema={"parent": pl.Int64, "child": pl.Int64, "n": pl.Float64})
    con.register(name, pa.table({"h": pa.array(np.unique(keys), pa.int64())}))
    df = duck_pl(con, f"""
        SELECT s.parent_hash AS parent, s.child_hash AS child,
               SUM(s.total)::DOUBLE AS n
        FROM read_parquet('{path}') s JOIN {name} k ON k.h = s.parent_hash
        WHERE s.child_hash IS NOT NULL
        GROUP BY 1, 2
    """)
    con.unregister(name)
    return df


def _shares(df: pl.DataFrame, min_share: float) -> pl.DataFrame:
    """Normalise n into p per parent, drop p < min_share, renormalise."""
    if not df.height:
        return df.with_columns(pl.lit(0.0).alias("p"))
    df = df.with_columns((pl.col("n") / pl.col("n").sum().over("parent")).alias("p"))
    df = df.filter(pl.col("p") >= min_share)
    return df.with_columns((pl.col("p") / pl.col("p").sum().over("parent")).alias("p"))


class EvalSource:
    """hash array -> our-perspective expected score (NaN where unevaluated).

    curves=None: the uniform Lichess curve. It is symmetric, so whose turn it is does
    not matter. Otherwise {'us': (a, k), 'them': (a, k)}: the SIDE TO MOVE scores
    sigmoid(a + k * cp), cp from its view clipped to +-EVAL_CLIP (fit_eval_curve.py),
    with 'us' where we are to move and 'them' where they are."""

    def __init__(self, our_white: bool, lookup=None, curves: dict | None = None):
        if lookup is None:
            from eval_arrays import lookup_evals, open_eval_arrays
            mm = open_eval_arrays()
            lookup = lambda keys: lookup_evals(keys, *mm)  # noqa: E731
        self._lookup, self.our_white, self.curves = lookup, our_white, curves

    def __call__(self, keys: np.ndarray, our_turn: bool = True) -> np.ndarray:
        cp = np.asarray(self._lookup(np.asarray(keys, dtype=np.int64)))
        if self.curves is None:
            es = 1.0 / (1.0 + np.exp(-LICHESS_CP_SCALE * cp.astype(np.float64)))
            es = np.where(cp == MISSING, np.nan, es)
            return es if self.our_white else np.where(np.isnan(es), np.nan, 1.0 - es)
        cp_us = cp.astype(np.float64) if self.our_white else -cp.astype(np.float64)
        a, k = self.curves["us" if our_turn else "them"]
        stm = np.clip(cp_us if our_turn else -cp_us, -EVAL_CLIP, EVAL_CLIP)
        p = 1.0 / (1.0 + np.exp(-(a + k * stm)))
        return np.where(cp == MISSING, np.nan, p if our_turn else 1.0 - p)


def load_curves(path: str, us_cell: str, them_cell: str) -> dict:
    """{'us': (a, k), 'them': (a, k)} from fit_eval_curve's eval_curves_pooled.parquet."""
    d = pl.read_parquet(path)
    out = {}
    for side, cell in (("us", us_cell), ("them", them_cell)):
        r = d.filter(pl.col("cell") == cell)
        if r.height != 1:
            raise ValueError(f"{path}: no single row for cell {cell!r}")
        out[side] = (float(r["a"][0]), float(r["k"][0]))
    return out


def weak_stop_values(con, x_hashes, y_hashes, *, pool: str, weak_pool: str,
                     evals: EvalSource, min_weak: float = 20.0,
                     min_share: float = 0.01, log=print
                     ) -> tuple[dict[int, float], dict[int, float], Counter]:
    """W for our-turn positions `x_hashes`, R for opponent-turn `y_hashes`.
    Returns (W, R, counts)."""
    counts: Counter = Counter()
    pool, weak_pool = pool.replace("\\", "/"), weak_pool.replace("\\", "/")
    y = np.unique(np.asarray(list(y_hashes), dtype=np.int64))
    # R(y) needs W at y's replies, so those join the x set first
    py = _shares(_edges(con, pool, y, "_ky"), min_share)
    x = np.unique(np.concatenate([np.asarray(list(x_hashes), dtype=np.int64),
                                  py["child"].to_numpy() if py.height else
                                  np.empty(0, np.int64)]))
    log(f"  weak stop: {len(x):,} our-turn and {len(y):,} opponent-turn positions")

    # our moves: the weak pool where it has >= min_weak games, else the main pool
    qw = _edges(con, weak_pool, x, "_kxw")
    wt = qw.group_by("parent").agg(pl.col("n").sum().alias("tot"))
    ok = set(wt.filter(pl.col("tot") >= min_weak)["parent"].to_list())
    counts["x_weak"] = len(ok)
    fb = np.array([h for h in x.tolist() if h not in ok], dtype=np.int64)
    counts["x_fallback_pool"] = len(fb)
    q = pl.concat([qw.filter(pl.col("parent").is_in(list(ok))),
                   _edges(con, pool, fb, "_kxp")])
    q = _shares(q, min_share)

    # their replies after each of our moves, then the engine
    m = q["child"].unique().to_numpy() if q.height else np.empty(0, np.int64)
    pr = _shares(_edges(con, pool, m, "_km"), min_share)
    if pr.height:
        pr = pr.with_columns(pl.Series("e", evals(pr["child"].to_numpy(), our_turn=True)))
        r1 = (pr.filter(pl.col("e").is_not_nan())
              .group_by("parent").agg(((pl.col("p") * pl.col("e")).sum()
                                       / pl.col("p").sum()).alias("r1")))
    else:
        r1 = pl.DataFrame(schema={"parent": pl.Int64, "r1": pl.Float64})
    em = pl.DataFrame({"child": m, "e_m": evals(m, our_turn=False)})
    q = (q.join(r1.rename({"parent": "child"}), on="child", how="left")
          .join(em, on="child", how="left")
          .with_columns(pl.when(pl.col("r1").is_not_null()).then(pl.col("r1"))
                        .otherwise(pl.col("e_m")).alias("v")))
    counts["moves_engine_only"] = int(q.filter(pl.col("r1").is_null()
                                               & pl.col("e_m").is_not_nan()).height)
    wq = (q.filter(pl.col("v").is_not_null() & pl.col("v").is_not_nan())
           .group_by("parent").agg(((pl.col("p") * pl.col("v")).sum()
                                    / pl.col("p").sum()).alias("w")))
    W = dict(zip(wq["parent"].to_list(), wq["w"].to_list()))
    counts["x_walked"] = len(W)
    rest = np.array([h for h in x.tolist() if h not in W], dtype=np.int64)
    if len(rest):
        for h, e in zip(rest.tolist(), evals(rest, our_turn=True).tolist()):
            if not math.isnan(e):
                W[h] = e
                counts["x_engine_only"] += 1
            else:
                counts["x_none"] += 1

    # R(y): their reply, then W
    R: dict[int, float] = {}
    if py.height:
        py = py.with_columns(pl.col("child").replace_strict(W, default=None,
                                                            return_dtype=pl.Float64)
                             .alias("w"))
        ry = (py.filter(pl.col("w").is_not_null())
                .group_by("parent").agg(((pl.col("p") * pl.col("w")).sum()
                                         / pl.col("p").sum()).alias("r")))
        R = dict(zip(ry["parent"].to_list(), ry["r"].to_list()))
    counts["y_walked"] = len(R)
    rest = np.array([h for h in y.tolist() if h not in R], dtype=np.int64)
    if len(rest):
        for h, e in zip(rest.tolist(), evals(rest, our_turn=False).tolist()):
            if not math.isnan(e):
                R[h] = e
                counts["y_engine_only"] += 1
            else:
                counts["y_none"] += 1
    return W, R, counts
