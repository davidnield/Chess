"""Real-data stop values (2026-10-27): play a position out to the pool's ply cap with real moves.

The budget optimiser's weak-us stop model (weak_stop.py) values a position where the book ends by
ONE move of play-out and then the engine. The convertibility evaluation that judges books
(scratch/python/convertibility_eval.py) plays every line out to ply 30 with real moves. This
module computes the optimiser-side counterpart once per (pool, colour): for every position of the
pool,

    V(x) = sum_m q(m | x) V(x_m)          our turn: how 1000-1799 players move (--weak-pool)
    V(y) = (sum_r n_r V(y_r) + aux) / (sum_r n_r + aux_den)
                                          their turn: how the pool moves, plus the games its
                                          edges don't show (aux sidecar: replies below the
                                          floor, games that ended there), valued by the same
                                          stage3.aux_opp_mix both engines use

by backwards induction over ply (deepest first). A child that is not a position of the pool, or
whose value is not known yet (a transposition back to an earlier ply), is valued by the engine:
the eval arrays through the uniform Lichess curve, or --curves fitted on real games. A position
with no usable moves takes its own engine value. Our-turn positions with fewer than --min-weak
weak games use the pool's own move frequencies (counted). Moves under --min-share of their
position's games are dropped and the rest renormalised, as weak_stop does.

Same data rules as the weak stop model; only the depth differs (to the pool's cap instead of one
move). Output, one row per pool position:
    position_hash, ply, our_turn, v (our expected score), n_moves_used, source
    source: 0 = played out, 1 = own eval (no usable move), 2 = our turn from the pool's moves

    .venv/Scripts/python.exe python/empirical_stop.py --colour white \\
        --pool E:/chess/position-stats/position_stats_pooled_ge1800_2013_2026_brc.parquet \\
        --weak-pool E:/chess/position-stats/weak_edges_b1000_1800_on_ge1800.parquet \\
        --out E:/chess/position-stats/empirical_stop_white_ge1800.parquet
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from budget_core import aux_row_buckets  # noqa: E402
from stage3_backwards_induction import aux_opp_mix  # noqa: E402
from weak_stop import EvalSource, load_curves  # noqa: E402


def _filtered(edges: pl.DataFrame, min_share: float) -> pl.DataFrame:
    """Drop moves under min_share of their position's games (the rest renormalise later)."""
    return (edges.with_columns((pl.col("n") / pl.col("n").sum().over("p")).alias("share"))
                 .filter(pl.col("share") >= min_share).drop("share"))


def build(pool: str, aux_path: str | None, weak_pool: str, colour: str, *,
          min_share: float = 0.01, min_weak: float = 20.0, eval_weight: float = 0.75,
          cov_blend: bool = True, curves: dict | None = None, evals: EvalSource | None = None,
          log=print) -> pl.DataFrame:
    t0 = time.time()
    our_white = colour == "white"
    # pool edges summed over slices; a position's ply is its earliest arrival
    pe = (pl.scan_parquet(pool).filter(pl.col("child_hash").is_not_null())
            .group_by("parent_hash", "child_hash")
            .agg(pl.col("total").sum().cast(pl.Float64).alias("n"),
                 (pl.col("ply").min() - 1).alias("ply"),
                 pl.col("parent_epd").first().str.contains(" w ").alias("wtm"))
            .rename({"parent_hash": "p", "child_hash": "c"}).collect())
    P = (pe.group_by("p").agg(pl.col("ply").min(), pl.col("wtm").first())
           .with_columns((pl.col("wtm") == our_white).alias("our_turn")).sort("p"))
    hashes = P["p"].to_numpy()
    our_turn = P["our_turn"].to_numpy()
    ply = P["ply"].to_numpy()
    log(f"pool: {pe.height:,} edges, {P.height:,} positions ({our_turn.sum():,} our turn) "
        f"[{time.time() - t0:.0f}s]")

    # our turn: the weak population's moves where it has enough games, else the pool's
    ours = P.filter(pl.col("our_turn")).select("p")
    we = (pl.scan_parquet(weak_pool).rename({"parent_hash": "p", "child_hash": "c"})
            .join(ours.lazy(), on="p", how="semi").filter(pl.col("c").is_not_null())
            .group_by("p", "c").agg(pl.col("total").sum().cast(pl.Float64).alias("n")).collect())
    wt = we.group_by("p").agg(pl.col("n").sum().alias("wn")).filter(pl.col("wn") >= min_weak)
    we = we.join(wt.select("p"), on="p", how="semi")
    fallback = ours.join(wt.select("p"), on="p", how="anti")
    pool_ours = pe.join(fallback, on="p", how="semi").select("p", "c", "n")
    theirs = pe.join(P.filter(~pl.col("our_turn")).select("p"), on="p", how="semi").select("p", "c", "n")
    edges = pl.concat([_filtered(we, min_share), _filtered(pool_ours, min_share),
                       _filtered(theirs, min_share)])
    log(f"weak moves at {wt.height:,} our-turn positions; pool's moves at {fallback.height:,} "
        f"(< {min_weak:g} weak games); {edges.height:,} edges used [{time.time() - t0:.0f}s]")
    del pe, we

    # the games their edges don't show, by the shared aux rule (our-perspective sums)
    aux_num = np.zeros(len(hashes))
    aux_den = np.zeros(len(hashes))
    if aux_path:
        theirs_h = P.filter(~pl.col("our_turn")).select(pl.col("p").alias("position_hash"))
        ax = pl.scan_parquet(aux_path).join(theirs_h.lazy(), on="position_hash", how="semi").collect()
        idx = np.searchsorted(hashes, ax["position_hash"].to_numpy())
        for i, row in zip(idx, ax.iter_rows(named=True)):
            (t_tot, t_sum, _crack, o_tot, o_sum, o_eval, o_cov,
             hr_tot, hr_sum) = aux_row_buckets(row, our_white)
            w_num, den = aux_opp_mix(t_tot, t_sum, o_tot, o_sum, o_eval, o_cov,
                                     hr_tot, hr_sum, None, eval_weight, cov_blend=cov_blend)
            aux_num[i] = w_num if our_white else den - w_num
            aux_den[i] = den
        log(f"aux at {ax.height:,} opponent-turn positions [{time.time() - t0:.0f}s]")

    # engine values: every child, and every position (its own fallback)
    ev = evals or EvalSource(our_white, curves=curves)
    pi = np.searchsorted(hashes, edges["p"].to_numpy())
    c = edges["c"].to_numpy()
    n = edges["n"].to_numpy()
    ci = np.searchsorted(hashes, c)
    ci_ok = ci < len(hashes)
    ci_ok[ci_ok] = hashes[ci[ci_ok]] == c[ci_ok]
    ci = np.where(ci_ok, ci, -1)
    child_us = ~our_turn[pi]                          # after our move it is their turn, and back
    e_child = np.full(len(c), np.nan)
    for flag in (True, False):
        m = child_us == flag
        e_child[m] = ev(c[m], our_turn=flag)
    e_own = np.full(len(hashes), np.nan)
    for flag in (True, False):
        m = our_turn == flag
        e_own[m] = ev(hashes[m], our_turn=flag)
    log(f"evals looked up [{time.time() - t0:.0f}s]")

    # backwards induction, deepest ply first
    V = np.full(len(hashes), np.nan)
    src = np.zeros(len(hashes), dtype=np.int8)
    src[our_turn & np.isin(hashes, fallback["p"].to_numpy())] = 2
    used = np.zeros(len(hashes), dtype=np.int32)
    order = np.argsort(-ply[pi], kind="stable")
    pi, ci, n, e_child = pi[order], ci[order], n[order], e_child[order]
    lvl = ply[pi]
    for L in range(int(ply.max()), -1, -1):
        lo, hi = np.searchsorted(-lvl, [-L, -L + 1], side="left")
        if lo == hi:
            continue
        p_, c_, n_, e_ = pi[lo:hi], ci[lo:hi], n[lo:hi], e_child[lo:hi]
        vc = np.where(c_ >= 0, V[np.maximum(c_, 0)], np.nan)
        vc = np.where(np.isnan(vc), e_, vc)
        ok = ~np.isnan(vc)
        num = np.bincount(p_[ok], weights=n_[ok] * vc[ok], minlength=len(hashes))
        den = np.bincount(p_[ok], weights=n_[ok], minlength=len(hashes))
        cnt = np.bincount(p_[ok], minlength=len(hashes))
        at = (ply == L)
        num, den = num + aux_num * at, den + aux_den * at
        has = at & (den > 0)
        V[has] = num[has] / den[has]
        used[at] = cnt[at]
        none = at & ~(den > 0)
        V[none] = e_own[none]
        src[none] = 1
        log(f"  ply {L:2d}: {at.sum():>10,} positions [{time.time() - t0:.0f}s]")
    # positions with no edge at all keep NaN; give them their own eval
    lone = np.isnan(V)
    V[lone] = e_own[lone]
    src[lone] = 1
    return pl.DataFrame({"position_hash": hashes, "ply": ply, "our_turn": our_turn,
                         "v": V, "n_moves_used": used, "source": src})


def stop_overrides(path: str, xs, ys, *, colour: str, pool: str) -> tuple[dict, dict, dict]:
    """The table's values for the optimiser's stop positions (budget_core.stop_model_positions):
    W for our-turn positions, R for opponent-turn ones. Refuses a table built for another colour
    or pool: it would value the wrong side's moves from the wrong population."""
    meta = json.loads(Path(path).with_suffix(".parquet.meta.json").read_text())
    if meta["colour"] != colour or Path(meta["pool"]).name != Path(pool).name:
        raise ValueError(f"{path} was built for {meta['colour']} on {meta['pool']}, "
                         f"not {colour} on {pool}")
    want = pl.DataFrame({"position_hash": np.fromiter(set(xs) | set(ys), dtype=np.int64)})
    t = (pl.scan_parquet(path).select("position_hash", "our_turn", "v")
           .join(want.lazy(), on="position_hash", how="semi").collect())
    xs, ys = set(xs), set(ys)
    W = {h: v for h, ot, v in t.iter_rows() if ot and h in xs and v == v}
    R = {h: v for h, ot, v in t.iter_rows() if not ot and h in ys and v == v}
    return W, R, {"table_x": len(W), "table_y": len(R),
                  "missing_x": len(xs) - len(W), "missing_y": len(ys) - len(R)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--colour", choices=["white", "black"], required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--aux-stats", default=None, help="default: the pool's sidecar (position_stats_aux_*)")
    ap.add_argument("--weak-pool", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-share", type=float, default=0.01)
    ap.add_argument("--min-weak", type=float, default=20.0)
    ap.add_argument("--eval-weight", type=float, default=0.75)
    ap.add_argument("--no-cov-blend", action="store_true")
    ap.add_argument("--curves", default=None, metavar="PATH",
                    help="fit_eval_curve curves (default: the uniform Lichess curve)")
    ap.add_argument("--curve-cells", default="stm1000-1799_vs_opp1800+,stm1800+_vs_opp1000-1799")
    a = ap.parse_args()
    aux = a.aux_stats or str(Path(a.pool).with_name(
        Path(a.pool).name.replace("position_stats_", "position_stats_aux_", 1)))
    curves = load_curves(a.curves, *a.curve_cells.split(",")) if a.curves else None
    df = build(a.pool, aux if Path(aux).exists() else None, a.weak_pool, a.colour,
               min_share=a.min_share, min_weak=a.min_weak, eval_weight=a.eval_weight,
               cov_blend=not a.no_cov_blend, curves=curves,
               log=lambda s: print(s, flush=True))
    out = Path(a.out)
    tmp = out.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(out)
    meta = {"pool": a.pool, "aux_stats": aux, "weak_pool": a.weak_pool, "colour": a.colour,
            "min_share": a.min_share, "min_weak": a.min_weak, "eval_weight": a.eval_weight,
            "cov_blend": not a.no_cov_blend, "curves": a.curves,
            "built": time.strftime("%Y-%m-%d %H:%M:%S"),
            "sources": {str(k): int(v) for k, v in zip(*np.unique(df["source"].to_numpy(), return_counts=True))}}
    out.with_suffix(".parquet.meta.json").write_text(json.dumps(meta, indent=1))
    print(f"{df.height:,} positions -> {out}; sources {meta['sources']}")


if __name__ == "__main__":
    main()
