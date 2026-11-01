"""Does the builder value the book's edge the way the referee does? (follow-up review R4)

The referee (convertibility_eval) scores a book by simulating games to ply 30. The budget
builder chooses the book with a cheaper model of what happens after it ends -- the weak stop
model's W(x): one move of ours like 1000-1799 players, one pool reply, then the engine. This
measures the gap where it matters, at the book's actual exits, weighted by how often the
referee's games reach them:

  exits   the referee's walk of the book (exact part + one sampled game per stratum) records
          each game's first our-turn position out of book, by (position, ply)
  V0      the referee's value of an exit with NO book from there on: --games sampled games
          from (position, ply) to ply 30, the referee's move tables, prior and engine
  VB      the same games with the book still in force, so a game that transposes back into
          the book plays its moves again: VB - V0 is what book re-entry is worth, which no
          per-position stop table can see
  W       the builder's stop value at the same positions (weak_stop.weak_stop_values on the
          book's own recipe: pool, weak pool, floor, share cut); and, where the recipe has
          one, the empirical stop table's value

Reports exit-weighted means, the bias W - V0, its RMSE net of the sampling noise, the rank
correlation, all by ply band, and the mass the top exits cover.

    PYTHONPATH=python .venv/Scripts/python.exe python/stop_alignment.py \\
        --book L=white=PATH [--top 2000] [--games 64] --our-moves DIR --opp-moves DIR --out PREFIX
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, "python")
import convertibility_eval as cv  # noqa: E402
from eval_arrays import lookup_evals, open_eval_arrays  # noqa: E402
from weak_stop import EvalSource, weak_stop_values  # noqa: E402


def per_start(leaves, n_starts, lookup, sf, curves, our_white):
    """Each start's mean utility over its sampled games, and that mean's SE."""
    v = cv.value_leaves(leaves, our_white, lookup, sf, curves)
    df = pl.DataFrame({"s": v["sid"], "w": v["ps"], "u": v["uniform"]})
    g = df.group_by("s").agg((pl.col("w") * pl.col("u")).sum().alias("wu"), pl.col("w").sum().alias("w"),
                             pl.col("u").std().alias("sd"), pl.len().alias("n"))
    val = np.full(n_starts, np.nan)
    se = np.full(n_starts, np.nan)
    for s, wu, w, sd, n in g.iter_rows():
        val[s] = wu / w
        se[s] = (sd or 0.0) / np.sqrt(n)
    return val, se


def wstats(x, y, w):
    """Weighted bias, RMSE and Spearman correlation of x against y."""
    ok = ~(np.isnan(x) | np.isnan(y))
    x, y, w = x[ok], y[ok], w[ok]
    if not len(x):
        return {}
    w = w / w.sum()
    d = x - y
    rx, ry = np.argsort(np.argsort(x)), np.argsort(np.argsort(y))
    rx, ry = rx - (w * rx).sum(), ry - (w * ry).sum()
    rho = float((w * rx * ry).sum() / np.sqrt((w * rx * rx).sum() * (w * ry * ry).sum()))
    return {"n": int(ok.sum()), "bias": float((w * d).sum()), "rmse": float(np.sqrt((w * d * d).sum())),
            "spearman": rho}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--book", required=True, metavar="LABEL=COLOUR=PATH")
    ap.add_argument("--meta", default=None, help="the recipe meta (default <book>.meta.json)")
    ap.add_argument("--top", type=int, default=2000)
    ap.add_argument("--games", type=int, default=64)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--our-moves", required=True)
    ap.add_argument("--opp-moves", required=True)
    ap.add_argument("--sf-cache", default="E:/chess/calibration/convertibility_sf_cache_align.parquet")
    ap.add_argument("--sf-procs", type=int, default=3)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    t0 = time.time()
    label, colour, path = a.book.split("=", 2)
    our_white = colour == "white"
    meta = json.loads(Path(a.meta or path + ".meta.json").read_text(encoding="utf-8"))
    ro = SimpleNamespace(tau=1e-4, rollouts=1, max_ply=30, k=100.0, min_p=0.005,
                         our_elo=1450, opp_elo=2000)
    con = duckdb.connect()
    con.execute("SET threads=8; SET memory_limit='16GB'")
    ours = cv.Counts(con, a.our_moves.format(colour=colour), "ours")
    opp = cv.Counts(con, a.opp_moves.format(colour="black" if our_white else "white"), "opp")
    mm = open_eval_arrays()

    def lookup(hs):
        return lookup_evals(hs, *mm)
    maia = cv.Maia()
    sf = cv.StockfishFill(a.sf_cache, 10, a.sf_procs)
    cvs = {r["cell"]: (r["a"], r["k"]) for r in pl.read_parquet(cv.CURVES).iter_rows(named=True)}
    curves = (cvs["stm1000-1799_vs_opp1800+"], cvs["stm1800+_vs_opp1000-1799"])
    bd = pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
    book = {h: m for h, s, m in bd.iter_rows() if s == colour and m}
    try:
        exits: dict = {}
        _, starts, _, _ = cv.enumerate_book(book, our_white, 0, ro, ours, opp, maia, lookup, exits)
        cv.rollouts(starts, book, our_white, 0, ro, a.seed, ours, opp, maia, lookup, exits)
        tot = sum(v[0] for v in exits.values())
        top = sorted(exits.items(), key=lambda kv: -kv[1][0])[:a.top]
        cover = sum(v[0] for _, v in top)
        print(f"{len(exits):,} exits, exit mass {tot:.4f}; the top {len(top):,} cover "
              f"{cover / tot:.1%} ({time.time() - t0:,.0f}s)", flush=True)
        st = [(h, epd, 1.0, ply, None, 0.0, 0.0) for (h, ply), (m, epd) in top]
        mass = np.array([v[0] for _, v in top])
        plies = np.array([ply for (h, ply), _ in top])
        go = SimpleNamespace(**{**vars(ro), "rollouts": a.games})
        l0, _ = cv.rollouts(st, {}, our_white, 0, go, a.seed, ours, opp, maia, lookup)
        v0, se0 = per_start(l0, len(st), lookup, sf, curves, our_white)
        print(f"  V0 done ({time.time() - t0:,.0f}s)", flush=True)
        lb, _ = cv.rollouts(st, book, our_white, 0, go, a.seed, ours, opp, maia, lookup)
        vb, seb = per_start(lb, len(st), lookup, sf, curves, our_white)
        print(f"  VB done ({time.time() - t0:,.0f}s)", flush=True)
    finally:
        maia.close()
        sf.close()
    xs = np.array([h for (h, _), _ in top], dtype=np.int64)
    W, _, wc = weak_stop_values(con, xs, [], pool=meta["stats"], weak_pool=meta["weak_pool"],
                                evals=EvalSource(our_white), min_weak=meta.get("weak_min_games", 20.0),
                                min_share=meta.get("weak_min_share", 0.01), log=print)
    w = np.array([W.get(int(h), np.nan) for h in xs])
    emp = np.full(len(xs), np.nan)
    if meta.get("stop_table"):
        from empirical_stop import stop_overrides
        E, _, _ = stop_overrides(meta["stop_table"], xs.tolist(), [], colour=colour, pool=meta["stats"])
        emp = np.array([E.get(int(h), np.nan) for h in xs])
    noise = float(np.sqrt(np.nansum(mass * se0 ** 2) / mass.sum()))
    res = {"book": a.book, "exits": len(exits), "exit_mass": tot, "top": len(top),
           "top_cover": cover / tot, "games_per_exit": a.games, "seed": a.seed,
           "V0": float(np.nansum(mass * v0) / mass.sum()), "VB": float(np.nansum(mass * vb) / mass.sum()),
           "reentry_VB_minus_V0": float(np.nansum(mass * (vb - v0)) / mass.sum()),
           "W_mean": float(np.nansum(mass * w) / mass[~np.isnan(w)].sum()),
           "W_vs_V0": wstats(w, v0, mass), "W_vs_VB": wstats(w, vb, mass),
           "emp_vs_V0": wstats(emp, v0, mass) if meta.get("stop_table") else None,
           "V0_sampling_noise_rms": noise, "weak_counts": dict(wc), "by_ply": {}}
    for lo, hi in ((0, 10), (10, 14), (14, 18), (18, 22), (22, 31)):
        m = (plies >= lo) & (plies < hi)
        if m.any():
            res["by_ply"][f"{lo}-{hi - 1}"] = {"mass": float(mass[m].sum() / mass.sum()),
                                               "W_vs_V0": wstats(w[m], v0[m], mass[m]),
                                               "reentry": float(np.nansum(mass[m] * (vb[m] - v0[m])) / mass[m].sum())}
    res["seconds"] = round(time.time() - t0)
    print(json.dumps(res, indent=1))
    Path(a.out + ".json").write_text(json.dumps(res, indent=1), encoding="utf-8")
    pl.DataFrame({"hash": xs, "ply": plies, "mass": mass, "V0": v0, "V0_se": se0, "VB": vb,
                  "VB_se": seb, "W": w, "emp": emp, "epd": [s[1] for s in st]}).write_parquet(a.out + ".parquet")


if __name__ == "__main__":
    main()
