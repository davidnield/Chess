"""The "other moves" bucket on the referee's scale (2026-11-02; flag off by default).

An opponent node's value is a mean over its pool replies and its aux buckets. The other bucket
holds the replies under the pool's floor: they have no edge, so no stop model reaches them, and
they keep the all-time 1800+ blend of results and engine. The White 400 sampled-stop trial showed
why that matters (scratch/python/exit_value_check.py). Sampled stops were accurate (bias +.004 at
the book's real exits) but fell about .10 below the weak stop, so the optimiser steered into
positions where most replies are rare: 61% of the book's mass sat in this bucket at .563, while the
referee values those exits at about .36. That gap is about the whole .128 between the book's
in-model value and its referee score. The bucket must be on the same scale as the stops.

  1. dump    build_budget_books --dump-other-positions PATH writes, for every opponent node with
             an other bucket: its route ply and reach, the bucket's share and value, the node's
             engine value and its pool children; then stops, before any stop model runs.
  2. sample  this script's CLI: for --nodes nodes (most bucket mass, spread over the ply bands), the
             referee's reply distribution at the node with the pool's replies taken out; each
             remaining reply's position is played out to the horizon with NO book, exactly as the
             referee plays (convertibility_eval.rollouts). Games per reply follow its share, about
             --games per node. Replies under the referee's --min-p are never played there; a node
             left with none has no sample, and gets the prediction below. Every remaining reply
             is played, at its true share (2026-11-04): capping them at eight and rescaling
             dropped 3.7% of the bucket on average and up to a third of a node, from training
             and held-out targets alike, so the held-out check could not see it.
  3. apply   build_budget_books --sampled-other TABLE: V ~ a(ply band) + b * bucket value + c *
             engine value, fitted on the sampled nodes weighted by bucket mass; an unsampled node
             gets the prediction, a sampled one the empirical-Bayes blend, as sampled_stop does.
             Each node's const_base moves by share * (new value - old value).

Only the other bucket moves. Terminations and the horizon bucket stay as they are: about 2% of the
trial book's mass, and the referee has no resignations to compare them with.

The ply is the one sampled_stop uses: the highest-reach route's (see its PLY IS APPROXIMATED).

    .venv/Scripts/python.exe python/sampled_other.py --dump PATH --colour white --nodes 2000 \\
        --games 32 --our-moves DIR --opp-moves DIR --out TABLE
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from sampled_stop import BANDS, band, file_identity, sampler_code, simulation  # noqa: E402

DUMP_SCHEMA = {"hash": pl.Int64, "ply": pl.Int32, "reach": pl.Float64, "share": pl.Float64,
               "value": pl.Float64, "engine": pl.Float64, "epd": pl.Utf8,
               "children": pl.List(pl.Int64)}


def other_positions(g) -> pl.DataFrame:
    """Every opponent node of graph g (built with track_other and track_route_ply) with an other
    bucket. weight = reach * share, the mass the bucket can carry."""
    if not g.route_ply:
        raise ValueError("other_positions needs build_graph(track_route_ply=True)")
    rows = []
    for h, (share, value, engine) in g.other.items():
        n = g.opp[h]
        kids = [ch for _p, ch, _s, _lv in n.kids if ch is not None]
        rows.append((h, g.route_ply.get(h, g.ply.get(h, 0)), g.reach.get(h, 0.0), share, value,
                     engine, g.epd.get(h), kids + [ch for _p, ch, _lv in n.floor]))
    df = pl.DataFrame(rows, schema=DUMP_SCHEMA, orient="row")
    return df.with_columns((pl.col("reach") * pl.col("share")).alias("weight"))


def below_floor_replies(epd: str, dist: dict, children) -> list[tuple[int, str, float]]:
    """The referee's replies at a node with the pool's taken out: (child hash, child EPD, share of
    what is left), most likely first, hash breaking ties. Empty when the referee plays only pool
    replies there."""
    from zobrist import zobrist_int64
    known = set(children)
    b = chess.Board(epd + " 0 1")
    out = []
    for san, p in dist.items():
        b.push(b.parse_san(san))
        h = zobrist_int64(b)
        if h not in known:
            out.append((h, b.epd(), p))
        b.pop()
    s = sum(r[2] for r in out)
    return sorted(((h, e, p / s) for h, e, p in out), key=lambda r: (-r[2], r[0]))


def select_replies(rep: list, max_replies: int) -> list:
    """The replies a node's value is averaged over: all of them, or (max_replies > 0, for a cost
    comparison) only the most likely, rescaled by the caller."""
    return rep[:max_replies] if max_replies > 0 else rep


def design(ply: np.ndarray, value: np.ndarray, engine: np.ndarray) -> np.ndarray:
    """Band dummies, the bucket's value, the node's engine value (its bucket value if unknown)."""
    nb = len(BANDS) - 1
    X = np.zeros((len(ply), nb + 2))
    X[np.arange(len(ply)), band(ply)] = 1.0
    X[:, nb] = value
    X[:, nb + 1] = np.where(np.isnan(engine), value, engine)
    return X


def fit(tab: pl.DataFrame) -> dict | None:
    """Weighted least squares of the sampled V on design(); tau^2 is the residual variance net of
    the sampling noise, floored as in sampled_stop.fit."""
    t = tab.filter(pl.col("v0").is_not_null() & pl.col("v0").is_not_nan())
    if t.height < 50:
        return None
    ply = t["ply"].to_numpy()
    X = design(ply, t["value"].to_numpy(), t["engine"].fill_null(np.nan).to_numpy())
    y = t["v0"].to_numpy()
    w = np.maximum(t["weight"].to_numpy(), 1e-12)
    w = w / w.sum()
    nb = len(BANDS) - 1
    used = np.unique(band(ply))
    cols = list(used) + [nb, nb + 1]
    sw = np.sqrt(w)
    coef, *_ = np.linalg.lstsq(X[:, cols] * sw[:, None], y * sw, rcond=None)
    a = np.full(nb, np.nan)
    a[used] = coef[:-2]
    for i in range(nb):                # a band nothing was sampled in: the nearest one
        if np.isnan(a[i]):
            a[i] = a[min(used, key=lambda u: abs(u - i))]
    full = np.r_[a, coef[-2:]]
    res = y - X @ full
    se2 = t["se"].fill_null(0.0).to_numpy() ** 2
    return {"a": a.tolist(), "b_value": float(coef[-2]), "c_engine": float(coef[-1]), "n": t.height,
            "tau2": max(float((w * res ** 2).sum() - (w * se2).sum()), 1e-6),
            "resid_rms": float(np.sqrt((w * res ** 2).sum())),
            "mean_shift": float((w * (y - t["value"].to_numpy())).sum())}


def predict(m: dict, ply: np.ndarray, value: np.ndarray, engine: np.ndarray) -> np.ndarray:
    return np.clip(design(ply, value, engine) @ np.r_[m["a"], m["b_value"], m["c_engine"]], 0.0, 1.0)


def holdout_check(tab: pl.DataFrame, modulus: int = 10) -> dict | None:
    """The calibration on nodes it was not fitted to, weighted by bucket mass: bias and RMSE of
    the prediction, the sampling noise in the held-out V, the old bucket value's RMSE."""
    t = tab.filter(pl.col("v0").is_not_null() & pl.col("v0").is_not_nan())
    held = t.filter(pl.col("hash") % modulus == 0)
    m = fit(t.filter(pl.col("hash") % modulus != 0))
    if m is None or held.height < 10:
        return None
    w = np.maximum(held["weight"].to_numpy(), 1e-12)
    w = w / w.sum()
    y, v = held["v0"].to_numpy(), held["value"].to_numpy()
    d = predict(m, held["ply"].to_numpy(), v, held["engine"].fill_null(np.nan).to_numpy()) - y
    noise2 = float((w * held["se"].fill_null(0.0).to_numpy() ** 2).sum())
    rmse2 = float((w * d * d).sum())
    return {"n": held.height, "bias": float((w * d).sum()), "rmse": float(np.sqrt(rmse2)),
            "noise_rms": float(np.sqrt(noise2)),
            "rmse_net_of_noise": float(np.sqrt(max(rmse2 - noise2, 0.0))),
            "old_value_bias": float((w * (v - y)).sum()),
            "old_value_rmse": float(np.sqrt((w * (v - y) ** 2).sum()))}


def apply(g, tab: pl.DataFrame) -> dict:
    """Move every tracked bucket of graph g to the referee's scale; a sample counts only at the
    ply the graph values the node at. Returns a report; with no usable calibration, a ValueError
    (this flag must not silently leave a book on the old scale)."""
    ply_of = {h: g.route_ply.get(h, g.ply.get(h, 0)) for h in g.other}
    keyed = tab.filter(pl.col("hash").is_in(list(ply_of)))
    keep = [ply_of[h] == p for h, p in zip(keyed["hash"].to_list(), keyed["ply"].to_list())]
    refused = len(keep) - sum(keep)
    keyed = keyed.filter(pl.Series(keep, dtype=pl.Boolean)) if keyed.height else keyed
    m = fit(keyed)
    if m is None:
        raise ValueError(f"--sampled-other: {keyed.height} sampled nodes on this graph, too few to "
                         f"calibrate")
    hs = list(g.other)
    vals = np.array([g.other[h][1] for h in hs])
    eng = np.array([np.nan if g.other[h][2] is None else g.other[h][2] for h in hs])
    pred = predict(m, np.array([ply_of[h] for h in hs]), vals, eng)
    new = dict(zip(hs, pred.tolist()))
    n_s = 0
    for h, v0, se in keyed.select("hash", "v0", "se").iter_rows():
        if v0 is not None and v0 == v0:
            lam = m["tau2"] / (m["tau2"] + (se or 0.0) ** 2)
            new[h] = float(np.clip(new[h] + lam * (v0 - new[h]), 0.0, 1.0))
            n_s += 1
    mass = shift = 0.0
    for h, v in new.items():
        share, old, _e = g.other[h]
        g.opp[h].const_base += share * (v - old)
        r = g.reach.get(h, 0.0) * share
        mass += r
        shift += r * (v - old)
    return {"model": m, "sampled": n_s, "predicted": len(new) - n_s, "cross_ply_refused": refused,
            "mean_shift": shift / mass if mass else 0.0}


def main():
    import convertibility_eval as cv
    import duckdb
    from eval_arrays import lookup_evals, open_eval_arrays
    from stop_alignment import per_start
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", required=True, help="build_budget_books --dump-other-positions output")
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--nodes", type=int, default=2000)
    ap.add_argument("--games", type=int, default=32, help="about this many per node, split over "
                    "its replies by share")
    ap.add_argument("--max-replies", type=int, default=0, help="0 = every reply (the full bucket); "
                    "N = only the most likely N, which biases the target")
    ap.add_argument("--chunk", type=int, default=250)
    ap.add_argument("--seed", type=int, default=41)
    ap.add_argument("--our-moves", required=True)
    ap.add_argument("--opp-moves", required=True)
    ap.add_argument("--k", type=float, default=100.0)
    ap.add_argument("--min-p", type=float, default=0.005)
    ap.add_argument("--max-ply", type=int, default=30, help="the referee's horizon H")
    ap.add_argument("--our-elo", type=int, default=1450)
    ap.add_argument("--opp-elo", type=int, default=2000)
    ap.add_argument("--sf-cache", default=None)
    ap.add_argument("--sf-procs", type=int, default=3)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    t0 = time.time()
    our_white = a.colour == "white"
    dmp = Path(a.dump + ".meta.json")
    if not dmp.exists():
        raise SystemExit(f"FATAL: {dmp} is missing: the dump's recipe is unknown")
    dump_meta = json.loads(dmp.read_text(encoding="utf-8"))
    bad = {k: (dump_meta.get(k), v) for k, v in (("colour", a.colour), ("horizon", a.max_ply))
           if dump_meta.get(k) != v}
    if bad:
        raise SystemExit(f"FATAL: the dump does not match this run (dump, run): {bad}")
    out = Path(a.out)
    parts = Path(str(out) + ".parts")
    parts.mkdir(parents=True, exist_ok=True)
    cfg = {k: getattr(a, k) for k in ("colour", "nodes", "games", "max_replies", "chunk", "seed",
                                      "our_moves", "opp_moves", "k", "min_p", "max_ply", "our_elo",
                                      "opp_elo")}
    cfg["referee"] = "convertibility_eval.rollouts, empty book, uniform curve, SF depth 10 fill"
    cfg["dump"] = dump_meta
    cfg["dump_file"] = file_identity(a.dump)
    cfg["sim"] = simulation(a, cv, our_white)
    cfg["sampler"] = sampler_code(cv)
    mp = parts / "_config.json"
    if mp.exists() and json.loads(mp.read_text(encoding="utf-8")) != json.loads(json.dumps(cfg)):
        raise SystemExit(f"FATAL: {parts} was sampled under other settings; delete it to start over")
    mp.write_text(json.dumps(cfg, indent=1), encoding="utf-8")
    dump = pl.read_parquet(a.dump)
    live = dump.filter((pl.col("ply") < a.max_ply) & pl.col("epd").is_not_null() & (pl.col("weight") > 0))
    live = live.with_columns(pl.Series("band", band(live["ply"].to_numpy()))).sort(
        "weight", "hash", descending=[True, False])
    per = a.nodes // (len(BANDS) - 1)
    strat = pl.concat([live.filter(pl.col("band") == b).head(per) for b in range(len(BANDS) - 1)])
    rest = live.join(strat.select("hash"), on="hash", how="anti").head(max(0, a.nodes - strat.height))
    pick = pl.concat([strat, rest]).drop("band")
    print(f"{pick.height:,} nodes to sample ({dump.height:,} with an other bucket; the picks hold "
          f"{pick['weight'].sum() / dump['weight'].sum():.1%} of the optimistic bucket mass)", flush=True)
    con = duckdb.connect()
    con.execute("SET threads=8; SET memory_limit='16GB'")
    ours = cv.Counts(con, a.our_moves.format(colour=a.colour), "ours")
    opp = cv.Counts(con, a.opp_moves.format(colour="black" if our_white else "white"), "opp")
    mm = open_eval_arrays()

    def lookup(hs):
        return lookup_evals(hs, *mm)
    cvs = {r["cell"]: (r["a"], r["k"]) for r in pl.read_parquet(cv.CURVES).iter_rows(named=True)}
    curves = (cvs["stm1000-1799_vs_opp1800+"], cvs["stm1800+_vs_opp1000-1799"])
    maia = cv.Maia()
    sf = cv.StockfishFill(a.sf_cache, 10, a.sf_procs)
    chunks = []
    try:
        for ci in range(0, pick.height, a.chunk):
            pp = parts / f"c{ci // a.chunk:05d}.parquet"
            chunks.append(pp)
            if pp.exists():
                continue
            ch = pick.slice(ci, a.chunk)
            hs, epds = ch["hash"].to_list(), ch["epd"].to_list()
            cs = opp.get(hs)
            ms = maia.probs(epds, a.opp_elo, a.our_elo, hs)
            # replies grouped by their game count, as rollouts plays K games per start
            groups: dict[int, list] = {}
            n_replies = [0] * len(hs)
            covered = [0.0] * len(hs)
            for i, (c, m) in enumerate(zip(cs, ms)):
                rep = below_floor_replies(epds[i], cv.blend(c, m, a.k, a.min_p), ch["children"][i])
                rep = select_replies(rep, a.max_replies)
                s = sum(r[2] for r in rep)
                n_replies[i], covered[i] = len(rep), s
                for h, e, p in rep:
                    K = max(2, 2 ** round(np.log2(max(p * a.games, 1.0))))
                    groups.setdefault(K, []).append((i, p / s, h, e))
            num, var, wt = np.zeros(len(hs)), np.zeros(len(hs)), np.zeros(len(hs))
            plies = ch["ply"].to_list()
            for K, rows in sorted(groups.items()):
                st = [(h, e, 1.0, plies[i] + 1, None, 0.0, 0.0) for i, _p, h, e in rows]
                ro = SimpleNamespace(tau=0.0, rollouts=K, max_ply=a.max_ply, k=a.k, min_p=a.min_p,
                                     our_elo=a.our_elo, opp_elo=a.opp_elo)
                lv, _ = cv.rollouts(st, {}, our_white, 0, ro, a.seed, ours, opp, maia, lookup)
                v0, se = per_start(lv, len(st), lookup, sf, curves, our_white)
                for (i, p, _h, _e), v, s_ in zip(rows, v0.tolist(), se.tolist()):
                    if v == v:
                        num[i] += p * v
                        var[i] += p * p * s_ * s_
                        wt[i] += p
            v_all = np.where(wt > 0, num / np.where(wt > 0, wt, 1.0), np.nan)
            res = ch.select("hash", "ply", "reach", "share", "value", "engine", "weight").with_columns(
                pl.Series("v0", v_all), pl.Series("se", np.sqrt(var)),
                pl.Series("n_replies", n_replies, dtype=pl.Int32), pl.Series("covered", covered))
            tmp = pp.with_suffix(".tmp")
            res.write_parquet(tmp)
            tmp.replace(pp)
            print(f"  {min(ci + a.chunk, pick.height):,}/{pick.height:,} ({time.time() - t0:,.0f}s)", flush=True)
    finally:
        maia.close()
        sf.close()
    tab = pl.concat([pl.read_parquet(p) for p in chunks])
    tmp = out.with_suffix(".parquet.tmp")
    tab.write_parquet(tmp)
    tmp.replace(out)
    m, hold = fit(tab), holdout_check(tab)
    Path(str(out) + ".meta.json").write_text(json.dumps({**cfg, "rows": tab.height, "fit": m,
                                                         "holdout": hold}, indent=1), encoding="utf-8")
    print(json.dumps({"fit": m, "holdout": hold}, indent=1))


if __name__ == "__main__":
    main()
