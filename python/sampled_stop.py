"""Stop values estimated by sampling the referee's own model (2026-10-31, owner's decision on
the follow-up review's R4).

The budget builder values every place a book can end with a stop model. The weak stop model's
W(x) -- one move of ours like 1000-1799 players, one pool reply, then the engine -- measured
0.149 ABOVE the referee's value at the White lifetime book's exits (stop_alignment.py; rank
correlation 0.75), because the referee goes on simulating to ply 30 (about nine weak moves of
ours a game). Book re-entry was worth +0.0006, so a value per (position, ply) loses almost
nothing. This module gives the builder the referee's value instead:

  1. dump    build_budget_books --dump-stop-positions PATH writes every stop position of the
             build's graph -- our-turn x (stop values, opponent replies leaving the subgraph,
             replies under the share floor) and opponent-turn y (after a candidate leaving it)
             -- with its ply, optimistic reach and weak-stop value, then stops.
  2. sample  this script's CLI: for --top positions of each side (the most reached in each ply
             band, so the calibration covers every band it is applied to), --games
             sampled games from (position, ply) to ply 30 with NO book, exactly as the referee
             plays them (convertibility_eval.rollouts: its move tables, Maia prior, cut-offs,
             engine and uniform curve), giving V0 and its sampling SE. Resumable by chunk.
  3. apply   build_budget_books --sampled-stop TABLE: per side, a calibration V0 ~ a(ply band)
             + b * stop is fitted on the sampled rows (reach-weighted); every unsampled position
             gets its prediction, and every sampled one the empirical-Bayes blend
             pred + lam (V0 - pred), lam = tau^2 / (tau^2 + se^2), tau^2 the calibration's
             residual variance net of sampling noise. The shrinkage keeps the optimiser from
             chasing positions whose few games happened to go well (the winner's curse).

The table carries the referee settings it was sampled under (<table>.meta.json); the builder
records them, the calibration and the counts in the book's meta.

THE HORIZON (2026-11-01). A referee horizon H means the last ply at which a move is PLAYED is
H - 1, and positions at ply H are leaves. The builder's --max-ply N keeps decisions AT ply N (its
children are the leaves), so a sampled-stop build must use --max-ply H - 1: --max-ply 30 let a
book learn a ply-30 move the referee never plays and valued ply-31 positions. The builder refuses
a table whose horizon is not its max_ply + 1. Legacy recipes keep their meaning of --max-ply.

BOUNDARY VALUES ARE NOT ESTIMATED. A stop position whose game is over gets its result; one at or
past the horizon gets the engine's eval through the uniform curve, the referee's own value
there. Neither is sampled, neither enters the calibration, and neither gets its prediction.

PLY IS APPROXIMATED (owner's decision, 2026-10-31, option A of three). The referee's value of a
position depends on its ply (fewer moves left at a later one), but the builder's graph has one
node per position -- the move one memorises there is shared by every move order -- so each
stop position carries ONE value, the one at the ply of its HIGHEST-REACH ROUTE
(budget_core.route_plies; ties to the shortest). The objective averages over the games actually
played, so one value should be the one at the ply most games reach; the earliest ply would
favour the shortest move order, usually the rare one. The route's reach is optimistic -- the
best single path, every candidate of ours playable -- so it is a proxy for where the final book's
games arrive, and a position reached at other plies gets this ply's value there too (1.e4 e6
2.d4 d5 3.Nc3 dxe4 4.Nxe4 Nf6 5.Nxf6+ gxf6 at ply 10 is 1.e4 d5 2.exd5 Qxd5 3.Nc3 Qd8 4.d4 e6
5.Ne4 Nf6 6.Nxf6+ gxf6 at ply 12). A sample taken at any other ply is REFUSED. Stop values feed
the move choice, so this can change a learned move: it has to be measured on real exits (actual
arrival ply against this one), not assumed small. Option B (a value averaged over the plies
weighted by arrivals; exact for the stop values, not for the whole builder) is the next step if
that mismatch is material, or at a longer horizon; option C (a node per (position, ply)) was
judged not worth its refactor. n_plies / multi_ply_reach_share are graph diagnostics: optimistic
reach summed over stop positions, NOT the probability of a mismatched exit.

    .venv/Scripts/python.exe python/sampled_stop.py --dump PATH --colour white --pool STATS \\
        --max-ply 30 --top 100000 --games 32 --our-moves DIR --opp-moves DIR --out TABLE
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

BANDS = (0, 10, 14, 18, 22, 999)
DUMP_SCHEMA = {"side": pl.Utf8, "hash": pl.Int64, "ply": pl.Int32, "reach": pl.Float64,
               "stop": pl.Float64, "epd": pl.Utf8, "parent_hash": pl.Int64, "parent_epd": pl.Utf8}


def ply_sets(g, horizon: int) -> dict[int, set[int]]:
    """A diagnostic: every ply up to the horizon each subgraph node can be reached at, walking
    the subgraph level by level from the root."""
    out: dict[int, set[int]] = {}
    level, t = {g.root}, 0
    while level and t <= horizon:
        nxt = set()
        for h in level:
            out.setdefault(h, set()).add(t)
            n = g.our.get(h)
            if n is not None:
                nxt.update(ch for _s, ch, in_sub, _lv in n.cands if in_sub and ch is not None)
            else:
                o = g.opp.get(h)
                if o is not None:
                    nxt.update(ch for _p, ch, in_sub, _lv in o.kids if in_sub and ch is not None)
        level, t = nxt, t + 1
    return out


def stop_positions(g) -> pl.DataFrame:
    """Every stop position of graph g (built with track_route_ply): side 'x' (our turn) or 'y'
    (opponent's), the ply of its highest-reach route, how many plies up to the horizon it can
    be reached at (n_plies; a diagnostic), that route's optimistic reach, and its EPD or (for a
    position outside the subgraph) its parent's hash and EPD. One row per (side, hash). Ply and
    reach always come from the same route: an outside position takes them from its most likely
    incoming edge (the parent's reach times that edge's share), ties to the shorter."""
    if not g.route_ply:
        raise ValueError("stop_positions needs build_graph(track_route_ply=True)")
    rows: dict[tuple, list] = {}
    horizon = g.meta["max_ply"] + 1
    plies = ply_sets(g, horizon)
    reached: dict[tuple, set] = {}
    route = g.route_ply

    def add(side, h, ply, reach, epd, ph, pepd, at):
        k = (side, h)
        reached.setdefault(k, set()).update(t for t in at if t <= horizon)
        r = rows.get(k)
        if r is None:
            rows[k] = [side, h, ply, reach, None, epd, ph, pepd]
        else:
            if reach > r[3] or (reach == r[3] and ply < r[2]):
                r[2], r[3], r[6], r[7] = ply, reach, ph, pepd
            if r[5] is None and epd is not None:
                r[5] = epd
    for h, n in g.our.items():
        p0 = route.get(h, g.ply.get(h, 0))
        ph = plies.get(h, {p0})
        add("x", h, p0, g.reach.get(h, 0.0), g.epd.get(h), None, None, ph)
        for _san, ch, in_sub, _lv in n.cands:
            if ch is not None and not in_sub:
                add("y", ch, p0 + 1, g.reach.get(h, 0.0), g.epd.get(ch), h, g.epd.get(h),
                    {t + 1 for t in ph})
    for h, n in g.opp.items():
        r0, p0 = g.reach.get(h, 0.0), route.get(h, g.ply.get(h, 0))
        nxt = {t + 1 for t in plies.get(h, {p0})}
        for p, ch, in_sub, _lv in n.kids:
            if ch is not None and not in_sub:
                add("x", ch, p0 + 1, r0 * p, g.epd.get(ch), h, g.epd.get(h), nxt)
        for p, ch, _lv in n.floor:
            add("x", ch, p0 + 1, r0 * p, g.epd.get(ch), h, g.epd.get(h), nxt)
    df = pl.DataFrame(list(rows.values()), schema=DUMP_SCHEMA, orient="row")
    return df.with_columns(pl.Series("n_plies", [len(reached[(sd, h)]) for sd, h in
                                                 df.select("side", "hash").iter_rows()],
                                     dtype=pl.Int32))


def terminal_outcomes(edges_by_parent: dict, our_white: bool) -> dict[int, float]:
    """Positions where the game is over -> the result, our view. A finished game has no outgoing
    edges, so only children without any are checked on the board; a SAN ending in # needs no
    board for the mate, only for whose move it was."""
    out: dict[int, float] = {}
    for es in edges_by_parent.values():
        epd = es[0].get("parent_epd")
        if epd is None:
            continue
        try:
            b0 = chess.Board(epd + " 0 1")
        except ValueError:
            continue
        for e in es:
            ch, san = e.get("child_hash"), e["move_san"]
            if ch is None or ch in edges_by_parent or ch in out:
                continue
            if san.endswith("#"):
                out[ch] = 1.0 if (b0.turn == chess.WHITE) == our_white else 0.0
                continue
            try:
                b0.push_san(san)
            except ValueError:
                continue
            if b0.is_game_over(claim_draw=False):
                r = b0.result(claim_draw=False)
                out[ch] = 0.5 if r == "1/2-1/2" else (1.0 if (r == "1-0") == our_white else 0.0)
            b0.pop()
    return out


def band(ply: np.ndarray) -> np.ndarray:
    return np.searchsorted(np.array(BANDS[1:]), ply, side="right")


def fit(tab: pl.DataFrame) -> dict:
    """Per side: V0 ~ a[band] + b * stop by reach-weighted least squares, and tau^2 = the
    weighted residual variance minus the weighted mean sampling variance (floored)."""
    model = {}
    for side in ("x", "y"):
        t = tab.filter((pl.col("side") == side) & pl.col("v0").is_not_null() & pl.col("stop").is_not_null())
        if t.height < 50:
            model[side] = None
            continue
        bd = band(t["ply"].to_numpy())
        nb = len(BANDS) - 1
        X = np.zeros((t.height, nb + 1))
        X[np.arange(t.height), bd] = 1.0
        X[:, nb] = t["stop"].to_numpy()
        y = t["v0"].to_numpy()
        w = np.maximum(t["reach"].to_numpy(), 1e-12)
        w = w / w.sum()
        used = np.unique(bd)
        cols = list(used) + [nb]
        sw = np.sqrt(w)
        coef, *_ = np.linalg.lstsq(X[:, cols] * sw[:, None], y * sw, rcond=None)
        a = np.full(nb, np.nan)
        a[used] = coef[:-1]
        if np.isnan(a).any():                    # a band nothing was sampled in: the nearest one
            for i in range(nb):
                if np.isnan(a[i]):
                    j = min(used, key=lambda u: abs(u - i))
                    a[i] = a[j]
        b = float(coef[-1])
        res = y - (a[bd] + b * X[:, nb])
        se2 = t["se"].fill_null(0.0).to_numpy() ** 2
        tau2 = max(float((w * res ** 2).sum() - (w * se2).sum()), 1e-6)
        model[side] = {"a": a.tolist(), "b": b, "tau2": tau2, "n": t.height,
                       "resid_rms": float(np.sqrt((w * res ** 2).sum())),
                       "mean_shift": float((w * (y - X[:, nb])).sum())}
    return model


def predict(m: dict, ply: np.ndarray, stop: np.ndarray) -> np.ndarray:
    return np.clip(np.array(m["a"])[band(ply)] + m["b"] * stop, 0.0, 1.0)


def apply(W: dict, R: dict, info: pl.DataFrame, tab: pl.DataFrame, horizon: int | None = None,
          terminal: dict | None = None, evals=None) -> tuple[dict, dict, dict]:
    """Replace the weak-stop W (our turn) and R (opponent's turn): finished games by their
    result, positions at or past the horizon by the engine (evals: hashes -> our expected score,
    NaN if unevaluated), the rest by the calibrated, shrunk samples. info: stop_positions(g).
    Returns (W, R, report)."""
    terminal = terminal or {}
    # a sample counts only at the ply the graph values the position at (see PLY IS APPROXIMATED)
    keyed = tab.join(info.select("side", "hash", pl.col("ply").alias("_gply")), on=["side", "hash"],
                     how="left")
    refused = keyed.filter(pl.col("_gply").is_not_null() & (pl.col("_gply") != pl.col("ply"))).height
    tab = keyed.filter(pl.col("_gply").is_null() | (pl.col("_gply") == pl.col("ply"))).drop("_gply")
    if horizon is not None:
        tab = tab.filter(pl.col("ply") < horizon)
    if terminal:
        tab = tab.filter(~pl.col("hash").is_in(list(terminal)))
    model = fit(tab)
    rep = {"model": model, "sampled": {}, "predicted": {}, "cross_ply_refused": refused,
           "terminal": {}, "horizon": {}, "horizon_no_eval": {}}
    if "n_plies" in info.columns:
        rep["multi_ply_reach_share"] = {
            sd: float(i.filter(pl.col("n_plies") > 1)["reach"].sum() / max(i["reach"].sum(), 1e-300))
            for sd in ("x", "y") if (i := info.filter(pl.col("side") == sd)).height}
    for side, D in (("x", W), ("y", R)):
        inf = info.filter(pl.col("side") == side).select("hash", "ply")
        ply_of = dict(zip(inf["hash"].to_list(), inf["ply"].to_list()))
        m = model.get(side)
        new: dict = {}
        if m is None or not D:
            rep["sampled"][side] = rep["predicted"][side] = 0
        else:
            hs = np.array([h for h in D if h in ply_of], dtype=np.int64)
            st = np.array([D[h] for h in hs.tolist()])
            pr = predict(m, np.array([ply_of[h] for h in hs.tolist()]), st)
            new = dict(zip(hs.tolist(), pr.tolist()))
            t = tab.filter((pl.col("side") == side) & pl.col("v0").is_not_null())
            n_s = 0
            for h, v0, se in t.select("hash", "v0", "se").iter_rows():
                if h in new:
                    lam = m["tau2"] / (m["tau2"] + (se or 0.0) ** 2)
                    new[h] = float(np.clip(new[h] + lam * (v0 - new[h]), 0.0, 1.0))
                    n_s += 1
            rep["sampled"][side], rep["predicted"][side] = n_s, len(new) - n_s
        # the boundary last: these values are known, whatever a sample or prediction said
        n_h = n_miss = 0
        if horizon is not None and evals is not None:
            hz = np.array([h for h, p in ply_of.items() if p >= horizon and h not in terminal],
                          dtype=np.int64)
            if len(hz):
                for h, v in zip(hz.tolist(), np.asarray(evals(hz)).tolist()):
                    if v == v:
                        new[h] = v
                        n_h += 1
                    else:
                        n_miss += 1
        n_t = 0
        for h in ply_of:
            if h in terminal:
                new[h] = terminal[h]
                n_t += 1
        rep["horizon"][side], rep["horizon_no_eval"][side], rep["terminal"][side] = n_h, n_miss, n_t
        D.update(new)
    return W, R, rep


def resolve_epds(dump: pl.DataFrame, pool: str) -> pl.DataFrame:
    """Fill the EPD of positions outside the subgraph from their parent's EPD and the move the
    pool records from parent to child."""
    need = dump.filter(pl.col("epd").is_null() & pl.col("parent_epd").is_not_null())
    if not need.height:
        return dump
    import duckdb
    from duck_compat import duck_pl
    con = duckdb.connect()
    con.execute("SET threads=8; SET memory_limit='8GB'")
    con.register("need", need.select("parent_hash", pl.col("hash").alias("child_hash")).to_arrow())
    mv = duck_pl(con, f"""SELECT s.parent_hash, s.child_hash, any_value(s.move_san) move_san
        FROM read_parquet('{pool.replace(chr(92), '/')}') s
        JOIN need n ON n.parent_hash = s.parent_hash AND n.child_hash = s.child_hash GROUP BY 1, 2""")
    san = {(p, c): m for p, c, m in mv.iter_rows()}
    out = []
    for r in dump.iter_rows(named=True):
        if r["epd"] is None and r["parent_epd"] is not None:
            m = san.get((r["parent_hash"], r["hash"]))
            if m is not None:
                b = chess.Board(r["parent_epd"] + " 0 1")
                try:
                    b.push_san(m)
                    r["epd"] = b.epd()
                except ValueError:
                    pass
        out.append(r)
    return pl.DataFrame(out, schema=dump.schema)


def holdout_check(tab: pl.DataFrame, modulus: int = 10) -> dict:
    """Is the calibration any good on positions it was not fitted to? Fit on the sampled rows
    with hash % modulus != 0, predict the rest. Reports, per side, the reach-weighted bias and
    RMSE against the held-out V0, the sampling noise on those V0, and the old stop value's own
    RMSE for comparison. Prediction error net of the noise is what an unsampled position gets."""
    out = {}
    held = tab.filter(pl.col("hash") % modulus == 0)
    model = fit(tab.filter(pl.col("hash") % modulus != 0))
    for side in ("x", "y"):
        m = model.get(side)
        t = held.filter((pl.col("side") == side) & pl.col("v0").is_not_null() & pl.col("stop").is_not_null())
        if m is None or t.height < 10:
            out[side] = None
            continue
        w = np.maximum(t["reach"].to_numpy(), 1e-12)
        w = w / w.sum()
        y, st = t["v0"].to_numpy(), t["stop"].to_numpy()
        d = predict(m, t["ply"].to_numpy(), st) - y
        noise2 = float((w * t["se"].fill_null(0.0).to_numpy() ** 2).sum())
        rmse2 = float((w * d * d).sum())
        out[side] = {"n": t.height, "bias": float((w * d).sum()), "rmse": float(np.sqrt(rmse2)),
                     "noise_rms": float(np.sqrt(noise2)),
                     "rmse_net_of_noise": float(np.sqrt(max(rmse2 - noise2, 0.0))),
                     "old_stop_rmse": float(np.sqrt((w * (st - y) ** 2).sum()))}
    return out


def file_identity(path) -> dict:
    st = Path(path).stat()
    return {"path": str(path), "size": st.st_size, "mtime_ns": st.st_mtime_ns}


def main():
    import convertibility_eval as cv
    import duckdb
    from eval_arrays import lookup_evals, open_eval_arrays
    from stop_alignment import per_start
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", required=True, help="build_budget_books --dump-stop-positions output")
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--pool", required=True, help="the build's --stats pool (to resolve EPDs)")
    ap.add_argument("--top", type=int, default=100_000, help="positions per side")
    ap.add_argument("--games", type=int, default=32)
    ap.add_argument("--chunk", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=21)
    ap.add_argument("--our-moves", required=True)
    ap.add_argument("--opp-moves", required=True)
    ap.add_argument("--k", type=float, default=100.0)
    ap.add_argument("--min-p", type=float, default=0.005)
    ap.add_argument("--max-ply", type=int, default=30, help="the referee's horizon H; the dump's "
                    "graph must be capped at H - 1")
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
    pool = a.pool.replace(chr(92), "/")
    bad = {k: (dump_meta.get(k), v) for k, v in (("colour", a.colour), ("horizon", a.max_ply),
                                                 ("stats", pool)) if dump_meta.get(k) != v}
    if bad:
        raise SystemExit(f"FATAL: the dump does not match this run (dump, run): {bad}")
    out = Path(a.out)
    parts = Path(str(out) + ".parts")
    parts.mkdir(parents=True, exist_ok=True)
    cfg = {k: getattr(a, k) for k in ("colour", "games", "seed", "our_moves", "opp_moves", "k",
                                      "min_p", "max_ply", "our_elo", "opp_elo", "top", "chunk")}
    cfg["referee"] = "convertibility_eval.rollouts, empty book, uniform curve, SF depth 10 fill"
    cfg["dump"] = dump_meta
    cfg["dump_file"] = file_identity(a.dump)
    cfg["code"] = cv.identity()
    # every chunk file belongs to exactly this selection: a different dump, --top or --chunk would
    # mix graphs or misread what a chunk holds, so it is refused (delete the parts to start over)
    mp = parts / "_config.json"
    if mp.exists() and json.loads(mp.read_text(encoding="utf-8")) != json.loads(json.dumps(cfg)):
        raise SystemExit(f"FATAL: {parts} was sampled under other settings; delete it to start over")
    mp.write_text(json.dumps(cfg, indent=1), encoding="utf-8")
    dump = pl.read_parquet(a.dump)
    # boundary positions have known values (see BOUNDARY VALUES): never sampled
    n_boundary = dump.filter(pl.col("exact").is_not_null() | (pl.col("ply") >= a.max_ply)).height
    live = dump.filter(pl.col("exact").is_null() & (pl.col("ply") < a.max_ply)).drop("exact")
    # the most-reached positions of each side, spread over the ply bands (top/bands per band,
    # the unused share going to the overall top) so the calibration is fitted where it is used
    live = live.with_columns(pl.Series("band", band(live["ply"].to_numpy())))
    picks = []
    for sd in ("x", "y"):
        d = live.filter(pl.col("side") == sd).sort("reach", "hash", descending=[True, False])
        per = a.top // (len(BANDS) - 1)
        strat = pl.concat([d.filter(pl.col("band") == b).head(per) for b in range(len(BANDS) - 1)])
        rest = d.join(strat.select("hash"), on="hash", how="anti").head(max(0, a.top - strat.height))
        picks.append(pl.concat([strat, rest]).drop("band"))
    pick = pl.concat(picks)
    pick = resolve_epds(pick, pool).filter(pl.col("epd").is_not_null())
    print(f"{pick.height:,} positions to sample ({dump.height:,} stop positions in the dump, "
          f"{n_boundary:,} of them boundary)", flush=True)
    ro = SimpleNamespace(tau=0.0, rollouts=a.games, max_ply=a.max_ply, k=a.k, min_p=a.min_p,
                         our_elo=a.our_elo, opp_elo=a.opp_elo)
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
            st = [(h, e, 1.0, p, None, 0.0, 0.0) for h, e, p in ch.select("hash", "epd", "ply").iter_rows()]
            lv, _ = cv.rollouts(st, {}, our_white, 0, ro, a.seed, ours, opp, maia, lookup)
            v0, se = per_start(lv, len(st), lookup, sf, curves, our_white)
            res = ch.select("side", "hash", "ply", "reach", "stop").with_columns(
                pl.Series("v0", v0), pl.Series("se", se), pl.lit(a.games).alias("games"))
            tmp = pp.with_suffix(".tmp")
            res.write_parquet(tmp)
            tmp.replace(pp)
            print(f"  {min(ci + a.chunk, pick.height):,}/{pick.height:,} ({time.time() - t0:,.0f}s)", flush=True)
    finally:
        maia.close()
        sf.close()
    tab = pl.concat([pl.read_parquet(p) for p in chunks])    # this selection's chunks, no others
    tmp = out.with_suffix(".parquet.tmp")
    tab.write_parquet(tmp)
    tmp.replace(out)
    m, hold = fit(tab), holdout_check(tab)
    Path(str(out) + ".meta.json").write_text(json.dumps({**cfg, "rows": tab.height, "fit": m,
                                                         "holdout": hold}, indent=1), encoding="utf-8")
    print(json.dumps({"fit": m, "holdout": hold}, indent=1))


if __name__ == "__main__":
    main()
