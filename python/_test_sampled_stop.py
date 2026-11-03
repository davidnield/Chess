"""Sampled stop values (sampled_stop.py; build_budget_books --dump-stop-positions/--sampled-stop).

  P  stop_positions lists every place the graph can end: our nodes and the opponent replies
     that leave the subgraph or fall under the share floor (side x), candidates leaving it
     (side y), each with its highest-reach route's ply and reach.
  R  the route ply carries reach and ply together: a long likely route beats a short unlikely
     one whichever is found first, edge order does not matter, ties go to the shorter route, and
     outside positions follow their most likely incoming edge.
  H  a referee horizon H is a graph capped at H - 1, from the collector on: no decision and no
     stop position past the boundary, and a value past it cannot change the book.
  B  boundary positions get their known values: the result of a finished game, the engine at
     the horizon; neither is estimated.
  F  fit recovers a known calibration V0 = a[band] + b * stop, and its tau^2 nets out the
     sampling noise.
  A  apply: an unsampled position gets the calibration's prediction; a sampled one is shrunk
     toward it by tau^2 / (tau^2 + se^2) (se 0 -> the sampled value itself; a huge se -> the
     prediction); positions not in the graph's stop set are left alone.

Run: .venv/Scripts/python.exe python/_test_sampled_stop.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import chess
import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
import sampled_stop as ss  # noqa: E402
from build_budget_books import collect_subgraph_edges  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


print("P  stop positions")
# White book. Root R (ours) -> x (to A, opponent) or y (to Z, a dead end); A -> B (ours, in
# the subgraph, one move to a dead end C) or D (a reply leaving the subgraph).
R, A, Z, B, C, D = 1, 2, 3, 4, 5, 6
edges = {R: [{"move_san": "x", "child_hash": A, "total": 100, "white_score_avg": 0.6, "parent_epd": "r"},
             {"move_san": "y", "child_hash": Z, "total": 100, "white_score_avg": 0.5}],
         A: [{"move_san": "b", "child_hash": B, "total": 90, "white_score_avg": 0.6, "parent_epd": "a"},
             {"move_san": "d", "child_hash": D, "total": 10, "white_score_avg": 0.4}],
         B: [{"move_san": "c", "child_hash": C, "total": 50, "white_score_avg": 0.7, "parent_epd": "b"}]}
g = bc.build_graph(edges, R, True, gates=False, prior_strength=1.0, eval_weight=0.0, eps=0.0,
                   share_floor=0.0, track_route_ply=True)
sp = ss.stop_positions(g)
x = dict(sp.filter(pl.col("side") == "x").select("hash", "ply").iter_rows())
y = dict(sp.filter(pl.col("side") == "y").select("hash", "ply").iter_rows())
check("our nodes and the reply leaving the subgraph are x, at their plies",
      x == {R: 0, B: 2, D: 2}, str(x))
check("candidates leaving the subgraph are y", y == {Z: 1, C: 3}, str(y))
reach = dict(sp.filter(pl.col("side") == "x").select("hash", "reach").iter_rows())
check("a leaving reply's reach = its parent's x its share", abs(reach[D] - 0.1) < 1e-12, str(reach))
check("a position outside the subgraph carries its parent's hash and EPD",
      sp.filter(pl.col("hash") == D).select("parent_hash", "parent_epd").row(0) == (A, "a"))

check("no position here is reachable at two plies", sp["n_plies"].max() == 1)

try:
    ss.stop_positions(bc.build_graph(edges, R, True, gates=False, prior_strength=1.0, eval_weight=0.0,
                                     eps=0.0, share_floor=0.0))
    check("a graph without route plies is refused", False)
except ValueError:
    check("a graph without route plies is refused", True)

print("M  a position reached at two plies (option A)")
# R -> A (opp); A -> B (ours, ply 2) or C (ours); C -> E (opp); E -> B again, at ply 4
R2, A2, B2, C2, E2, G2, H2 = 11, 12, 13, 14, 15, 16, 17
e2 = {R2: [{"move_san": "a", "child_hash": A2, "total": 100, "white_score_avg": 0.6, "parent_epd": "r"}],
      A2: [{"move_san": "b", "child_hash": B2, "total": 60, "white_score_avg": 0.6, "parent_epd": "a"},
           {"move_san": "c", "child_hash": C2, "total": 40, "white_score_avg": 0.6}],
      C2: [{"move_san": "e", "child_hash": E2, "total": 40, "white_score_avg": 0.6, "parent_epd": "c"}],
      E2: [{"move_san": "f", "child_hash": B2, "total": 40, "white_score_avg": 0.6, "parent_epd": "e"}],
      B2: [{"move_san": "g", "child_hash": G2, "total": 60, "white_score_avg": 0.6, "parent_epd": "b"},
           {"move_san": "h", "child_hash": H2, "total": 1, "white_score_avg": 0.6}]}
g2 = bc.build_graph(e2, R2, True, gates=False, prior_strength=1.0, eval_weight=0.0, eps=0.0,
                    share_floor=0.0, track_route_ply=True)
sp2 = ss.stop_positions(g2)
row = sp2.filter((pl.col("side") == "x") & (pl.col("hash") == B2)).select("ply", "n_plies").row(0)
check("B: most reached at ply 2 (60%), reachable at 2 plies", row == (2, 2), str(row))
check("B's dead-end move G inherits both plies", sp2.filter(pl.col("hash") == G2)["n_plies"][0] == 2)
tabm = pl.DataFrame({"side": ["x", "x"], "hash": [B2, C2], "ply": [4, 2], "reach": [1.0, 1.0],
                     "stop": [0.6, 0.6], "v0": [0.2, 0.3], "se": [0.0, 0.0]})
_, _, repm = ss.apply({B2: 0.6, C2: 0.6}, {}, sp2, tabm)
check("a sample at ply 4 for a position valued at ply 2 is refused", repm["cross_ply_refused"] == 1,
      str(repm["cross_ply_refused"]))
check("the multi-ply share of stop reach is reported", repm["multi_ply_reach_share"]["x"] > 0,
      str(repm["multi_ply_reach_share"]))

print("R  the highest-reach route's ply")
OP, X, Y, Z, W_, LEAF = 21, 22, 23, 24, 25, 26


def e_(san, ch, n):
    return {"move_san": san, "child_hash": ch, "total": n, "white_score_avg": 0.5}


def rgraph(kids, flip=False):
    es = {k: (list(reversed(v)) if flip else list(v)) for k, v in kids.items()}
    return bc.build_graph(es, R, True, gates=False, prior_strength=1.0, eval_weight=0.0, eps=0.0,
                          share_floor=0.0, track_route_ply=True)


# the short route is the unlikely edge of a likely parent: 10% to X at ply 2; 90% via Y and Z to X
# at ply 4. The legacy ply takes the short route.
short_edge = {R: [e_("a", OP, 10)],
              OP: [e_("x", X, 10), e_("y", Y, 90)],
              Y: [e_("z", Z, 10)],
              Z: [e_("w", X, 10)],
              X: [e_("m", LEAF, 10)]}
for flip in (False, True):
    g = rgraph(short_edge, flip)
    check(f"a 10% short edge does not set X's ply ({'reversed' if flip else 'given'} order)",
          g.route_ply[X] == 4 and abs(g.reach[X] - 0.9) < 1e-12 and g.ply[X] == 2,
          f"route {g.route_ply[X]}, reach {g.reach[X]:.2f}, legacy {g.ply[X]}")
    rows = dict(ss.stop_positions(g).filter(pl.col("side") == "x").select("hash", "ply").iter_rows())
    check("...and the stop table says ply 4", rows[X] == 4, str(rows[X]))
# the long route is finalised first: 90% at ply 4 is found before 10% at ply 2 (the review's case)
long_first = {R: [e_("a", OP, 10)],
              OP: [e_("y", Y, 90), e_("x", W_, 10)],
              Y: [e_("z", Z, 10)],
              Z: [e_("w", X, 10)],
              W_: [e_("u", 27, 10)],
              27: [e_("v", 28, 10)],
              28: [e_("t", 29, 10)],
              29: [e_("s", X, 10)],
              X: [e_("m", LEAF, 10)]}
g = rgraph(long_first)
check("a later, less likely route does not move it", g.route_ply[X] == 4, str(g.route_ply[X]))
# equal reach by two routes: the shorter, in either edge order
tie = {R: [e_("a", OP, 10)],
       OP: [e_("p", Y, 50), e_("q", W_, 50)],
       Y: [e_("z", X, 10)],
       W_: [e_("u", 27, 10)],
       27: [e_("v", 28, 10)],
       28: [e_("t", X, 10)],
       X: [e_("m", LEAF, 10)]}
check("equal reach: the shorter route, either edge order",
      rgraph(tie).route_ply[X] == 3 and rgraph(tie, True).route_ply[X] == 3,
      f"{rgraph(tie).route_ply[X]} {rgraph(tie, True).route_ply[X]}")
# an outside stop: reached by a 10% reply at ply 2 and a 90% reply at ply 4
outside = {R: [e_("a", OP, 10)],
           OP: [e_("x", D, 10), e_("y", Y, 90)],
           Y: [e_("z", Z, 10)],
           Z: [e_("w", D, 10)]}
for flip in (False, True):
    g = rgraph(outside, flip)
    r = ss.stop_positions(g).filter((pl.col("side") == "x") & (pl.col("hash") == D)).select(
        "ply", "reach", "parent_hash").row(0)
    check(f"an outside stop takes ply and reach from one edge ({'reversed' if flip else 'given'} order)",
          r[0] == 4 and abs(r[1] - 0.9) < 1e-12 and r[2] == Z, str(r))
check("the route ply is off by default", bc.build_graph(outside, R, True, gates=False, prior_strength=1.0,
                                                         eval_weight=0.0, eps=0.0, share_floor=0.0).route_ply == {})

print("H  the horizon, collector to builder")
with tempfile.TemporaryDirectory() as td:
    pq = Path(td) / "pool.parquet"
    # a line of 40 plies from the start hash 1000: 1000 -> 1001 -> ...
    rows = []
    for i in range(40):
        rows.append({"parent_hash": 1000 + i, "move_san": f"m{i}", "child_hash": 1000 + i + 1,
                     "total": 1000, "white_wins": 500, "draws": 0, "black_wins": 500,
                     "white_score_avg": 0.5, "parent_epd": f"e{i}", "ply": i + 1})
    pl.DataFrame(rows).write_parquet(pq)
    con = duckdb.connect()
    for H in (29, 30):
        for our_white in (True, False):
            c = "White" if our_white else "Black"
            df = collect_subgraph_edges(con, pq.as_posix(), root=1000, our_white=our_white, eps=0.0,
                                        max_ply=H - 1, share_floor=0.0, min_games=1)
            bp: dict = {}
            for r in df.iter_rows(named=True):
                bp.setdefault(r["parent_hash"], []).append(r)
            g = bc.build_graph(bp, 1000, our_white, gates=False, prior_strength=1.0, eval_weight=0.0,
                               eps=0.0, share_floor=0.0, max_ply=H - 1, track_route_ply=True)
            last = max(g.ply[h] for h, n in g.our.items() if n.cands)
            want = H - 1 if ((H - 1) % 2 == 0) == our_white else H - 2
            sp3 = ss.stop_positions(g)
            check(f"H {H}, {c}: last decision at ply {want}, the dump ends at H",
                  last == want and sp3["ply"].max() == H and last < H,
                  f"last {last}, dump max {sp3['ply'].max()}")
            # a value past the boundary has nowhere to go: nothing in the graph is past it
            check(f"H {H}, {c}: no node past H - 1", max(g.ply[h] for h in (*g.our, *g.opp)) <= H - 1)
    # the defect this contract removes: a graph capped AT the horizon decides there, and an
    # attractive value past it buys a move the referee never plays. --max-ply keeps that meaning.
    df = collect_subgraph_edges(con, pq.as_posix(), root=1000, our_white=True, eps=0.0, max_ply=30,
                                share_floor=0.0, min_games=1)
    bp = {}
    for r in df.iter_rows(named=True):
        bp.setdefault(r["parent_hash"], []).append(r)
    g = bc.build_graph(bp, 1000, True, gates=False, prior_strength=1.0, eval_weight=0.0, eps=0.0,
                       share_floor=0.0, max_ply=30, track_route_ply=True)
    late = [h for h, n in g.our.items() if n.cands and g.ply[h] == 30]
    for h, n in g.our.items():
        n.l_node = 0.1
    bc.apply_stop_values(g, {}, {n.cands[0][1]: 0.99 for n in g.our.values() if not n.cands[0][2]})
    res, _ = bc.price_search(g, 16, log=lambda m: None)
    check("capped AT 30 (legacy meaning): an attractive value past it books a ply-30 move",
          len(late) == 1 and late[0] in res["booked"], str(len(res["booked"])))

print("B  boundary values")
ch_mate, ch_stale = 91, 92
fool = chess.Board()
for m in ("f3", "e5", "g4"):
    fool.push_san(m)
pre = chess.Board("7k/8/5QK1/8/8/8/8/8 w - - 0 1")
tb = {1: [{"move_san": "Qh4#", "child_hash": ch_mate, "total": 5, "parent_epd": fool.epd()},
          {"move_san": "Nc6", "child_hash": 93, "total": 5}],
      2: [{"move_san": "Qf7", "child_hash": ch_stale, "total": 5, "parent_epd": pre.epd()},
          {"move_san": "Kh6", "child_hash": 94, "total": 5}]}
check("a mate: the mover's side wins, either book", ss.terminal_outcomes(tb, False)[ch_mate] == 1.0
      and ss.terminal_outcomes(tb, True)[ch_mate] == 0.0)
check("stalemate is a draw; ordinary moves are not finished",
      ss.terminal_outcomes(tb, True)[ch_stale] == 0.5 and 93 not in ss.terminal_outcomes(tb, True)
      and 94 not in ss.terminal_outcomes(tb, True))
infob = pl.DataFrame({"side": ["x"] * 4 + ["y"], "hash": [100, 101, 102, 103, 104],
                      "ply": [10, 30, 30, 12, 30], "reach": [1.0] * 5})
tabb = pl.DataFrame({"side": ["x"] * 60, "hash": list(range(200, 260)), "ply": [10] * 60,
                     "reach": [1.0] * 60, "stop": [0.4 + i / 100 for i in range(60)],
                     "v0": [0.3 + i / 100 for i in range(60)], "se": [0.01] * 60})
tabb = pl.concat([tabb, pl.DataFrame({"side": ["x"], "hash": [101], "ply": [30], "reach": [1.0],
                                      "stop": [0.9], "v0": [0.01], "se": [0.0]})])
Wb = {100: 0.6, 101: 0.6, 102: 0.6, 103: 0.6}
Rb = {104: 0.6}
EV = {101: 0.77, 104: 0.33}          # 102 has no eval


def evs(hs):
    return np.array([EV.get(int(h), float("nan")) for h in hs])


Wb, Rb, repb = ss.apply(Wb, Rb, infob, tabb, horizon=30, terminal={103: 1.0}, evals=evs)
check("a position at the horizon gets the engine, whatever a sample said", Wb[101] == 0.77 and Rb[104] == 0.33,
      f"{Wb[101]} {Rb[104]}")
check("a finished game gets its result", Wb[103] == 1.0)
check("a horizon position without an eval is counted, not invented",
      repb["horizon_no_eval"]["x"] == 1 and repb["horizon"]["x"] == 1 and repb["terminal"]["x"] == 1, str(repb["horizon_no_eval"]))
check("boundary rows stay out of the calibration", repb["model"]["x"]["n"] == 60, str(repb["model"]["x"]["n"]))
check("an unsampled ordinary position gets the prediction", abs(Wb[100] - 0.5) < 1e-6, f"{Wb[100]:.5f}")

print("F  calibration")
rng = np.random.default_rng(3)
n = 4000
ply = rng.integers(4, 30, n)
stop = rng.uniform(0.3, 0.9, n)
a_true = np.array([-0.10, -0.12, -0.14, -0.15, -0.16])
truth = a_true[ss.band(ply)] + 0.9 * stop + rng.normal(0, 0.05, n)     # tau = 0.05
se = np.full(n, 0.04)
v0 = truth + rng.normal(0, 1, n) * se
tab = pl.DataFrame({"side": ["x"] * n, "hash": np.arange(n), "ply": ply, "reach": np.ones(n),
                    "stop": stop, "v0": v0, "se": se})
m = ss.fit(tab)["x"]
check("the slope is recovered", abs(m["b"] - 0.9) < 0.03, f"{m['b']:.4f}")
check("the band intercepts are recovered", np.allclose(m["a"], a_true, atol=0.03), str(np.round(m["a"], 3)))
check("tau^2 nets out the sampling noise", abs(m["tau2"] - 0.05 ** 2) < 0.0006, f"{m['tau2']:.5f}")

print("A  apply")
W = {int(h): float(s) for h, s in zip(range(n), stop)}
W[n] = 0.6                                            # in the stop set, not sampled
W[n + 1] = 0.6                                        # not in the stop set at all
info = pl.concat([tab.select("side", "hash", "ply"),
                  pl.DataFrame({"side": ["x"], "hash": [n], "ply": [12]})])
se2 = se.copy()
se2[0], se2[1] = 0.0, 10.0                            # exact, and hopelessly noisy
tab2 = tab.with_columns(pl.Series("se", se2))
W2, R2, rep = ss.apply(dict(W), {}, info, tab2)
m = ss.fit(tab2)["x"]                                 # apply fits on the table it is given
pred = lambda h, p: float(ss.predict(m, np.array([p]), np.array([W[h]]))[0])  # noqa: E731
check("an unsampled stop position gets the prediction",
      abs(W2[n] - pred(n, 12)) < 1e-6, f"{W2[n]:.4f} vs {pred(n, 12):.4f}")
check("se 0: the sampled value itself", abs(W2[0] - np.clip(v0[0], 0, 1)) < 1e-9)
check("a huge se: (almost) the prediction", abs(W2[1] - pred(1, int(ply[1]))) < 1e-3)
lam = m["tau2"] / (m["tau2"] + 0.04 ** 2)
want = pred(2, int(ply[2])) + lam * (v0[2] - pred(2, int(ply[2])))
check("otherwise pred + lam (V0 - pred)", abs(W2[2] - want) < 1e-6)
check("a position outside the stop set is untouched", W2[n + 1] == 0.6)
check("counts", rep["sampled"]["x"] == n and rep["predicted"]["x"] == 1, str(rep["sampled"]))

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
