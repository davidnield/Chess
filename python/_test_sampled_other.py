"""The other bucket on the referee's scale (sampled_other.py; build_budget_books
--dump-other-positions / --sampled-other).

  O  off is an exact no-op: tracking changes no value, and without it nothing is recorded.
  T  the tracked share and value are the bucket's own part of const_base, both colours, with
     terminations and horizon games beside it.
  R  below_floor_replies takes out the pool's replies by position and renormalises the rest.
  F  fit recovers a known calibration, nets the sampling noise out of tau^2, and skips nodes the
     referee never plays a below-floor reply at.
  A  apply: a sampled node (se 0) gets the sampled value, a huge se the prediction, an unsampled
     node the prediction; const_base moves by share x the change and the root value follows through
     the optimiser's own walk; a sample at another ply is refused; too few samples fail loudly.

Run: .venv/Scripts/python.exe python/_test_sampled_other.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
import sampled_other as so  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def e_(san, ch, n, s=0.5):
    return {"move_san": san, "child_hash": ch, "total": n, "white_score_avg": s, "parent_epd": "e"}


# Root (ours) -> A (opponent); A: reply B 60 games, then aux: 40 other at .3 white score (no eval),
# 20 terminations at .9, 20 horizon at .7. So denom 140 and the other bucket's share 40/140.
R, A, B, C = 1, 2, 3, 4
edges = {R: [e_("a", A, 140)], A: [e_("b", B, 60)], B: [e_("c", C, 60)]}
aux = {A: {"other_total": 40, "other_white_wins": 8, "other_draws": 8, "other_eval_mean": None,
           "other_eval_cov": 0.0, "term_normal_total": 20, "term_normal_white_wins": 18,
           "term_normal_black_wins": 2, "term_normal_draws": 0, "horizon_total": 20,
           "horizon_white_wins": 14, "horizon_draws": 0}}
kw = dict(gates=False, prior_strength=1.0, eval_weight=0.0, eps=0.0, share_floor=0.0, aux_rows=aux)


def node_snapshot(g):
    return ({h: (n.const_base, n.kids, n.floor) for h, n in g.opp.items()},
            {h: (n.l_node, n.cands) for h, n in g.our.items()})


print("O  off is a no-op")
off = bc.build_graph(edges, R, True, **kw)
on = bc.build_graph(edges, R, True, track_other=True, track_route_ply=True, **kw)
check("tracking changes no value", node_snapshot(off) == node_snapshot(on))
check("nothing is recorded without the flag", off.other == {})

print("T  the bucket's own part of const_base")
share, value, engine = on.other[A]
check("share = other / (replies + all aux)", abs(share - 40 / 140) < 1e-15, str(share))
check("value = the bucket's score, our view", abs(value - 0.3) < 1e-15, str(value))
check("no eval for the node -> engine None", engine is None)
rest = (18 + 14) / 140      # the terminations' and horizon games' white score
check("const_base = share x value + the other buckets",
      abs(on.opp[A].const_base - (share * value + rest)) < 1e-15, str(on.opp[A].const_base))
# Black's opponent moves at the root
blk = bc.build_graph(edges, R, False, track_other=True, track_route_ply=True,
                     **{**kw, "aux_rows": {R: aux[A]}})
sb, vb, _ = blk.other[R]
check("Black: the same share at 1 - value", abs(sb - 40 / 220) < 1e-15 and abs(vb - 0.7) < 1e-15,
      f"{sb} {vb}")
check("Black: const_base holds it in our view",
      abs(blk.opp[R].const_base - (sb * vb + (2 + 6) / 220)) < 1e-15, str(blk.opp[R].const_base))

print("R  below-floor replies")
b = chess.Board()
e4 = b.copy()
e4.push_san("e4")
e5 = e4.copy()
e5.push_san("e5")
c5 = e4.copy()
c5.push_san("c5")
reps = so.below_floor_replies(e4.epd(), {"e5": 0.5, "c5": 0.3, "e6": 0.2}, [zobrist_int64(e5)])
check("the pool's reply is out", zobrist_int64(e5) not in [r[0] for r in reps])
check("the rest are renormalised, most likely first",
      reps[0][0] == zobrist_int64(c5) and abs(reps[0][2] - 0.6) < 1e-12 and abs(reps[1][2] - 0.4) < 1e-12,
      str([r[2] for r in reps]))
check("the reply's EPD is the position after it", reps[0][1] == c5.epd())
check("only pool replies -> none", so.below_floor_replies(e4.epd(), {"e5": 1.0}, [zobrist_int64(e5)]) == [])

print("F  fit")
rng = np.random.default_rng(5)
n = 3000
ply = rng.choice([5, 11, 15, 19, 25], n)
val = rng.uniform(0.2, 0.9, n)
eng = rng.uniform(0.2, 0.9, n)
A_ = {5: 0.05, 11: 0.03, 15: 0.0, 19: -0.02, 25: -0.04}
noise = rng.normal(0, 0.05, n)
truth = np.array([A_[p] for p in ply]) + 0.3 * val + 0.5 * eng
tab = pl.DataFrame({"hash": np.arange(100, 100 + n), "ply": ply.astype(np.int32),
                    "weight": rng.uniform(0.1, 1.0, n), "value": val, "engine": eng,
                    "v0": truth + noise, "se": np.full(n, 0.05)})
m = so.fit(tab)
check("the slopes are recovered", abs(m["b_value"] - 0.3) < 0.02 and abs(m["c_engine"] - 0.5) < 0.02,
      f"{m['b_value']:.3f} {m['c_engine']:.3f}")
check("tau^2 nets out the sampling noise", m["tau2"] < 2e-4, str(m["tau2"]))
hold = so.holdout_check(tab)
check("held out: prediction beats the old bucket value", hold["rmse"] < hold["old_value_rmse"], str(hold))
nanrow = tab.head(60).with_columns(pl.lit(float("nan")).alias("v0"))
check("a node with no below-floor reply is not in the fit", so.fit(pl.concat([tab, nanrow]))["n"] == n)
check("too few nodes -> no calibration", so.fit(tab.head(10)) is None)

print("A  apply")
# Many opponent nodes under one root: the root picks one; values read back from const_base.
N = 80
big = {R: [e_(f"m{i}", 1000 + i, 100) for i in range(N)]}
auxb = {}
for i in range(N):
    h = 1000 + i
    big[h] = [e_("r", 2000 + i, 60)]
    big[2000 + i] = [e_("s", 3000 + i, 60)]
    auxb[h] = {"other_total": 40, "other_white_wins": 16 + i % 5, "other_draws": 0,
               "other_eval_mean": None, "other_eval_cov": 0.0}
kwb = dict(gates=False, prior_strength=1.0, eval_weight=0.0, eps=0.0, share_floor=0.0, aux_rows=auxb)
g = bc.build_graph(big, R, True, track_other=True, track_route_ply=True, **kwb)
base = {h: g.opp[h].const_base for h in g.opp}
info = so.other_positions(g)
check("the dump lists every bucket with its pool children",
      info.height == N and info.filter(pl.col("hash") == 1000)["children"][0].to_list() == [2000])
check("weight = reach x share", abs(info["weight"][0] - info["reach"][0] * info["share"][0]) < 1e-15)
hs = info["hash"].to_list()
vals = info["value"].to_numpy()
t = pl.DataFrame({"hash": hs[:70], "ply": info["ply"][:70], "weight": info["weight"][:70],
                  "value": vals[:70], "engine": [None] * 70,
                  "v0": (0.1 + 0.5 * vals[:70]).tolist(), "se": [0.0] * 60 + [1e6] * 10},
                 schema_overrides={"engine": pl.Float64})
rep = so.apply(g, t)
check("sampled and predicted counts", rep["sampled"] == 70 and rep["predicted"] == 10, str(rep))


def bucket_value(h):
    s, old, _ = g.other[h]
    return old + (g.opp[h].const_base - base[h]) / s


pred = so.predict(rep["model"], np.array([info["ply"][0]]), np.array([vals[0]]), np.array([np.nan]))[0]
check("se 0 -> the sampled value", abs(bucket_value(hs[0]) - (0.1 + 0.5 * vals[0])) < 1e-9)
check("a huge se -> the prediction", abs(bucket_value(hs[65]) - (0.1 + 0.5 * vals[65])) < 1e-6)
check("unsampled -> the prediction", abs(bucket_value(hs[75]) - (0.1 + 0.5 * vals[75])) < 1e-6, str(pred))
check("the mean shift is reported, below 0 here", rep["mean_shift"] < 0, str(rep["mean_shift"]))

g2 = bc.build_graph(big, R, True, track_other=True, track_route_ply=True, **kwb)
rep2 = so.apply(g2, pl.concat([t, t.head(1).with_columns(pl.col("ply") + 2, pl.lit(1.0).alias("v0"))]))
check("a sample at another ply is refused", rep2["cross_ply_refused"] == 1, str(rep2["cross_ply_refused"]))
check("and does not move the node", abs(g2.opp[hs[0]].const_base - g.opp[hs[0]].const_base) < 1e-9)
try:
    so.apply(bc.build_graph(big, R, True, track_other=True, track_route_ply=True, **kwb), t.head(5))
    check("too few samples fail loudly", False)
except ValueError:
    check("too few samples fail loudly", True)

# the optimiser's own walk follows: the root's value moves by the bucket's share x the change
g3 = bc.build_graph(big, R, True, track_other=True, track_route_ply=True, **kwb)
order, _cut = bc.topo_up(g3)
moves = {R: "m0"}
v_before = bc.policy_value(g3, moves, order)["value"]
so.apply(g3, t)
s0, old0, _ = g3.other[hs[0]]
check("the root's policy value moves by share x the change",
      abs(bc.policy_value(g3, moves, order)["value"] - v_before - s0 * (0.1 + 0.5 * old0 - old0)) < 1e-9)

sys.exit(FAIL)
