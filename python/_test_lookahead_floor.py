"""--lookahead-floor (lookahead_floor.py), 2026-11-14.

  A  a move that passes the absolute gate but whose common reply leaves no passing move is vetoed
  B  the veto is void where it would leave no passing move
  C  a reply under the share floor does not count; a node with no evaluated move is not dead
  D  depth 2 reaches a node whose only passing move is itself vetoed; depth 1 does not
  E  verified exclusions: an excluded move is no alternative, and cannot keep a node alive
  F  an engine proposal that passes keeps a node alive
  G  Black: evals are read from our side
  H  depth 0 vetoes nothing
  I  through build_graph: the vetoed move leaves the candidates, the source move included
  J  the frame implementation equals the reference on random graphs (both colours, depths 1-99,
     exclusions, engine proposals, source moves, reach_share)
  K  a source move that fails the gate is vetoed where the node keeps a passing move
  L  the walk reports each step and stops at its fixed point
"""
import random
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
import lookahead_floor as lf  # noqa: E402
from stage3_backwards_induction import cp_to_expected_score as ws  # noqa: E402

fails = 0
PRIOR = 0.5183          # the White pool's slice prior: the bar is about -90 cp for White


def check(name, ok, detail=""):
    global fails
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail and not ok else ""))
    fails += 0 if ok else 1


def epd(side):
    return f"8/8/8/8/8/8/8/8 {side} - -"


def node(side, *moves):
    """moves: (san, child, total)"""
    return [{"move_san": s, "child_hash": c, "total": t, "white_score_avg": 0.5,
             "parent_epd": epd(side)} for s, c, t in moves]


def graph(our="w"):
    opp = "b" if our == "w" else "w"
    return {
        1: node(our, ("A", 10, 900), ("B", 11, 100)),
        10: node(opp, ("r1", 20, 900), ("r2", 21, 100)),
        11: node(opp, ("r3", 22, 1000)),
        20: node(our, ("x", 30, 500), ("y", 31, 400)),
        21: node(our, ("z", 32, 100)),
        22: node(our, ("q", 33, 1000)),
    }


# White-view cp; the same numbers negated make the Black mirror
CP = {10: -85, 11: -45, 30: -93, 31: -132, 32: -80, 33: -50}
EV = {h: ws(c) for h, c in CP.items()}
kw = dict(depth=1, robustness_floor=0.1, share_floor=0.002)

v, st = lf.vetoes_ref(graph(), EV, True, PRIOR, **kw)
check("A: A is vetoed at the root, nothing else", v == {1: {"A"}}, str(v))
check("A: one dead node, one vetoed move", st["dead_nodes"] == 1 and st["vetoed_moves"] == 1, str(st))

g = graph()
g[1] = node("w", ("A", 10, 900))
v, st = lf.vetoes_ref(g, EV, True, PRIOR, **kw)
check("B: void when A is the only move", v == {} and st["void_nodes"] == 1, f"{v} {st}")
v, st = lf.vetoes_ref(graph(), {**EV, 11: ws(-95)}, True, PRIOR, **kw)
check("B: void when the alternative fails the gate", v == {} and st["void_nodes"] == 1, f"{v} {st}")

g = graph()
g[10] = node("b", ("r1", 20, 1), ("r2", 21, 999))
v, _ = lf.vetoes_ref(g, EV, True, PRIOR, **kw)
check("C: a reply under the share floor does not veto", v == {})
v, _ = lf.vetoes_ref(graph(), {h: e for h, e in EV.items() if h not in (30, 31)}, True, PRIOR, **kw)
check("C: a node with no evaluated move is not dead", v == {})

# D: 20's only passing move x leads (via 40) to the dead node 50
g = graph()
g[30] = node("b", ("s", 50, 100))
g[50] = node("w", ("t", 60, 100))
EVD = {**EV, 30: ws(-70), 60: ws(-120)}
v1, s1 = lf.vetoes_ref(g, EVD, True, PRIOR, **kw)
v2, s2 = lf.vetoes_ref(g, EVD, True, PRIOR, **{**kw, "depth": 2})
check("D: depth 1 is void at 20 and leaves the root alone", v1 == {} and s1["void_nodes"] == 1, f"{v1} {s1}")
check("D: depth 2 vetoes A at the root", v2 == {1: {"A"}}, str(v2))
v9, _ = lf.vetoes_ref(g, EVD, True, PRIOR, **{**kw, "depth": 9})
check("D: a larger depth is the same fixed point", v9 == v2)
check("L: depth 2 used both steps and cannot say it is the fixed point",
      s2["bad_moves_by_depth"] == [1, 2] and not s2["fixed_point"], str(s2))
_, s9 = lf.vetoes_ref(g, EVD, True, PRIOR, **{**kw, "depth": 9})
check("L: depth 9 stops after two steps, at the fixed point",
      s9["bad_moves_by_depth"] == [1, 2] and s9["fixed_point"] and s9["iterations"] == 2, str(s9))

v, st = lf.vetoes_ref(graph(), EV, True, PRIOR, excluded={1: {"B"}}, **kw)
check("E: an excluded alternative makes the veto void", v == {} and st["void_nodes"] == 1, f"{v} {st}")
v, _ = lf.vetoes_ref(graph(), {**EV, 30: ws(-60)}, True, PRIOR, **kw)
check("E: 20 alive with x at -60", v == {})
v, _ = lf.vetoes_ref(graph(), {**EV, 30: ws(-60)}, True, PRIOR, excluded={20: {"x"}}, **kw)
check("E: ...and dead again once x is excluded", v == {1: {"A"}}, str(v))

v, _ = lf.vetoes_ref(graph(), {**EV, 99: ws(-20)}, True, PRIOR, engine_moves={20: [("e+", 99)]}, **kw)
check("F: a passing engine proposal keeps 20 alive", v == {})
v, _ = lf.vetoes_ref(graph(), {**EV, 99: ws(-120)}, True, PRIOR, engine_moves={20: [("e+", 99)]}, **kw)
check("F: a failing one does not", v == {1: {"A"}})

# Black: prior 0.5196 (White's score), so Black's bar is about -133 cp from Black's side
EVB = {h: ws(-c * 1.5) for h, c in CP.items()}      # Black-view cp = 1.5 * CP: -127, -67, -139, -198
v, _ = lf.vetoes_ref(graph("b"), EVB, False, 0.5196, **kw)
check("G: Black mirror vetoes A", v == {1: {"A"}}, str(v))
v, _ = lf.vetoes_ref(graph("b"), EV, False, 0.5196, **kw)
check("G: White-good evals are below Black's bar everywhere: no passing move, void", v == {}, str(v))

v, st = lf.vetoes_ref(graph(), EV, True, PRIOR, **{**kw, "depth": 0})
check("H: depth 0 vetoes nothing", v == {} and st["vetoed_moves"] == 0)

# I: through build_graph (strict source, gated reach), the source move A included
bkw = dict(rep_moves={1: "A"}, eval_ws=EV, slice_prior=PRIOR, eps=1e-6, max_ply=10, share_floor=0.002,
           gated_reach=True, strict_source=True, max_eval_loss=100)
base = bc.build_graph(graph(), 1, True, **bkw)
check("I: without the flag A is a candidate", "A" in {c[0] for c in base.our[1].cands})
v, _ = lf.vetoes_ref(graph(), EV, True, PRIOR, **kw)
gv = bc.build_graph(graph(), 1, True, excluded=lf.merged(None, v), **bkw)
check("I: with it the root keeps only B", {c[0] for c in gv.our[1].cands} == {"B"},
      str(gv.our[1].cands))
check("I: the dead node leaves the graph", 20 not in gv.our and 22 in gv.our)
check("I: merged() leaves its inputs alone and adds to an existing entry",
      lf.merged({1: {"C"}}, v) == {1: {"A", "C"}} and v == {1: {"A"}})


# K: the source move A fails the gate (-95), B passes
gk = {1: node("w", ("A+", 10, 900), ("B", 11, 100))}
EVK = {10: ws(-95), 11: ws(-45)}
v, st = lf.vetoes_ref(gk, EVK, True, PRIOR, source_moves={1: "A+"}, **kw)
check("K: the failing source move is vetoed", v == {1: {"A"}} and st["source_moves_vetoed"] == 1, f"{v} {st}")
v, _ = lf.vetoes_ref(gk, EVK, True, PRIOR, **kw)
check("K: only the source move (others never pass the gate anyway)", v == {})
v, _ = lf.vetoes_ref(gk, {10: ws(-95), 11: ws(-100)}, True, PRIOR, source_moves={1: "A+"}, **kw)
check("K: not where no move passes", v == {})
v, _ = lf.vetoes_ref(gk, EVK, True, PRIOR, source_moves={1: "A+"}, **{**kw, "depth": 0})
check("K: not with the flag off", v == {})
base = bc.build_graph(gk, 1, True, rep_moves={1: "A+"}, eval_ws=EVK, slice_prior=PRIOR, eps=1e-6,
                      max_ply=10, gated_reach=True, strict_source=True, max_eval_loss=100)
check("K: the gates alone keep the failing source move", "A+" in {c[0] for c in base.our[1].cands},
      str(base.our[1].cands))


# J: frame == reference on random layered graphs
def random_graph(rng, our):
    opp = "b" if our == "w" else "w"
    levels = [[1]] + [[lv * 1000 + i for i in range(rng.randint(3, 9))] for lv in range(1, 8)]
    edges, evals = {}, {}
    for lv, nodes in enumerate(levels[:-1]):
        side = our if lv % 2 == 0 else opp
        for h in nodes:
            ms = []
            for k in range(rng.randint(0, 4)):
                ch = rng.choice(levels[lv + 1]) if rng.random() < 0.9 else None
                ms.append((f"m{k}" + ("+" if rng.random() < 0.2 else ""), ch, rng.randint(1, 400)))
            if ms:
                edges[h] = node(side, *ms)
                if side != our and rng.random() < 0.3:
                    for e in edges[h]:
                        e["reach_share"] = rng.random() if rng.random() < 0.7 else None
    for nodes in levels:
        for h in nodes:
            if rng.random() < 0.85:
                evals[h] = ws(rng.randint(-220, 60) * (1 if our == "w" else -1))
    ours = [h for h, es in edges.items() if es[0]["parent_epd"].split()[1] == our]
    excluded = {h: {e["move_san"].rstrip("+#") for e in edges[h] if rng.random() < 0.3}
                for h in ours if rng.random() < 0.2}
    excluded = {h: bs for h, bs in excluded.items() if bs}
    eng = {}
    for h in ours:
        if rng.random() < 0.2:
            lv = h // 1000 if h != 1 else 0
            eng[h] = [(rng.choice(["m0", "n1+", "n2"]), rng.choice(levels[lv + 1]))
                      for _ in range(rng.randint(1, 2))]
    src = {h: rng.choice(edges[h])["move_san"] for h in ours if rng.random() < 0.6}
    return edges, evals, excluded, eng, src


def frame(edges):
    rows = [{"reach_share": None, **e, "parent_hash": h} for h, es in edges.items() for e in es]
    return pl.DataFrame(rows, schema_overrides={"child_hash": pl.Int64, "reach_share": pl.Float64},
                        infer_schema_length=None)


rng = random.Random(20261114)
bad, hits, src_hits, deep = [], 0, 0, 0
for trial in range(300):
    our = "w" if trial % 2 == 0 else "b"
    edges, evals, excluded, eng, src = random_graph(rng, our)
    if not edges:
        continue
    ek = np.fromiter(evals.keys(), np.int64, len(evals))
    ev = np.fromiter(evals.values(), np.float64, len(evals))
    prior = 0.5183 if our == "w" else 0.5196
    fr = frame(edges)
    if trial % 3 == 0:
        fr = fr.drop("reach_share")
        for es in edges.values():
            for e in es:
                e.pop("reach_share", None)
    for depth in (1, 2, 99):
        args = dict(depth=depth, robustness_floor=0.1, share_floor=0.05, excluded=excluded,
                    engine_moves=eng, source_moves=src)
        a = lf.vetoes_ref(edges, evals, our == "w", prior, **args)
        b = lf.vetoes(fr, ek, ev, our == "w", prior, **args)
        hits += bool(a[0])
        src_hits += a[1]["source_moves_vetoed"]
        deep += a[1]["iterations"] > 2
        if a != b:
            bad.append((trial, depth, a, b))
check("J: frame == reference on 300 random graphs x 3 depths", not bad, str(bad[:1]))
check("J: the random graphs exercise the veto, the source rule and deeper steps",
      hits > 100 and src_hits > 20 and deep >= 10, f"{hits} {src_hits} {deep}")

sys.exit(1 if fails else 0)
