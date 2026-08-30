"""T7: brute-force oracle on random tiny trees, plus production-parity checks
(T5/T6) against stage3's run_backwards_induction on a real-board fixture.

BRUTE FORCE
-----------
Random alternating trees (<=10 our-decisions, fixed seeds). The oracle
enumerates EVERY (booking set, policy) pair — booking sets need no closure
constraint because an unreachable booked node is merely wasted budget, which
never helps — and takes the best realized value at each budget. Invariants
asserted against the DP:

  feasibility    extraction's realized value <= brute optimum      (always)
  achievability  the root hull's eval        <= brute optimum      (always)
  floor          extraction's realized value >= root hull eval     (always)
  monotone       realized and hull values non-decreasing in budget

The hull-vs-optimum GAP (the localized straddle) is measured and printed, not
asserted to a theoretical bound — tier-3 runs measure it at scale.

PRODUCTION PARITY (T5/T6)
-------------------------
A real-board fixture is valued by stage3's run_backwards_induction (imported,
the repo's oracle convention); the same edges go through build_graph +
fixed-policy DP at full capacity, asserting the booked set matches stage3's
decisions and the root value reconciles. Gates: a candidate failing the
absolute eval gate is never booked at any budget, cross-checked against stage3
rejecting it.
"""
from __future__ import annotations

import itertools
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import chess

from budget_core import (
    Graph, OppNode, OurNode, build_curves, build_graph, extract_book,
)
from stage3_backwards_induction import run_backwards_induction, zobrist_int64

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


# ── random tree generation ─────────────────────────────────────────────────

def random_tree(rng: random.Random) -> Graph:
    our: dict[int, OurNode] = {}
    opp: dict[int, OppNode] = {}
    ply: dict[int, int] = {}
    nid = itertools.count(1)

    def mk_our(depth: int) -> int:
        h = next(nid)
        ply[h] = depth
        node = OurNode(l_node=round(rng.uniform(0.30, 0.60), 3))
        n_cands = rng.randint(1, 2)
        for i in range(n_cands):
            if depth >= 4 or rng.random() < 0.4:
                node.cands.append((f"m{h}_{i}", None, False,
                                   round(rng.uniform(0.35, 0.90), 3), 0.0))
            else:
                ch = mk_opp(depth + 1)
                node.cands.append((f"m{h}_{i}", ch, True, 0.0, 0.0))
        our[h] = node
        return h

    def mk_opp(depth: int) -> int:
        h = next(nid)
        ply[h] = depth
        k = rng.randint(1, 3)
        ws = [rng.uniform(0.2, 1.0) for _ in range(k)]
        tot = sum(ws)
        node = OppNode(const_base=0.0)
        for w in ws:
            p = w / tot
            if depth >= 4 or rng.random() < 0.3:
                node.const_base += p * round(rng.uniform(0.35, 0.85), 3)
            else:
                node.kids.append((p, mk_our(depth + 1), True, 0.0))
        opp[h] = node
        return h

    root = mk_our(0)
    return Graph(root=root, our=our, opp=opp, epd={}, ply=ply,
                 reach={h: 1.0 for h in list(our) + list(opp)})


def brute_best(g: Graph, budget: int) -> float:
    our_nodes = sorted(g.our)
    cand_lists = {h: g.our[h].cands for h in our_nodes}

    def realized(h: int, booked: frozenset, policy: dict) -> float:
        if h in g.our:
            if h not in booked:
                return g.our[h].l_node
            san = policy[h]
            for s, ch, in_sub, lv, _ in cand_lists[h]:
                if s == san:
                    return realized(ch, booked, policy) if in_sub else lv
            return g.our[h].l_node
        n = g.opp[h]
        return n.const_base + sum(p * realized(k, booked, policy)
                                  for p, k, in_sub, _ in n.kids if in_sub)

    best = -1.0
    policies = itertools.product(*([c[0] for c in cand_lists[h]]
                                   for h in our_nodes))
    for pol_tuple in policies:
        policy = dict(zip(our_nodes, pol_tuple))
        for r in range(0, min(budget, len(our_nodes)) + 1):
            for combo in itertools.combinations(our_nodes, r):
                v = realized(g.root, frozenset(combo), policy)
                if v > best:
                    best = v
    return best


worst_gap, worst_seed = 0.0, None
for seed in range(30):
    rng = random.Random(seed)
    g = random_tree(rng)
    if len(g.our) > 10:
        continue
    curves, _ = build_curves(g, bmax=12)
    cap = curves[g.root].capacity
    prev_real, prev_hull = -1.0, -1.0
    ok_feas = ok_ach = ok_floor = ok_mono = True
    for b in range(0, min(cap, 6) + 1):
        bb = brute_best(g, b)
        r = extract_book(g, curves, b)
        real = r["root_value_realized"]
        hull = curves[g.root].eval(b)
        if real > bb + 1e-9:
            ok_feas = False
        if hull > bb + 1e-9:
            ok_ach = False
        if real < hull - 1e-9:
            ok_floor = False
        if real < prev_real - 1e-9 or hull < prev_hull - 1e-9:
            ok_mono = False
        prev_real, prev_hull = real, hull
        gap = bb - real
        if gap > worst_gap:
            worst_gap, worst_seed = gap, (seed, b)
    check(f"seed {seed}: feasible/achievable/floor/monotone "
          f"({len(g.our)} decisions, cap {cap})",
          ok_feas and ok_ach and ok_floor and ok_mono,
          f"feas={ok_feas} ach={ok_ach} floor={ok_floor} mono={ok_mono}")

print(f"  [info] worst extraction-vs-optimum gap: {worst_gap:.4f} "
      f"at (seed, budget)={worst_seed}  — the localized straddle, measured")

# ── T5/T6: production parity on a real-board fixture ───────────────────────

def board_after(*sans: str) -> chess.Board:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def edge(parent: chess.Board, san: str, score: float, total: int) -> dict:
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": 0}


start = board_after()
sh = zobrist_int64(start)
after_e4 = board_after("e4")
oh = zobrist_int64(after_e4)
u_e5 = board_after("e4", "e5")
u_c5 = board_after("e4", "c5")

edges = [
    edge(start, "e4", 0.55, 9000),
    edge(start, "d4", 0.54, 8000),
    edge(after_e4, "e5", 0.52, 5000),
    edge(after_e4, "c5", 0.50, 4000),
    edge(u_e5, "Nf3", 0.56, 4000),
    edge(u_e5, "Bc4", 0.53, 900),
    edge(u_c5, "Nf3", 0.55, 3500),
]
eval_ws = {
    oh: 0.56, zobrist_int64(board_after("d4")): 0.55,
    zobrist_int64(u_e5): 0.55, zobrist_int64(u_c5): 0.54,
    zobrist_int64(board_after("e4", "e5", "Nf3")): 0.57,
    zobrist_int64(board_after("e4", "e5", "Bc4")): 0.10,   # gate bait: refuted
    zobrist_int64(board_after("e4", "c5", "Nf3")): 0.56,
    sh: 0.55,
}

vals, bm, *_ = run_backwards_induction(
    edges, "white", prior_strength=0.0, min_move_games=0, eval_weight=1.0,
    require_eval=True, eval_lookup=eval_ws, robustness_floor=0.1,
    gate_metric="eval", gate_rel_floor=0.1, gate_rel_baseline="own-eval",
    gate_rel_own_margin=0.02)

check("stage3 books e4 at root", bm.get(sh) == "e4", f"got {bm.get(sh)!r}")
check("stage3 books Nf3 after e5 (Bc4 gate-rejected)",
      bm.get(zobrist_int64(u_e5)) == "Nf3")

by_parent: dict[int, list[dict]] = {}
for e in edges:
    by_parent.setdefault(e["parent_hash"], []).append(e)
# best_moves is an _ObjMap (get-only); enumerate the fixture's our-turn hashes.
our_hashes = [sh, zobrist_int64(u_e5), zobrist_int64(u_c5)]
policy = {h: bm.get(h) for h in our_hashes if bm.get(h)}

g2 = build_graph(by_parent, sh, True, rep_moves=policy, eval_ws=eval_ws,
                 slice_prior=0.5, eps=1e-6, max_ply=10, share_floor=0.0,
                 robustness_floor=0.1, gate_rel_floor=0.1,
                 gate_rel_own_margin=0.02)

bc4_gated = all(san != "Bc4"
                for san, *_r in g2.our[zobrist_int64(u_e5)].cands)
check("budget gates also reject Bc4 (eval 0.10 vs floor)", bc4_gated,
      f"cands={[c[0] for c in g2.our[zobrist_int64(u_e5)].cands]}")

fcurves, _ = build_curves(g2, bmax=20, fixed_policy=policy)
rfix = extract_book(g2, fcurves, 20, fixed_policy=policy)
stage3_decisions = {h for h, m in policy.items() if h in g2.our}
check("fixed-policy DP at capacity books the stage3 decision set",
      set(rfix["booked"]) == stage3_decisions,
      f"got {len(rfix['booked'])} vs {len(stage3_decisions)}")

# Value reconciliation at capacity, with the DP's leaf blends mirroring the
# stage3 fixture's knobs exactly (prior_strength=0, eval_weight=1.0: every
# leaf IS its eval). Fixed-policy DP at full budget must then reproduce
# stage3's opponent means to fp precision — the D7 reconciliation check.
g3 = build_graph(by_parent, sh, True, rep_moves=policy, eval_ws=eval_ws,
                 slice_prior=0.5, eps=1e-6, max_ply=10, share_floor=0.0,
                 prior_strength=0.0, eval_weight=1.0,
                 robustness_floor=0.1, gate_rel_floor=0.1,
                 gate_rel_own_margin=0.02)
fcurves3, _ = build_curves(g3, bmax=20, fixed_policy=policy)
rfix3 = extract_book(g3, fcurves3, 20, fixed_policy=policy)
v_dp = rfix3["root_value_realized"]
v_s3 = vals.get(sh)
check("root value reconciles with stage3 (matched knobs, tight)",
      v_dp is not None and v_s3 is not None and abs(v_dp - v_s3) < 1e-9,
      f"dp={v_dp} stage3={v_s3}")

# -- candidate cap (default-on; measured 10x subgraph reduction) ------------
# Four candidates at one node; cap 2 must keep the SOURCE move (even though its
# eval is worst) and the most FORCING alternative (cheapest subtree), not simply
# the two best evals -- eval-ranking alone would bias the cap toward the
# unconstrained book's own preference and hide the cheap-subtree option the
# budget exists to find.
# Root must be a WHITE-to-move position for a white book: build_graph anchors
# ply parity at the game start, so the fixture uses the start position.
cap_root = board_after()
cs = zobrist_int64(cap_root)
kids = {s: board_after(s) for s in ("e4", "d4", "c4", "Nf3")}
cap_edges = [edge(cap_root, s, 0.52, 5000) for s in kids]
cap_edges.append(edge(kids["c4"], "e5", 0.55, 9000))          # c4: one reply
for i, s_ in enumerate(["e5", "c5", "e6", "c6"]):             # e4: many replies
    cap_edges.append(edge(kids["e4"], s_, 0.55, 2000 + i))
cap_by_parent = {}
for e in cap_edges:
    cap_by_parent.setdefault(e["parent_hash"], []).append(e)
cap_eval = {zobrist_int64(kids["e4"]): 0.62, zobrist_int64(kids["d4"]): 0.60,
            zobrist_int64(kids["c4"]): 0.58, zobrist_int64(kids["Nf3"]): 0.55,
            cs: 0.60}
gcap = build_graph(cap_by_parent, cs, True, rep_moves={cs: "Nf3"},
                   eval_ws=cap_eval, slice_prior=0.5, eps=1e-9, max_ply=6,
                   share_floor=0.0, max_cands=2, gates=False)
kept = {c[0] for c in gcap.our[cs].cands}
check("cap keeps the source move even at worst eval", "Nf3" in kept, f"kept={kept}")
check("cap respects the limit", len(kept) <= 3, f"kept={kept}")
check("cap keeps the most forcing alternative (c4, one reply)", "c4" in kept,
      f"kept={kept}")
uncapped = build_graph(cap_by_parent, cs, True, rep_moves={cs: "Nf3"},
                       eval_ws=cap_eval, slice_prior=0.5, eps=1e-9, max_ply=6,
                       share_floor=0.0, max_cands=0, gates=False)
check("uncapped keeps all four", len(uncapped.our[cs].cands) == 4,
      f"n={len(uncapped.our[cs].cands)}")

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
