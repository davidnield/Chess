"""Price-per-move optimiser (budget_core.price_book / price_search, 2026-10-19)
held to an EXACT oracle.

The oracle is the exact budget table on a tree: every node stores its best value
with at most b booked positions for every b, our nodes take the max over moves,
opponent nodes combine their replies by exact max-plus convolution. It has no
hulls, atoms or density ordering, so it is the true optimum.

  A  On random trees (both colours to move at the root, mixed leaves, lumpy
     chains), every book price_book returns is the best book of its own size:
     its value equals the oracle at that size, and its penalised value equals
     max over b of (oracle(b) - lam * b).
  B  price_search: the result is optimal for its size, and the reported upper
     bound for the target is >= the oracle at the target.
  C  A lumpy chain: three 1-move items worth 0.05 each (root units) and one
     3-move chain worth 0.125 that pays only when complete. At a price between
     the two densities the items are bought and the chain is not; below it the
     chain is bought whole, never in part; each book matches the oracle.
  D  Transpositions: a position reached by two paths is charged once when priced
     with the book's total reach (price_search's second round), not once per path.
  E  fixed_policy restricts every node to the source move.
  F  a repetition cycle through a main-line position does not cost the line.
  H  policy_value (the shared book evaluator) reproduces each price book's value.
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from budget_core import (Graph, OppNode, OurNode, policy_value,  # noqa: E402
                         price_book, price_search, topo_up)

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


# ── the exact oracle ────────────────────────────────────────────────────────

def exact(g: Graph, bmax: int) -> np.ndarray:
    memo: dict[int, np.ndarray] = {}

    def vec(h):
        if h in memo:
            return memo[h]
        if h in g.our:
            n = g.our[h]
            a = np.full(bmax + 1, n.l_node)
            for _san, ch, in_sub, lv in n.cands:
                c = vec(ch) if in_sub else np.full(bmax + 1, lv)
                a[1:] = np.maximum(a[1:], c[:-1])
        else:
            n = g.opp[h]
            a = np.full(bmax + 1, n.const_base)
            for p, ch, in_sub, lv in n.kids:
                k = p * (vec(ch) if in_sub else np.full(bmax + 1, lv))
                new = np.full(bmax + 1, -np.inf)
                for b in range(bmax + 1):
                    new[b] = np.max(a[:b + 1][::-1] + k[:b + 1])
                a = new
        a = np.maximum.accumulate(a)
        memo[h] = a
        return a

    return vec(g.root)


# ── random trees ────────────────────────────────────────────────────────────

def random_tree(rng: random.Random, root_ours: bool, depth: int = 5) -> Graph:
    g = Graph(root=0, our={}, opp={}, epd={}, ply={}, reach={})
    nid = [0]

    def new_id():
        nid[0] += 1
        return nid[0]

    def build(h, ours, d, r, ply):
        g.ply[h], g.reach[h] = ply, r
        if ours:
            n = OurNode(l_node=rng.uniform(0.35, 0.6))
            for k in range(rng.randint(1, 3)):
                if d > 0 and rng.random() < 0.75:
                    c = new_id()
                    build(c, False, d - 1, r, ply + 1)
                    n.cands.append((f"m{k}", c, True, 0.0))
                else:
                    n.cands.append((f"m{k}", None, False, rng.uniform(0.3, 0.75)))
            g.our[h] = n
        else:
            k = rng.randint(1, 3)
            w = [rng.random() for _ in range(k)]
            rest = rng.uniform(0.0, 0.3)
            ps = [x / sum(w) * (1 - rest) for x in w]
            n = OppNode(const_base=rest * rng.uniform(0.3, 0.7))
            for p in ps:
                if d > 0 and rng.random() < 0.8:
                    c = new_id()
                    build(c, True, d - 1, r * p, ply + 1)
                    n.kids.append((p, c, True, 0.0))
                else:
                    n.kids.append((p, None, False, rng.uniform(0.3, 0.7)))
            g.opp[h] = n

    build(0, root_ours, depth, 1.0, 0 if root_ours else 1)
    return g


def main() -> int:
    rng = random.Random(20261019)
    bad_a = bad_pen = bad_b = 0
    n_books = 0
    for t in range(40):
        g = random_tree(rng, root_ours=(t % 2 == 0))
        size = len(g.our)
        opt = exact(g, size)
        order, _cut = topo_up(g)
        for lam in np.logspace(-5, 0, 40):
            r = price_book(g, float(lam), order)
            n = r["spent_distinct"]
            n_books += 1
            if abs(r["root_value_realized"] - opt[n]) > 1e-9:
                bad_a += 1
            if abs(r["root_value_penalized"] - float(np.max(opt - lam * np.arange(size + 1)))) > 1e-9:
                bad_pen += 1
        for target in sorted({min(size, x) for x in (1, 3, max(1, size // 3), max(1, size // 2))}):
            res, info = price_search(g, target, log=lambda m: None)
            nb = res["spent_distinct"]
            if (abs(res["root_value_realized"] - opt[nb]) > 1e-9
                    or info["upper_bound"] < opt[target] - 1e-9):
                bad_b += 1
    check("A: every price book is the best book of its size", bad_a == 0,
          f"{bad_a} of {n_books} off the oracle")
    check("A: penalised value = max_b (oracle(b) - lam*b)", bad_pen == 0, f"{bad_pen} off")
    check("B: price_search is optimal for its size and its bound holds", bad_b == 0,
          f"{bad_b} failures")

    # policy_value (the shared evaluator) reproduces each price book's own value
    rng2 = random.Random(7)
    bad_pv = 0
    for t in range(20):
        g = random_tree(rng2, root_ours=(t % 2 == 0))
        order, _cut = topo_up(g)
        for lam in (1e-4, 1e-3, 1e-2):
            r = price_book(g, lam, order)
            pv = policy_value(g, r["booked"], order)
            if abs(pv["value"] - r["root_value_realized"]) > 1e-12 or pv["played"] != r["spent_distinct"]:
                bad_pv += 1
    check("H: policy_value reproduces price_book's value and size", bad_pv == 0, f"{bad_pv} off")

    # C: three items worth 0.05 each against one 3-move chain worth 0.125
    g = Graph(root=1, our={}, opp={}, epd={}, ply={}, reach={})
    g.opp[1] = OppNode(const_base=0.0, kids=[(0.25, 2, True, 0), (0.25, 3, True, 0),
                                             (0.25, 4, True, 0), (0.25, 5, True, 0)])
    for x in (2, 3, 4):
        g.our[x] = OurNode(l_node=0.5, cands=[("a", None, False, 0.7)])
    g.our[5] = OurNode(l_node=0.5, cands=[("c1", 6, True, 0)])
    g.opp[6] = OppNode(const_base=0.0, kids=[(1.0, 7, True, 0)])
    g.our[7] = OurNode(l_node=0.5, cands=[("c2", 8, True, 0)])
    g.opp[8] = OppNode(const_base=0.0, kids=[(1.0, 9, True, 0)])
    g.our[9] = OurNode(l_node=0.5, cands=[("c3", None, False, 1.0)])
    g.ply = {1: 1, 2: 2, 3: 2, 4: 2, 5: 2, 6: 3, 7: 4, 8: 5, 9: 6}
    g.reach = {1: 1, 2: .25, 3: .25, 4: .25, 5: .25, 6: .25, 7: .25, 8: .25, 9: .25}
    opt = exact(g, 6)
    res, info = price_search(g, 6, log=lambda m: None)
    check("C: the whole budget books the chain and all three items (oracle 0.775)",
          abs(opt[6] - 0.775) < 1e-12 and res["spent_distinct"] == 6
          and abs(res["root_value_realized"] - 0.775) < 1e-12,
          f"oracle {opt[6]}, price {res['spent_distinct']} -> {res['root_value_realized']}")
    r3 = price_book(g, 0.045, *topo_up(g)[:1])
    r6 = price_book(g, 0.040, *topo_up(g)[:1])
    check("C: at 0.045 the items are bought and the chain is not (0.65 = oracle(3))",
          r3["spent_distinct"] == 3 and abs(r3["root_value_realized"] - opt[3]) < 1e-12
          and abs(opt[3] - 0.65) < 1e-12,
          f"{r3['spent_distinct']} positions, {r3['root_value_realized']}")
    check("C: at 0.040 the chain is bought whole as well (6 positions)",
          r6["spent_distinct"] == 6 and abs(r6["root_value_realized"] - opt[6]) < 1e-12,
          f"{r6['spent_distinct']} positions")

    # D: X is reached through A and through B; worth 0.1 over its stop value
    g = Graph(root=1, our={}, opp={}, epd={}, ply={}, reach={})
    g.opp[1] = OppNode(const_base=0.0, kids=[(0.5, 2, True, 0), (0.5, 3, True, 0)])
    g.our[2] = OurNode(l_node=0.5, cands=[("a", 4, True, 0)])
    g.our[3] = OurNode(l_node=0.5, cands=[("b", 5, True, 0)])
    g.opp[4] = OppNode(const_base=0.0, kids=[(1.0, 6, True, 0)])
    g.opp[5] = OppNode(const_base=0.0, kids=[(1.0, 6, True, 0)])
    g.our[6] = OurNode(l_node=0.5, cands=[("x", None, False, 0.6)])
    g.ply = {1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4}
    g.reach = {1: 1, 2: .5, 3: .5, 4: .5, 5: .5, 6: .5}       # max over paths
    order, _cut = topo_up(g)
    per_path = price_book(g, 0.03, order)
    total = dict(g.reach)
    total[6] = 1.0                                          # both paths taken
    once = price_book(g, 0.03, order, total)
    check("D: priced per path (cost counted twice) the line is not worth it at 0.03",
          per_path["spent_distinct"] == 0, f"{per_path['booked']}")
    check("D: priced by total reach (charged once) it is: 3 positions, value 0.6",
          once["spent_distinct"] == 3 and abs(once["root_value_realized"] - 0.6) < 1e-12,
          f"{once['booked']}, {once['root_value_realized']}")
    res, _ = price_search(g, 3, log=lambda m: None)
    check("D: price_search's second round finds it", res["spent_distinct"] == 3,
          f"{res['booked']}")

    # F: a repetition cycle through a main-line position. A (our node) can play
    # "back" (to O2, whose only reply returns to A) or "good" (to O3 -> C, where
    # booking x is worth 0.4 over the stop value). Every node in the cycle has
    # reach 1, so the cycle is broken at a tie: the book must still find A->good,
    # C->x however the tie falls.
    for back_hash in (50, 5):                  # O2 hashed above and below A
        g = Graph(root=1, our={}, opp={}, epd={}, ply={}, reach={})
        g.our[1] = OurNode(l_node=0.5, cands=[("m", 2, True, 0)])
        g.opp[2] = OppNode(const_base=0.0, kids=[(1.0, 10, True, 0)])
        g.our[10] = OurNode(l_node=0.5, cands=[("back", back_hash, True, 0),
                                               ("good", 30, True, 0)])
        g.opp[back_hash] = OppNode(const_base=0.0, kids=[(1.0, 10, True, 0)])
        g.opp[30] = OppNode(const_base=0.0, kids=[(1.0, 40, True, 0)])
        g.our[40] = OurNode(l_node=0.5, cands=[("x", None, False, 0.9)])
        g.ply = {1: 0, 2: 1, 10: 2, back_hash: 3, 30: 3, 40: 4}
        g.reach = {h: 1.0 for h in g.ply}
        res, _ = price_search(g, 3, log=lambda m: None)
        check(f"F: a repetition cycle does not cost the main line (back edge hash {back_hash})",
              res["booked"] == {1: "m", 10: "good", 40: "x"}
              and abs(res["root_value_realized"] - 0.9) < 1e-12,
              f"{res['booked']}, {res['root_value_realized']}")

    # E: fixed policy
    g = Graph(root=1, our={1: OurNode(l_node=0.5, cands=[("good", None, False, 0.7),
                                                        ("src", None, False, 0.6)])},
              opp={}, epd={}, ply={1: 0}, reach={1: 1.0})
    r = price_book(g, 0.01, topo_up(g)[0], fixed_policy={1: "src"})
    check("E: fixed_policy books only the source move", r["booked"] == {1: "src"},
          f"{r['booked']}")

    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
