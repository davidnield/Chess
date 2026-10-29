"""Budget engines on DAGs (transpositions) held to an EXACT oracle -- plan item 11.

_test_budget_price.py holds the price engine to an exact max-plus oracle, but only on
TREES. On a DAG a position reached by several move orders is learned once (one budget
unit) while its value accrues on every path, and neither the tree oracle nor the price
recursion accounts for that. This script measures both engines where the optimum is
known.

THE ORACLE. A book is a move per booked our-node. Its value is budget_core.policy_value
(the shared evaluator every method's books are compared with) and its cost is the number
of distinct positions the walk plays (policy_value's 'played', which is price_book's
spent_distinct). `exact_dag` enumerates every book exhaustively: walking from the root it
branches, at the first reached undecided our-node in top-down order, on stop or each
candidate, so every reachable book is visited once. Books never shrink a reach set, so a
branch that has spent more than bmax is cut. opt[b] = the best value of any book of at
most b positions. It is exponential and meant for graphs of a dozen our-nodes.

  O  the oracle equals _test_budget_price's tree oracle on random trees, and gives the
     hand-computed answers on cases D (transposition), F (repetition cycle) and T8
     (two replies to one position).
  E  the region evaluator behind book_result / polish_book is policy_value bit for bit
     (value, size, reach), on random books over random DAGs and case F's cycle.
  N  price_search(dag_mode=None) is the unchanged search.
  P  polish_book never exceeds the target and never lowers its starting book's value.
  R  REPORT over random DAGs (both colours to move at the root, mixed leaf values, lumpy
     chains, transpositions of both kinds: an opponent reply into a position another
     branch reaches, and one of our moves into another branch's opponent node) at
     several budgets. For each engine: how often its book fits the budget, its gap to
     the oracle at the budget (feasible books), and its gap to the oracle at its OWN
     size (does it waste the positions it spends); and for each DAG candidate, on how
     many instances it scores below both baselines / below the better one.
  G  gates on the report (plan item 11): polish always fits, is never below both price
     and dp, and is exact on >= 99% of instances with mean gap < 1e-4; dp+polish always
     fits, is never below dp, and is exact at least as often as dp; polish+dp is the
     better of polish and dp+polish (to price_search's 1e-15 tie rule).
  S  stickiness (g.meta["stick"], --incumbent; polish_book's objective J = value - margin
     x reach of the changed positions). S0: no stick and margin 0 are the same polish bit
     for bit. S1: with a huge margin polish never changes a move its start kept from the
     incumbent. S2: sticky polish fits, never lowers its start's J, and reports J and the
     churn as an independent computation does. REPORT: churn vs value at margins 0 /
     0.005 / 0.01 / 0.02, the incumbent being the exact optimum of a perturbed copy of
     each graph, every engine held to the exact optimum of J by the same enumeration;
     gated on sticky polish fitting, being J-exact on >= 98%, and churning less as the
     margin rises.

Engines:
  price         price_search with the production DAG flags (--price-seed source
                --price-reprice max --price-rounds 4 --price-select best); the "source
                book" is the unbudgeted best book (price_book at a vanishing price), the
                synthetic stand-in for the Stage-3 repertoire.
  price_dflt    price_search defaults (2 rounds, closest).
  fixpoint, polish, fixpoint+polish
                the production flags plus price_search(dag_mode=...), plan item 11's
                candidates (a) and (c).
  dp            build_curves + extract_book (the curve engine, as used for Black).
  dp_leftover   the same with --leftover.
  dp+polish     polish_book started from the dp book (build_budget_books --method dp
                --polish).
  polish+dp     polish with the dp book as a fourth start (price_search polish_from,
                --polish-from).

Usage:  _test_budget_dag.py [N_GRAPHS [N_STICK]]  checks + reports (default 300 graphs, 60
                                                 for the stickiness report)
        _test_budget_dag.py --scale 10000,100000 400,2000
                                                 wall-clock of each mode on big_dag
                                                 graphs of those sizes, at those targets
                                                 [--modes polish,...: a subset of the modes,
                                                 plus dp+polish and polish+stick]
Exit 1 when a check fails.
"""
from __future__ import annotations

import copy
import random
import sys
import time
from collections import deque
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from budget_core import (Graph, OppNode, OurNode, book_result,  # noqa: E402
                         build_curves, extract_book, policy_reach, policy_value,
                         polish_book, price_book, price_search, topo_down, topo_up)

FAIL = 0
TOL = 1e-9


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


# ── the exact oracle ────────────────────────────────────────────────────────

def exact_dag(g: Graph, bmax: int, on_book=None) -> tuple[np.ndarray, list[dict | None]]:
    """(opt, books): opt[b] = best policy_value of any book playing <= b positions,
    books[b] one book attaining it. on_book(moves, value, size) sees every book."""
    order, _cut = topo_up(g)
    pos = {h: i for i, h in enumerate(topo_down(g))}
    best = np.full(bmax + 1, -np.inf)
    books: list[dict | None] = [None] * (bmax + 1)

    def frontier(moves: dict, decided: set) -> int | None:
        """The first reached our-node not yet decided, in top-down order."""
        stack, seen, out = [g.root], set(), None
        while stack:
            h = stack.pop()
            if h in seen:
                continue
            seen.add(h)
            if h in g.our:
                if h not in decided:
                    if out is None or pos[h] < pos[out]:
                        out = h
                    continue
                want = moves.get(h)
                if want is None:
                    continue
                c = next(c for c in g.our[h].cands if c[0] == want)
                if c[2] and c[1] is not None:
                    stack.append(c[1])
            elif h in g.opp:
                stack.extend(ch for _p, ch, in_sub, _lv in g.opp[h].kids
                             if in_sub and ch is not None)
        return out

    def rec(moves: dict, decided: set) -> None:
        u = frontier(moves, decided)
        if u is None:
            pv = policy_value(g, moves, order)
            n = pv["played"]
            assert n == len(moves)
            if on_book is not None:
                on_book(moves, pv["value"], n)
            if pv["value"] > best[n]:
                best[n] = pv["value"]
                books[n] = dict(moves)
            return
        decided.add(u)
        rec(moves, decided)                          # stop at u
        if len(moves) < bmax:
            for san, *_rest in g.our[u].cands:
                moves[u] = san
                rec(moves, decided)
                del moves[u]
        decided.discard(u)

    rec({}, set())
    for b in range(1, bmax + 1):                     # "at most b"
        if best[b - 1] > best[b]:
            best[b], books[b] = best[b - 1], books[b - 1]
    return best, books


# ── random DAGs ─────────────────────────────────────────────────────────────

def random_dag(rng: random.Random, root_ours: bool, depth: int = 6,
               max_our: int = 18, transpo: float = 0.35, chain: float = 0.12) -> Graph:
    """A random game DAG. Edges always go from ply p to p+1, so it is acyclic; a
    transposition is an edge into an existing node of the right ply. g.reach is the
    max-path reach (what optimistic_reach gives the price engine)."""
    g = Graph(root=0, our={}, opp={}, epd={}, ply={}, reach={})
    nid = [0]
    at_ply: dict[tuple[bool, int], list[int]] = {}

    def new_id():
        nid[0] += 1
        return nid[0]

    def reg(h, ours, ply):
        g.ply[h] = ply
        at_ply.setdefault((ours, ply), []).append(h)

    def existing(ours, ply):
        return at_ply.get((ours, ply), [])

    def lumpy_chain(h, ply, l0):
        """our node h -> forced chain of k moves that pays only when complete"""
        k = rng.randint(2, 3)
        cur, p = h, ply
        for i in range(k):
            o = new_id()
            reg(o, False, p + 1)
            nxt = new_id()
            g.opp[o] = OppNode(const_base=0.0, kids=[(1.0, nxt, True, 0.0)])
            g.our[cur].cands.append((f"c{i}", o, True, 0.0))
            reg(nxt, True, p + 2)
            g.our[nxt] = OurNode(l_node=l0)
            cur, p = nxt, p + 2
        g.our[cur].cands.append(("pay", None, False, rng.uniform(0.75, 0.95)))

    def build(h, ours, d, ply):
        reg(h, ours, ply)
        if ours:
            l0 = rng.uniform(0.35, 0.6)
            n = g.our[h] = OurNode(l_node=l0)
            if d > 1 and len(g.our) < max_our and rng.random() < chain:
                lumpy_chain(h, ply, l0)
                return
            for k in range(rng.randint(1, 3)):
                if d > 0 and rng.random() < 0.75:
                    old = existing(False, ply + 1)
                    if old and rng.random() < transpo:
                        n.cands.append((f"m{k}", rng.choice(old), True, 0.0))
                        continue
                    if len(g.our) < max_our:
                        c = new_id()
                        build(c, False, d - 1, ply + 1)
                        n.cands.append((f"m{k}", c, True, 0.0))
                        continue
                n.cands.append((f"m{k}", None, False, rng.uniform(0.3, 0.75)))
        else:
            k = rng.randint(1, 3)
            w = [rng.random() for _ in range(k)]
            rest = rng.uniform(0.0, 0.3)
            ps = [x / sum(w) * (1 - rest) for x in w]
            n = g.opp[h] = OppNode(const_base=rest * rng.uniform(0.3, 0.7))
            for p in ps:
                if d > 0 and rng.random() < 0.8:
                    old = existing(True, ply + 1)
                    if old and rng.random() < transpo:
                        n.kids.append((p, rng.choice(old), True, 0.0))
                        continue
                    if len(g.our) < max_our:
                        c = new_id()
                        build(c, True, d - 1, ply + 1)
                        n.kids.append((p, c, True, 0.0))
                        continue
                n.kids.append((p, None, False, rng.uniform(0.3, 0.7)))

    build(0, root_ours, depth, 0 if root_ours else 1)
    set_max_reach(g)
    return g


def set_max_reach(g: Graph) -> None:
    g.reach = {g.root: 1.0}
    for h in sorted(g.ply, key=lambda h: g.ply[h]):
        r = g.reach.get(h, 0.0)
        if h in g.our:
            for _s, ch, in_sub, _lv in g.our[h].cands:
                if in_sub and ch is not None:
                    g.reach[ch] = max(g.reach.get(ch, 0.0), r)
        else:
            for p, ch, in_sub, _lv in g.opp[h].kids:
                if in_sub and ch is not None:
                    g.reach[ch] = max(g.reach.get(ch, 0.0), r * p)


def big_dag(rng: random.Random, n_nodes: int, root_ours: bool = True,
            transpo: float = 0.3, chain: float = 0.05) -> Graph:
    """A large random DAG for timing, grown breadth-first to n_nodes: the same node
    shapes as random_dag (leaf values, lumpy chains, transpositions into recent nodes of
    the next ply), so its books have the same structure at scale."""
    g = Graph(root=0, our={}, opp={}, epd={}, ply={}, reach={})
    q: deque = deque([(0, root_ours, 0 if root_ours else 1)])
    at_ply: dict[tuple[bool, int], list[int]] = {}
    nxt = [1]

    def node(ours, ply):
        old = at_ply.get((ours, ply))
        if old and rng.random() < transpo:
            return old[-1 - int(rng.random() ** 2 * min(len(old), 2000))]
        h = nxt[0]
        nxt[0] += 1
        at_ply.setdefault((ours, ply), []).append(h)
        q.append((h, ours, ply))
        return h

    lumpy: set[int] = set()
    while q:
        h, ours, ply = q.popleft()
        g.ply[h] = ply
        grow = nxt[0] < n_nodes or h == 0
        if ours:
            l0 = rng.uniform(0.35, 0.6)
            n = g.our[h] = OurNode(l_node=l0)
            if not grow:
                n.cands.append(("m0", None, False, rng.uniform(0.3, 0.75)))
                continue
            if rng.random() < chain:              # a forced chain that pays at its end
                cur, p = h, ply
                for i in range(rng.randint(2, 3)):
                    o, nx = nxt[0], nxt[0] + 1
                    nxt[0] += 2
                    g.ply[o], g.ply[nx] = p + 1, p + 2
                    g.opp[o] = OppNode(const_base=0.0, kids=[(1.0, nx, True, 0.0)])
                    g.our[cur].cands.append((f"c{i}", o, True, 0.0))
                    g.our[nx] = OurNode(l_node=l0)
                    lumpy.add(nx)
                    cur, p = nx, p + 2
                lumpy.add(h)
                g.our[cur].cands.append(("pay", None, False, rng.uniform(0.75, 0.95)))
                continue
            for k in range(rng.randint(1, 3)):
                if rng.random() < 0.9:
                    n.cands.append((f"m{k}", node(False, ply + 1), True, 0.0))
                else:
                    n.cands.append((f"m{k}", None, False, rng.uniform(0.3, 0.75)))
        else:
            k = rng.randint(1, 5)
            w = [rng.random() for _ in range(k)]
            rest = rng.uniform(0.0, 0.3)
            n = g.opp[h] = OppNode(const_base=rest * rng.uniform(0.3, 0.7))
            for x in w:
                p = x / sum(w) * (1 - rest)
                if grow and rng.random() < 0.9:
                    n.kids.append((p, node(True, ply + 1), True, 0.0))
                else:
                    n.kids.append((p, None, False, rng.uniform(0.3, 0.7)))
    # stop values: in real data the book move is a small edge over what is played out
    # of book, so set each (non-chain) stop value just under its best move's value,
    # bottom-up; without this most of a big random graph is not worth booking at all
    for h in sorted(g.our, key=lambda x: -g.ply[x]):
        if h in lumpy:
            continue
        n = g.our[h]
        best = max((_static(g, ch) if (in_s and ch is not None) else lv)
                   for _s, ch, in_s, lv in n.cands)
        n.l_node = best - rng.uniform(0.0, 0.04)
    set_max_reach(g)
    return g


def _static(g: Graph, h: int) -> float:
    if h in g.our:
        return g.our[h].l_node
    n = g.opp[h]
    return n.const_base + sum(p * (g.our[k].l_node if (in_s and k in g.our) else lv)
                              for p, k, in_s, lv in n.kids)


def scale(sizes: list[int], targets: list[int], modes=None) -> None:
    """Wall-clock of the production price search against the DAG modes on big_dag."""
    quiet = lambda *_: None  # noqa: E731
    print(f"  {'nodes':>9}{'target':>8}  {'mode':<16}{'seconds':>9}{'x price':>9}"
          f"{'size':>7}{'value':>10}")
    for n in sizes:
        g = big_dag(random.Random(n), n)
        order, _cut = topo_up(g)
        seed = source_seed(g, order)
        incs: dict[int, dict] = {}
        for B in targets:
            base = None
            for mode in (None,) + tuple(DAG_MODES if modes is None else modes):
                t0 = time.time()
                if mode == "dp+polish":
                    # the curve engine, then polish from its book: two rows, the second
                    # timing the polish alone
                    curves, _ = build_curves(g, bmax=B)
                    dp = extract_book(g, curves, B)["booked"]
                    dt = time.time() - t0
                    pv = policy_value(g, dp, order)
                    print(f"  {len(g.our) + len(g.opp):>9,}{B:>8,}  {'dp':<16}"
                          f"{dt:>9.1f}{dt / base:>9.2f}{pv['played']:>7,}"
                          f"{pv['value']:>10.5f}", flush=True)
                    t0 = time.time()
                    moves, _st = polish_book(g, dp, B, order)
                    res = book_result(g, moves, order)
                    mode = "+polish (alone)"
                elif mode == "polish+stick":
                    # rebuild stickiness at margin 0.01: the incumbent is the polished
                    # book of a perturbed copy (perturbed()); the search runs on g
                    if B not in incs:
                        h = perturbed(g, random.Random(B))
                        incs[B] = price_search(h, B, log=quiet, rounds=4, select="best",
                                               reprice="max", seed_reach=seed,
                                               dag_mode="polish")[0]["booked"]
                        t0 = time.time()
                    g.meta = {"stick": (incs[B], 0.01)}
                    res, _ = price_search(g, B, log=quiet, rounds=4, select="best",
                                          reprice="max", seed_reach=seed, dag_mode="polish")
                    g.meta = {}
                else:
                    res, _ = price_search(g, B, log=quiet, rounds=4, select="best",
                                          reprice="max", seed_reach=seed, dag_mode=mode)
                dt = time.time() - t0
                base = dt if mode is None else base
                print(f"  {len(g.our) + len(g.opp):>9,}{B:>8,}  {mode or 'price':<16}"
                      f"{dt:>9.1f}{dt / base:>9.2f}{res['spent_distinct']:>7,}"
                      f"{res['root_value_realized']:>10.5f}", flush=True)


# ── hand cases (as in _test_budget_price / _test_budget_transpo) ────────────

def case_d() -> Graph:
    g = Graph(root=1, our={}, opp={}, epd={}, ply={}, reach={})
    g.opp[1] = OppNode(const_base=0.0, kids=[(0.5, 2, True, 0), (0.5, 3, True, 0)])
    g.our[2] = OurNode(l_node=0.5, cands=[("a", 4, True, 0)])
    g.our[3] = OurNode(l_node=0.5, cands=[("b", 5, True, 0)])
    g.opp[4] = OppNode(const_base=0.0, kids=[(1.0, 6, True, 0)])
    g.opp[5] = OppNode(const_base=0.0, kids=[(1.0, 6, True, 0)])
    g.our[6] = OurNode(l_node=0.5, cands=[("x", None, False, 0.6)])
    g.ply = {1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4}
    g.reach = {1: 1, 2: .5, 3: .5, 4: .5, 5: .5, 6: .5}
    return g


def case_f(back_hash: int) -> Graph:
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
    return g


def case_t8() -> Graph:
    return Graph(root=1,
                 our={1: OurNode(l_node=0.40, cands=[("m", 2, True, 0.0)]),
                      3: OurNode(l_node=0.50, cands=[("t", None, False, 0.90)])},
                 opp={2: OppNode(const_base=0.0, kids=[(0.5, 3, True, 0.0),
                                                       (0.5, 3, True, 0.0)])},
                 epd={}, ply={1: 0, 2: 1, 3: 2}, reach={1: 1.0, 2: 1.0, 3: 0.5})


# ── engines under test ──────────────────────────────────────────────────────

def source_seed(g: Graph, order: list[int]) -> dict[int, float]:
    """--price-seed source with the unbudgeted best book as the source rep."""
    src = price_book(g, 1e-12, order)["booked"]
    seed = dict(g.reach)
    seed.update({h: max(r, seed.get(h, 0.0))
                 for h, r in policy_reach(g, src, order).items() if r > 0})
    return seed


DAG_MODES = ("fixpoint", "polish", "fixpoint+polish")
BASELINES = ("price", "price_dflt", "dp", "dp_leftover")


def engines(g: Graph, order: list[int], B: int) -> dict[str, dict]:
    quiet = lambda *_: None  # noqa: E731
    out = {}
    res, _ = price_search(g, B, log=quiet, rounds=4, select="best", reprice="max",
                          seed_reach=source_seed(g, order))
    out["price"] = res["booked"]
    res, _ = price_search(g, B, log=quiet)
    out["price_dflt"] = res["booked"]
    for mode in DAG_MODES:
        res, _ = price_search(g, B, log=quiet, rounds=4, select="best", reprice="max",
                              seed_reach=source_seed(g, order), dag_mode=mode)
        out[mode] = res["booked"]
    curves, _ = build_curves(g, bmax=B)
    out["dp"] = extract_book(g, curves, B)["booked"]
    out["dp_leftover"] = extract_book(g, curves, B, leftover=True)["booked"]
    out["dp+polish"] = polish_book(g, out["dp"], B, order)[0]
    res, _ = price_search(g, B, log=quiet, rounds=4, select="best", reprice="max",
                          seed_reach=source_seed(g, order), dag_mode="polish",
                          polish_from=[out["dp"]])
    out["polish+dp"] = res["booked"]
    return out


class Tally:
    def __init__(self):
        self.n = self.fit = self.exact = 0
        self.gaps: list[float] = []
        self.own: list[float] = []

    def add(self, opt, B, value, played):
        self.n += 1
        if played < len(opt):
            self.own.append(opt[played] - value)
        if played <= B:
            self.fit += 1
            gap = opt[B] - value
            self.gaps.append(gap)
            if gap <= TOL:
                self.exact += 1

    def line(self, name):
        g = np.array(self.gaps) if self.gaps else np.zeros(1)
        o = np.array(self.own) if self.own else np.zeros(1)
        return (f"  {name:<14}{self.fit / self.n:>7.1%}{self.exact / self.n:>8.1%}"
                f"{g.mean():>10.5f}{g.max():>10.5f}{o.mean():>10.5f}{o.max():>10.5f}")


def report(n_graphs: int = 300, seed: int = 20261028, transpo: float = 0.35
           ) -> tuple[dict[str, Tally], dict]:
    rng = random.Random(seed)
    tallies: dict[str, Tally] = {}
    t_or = t_en = 0.0
    n_inst = n_transpo = 0
    worse = {m: {"both": 0, "either": 0} for m in DAG_MODES + ("dp+polish",)}
    below_dp = 0                        # dp+polish below dp
    not_max = 0                         # polish+dp is not the better of polish, dp+polish
    for t in range(n_graphs):
        g = random_dag(rng, root_ours=(t % 2 == 0), transpo=transpo)
        while len(g.our) < 3:
            g = random_dag(rng, root_ours=(t % 2 == 0), transpo=transpo)
        n_our = len(g.our)
        budgets = sorted({b for b in (1, 2, 3, 5, 8, 12) if b <= n_our})
        t0 = time.time()
        opt, _books = exact_dag(g, max(budgets))
        t_or += time.time() - t0
        order, _cut = topo_up(g)
        par: dict[int, int] = {}
        for n in list(g.our.values()) + list(g.opp.values()):
            for e in getattr(n, "cands", []) or getattr(n, "kids", []):
                if e[2] and e[1] is not None:
                    par[e[1]] = par.get(e[1], 0) + 1
        n_transpo += any(v > 1 for v in par.values())
        for B in budgets:
            t0 = time.time()
            books = engines(g, order, B)
            t_en += time.time() - t0
            n_inst += 1
            got = {}
            for name, moves in books.items():
                pv = policy_value(g, moves, order)
                tallies.setdefault(name, Tally()).add(opt, B, pv["value"], pv["played"])
                got[name] = pv["value"] if pv["played"] <= B else -np.inf
            for mode in DAG_MODES + ("dp+polish",):
                v = got[mode]
                worse[mode]["both"] += (v < got["price"] - TOL and v < got["dp"] - TOL)
                worse[mode]["either"] += (v < max(got["price"], got["dp"]) - TOL)
            below_dp += got["dp+polish"] < got["dp"] - TOL
            not_max += abs(got["polish+dp"] - max(got["polish"], got["dp+polish"])) > 1e-15
    print(f"\n  {n_graphs} random DAGs ({n_transpo} with a transposition), {n_inst} "
          f"(graph, budget) instances; oracle {t_or:.1f}s, engines {t_en:.1f}s")
    print(f"  {'engine':<14}{'fits':>7}{'exact':>8}{'gap mean':>10}{'gap max':>10}"
          f"{'own mean':>10}{'own max':>10}")
    for name, tl in tallies.items():
        print(tl.line(name))
    print("  (fits: book within budget; exact: fits and equals the oracle at the budget;"
          "\n   gap: oracle(budget) - value over the books that fit;"
          " own: oracle(own size) - value)")
    for mode in DAG_MODES + ("dp+polish",):
        print(f"  {mode}: below BOTH price and dp on {worse[mode]['both']} instances, "
              f"below the better of the two on {worse[mode]['either']}")
    print(f"  dp+polish: below dp on {below_dp} instances; polish+dp (polish_from=[dp]) "
          f"differs from the better of polish and dp+polish on {not_max}")
    worse["dp+polish"]["below_dp"] = below_dp
    worse["polish+dp"] = {"not_max": not_max}
    return tallies, worse


# ── stickiness (--incumbent) ────────────────────────────────────────────────

STICK_MARGINS = (0.0, 0.005, 0.01, 0.02)


def changed(g: Graph, moves: dict, inc: dict, W: dict) -> list[int]:
    """Positions the book plays (W > 0) where the incumbent's move is a candidate and
    the book plays another: what polish_book's objective charges."""
    return [u for u, m in moves.items() if W.get(u, 0.0) > 0 and u in inc
            and m.rstrip("+#") != inc[u]
            and any(c[0].rstrip("+#") == inc[u] for c in g.our[u].cands)]


def sticky(g: Graph, moves: dict, order: list[int], inc: dict, margin: float) -> dict:
    """J = value - margin * (reach of the changed positions), from policy_value and
    policy_reach alone (an independent statement of polish_book's objective)."""
    pv = policy_value(g, moves, order)
    W = policy_reach(g, moves, order)
    ch = changed(g, moves, inc, W)
    rc = sum(W[u] for u in ch)
    return {"value": pv["value"], "played": pv["played"], "changed": len(ch),
            "reach_changed": rc, "J": pv["value"] - margin * rc}


def perturbed(g: Graph, rng: random.Random, sd: float = 0.03) -> Graph:
    """The same graph with every stop and leaf value moved by up to sd: a stand-in for
    the data the incumbent book was built on."""
    h = copy.deepcopy(g)
    for n in h.our.values():
        n.l_node += rng.uniform(-sd, sd)
        n.cands = [(s, ch, ins, lv if ins else lv + rng.uniform(-sd, sd))
                   for s, ch, ins, lv in n.cands]
    for n in h.opp.values():
        n.kids = [(p, ch, ins, lv if ins else lv + rng.uniform(-sd, sd))
                  for p, ch, ins, lv in n.kids]
    return h


def sticky_engines(g: Graph, order: list[int], B: int, seed: dict) -> dict[str, dict]:
    """The recipe engines under whatever g.meta["stick"] holds: the price search alone
    and with polish (--price-seed source --price-reprice max --price-rounds 1
    --price-select best [--price-dag-mode polish]), dp and dp+polish."""
    quiet = lambda *_: None  # noqa: E731
    out = {}
    for name, mode in (("price", None), ("polish", "polish")):
        res, _ = price_search(g, B, log=quiet, rounds=1, select="best", reprice="max",
                              seed_reach=seed, dag_mode=mode)
        out[name] = res["booked"]
    curves, _ = build_curves(g, bmax=B)
    out["dp"] = extract_book(g, curves, B)["booked"]
    out["dp+polish"] = polish_book(g, out["dp"], B, order)[0]
    return out


def stick_checks() -> None:
    """S0: no stick, or a margin of 0, is the same polish bit for bit (polish_book and
    price_search dag_mode='polish'). S1: with a huge margin polish never changes a move
    its start kept from the incumbent. S2: sticky polish fits, never lowers its start's
    J, and reports J and the changed positions as sticky() computes them."""
    quiet = lambda *_: None  # noqa: E731
    rng = random.Random(41)
    bad0 = bad1 = bad2 = n1 = 0
    for t in range(60):
        g = random_dag(rng, root_ours=(t % 2 == 0))
        order, _cut = topo_up(g)
        seed = source_seed(g, order)
        inc = {h: rng.choice(n.cands)[0] for h, n in g.our.items() if rng.random() < 0.7}
        starts = [{}, {h: rng.choice(n.cands)[0] for h, n in g.our.items()
                       if rng.random() < 0.5}, dict(inc)]
        for B in (2, 5, 8):
            g.meta = {}
            ref = [polish_book(g, st, B, order) for st in starts]
            rp, ip = price_search(g, B, log=quiet, rounds=1, select="best", reprice="max",
                                  seed_reach=seed, dag_mode="polish")
            g.meta = {"stick": (inc, 0.0)}
            got = [polish_book(g, st, B, order) for st in starts]
            rq, iq = price_search(g, B, log=quiet, rounds=1, select="best", reprice="max",
                                  seed_reach=seed, dag_mode="polish")
            for (ma, sa), (mb, sb) in zip(ref, got):
                bad0 += ma != mb or {**sa, "seconds": 0} != {**sb, "seconds": 0}
            bad0 += (rp["booked"] != rq["booked"]
                     or rp["root_value_realized"] != rq["root_value_realized"]
                     or ip["sweep"] != iq["sweep"])
            for margin in (1e9, 0.01):
                g.meta = {"stick": (inc, margin)}
                rs, _ = price_search(g, B, log=quiet, rounds=1, select="best",
                                     reprice="max", seed_reach=seed)
                curves, _ = build_curves(g, bmax=B)
                for st in starts + [rs["booked"], extract_book(g, curves, B)["booked"]]:
                    mv, pst = polish_book(g, st, B, order)
                    sj = sticky(g, mv, order, inc, margin)
                    s0 = sticky(g, st, order, inc, margin)
                    if margin > 1:
                        W0 = policy_reach(g, st, order)
                        kept = [u for u, m in st.items() if W0.get(u, 0.0) > 0
                                and inc.get(u) == m]
                        n1 += len(kept)
                        bad1 += sum(1 for u in kept if u in mv and mv[u] != inc[u])
                    else:
                        bad2 += (sj["played"] > B
                                 or (s0["played"] <= B and sj["J"] < s0["J"] - 1e-12)
                                 or abs(pst.get("objective", pst["value"]) - sj["J"]) > 1e-12
                                 or pst.get("changed", 0) != sj["changed"])
    check("S0: no stick and margin 0 give the same polish, bit for bit", bad0 == 0,
          f"{bad0} differ")
    check("S1: with a huge margin polish never changes a move its start kept",
          bad1 == 0, f"{bad1} of {n1} kept moves changed")
    check("S2: sticky polish fits, never lowers its start's J, reports J and churn",
          bad2 == 0, f"{bad2} off")


def stick_report(n_graphs: int = 60, seed: int = 20261029, sd: float = 0.03) -> dict:
    """Churn vs value on random DAGs. The incumbent at each budget is the exact optimum
    of a perturbed copy of the graph (perturbed()); the rebuild runs each recipe engine
    with g.meta["stick"] = (incumbent, margin). Per engine and margin: value gap to the
    true optimum, gap to the exact optimum of J (sticky polish's objective, by the same
    enumeration), and the changed positions (count, reach). Margin 0 is the unsticky
    rebuild, its churn measured against the same incumbent."""
    rng = random.Random(seed)
    names = ("incumbent", "price", "polish", "dp", "dp+polish")
    acc = {(e, m): {"n": 0, "fit": 0, "gap": 0.0, "jgap": 0.0, "jexact": 0, "changed": 0,
                    "reach_changed": 0.0} for e in names for m in STICK_MARGINS}
    t0 = time.time()
    for t in range(n_graphs):
        g = random_dag(rng, root_ours=(t % 2 == 0))
        while len(g.our) < 3:
            g = random_dag(rng, root_ours=(t % 2 == 0))
        budgets = sorted({b for b in (2, 3, 5, 8, 12) if b <= len(g.our)})
        bmax = max(budgets)
        _o, inc_books = exact_dag(perturbed(g, rng, sd), bmax)
        incs = {B: {h: m for h, m in (inc_books[B] or {}).items()} for B in budgets}
        order, _cut = topo_up(g)
        # J's optimum at every (budget, margin) from ONE enumeration of the true graph
        jbest = {(B, m): np.full(bmax + 1, -np.inf) for B in budgets for m in STICK_MARGINS}

        def on_book(moves, value, n, order=order, incs=incs, jbest=jbest):
            W = policy_reach(g, moves, order)
            for B in budgets:
                rc = sum(W[u] for u in changed(g, moves, incs[B], W))
                for m in STICK_MARGINS:
                    a = jbest[(B, m)]
                    a[n] = max(a[n], value - m * rc)
        opt, _books = exact_dag(g, bmax, on_book)
        for a in jbest.values():
            np.maximum.accumulate(a, out=a)
        seed_r = source_seed(g, order)
        for B in budgets:
            inc = incs[B]
            for m in STICK_MARGINS:
                g.meta = {"stick": (inc, m)} if m else {}
                books = sticky_engines(g, order, B, seed_r)
                books["incumbent"] = inc
                g.meta = {}
                for e in names:
                    r = sticky(g, books[e], order, inc, m)
                    a = acc[(e, m)]
                    a["n"] += 1
                    if r["played"] > B:
                        continue
                    a["fit"] += 1
                    a["gap"] += opt[B] - r["value"]
                    jg = jbest[(B, m)][B] - r["J"]
                    a["jgap"] += jg
                    a["jexact"] += jg <= TOL
                    a["changed"] += r["changed"]
                    a["reach_changed"] += r["reach_changed"]
    n_inst = acc[("polish", 0.0)]["n"]
    print(f"\n  stickiness: {n_graphs} random DAGs, {n_inst} (graph, budget) instances; "
          f"incumbent = the exact optimum of a copy perturbed by +-{sd:g}; "
          f"{time.time() - t0:.1f}s")
    print(f"  {'engine':<11}{'margin':>7}{'fits':>7}{'gap':>9}{'J gap':>9}{'J exact':>9}"
          f"{'changed':>9}{'reach chg':>11}")
    for e in names:
        for m in STICK_MARGINS:
            a = acc[(e, m)]
            f = max(a["fit"], 1)
            print(f"  {e:<11}{m:>7g}{a['fit'] / a['n']:>7.1%}{a['gap'] / f:>9.5f}"
                  f"{a['jgap'] / f:>9.5f}{a['jexact'] / a['n']:>9.1%}"
                  f"{a['changed'] / f:>9.3f}{a['reach_changed'] / f:>11.4f}")
    print("  (gap: true optimum - value; J gap: optimum of J - J, J = value - margin x reach "
          "of changed\n   positions; changed: positions where the book plays another move "
          "than the incumbent's, per instance)")
    return {k: {**v, "reach_changed": v["reach_changed"] / max(v["fit"], 1)}
            for k, v in acc.items() if k[0] == "polish"}


def main() -> int:
    # O: the oracle on trees equals the tree oracle
    from _test_budget_price import exact as exact_tree, random_tree
    rng = random.Random(5)
    bad = 0
    for t in range(25):
        g = random_tree(rng, root_ours=(t % 2 == 0), depth=4)
        bmax = min(len(g.our), 6)
        ours, _ = exact_dag(g, bmax)
        if np.max(np.abs(ours - exact_tree(g, bmax))) > TOL:
            bad += 1
    check("O: on random trees the DAG oracle equals the tree oracle", bad == 0, f"{bad} off")

    opt, books = exact_dag(case_d(), 3)
    check("O: case D -- X counted once: 3 positions give 0.6, 2 only one path's 0.55",
          abs(opt[3] - 0.6) < TOL and abs(opt[2] - 0.55) < TOL and books[3] is not None
          and len(books[3]) == 3, f"{opt}")
    for bh in (50, 5):
        opt, books = exact_dag(case_f(bh), 3)
        check(f"O: case F (back edge hash {bh}) -- the main line survives the cycle",
              abs(opt[3] - 0.9) < TOL and books[3] == {1: "m", 10: "good", 40: "x"},
              f"{opt}, {books[3]}")
    opt, _ = exact_dag(case_t8(), 2)
    check("O: case T8 -- two replies into one position, booked once: 2 moves give 0.9",
          abs(opt[2] - 0.9) < TOL and abs(opt[1] - 0.5) < TOL, f"{opt}")

    if "--scale" in sys.argv:
        i = sys.argv.index("--scale")
        sizes = [int(x) for x in sys.argv[i + 1].split(",")]
        targets = ([int(x) for x in sys.argv[i + 2].split(",")]
                   if len(sys.argv) > i + 2 and not sys.argv[i + 2].startswith("--") else [400, 2000])
        modes = (sys.argv[sys.argv.index("--modes") + 1].split(",") if "--modes" in sys.argv
                 else DAG_MODES)
        scale(sizes, targets, modes)
        return 0
    # E: the region evaluator behind book_result / polish_book is policy_value
    rng = random.Random(17)
    bad = 0
    for t in range(60):
        g = random_dag(rng, root_ours=(t % 2 == 0))
        order, _cut = topo_up(g)
        for _ in range(5):
            moves = {h: rng.choice(n.cands)[0] for h, n in g.our.items() if rng.random() < 0.6}
            pv = policy_value(g, moves, order)
            br = book_result(g, moves, order)
            if pv["value"] != br["root_value_realized"] or pv["played"] != br["spent_distinct"]:
                bad += 1
            if br["reach_policy"] != {h: r for h, r in policy_reach(g, br["booked"], order).items()
                                      if r > 0}:
                bad += 1
    for bh in (50, 5):
        g = case_f(bh)
        order, _cut = topo_up(g)
        for moves in ({1: "m", 10: "good", 40: "x"}, {1: "m", 10: "back"}, {10: "good"}):
            pv = policy_value(g, moves, order)
            br = book_result(g, moves, order)
            bad += pv["value"] != br["root_value_realized"]
    check("E: book_result's value, size and reach are policy_value's, bit for bit", bad == 0,
          f"{bad} off")

    # N: dag_mode=None is the unchanged search; P: polish never exceeds the target and
    # never lowers its start's value
    rng = random.Random(23)
    bad_n = bad_p = 0
    quiet = lambda *_: None  # noqa: E731
    for t in range(40):
        g = random_dag(rng, root_ours=(t % 2 == 0))
        order, _cut = topo_up(g)
        for B in (2, 5):
            a, ia = price_search(g, B, log=quiet, rounds=3, reprice="max")
            b, ib = price_search(g, B, log=quiet, rounds=3, reprice="max", dag_mode=None)
            bad_n += (a["booked"] != b["booked"] or a["root_value_realized"] != b["root_value_realized"]
                      or ia["sweep"] != ib["sweep"] or "polish" in ib)
            for start in (a["booked"], {}):
                moves, st = polish_book(g, start, B, order)
                v0 = book_result(g, start, order)
                pv = policy_value(g, moves, order)
                bad_p += (pv["played"] > B or pv["value"] != st["value"]
                          or (v0["spent_distinct"] <= B
                              and st["value"] < v0["root_value_realized"] - 1e-15))
    check("N: dag_mode=None returns the unchanged search", bad_n == 0, f"{bad_n} differ")
    check("P: polish fits the target, never loses value, reports policy_value's value",
          bad_p == 0, f"{bad_p} off")

    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    tallies, worse = report(n)
    pol = tallies["polish"]
    gaps = np.array(pol.gaps)
    check("G: polish always fits the budget", pol.fit == pol.n, f"{pol.fit}/{pol.n}")
    check("G: polish is never below both price and dp", worse["polish"]["both"] == 0,
          f"{worse['polish']['both']} instances")
    check("G: polish is (near-)exact: >= 99% of instances, mean gap < 1e-4",
          pol.exact >= 0.99 * pol.n and gaps.mean() < 1e-4,
          f"{pol.exact / pol.n:.1%} exact, mean {gaps.mean():.2e}, max {gaps.max():.4f}")
    dpp = tallies["dp+polish"]
    check("G: dp+polish always fits and is never below dp",
          dpp.fit == dpp.n and worse["dp+polish"]["below_dp"] == 0,
          f"fits {dpp.fit}/{dpp.n}, below dp on {worse['dp+polish']['below_dp']}")
    check("G: polish_from=[dp] returns the better of polish and dp+polish (to 1e-15)",
          worse["polish+dp"]["not_max"] == 0, f"{worse['polish+dp']['not_max']} differ")
    check("G: dp+polish is at least as often exact as dp",
          dpp.exact >= tallies["dp"].exact,
          f"{dpp.exact / dpp.n:.1%} vs dp {tallies['dp'].exact / dpp.n:.1%}")

    # S (stickiness): checks, then churn vs value at the margins of the brief
    stick_checks()
    ns = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    sr = stick_report(ns)
    check("S: under every margin, sticky polish fits and is (near-)exact on its objective "
          "J: >= 98% of instances", all(r["fit"] == r["n"] and r["jexact"] >= 0.98 * r["n"]
                                        for (e, _m), r in sr.items() if e == "polish"),
          ", ".join(f"{m:g}: {r['jexact'] / r['n']:.1%}" for (e, m), r in sr.items()
                    if e == "polish"))
    check("S: churn falls as the margin rises (sticky polish, changed reach)",
          all(sr[("polish", a)]["reach_changed"] >= sr[("polish", b)]["reach_changed"] - 1e-12
              for a, b in zip(STICK_MARGINS, STICK_MARGINS[1:])))
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
