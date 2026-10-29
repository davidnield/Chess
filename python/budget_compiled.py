"""Compiled (integer-indexed, numpy) form of a budget_core.Graph for the price search.

WHY
---
price_book, policy_reach and policy_value walk the whole graph in topological order
with dict lookups per edge: O(graph) pure Python per call, and price_search calls
price_book 11-120 times per book. A CompiledGraph is built once per search (one O(graph)
Python pass, about the cost of one dict solve) and every later solve is a handful of
numpy array sweeps.

BIT-IDENTICAL BY CONSTRUCTION
-----------------------------
The arrays reproduce the dict walks' floats exactly, not approximately:

- Node index = position in topo_up's order, so "child already valued" (the dict walks'
  `ch in V`) is `child index < parent index`; any other in-subgraph child is a back
  edge and reads the node's static value, computed by the same Python expression.
- The bottom-up sweep runs level by level (a node's level is one more than its highest
  forward child's), so every value a node reads is final before it is read. Inside a
  node, its edges are visited in list order: an opponent node's value is the same left
  fold `((const_base + p0*v0) + p1*v1) + ...` (one IEEE multiply and one add per edge,
  no fused multiply-add, no reassociation), and an our-node keeps the FIRST candidate
  with the strictly greatest key, as the dict walk's `key > best[0]` does.
- The top-down reach pass sums each node's incoming reach in the order the dict walk
  adds it: parents by descending order position, a parent's edges in list order, then
  the contributions that arrive through cut back edges.

POLISH
------
polish_book verifies up to 24 proposals a step, each of which changes a few moves of a
book whose region (the positions it reaches) is thousands of nodes. region() is
_book_region on indices; quick() prices a proposal exactly by re-deriving only what the
change can touch -- membership below the changed moves, and the values of the nodes whose
inputs changed, children first with the same expressions, stopping where a value comes
out bitwise unchanged; advance() rebuilds the accepted proposal's region from the same
delta, in _book_region's dict insertion orders. Every node they do not recompute keeps
floats that a full pass would reproduce, because its inputs are identical.

The equality is pinned by _test_budget_speed.py against the dict implementations, which
stay in budget_core (`compiled=None`, `engine="legacy"`) for exactly that purpose.

A CompiledGraph is a SNAPSHOT: compile after the graph's values are final (after
apply_stop_values) and recompile after mutating it. Graphs the arrays cannot represent
faithfully (an in-subgraph child that is not a node of the graph, a root outside the
graph) compile with ok=False and every caller falls back to the dict implementation.
"""
from __future__ import annotations

import bisect
import contextlib
import gc
import heapq
from array import array

import numpy as np

# Tests set this: polish_book then re-evaluates every incrementally rebuilt region in full
# and raises if any dict, insertion order or float differs.
CHECK_ADVANCE = False


def _expand(starts: np.ndarray, lens: np.ndarray) -> np.ndarray:
    """Concatenated ranges [s, s+l) for each (s, l), in order."""
    lens = lens.astype(np.int64, copy=False)
    total = int(lens.sum())
    if total == 0:
        return np.zeros(0, dtype=np.int64)
    offs = np.cumsum(lens) - lens
    return np.repeat(starts.astype(np.int64, copy=False) - offs, lens) + np.arange(total)


def _jds(group_nodes: np.ndarray, level: np.ndarray, nlev: int, deg: np.ndarray,
         e_owner: np.ndarray, e_col: np.ndarray):
    """Jagged-diagonal layout per level. Nodes of each level are ordered by degree
    descending, so column j's edges belong to the first cnt_j nodes of the level and
    `acc[:cnt_j] += ...` touches exactly the nodes that have a j-th edge. Returns
    (per-level node arrays, per-level [(cnt, start, stop)] column slices, edge perm)."""
    ordn = group_nodes[np.lexsort((-deg[group_nodes], level[group_nodes]))]
    lvls = level[ordn]
    bounds = np.searchsorted(lvls, np.arange(nlev + 1))
    rank = np.zeros(level.size, dtype=np.int64)
    rank[ordn] = np.arange(ordn.size) - bounds[lvls]
    perm = np.lexsort((rank[e_owner], e_col, level[e_owner]))
    nodes = [ordn[bounds[L]:bounds[L + 1]].astype(np.int64) for L in range(nlev)]
    cols: list[list[tuple[int, int, int]]] = [[] for _ in range(nlev)]
    if perm.size:
        lv_e = level[e_owner][perm]
        col_e = e_col[perm]
        brk = np.flatnonzero((np.diff(lv_e) != 0) | (np.diff(col_e) != 0)) + 1
        starts = np.concatenate(([0], brk))
        stops = np.concatenate((brk, [perm.size]))
        for a, b, L in zip(starts.tolist(), stops.tolist(), lv_e[starts].tolist()):
            cols[L].append((b - a, a, b))
    return nodes, cols, perm


class CompiledGraph:
    """Integer-indexed arrays of a Graph (see the module docstring). Index i is the
    node at order[i]. Our-node candidate edges and opponent-node reply edges are CSR
    (c_* and k_*); an edge whose value is fixed (out of the subgraph: its leaf value;
    a cut back edge: the child's static value) reads a constant slot past N in the
    value arrays, so every edge is one gather."""

    def __init__(self, g, order=None, cut=None):
        from budget_core import topo_up
        if order is None:
            order, cut = topo_up(g)
        self.g = g
        self.order = order
        self.cut = set() if cut is None else cut
        N = self.N = len(order)
        pos = self.pos = {h: i for i, h in enumerate(order)}
        r = pos.get(g.root)
        self.root = -1 if r is None else r
        self.ok = r is not None

        flags = bytearray(N)
        lnode = array("d")
        cbase = array("d")
        c_ptr, c_child, c_lv, c_san = array("q", [0]), array("q"), array("d"), []
        k_ptr, k_child, k_p, k_lv = array("q", [0]), array("q"), array("d"), array("d")
        dup_nodes: list[int] = []          # our nodes carrying a repeated SAN
        our_d, opp_d, pget = g.our, g.opp, pos.get
        # child index per edge: -1 out of the subgraph, -2 an in-subgraph child that is not
        # a node (the dict walks would fail on it: ok=False sends every call to them)
        for i, h in enumerate(order):
            n = our_d.get(h)
            if n is not None:
                flags[i] = 1
                lnode.append(n.l_node)
                cbase.append(0.0)
                if n.cands:
                    sans, chs, subs, lvs = zip(*n.cands)
                    c_san.extend(sans)
                    c_child.extend([pget(ch, -2) if (sb and ch is not None) else -1
                                    for ch, sb in zip(chs, subs)])
                    c_lv.extend(lvs)
                    if len(set(sans)) < len(sans):
                        dup_nodes.append(i)
            else:
                n = opp_d[h]
                lnode.append(0.0)
                cbase.append(n.const_base)
                if n.kids:
                    ps, chs, subs, lvs = zip(*n.kids)
                    k_child.extend([pget(ch, -2) if (sb and ch is not None) else -1
                                    for ch, sb in zip(chs, subs)])
                    k_p.extend(ps)
                    k_lv.extend(lvs)
            c_ptr.append(len(c_child))
            k_ptr.append(len(k_child))
        if N >= 2 ** 31:
            self.ok = False
        self.is_our = np.frombuffer(bytes(flags), dtype=np.uint8).astype(bool)
        self.l_node = np.frombuffer(lnode, dtype=np.float64).copy()
        self.const_base = np.frombuffer(cbase, dtype=np.float64).copy()
        self.c_ptr = np.frombuffer(c_ptr, dtype=np.int64).copy()
        self.k_ptr = np.frombuffer(k_ptr, dtype=np.int64).copy()
        self.c_child = np.frombuffer(c_child, dtype=np.int64).astype(np.int32)
        self.k_child = np.frombuffer(k_child, dtype=np.int64).astype(np.int32)
        for a in (self.c_child, self.k_child):
            bad = a == -2
            if bad.any():
                self.ok = False
                a[bad] = -1
        self.k_p = np.frombuffer(k_p, dtype=np.float64).copy()
        self.c_san = c_san
        del c_child, k_child, k_p, lnode, cbase, c_ptr, k_ptr
        Ec, Ek = self.Ec, self.Ek = len(c_san), self.k_child.size
        # the first edge of the same node with the same SAN: the edge a booked move's
        # reach follows (the dict walks take the first candidate carrying the SAN)
        c_first = np.arange(Ec, dtype=np.int32)
        for i in dup_nodes:
            seen: dict[str, int] = {}
            for e in range(int(self.c_ptr[i]), int(self.c_ptr[i + 1])):
                c_first[e] = seen.setdefault(c_san[e], e)
        self.c_first = c_first
        c_deg = np.diff(self.c_ptr)
        k_deg = np.diff(self.k_ptr)
        c_par = np.repeat(np.arange(N, dtype=np.int64), c_deg)
        k_par = np.repeat(np.arange(N, dtype=np.int64), k_deg)

        # value sources: a forward child's index, or a constant slot past N
        c_fwd = (self.c_child >= 0) & (self.c_child < c_par)
        k_fwd = (self.k_child >= 0) & (self.k_child < k_par)
        self._static: dict[int, float] = {}
        static = self.static

        c_const = np.frombuffer(c_lv, dtype=np.float64)[~c_fwd].copy()
        k_const = np.frombuffer(k_lv, dtype=np.float64)[~k_fwd].copy()
        del c_lv, k_lv
        for consts, child, fwd in ((c_const, self.c_child, c_fwd), (k_const, self.k_child, k_fwd)):
            back = np.flatnonzero(child[~fwd] >= 0)
            for j, ci in zip(back.tolist(), child[~fwd][back].tolist()):
                consts[j] = static(ci)
        nc = c_const.size
        c_src = np.empty(Ec, dtype=np.int64)
        c_src[c_fwd] = self.c_child[c_fwd]
        c_src[~c_fwd] = N + np.arange(nc)
        k_src = np.empty(Ek, dtype=np.int64)
        k_src[k_fwd] = self.k_child[k_fwd]
        k_src[~k_fwd] = N + nc + np.arange(k_const.size)
        self.c_src = c_src.astype(np.int32)
        self.ext0 = np.concatenate((np.zeros(N), c_const, k_const))
        if self.ext0.size >= 2 ** 31:
            self.ok = False

        # levels over forward edges: 0 = no forward child; else 1 + the highest child's
        fp = np.concatenate((c_par[c_fwd], k_par[k_fwd]))
        fc = np.concatenate((self.c_child[c_fwd], self.k_child[k_fwd])).astype(np.int64)
        fe = np.concatenate((np.flatnonzero(c_fwd), Ec + np.flatnonzero(k_fwd)))
        remaining = np.bincount(fp, minlength=N)
        o = np.argsort(fc, kind="stable")
        in_ptr = np.concatenate(([0], np.cumsum(np.bincount(fc, minlength=N))))
        par_by_child = fp[o]
        level = np.full(N, -1, dtype=np.int64)
        cur = np.flatnonzero(remaining == 0)
        L = 0
        while cur.size:
            level[cur] = L
            par = par_by_child[_expand(in_ptr[cur], in_ptr[cur + 1] - in_ptr[cur])]
            if not par.size:
                break
            u, cnt = np.unique(par, return_counts=True)
            remaining[u] -= cnt
            cur = u[remaining[u] == 0]
            L += 1
        if (level < 0).any():                  # cannot happen: forward edges go down
            self.ok = False
            level[level < 0] = 0
        nlev = self.nlev = int(level.max()) + 1 if N else 0

        # bottom-up layouts
        our_nodes = np.flatnonzero(self.is_our)
        opp_nodes = np.flatnonzero(~self.is_our)
        self.u_nodes, self.u_cols, perm = _jds(our_nodes, level, nlev, c_deg, c_par,
                                               np.arange(Ec) - self.c_ptr[c_par])
        self.u_src = self.c_src[perm]
        self.u_eid = perm.astype(np.int32)
        self.o_nodes, self.o_cols, perm = _jds(opp_nodes, level, nlev, k_deg, k_par,
                                               np.arange(Ek) - self.k_ptr[k_par])
        self.o_src = k_src[perm].astype(np.int32)
        self.o_p = self.k_p[perm]
        del perm, c_src, k_src

        # top-down layout: each node's forward in-edges by parent position descending,
        # a parent's edges in list order (global edge id: our e, opponent Ec + k)
        o = np.lexsort((fe, -fp, fc))
        fp, fc, fe = fp[o], fc[o], fe[o]
        indeg = np.bincount(fc, minlength=N)
        in_start = np.concatenate(([0], np.cumsum(indeg)))[:-1]
        tcol = np.arange(fc.size) - in_start[fc]
        self.t_nodes, self.t_cols, perm = _jds(np.flatnonzero(indeg > 0), level, nlev,
                                               indeg, fc, tcol)
        self.t_parent = fp[perm].astype(np.int32)
        self.t_eid = fe[perm].astype(np.int32)
        del o, fp, fc, fe, perm, tcol, in_start, par_by_child

        # cut back edges (child at or above the parent in the order): reach they carry
        # arrives after the child's own visit, by parent position descending
        cb = np.flatnonzero((self.c_child >= 0) & ~c_fwd)
        kb = np.flatnonzero((self.k_child >= 0) & ~k_fwd)
        bp = np.concatenate((c_par[cb], k_par[kb]))
        bc_ = np.concatenate((self.c_child[cb], self.k_child[kb]))
        be = np.concatenate((cb, Ec + kb))
        o = np.lexsort((be, -bp))
        self.back = list(zip(bp[o].tolist(), bc_[o].tolist(), be[o].tolist()))

        self.all_child = np.concatenate((self.c_child, self.k_child))
        self.w = np.concatenate((np.zeros(Ec), self.k_p))   # reach weight per edge
        self.our_idx = our_nodes
        self.our_hash = [order[i] for i in our_nodes.tolist()]
        self.our_flag = bytes(flags)
        self._adj: dict[int, object] = {}
        self._bonus = None
        self._reach_cache: tuple | None = None
        self._fixed_cache: tuple | None = None

    def static(self, i: int) -> float:
        """budget_core._static_value of node i, memoised (a pure function of the graph)."""
        v = self._static.get(i)
        if v is None:
            from budget_core import _static_value
            v = self._static[i] = _static_value(self.g, self.order[i])
        return v

    def adj(self, i: int):
        """Node i's edges with child indices resolved, built on first use (polish touches
        the book's region and frontier, a tiny part of the graph). Our node: SAN -> (child
        index or -1 when out of the subgraph, leaf value) of the FIRST candidate carrying
        the SAN. Opponent node: [(p, child index or -1, leaf value)] in reply order."""
        a = self._adj.get(i)
        if a is None:
            h, pos = self.order[i], self.pos
            n = self.g.our.get(h)
            if n is not None:
                a = {}
                for san, ch, in_sub, lv in n.cands:
                    if san not in a:
                        a[san] = (pos[ch] if (in_sub and ch is not None) else -1, lv)
            else:
                a = [(p, pos[ch] if (in_sub and ch is not None) else -1, lv)
                     for p, ch, in_sub, lv in self.g.opp[h].kids]
            self._adj[i] = a
        return a

    # ── per-call inputs ────────────────────────────────────────────────────────

    def reach_array(self, reach) -> np.ndarray:
        """Reach per node (our nodes only; price_book reads no other), cached for the
        last dict passed (one search round passes the same dict to every solve)."""
        if isinstance(reach, np.ndarray):
            return reach
        c = self._reach_cache
        if c is not None and c[0] is reach:
            return c[1]
        arr = np.zeros(self.N)
        get = reach.get
        arr[self.our_idx] = np.fromiter((get(h, 0.0) for h in self.our_hash), dtype=np.float64,
                                        count=len(self.our_hash))
        self._reach_cache = (reach, arr)
        return arr

    def bonus(self) -> np.ndarray | None:
        """Stickiness bonus per candidate edge (budget_core.incumbent_move / _bonus), or
        None when the graph has no incumbent (every key is the bare value)."""
        st = self.g.meta.get("stick") if self.g.meta else None
        if not st:
            return None
        if self._bonus is not None and self._bonus[0] is st:
            return self._bonus[1]
        moves, margin = st
        b = np.zeros(self.Ec)
        for i, h in zip(self.our_idx.tolist(), self.our_hash):
            im = moves.get(h)
            if im is None:
                continue
            for e in range(int(self.c_ptr[i]), int(self.c_ptr[i + 1])):
                if self.c_san[e].rstrip("+#") == im:
                    b[e] = margin
        self._bonus = (st, b)
        return b

    def allowed(self, fixed_policy) -> np.ndarray | None:
        """Per candidate edge: allowed by fixed_policy (its SAN is the policy's move)."""
        if fixed_policy is None:
            return None
        c = self._fixed_cache
        if c is not None and c[0] is fixed_policy:
            return c[1]
        a = np.zeros(self.Ec, dtype=bool)
        for h, want in fixed_policy.items():
            i = self.pos.get(h)
            if i is None or not self.is_our[i]:
                continue
            for e in range(int(self.c_ptr[i]), int(self.c_ptr[i + 1])):
                if self.c_san[e] == want:
                    a[e] = True
        self._fixed_cache = (fixed_policy, a)
        return a

    def first_match(self, moves) -> np.ndarray:
        """Per node: the first candidate edge whose SAN is moves[h], else -1 (the dict
        walks' `next(c for c in cands if c[0] == want)`)."""
        out = np.full(self.N, -1, dtype=np.int64)
        pget, is_our, c_ptr, c_san = self.pos.get, self.is_our, self.c_ptr, self.c_san
        for h, want in moves.items():
            i = pget(h)
            if i is None or want is None or not is_our[i]:
                continue
            for e in range(int(c_ptr[i]), int(c_ptr[i + 1])):
                if c_san[e] == want:
                    out[i] = e
                    break
        return out

    # ── sweeps ─────────────────────────────────────────────────────────────────

    def solve(self, lam: float, r_arr: np.ndarray, allowed=None):
        """price_book's bottom-up pass: (V, T, choice) with choice = the chosen edge per
        node (-1: stop)."""
        V = self.ext0.copy()
        T = self.ext0.copy()
        choice = np.full(self.N, -1, dtype=np.int64)
        bonus = self.bonus()
        u_src, u_eid, o_src, o_p = self.u_src, self.u_eid, self.o_src, self.o_p
        if allowed is not None:
            allowed = allowed[u_eid]
        if bonus is not None:
            bonus = bonus[u_eid]
        with np.errstate(divide="ignore", invalid="ignore"):
            for L in range(self.nlev):
                nodes = self.o_nodes[L]
                if nodes.size:
                    accV = self.const_base[nodes]
                    accT = accV.copy()
                    for cnt, a, b in self.o_cols[L]:
                        p, s = o_p[a:b], o_src[a:b]
                        accV[:cnt] += p * V[s]
                        accT[:cnt] += p * T[s]
                    V[nodes] = accV
                    T[nodes] = accT
                nodes = self.u_nodes[L]
                if nodes.size:
                    bv, bt, be, has = self._best(nodes.size, self.u_cols[L], V, T, bonus,
                                                 allowed)
                    r = r_arr[nodes]
                    ln = self.l_node[nodes]
                    pen = bv - lam / r
                    ok = has & (r > 0) & (pen > ln + 1e-15)
                    V[nodes] = np.where(ok, pen, ln)
                    T[nodes] = np.where(ok, bt, ln)
                    choice[nodes] = np.where(ok, be, -1)
        return V, T, choice

    def _best(self, m, cols, V, T, bonus, allowed):
        """Per our node of one level: the candidate the dict walk keeps -- the FIRST with
        the strictly greatest key (value + stickiness bonus) among the allowed ones --
        as (its value, its true value, its edge, whether there was any)."""
        u_src, u_eid = self.u_src, self.u_eid
        bv = np.zeros(m)
        bt = np.zeros(m)
        be = np.full(m, -1, dtype=np.int64)
        if allowed is None:
            # the first candidate always sets the best; nodes past cnt0 have none, and
            # every later column only touches nodes that have a first candidate
            cnt0 = cols[0][0] if cols else 0
            if cnt0:
                _c, a, b = cols[0]
                s = u_src[a:b]
                bv[:cnt0] = V[s]
                bt[:cnt0] = T[s]
                be[:cnt0] = u_eid[a:b]
            bk = bv if bonus is None else bv + np.concatenate((bonus[cols[0][1]:cols[0][2]],
                                                              np.zeros(m - cnt0)))
            for cnt, a, b in cols[1:]:
                s = u_src[a:b]
                v = V[s]
                key = v if bonus is None else v + bonus[a:b]
                upd = key > bk[:cnt]
                if bonus is not None:
                    np.copyto(bk[:cnt], key, where=upd)
                np.copyto(bv[:cnt], v, where=upd)
                np.copyto(bt[:cnt], T[s], where=upd)
                np.copyto(be[:cnt], u_eid[a:b], where=upd)
            has = np.arange(m) < cnt0
            return bv, bt, be, has
        bk = np.zeros(m)
        has = np.zeros(m, dtype=bool)
        for cnt, a, b in cols:
            s = u_src[a:b]
            v, t = V[s], T[s]
            key = v + bonus[a:b] if bonus is not None else v
            upd = (~has[:cnt] | (key > bk[:cnt])) & allowed[a:b]
            np.copyto(bk[:cnt], key, where=upd)
            np.copyto(bv[:cnt], v, where=upd)
            np.copyto(bt[:cnt], t, where=upd)
            np.copyto(be[:cnt], u_eid[a:b], where=upd)
            has[:cnt] |= upd
        return bv, bt, be, has

    def values(self, chosen: np.ndarray) -> np.ndarray:
        """policy_value's bottom-up pass for a fixed edge per our node (-1: stop)."""
        T = self.ext0.copy()
        o_src, o_p, c_src = self.o_src, self.o_p, self.c_src
        for L in range(self.nlev):
            nodes = self.o_nodes[L]
            if nodes.size:
                acc = self.const_base[nodes]
                for cnt, a, b in self.o_cols[L]:
                    acc[:cnt] += o_p[a:b] * T[o_src[a:b]]
                T[nodes] = acc
            nodes = self.u_nodes[L]
            if nodes.size:
                e = chosen[nodes]
                on = e >= 0
                t = self.l_node[nodes]
                t[on] = T[c_src[e[on]]]
                T[nodes] = t
        return T

    def reach(self, reach_edge: np.ndarray):
        """The dict walks' top-down reach: R[root] = 1, a visited node (R > 0) passes R to
        its reach edge's child (our) or R*p to each reply (opponent). reach_edge = the
        edge an our node's move follows, -1 none. Returns (R at visit time, final R,
        the visited nodes in visit order)."""
        w = self.w
        on = np.flatnonzero(reach_edge >= 0)
        re = reach_edge[on]
        w[re] = 1.0
        try:
            R = np.zeros(self.N)
            R[self.root] = 1.0
            t_parent, t_eid = self.t_parent, self.t_eid
            for L in range(self.nlev - 1, -1, -1):
                nodes = self.t_nodes[L]
                if not nodes.size:
                    continue
                acc = R[nodes]
                for cnt, a, b in self.t_cols[L]:
                    acc[:cnt] += R[t_parent[a:b]] * w[t_eid[a:b]]
                R[nodes] = acc
            Rf = R
            if self.back:
                Rf = R.copy()
                act = set(re.tolist())
                Ec = self.Ec
                for p, c, e in self.back:
                    rp = Rf[p]
                    if rp > 0 and (e >= Ec or e in act):
                        R[c] = R[c] + rp * w[e]
        finally:
            w[re] = 0.0
        vis = np.flatnonzero(Rf > 0)[::-1]
        return Rf, R, vis

    def reach_keys(self, vis: np.ndarray, reach_edge: np.ndarray) -> np.ndarray:
        """The dict R's keys in insertion order: the root, then every child an edge from a
        visited node feeds, in the order first fed."""
        is_o = self.is_our[vis]
        re = reach_edge[vis]
        starts = np.where(is_o, np.maximum(re, 0), self.Ec + self.k_ptr[vis])
        lens = np.where(is_o, (re >= 0).astype(np.int64), self.k_ptr[vis + 1] - self.k_ptr[vis])
        ch = self.all_child[_expand(starts, lens)]
        ch = ch[ch >= 0]
        _u, first = np.unique(ch, return_index=True)
        seq = ch[np.sort(first)]
        return np.concatenate(([self.root], seq[seq != self.root]))

    def hashes(self, idx) -> list:
        order = self.order
        return [order[i] for i in idx.tolist()]

    def played(self, chosen: np.ndarray) -> int:
        """policy_value's 'played': our nodes with a book move that the walk from the root
        reaches (following every in-subgraph edge it plays, cut back edges included)."""
        seen = np.zeros(self.N, dtype=bool)
        cur = np.array([self.root], dtype=np.int64)
        seen[self.root] = True
        n = 0
        while cur.size:
            is_o = self.is_our[cur]
            e = chosen[cur]
            n += int((is_o & (e >= 0)).sum())
            starts = np.where(is_o, np.maximum(e, 0), self.Ec + self.k_ptr[cur])
            lens = np.where(is_o, (e >= 0).astype(np.int64), self.k_ptr[cur + 1] - self.k_ptr[cur])
            ch = self.all_child[_expand(starts, lens)]
            ch = np.unique(ch[ch >= 0])
            ch = ch[~seen[ch]]
            seen[ch] = True
            cur = ch
        return n


@contextlib.contextmanager
def gc_paused():
    """The cyclic garbage collector off for the block (restored after, also on error).
    A full collection rescans every container in the process, and the graph is
    millions of them (~3 s a pass at 1M nodes); nothing here builds cycles to collect."""
    was = gc.isenabled()
    gc.disable()
    try:
        yield
    finally:
        if was:
            gc.enable()


def compile_graph(g, order=None, cut=None) -> CompiledGraph:
    """Compile g once (see CompiledGraph); order/cut = topo_up(g) when given."""
    with gc_paused():
        return CompiledGraph(g, order, cut)


# ── the dict functions' results, from the arrays ────────────────────────────

def price_book(cg: CompiledGraph, lam: float, reach, fixed_policy) -> dict:
    """budget_core.price_book's result dict, bit for bit."""
    allowed = cg.allowed(fixed_policy)
    V, T, choice = cg.solve(lam, cg.reach_array(reach), allowed)
    reach_edge = np.full(cg.N, -1, dtype=np.int64)
    on = choice >= 0
    reach_edge[on] = cg.c_first[choice[on]]
    Rf, R, vis = cg.reach(reach_edge)
    order = cg.order
    vis_h = cg.hashes(vis)
    bk = vis[cg.is_our[vis] & (choice[vis] >= 0)]
    c_san = cg.c_san
    booked = dict(zip(cg.hashes(bk), [c_san[e] for e in choice[bk].tolist()]))
    keys = cg.reach_keys(vis, reach_edge)
    root = cg.root
    return {"booked": booked, "alloc": {}, "visited": vis_h,
            "realized": dict(zip(vis_h, T[vis].tolist())),
            "spent_paths": len(booked), "spent_distinct": len(booked),
            "root_value_curve": None, "root_value_realized": float(T[root]),
            "root_value_penalized": float(V[root]), "lam": lam,
            "reach_policy": dict(zip([order[i] for i in keys.tolist()], R[keys].tolist()))}


def policy_reach(cg: CompiledGraph, moves) -> dict:
    """budget_core.policy_reach's dict, bit for bit."""
    fm = cg.first_match(moves)
    ok = fm >= 0
    reach_edge = np.full(cg.N, -1, dtype=np.int64)
    reach_edge[ok] = np.where(cg.c_child[fm[ok]] >= 0, fm[ok], -1)
    _Rf, R, vis = cg.reach(reach_edge)
    keys = cg.reach_keys(vis, reach_edge)
    return dict(zip(cg.hashes(keys), R[keys].tolist()))


def policy_value(cg: CompiledGraph, moves, detail: bool = False) -> dict:
    """budget_core.policy_value's dict, bit for bit."""
    fm = cg.first_match(moves)
    missing = 0
    pget, is_our = cg.pos.get, cg.is_our
    for h, want in moves.items():
        i = pget(h)
        if want is not None and i is not None and is_our[i] and fm[i] < 0:
            missing += 1
    T = cg.values(fm)
    out = {"value": float(T[cg.root]), "played": cg.played(fm), "missing": missing}
    if detail:
        out["T"] = dict(zip(cg.order, T[:cg.N].tolist()))
        out["W"] = policy_reach(cg, moves)
    return out




# ── the book region (polish_book's evaluator), on indices ────────────────────
#
# polish_book verifies up to max_verify proposals per step, each a full region evaluation
# (budget_core._book_region) although a proposal changes a few moves. region() is that
# evaluator on node indices with the same traversal, sums and dict insertion orders; it
# also returns, keyed by index, the region's reach, values and forward parents. quick()
# uses them to price a proposal exactly by re-evaluating only what the change can touch.

def _same(a, b) -> bool:
    """Bitwise equality of two values (same type; 0.0 and -0.0 differ; NaN never equals)."""
    return type(a) is type(b) and a == b and (a != 0.0 or str(a) == str(b))


class RegionAux:
    """Index-keyed view of a region: Wi (reach, the region's membership), Ti (values),
    parents (node -> region nodes that push reach into it over a forward edge, in pop
    order, once per edge), booked_i (the booked nodes, in pop order)."""
    __slots__ = ("Wi", "Ti", "parents", "booked_i", "tchg", "bchg")

    def __init__(self, Wi, Ti, parents, booked_i=None):
        self.Wi, self.Ti, self.parents, self.booked_i = Wi, Ti, parents, booked_i
        # set by advance(): positions whose value or membership (tchg), or booked status
        # (bchg), differ from the region it was advanced from
        self.tchg = self.bchg = None


def region(cg: CompiledGraph, moves, fixed_policy):
    """budget_core._book_region of `moves`, on indices. Returns (W, T, booked, child,
    value, aux): the first five exactly _book_region's (hash-keyed, same insertion
    orders, same floats), aux a RegionAux."""
    order, flag, adj = cg.order, cg.our_flag, cg.adj
    our_d, opp_d = cg.g.our, cg.g.opp
    root = cg.root
    Wi: dict[int, float] = {root: 1.0}
    heap = [-root]
    booked: dict = {}
    child: dict = {}
    move_i: dict[int, tuple] = {}
    parents: dict[int, list[int]] = {}
    fp = fixed_policy
    pop, push = heapq.heappop, heapq.heappush
    while heap:                        # parents first: descending order position
        i = -pop(heap)
        r = Wi[i]
        if flag[i]:
            h = order[i]
            want = moves.get(h)
            if want is None or (fp is not None and fp.get(h) != want):
                continue
            mv = adj(i).get(want)
            if mv is None:
                continue
            booked[h] = want
            move_i[i] = mv
            pc = mv[0]
            if pc < 0:
                child[h] = None
                continue
            child[h] = order[pc]
            if pc > i:                     # a cut back edge carries no reach
                continue
            new = pc not in Wi
            Wi[pc] = Wi.get(pc, 0.0) + r
            if pc < i:
                if new:
                    parents[pc] = [i]
                    push(heap, -pc)
                else:
                    parents[pc].append(i)
        else:
            for p, pc, _lv in adj(i):
                if pc < 0 or pc > i:
                    continue
                new = pc not in Wi
                Wi[pc] = Wi.get(pc, 0.0) + r * p
                if pc < i:
                    if new:
                        parents[pc] = [i]
                        push(heap, -pc)
                    else:
                        parents[pc].append(i)
    Ti: dict[int, float] = {}
    static = cg.static
    for i in sorted(Wi):                # children first
        if flag[i]:
            mv = move_i.get(i)
            if mv is None:
                t = our_d[order[i]].l_node
            elif mv[0] < 0:
                t = mv[1]
            else:
                ci = mv[0]
                t = Ti[ci] if ci in Ti else static(ci)
        else:
            t = opp_d[order[i]].const_base
            for p, pc, lv in adj(i):
                if pc >= 0:
                    t += p * (Ti[pc] if pc in Ti else static(pc))
                else:
                    t += p * lv
        Ti[i] = t
    W = dict(zip([order[i] for i in Wi], Wi.values()))
    T = dict(zip([order[i] for i in Ti], Ti.values()))
    return W, T, booked, child, Ti[root], RegionAux(Wi, Ti, parents, list(move_i))


def region_diff(a, b) -> list[str]:
    """The parts in which two budget_core._Region differ (dicts compared in order)."""
    out = [k for k in ("W", "T", "booked", "child")
           if list(getattr(a, k).items()) != list(getattr(b, k).items())]
    if a.aux.parents != b.aux.parents:
        out.append("parents")
    if a.aux.booked_i != b.aux.booked_i:
        out.append("booked_i")
    return out


def _move(cg: CompiledGraph, i: int, moves, fixed_policy):
    """(booked?, child index or -1, leaf value) of our node i under `moves`: the first
    candidate carrying the move, as _book_region reads it."""
    h = cg.order[i]
    want = moves.get(h)
    if want is None or (fixed_policy is not None and fixed_policy.get(h) != want):
        return False, -1, None
    mv = cg.adj(i).get(want)
    if mv is None:
        return False, -1, None
    return (True, mv[0], None) if mv[0] >= 0 else (True, -1, mv[1])


class _Delta:
    """What changes between a region and the region of a book differing at a few moves
    (see quick): A = every node whose membership could change; newr[x] its membership;
    new_par[x] its parents in A over the new book's edges; mv[x] an our node's move;
    Tn the recomputed values; size the new booked count."""
    __slots__ = ("A", "newr", "new_par", "mv", "Tn", "size")


def _delta(cg: CompiledGraph, aux: RegionAux, booked: dict, size: int, moves, changed,
           fixed_policy) -> _Delta | None:
    """quick's work (None: no reached move changes)."""
    order, pos, flag, adj = cg.order, cg.pos, cg.our_flag, cg.adj
    our_d, opp_d = cg.g.our, cg.g.opp
    Wi, Ti, parents = aux.Wi, aux.Ti, aux.parents
    D = []
    for h in dict.fromkeys(changed):            # a position can be both added and dropped
        i = pos.get(h)
        if i is None or not flag[i] or i not in Wi:
            continue
        nb, nc, nlv = _move(cg, i, moves, fixed_policy)
        ob, oc, olv = _move(cg, i, booked, fixed_policy)
        if nb != ob or (nb and moves.get(h) != booked.get(h)):
            D.append(i)
    if not D:
        return None
    # every node whose membership could change: below a changed move, by old or new edges
    A = set(D)
    stack = list(D)
    new_par: dict[int, list[int]] = {}
    mv: dict[int, tuple] = {}
    while stack:
        x = stack.pop()
        kids = []
        if flag[x]:
            nb, nc, nlv = mv[x] = _move(cg, x, moves, fixed_policy)
            if nb and 0 <= nc < x:
                new_par.setdefault(nc, []).append(x)
                kids.append(nc)
            if x in Wi:
                ob, oc, _olv = _move(cg, x, booked, fixed_policy)
                if ob and 0 <= oc < x and oc != nc:
                    kids.append(oc)
        else:
            for _p, c, _lv in adj(x):
                if 0 <= c < x:
                    new_par.setdefault(c, []).append(x)
                    kids.append(c)
        for c in kids:
            if c not in A:
                A.add(c)
                stack.append(c)
    newr: dict[int, bool] = {}
    root = cg.root
    for x in sorted(A, reverse=True):          # parents first
        ok = x == root
        if not ok:
            for p in parents.get(x, ()):
                if p not in A:
                    ok = True
                    break
        if not ok:
            for p in new_par.get(x, ()):
                if newr[p]:
                    ok = True
                    break
        newr[x] = ok
    for x in A:
        if flag[x]:
            size += (newr[x] and mv[x][0]) - (order[x] in booked)

    def member(c):
        r = newr.get(c)
        return (c in Wi) if r is None else r

    static = cg.static
    Tn: dict[int, float] = {}
    heap = sorted(x for x in A if newr[x])
    done: set[int] = set()
    pop, push = heapq.heappop, heapq.heappush
    while heap:                                # children first
        x = pop(heap)
        if x in done:
            continue
        done.add(x)
        h = order[x]
        if flag[x]:
            nb, nc, nlv = mv[x] if x in mv else _move(cg, x, moves, fixed_policy)
            if not nb:
                t = our_d[h].l_node
            elif nc < 0:
                t = nlv
            else:
                t = (Tn[nc] if nc in Tn else Ti[nc]) if (nc < x and member(nc)) else static(nc)
        else:
            t = opp_d[h].const_base
            for p, c, lv in adj(x):
                if c >= 0:
                    t += p * ((Tn[c] if c in Tn else Ti[c]) if (c < x and member(c))
                              else static(c))
                else:
                    t += p * lv
        Tn[x] = t
        if x in Wi and _same(t, Ti[x]):
            continue
        if x in A:
            ups = [p for p in parents.get(x, ()) if p not in A]
            ups += [p for p in new_par.get(x, ()) if newr[p]]
        else:
            ups = parents.get(x, ())
        for p in ups:
            if p not in done:
                push(heap, p)
    d = _Delta()
    d.A, d.newr, d.new_par, d.mv, d.Tn, d.size = A, newr, new_par, mv, Tn, size
    return d


def quick(cg: CompiledGraph, aux: RegionAux, booked: dict, size: int, value: float,
          moves, changed, fixed_policy) -> tuple[int, float]:
    """(size, value) that region(cg, moves) would return, where `moves` differs from the
    region's book `booked` only at the positions in `changed`. Exact: it re-derives the
    membership of every node below a changed move (descending order position, from the
    node's region parents) and re-values, children first with the same expressions, every
    node whose inputs changed, stopping where a recomputed value is bitwise unchanged.
    Every other node keeps the region's floats, which a full evaluation would reproduce
    because its inputs are identical."""
    d = _delta(cg, aux, booked, size, moves, changed, fixed_policy)
    if d is None:
        return size, value
    return d.size, (d.Tn[cg.root] if cg.root in d.Tn else aux.Ti[cg.root])


def advance(cg: CompiledGraph, W: dict, T: dict, booked: dict, child: dict, value: float,
            aux: RegionAux, moves, changed, fixed_policy):
    """region(cg, moves, fixed_policy), from the region (W, T, booked, child, value, aux)
    of a book that `moves` differs from only at `changed`, by quick's delta: reach is
    re-summed (same parents, same order) for the nodes below a changed move only, values
    come from the delta, and the dicts are rebuilt in region's insertion orders (W by
    discovery: the first parent popped, then its edge order; T ascending; booked and
    child by pop order). Returns the same tuple as region()."""
    d = _delta(cg, aux, booked, len(booked), moves, changed, fixed_policy)
    if d is None:
        same = RegionAux(aux.Wi, aux.Ti, aux.parents, aux.booked_i)
        same.tchg, same.bchg = [], []
        return W, T, booked, child, value, same
    order, flag, adj, root = cg.order, cg.our_flag, cg.adj, cg.root
    A, newr, new_par, mv, Tn = d.A, d.newr, d.new_par, d.mv, d.Tn
    Wi, Ti, parents = aux.Wi, aux.Ti, aux.parents
    members = sorted((x for x in A if newr[x]), reverse=True)      # parents first
    np_ = dict(parents)
    for x in A:
        if newr[x] and x != root:
            pl = [p for p in parents.get(x, ()) if p not in A]
            pl += [p for p in new_par.get(x, ()) if newr[p]]
            pl.sort(reverse=True)                  # pop order; a parent's edges stay adjacent
            np_[x] = pl
        else:
            np_.pop(x, None)
    Wn: dict[int, float] = {}
    for x in members:
        if x == root:
            Wn[x] = 1.0
            continue
        w = 0.0
        last = -1
        for p in np_[x]:
            if p == last:
                continue
            last = p
            r = Wn[p] if p in Wn else Wi[p]
            if flag[p]:
                w = w + r
            else:
                for prob, pc, _lv in adj(p):
                    if pc == x:
                        w = w + r * prob
        Wn[x] = w

    big = cg.N + 1

    def disc(c: int) -> tuple[int, int]:
        """discovery key: the first parent popped (the highest), then its edge order"""
        if c == root:
            return (-big, -1)
        p0 = np_[c][0]
        if flag[p0]:
            return (-p0, 0)
        for e, (_prob, pc, _lv) in enumerate(adj(p0)):
            if pc == c:
                return (-p0, e)
        raise AssertionError("advance: a parent without an edge to its child")

    seq = [c for c in Wi if c not in A]
    for c in sorted(members, key=disc):
        bisect.insort(seq, c, key=disc)
    Wi2 = {c: (Wn[c] if c in Wn else Wi[c]) for c in seq}
    Ti2 = {x: (Tn[x] if x in Tn else Ti[x]) for x in sorted(Wi2)}
    bk = [x for x in aux.booked_i if x not in A]
    bk += [x for x in members if flag[x] and mv[x][0]]
    bk.sort(reverse=True)
    booked2: dict = {}
    child2: dict = {}
    for x in bk:
        h = order[x]
        if x in A:
            booked2[h] = moves[h]
            nc = mv[x][1]
            child2[h] = order[nc] if nc >= 0 else None
        else:
            booked2[h] = booked[h]
            child2[h] = child[h]
    aux2 = RegionAux(Wi2, Ti2, np_)
    aux2.booked_i = bk
    aux2.tchg = [order[x] for x in A if newr[x] != (x in Wi)]
    aux2.tchg += [order[x] for x, t in Tn.items() if x in Wi and not _same(t, Ti[x])]
    aux2.bchg = [order[x] for x in A if flag[x]
                 and (order[x] in booked) != bool(newr[x] and mv[x][0])]
    W2 = dict(zip([order[c] for c in Wi2], Wi2.values()))
    T2 = dict(zip([order[x] for x in Ti2], Ti2.values()))
    return W2, T2, booked2, child2, Ti2[root], aux2
