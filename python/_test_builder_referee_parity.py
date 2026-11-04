"""Does the builder value a fixed book the way the referee does, under the sampled-stop contract?

A real-chess world small enough to walk exactly, with a reference recursion that plays the
simulation the owner specified: V_B(s, t) = the game's result if it is over, the engine through
the uniform curve at t = H, the book's move at our turn in book, else the shares of the weak
table (our turn) or the opponent's table.

  P  the reference recursion equals the referee's exact walk (convertibility_eval.enumerate_book)
     and the mean of its rollouts, so the reference really is the referee.
  T  on a tree, with stop values V_0 (no book from there on, at the position's ply), the builder's
     policy value of a book equals the referee's score exactly.
  A  with a position reached at two plies (1.e3 a5 2.Bb5, and 1.e3 a6 2.Be2 a5 3.Bb5), the gap is
     exactly option A's stated approximation: late arrivals get the highest-reach route's value.
     The identity holds whichever route is the likely one, and the highest-reach ply gives a
     smaller error than the earliest ply where the long route carries the mass.
  R  an out-of-book move that re-enters the book: the referee resumes the book, the absorbing stop
     value does not, and the gap is exactly that difference.
  C  book choice: with two candidates at the root both optimisers pick the move the referee's
     own scores of the two complete books prefer.
  E  a finished game inside the book: both sides score its result.

The move tables here are plain shares (k = 0), so the builder's opponent transitions equal the
referee's. On real data they differ: the referee blends Maia as a prior and reads holdout tables;
the builder's reply shares come from the pool. That remainder is what the paired book trial
measures; it is not a claim of this test. Nor are cycles (the builder cuts them; the referee walks
position by position) or evals missing from the arrays (the referee asks Stockfish, the builder
keeps its prediction and counts it).

Run: .venv/Scripts/python.exe python/_test_builder_referee_parity.py
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
import convertibility_eval as cv  # noqa: E402
import sampled_stop as ss  # noqa: E402
from stage3_backwards_induction import LICHESS_CP_SCALE  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def hash_after(*sans) -> int:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return zobrist_int64(b)


def lookup(hs):
    return np.array([(int(h) % 401) - 200 for h in hs], dtype=np.int64)


def default_moves(b: chess.Board) -> dict:
    """Off the fixture's lines: the first two legal moves by UCI, 60/40."""
    ms = sorted(b.legal_moves, key=lambda m: m.uci())[:2]
    return {b.san(m): s for m, s in zip(ms, (0.6, 0.4))}


class World:
    """Fixed move shares by position (SAN line -> shares), the same to referee and builder."""

    def __init__(self, table: dict, our_white: bool, H: int):
        self.table = {hash_after(*line): d for line, d in table.items()}
        self.our_white, self.H = our_white, H

    def shares(self, b: chess.Board) -> dict:
        return self.table.get(zobrist_int64(b)) or default_moves(b)

    def value(self, b: chess.Board, t: int, book: dict) -> float:
        """The reference: V_B(position, ply), our view."""
        if b.is_game_over(claim_draw=False):
            r = b.result(claim_draw=False)
            return 0.5 if r == "1/2-1/2" else (1.0 if (r == "1-0") == self.our_white else 0.0)
        if t >= self.H:
            cp = float(lookup([zobrist_int64(b)])[0])
            cp = cp if self.our_white else -cp
            return 1 / (1 + np.exp(-LICHESS_CP_SCALE * np.clip(cp, -1000, 1000)))
        us = (b.turn == chess.WHITE) == self.our_white
        h = zobrist_int64(b)
        if us and h in book:
            items = {book[h]: 1.0}
        else:
            items = self.shares(b)
        v = 0.0
        for san, p in items.items():
            b.push_san(san)
            v += p * self.value(b, t + 1, book)
            b.pop()
        return v


class Tables:
    """Counts and Maia for the referee: the world's shares as counts, k = 0."""

    def __init__(self, w: World):
        self.w = w
        self.by_epd: dict = {}

    def probs(self, epds, se, oe, keys):
        out = []
        for e, k in zip(epds, keys):
            d = self.w.shares(chess.Board(e + " 0 1"))
            self.by_epd[k] = d
            out.append(d)
        return out

    def get(self, hashes, rep=0):
        return [{} for _ in hashes]          # with k > 0 the "prior" is the shares themselves


def referee(w: World, book: dict, tau=0.0, seed=None, K=2) -> float:
    t = Tables(w)
    a = SimpleNamespace(tau=tau, rollouts=K, max_ply=w.H, k=1.0, min_p=0.0, our_elo=1, opp_elo=2)
    ex, starts, _, _ = cv.enumerate_book(book, w.our_white, 0, a, t, t, t, lookup)
    leaves = list(ex)
    if starts:
        leaves += cv.rollouts(starts, book, w.our_white, 0, a, seed or 1, t, t, t, lookup)[0]
    v = cv.value_leaves(leaves, w.our_white, lookup, None, ((0.0, 0.004), (0.0, 0.004)))
    return float((v["ps"] * v["uniform"]).sum() / v["ps"].sum())


def graph(w: World, book_lines: dict, our_white: bool, extra_cands=None):
    """The builder's graph of the book's lines: every position on them, with the world's shares
    as edges at opponent nodes and the book's move (plus extra_cands) at ours; stop values are
    V_0 at the route ply, R at ply + 1 for candidates leaving the graph."""
    edges: dict = {}
    frontier = [chess.Board()]
    seen = set()
    extra_cands = extra_cands or {}
    while frontier:
        b = frontier.pop()
        h = zobrist_int64(b)
        if h in seen or b.is_game_over(claim_draw=False):
            continue
        seen.add(h)
        us = (b.turn == chess.WHITE) == our_white
        if us:
            sans = ([book_lines[h]] if h in book_lines else []) + extra_cands.get(h, [])
            if not sans:
                continue                                # out of book: a stop position
            shares = {s: 1.0 for s in sans}
        else:
            shares = w.shares(b)
        rows = []
        for san, p in shares.items():
            b.push_san(san)
            rows.append({"move_san": san, "child_hash": zobrist_int64(b), "total": int(round(p * 1000)),
                         "white_score_avg": 0.5})
            frontier.append(b.copy(stack=False))
            b.pop()
        rows[0]["parent_epd"] = b.epd()
        edges[h] = rows
    g = bc.build_graph(edges, zobrist_int64(chess.Board()), our_white, gates=False, prior_strength=1.0,
                       eval_weight=0.0, eps=0.0, share_floor=0.0, max_ply=w.H - 1, track_route_ply=True,
                       require_eval=False)
    return g, edges


def boards_of(edges: dict) -> dict:
    """Every position on the edges -> its board."""
    out = {zobrist_int64(chess.Board()): chess.Board()}
    frontier = [chess.Board()]
    while frontier:
        b = frontier.pop()
        for e in edges.get(zobrist_int64(b), []):
            c = b.copy(stack=False)
            c.push_san(e["move_san"])
            h = zobrist_int64(c)
            if h not in out:
                out[h] = c
                frontier.append(c)
    return out


def stop_values(g, w: World, edges: dict, use_ply=None):
    """V_0 (no book) at each stop position's route ply, or at use_ply(h) if given."""
    bd = boards_of(edges)
    W, R = {}, {}
    for side, h, ply in ss.stop_positions(g).select("side", "hash", "ply").iter_rows():
        if h not in bd:
            continue
        v = w.value(bd[h].copy(), use_ply(h, ply) if use_ply else ply, {})
        (W if side == "x" else R)[h] = v
    bc.apply_stop_values(g, W, R)
    return W, R


def builder_value(g, book_moves: dict) -> float:
    order = bc.topo_up(g)[0]
    return bc.policy_value(g, book_moves, order)["value"]


S = zobrist_int64(chess.Board())
print("P  the reference is the referee")
tree = {(): {"e3": 1.0}, ("e3",): {"a5": 0.3, "a6": 0.7}}
w = World(tree, True, 6)
book = {S: "e3", hash_after("e3", "a5"): "Bb5", hash_after("e3", "a6"): "Be2"}
ref = w.value(chess.Board(), 0, book)
check("the exact walk equals the recursion", abs(referee(w, book) - ref) < 1e-12,
      f"{referee(w, book):.10f} vs {ref:.10f}")
runs = [referee(w, book, tau=1.01, seed=s) for s in range(1, 301)]
se = float(np.std(runs, ddof=1) / np.sqrt(len(runs)))
check("the mean of sampled walks is within 4 SE of it", abs(np.mean(runs) - ref) < 4 * se,
      f"{np.mean(runs):.5f} vs {ref:.5f} (se {se:.5f})")

print("T  a tree: exact parity")
for our_white, wtree, bk, c in (
        (True, tree, book, "White"),
        (False, {(): {"e4": 0.5, "d4": 0.5}},
         {hash_after("e4"): "e5", hash_after("d4"): "d5"}, "Black")):
    w = World(wtree, our_white, 6)
    g, edges = graph(w, bk, our_white)
    stop_values(g, w, edges)
    bv, rv = builder_value(g, bk), referee(w, bk)
    check(f"{c}: builder = referee", abs(bv - rv) < 1e-9, f"{bv:.10f} vs {rv:.10f}")

print("A  one position at two plies")
# Y = e3, Bb5, a5 (Black to move): at ply 3 by a5 at once, at ply 5 by a6 then a5
L_SHORT = ("e3", "a5")
L_LONG = ("e3", "a6")
L_MID = ("e3", "a6", "Be2")
BOOK2 = {S: "e3", hash_after(*L_SHORT): "Bb5", hash_after(*L_LONG): "Be2",
         hash_after(*L_LONG, "Be2", "a5"): "Bb5"}
Y = hash_after(*L_SHORT, "Bb5")
for p_short, want_ply, label in ((0.1, 5, "long route likely"), (0.9, 3, "short route likely")):
    wt = {(): {"e3": 1.0}, ("e3",): {"a5": p_short, "a6": 1 - p_short}, L_MID: {"a5": 1.0}}
    w = World(wt, True, 7)
    g, edges = graph(w, BOOK2, True)
    check(f"{label}: Y's route ply is {want_ply}", g.route_ply[Y] == want_ply, str(g.route_ply[Y]))
    W, R = stop_values(g, w, edges)
    bv, rv = builder_value(g, BOOK2), referee(w, BOOK2)
    # the stated approximation: every exit under Y is valued at the route's ply; the rare route's
    # games really arrive at the other ply
    other = 3 if want_ply == 5 else 5
    b = chess.Board()
    for m in (*L_SHORT, "Bb5"):
        b.push_san(m)
    late_mass = min(p_short, 1 - p_short)
    gap = late_mass * (w.value(b.copy(), want_ply, {}) - w.value(b.copy(), other, {}))
    check(f"{label}: builder - referee is exactly the rare route's ply error",
          abs((bv - rv) - gap) < 1e-9, f"{bv - rv:+.6f} vs {gap:+.6f}")
    # earliest-ply on the same world
    g2, edges2 = graph(w, BOOK2, True)
    stop_values(g2, w, edges2, use_ply=lambda h, ply: ply - 2 if ply >= 6 else ply)
    if want_ply == 5:
        err_route, err_early = abs(bv - rv), abs(builder_value(g2, BOOK2) - rv)
        check("the highest-reach ply errs less than the earliest ply here", err_route < err_early,
              f"{err_route:.6f} vs {err_early:.6f}")

print("R  re-entering the book")
# after 1.e3 a6 our weak side plays a move that the opponent's reply brings back to a booked
# position: the book resumes in the referee, not in an absorbing stop value
L = ("e3", "a6")
wt = {(): {"e3": 1.0}, ("e3",): {"a6": 1.0}, L: {"Be2": 0.5, "Nf3": 0.5},
      (*L, "Be2"): {"a5": 1.0}, (*L, "Nf3"): {"a5": 1.0}}
w = World(wt, True, 7)
BK = {S: "e3", hash_after(*L, "Be2", "a5"): "Bb5"}
g, edges = graph(w, BK, True)
# ...the booked Be2-a5 position is reached only through the weak move: outside the builder's graph
b = chess.Board()
for m in L:
    b.push_san(m)
rv = referee(w, BK)
absorbing = w.value(b.copy(), 2, {})
with_book = w.value(b.copy(), 2, BK)
check("the referee resumes the book", abs(rv - with_book) < 1e-12, f"{rv:.8f} vs {with_book:.8f}")
stop_values(g, w, edges)
bv = builder_value(g, BK)
check("the builder's stop value is the absorbing one", abs(bv - absorbing) < 1e-9, f"{bv:.8f} vs {absorbing:.8f}")
check("the gap is exactly re-entry's worth", abs((bv - rv) - (absorbing - with_book)) < 1e-9
      and abs(absorbing - with_book) > 1e-6, f"{bv - rv:+.6f}")

print("C  book choice at the root")
for white_cands, c in ((["e3", "d3"], "e3 or d3"), (["a3", "h3"], "a3 or h3")):
    w = World({(): {white_cands[0]: 1.0}}, True, 6)
    scores = {m: referee(w, {S: m}) for m in white_cands}
    want = max(scores, key=scores.get)
    g, edges = graph(w, {S: white_cands[0]}, True, extra_cands={S: white_cands[1:]})
    stop_values(g, w, edges)
    for n in g.our.values():
        n.l_node = 0.0                       # stopping at the root is not on offer here
    curves, _ = bc.build_curves(g, bmax=2)
    dp = bc.extract_book(g, curves, 1)["booked"].get(S)
    pr = bc.price_search(g, 1, log=lambda m: None)[0]["booked"].get(S)
    check(f"{c}: both optimisers pick the referee's better book",
          dp == want and pr == want, f"want {want} {scores}, dp {dp}, price {pr}")

print("E  a finished game inside the book")
L = ("f3", "e5", "g4")
wt = {(): {"f3": 1.0}, ("f3",): {"e5": 1.0}, ("f3", "e5"): {"g4": 1.0}}
w = World(wt, False, 8)
BK = {hash_after("f3"): "e5", hash_after(*L): "Qh4#"}
check("the referee scores the mate 1.0", abs(referee(w, BK) - 1.0) < 1e-12)
g, edges = graph(w, BK, False)
term = ss.terminal_outcomes(edges, False)
check("the builder's boundary pass finds it", term.get(hash_after(*L, "Qh4#")) == 1.0, str(term))
W, R, _ = ss.apply({}, {}, ss.stop_positions(g), pl.DataFrame(
    schema={"side": str, "hash": int, "ply": int, "reach": float, "stop": float, "v0": float, "se": float}),
    horizon=8, terminal=term)
bc.apply_stop_values(g, W, R)
check("and its value is the result", abs(builder_value(g, BK) - 1.0) < 1e-12, f"{builder_value(g, BK):.8f}")

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
