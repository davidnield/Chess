"""The lookahead floor (build_budget_books --lookahead-floor N, default 0 = off, an exact no-op).

The absolute gate (budget_core.passes_abs_gate) checks one move: the eval right after our move
must stay within --robustness-floor of the slice prior. It does not look at what we can still
play one reply later. A booked move can pass at -85 cp and, after a common reply, leave only
moves below the bar: the book then plays the least bad one, and the line sits under the floor.

With --lookahead-floor N a move is vetoed when it walks into that. Definitions, on the collected
subgraph (our moves: every recorded move and engine proposal that is not already excluded):

  passes(m)     m has an eval and passes the absolute gate
  dead_1(g)     our node g has an evaluated move and none passes
  bad_k(m)      m's child has a reply the collector follows (share >= --share-floor) into a
                dead_k node
  dead_k+1(g)   g has an evaluated move and every passing move is bad_k

The veto is bad_N. N = 1 looks one reply ahead. bad_k only grows with k and the walk stops when
it no longer does, so a large N is the fixed point: a move is kept only if, against every
followed reply, we can go on playing moves that pass the gate for as far as the graph reaches
(positions the collector did not expand count as alive). That is the backward pruning of every
decision that cannot avoid the floor.

The veto is void at a node where it would leave no passing move: there the node keeps its menu
(nothing better exists to play), which is what the gates already do when every move fails. At the
fixed point that only happens where the opponent can force the node from a position we cannot
avoid.

The source book's move is exempt from the absolute gate itself (gate_candidates admits it
whatever its eval). With the flag on, a source move that has an eval and fails the gate is also
vetoed wherever the node keeps a passing move (source_moves), so a booked move passes the gate
at every node that has one.

The vetoes join the verified exclusions (final_candidates' `excluded`), so they are removed
before every gate and cannot set a gate reference.

The collector does not know the vetoes, so its frame is a superset of the vetoed graph. The
graph's own reach walk admits only what a node can finally book, so positions behind a vetoed
move get no reach there.

Two implementations: `vetoes` on the collector's frame (what the builder runs) and `vetoes_ref`
on build_graph's dicts (the tests' reference). _test_lookahead_floor.py holds them equal.
"""
from __future__ import annotations

import math

import numpy as np

import budget_core as bc


def _bare(san: str) -> str:
    return san.rstrip("+#")


def _stats(depth, dead_first, by_depth, out, void, src_vetoes):
    return {"depth": depth, "iterations": len(by_depth), "dead_nodes": dead_first,
            # bad moves after each step: by_depth[0] is what --lookahead-floor 1 finds
            "bad_moves_by_depth": by_depth, "bad_moves": by_depth[-1] if by_depth else 0,
            # the walk stopped by itself before N steps: nothing more to veto at any depth
            "fixed_point": len(by_depth) < depth,
            "vetoed_moves": sum(map(len, out.values())), "vetoed_nodes": len(out),
            "void_nodes": void, "source_moves_vetoed": src_vetoes}


def vetoes_ref(edges_by_parent: dict[int, list[dict]], eval_ws: dict[int, float], our_white: bool,
               slice_prior: float, *, depth: int, robustness_floor: float = bc.ROBUSTNESS_FLOOR,
               share_floor: float = 0.002, excluded: dict | None = None,
               engine_moves: dict | None = None, source_moves: dict | None = None
               ) -> tuple[dict[int, set[str]], dict]:
    """{our node: bare SANs vetoed}, stats. Edges carry parent_epd (the side to move), as the
    collector's rows do."""
    excluded, engine_moves, source_moves = excluded or {}, engine_moves or {}, source_moves or {}
    side = "w" if our_white else "b"
    menu, replies = {}, {}
    for h, es in edges_by_parent.items():
        if not es:
            continue
        if es[0]["parent_epd"].split()[1] == side:
            veto = excluded.get(h, ())
            rows = [(_bare(e["move_san"]), e.get("child_hash")) for e in es]
            have = {b for b, _ in rows}
            for s, ch in engine_moves.get(h, ()):      # a move proposed twice counts once
                if _bare(s) not in have:
                    have.add(_bare(s))
                    rows.append((_bare(s), ch))
            menu[h] = [(b, ch, eval_ws.get(ch) if ch is not None else None)
                       for b, ch in rows if b not in veto]
        else:
            tot = sum(e["total"] for e in es)
            replies[h] = [e["child_hash"] for e in es if e.get("child_hash") is not None
                          and bc.reply_share(e, tot) >= share_floor]

    def passes(ev):
        return bc.passes_abs_gate(ev, slice_prior, our_white, robustness_floor)

    def dead_set(bad):
        return {g for g, ms in menu.items() if any(ev is not None for _, _, ev in ms)
                and not any(passes(ev) and (g, b) not in bad for b, _, ev in ms)}

    bad: set = set()
    dead = dead_set(bad)
    dead_first, by_depth = len(dead), []
    for _ in range(depth):
        bad_child = {c for c, kids in replies.items() if any(k in dead for k in kids)}
        new = {(h, b) for h, ms in menu.items() for b, ch, _ in ms if ch in bad_child}
        if new == bad and by_depth:
            break
        bad = new
        by_depth.append(len(bad))
        dead = dead_set(bad)
    out, void, n_src = {}, 0, 0
    for h in {h for h, _ in bad}:
        if h in dead:           # no passing move would be left: the veto is void here
            void += 1
            continue
        out[h] = {b for hh, b in bad if hh == h}
    if depth:
        for h, ms in menu.items():
            src = source_moves.get(h)
            if src is None or h in dead:
                continue
            for b, _, ev in ms:
                if b == _bare(src) and ev is not None and not passes(ev) and any(
                        passes(e2) and (h, b2) not in bad for b2, _, e2 in ms):
                    if b not in out.get(h, ()):
                        n_src += 1
                    out.setdefault(h, set()).add(b)
    return out, _stats(depth, dead_first, by_depth, out, void, n_src)


def vetoes(edf, ek: np.ndarray, ev: np.ndarray, our_white: bool, slice_prior: float, *, depth: int,
           robustness_floor: float = bc.ROBUSTNESS_FLOOR, share_floor: float = 0.002,
           excluded: dict | None = None, engine_moves: dict | None = None,
           source_moves: dict | None = None) -> tuple[dict[int, set[str]], dict]:
    """vetoes_ref on the collector's frame (parent_hash, move_san, child_hash, parent_epd, total
    and, with an opponent prior, reach_share). ek, ev: the eval arrays the graph is built from
    (position hash, White-view expected score)."""
    import polars as pl
    excluded, engine_moves, source_moves = excluded or {}, engine_moves or {}, source_moves or {}
    side = "w" if our_white else "b"
    cols = ["parent_hash", "move_san", "child_hash", "parent_epd", "total"]
    has_share = "reach_share" in edf.columns
    d = edf.select(cols + (["reach_share"] if has_share else [])).with_columns(
        pl.col("parent_hash").cast(pl.Int64), pl.col("child_hash").cast(pl.Int64),
        pl.col("move_san").str.strip_chars_end("+#").alias("bare"),
        (pl.col("parent_epd").str.splitn(" ", 3).struct.field("field_1") == side).alias("our"))
    share = pl.col("total") / pl.col("total").sum().over("parent_hash")
    if has_share:
        share = pl.coalesce(pl.col("reach_share"), share)
    opp = (d.filter(~pl.col("our")).with_columns(share.alias("share"))
           .filter(pl.col("child_hash").is_not_null() & (pl.col("share") >= share_floor))
           .select("parent_hash", "child_hash"))
    our = d.filter(pl.col("our")).select("parent_hash", "bare", "child_hash")
    del d
    key = pl.Schema({"parent_hash": pl.Int64, "bare": pl.String})
    parents = our["parent_hash"].unique()
    if engine_moves:
        pset = set(parents.to_list())
        eng = [(h, _bare(s), ch) for h, lst in engine_moves.items() if h in pset for s, ch in lst]
        if eng:
            extra = pl.DataFrame(eng, schema={**key, "child_hash": pl.Int64}, orient="row")
            extra = extra.unique(["parent_hash", "bare"], keep="first", maintain_order=True)
            our = pl.concat([our, extra.join(our, on=["parent_hash", "bare"], how="anti")])
    if excluded:
        ex = pl.DataFrame([(h, b) for h, bs in excluded.items() for b in bs], schema=key, orient="row")
        our = our.join(ex, on=["parent_hash", "bare"], how="anti")
    # each move's eval, by binary search in the graph's eval arrays
    o = np.argsort(ek, kind="stable")
    sk, sv = ek[o], ev[o]
    ch = our["child_hash"].fill_null(0).to_numpy()
    i = np.clip(np.searchsorted(sk, ch), 0, max(len(sk) - 1, 0))
    hit = ((sk[i] == ch) if len(sk) else np.zeros(len(ch), bool)) & our["child_hash"].is_not_null().to_numpy()
    e = np.where(hit, sv[i] if len(sk) else 0.0, math.nan)
    bar = slice_prior - robustness_floor if our_white else slice_prior + robustness_floor
    ok = hit & ((e >= bar) if our_white else (e <= bar))
    our = our.with_columns(pl.Series("has_eval", hit), pl.Series("passes", ok),
                           pl.lit(False).alias("bad"))

    def dead_nodes(frame):
        return (frame.group_by("parent_hash").agg(
            pl.col("has_eval").any().alias("any_eval"),
            (pl.col("passes") & ~pl.col("bad")).any().alias("alive"))
            .filter(pl.col("any_eval") & ~pl.col("alive"))["parent_hash"])

    dead = dead_nodes(our)
    dead_first, by_depth = len(dead), []
    for _ in range(depth):
        bad_child = opp.filter(pl.col("child_hash").is_in(dead.implode()))["parent_hash"].unique()
        new = our.with_columns((pl.col("child_hash").is_not_null()
                                & pl.col("child_hash").is_in(bad_child.implode())).alias("bad"))
        now = int(new["bad"].sum())
        if by_depth and now == by_depth[-1]:     # bad_k only grows with k: a fixed point
            break
        our = new
        by_depth.append(now)
        dead = dead_nodes(our)
    hit_nodes = our.filter(pl.col("bad"))["parent_hash"].unique()
    void = int(hit_nodes.is_in(dead.implode()).sum())
    alive = ~pl.col("parent_hash").is_in(dead.implode())
    out: dict[int, set[str]] = {}
    for h, b in our.filter(pl.col("bad") & alive).select("parent_hash", "bare").iter_rows():
        out.setdefault(h, set()).add(b)
    n_src = 0
    if depth and source_moves:
        pset = set(parents.to_list())
        src = pl.DataFrame([(h, _bare(s)) for h, s in source_moves.items() if h in pset and s],
                           schema=key, orient="row")
        keep = (our.group_by("parent_hash").agg((pl.col("passes") & ~pl.col("bad")).any().alias("keep"))
                .filter(pl.col("keep"))["parent_hash"])
        rows = (our.filter(pl.col("has_eval") & ~pl.col("passes") & alive
                           & pl.col("parent_hash").is_in(keep.implode()))
                .join(src, on=["parent_hash", "bare"], how="semi"))
        for h, b in rows.select("parent_hash", "bare").iter_rows():
            if b not in out.get(h, ()):
                n_src += 1
            out.setdefault(h, set()).add(b)
    return out, _stats(depth, dead_first, by_depth, out, void, n_src)


def merged(excluded: dict | None, extra: dict[int, set[str]]) -> dict:
    """The verified exclusions with the lookahead vetoes added (neither input is changed)."""
    out = {h: set(bs) for h, bs in (excluded or {}).items()}
    for h, bs in extra.items():
        out.setdefault(h, set()).update(bs)
    return out
