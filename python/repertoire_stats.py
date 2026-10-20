"""Repertoire-level statistics for ANY book in the repertoire format (our generated
books and course books alike), against a pool's reply frequencies.

For a book (position_hash -> our move at our turns), walks it path-exact from the
start position: at our turn the book's move, at the opponent's turn every reply
the pool records, weighted by its share. A reply the book has no position for
leaves the book there. Reports:

  moves              our-turn positions the walk reaches with a book move
  variations         start-to-end paths; a repeated position ends a path (books
                     can contain repetition loops), so the count is finite
  length             our moves per variation: mean, median, max

  The walk follows only replies the POOL records, so for a course whose deep
  theory sits below the pool's floor it sees part of the tree (measured: a median
  30% of a course's moves). Use it for the REACH-weighted figures; course size
  (moves, variations, lengths over the course's own tree) is computed by
  course_books.structure_stats.
  in_book_at[N]      share of games still in book at our N-th move
  full_line_share    share of games that follow some variation to its end
  first_moves        for a White book our first move; for Black, the opponent
                     first moves the book answers and their share
  openings           Lichess opening names (deepest name on each path), by share
  eval_drop          how much the book's own moves give up by the engine:
                     expected-score drop parent -> child from our side, its
                     reach-weighted mean, and the count of drops over 0.10

    from repertoire_stats import PoolIndex, book_stats
    idx = PoolIndex(POOL); s = book_stats(load_book(path, "white"), "white", idx)
"""
from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from pathlib import Path

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from zobrist import zobrist_int64  # noqa: E402

POOL = "E:/chess/position-stats/position_stats_pooled_ge1800_2013_2026_brc.parquet"
OPENINGS_TSV = Path("C:/Users/David/Documents/Chess Opening Dashboard/data/vendor/chess-openings")
ROOT_HASH = zobrist_int64(chess.Board())


def nsan(s):
    return None if s is None else s.rstrip("+#")


class PoolIndex:
    """Sorted edge arrays of a pool: parent -> [(san, child, total)]."""

    def __init__(self, path: str = POOL):
        e = (pl.read_parquet(path, columns=["parent_hash", "move_san", "total", "child_hash"])
             .filter(pl.col("child_hash").is_not_null()).sort("parent_hash"))
        self.p = e["parent_hash"].to_numpy()
        self.s = [nsan(x) for x in e["move_san"].to_list()]
        self.t = e["total"].to_numpy().astype(np.float64)
        self.c = e["child_hash"].to_numpy()

    def edges(self, h: int):
        lo = int(np.searchsorted(self.p, h, "left"))
        hi = int(np.searchsorted(self.p, h, "right"))
        return [(self.s[k], int(self.c[k]), self.t[k]) for k in range(lo, hi)]


_NAMES: dict[int, tuple[str, str]] | None = None


def opening_names() -> dict[int, tuple[str, str]]:
    """position hash -> (eco, name), the deepest Lichess name for a position."""
    global _NAMES
    if _NAMES is None:
        names, depth = {}, {}
        for f in sorted(OPENINGS_TSV.glob("*.tsv")):
            with open(f, encoding="utf-8") as fh:
                for row in csv.DictReader(fh, delimiter="\t"):
                    b = chess.Board()
                    for tok in row["pgn"].split():
                        if not tok.endswith("."):
                            b.push_san(tok)
                    h = zobrist_int64(b)
                    if h not in names or len(b.move_stack) > depth[h]:
                        names[h], depth[h] = (row["eco"], row["name"]), len(b.move_stack)
        _NAMES = names
    return _NAMES


def load_book(path, colour: str) -> dict:
    """{'moves': {hash: san}, 'child': {hash: child_hash or None},
        'rows': set of hashes, 'value': {hash: white-perspective value}}"""
    cols = pl.read_parquet_schema(path).names()
    want = ["position_hash", "side_to_move", "best_move", "value"] + \
        (["best_child"] if "best_child" in cols else [])
    d = pl.read_parquet(path, columns=want)
    moves, child, value = {}, {}, {}
    for r in d.iter_rows(named=True):
        h = r["position_hash"]
        if r["value"] is not None:
            value[h] = r["value"]
        if r["side_to_move"] == colour and r["best_move"]:
            moves[h] = nsan(r["best_move"])
            child[h] = r.get("best_child")
    return {"moves": moves, "child": child, "rows": set(d["position_hash"].to_list()),
            "value": value}


def book_stats(book: dict, colour: str, idx: PoolIndex, evals=None,
               min_reach: float = 1e-7, max_depth: int = 120) -> dict:
    our_white = colour == "white"
    moves, child_of, rows = book["moves"], book["child"], book["rows"]
    names = opening_names()
    in_book_at = defaultdict(float)
    lengths, leaf_reach = [], []
    open_share = defaultdict(float)
    first = defaultdict(float)
    reached_moves = set()
    drops = []                 # (reach, drop) for each booked move on a path

    def our_child(h):
        c = child_of.get(h)
        if c is not None:
            return c
        m = moves[h]
        for s, ch, _t in idx.edges(h):
            if s == m:
                return ch
        return None

    # iterative DFS: (hash, white_to_move, reach, our_moves_so_far, last_name, path_set, first)
    stack = [(ROOT_HASH, True, 1.0, 0, None, frozenset([ROOT_HASH]), None)]
    while stack:
        h, wtm, r, n, name, onpath, fm = stack.pop()
        nm = names.get(h)
        if nm:
            name = nm
        if len(onpath) > max_depth:
            lengths.append(n); leaf_reach.append(r); open_share[name] += r
            continue
        if wtm == our_white:
            m = moves.get(h)
            if m is None:
                if n:
                    lengths.append(n); leaf_reach.append(r)
                open_share[name] += r
                continue
            reached_moves.add(h)
            in_book_at[n + 1] += r
            if our_white and n == 0:
                first[m] += r
            ch = our_child(h)
            if evals is not None and ch is not None:
                drops.append((r, h, ch))
            if ch is None or ch in onpath:
                lengths.append(n + 1); leaf_reach.append(r); open_share[name] += r
                continue
            stack.append((ch, not wtm, r, n + 1, name, onpath | {ch}, fm))
        else:
            es = idx.edges(h)
            tot = sum(t for *_, t in es)
            cont = 0.0
            for s, ch, t in es:
                share = t / tot if tot else 0.0
                rr = r * share
                if ch in rows and ch not in onpath and rr >= min_reach:
                    cont += share
                    if not our_white and h == ROOT_HASH:
                        first[s] += rr
                    stack.append((ch, not wtm, rr, n, name, onpath | {ch}, fm or s))
            lost = r * (1.0 - cont)
            if lost > 0:
                open_share[name] += lost
            if cont == 0.0 and n:
                # the book ended with our last move: a variation followed to its end
                lengths.append(n); leaf_reach.append(r)
    L = np.array(lengths) if lengths else np.array([0])
    R = np.array(leaf_reach) if leaf_reach else np.array([0.0])
    out = {"moves_in_file": len(moves), "moves_reached": len(reached_moves),
           "variations": int(len(lengths)),
           "len_mean": float(L.mean()), "len_median": float(np.median(L)), "len_max": int(L.max()),
           "full_line_share": float(R.sum()),
           **{f"in_book_at_{k}": round(in_book_at.get(k, 0.0), 4) for k in (1, 2, 4, 6, 8, 10)},
           "first_moves": dict(sorted(first.items(), key=lambda x: -x[1])[:8]),
           "openings": {(f"{k[0]} {k[1]}" if k else "(unnamed)"): v for k, v in
                        sorted(open_share.items(), key=lambda x: -x[1])[:15]}}
    if evals is not None and drops:
        hs = np.array([x[1] for x in drops] + [x[2] for x in drops], dtype=np.int64)
        ev = evals(hs)
        k = len(drops)
        par, chd = ev[:k], ev[k:]
        rr = np.array([x[0] for x in drops])
        ok = ~(np.isnan(par) | np.isnan(chd))
        # our perspective: white score for White books, 1 - white score for Black
        dp = (par - chd) if our_white else (chd - par)
        dp = np.where(ok, dp, np.nan)
        w = rr[ok]
        out["eval_drop_mean"] = float((dp[ok] * w).sum() / w.sum()) if w.sum() else math.nan
        out["eval_drop_over_010"] = int((dp[ok] > 0.10).sum())
        out["eval_coverage"] = float(ok.mean())
    return out
