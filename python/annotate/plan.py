"""Plan facts for repertoire annotations: what our book does next from a position.

The training pack holds only the ladder stage being learned (a few hundred cards), so
looking ahead from a card stops after a few moves. PlanTree walks the FULL book (the
lifetime repertoire the trainer's deviation scanner uses) with the pack builder's own
tree walk, loading stats only for the positions that book reaches, and answers, for
an our-turn position:

  continuation    our move, then the most common reply and our answer, for up to
                  `max_ours` of our moves -- the plan if nothing unusual happens;
  branches        replies (share >= min_share) at the first opponent turns of that
                  continuation, with our booked answer and whether it differs from
                  the move the main line plays there ("we do X unless they play Y");
  recurring_ideas idea tokens (stage 3's idea_token: piece + destination, pawn
                  moves, castling) of our moves across the whole variation below,
                  weighted by reach -- the setup we aim for whatever they do.

Everything is deterministic and rounded, so fact hashes stay stable.
"""

from __future__ import annotations

from collections import defaultdict

import chess
import polars as pl

from stage3_backwards_induction import idea_token
from trainer_app.build_training_pack import build_tree
from zobrist import zobrist_int64


def _r(x, nd=3):
    return None if x is None else round(float(x), nd)


class PlanTree:
    def __init__(self, color: str, book_path: str, stats_path: str,
                 max_branches: int = 12, max_ply: int = 50):
        self.color = color
        book = pl.read_parquet(book_path, columns=["position_hash", "position_epd", "best_move"])
        best = {h: b for h, b in zip(book["position_hash"], book["best_move"])}
        # opponent-turn positions the book reaches: the children of our booked moves,
        # plus the start position (White's first move is the opponent's for Black)
        opp = {zobrist_int64(chess.Board())}
        for epd, san in zip(book["position_epd"], book["best_move"]):
            if san is None:
                continue
            b = chess.Board(epd + " 0 1")
            try:
                b.push_san(san)
            except ValueError:
                continue
            opp.add(zobrist_int64(b))
        st = (pl.scan_parquet(stats_path)
                .filter(pl.col("parent_hash").is_in(list(opp)))
                .select(["parent_hash", "move_san", "child_hash", "total"])
                .collect())
        edges: dict[int, list[tuple[str, int, int]]] = defaultdict(list)
        for p, m, c, t in zip(st["parent_hash"], st["move_san"], st["child_hash"], st["total"]):
            edges[p].append((m, c, t))
        for v in edges.values():
            v.sort(key=lambda e: -e[2])
        rows = build_tree(best, edges, color, min_reach=0.0,
                          max_branches=max_branches, max_ply=max_ply)
        self.rows = rows
        self.kids: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            if r["parent_id"] >= 0:
                self.kids[r["parent_id"]].append(r["node_id"])
        self.node_of: dict[int, int] = {}            # hash -> arrival with max reach
        for r in rows:
            n = self.node_of.get(r["position_hash"])
            if n is None or r["path_reach"] > rows[n]["path_reach"]:
                self.node_of[r["position_hash"]] = r["node_id"]

    def _main_reply(self, nid: int) -> int | None:
        ch = self.kids.get(nid, [])
        return max(ch, key=lambda c: self.rows[c]["reply_share"] or 0.0) if ch else None

    def _our_next(self, nid: int) -> str | None:
        ch = self.kids.get(nid, [])
        return self.rows[ch[0]]["move_san"] if ch else None

    def plan(self, position_hash: int, max_ours: int = 6, min_share: float = 0.10,
             branch_turns: int = 3, top_ideas: int = 6) -> dict | None:
        nid = self.node_of.get(position_hash)
        if nid is None or not self.rows[nid]["is_our_turn"]:
            return None
        rows = self.rows

        # continuation along the most common replies
        cont, opp_nodes, cur, n_ours = [], [], nid, 0
        while n_ours < max_ours:
            ch = self.kids.get(cur, [])
            if not ch:
                break
            if rows[cur]["is_our_turn"]:
                cur = ch[0]
                n_ours += 1
            else:
                opp_nodes.append(cur)
                cur = self._main_reply(cur)
            cont.append(rows[cur]["move_san"])

        # branches at the first opponent turns of the continuation
        branches = []
        for k, on in enumerate(opp_nodes[:branch_turns]):
            main = self._main_reply(on)
            main_next = self._our_next(main) if main is not None else None
            for c in sorted(self.kids.get(on, []), key=lambda c: -(rows[c]["reply_share"] or 0)):
                if c == main or (rows[c]["reply_share"] or 0) < min_share:
                    continue
                ans = self._our_next(c)
                branches.append({
                    "after": cont[:2 * k + 1],            # our move ... up to this reply
                    "reply": rows[c]["move_san"],
                    "share_pct": _r((rows[c]["reply_share"] or 0) * 100, 1),
                    "our_answer": ans,
                    "main_line_answer": main_next,
                    "plan_changes": (ans or "").rstrip("+#") != (main_next or "").rstrip("+#"),
                })

        # recurring ideas across the variation below (reach-weighted our moves)
        base = rows[nid]["path_reach"] or 0.0
        weight: dict[str, float] = defaultdict(float)
        stack = [nid]
        while stack:
            m = stack.pop()
            if rows[m]["is_our_turn"] and rows[m]["best_move"] and m != nid:
                weight[idea_token(rows[m]["best_move"])] += rows[m]["path_reach"] or 0.0
            stack.extend(self.kids.get(m, []))
        ideas = sorted(weight.items(), key=lambda kv: -kv[1])[:top_ideas]
        recurring = [{"token": t, "reach_pct": _r(min(w / base, 1.0) * 100, 1)}
                     for t, w in ideas if base > 0 and w / base >= 0.15]
        return {"continuation": cont, "branches": branches, "recurring_ideas": recurring}
