"""Fact-sheet construction for repertoire annotations (Stage A library).

Everything an annotation may claim is computed here, deterministically, from
pipeline data: the LLM stage is only allowed to phrase these facts. Evals are
expressed as OUR-side expected scores (0-1) plus verbal buckets — never raw
centipawns (the eval DB saturates mates at +-2000).

fact_hash = sha256 of the canonical facts JSON (floats rounded at
construction, no tree-node ids inside) so pack rebuilds that renumber nodes
don't churn hashes.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

import chess
import numpy as np
import polars as pl

from stage3_backwards_induction import LICHESS_CP_SCALE, idea_token
from trainer_app.pack import TrainingPack
from zobrist import zobrist_int64


def cp_to_es(cp: int) -> float:
    return 1.0 / (1.0 + np.exp(-LICHESS_CP_SCALE * cp))


def es_bucket(es_our: float) -> str:
    """Verbal assessment from OUR side's expected score."""
    if es_our >= 0.90: return "completely winning"
    if es_our >= 0.78: return "winning"
    if es_our >= 0.66: return "clearly better"
    if es_our >= 0.56: return "slightly better"
    if es_our >= 0.44: return "balanced"
    if es_our >= 0.34: return "slightly worse"
    if es_our >= 0.22: return "clearly worse"
    return "losing"


def _r(x, nd=3):
    return None if x is None else round(float(x), nd)


class EvalDB:
    """Sorted-array eval lookup (stage3 full-DB pattern)."""

    def __init__(self, path: str):
        df = pl.read_parquet(path, columns=["position_hash", "eval_cp"]).sort("position_hash")
        self.ph = df["position_hash"].to_numpy()
        self.cp = df["eval_cp"].to_numpy()

    def get_cp(self, h: int) -> int | None:
        i = int(np.searchsorted(self.ph, h))
        if i < len(self.ph) and int(self.ph[i]) == h:
            return int(self.cp[i])
        return None


class FactsBuilder:
    def __init__(self, pack: TrainingPack, color: str, *, evaldb: EvalDB,
                 rep_path: str, stats_path: str, prior_path: str | None,
                 engine_cache_path: str | None, trainer_db_path: str | None):
        self.pack = pack
        self.color = color
        self.evaldb = evaldb
        self.sign = 1.0 if color == "white" else 0.0   # our ES = es if white else 1-es

        cards = pack.cards[color]
        card_hashes = set(cards["position_hash"].to_list())

        # after-best_move child hashes (for the reply tables)
        self._after: dict[int, tuple[str, int]] = {}   # card hash -> (epd_after, hash_after)
        for row in cards.iter_rows(named=True):
            b = chess.Board(row["epd"] + " 0 1")
            b.push_san(row["best_move"])
            self._after[row["position_hash"]] = (b.epd(), zobrist_int64(b))
        after_hashes = {h for _, h in self._after.values()}

        # rep metrics for card positions (semi-filtered scan of the 13M-row rep)
        rep = (pl.scan_parquet(rep_path)
                 .filter(pl.col("position_hash").is_in(list(card_hashes)))
                 .select(["position_hash", "value", "value_worst", "crush_potential",
                          "opponent_error", "forcingness", "cover_eff", "eval_score"])
                 .collect())
        self.rep = {r["position_hash"]: r for r in rep.iter_rows(named=True)}

        # stats edges from card positions AND after-move positions
        want = list(card_hashes | after_hashes)
        st = (pl.scan_parquet(stats_path)
                .filter(pl.col("parent_hash").is_in(want))
                .select(["parent_hash", "move_san", "total", "white_score_avg",
                         "child_hash"])
                .collect())
        self.edges: dict[int, list[dict]] = defaultdict(list)
        for r in st.iter_rows(named=True):
            self.edges[r["parent_hash"]].append(r)
        for v in self.edges.values():
            v.sort(key=lambda e: -e["total"])

        # theme prior: ctx -> token -> game_freq
        self.prior: dict[str, dict[str, float]] = defaultdict(dict)
        if prior_path and Path(prior_path).exists():
            pr = pl.read_parquet(prior_path)
            for r in pr.iter_rows(named=True):
                self.prior[r["ctx"]][r["token"]] = r["game_freq"]

        # theme cross-index: token -> [(reach, position_hash)] over this color's cards
        self.token_index: dict[str, list[tuple[float, int]]] = defaultdict(list)
        for row in cards.iter_rows(named=True):
            self.token_index[idea_token(row["best_move"])].append(
                (row["reach"], row["position_hash"]))
        for v in self.token_index.values():
            v.sort(reverse=True)

        # engine cache (Stage B output): (epd, mode) -> list of rows by rank
        self.engine: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self.has_engine_cache = False
        if engine_cache_path and Path(engine_cache_path).exists():
            ec = pl.read_parquet(engine_cache_path)
            for r in ec.iter_rows(named=True):
                self.engine[(r["epd"], r["mode"])].append(r)
            for v in self.engine.values():
                v.sort(key=lambda r: r["rank"])
            self.has_engine_cache = True

        # user deviations (personalization)
        self.deviations: dict[int, list[dict]] = defaultdict(list)
        if trainer_db_path and Path(trainer_db_path).exists():
            con = sqlite3.connect(trainer_db_path)
            q = ("SELECT position_hash, played_move, COUNT(*) n FROM deviations "
                 "WHERE rep=? AND dismissed=0 GROUP BY position_hash, played_move "
                 "ORDER BY n DESC")
            for h, mv, n in con.execute(q, (color,)):
                self.deviations[h].append({"san": mv, "times": n})
            con.close()

    # ── helpers ─────────────────────────────────────────────────────────────

    def our_es(self, cp: int | None) -> float | None:
        if cp is None:
            return None
        es = cp_to_es(cp)
        return float(es if self.color == "white" else 1.0 - es)

    def _line_str(self, sans: list[str]) -> str:
        out = []
        for i, s in enumerate(sans):
            if i % 2 == 0:
                out.append(f"{i // 2 + 1}.{s}")
            else:
                out.append(s)
        return " ".join(out)

    def _ctx(self, path: list[int]) -> str:
        cols = self.pack.tree[self.color]
        for nid in path[1:]:
            parent = cols["parent_id"][nid]
            if not cols["is_our_turn"][parent]:
                return cols["move_san"][nid]
        return "(all games)"

    def _pv_to_sans(self, epd: str, pv_uci: str, limit: int = 8) -> list[str]:
        b = chess.Board(epd + " 0 1")
        out = []
        for u in pv_uci.split()[:limit]:
            mv = chess.Move.from_uci(u)
            if mv == chess.Move.null():
                out.append("--")
                b.push(chess.Move.null())
                continue
            if not b.is_legal(mv):
                break
            out.append(b.san(mv))
            b.push(mv)
        return out

    def _engine_block(self, epd: str, epd_after: str) -> dict | None:
        def mp(e, mode, lim):
            rows = self.engine.get((e, mode), [])
            return [{"rank": r["rank"], "es": _r(self.our_es(r["cp"])),
                     "assessment": es_bucket(self.our_es(r["cp"])),
                     "pv": self._pv_to_sans(e, r["pv_uci"], lim)} for r in rows]
        here = mp(epd, "here", 8)
        after = mp(epd_after, "after", 8)
        threat_rows = self.engine.get((epd_after, "threat"), [])
        threat = None
        if threat_rows:
            r = threat_rows[0]
            b = chess.Board(epd_after + " 0 1")
            b.push(chess.Move.null())
            threat = {"es": _r(self.our_es(r["cp"])),
                      "pv": self._pv_to_sans(b.epd(), r["pv_uci"], 8)}
        if not here and not after and threat is None:
            return None
        return {"here_multipv": here, "after_move_multipv": after, "threat": threat}

    # ── per-card fact sheet ─────────────────────────────────────────────────

    def build(self, card: dict) -> tuple[dict, bool]:
        """Returns (facts dict, has_engine)."""
        h = card["position_hash"]
        epd = card["epd"]
        best = card["best_move"]
        board = chess.Board(epd + " 0 1")
        path = self.pack.path_to_root(self.color, card["canonical_node_id"])
        sans = self.pack.line_sans(self.color, path)
        ctx = self._ctx(path)
        epd_after, hash_after = self._after[h]

        # transpositions: other tree arrivals at this hash (short alternates)
        cols = self.pack.tree[self.color]
        transpositions = []
        for nid, ph in enumerate(cols["position_hash"]):
            if ph == h and nid != card["canonical_node_id"]:
                alt = self.pack.line_sans(self.color, self.pack.path_to_root(self.color, nid))
                transpositions.append(self._line_str(alt))
                if len(transpositions) >= 3:
                    break

        # candidates at this position: rep edges + eval-only legal moves
        edges_here = {e["move_san"]: e for e in self.edges.get(h, [])}
        total_here = sum(e["total"] for e in edges_here.values()) or 1
        cand: list[dict] = []
        chosen_cp = None
        for mv in board.legal_moves:
            san = board.san(mv)
            b2 = board.copy(stack=False)
            b2.push(mv)
            cp = self.evaldb.get_cp(zobrist_int64(b2))
            e = edges_here.get(san)
            row = {"san": san,
                   "games": int(e["total"]) if e else 0,
                   "emp_score": _r((e["white_score_avg"] if self.color == "white"
                                    else 1 - e["white_score_avg"])) if e else None,
                   "es": _r(self.our_es(cp)),
                   "assessment": es_bucket(self.our_es(cp)) if cp is not None else None,
                   "source": "played" if e else "eval-only"}
            if san == best:
                chosen_cp = cp
            cand.append(row)
        chosen_es = self.our_es(chosen_cp)
        for c in cand:
            c["delta_es_vs_chosen"] = (_r(c["es"] - chosen_es)
                                       if (c["es"] is not None and chosen_es is not None)
                                       else None)
        chosen_row = next((c for c in cand if c["san"] == best), None)
        others = [c for c in cand if c["san"] != best and
                  (c["games"] > 0 or (c["es"] is not None and chosen_es is not None
                                      and c["es"] >= chosen_es - 0.08))]
        others.sort(key=lambda c: (-c["games"], -(c["es"] or 0)))
        candidates = ([chosen_row] if chosen_row else []) + others[:5]

        # opponent replies after our move: share, quality, our booked answer
        replies = []
        edges_after = self.edges.get(hash_after, [])
        total_after = sum(e["total"] for e in edges_after) or 1
        reply_es = []
        for e in edges_after:
            cp = self.evaldb.get_cp(e["child_hash"]) if e["child_hash"] is not None else None
            reply_es.append((e, cp, self.our_es(cp)))
        # their best reply = max THEIR es = min OUR es among covered replies
        our_es_covered = [oes for _, _, oes in reply_es if oes is not None]
        best_for_them = min(our_es_covered) if our_es_covered else None
        for e, cp, oes in reply_es:
            share = e["total"] / total_after
            if share < 0.03 and len(replies) >= 4:
                continue
            booked = self.pack.book_move(self.color, e["child_hash"]) \
                if e["child_hash"] is not None else None
            replies.append({
                "san": e["move_san"], "share_pct": _r(share * 100, 1),
                "games": int(e["total"]),
                "es_after": _r(oes),
                "assessment": es_bucket(oes) if oes is not None else None,
                "gives_us_pct": (_r((oes - best_for_them) * 100, 1)
                                 if (oes is not None and best_for_them is not None)
                                 else None),        # how much this reply concedes
                "our_booked_response": booked,
            })
            if len(replies) >= 8:
                break

        # themes
        token = idea_token(best)
        related = [ph2 for _, ph2 in self.token_index.get(token, []) if ph2 != h][:6]
        related_lines = []
        for ph2 in related[:2]:
            c2 = self.pack.card(self.color, ph2)
            if c2:
                p2 = self.pack.path_to_root(self.color, c2["canonical_node_id"])
                related_lines.append(self._line_str(self.pack.line_sans(self.color, p2)))
        themes = {
            "token": token,
            "freq_in_ctx": _r(self.prior.get(ctx, {}).get(token)),
            "freq_global": _r(self.prior.get("(all games)", {}).get(token)),
            "other_positions_same_idea": len(related),
            "sample_lines_same_idea": related_lines,
        }

        m = self.rep.get(h, {})
        engine = self._engine_block(epd, epd_after)
        facts = {
            "color": self.color,
            "line": self._line_str(sans),
            "transpositions": transpositions,
            "ctx": ctx,
            "reach_pct": _r(card["reach"] * 100, 2),
            "our_move": {
                "san": best, "idea_token": token,
                "es": _r(chosen_es),
                "assessment": es_bucket(chosen_es) if chosen_es is not None else None,
                "games": chosen_row["games"] if chosen_row else 0,
                "emp_score": chosen_row["emp_score"] if chosen_row else None,
                "value_worst": _r(m.get("value_worst")),
                "crush_potential": _r(m.get("crush_potential")),
            },
            "candidates": candidates,
            "replies": replies,
            "node_metrics": {
                "opponent_error": _r(m.get("opponent_error")),
                "forcingness": _r(m.get("forcingness")),
                "cover_eff": _r(m.get("cover_eff")),
            },
            "engine": engine,
            "themes": themes,
            "past_mistakes": self.deviations.get(h, [])[:3],
        }
        return facts, engine is not None


def fact_hash(facts: dict) -> str:
    return hashlib.sha256(
        json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
