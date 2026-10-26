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


PIECE_VALUE = {chess.PAWN: 1, chess.KNIGHT: 3, chess.BISHOP: 3, chess.ROOK: 5,
               chess.QUEEN: 9, chess.KING: 0}
_PLURAL = {"pawn": "pawns", "knight": "knights", "bishop": "bishops", "rook": "rooks",
           "queen": "queen"}


def _material_words(diff: int) -> str:
    if diff == 0:
        return "material is level"
    n = abs(diff)
    amount = "a pawn" if n == 1 else f"{n} pawns' worth"
    return f"we are {amount} {'up' if diff > 0 else 'down'}"


def board_facts(b: chess.Board, us: chess.Color) -> dict:
    """What a note may say about the position on the board: material (counts and
    the balance in pawn units), each king (square, castled, castling rights), our
    pieces still undeveloped, and loose pieces of either side -- attacked and
    either undefended or attacked by a cheaper piece (forks show up as one attacker
    on two targets)."""
    def counts(c):
        return {chess.piece_name(p): len(b.pieces(p, c))
                for p in (chess.QUEEN, chess.ROOK, chess.BISHOP, chess.KNIGHT, chess.PAWN)}

    def total(c):
        return sum(PIECE_VALUE[p] * len(b.pieces(p, c)) for p in PIECE_VALUE)

    def describe(c):
        return ", ".join(f"{n} {_PLURAL[k] if n > 1 else k}" for k, n in counts(c).items() if n)

    diff = total(us) - total(not us)

    def king(c):
        sq = b.king(c)
        home = 0 if c == chess.WHITE else 7
        castled = sq in (chess.square(6, home), chess.square(2, home), chess.square(1, home)) \
            and not b.has_castling_rights(c)
        rights = [n for n, ok in (("kingside", b.has_kingside_castling_rights(c)),
                                  ("queenside", b.has_queenside_castling_rights(c))) if ok]
        return {"square": chess.square_name(sq), "castled": castled,
                "can_still_castle": rights,
                "in_check": b.is_check() and b.turn == c}

    home = 0 if us == chess.WHITE else 7
    undeveloped = [f"{chess.piece_name(b.piece_at(sq).piece_type)} on {chess.square_name(sq)}"
                   for sq in (chess.square(f, home) for f in (1, 2, 5, 6))
                   if b.piece_at(sq) and b.piece_at(sq).color == us
                   and b.piece_at(sq).piece_type in (chess.KNIGHT, chess.BISHOP)]

    def loose(c):
        out = []
        for sq, pc in b.piece_map().items():
            if pc.color != c or pc.piece_type in (chess.KING, chess.PAWN):
                continue
            defended = bool(b.attackers(c, sq))
            attackers = [a for a in b.attackers(not c, sq)        # a king can't take a
                         if not (defended and b.piece_at(a).piece_type == chess.KING)]  # guarded piece
            if not attackers:
                continue
            cheapest = min(PIECE_VALUE[b.piece_at(a).piece_type] for a in attackers)
            if defended and cheapest >= PIECE_VALUE[pc.piece_type]:
                continue
            by = ", ".join(f"{chess.piece_name(b.piece_at(a).piece_type)} on {chess.square_name(a)}"
                           for a in attackers)
            out.append(f"{chess.piece_name(pc.piece_type)} on {chess.square_name(sq)} "
                       f"(attacked by {by}{'' if defended else ', undefended'})")
        return out

    return {"material": {"ours": describe(us), "theirs": describe(not us),
                         "balance_for_us": diff, "summary": _material_words(diff)},
            "our_king": king(us), "their_king": king(not us),
            "our_undeveloped": undeveloped,
            "our_loose_pieces": loose(us), "their_loose_pieces": loose(not us)}


class EvalDB:
    """Sorted-array eval lookup: an eval-arrays directory (eval_arrays.py; verified,
    then memory-mapped -- the default, 6B entries, cannot be loaded) or a legacy
    (position_hash, eval_cp) parquet read whole."""

    def __init__(self, path: str):
        from eval_arrays import META_NAME, open_eval_arrays, verify_eval_arrays
        p = Path(path)
        if (p / META_NAME).is_file():
            self.status = verify_eval_arrays(p, adopt=False)
            self.ph, self.cp = open_eval_arrays(p)
            return
        df = pl.read_parquet(path, columns=["position_hash", "eval_cp"]).sort("position_hash")
        self.ph = df["position_hash"].to_numpy()
        self.cp = df["eval_cp"].to_numpy()
        self.status = f"read {p.name}"

    def get_cp(self, h: int) -> int | None:
        i = int(np.searchsorted(self.ph, h))
        if i < len(self.ph) and int(self.ph[i]) == h:
            return int(self.cp[i])
        return None


class FactsBuilder:
    def __init__(self, pack: TrainingPack, color: str, *, evaldb: EvalDB,
                 rep_path: str, stats_path: str, prior_path: str | None,
                 engine_cache_path: str | None, trainer_db_path: str | None,
                 plan_tree=None, course_notes=None):
        self.pack = pack
        self.color = color
        self.evaldb = evaldb
        self.sign = 1.0 if color == "white" else 0.0   # our ES = es if white else 1-es
        self.plan_tree = plan_tree              # annotate.plan.PlanTree over the full book
        self.course_notes = course_notes        # annotate.course_notes.CourseNotes

        cards = pack.cards[color]
        card_hashes = set(cards["position_hash"].to_list())

        # after-best_move child hashes (for the reply tables)
        self._after: dict[int, tuple[str, int]] = {}   # card hash -> (epd_after, hash_after)
        for row in cards.iter_rows(named=True):
            b = chess.Board(row["epd"] + " 0 1")
            b.push_san(row["best_move"])
            self._after[row["position_hash"]] = (b.epd(), zobrist_int64(b))
        after_hashes = {h for _, h in self._after.values()}

        # rep metrics for card positions (semi-filtered scan of the 13M-row rep);
        # budget books (the 2026-10 lifetime books) lack the Stage-3-only columns
        have = set(pl.read_parquet_schema(rep_path))
        rep = (pl.scan_parquet(rep_path)
                 .filter(pl.col("position_hash").is_in(list(card_hashes)))
                 .select([c for c in ("position_hash", "value", "value_worst", "opponent_error",
                                      "forcingness", "cover_eff", "eval_score") if c in have])
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
        # one level deeper for where the book ends: our follow-ups after their
        # three most common replies (what players actually did next)
        deeper = {e["child_hash"] for h in after_hashes for e in self.edges.get(h, [])[:3]
                  if e["child_hash"] is not None} - set(want)
        if deeper:
            st2 = (pl.scan_parquet(stats_path)
                     .filter(pl.col("parent_hash").is_in(list(deeper)))
                     .select(["parent_hash", "move_san", "total", "white_score_avg",
                              "child_hash"])
                     .collect())
            for r in st2.iter_rows(named=True):
                self.edges[r["parent_hash"]].append(r)
            for h in deeper:
                if h in self.edges:
                    self.edges[h].sort(key=lambda e: -e["total"])

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

    def our_cp(self, cp: int | None) -> int:
        """White-POV engine centipawns -> ours (0 when unknown)."""
        if cp is None:
            return 0
        return int(cp) if self.color == "white" else -int(cp)

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
                     "our_cp": self.our_cp(r["cp"]),
                     "pv": self._pv_to_sans(e, r["pv_uci"], lim)} for r in rows]
        here = mp(epd, "here", 8)
        after = mp(epd_after, "after", 12)       # long enough to show how to go on
        threat = self._null_threat(epd_after, "threat", after, ours=True)
        their = self._null_threat(epd, "pass", here, ours=False)
        if not here and not after and threat is None and their is None:
            return None
        return {"here_multipv": here, "after_move_multipv": after, "threat": threat,
                "their_threat_if_we_pass": their}

    def _null_threat(self, epd: str, mode: str, real: list[dict], ours: bool) -> dict | None:
        """What the side NOT to move would do with a free move (Stage B's null-move
        analyses): mode 'threat' after our move = OUR threat; mode 'pass' at the
        card = what the opponent threatens, i.e. what our move has to deal with.
        es_change_for_us = our expected score with the free move played minus with
        best play (positive for our threat, negative for theirs). A threat is real
        when it wins a piece (minor or more), mates, swings the engine by 1.5 pawns,
        or moves the expected score by 0.08 -- the score alone saturates in won
        positions, where winning a whole rook barely moves it."""
        rows = self.engine.get((epd, mode), [])
        if not rows or not real or real[0]["es"] is None:
            return None
        r = rows[0]
        b = chess.Board(epd + " 0 1")
        b.push(chess.Move.null())
        pv = self._pv_to_sans(b.epd(), r["pv_uci"], 8)
        es = self.our_es(r["cp"])
        if not pv or es is None:
            return None
        first = chess.Move.from_uci(r["pv_uci"].split()[0])
        tags = []
        cap_value = 0
        if b.is_capture(first):
            cap = b.piece_at(first.to_square)
            cap_value = PIECE_VALUE[cap.piece_type] if cap else 1
            tags.append(f"captures the {chess.piece_name(cap.piece_type) if cap else 'pawn'} on "
                        f"{chess.square_name(first.to_square)}")
        b.push(first)
        mate = b.is_checkmate()
        if mate:
            tags.append("mate")
        elif b.is_check():
            tags.append("check")
        change = es - real[0]["es"]
        cp_change = self.our_cp(r["cp"]) - real[0]["our_cp"]
        gain = (change, cp_change) if ours else (-change, -cp_change)
        return {"move": pv[0], "pv": pv, "what": tags,
                "assessment_if_free_move": es_bucket(es),
                "es_change_for_us": _r(change),
                "real": gain[0] >= 0.08 or gain[1] >= 150 or cap_value >= 3 or mate}

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
        plan = self.plan_tree.plan(h) if self.plan_tree else None

        # the board after our move: material, kings, development, loose pieces
        us = chess.WHITE if self.color == "white" else chess.BLACK
        b_after = board.copy(stack=False)
        b_after.push_san(best)
        position = board_facts(b_after, us)
        position["material_change_by_our_move"] = (
            position["material"]["balance_for_us"] - board_facts(board, us)["material"]["balance_for_us"])

        # where the book ends: how play goes on (engine lines, our recurring moves
        # in them, what players did next) -- the note must leave a plan from here
        book_end = None
        if plan is not None and len(plan.get("continuation", [])) <= 1:
            lines = (engine or {}).get("after_move_multipv") or []
            ours: dict[str, int] = defaultdict(int)
            for ln in lines:
                for s in ln["pv"][1::2]:               # PVs start with their reply
                    ours[s] += 1
            followups = []
            for e in edges_after[:3]:
                nxt = self.edges.get(e["child_hash"], []) if e["child_hash"] is not None else []
                tot = sum(x["total"] for x in nxt) or 1
                followups.append({
                    "reply": e["move_san"], "reply_share_pct": _r(e["total"] / total_after * 100, 1),
                    "our_moves": [{"san": x["move_san"], "share_pct": _r(x["total"] / tot * 100, 1),
                                   "games": int(x["total"])} for x in nxt[:3]]})
            book_end = {"engine_lines": [{"assessment": ln["assessment"], "pv": ln["pv"]}
                                         for ln in lines],
                        "our_moves_in_engine_lines": sorted(ours.items(), key=lambda kv: -kv[1])[:6],
                        "what_players_did_next": followups}

        # engine's first choice when it is not our move (our move is a practical choice)
        engine_top = None
        if engine and engine["here_multipv"] and engine["here_multipv"][0]["pv"]:
            top = engine["here_multipv"][0]
            if top["pv"][0].rstrip("+#") != best.rstrip("+#"):
                trow = next((c for c in cand if c["san"].rstrip("+#") == top["pv"][0].rstrip("+#")), None)
                engine_top = {"san": top["pv"][0], "assessment": top["assessment"],
                              "games": trow["games"] if trow else 0,
                              "emp_score": trow["emp_score"] if trow else None}
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
            },
            "candidates": candidates,
            "replies": replies,
            "node_metrics": {
                "opponent_error": _r(m.get("opponent_error")),
                "forcingness": _r(m.get("forcingness")),
                "cover_eff": _r(m.get("cover_eff")),
            },
            "engine": engine,
            "engine_top": engine_top,
            "plan": plan,
            "position_after_our_move": position,
            "book_end": book_end,
            "course_notes": self.course_notes.get(hash_after) if self.course_notes else [],
            "themes": themes,
            "past_mistakes": self.deviations.get(h, [])[:3],
        }
        return facts, engine is not None


def fact_hash(facts: dict) -> str:
    return hashlib.sha256(
        json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
