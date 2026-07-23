"""Drill construction: turn the next scheduled card into a full playable line.

A drill is one root-to-leaf line through the pack tree:
  target   = highest-priority due card (else next new card under the daily cap)
  path     = parent-pointer walk root -> target's canonical node
  start    = deepest path node whose position was already shown this session
             (session_positions), else the root — so later drills skip
             already-rehearsed prefixes but never drop the user into an
             unseen middlegame position.
  extension: below the target, our nodes follow their single child; opponent
             nodes sample a child weighted by reply_share (seeded RNG) until a
             leaf. Mid-line our-positions become gradable encounters too.

The server keeps active drills in memory (single-user app): grading must be
server-authoritative and the payload's expected moves are not trusted from the
client.
"""

from __future__ import annotations

import random
import sqlite3
import uuid

from fsrs import Rating

from . import scheduler as sched
from .pack import TrainingPack


class DrillManager:
    def __init__(self, pack: TrainingPack, con: sqlite3.Connection):
        self.pack = pack
        self.con = con
        self.active: dict[str, dict] = {}      # drill_id -> state

    # ── construction ────────────────────────────────────────────────────────

    def _pick_target(self, rep: str, settings: dict) -> tuple[dict | None, str]:
        due = sched.due_queue(self.con, self.pack, rep, settings)
        if due:
            return due[0], "due"
        cap = int(settings["new_per_day"])
        if sched.new_introduced_today(self.con, rep) < cap:
            new = sched.new_queue(self.con, self.pack, rep, settings)
            if new:
                return new[0], "new"
        return None, "empty"

    def _session_seen(self, session_id: int) -> set[int]:
        return {r[0] for r in self.con.execute(
            "SELECT position_hash FROM session_positions WHERE session_id=?",
            (session_id,))}

    def _extend_below(self, rep: str, node_id: int, rng: random.Random,
                      allow_new: bool) -> list[int]:
        """Continue past the target node to a leaf; returns extra node ids.
        With allow_new=False (daily new-card cap exhausted and extend_past_cap
        off), the line is truncated before the first our-position that has no
        card yet — the drill still plays out known territory but stops rather
        than introducing more new material."""
        kids = self.pack.children[rep]
        cols = self.pack.tree[rep]
        out = []
        nid = node_id
        while True:
            ch = kids.get(nid, [])
            if not ch:
                return out
            if cols["is_our_turn"][nid]:
                nid = ch[0]                      # single best_move edge
            else:
                weights = [cols["reply_share"][c] or 0.0 for c in ch]
                tot = sum(weights)
                if tot <= 0:
                    nid = ch[0]
                else:
                    nid = rng.choices(ch, weights=weights, k=1)[0]
            if (not allow_new and cols["is_our_turn"][nid]
                    and sched.get_card_row(self.con, rep,
                                           cols["position_hash"][nid]) is None):
                return out                       # would introduce a new card
            out.append(nid)

    def build_drill(self, rep: str, session_id: int, settings: dict) -> dict | None:
        target, kind = self._pick_target(rep, settings)
        if target is None:
            return None
        path = self.pack.path_to_root(rep, target["canonical_node_id"])
        rng = random.Random(f"{session_id}:{target['position_hash']}")
        cap_left = (sched.new_introduced_today(self.con, rep)
                    < int(settings["new_per_day"]))
        allow_new = bool(settings["extend_past_cap"]) or cap_left
        path = path + self._extend_below(rep, path[-1], rng, allow_new)

        seen = self._session_seen(session_id)
        cols = self.pack.tree[rep]
        start_idx = 0
        for i, nid in enumerate(path):
            if cols["position_hash"][nid] in seen:
                start_idx = i                    # deepest seen node on the path

        steps = []
        for i, nid in enumerate(path[1:], start=1):
            parent = path[i - 1]
            steps.append({
                "san": cols["move_san"][nid],
                "our": bool(cols["is_our_turn"][parent]),
                "ph": cols["position_hash"][parent],   # position the move is played FROM
                "epd": cols["epd"][parent],
            })
        while steps and not steps[-1]["our"]:
            steps.pop()                          # trailing opponent moves train nothing
        drill_id = uuid.uuid4().hex[:12]
        self.active[drill_id] = {
            "rep": rep, "session_id": session_id, "steps": steps,
            "graded_plies": set(), "settings": settings,
            "target_hash": target["position_hash"], "kind": kind,
        }
        # prune finished/abandoned drills so the dict can't grow unbounded
        if len(self.active) > 50:
            for k in list(self.active)[:-50]:
                del self.active[k]
        return {
            "drill_id": drill_id, "rep": rep, "kind": kind,
            "start_index": start_idx,            # steps[:start_index] = lead-in
            "steps": [{"san": s["san"], "our": s["our"]} for s in steps],
            "target_ply": len(self.pack.path_to_root(rep, target["canonical_node_id"])) - 1,
        }

    # ── answering ───────────────────────────────────────────────────────────

    def answer(self, drill_id: str, ply_index: int, san: str,
               fsrs_scheduler, duration_ms: int | None = None) -> dict:
        """Grade the user's move at steps[ply_index]. Only the FIRST answer per
        ply is graded; a card is graded at most once per drill and (setting) at
        most once per local day."""
        st = self.active.get(drill_id)
        if st is None:
            return {"error": "unknown or expired drill"}
        steps = st["steps"]
        if not (0 <= ply_index < len(steps)) or not steps[ply_index]["our"]:
            return {"error": "not an our-move ply"}
        step = steps[ply_index]
        correct = (san == step["san"])

        first_attempt = ply_index not in st["graded_plies"]
        st["graded_plies"].add(ply_index)
        graded = False
        if first_attempt:
            # record the position as seen this session (drilled region only)
            with self.con:
                self.con.execute(
                    "INSERT OR IGNORE INTO session_positions(session_id, "
                    "position_hash, first_seen) VALUES(?,?,?)",
                    (st["session_id"], step["ph"], sched.utcnow_iso()))
            card = sched.get_or_create_card(self.con, st["rep"], step["ph"],
                                            step["epd"], step["san"])
            once_per_day = bool(st["settings"]["review_once_per_day"])
            if not (once_per_day and sched.reviewed_today(self.con, card["card_id"])):
                sched.record_review(
                    self.con, fsrs_scheduler, card,
                    Rating.Good if correct else Rating.Again,
                    source="train", session_id=st["session_id"],
                    duration_ms=duration_ms)
                graded = True
            with self.con:
                self.con.execute(
                    "UPDATE sessions SET n_reviews=n_reviews+1, "
                    "n_correct=n_correct+? WHERE session_id=?",
                    (1 if correct else 0, st["session_id"]))

        remaining_our = [i for i in range(ply_index + 1, len(steps)) if steps[i]["our"]]
        done = correct and not remaining_our
        if done:
            self.active.pop(drill_id, None)
        return {"correct": correct, "expected_san": step["san"],
                "graded": graded, "done": done,
                "rep": st["rep"], "position_hash": step["ph"]}
