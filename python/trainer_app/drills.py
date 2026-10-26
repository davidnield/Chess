"""Drill construction: turn the next scheduled card into a full playable line.

A drill is one root-to-leaf line through the pack tree:
  target   = drills come in BLOCKS by opening family (trainer_app.families): up to
             `drills_per_family` drills from one family -- its due cards first,
             then its new cards -- before moving to the family of the most urgent
             card elsewhere. Within a family the highest-priority card goes first.
             A new card is offered only once every earlier our-position on its
             line is learned (_ready_new).
  path     = parent-pointer walk root -> target's canonical node
  start    = deepest path node, at or above the target, whose position was
             already shown this session (session_positions), else the root —
             the target's own move is always asked; later drills skip
             already-rehearsed prefixes but never drop the user into an
             unseen middlegame position.
  extension: below the target, our nodes follow their single child. A REVIEW
             (due) drill samples opponent replies by reply_share (seeded RNG)
             and stops before any our-position not learned yet: reviews never
             quiz unseen moves. A NEW drill follows the most common reply and
             teaches further new our-positions of the same family, up to the
             daily cap, only while each is at least as important (priority) as
             the family's best other unlearned card: breadth before depth, so a
             common 4th-move alternative is learned before ply 20 of the main
             line. The client teaches new moves (learn mode) before quizzing.

Each payload step carries `new`: an our-move whose card has never been
reviewed. The client shows those moves first and grades them on a replay from
memory; answer() therefore treats a drill as done only once every our-step
from the start is graded, not when the last one is.

The server keeps active drills in memory (single-user app): grading must be
server-authoritative and the payload's expected moves are not trusted from the
client.
"""

from __future__ import annotations

import random
import sqlite3
import uuid

from fsrs import Rating

from annotate.chunks import build_chunks

from . import scheduler as sched
from .families import MIN_REACH, SPLIT_REACH, build_families
from .pack import TrainingPack


class DrillManager:
    def __init__(self, pack: TrainingPack, con: sqlite3.Connection):
        self.pack = pack
        self.con = con
        self.active: dict[str, dict] = {}      # drill_id -> state
        self._chunk_ix: dict[str, dict[int, str]] = {}   # rep -> card hash -> chunk_id
        self._family_ix: dict[str, dict[int, tuple[str, str]]] = {}  # hash -> (id, label)
        self.split_reach, self.min_reach = SPLIT_REACH, MIN_REACH
        self._block: dict[int, dict] = {}       # session_id -> {rep, family, n}

    # ── construction ────────────────────────────────────────────────────────

    def _pick_target(self, rep: str, settings: dict,
                     session_id: int) -> tuple[dict | None, str]:
        due = sched.due_queue(self.con, self.pack, rep, settings)          # priority order
        cap_left = int(settings["new_per_day"]) - sched.new_introduced_today(self.con, rep)
        new = self._ready_new(rep, settings) if cap_left > 0 else []
        fam = self.family_of(rep)

        def f(c):
            return fam.get(c["position_hash"], ("", ""))[0]

        blk = self._block.get(session_id)
        cur = blk["family"] if blk and blk["rep"] == rep else None
        if cur is not None and blk["n"] < int(settings["drills_per_family"]):
            for pool, kind in ((due, "due"), (new, "new")):
                c = next((c for c in pool if f(c) == cur), None)
                if c is not None:
                    return c, kind
        # next block: the family of the most urgent card, another family if possible
        for pool, kind in ((due, "due"), (new, "new")):
            c = next((c for c in pool if f(c) != cur), pool[0] if pool else None)
            if c is not None:
                return c, kind
        return None, "empty"

    def _started(self, rep: str) -> set[int]:
        """Positions whose card has been reviewed at least once (learned)."""
        return {r[0] for r in self.con.execute(
            "SELECT position_hash FROM cards WHERE rep=? AND state IS NOT NULL", (rep,))}

    def chunk_of(self, rep: str) -> dict[int, str]:
        if rep not in self._chunk_ix:
            self._chunk_ix[rep] = {h: ch.chunk_id for ch in build_chunks(self.pack, rep)
                                   for h in ch.card_hashes}
        return self._chunk_ix[rep]

    def family_of(self, rep: str) -> dict[int, tuple[str, str]]:
        """Card hash -> (family_id, label), by the card's canonical node."""
        if rep not in self._family_ix:
            node_fam = {n: (f.family_id, f.label)
                        for f in build_families(self.pack, rep, self.split_reach, self.min_reach)
                        for n in f.nodes}
            self._family_ix[rep] = {
                r["position_hash"]: node_fam[r["canonical_node_id"]]
                for r in self.pack.cards[rep].iter_rows(named=True)
                if r["canonical_node_id"] in node_fam}
        return self._family_ix[rep]

    def _ready_new(self, rep: str, settings: dict) -> list[dict]:
        """New cards, priority order, whose earlier our-positions on the line are
        all learned (or suspended): never a move whose lead-up was never taught."""
        new = sched.new_queue(self.con, self.pack, rep, settings)
        if not new:
            return []
        done = self._started(rep) | {r[0] for r in self.con.execute(
            "SELECT position_hash FROM cards WHERE rep=? AND suspended=1", (rep,))}
        cols = self.pack.tree[rep]

        def ready(card: dict) -> bool:
            path = self.pack.path_to_root(rep, card["canonical_node_id"])[:-1]
            return all(cols["position_hash"][n] in done
                       for n in path if cols["is_our_turn"][n])

        return [c for c in new if ready(c)]

    def _session_seen(self, session_id: int) -> set[int]:
        return {r[0] for r in self.con.execute(
            "SELECT position_hash FROM session_positions WHERE session_id=?",
            (session_id,))}

    def _extend_below(self, rep: str, node_id: int, rng: random.Random,
                      started: set[int], new_ok, main_line: bool) -> list[int]:
        """Continue past the target node to a leaf; returns extra node ids.
        Stops before an our-position not learned yet unless new_ok(hash) admits
        it (reviews admit none: they never reach unseen moves). main_line:
        opponent nodes take their most common reply instead of a sampled one."""
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
                if sum(weights) <= 0:
                    nid = ch[0]
                elif main_line:
                    nid = ch[max(range(len(ch)), key=lambda i: weights[i])]
                else:
                    nid = rng.choices(ch, weights=weights, k=1)[0]
            if cols["is_our_turn"][nid] and cols["position_hash"][nid] not in started:
                if not new_ok(cols["position_hash"][nid]):
                    return out                   # would quiz a move never taught
            out.append(nid)

    def build_drill(self, rep: str, session_id: int, settings: dict) -> dict | None:
        target, kind = self._pick_target(rep, settings, session_id)
        if target is None:
            return None
        fam = self.family_of(rep)
        fam_id, fam_label = fam.get(target["position_hash"], ("", ""))
        blk = self._block.get(session_id)
        if not blk or blk["rep"] != rep or blk["family"] != fam_id:
            blk = self._block[session_id] = {"rep": rep, "family": fam_id, "n": 0}
        blk["n"] += 1
        path = self.pack.path_to_root(rep, target["canonical_node_id"])
        target_idx = len(path) - 1
        rng = random.Random(f"{session_id}:{target['position_hash']}")
        started = self._started(rep)
        if kind == "new":
            # breadth before depth: go on teaching down this line only while the
            # next new move matters as much as the family's best other unlearned card
            # (competitors = the family's other unlearned cards whose lead-up is
            # learned or taught by this very line, so 2...d6 competes with the
            # 3.Bc4 line once 2.Nf3 is in it)
            fam_new = [c for c in sched.new_queue(self.con, self.pack, rep, settings)
                       if fam.get(c["position_hash"], ("",))[0] == fam_id]
            prio = {c["position_hash"]: c["priority"] for c in fam_new}
            cols0 = self.pack.tree[rep]
            taught = started | {target["position_hash"]} | {r[0] for r in self.con.execute(
                "SELECT position_hash FROM cards WHERE rep=? AND suspended=1", (rep,))}
            budget = [int(settings["new_per_day"])
                      - sched.new_introduced_today(self.con, rep) - 1]   # target counts

            def unlocked(c: dict) -> bool:
                path = self.pack.path_to_root(rep, c["canonical_node_id"])[:-1]
                return all(cols0["position_hash"][n] in taught
                           for n in path if cols0["is_our_turn"][n])

            def new_ok(h: int) -> bool:
                if budget[0] <= 0 or h not in prio:          # cap reached / other family
                    return False
                bar = max((c["priority"] for c in fam_new
                           if c["position_hash"] not in taught and c["position_hash"] != h
                           and unlocked(c)), default=0.0)
                if prio[h] < bar:
                    return False
                budget[0] -= 1
                taught.add(h)
                return True
            ext = self._extend_below(rep, path[-1], rng, started, new_ok, True)
        else:
            ext = self._extend_below(rep, path[-1], rng, started, lambda h: False, False)
        path = path + ext

        seen = self._session_seen(session_id)
        cols = self.pack.tree[rep]
        start_idx = 0
        for i, nid in enumerate(path[:target_idx + 1]):
            if cols["position_hash"][nid] in seen:
                start_idx = i                    # deepest seen node up to the target
        # never past the target: a lead-in that auto-plays the target's move
        # leaves the due card ungraded, so the same drill comes back forever

        steps = []
        for i, nid in enumerate(path[1:], start=1):
            parent = path[i - 1]
            steps.append({
                "san": cols["move_san"][nid],
                "our": bool(cols["is_our_turn"][parent]),
                "ph": cols["position_hash"][parent],   # position the move is played FROM
                "epd": cols["epd"][parent],
                "new": bool(cols["is_our_turn"][parent])
                       and cols["position_hash"][parent] not in started,
            })
        while steps and not steps[-1]["our"]:
            steps.pop()                          # trailing opponent moves train nothing
        drill_id = uuid.uuid4().hex[:12]
        self.active[drill_id] = {
            "rep": rep, "session_id": session_id, "steps": steps,
            "graded_plies": set(), "settings": settings, "start_index": start_idx,
            "target_hash": target["position_hash"], "kind": kind,
        }
        # prune finished/abandoned drills so the dict can't grow unbounded
        if len(self.active) > 50:
            for k in list(self.active)[:-50]:
                del self.active[k]
        return {
            "drill_id": drill_id, "rep": rep, "kind": kind,
            "start_index": start_idx,            # steps[:start_index] = lead-in
            "steps": [{"san": s["san"], "our": s["our"], "new": s["new"]} for s in steps],
            "target_ply": len(self.pack.path_to_root(rep, target["canonical_node_id"])) - 1,
            "family": fam_label, "family_drill": blk["n"],
            "drills_per_family": int(settings["drills_per_family"]),
        }

    # ── answering ───────────────────────────────────────────────────────────

    def answer(self, drill_id: str, ply_index: int, san: str,
               fsrs_scheduler, duration_ms: int | None = None) -> dict:
        """Grade the user's move at steps[ply_index]. Only the FIRST answer per
        ply is graded; a card is graded at most once per drill and (setting) at
        most once per local day unless it has come due again."""
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
            # once-per-day stops incidental same-day re-encounters inflating a
            # card, but a card that is DUE again (a learning step after a miss)
            # must be graded, or it stays due and keeps being drilled
            once_per_day = bool(st["settings"]["review_once_per_day"])
            if not (once_per_day and sched.reviewed_today(self.con, card["card_id"])
                    and not sched.is_due(card)):
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

        # done once every our-step from the start is graded: in learn mode new
        # moves are shown first and graded on the replay, out of line order
        ungraded = [i for i in range(st["start_index"], len(steps))
                    if steps[i]["our"] and i not in st["graded_plies"]]
        done = correct and not ungraded
        if done:
            self.active.pop(drill_id, None)
        return {"correct": correct, "expected_san": step["san"],
                "graded": graded, "done": done,
                "rep": st["rep"], "position_hash": step["ph"]}
