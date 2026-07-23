"""FSRS scheduling layer: card persistence, review recording, due/new queues,
and the reach/memo priority score.

All py-fsrs calls live here (pinned fsrs>=5,<6) so an upstream API change is a
one-file fix. Datetimes are stored ISO-8601 UTC; "today" boundaries use local
midnight (SQLite date(..., 'localtime')).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from fsrs import Card, Rating, Scheduler

from .pack import TrainingPack

FSRS_FIELDS = ("state", "step", "stability", "difficulty", "due", "last_review")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_scheduler(settings: dict) -> Scheduler:
    return Scheduler(desired_retention=float(settings["desired_retention"]))


# ── card persistence ────────────────────────────────────────────────────────

def get_card_row(con: sqlite3.Connection, rep: str, position_hash: int):
    cur = con.execute("SELECT * FROM cards WHERE rep=? AND position_hash=?",
                      (rep, position_hash))
    cur.row_factory = sqlite3.Row
    return cur.fetchone()


def get_or_create_card(con: sqlite3.Connection, rep: str, position_hash: int,
                       epd: str | None, best_move: str | None):
    row = get_card_row(con, rep, position_hash)
    if row is not None:
        return row
    with con:
        con.execute(
            "INSERT INTO cards(rep, position_hash, epd, best_move, created_at) "
            "VALUES(?,?,?,?,?)",
            (rep, position_hash, epd, best_move, utcnow_iso()))
    return get_card_row(con, rep, position_hash)


def _row_to_fsrs(row) -> Card:
    if row["state"] is None:                      # never reviewed
        return Card(card_id=row["card_id"])
    return Card.from_dict({
        "card_id": row["card_id"], "state": row["state"], "step": row["step"],
        "stability": row["stability"], "difficulty": row["difficulty"],
        "due": row["due"], "last_review": row["last_review"],
    })


def record_review(con: sqlite3.Connection, scheduler: Scheduler, card_row,
                  rating: Rating, *, when: datetime | None = None,
                  source: str = "train", session_id: int | None = None,
                  game_id: str | None = None,
                  duration_ms: int | None = None) -> None:
    """Apply one FSRS review and persist. Out-of-order guard: a review dated
    before the card's last_review (e.g. an old imported game) is applied at
    now instead — FSRS state never moves backward in time."""
    now = datetime.now(timezone.utc)
    when = when or now
    if card_row["last_review"] is not None:
        last = datetime.fromisoformat(card_row["last_review"])
        if when < last:
            when = now
    card = _row_to_fsrs(card_row)
    card, _log = scheduler.review_card(card, rating, review_datetime=when)
    d = card.to_dict()
    with con:
        con.execute(
            "UPDATE cards SET state=?, step=?, stability=?, difficulty=?, due=?, "
            "last_review=?, reps=reps+1, lapses=lapses+? WHERE card_id=?",
            (d["state"], d["step"], d["stability"], d["difficulty"], d["due"],
             d["last_review"], 1 if rating == Rating.Again else 0,
             card_row["card_id"]))
        con.execute(
            "INSERT INTO review_log(card_id, rating, review_datetime, "
            "review_duration_ms, source, session_id, game_id) VALUES(?,?,?,?,?,?,?)",
            (card_row["card_id"], int(rating), when.isoformat(), duration_ms,
             source, session_id, game_id))


def reviewed_today(con: sqlite3.Connection, card_id: int) -> bool:
    """True if the card already has a graded 'train' review today (local day)."""
    return con.execute(
        "SELECT 1 FROM review_log WHERE card_id=? AND source='train' AND "
        "date(review_datetime, 'localtime') = date('now', 'localtime') LIMIT 1",
        (card_id,)).fetchone() is not None


# ── priority + queues ───────────────────────────────────────────────────────

def priority(reach: float, memo_cost: float | None, max_reach: float,
             a: float, b: float) -> float:
    rn = (reach / max_reach) if max_reach > 0 else 0.0
    return (rn ** a) * ((1.0 + (memo_cost or 0.0)) ** b)


def _prioritized_cards(pack: TrainingPack, rep: str, settings: dict) -> list[dict]:
    """All pack cards with priority attached, sorted descending."""
    cards = pack.cards[rep]
    max_reach = float(cards["reach"][0])          # cards are reach-sorted at build
    a, b = float(settings["reach_exp_a"]), float(settings["memo_exp_b"])
    out = [dict(r, priority=priority(r["reach"], r["memo_cost"], max_reach, a, b))
           for r in cards.iter_rows(named=True)]
    out.sort(key=lambda c: -c["priority"])
    return out


def due_queue(con: sqlite3.Connection, pack: TrainingPack, rep: str,
              settings: dict) -> list[dict]:
    """Due, unsuspended cards that exist in the pack, priority-ordered."""
    rows = con.execute(
        "SELECT position_hash FROM cards WHERE rep=? AND suspended=0 AND "
        "due IS NOT NULL AND due <= ?", (rep, utcnow_iso())).fetchall()
    due_set = {r[0] for r in rows}
    return [c for c in _prioritized_cards(pack, rep, settings)
            if c["position_hash"] in due_set]


def new_queue(con: sqlite3.Connection, pack: TrainingPack, rep: str,
              settings: dict) -> list[dict]:
    """Pack cards never reviewed (no state), priority-ordered."""
    rows = con.execute(
        "SELECT position_hash FROM cards WHERE rep=? AND state IS NOT NULL",
        (rep,)).fetchall()
    seen = {r[0] for r in rows}
    suspended = {r[0] for r in con.execute(
        "SELECT position_hash FROM cards WHERE rep=? AND suspended=1", (rep,))}
    return [c for c in _prioritized_cards(pack, rep, settings)
            if c["position_hash"] not in seen and c["position_hash"] not in suspended]


def new_introduced_today(con: sqlite3.Connection, rep: str) -> int:
    """Cards whose FIRST graded train review happened today (local day)."""
    return con.execute(
        "SELECT COUNT(*) FROM cards c WHERE c.rep=? AND (SELECT MIN(rl.review_datetime) "
        "FROM review_log rl WHERE rl.card_id=c.card_id AND rl.source='train') IS NOT NULL "
        "AND date((SELECT MIN(rl.review_datetime) FROM review_log rl "
        "WHERE rl.card_id=c.card_id AND rl.source='train'), 'localtime') "
        "= date('now', 'localtime')", (rep,)).fetchone()[0]
