"""Lichess game import + repertoire-deviation scan.

Fetch: GET https://lichess.org/api/games/user/{username} (public API, NDJSON
stream), rated blitz/rapid/classical standard games since the user-configured
date. Idempotent: game_id is the primary key, re-imports upsert.

Scan: replay each unscanned game's SAN moves with python-chess; before each of
the USER's moves probe the full-book dev_lookup (pack.book_move). In book and
played != expected -> record a deviation and apply an FSRS "Again" review dated
at the game's timestamp (scheduler's out-of-order guard protects newer state).
Scanning continues after a deviation — transpositions can re-enter book.

Runs in a daemon thread owned by ImportService; import_runs rows carry
progress/status for UI polling.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone

import chess
import requests

from . import scheduler as sched
from .pack import TrainingPack
from zobrist import zobrist_int64

API = "https://lichess.org/api/games/user/{username}"
PERFS = "blitz,rapid,classical"
SKIP_STATUSES = {"aborted", "noStart", "unknownFinish"}


def _iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def fetch_games(con: sqlite3.Connection, settings: dict, run_id: int) -> int:
    """Stream the user's games into lichess_games. Returns count fetched."""
    username = settings["lichess_username"].strip()
    since = settings["import_since"].strip()
    params: dict = {"rated": "true", "perfType": PERFS, "moves": "true"}
    if since:
        dt = datetime.fromisoformat(since).replace(tzinfo=timezone.utc)
        params["since"] = int(dt.timestamp() * 1000)
    headers = {"Accept": "application/x-ndjson"}
    token = settings.get("lichess_token", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    n = 0
    uname = username.lower()
    with requests.get(API.format(username=username), params=params,
                      headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            if not line:
                continue
            g = json.loads(line)
            if g.get("variant", "standard") != "standard":
                continue
            if g.get("status") in SKIP_STATUSES or not g.get("moves"):
                continue
            players = g.get("players", {})
            w_user = ((players.get("white") or {}).get("user") or {})
            b_user = ((players.get("black") or {}).get("user") or {})
            if (w_user.get("id") or "").lower() == uname:
                color, opp = "white", b_user.get("name")
                my_r = (players.get("white") or {}).get("rating")
                opp_r = (players.get("black") or {}).get("rating")
            elif (b_user.get("id") or "").lower() == uname:
                color, opp = "black", w_user.get("name")
                my_r = (players.get("black") or {}).get("rating")
                opp_r = (players.get("white") or {}).get("rating")
            else:
                continue                       # anonymous side / not our user
            result = g.get("winner") or ("draw" if g.get("status") in
                                         ("draw", "stalemate") else g.get("status"))
            with con:
                con.execute(
                    "INSERT INTO lichess_games(game_id, played_at, color, speed, "
                    "rated, opponent, opp_rating, my_rating, result, moves_san, "
                    "imported_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(game_id) DO NOTHING",
                    (g["id"], _iso(g.get("createdAt")), color, g.get("speed"),
                     1 if g.get("rated") else 0, opp, opp_r, my_r, result,
                     g["moves"], sched.utcnow_iso()))
            n += 1
            if n % 50 == 0:
                with con:
                    con.execute("UPDATE import_runs SET n_games=? WHERE id=?",
                                (n, run_id))
    with con:
        con.execute("UPDATE import_runs SET n_games=? WHERE id=?", (n, run_id))
    return n


def scan_games(con: sqlite3.Connection, pack: TrainingPack,
               settings: dict) -> int:
    """Deviation-scan every unscanned game. Returns deviations found."""
    fsrs = sched.make_scheduler(settings)
    rows = con.execute(
        "SELECT game_id, played_at, color, moves_san FROM lichess_games "
        "WHERE scanned=0 ORDER BY played_at").fetchall()
    n_dev = 0
    for game_id, played_at, color, moves_san in rows:
        board = chess.Board()
        user_is_white = (color == "white")
        when = (datetime.fromisoformat(played_at)
                if played_at else datetime.now(timezone.utc))
        for san in moves_san.split():
            users_move = (board.turn == chess.WHITE) == user_is_white
            if users_move:
                ph = zobrist_int64(board)
                expected = pack.book_move(color, ph)
                try:
                    mv = board.parse_san(san)
                except ValueError:
                    break                       # unparsable tail — stop this game
                if expected is not None:
                    played = board.san(mv)      # normalized SAN
                    if played != expected:
                        card_meta = pack.card(color, ph)
                        with con:
                            con.execute(
                                "INSERT INTO deviations(game_id, rep, position_hash, "
                                "epd, ply, expected_move, played_move, reach, "
                                "memo_cost, fsrs_applied, created_at) "
                                "VALUES(?,?,?,?,?,?,?,?,?,1,?)",
                                (game_id, color, ph, board.epd(), board.ply(),
                                 expected, played,
                                 card_meta["reach"] if card_meta else None,
                                 card_meta["memo_cost"] if card_meta else None,
                                 sched.utcnow_iso()))
                        card = sched.get_or_create_card(con, color, ph,
                                                        board.epd(), expected)
                        sched.record_review(con, fsrs, card, sched.Rating.Again,
                                            when=when, source="deviation",
                                            game_id=game_id)
                        n_dev += 1
                board.push(mv)
            else:
                try:
                    board.push_san(san)
                except ValueError:
                    break
        with con:
            con.execute("UPDATE lichess_games SET scanned=1 WHERE game_id=?",
                        (game_id,))
    return n_dev


class ImportService:
    """One import at a time; status via the import_runs table."""

    def __init__(self, pack: TrainingPack | None, db_path):
        self.pack = pack
        self.db_path = db_path                  # thread opens its own connection
        self._running = False
        self._lock = threading.Lock()

    def start(self, settings: dict) -> tuple[bool, str]:
        if self.pack is None:
            return False, "no training pack — run build_training_pack.py"
        if not settings["lichess_username"].strip():
            return False, "set your Lichess username in Settings first"
        with self._lock:
            if self._running:
                return False, "an import is already running"
            self._running = True
        threading.Thread(target=self._run, args=(settings,), daemon=True).start()
        return True, "started"

    def _run(self, settings: dict) -> None:
        con = sqlite3.connect(self.db_path, check_same_thread=False)
        con.execute("PRAGMA foreign_keys=ON")
        try:
            with con:
                cur = con.execute(
                    "INSERT INTO import_runs(started_at, since, status) "
                    "VALUES(?,?, 'running')",
                    (sched.utcnow_iso(), settings["import_since"]))
            run_id = cur.lastrowid
            n_games = fetch_games(con, settings, run_id)
            n_dev = scan_games(con, self.pack, settings)
            with con:
                con.execute(
                    "UPDATE import_runs SET status='done', until=?, n_games=?, "
                    "n_deviations=? WHERE id=?",
                    (sched.utcnow_iso(), n_games, n_dev, run_id))
        except Exception as e:
            with con:
                con.execute(
                    "UPDATE import_runs SET status='error', error=? WHERE "
                    "id=(SELECT MAX(id) FROM import_runs)",
                    (f"{type(e).__name__}: {e}",))
        finally:
            con.close()
            self._running = False
