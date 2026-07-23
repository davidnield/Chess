"""SQLite connection + schema for trainer.db.

Single-file DB inside the data dir; WAL mode so the FastAPI worker threads can
read while a write is in flight. Migrations are append-only entries in
MIGRATIONS keyed by target schema_version — never edit an applied entry.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

DDL_V1 = """
CREATE TABLE IF NOT EXISTS schema_version(version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS settings(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL              -- JSON-encoded
);

CREATE TABLE IF NOT EXISTS cards(
    card_id       INTEGER PRIMARY KEY,
    rep           TEXT NOT NULL CHECK (rep IN ('white','black')),
    position_hash INTEGER NOT NULL,
    epd           TEXT,
    best_move     TEXT,
    -- py-fsrs Card state (Card.to_dict round-trip; datetimes ISO-8601 UTC)
    state         INTEGER,
    step          INTEGER,
    stability     REAL,
    difficulty    REAL,
    due           TEXT,
    last_review   TEXT,
    reps          INTEGER NOT NULL DEFAULT 0,
    lapses        INTEGER NOT NULL DEFAULT 0,
    suspended     INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    UNIQUE(rep, position_hash)
);
CREATE INDEX IF NOT EXISTS idx_cards_due ON cards(rep, suspended, due);

CREATE TABLE IF NOT EXISTS review_log(          -- append-only: FSRS optimizer input
    id               INTEGER PRIMARY KEY,
    card_id          INTEGER NOT NULL REFERENCES cards(card_id),
    rating           INTEGER NOT NULL,           -- 1=Again, 3=Good (2/4 reserved)
    review_datetime  TEXT NOT NULL,
    review_duration_ms INTEGER,
    source           TEXT NOT NULL CHECK (source IN ('train','deviation')),
    session_id       INTEGER,
    game_id          TEXT
);
CREATE INDEX IF NOT EXISTS idx_review_log_card ON review_log(card_id, review_datetime);

CREATE TABLE IF NOT EXISTS sessions(
    session_id INTEGER PRIMARY KEY,
    rep        TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at   TEXT,
    n_reviews  INTEGER NOT NULL DEFAULT 0,
    n_correct  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS session_positions(   -- powers session-ancestor drill starts
    session_id    INTEGER NOT NULL REFERENCES sessions(session_id),
    position_hash INTEGER NOT NULL,
    first_seen    TEXT NOT NULL,
    PRIMARY KEY(session_id, position_hash)
);

CREATE TABLE IF NOT EXISTS lichess_games(
    game_id     TEXT PRIMARY KEY,
    played_at   TEXT,
    color       TEXT,
    speed       TEXT,
    rated       INTEGER,
    opponent    TEXT,
    opp_rating  INTEGER,
    my_rating   INTEGER,
    result      TEXT,
    moves_san   TEXT,
    imported_at TEXT NOT NULL,
    scanned     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS deviations(
    id            INTEGER PRIMARY KEY,
    game_id       TEXT NOT NULL REFERENCES lichess_games(game_id),
    rep           TEXT NOT NULL,
    position_hash INTEGER NOT NULL,
    epd           TEXT,
    ply           INTEGER,
    expected_move TEXT NOT NULL,
    played_move   TEXT NOT NULL,
    reach         REAL,
    memo_cost     REAL,
    fsrs_applied  INTEGER NOT NULL DEFAULT 0,
    dismissed     INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deviations_pos ON deviations(rep, position_hash);

CREATE TABLE IF NOT EXISTS import_runs(
    id           INTEGER PRIMARY KEY,
    started_at   TEXT NOT NULL,
    since        TEXT,
    until        TEXT,
    n_games      INTEGER NOT NULL DEFAULT 0,
    n_deviations INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL,                 -- running | done | error
    error        TEXT
);
"""

MIGRATIONS: dict[int, str] = {1: DDL_V1}
SCHEMA_VERSION = max(MIGRATIONS)


def connect(data_dir: Path) -> sqlite3.Connection:
    """Open (creating/migrating as needed) trainer.db in the data dir."""
    con = sqlite3.connect(data_dir / "trainer.db", check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    row = con.execute(
        "SELECT version FROM schema_version"
        if con.execute("SELECT 1 FROM sqlite_master WHERE name='schema_version'").fetchone()
        else "SELECT 0 WHERE 1=0"
    ).fetchone()
    current = row[0] if row else 0
    for v in sorted(MIGRATIONS):
        if v > current:
            with con:
                con.executescript(MIGRATIONS[v])
                con.execute("DELETE FROM schema_version")
                con.execute("INSERT INTO schema_version(version) VALUES(?)", (v,))
    return con
