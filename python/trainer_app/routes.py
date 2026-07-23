"""FastAPI app factory + routes.

M0 scope: static shell, settings GET/PUT, pack info. Trainer/Explorer/
Deviations routers land in their milestones and register here.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import __version__, config, db
from . import scheduler as sched
from .drills import DrillManager
from .explorer_backend import ExplorerService, render_explorer_html
from .annotations import Annotations
from .lichess_import import ImportService
from .pack import TrainingPack

STATIC_DIR = Path(__file__).parent / "static"


def create_app(data_dir: Path) -> FastAPI:
    app = FastAPI(title="Opening Trainer", version=__version__)
    con: sqlite3.Connection = db.connect(data_dir)
    app.state.data_dir = data_dir
    app.state.con = con

    # Training pack (optional until built). DrillManager only exists with a pack.
    app.state.pack = None
    app.state.drills = None
    try:
        app.state.pack = TrainingPack(data_dir / "pack")
        app.state.drills = DrillManager(app.state.pack, con)
    except FileNotFoundError:
        pass

    def require_pack() -> DrillManager:
        if app.state.drills is None:
            raise HTTPException(409, "no training pack — run build_training_pack.py")
        return app.state.drills

    # Annotations (optional; hot-reloads when the parquet is regenerated).
    app.state.annotations = Annotations(data_dir)

    @app.get("/api/annotation")
    def get_annotation(rep: str, position_hash: int):
        a = app.state.annotations.get(rep, position_hash)
        return a or {}

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/settings")
    def get_settings():
        return config.get_settings(con)

    @app.put("/api/settings")
    def put_settings(updates: dict):
        try:
            return config.put_settings(con, updates)
        except KeyError as e:
            raise HTTPException(400, str(e))

    @app.get("/api/pack/info")
    def pack_info():
        meta_path = data_dir / "pack" / "pack_meta.json"
        if not meta_path.exists():
            return JSONResponse({"built": None,
                                 "detail": "no training pack — run build_training_pack.py"})
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        # Staleness: a source repertoire rebuilt after the pack was = stale pack.
        # (Source absent — e.g. satellite PC without E: — is NOT stale.)
        import time as _time
        stale = []
        for color, info in meta.get("colors", {}).items():
            rp = Path(info.get("rep", ""))
            if rp.exists():
                mtime = _time.strftime("%Y-%m-%d %H:%M:%S",
                                       _time.localtime(rp.stat().st_mtime))
                if mtime > meta.get("built", ""):
                    stale.append(color)
        meta["stale"] = stale
        return meta

    # ── trainer ─────────────────────────────────────────────────────────────

    @app.get("/api/trainer/status")
    def trainer_status(rep: str):
        dm = require_pack()
        settings = config.get_settings(con)
        due = sched.due_queue(con, dm.pack, rep, settings)
        new_today = sched.new_introduced_today(con, rep)
        cap = int(settings["new_per_day"])
        row = con.execute(
            "SELECT session_id, started_at, n_reviews, n_correct FROM sessions "
            "WHERE rep=? AND ended_at IS NULL ORDER BY session_id DESC LIMIT 1",
            (rep,)).fetchone()
        return {
            "rep": rep, "due": len(due),
            "new_total": len(sched.new_queue(con, dm.pack, rep, settings)),
            "new_today": new_today, "new_remaining": max(0, cap - new_today),
            "cards_total": dm.pack.cards[rep].height,
            "active_session": None if row is None else
                {"session_id": row[0], "started_at": row[1],
                 "n_reviews": row[2], "n_correct": row[3]},
        }

    @app.post("/api/trainer/session/start")
    def session_start(body: dict):
        rep = body.get("rep")
        if rep not in ("white", "black"):
            raise HTTPException(400, "rep must be white|black")
        require_pack()
        row = con.execute(
            "SELECT session_id FROM sessions WHERE rep=? AND ended_at IS NULL "
            "ORDER BY session_id DESC LIMIT 1", (rep,)).fetchone()
        if row is not None:                       # idempotent: reuse open session
            return {"session_id": row[0], "resumed": True}
        with con:
            cur = con.execute(
                "INSERT INTO sessions(rep, started_at) VALUES(?,?)",
                (rep, sched.utcnow_iso()))
        return {"session_id": cur.lastrowid, "resumed": False}

    @app.post("/api/trainer/session/end")
    def session_end(body: dict):
        with con:
            con.execute("UPDATE sessions SET ended_at=? WHERE session_id=? "
                        "AND ended_at IS NULL",
                        (sched.utcnow_iso(), body.get("session_id")))
        return {"ok": True}

    @app.get("/api/trainer/drill")
    def next_drill(rep: str, session_id: int):
        dm = require_pack()
        drill = dm.build_drill(rep, session_id, config.get_settings(con))
        if drill is None:
            raise HTTPException(404, "queue empty — no due or new cards")
        return drill

    @app.get("/api/trainer/stats")
    def trainer_stats(rep: str):
        require_pack()
        days = con.execute(
            "SELECT date(review_datetime, 'localtime') d, COUNT(*), "
            "SUM(CASE WHEN rating >= 3 THEN 1 ELSE 0 END) "
            "FROM review_log rl JOIN cards c ON c.card_id = rl.card_id "
            "WHERE c.rep=? AND rl.source='train' AND "
            "rl.review_datetime >= datetime('now', '-30 days') "
            "GROUP BY d ORDER BY d", (rep,)).fetchall()
        forecast = con.execute(
            "SELECT date(due, 'localtime') d, COUNT(*) FROM cards "
            "WHERE rep=? AND suspended=0 AND due IS NOT NULL AND "
            "due < datetime('now', '+7 days') GROUP BY d ORDER BY d",
            (rep,)).fetchall()
        totals = con.execute(
            "SELECT COUNT(*), SUM(lapses), SUM(reps) FROM cards "
            "WHERE rep=? AND state IS NOT NULL", (rep,)).fetchone()
        hardest = con.execute(
            "SELECT epd, best_move, lapses, reps FROM cards WHERE rep=? AND "
            "lapses > 0 ORDER BY lapses DESC, reps ASC LIMIT 10", (rep,)).fetchall()
        n_rev = sum(r[1] for r in days)
        n_good = sum(r[2] for r in days)
        return {
            "days": [{"date": d, "reviews": n, "correct": g} for d, n, g in days],
            "due_forecast": [{"date": d, "count": n} for d, n in forecast],
            "cards_started": totals[0], "total_lapses": totals[1] or 0,
            "total_reps": totals[2] or 0,
            "retention_30d": (n_good / n_rev) if n_rev else None,
            "hardest": [{"epd": e, "best_move": b, "lapses": l, "reps": r}
                        for e, b, l, r in hardest],
        }

    @app.post("/api/trainer/card/{card_id}/suspend")
    def suspend(card_id: int):
        with con:
            con.execute("UPDATE cards SET suspended=1 WHERE card_id=?", (card_id,))
        return {"ok": True}

    @app.post("/api/trainer/card/{card_id}/unsuspend")
    def unsuspend(card_id: int):
        with con:
            con.execute("UPDATE cards SET suspended=0 WHERE card_id=?", (card_id,))
        return {"ok": True}

    @app.post("/api/trainer/answer")
    def answer(body: dict):
        dm = require_pack()
        settings = config.get_settings(con)
        result = dm.answer(body["drill_id"], int(body["ply"]), body["san"],
                           sched.make_scheduler(settings),
                           duration_ms=body.get("duration_ms"))
        if "error" in result:
            raise HTTPException(400, result["error"])
        # attach the annotation for the position just answered (post-answer reveal)
        if result.get("position_hash") is not None:
            result["annotation"] = app.state.annotations.get(
                result["rep"], result["position_hash"])
        return result

    # ── explorer ────────────────────────────────────────────────────────────

    svc = ExplorerService()
    app.state.explorer_service = svc

    @app.get("/api/explorer/status")
    def explorer_status():
        return {"status": svc.status, "error": svc.error}

    @app.post("/api/explorer/load")
    def explorer_load():
        return {"status": svc.start_load(config.get_settings(con))}

    def require_explorer():
        if svc.status != "ready":
            raise HTTPException(409, f"explorer not ready (status: {svc.status})")
        return svc.explorer

    @app.get("/explorer")
    def explorer_page():
        require_explorer()
        from fastapi.responses import HTMLResponse
        return HTMLResponse(render_explorer_html(svc.source_label, svc.crush_weight))

    # Original repertoire_explorer API paths, as INDEX_HTML expects them.
    @app.get("/api/slices")
    def api_slices():
        return require_explorer().list_slices()

    @app.get("/api/slice_info")
    def api_slice_info(label: str, event: str, elo_band: int):
        return require_explorer().slice_info(label, event, elo_band)

    @app.get("/api/position")
    def api_position(label: str, event: str, elo_band: int, epd: str):
        out = require_explorer().get_position(label, event, elo_band, epd)
        if out is None:
            raise HTTPException(404, "slice or position not found")
        # annotation for this position (our-turn positions are the cards)
        out["annotation"] = app.state.annotations.get(
            label.lower(), out.get("position_hash"))
        return out

    # ── deviations ──────────────────────────────────────────────────────────

    imp = ImportService(app.state.pack, data_dir / "trainer.db")
    app.state.import_service = imp

    @app.post("/api/deviations/import")
    def deviations_import():
        ok, msg = imp.start(config.get_settings(con))
        if not ok:
            raise HTTPException(409, msg)
        return {"status": msg}

    @app.get("/api/deviations/import/status")
    def import_status():
        cur = con.execute("SELECT * FROM import_runs ORDER BY id DESC LIMIT 1")
        cur.row_factory = sqlite3.Row
        row = cur.fetchone()
        return dict(row) if row else {"status": "never run"}

    @app.get("/api/deviations")
    def list_deviations(rep: str | None = None, include_dismissed: bool = False):
        q = ("SELECT d.*, g.played_at, g.opponent, g.speed, g.result, "
             "(SELECT COUNT(*) FROM deviations d2 WHERE d2.position_hash = "
             "d.position_hash AND d2.rep = d.rep) AS times_here "
             "FROM deviations d JOIN lichess_games g ON g.game_id = d.game_id "
             "WHERE 1=1")
        args: list = []
        if rep in ("white", "black"):
            q += " AND d.rep = ?"
            args.append(rep)
        if not include_dismissed:
            q += " AND d.dismissed = 0"
        q += " ORDER BY g.played_at DESC LIMIT 500"
        cur = con.execute(q, args)
        cur.row_factory = sqlite3.Row
        return [dict(r) for r in cur.fetchall()]

    @app.get("/api/deviations/{dev_id}/game")
    def deviation_game(dev_id: int):
        row = con.execute(
            "SELECT d.ply, d.game_id, g.moves_san, g.color FROM deviations d "
            "JOIN lichess_games g ON g.game_id = d.game_id WHERE d.id = ?",
            (dev_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "deviation not found")
        return {"ply": row[0], "game_id": row[1], "moves_san": row[2],
                "color": row[3]}

    @app.post("/api/deviations/{dev_id}/dismiss")
    def dismiss_deviation(dev_id: int):
        with con:
            con.execute("UPDATE deviations SET dismissed=1 WHERE id=?", (dev_id,))
        return {"ok": True}

    return app
