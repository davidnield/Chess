"""M2 synthetic test: pack build_tree/build_cards + DrillManager grading +
FSRS scheduling, on a tiny hand-built repertoire (imports production logic,
per repo convention — no HTTP, no real data).

Run:  .venv/Scripts/python.exe python/_test_trainer_m2.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")

import chess
import polars as pl
from fsrs import Rating

from trainer_app import config, db
from trainer_app import scheduler as sched
from trainer_app.build_training_pack import TREE_SCHEMA, build_cards, build_tree
from trainer_app.drills import DrillManager
from trainer_app.pack import TrainingPack
from zobrist import zobrist_int64

FAILS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAILS
    mark = "ok " if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILS += 1


def h(*sans: str) -> int:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return zobrist_int64(b)


# ── synthetic repertoire: 1.e4 (e5 2.Nf3 | c5 2.Nc3) ───────────────────────
best = {h(): "e4", h("e4", "e5"): "Nf3", h("e4", "c5"): "Nc3"}
memo = {h(): 0.05, h("e4", "e5"): 0.02, h("e4", "c5"): 0.08}
edges = {h("e4"): [("e5", h("e4", "e5"), 60), ("c5", h("e4", "c5"), 40)]}

print("build_tree / build_cards:")
tree = build_tree(best, edges, "white", min_reach=1e-6, max_branches=12, max_ply=10)
cards = build_cards(tree, memo)
check("tree node count = 6 (root,e4,e5,c5,Nf3,Nc3)", len(tree) == 6, str(len(tree)))
check("card count = 3", len(cards) == 3, str(len(cards)))
by_hash = {c["position_hash"]: c for c in cards}
check("root card reach 1.0", abs(by_hash[h()]["reach"] - 1.0) < 1e-12)
check("e5-node card reach 0.6", abs(by_hash[h("e4", "e5")]["reach"] - 0.6) < 1e-12)
check("memo_cost joined", by_hash[h("e4", "c5")]["memo_cost"] == 0.08)

# ── write a pack + temp DB ──────────────────────────────────────────────────
tmp = Path(tempfile.mkdtemp(prefix="trainer_m2_"))
pack_dir = tmp / "pack"
pack_dir.mkdir()
for color in ("white", "black"):
    # black side gets the same tiny tree (content irrelevant; loader needs both)
    pl.DataFrame(tree, schema=TREE_SCHEMA).write_parquet(pack_dir / f"{color}_tree.parquet")
    pl.DataFrame(cards).write_parquet(pack_dir / f"{color}_cards.parquet")
    (pl.DataFrame({"position_hash": sorted(best),
                   "best_move": [best[k] for k in sorted(best)]})
       .write_parquet(pack_dir / f"dev_lookup_{color}.parquet"))
(pack_dir / "pack_meta.json").write_text(json.dumps({"built": "test"}), encoding="utf-8")

pack = TrainingPack(pack_dir)
con = db.connect(tmp)
settings = config.get_settings(con)
fsrs = sched.make_scheduler(settings)
dm = DrillManager(pack, con)
with con:
    cur = con.execute("INSERT INTO sessions(rep, started_at) VALUES('white', ?)",
                      (sched.utcnow_iso(),))
sid = cur.lastrowid

# ── drill construction ──────────────────────────────────────────────────────
print("\ndrill construction:")
d = dm.build_drill("white", sid, settings)
check("drill returned", d is not None)
check("kind = new (nothing due yet)", d["kind"] == "new")
check("starts at root", d["start_index"] == 0)
check("first step = our e4", d["steps"][0]["san"] == "e4" and d["steps"][0]["our"])
check("3 plies (e4, reply, our answer)", len(d["steps"]) == 3, str(d["steps"]))
check("ply1 is opponent", not d["steps"][1]["our"])

# ── grading: wrong then right ───────────────────────────────────────────────
print("\ngrading:")
res = dm.answer(d["drill_id"], 0, "d4", fsrs)
check("wrong answer flagged", res["correct"] is False)
check("expected_san = e4", res["expected_san"] == "e4")
check("graded on first attempt", res["graded"] is True)
row = sched.get_card_row(con, "white", h())
check("lapse recorded", row["lapses"] == 1, str(dict(row)))
check("card state set", row["state"] is not None)

res2 = dm.answer(d["drill_id"], 0, "e4", fsrs)
check("second attempt not re-graded", res2["graded"] is False)

res3 = dm.answer(d["drill_id"], 2, d["steps"][2]["san"], fsrs)
check("correct mid-line answer graded Good", res3["correct"] and res3["graded"])
check("drill done after last our-move", res3["done"] is True)
ply2_hash = h("e4", "e5") if d["steps"][1]["san"] == "e5" else h("e4", "c5")
row2 = sched.get_card_row(con, "white", ply2_hash)
check("Good review: 0 lapses, reps=1", row2["lapses"] == 0 and row2["reps"] == 1)

logs = con.execute("SELECT card_id, rating, source FROM review_log").fetchall()
check("review_log has exactly 2 rows", len(logs) == 2, str(logs))
check("ratings are Again(1) then Good(3)", [r[1] for r in logs] == [1, 3])

# ── once-per-day rule ───────────────────────────────────────────────────────
print("\nonce-per-day rule:")
check("reviewed_today true after grade", sched.reviewed_today(con, row["card_id"]))
d2 = dm.build_drill("white", sid, settings)
check("second drill exists", d2 is not None)
res4 = dm.answer(d2["drill_id"], 0, "e4", fsrs)
check("same-day re-encounter not graded", res4["graded"] is False)
n_logs = con.execute("SELECT COUNT(*) FROM review_log").fetchone()[0]
check("review_log unchanged (once-per-day blocked the regrade)", n_logs == 2, str(n_logs))
check("drill 2 starts at root (only shared seen position IS the root)",
      d2["start_index"] == 0, f"start_index={d2['start_index']}")

# ── forged multi-day FSRS behavior + out-of-order guard ────────────────────
print("\nFSRS time behavior:")
now = datetime.now(timezone.utc)
card = sched.get_or_create_card(con, "black", 999999, "fake", "Nf3")
sched.record_review(con, fsrs, card, Rating.Good, when=now - timedelta(days=30))
card = sched.get_card_row(con, "black", 999999)
sched.record_review(con, fsrs, card, Rating.Good, when=now - timedelta(days=20))
card = sched.get_card_row(con, "black", 999999)
due_after_2 = datetime.fromisoformat(card["due"])
check("2 spaced Goods -> due in the future of review time",
      due_after_2 > now - timedelta(days=20))
stab_before = card["stability"]
# out-of-order: a review dated BEFORE last_review must apply at now, not then
sched.record_review(con, fsrs, card, Rating.Again, when=now - timedelta(days=25))
card = sched.get_card_row(con, "black", 999999)
check("out-of-order review applied at now",
      datetime.fromisoformat(card["last_review"]) >= now - timedelta(minutes=1))
check("Again dropped stability", card["stability"] < stab_before,
      f"{card['stability']} vs {stab_before}")

# ── due queue after forged past-due ────────────────────────────────────────
print("\nqueues:")
con.execute("UPDATE cards SET due=? WHERE rep='white' AND position_hash=?",
            ((now - timedelta(hours=1)).isoformat(), h()))
con.commit()
dq = sched.due_queue(con, pack, "white", settings)
check("forged past-due card appears in due queue",
      any(c["position_hash"] == h() for c in dq))
d3 = dm.build_drill("white", sid, settings)
check("due card becomes drill target", d3 is not None and d3["kind"] == "due")

# ── new-card daily cap ─────────────────────────────────────────────────────
cap_settings = dict(settings, new_per_day=0)
con.execute("UPDATE cards SET due=? WHERE rep='white'",
            ((now + timedelta(days=1)).isoformat(),))
con.commit()
d4 = dm.build_drill("white", sid, cap_settings)
check("cap 0 + nothing due -> no drill", d4 is None)

# ── session-ancestor start ─────────────────────────────────────────────────
print("\nsession-ancestor:")
# Only the e5-node card is due; its position was already seen this session ->
# the drill must start AT that position (lead-in e4 e5 animated, quiz Nf3).
con.execute("UPDATE cards SET due=? WHERE rep='white' AND position_hash=?",
            ((now - timedelta(hours=1)).isoformat(), h("e4", "e5")))
con.execute("INSERT OR IGNORE INTO session_positions(session_id, position_hash, "
            "first_seen) VALUES(?,?,?)", (sid, h("e4", "e5"), sched.utcnow_iso()))
con.commit()
d5 = dm.build_drill("white", sid, cap_settings)
check("due drill built", d5 is not None and d5["kind"] == "due")
check("starts at the seen ancestor (start_index=2)", d5["start_index"] == 2,
      f"start_index={d5['start_index']}")
check("line = e4, e5, Nf3", [s["san"] for s in d5["steps"]] == ["e4", "e5", "Nf3"],
      str(d5["steps"]))

# ── M3: extend_past_cap truncation ─────────────────────────────────────────
print("\nextend_past_cap:")
with con:
    con.execute("DELETE FROM review_log WHERE card_id IN "
                "(SELECT card_id FROM cards WHERE rep='white' AND position_hash != ?)",
                (h(),))
    con.execute("DELETE FROM cards WHERE rep='white' AND position_hash != ?", (h(),))
    con.execute("UPDATE cards SET due=? WHERE rep='white'",
                ((now - timedelta(hours=1)).isoformat(),))
    con.execute("DELETE FROM session_positions")
trunc_settings = dict(settings, new_per_day=0, extend_past_cap=False)
d6 = dm.build_drill("white", sid, trunc_settings)
check("truncated drill built", d6 is not None)
check("line stops before un-carded our-node (only e4 graded)",
      [s["san"] for s in d6["steps"]] == ["e4"], str(d6 and d6["steps"]))
loose_settings = dict(settings, new_per_day=0, extend_past_cap=True)
d7 = dm.build_drill("white", sid, loose_settings)
check("extend_past_cap=True keeps the full line", len(d7["steps"]) == 3,
      str(d7 and d7["steps"]))

# ── M3: stats endpoint vs hand-computed ────────────────────────────────────
print("\nstats endpoint:")
from fastapi.testclient import TestClient
from trainer_app.routes import create_app

app = create_app(tmp)                      # same tmp data dir (pack + DB)
client = TestClient(app)
r = client.get("/api/trainer/stats?rep=white")
check("stats 200", r.status_code == 200, str(r.status_code))
s = r.json()
con2 = app.state.con
n_rev = con2.execute(
    "SELECT COUNT(*) FROM review_log rl JOIN cards c ON c.card_id=rl.card_id "
    "WHERE c.rep='white' AND rl.source='train'").fetchone()[0]
n_good = con2.execute(
    "SELECT COUNT(*) FROM review_log rl JOIN cards c ON c.card_id=rl.card_id "
    "WHERE c.rep='white' AND rl.source='train' AND rating >= 3").fetchone()[0]
got = sum(d["reviews"] for d in s["days"])
check("day-bucket reviews match hand SQL", got == n_rev, f"{got} vs {n_rev}")
exp_ret = (n_good / n_rev) if n_rev else None
check("retention matches hand SQL",
      (s["retention_30d"] is None and exp_ret is None) or
      abs(s["retention_30d"] - exp_ret) < 1e-12,
      f"{s['retention_30d']} vs {exp_ret}")
n_due7 = con2.execute(
    "SELECT COUNT(*) FROM cards WHERE rep='white' AND suspended=0 AND due IS NOT "
    "NULL AND due < datetime('now', '+7 days')").fetchone()[0]
check("forecast total matches hand SQL",
      sum(d["count"] for d in s["due_forecast"]) == n_due7)

# suspend removes from due queue
card_row = sched.get_card_row(con2, "white", h())
client.post(f"/api/trainer/card/{card_row['card_id']}/suspend")
dq2 = sched.due_queue(con2, pack, "white", settings)
check("suspended card leaves due queue",
      not any(c["position_hash"] == h() for c in dq2))
client.post(f"/api/trainer/card/{card_row['card_id']}/unsuspend")
dq3 = sched.due_queue(con2, pack, "white", settings)
check("unsuspend restores it", any(c["position_hash"] == h() for c in dq3))

print(f"\n{'ALL PASS' if FAILS == 0 else f'{FAILS} FAILURES'}")
sys.exit(1 if FAILS else 0)
