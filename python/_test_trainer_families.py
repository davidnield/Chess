"""Opening-family blocks and breadth-first teaching (trainer_app.families + drills).

Tree (White): 1.e4; 1...e5 (50%) 2.Nf3, then 2...Nc6 (55%) 3.Bc4 [3...Nf6 (70%) 4.d4,
3...Bc5 (30%) 4.c3] and 2...d6 (45%) 3.d4; 1...c5 (35%) 2.d4; 1...e6 (15%) 2.d4.
Card reach: e4 1, Nf3 .5, c5:d4 .35, Bc4 .275, d6:d4 .225, Nf6:d4 .1925,
e6:d4 .15, Bc5:c3 .0825. With split_reach 0.6 the families are 1.e4 (other moves),
1.e4 e5 (five cards), 1.e4 c5 and 1.e4 e6. Imports the production code.

Run:  .venv/Scripts/python.exe python/_test_trainer_families.py
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
from trainer_app.families import build_families
from trainer_app.pack import TrainingPack
from zobrist import zobrist_int64

FAILS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAILS
    print(f"  [{'ok ' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
    FAILS += not cond


def h(*sans: str) -> int:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return zobrist_int64(b)


E5, C5, E6 = ("e4", "e5"), ("e4", "c5"), ("e4", "e6")
NC6, D6 = E5 + ("Nf3", "Nc6"), E5 + ("Nf3", "d6")
NF6, BC5 = NC6 + ("Bc4", "Nf6"), NC6 + ("Bc4", "Bc5")
best = {h(): "e4", h(*E5): "Nf3", h(*C5): "d4", h(*E6): "d4", h(*NC6): "Bc4",
        h(*D6): "d4", h(*NF6): "d4", h(*BC5): "c3"}
edges = {h("e4"): [("e5", h(*E5), 50), ("c5", h(*C5), 35), ("e6", h(*E6), 15)],
         h(*E5, "Nf3"): [("Nc6", h(*NC6), 55), ("d6", h(*D6), 45)],
         h(*NC6, "Bc4"): [("Nf6", h(*NF6), 70), ("Bc5", h(*BC5), 30)]}
NAME = {h(): "e4", h(*E5): "Nf3", h(*C5): "c5:d4", h(*E6): "e6:d4", h(*NC6): "Bc4",
        h(*D6): "d6:d4", h(*NF6): "Nf6:d4", h(*BC5): "Bc5:c3"}

tree = build_tree(best, edges, "white", min_reach=1e-6, max_branches=12, max_ply=12)
cards = build_cards(tree, {})
tmp = Path(tempfile.mkdtemp(prefix="trainer_fam_"))
pack_dir = tmp / "pack"
pack_dir.mkdir()
for color in ("white", "black"):
    pl.DataFrame(tree, schema=TREE_SCHEMA).write_parquet(pack_dir / f"{color}_tree.parquet")
    pl.DataFrame(cards).write_parquet(pack_dir / f"{color}_cards.parquet")
    (pl.DataFrame({"position_hash": sorted(best), "best_move": [best[k] for k in sorted(best)]})
       .write_parquet(pack_dir / f"dev_lookup_{color}.parquet"))
(pack_dir / "pack_meta.json").write_text(json.dumps({"built": "test"}), encoding="utf-8")
pack = TrainingPack(pack_dir)

print("families:")
fams = build_families(pack, "white", split_reach=0.6, min_reach=0.02)
labels = sorted(f.label for f in fams)
check("split at 0.6: 1.e4 (other moves), 1...e5 / c5 / e6 (labels include our reply)",
      labels == ["1.e4 (other moves)", "1.e4 c5 2.d4", "1.e4 e5 2.Nf3", "1.e4 e6 2.d4"], str(labels))
fine = sorted(f.label for f in build_families(pack, "white", split_reach=0.10, min_reach=0.02))
check("split at 0.10: 1...e5 splits at the 2nd and 3rd moves",
      "1.e4 e5 2.Nf3 Nc6 3.Bc4 (other moves)" in fine and "1.e4 e5 2.Nf3 d6 3.d4" in fine
      and "1.e4 e5 2.Nf3 Nc6 3.Bc4 Nf6 4.d4" in fine, str(fine))
card_nodes = {c["canonical_node_id"] for c in cards}
covered = [n for f in fams for n in f.nodes if n in card_nodes]
check("every card is in exactly one family", sorted(covered) == sorted(card_nodes))


def fresh(settings_extra: dict):
    d = Path(tempfile.mkdtemp(prefix="trainer_fam_db_"))
    (d / "pack").mkdir()
    con = db.connect(d)
    dm = DrillManager(pack, con)
    dm.split_reach = 0.6
    with con:
        sid = con.execute("INSERT INTO sessions(rep, started_at) VALUES('white', ?)",
                          (sched.utcnow_iso(),)).lastrowid
    st = dict(config.get_settings(con), **settings_extra)
    return con, dm, sid, st, sched.make_scheduler(st)


print("\nreview blocks (everything due, 3 drills per family):")
con, dm, sid, st, fsrs = fresh({"drills_per_family": 3, "new_per_day": 0})
now = datetime.now(timezone.utc)
for c in cards:                                   # every card learned and due
    row = sched.get_or_create_card(con, "white", c["position_hash"], c["epd"], c["best_move"])
    sched.record_review(con, fsrs, row, Rating.Good, when=now - timedelta(days=10))
with con:
    con.execute("UPDATE cards SET due=?", ((now - timedelta(hours=1)).isoformat(),))
order = []
for _ in range(len(cards)):
    d = dm.build_drill("white", sid, st)
    tgt = dm.active[d["drill_id"]]["target_hash"]
    order.append(NAME[tgt])
    with con:                                     # reviewed: no longer due
        con.execute("UPDATE cards SET due=? WHERE position_hash=?",
                    ((now + timedelta(days=5)).isoformat(), tgt))
check("drills stay in a family for 3 before moving on (Bc4 before the c5 card)",
      order == ["e4", "Nf3", "Bc4", "d6:d4", "c5:d4", "Nf6:d4", "Bc5:c3", "e6:d4"], str(order))
check("payload names the family and the block position",
      d["family"] == "1.e4 e6 2.d4" and d["family_drill"] == 1 and d["drills_per_family"] == 3, str(d))

print("\nnew lines: breadth before depth:")
con, dm, sid, st, fsrs = fresh({"drills_per_family": 3, "new_per_day": 20})


def learn(d: dict) -> None:
    for i, s in enumerate(d["steps"]):
        if s["our"] and i >= d["start_index"]:
            dm.answer(d["drill_id"], i, s["san"], fsrs)


d1 = dm.build_drill("white", sid, st)
check("first new line: 1.e4 alone (its family has nothing else)",
      [s["san"] for s in d1["steps"]] == ["e4"], str(d1["steps"]))
learn(d1)
d2 = dm.build_drill("white", sid, st)            # 1...e5 family: Nf3, then 3.Bc4
learn(d2)
check("Nf3 line teaches 3.Bc4 too (it outranks every other unlearned card there)",
      [s["san"] for s in d2["steps"] if s["new"]] == ["Nf3", "Bc4"], str(d2["steps"]))
d3 = dm.build_drill("white", sid, st)
new3 = [s["san"] for s in d3["steps"] if s["new"]]
check("next: the common 2...d6 answer, not 4.d4 deep in the 3.Bc4 line",
      d3["kind"] == "new" and new3 == ["d4"]
      and [s["san"] for s in d3["steps"]][:4] == ["e4", "e5", "Nf3", "d6"], str(d3["steps"]))
learn(d3)
d4 = dm.build_drill("white", sid, st)
check("third drill of the block: back into the 3.Bc4 line for 4.d4",
      d4["family"] == "1.e4 e5 2.Nf3" and [s["san"] for s in d4["steps"] if s["new"]] == ["d4"]
      and "Nf6" in [s["san"] for s in d4["steps"]], str(d4["steps"]))
learn(d4)
d5 = dm.build_drill("white", sid, st)
check("block of 3 done: move to the 1...c5 family",
      d5["family"] == "1.e4 c5 2.d4", str(d5.get("family")))

print(f"\n{'ALL PASS' if FAILS == 0 else f'{FAILS} FAILURES'}")
sys.exit(1 if FAILS else 0)
