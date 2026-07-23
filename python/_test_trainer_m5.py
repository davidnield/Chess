"""M5 synthetic test: deviation scan on hand-built games against the REAL
training pack (temp DB, no pollution). Covers: basic deviation, black
perspective, castling SAN, transposition re-entry after leaving book, and the
FSRS out-of-order guard for old games.

Run:  .venv/Scripts/python.exe python/_test_trainer_m5.py
"""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")

import chess

from trainer_app import config, db
from trainer_app import scheduler as sched
from trainer_app.lichess_import import scan_games
from trainer_app.pack import TrainingPack
from zobrist import zobrist_int64

FAILS = 0


def check(name, cond, detail=""):
    global FAILS
    print(f"  [{'ok ' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILS += 1


pack = TrainingPack(r"C:\Users\David\Documents\Chess\trainer_data\pack")
tmp = Path(tempfile.mkdtemp(prefix="trainer_m5_"))
con = db.connect(tmp)
settings = config.get_settings(con)


def board_after(*sans):
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def add_game(gid, color, moves, played_at=None):
    with con:
        con.execute(
            "INSERT INTO lichess_games(game_id, played_at, color, speed, rated, "
            "opponent, result, moves_san, imported_at) "
            "VALUES(?,?,?,?,1,'opp','win',?,?)",
            (gid, (played_at or datetime.now(timezone.utc)).isoformat(), color,
             "blitz", moves, sched.utcnow_iso()))


# Expected book moves from the real pack, derived not assumed:
exp_root = pack.book_move("white", zobrist_int64(chess.Board()))
exp_m3 = pack.book_move("white", zobrist_int64(board_after("e4", "e5", "Nf3", "Nc6")))
exp_black1 = pack.book_move("black", zobrist_int64(board_after("e4")))
print(f"pack expectations: root={exp_root}, after 2...Nc6={exp_m3}, black vs e4={exp_black1}")
check("pack has expected moves for the probe nodes",
      all(x is not None for x in (exp_root, exp_m3, exp_black1)))

# g1: white deviates at move 3 (plays a legal non-book move instead of exp_m3)
dev3 = "Bb5" if exp_m3 != "Bb5" else "Be2"
add_game("g1", "white", f"e4 e5 Nf3 Nc6 {dev3} a6 Ba4 Nf6")
# g2: black deviates immediately vs 1.e4
devb = "d6" if exp_black1 != "d6" else "c6"
add_game("g2", "black", f"e4 {devb} d4 Nf6")
# g3: transposition re-entry — 2.Bc4 leaves the book move order (dev if expected
# differs), then 3.Nf3 transposes back; the NEXT white move is probed in book.
b_reentry = board_after("e4", "e5", "Bc4", "Nc6", "Nf3", "Bc5")
exp_reentry = pack.book_move("white", zobrist_int64(b_reentry))
print(f"re-entry node book move: {exp_reentry}")
dev_re = "d3" if exp_reentry != "d3" else "a3"
add_game("g3", "white", f"e4 e5 Bc4 Nc6 Nf3 Bc5 {dev_re} d6")
# g4: castling SAN handled — full book line through O-O (from the M2 live drill
# line), deviating right after with a quiet move if possible; at minimum the
# scan must not crash on O-O/O-O-O and must replay through it.
add_game("g4", "white", "e4 e5 Nf3 Nc6 Bc4 Nf6 d4 exd4 O-O d6 c3 dxc3 Nxc3")

print("\nscan:")
n = scan_games(con, pack, settings)
devs = con.execute("SELECT game_id, rep, expected_move, played_move, ply FROM "
                   "deviations ORDER BY game_id, ply").fetchall()
for d in devs:
    print(f"    {d}")
g1 = [d for d in devs if d[0] == "g1"]
check("g1: first deviation at ply 4 with the book move",
      g1 and g1[0][2] == exp_m3 and g1[0][4] == 4, str(g1))
g2 = [d for d in devs if d[0] == "g2"]
check("g2: black deviation vs 1.e4 recorded first",
      g2 and g2[0][1] == "black" and g2[0][2] == exp_black1 and g2[0][3] == devb,
      str(g2))

# Content-independent invariant: EVERY recorded deviation must reproduce from a
# fresh replay — position hash matches, pack.book_move(pos) == expected_move,
# and the played move really differs. (Extra deviations after book re-entry are
# by-design: the full-book lookup covers ~6.6M positions.)
games = {gid: (mv, col) for gid, mv, col in con.execute(
    "SELECT game_id, moves_san, color FROM lichess_games")}
all_devs = con.execute("SELECT game_id, rep, position_hash, ply, expected_move, "
                       "played_move FROM deviations").fetchall()
bad = 0
for gid, rep, ph, ply, exp, played in all_devs:
    moves, col = games[gid]
    b = chess.Board()
    for s in moves.split()[:ply]:
        b.push_san(s)
    if (zobrist_int64(b) != ph or pack.book_move(rep, zobrist_int64(b)) != exp
            or exp == played):
        bad += 1
check(f"all {len(all_devs)} deviations replay-consistent with the pack", bad == 0,
      f"{bad} bad")
g3 = [d for d in devs if d[0] == "g3"]
exp_move2 = pack.book_move("white", zobrist_int64(board_after("e4", "e5")))
want_g3 = (1 if exp_move2 == "Bc4" else 1) + (1 if exp_reentry else 0)
check("g3: re-entry deviation recorded after transposition",
      any(d[4] == 6 for d in g3), str(g3))
check("g3: correct expected move at re-entry node",
      any(d[2] == exp_reentry and d[3] == dev_re for d in g3), str(g3))
g4 = [d for d in devs if d[0] == "g4"]
check("g4: castled book line scans clean through O-O (no false deviations "
      "before move 7)", all(d[4] > 8 for d in g4), str(g4))
check("all games marked scanned",
      con.execute("SELECT COUNT(*) FROM lichess_games WHERE scanned=0").fetchone()[0] == 0)

print("\nFSRS wiring:")
logs = con.execute("SELECT source, game_id FROM review_log").fetchall()
check("every deviation logged as source='deviation'",
      len(logs) == len(devs) and all(s == "deviation" for s, _ in logs),
      f"{len(logs)} logs vs {len(devs)} devs")
card = sched.get_card_row(con, "white", zobrist_int64(board_after("e4", "e5", "Nf3", "Nc6")))
check("deviation created the card with a lapse", card is not None and card["lapses"] == 1)

# out-of-order guard: train-review a card NOW, then scan an OLD game deviating there
fsrs = sched.make_scheduler(settings)
root_card = sched.get_or_create_card(con, "white", zobrist_int64(chess.Board()),
                                     chess.Board().epd(), exp_root)
sched.record_review(con, fsrs, root_card, sched.Rating.Good, source="train")
before = sched.get_card_row(con, "white", zobrist_int64(chess.Board()))["last_review"]
old_dev = "d4" if exp_root != "d4" else "c4"
add_game("g5", "white", f"{old_dev} d5",
         played_at=datetime.now(timezone.utc) - timedelta(days=90))
scan_games(con, pack, settings)
after = sched.get_card_row(con, "white", zobrist_int64(chess.Board()))
check("old-game review did not move last_review backward",
      after["last_review"] >= before, f"{after['last_review']} vs {before}")
check("old-game deviation still recorded",
      con.execute("SELECT COUNT(*) FROM deviations WHERE game_id='g5'").fetchone()[0] == 1)

print(f"\n{'ALL PASS' if FAILS == 0 else f'{FAILS} FAILURES'}")
sys.exit(1 if FAILS else 0)
