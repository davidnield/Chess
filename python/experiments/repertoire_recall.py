"""Equal-practice recall scenarios for frozen repertoire books.

Reads a book manifest (JSON list of label, colour, book, counts), the trainer in
read-only mode, and writes a card profile for every schedule and milestone.
counts must cover the book's opponent nodes in the intended construction pool.
"""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from zobrist import zobrist_int64


ROOT = zobrist_int64(chess.Board())
MILESTONES = (0, 30, 90, 240)
# Initial half-life (days), multiplier after successful retrieval.
RETENTION = {"low": (2., 1.6), "central": (5., 2.), "high": (10., 2.4)}


def trainer_history(path, as_of):
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        rows = db.execute("""SELECT rep,position_hash,best_move,review_datetime,rating
            FROM review_log JOIN cards USING(card_id) WHERE source='train'
            ORDER BY review_datetime, id""")
        history = defaultdict(list)
        for colour, h, move, stamp, rating in rows:
            when = (datetime.fromisoformat(stamp).date() - as_of).days
            if when <= 0:
                history[colour, h, move].append((when, rating))
    return history


def reference(arm):
    colour = arm["colour"]
    frame = (pl.read_parquet(arm["book"])
             .filter((pl.col("side_to_move") == colour) & pl.col("best_move").is_not_null())
             .select("position_hash", "position_epd", "best_move").sort("position_hash"))
    hs, moves = frame["position_hash"].to_list(), frame["best_move"].to_list()
    index = {h: i for i, h in enumerate(hs)}
    if len(index) != len(hs) or len(index) != 2000:
        raise ValueError(f"{arm['label']}: expected 2,000 unique decisions")
    after = []
    for h, epd, move in frame.iter_rows():
        b = chess.Board(epd + " 0 1")
        if zobrist_int64(b) != h or (b.turn == chess.WHITE) != (colour == "white"):
            raise ValueError(f"Invalid card {h}")
        b.push_san(move)
        after.append(zobrist_int64(b))
    targets = set(after)
    if colour == "black":
        targets.add(ROOT)
    replies = defaultdict(list)
    edges = pl.read_parquet(arm["counts"]).filter(pl.col("h").is_in(sorted(targets)))
    for h, group in edges.sort("h", "child_hash").group_by("h", maintain_order=True):
        weights = group["total"].to_numpy()
        replies[h[0]] = list(zip(group["child_hash"].to_list(), weights / weights.sum()))
    reach = np.zeros(len(hs))
    first_ply = np.full(len(hs), -1)
    front = {ROOT: 1.}
    missing_mass = 0.
    for ply in range(30):
        nxt = Counter()
        for h, mass in front.items():
            if (ply % 2 == 0) == (colour == "white"):
                if h in index:
                    i = index[h]
                    reach[i] += mass
                    if first_ply[i] < 0:
                        first_ply[i] = ply
                    nxt[after[i]] += mass
            elif h in replies:
                for child, p in replies[h]:
                    if child in index:
                        nxt[child] += mass * p
            else:
                missing_mass += mass
        front = nxt
    return hs, moves, index, after, replies, reach, first_ply, missing_mass


def scenario(ref, colour, history, start, curve, seed, daily, games):
    hs, moves, index, after, replies, reach, _, _ = ref
    n = len(hs)
    initial, growth = RETENTION[curve]
    strength = np.zeros(n)
    last = np.zeros(n)
    if start == "switch":
        for i, (h, move) in enumerate(zip(hs, moves)):
            for day, rating in history.get((colour, h, move), ()):
                strength[i] = (min(365., strength[i] * growth) if rating >= 3 and strength[i]
                               else initial)
                last[i] = day
    # Three common streams keep review responses, human replies and game
    # retrieval coupled across books without variable-length random consumption.
    practice_rng, reply_rng, game_rng = (np.random.default_rng(s)
        for s in np.random.SeedSequence(seed).spawn(3))
    order = np.argsort(-reach, kind="stable")
    for day in range(MILESTONES[-1] + 1):
        learned = strength > 0
        p = np.zeros(n)
        p[learned] = np.exp2(-(day - last[learned]) / strength[learned])
        if day in MILESTONES:
            yield day, p.copy(), learned.copy()
        if day == MILESTONES[-1]:
            break
        # Daily slots are equally costly. Reserve one third for new cards.
        # Fill any spare capacity, never review a card twice in one session.
        fresh = order[~learned[order]]
        known = np.flatnonzero(learned)
        n_new = min(len(fresh), max(daily // 3, daily - len(known)))
        priority = reach[known] * (1. - p[known])
        review = known[np.argsort(-priority, kind="stable")[:daily - n_new]]
        draws = practice_rng.random(n)
        good = draws[review] < p[review]
        strength[review] = np.where(good, np.minimum(365., strength[review] * growth), initial)
        new = fresh[:n_new]
        strength[new] = initial
        last[new] = last[review] = day
        for _ in range(games):
            rd, gd = reply_rng.random(30), game_rng.random(30)
            h = ROOT
            for ply in range(30):
                if (ply % 2 == 0) == (colour == "white"):
                    i = index.get(h)
                    if i is None or not strength[i]:
                        break
                    elapsed = day - last[i]
                    if gd[ply] >= 2. ** (-elapsed / strength[i]):
                        break
                    # An immediate repeat after practice is not spaced retrieval.
                    if elapsed >= 1:
                        strength[i] = min(365., strength[i] * growth)
                        last[i] = day
                    h = after[i]
                elif h in replies:
                    acc = 0.
                    for child, weight in replies[h]:
                        acc += weight
                        if rd[ply] < acc:
                            break
                    h = child
                else:
                    break


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "trainer", "out"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--as-of", type=date.fromisoformat, required=True)
    p.add_argument("--schedules", type=int, default=8)
    p.add_argument("--daily", type=int, default=30)
    p.add_argument("--games", type=int, default=1)
    a = p.parse_args()
    if a.schedules < 1 or not 3 <= a.daily <= 2000 or a.games < 0:
        p.error("invalid practice budget")
    arms = json.loads(a.manifest.read_text(encoding="utf-8-sig"))
    a.out.mkdir(parents=True, exist_ok=True)
    hist = trainer_history(a.trainer, a.as_of)
    summary, audit = [], {}
    for arm in arms:
        label, colour = arm["label"], arm["colour"]
        ref = reference(arm)
        hs, moves, _, _, _, reach, ply, missing = ref
        own_history = {(h, m) for c, h, m in hist if c == colour}
        cards = set(zip(hs, moves))
        audit[label] = dict(book_sha256=hashlib.sha256(Path(arm["book"]).read_bytes()).hexdigest(),
            matched=len(cards & own_history), changed=len({h for h, m in own_history
            if h in ref[2] and (h, m) not in cards}), reachable=int((reach > 0).sum()),
            book_visits=float(reach.sum()), missing_reply_mass=missing)
        pl.DataFrame(dict(position_hash=hs,best_move=moves,reach=reach,first_ply=ply)
                     ).write_parquet(a.out / f"{label}-cards.parquet")
        # Store each draw independently so the referee can preserve correlated
        # uncertainty about which positions one learner has studied.
        for start in ("scratch", "switch"):
            for curve in RETENTION:
                dest = a.out / label / start / curve
                dest.mkdir(parents=True, exist_ok=True)
                for seed in range(a.schedules):
                    snapshots = {"position_hash": hs}
                    for day, prob, learned in scenario(ref, colour, hist, start, curve, seed,
                                                       a.daily, a.games):
                        snapshots[f"day{day}"] = prob
                        summary.append(dict(label=label,start=start,curve=curve,seed=seed,day=day,
                            learned=int(learned.sum()), recalled=float(reach @ prob / reach.sum())))
                    pl.DataFrame(snapshots).write_parquet(dest / f"{seed}.parquet")
        print(label, audit[label], flush=True)
    pl.DataFrame(summary).write_parquet(a.out / "schedules.parquet")
    (a.out / "design.json").write_text(json.dumps(dict(as_of=str(a.as_of),daily=a.daily,
        games_per_day=a.games,schedules=a.schedules,retention=RETENTION,arms=arms,audit=audit,
        training_reviews=sum(map(len, hist.values())),training_cards=len(hist)),indent=2))


if __name__ == "__main__":
    main()
