"""Blind practice from repertoire exits, and first-departure review of personal PGNs.

deck samples both shared and disagreeing positions using their exit masses. The two
practice PGNs are for the player. Keep the separate answer key private until finishing.
review-games writes a worksheet for classifying what happened in actual games.
"""
import argparse
import csv
import hashlib
from collections import defaultdict
from datetime import date, timedelta
import json
import math
from pathlib import Path
import random
import tempfile

import chess
import chess.pgn
import polars as pl

from exit_difficulty import read_exits
from zobrist import zobrist_int64


def load_moves(path, colour):
    rows = pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
    return {h: move for h, side, move in rows.iter_rows() if side == colour and move}


def draw_positions(inputs, books, count, rng, colour):
    positions = {}
    for label, paths in inputs.items():
        frame = read_exits(paths)
        total = frame["mass"].sum()
        if not total:
            raise ValueError(f"No exit mass for {label}")
        for row in frame.iter_rows(named=True):
            board = chess.Board(row["epd"] + " 0 1")
            if board.turn != (colour == "white") or zobrist_int64(board) != row["hash"]:
                raise ValueError(f"Invalid or wrong-colour exit in {label}")
            if board.is_game_over() or row["mass"] <= 0:
                continue
            key = row["epd"]
            item = positions.setdefault(key, {"epd": key, "weight": 0., "sources": {},
                                              "moves": {k: bk.get(row["hash"]) for k,bk in books.items()}})
            item["weight"] += row["mass"] / total / len(inputs)
            item["sources"][label] = item["sources"].get(label, 0.) + row["mass"]
    # Exponential keys give weighted sampling without replacement. Reserve up to
    # half the deck for policy disagreements, then draw the rest from all exits.
    items = sorted(positions.values(), key=lambda x: rng.expovariate(x["weight"]))
    special = [x for x in items if len(set(x["moves"].values())) > 1][:count//2]
    chosen = (special + [x for x in items if x not in special])[:count]
    if not chosen:
        raise ValueError("no playable exits")
    return chosen


def worksheet(path, rows):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def publish_deck(folder, output):
    """Publish staged files individually. Windows can lock populated directory renames."""
    files = sorted(p for p in folder.rglob("*") if p.is_file())
    manifest = {p.relative_to(folder).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in files}
    marker = output / "INCOMPLETE.json"
    if output.exists():
        if not marker.exists() or json.loads(marker.read_text()) != manifest:
            raise ValueError("existing deck or different interrupted run; choose a fresh directory")
    else:
        output.mkdir()
        marker.write_text(json.dumps(manifest))
    for name, digest in manifest.items():
        dest = output / name
        if dest.exists():
            if hashlib.sha256(dest.read_bytes()).hexdigest() != digest:
                raise ValueError(f"{dest} was edited; resume in a fresh directory")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        (folder / name).replace(dest)
    marker.unlink(missing_ok=True)


def deck(a):
    if a.count < 1 or (a.out.exists() and not (a.out/"INCOMPLETE.json").exists()):
        raise ValueError("choose a positive count and a fresh deck directory to preserve earlier observations")
    sources = defaultdict(list)
    for arg in a.exits:
        label, path = arg.split("=", 1)
        sources[label].append(path)
    books = {lab: load_moves(path, a.colour) for lab,path in (x.split("=",1) for x in a.book)}
    if books.keys() != sources.keys():
        raise ValueError("supply each exit source's book with the same label")
    rng = random.Random(a.seed)
    chosen = draw_positions(sources, books, a.count, rng, a.colour)
    rng.shuffle(chosen)
    key = {f"P{i:03d}": row for i, row in enumerate(chosen, 1)}
    a.out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=a.out.parent) as temp:
        folder = Path(temp)/"deck"
        folder.mkdir()
        notes = []
        for session in [1, 2]:
            order = list(key)
            rng.shuffle(order)
            with (folder/f"session-{session}.pgn").open("w",encoding="utf-8") as stream:
                for tag in order:
                    game = chess.pgn.Game()
                    game.setup(chess.Board(key[tag]["epd"] + " 0 1"))
                    game.headers["Event"] = f"Practice {tag}"
                    control = a.fast if (int(tag[1:]) + session) % 2 else a.slow
                    game.headers["TimeControl"] = control
                    game.headers["Date"] = (a.start + timedelta(days=7*(session-1))).strftime("%Y.%m.%d")
                    print(game, file=stream, end="\n\n")
                    notes.append({"session":session,"position":tag,"date":game.headers["Date"],
                                  "time_control":control,"first_move":"","first_move_seconds":"",
                                  "plan":"","first_serious_error":"","result":"","notes":""})
        worksheet(folder/"observations.csv", notes)
        (folder/"private").mkdir()
        (folder/"private"/"key.json").write_text(json.dumps(key,indent=2),encoding="utf-8")
        publish_deck(folder, a.out)
    print(f"{len(key)} positions, two sessions seven days apart -> {a.out}")


def departures(game, us, moves):
    board = game.board()
    for node in game.mainline():
        move = node.move
        if board.turn == us:
            prescribed = moves.get(zobrist_int64(board))
            san = board.san(move)
            if not prescribed or board.parse_san(prescribed) != move:
                return {"ply":board.ply()+1,"epd":board.epd(),"actual":san,"prescribed":prescribed or "",
                        "clock_after_seconds":node.clock(), "pgn_move_seconds":node.emt(),
                        "event":"first uncovered position" if not prescribed else "departed from book"}
        board.push(move)
    return {"ply":"","epd":"","actual":"","prescribed":"","clock_after_seconds":"",
            "pgn_move_seconds":"","event":"followed throughout"}


def review_games(a):
    if a.out.exists():
        raise ValueError("choose a new worksheet so earlier annotations are preserved")
    books = {side:load_moves(file,side) for side,file in (x.split("=",1) for x in a.book)}
    rows = []
    for path in a.pgn:
        with Path(path).open(encoding="utf-8-sig") as stream:
            while (game := chess.pgn.read_game(stream)) is not None:
                if game.errors:
                    raise ValueError(f"Malformed PGN in {path}: {game.errors}")
                sides = [s for s in ("White","Black") if game.headers.get(s,"").casefold()==a.player.casefold()]
                if not sides:
                    continue
                if len(sides) != 1 or sides[0].lower() not in books:
                    raise ValueError(f"Supply a book for the player's side: {sides}")
                side = sides[0]
                rows.append({"file":str(path),"date":game.headers.get("Date"),"round":game.headers.get("Round"),
                             "white":game.headers["White"],"black":game.headers["Black"],
                             "result":game.headers["Result"],"colour":side.lower(),
                             **departures(game, side=="White", books[side.lower()]),
                             "cause_recall_plan_tactics_or_other":"","first_serious_error_ply":"",
                             "thinking_time_seconds":"","lesson":""})
    if not rows:
        raise ValueError("no games match --player")
    a.out.parent.mkdir(parents=True,exist_ok=True)
    worksheet(a.out,rows)
    print(f"{len(rows)} games -> {a.out}")


def practice_results(a):
    """Reveal source membership after the player has recorded their observations."""
    key = json.loads((a.deck / "private/key.json").read_text(encoding="utf-8"))
    with (a.deck / "observations.csv").open(encoding="utf-8-sig", newline="") as stream:
        responses = list(csv.DictReader(stream))
    completed = []
    for response in responses:
        if not any(response[field].strip() for field in
                   ("first_move", "plan", "first_serious_error", "result", "first_move_seconds")):
            continue
        card = key[response["position"]]
        board = chess.Board(card["epd"] + " 0 1")
        move = response["first_move"].strip()
        if move:
            move = board.san(board.parse_san(move))
        seconds = response["first_move_seconds"].strip()
        if seconds and (not math.isfinite(float(seconds)) or float(seconds) < 0):
            raise ValueError("record a finite nonnegative first-move time")
        result = response["result"].strip()
        if result not in ("", "*", "1-0", "0-1", "1/2-1/2"):
            raise ValueError(f"Unknown result on {response['position']}: {result}")
        scores = {"1-0": float(board.turn), "0-1": float(not board.turn), "1/2-1/2": .5}
        completed.append({**response, "first_move": move, "our_score": scores.get(result),
                          "exit_sources": ", ".join(sorted(card["sources"])),
                          "prescriptions": json.dumps(card["moves"], sort_keys=True)})
    if not completed:
        raise ValueError("record at least one observation before revealing the key")
    if a.out.exists():
        raise ValueError("choose a fresh results file")
    a.out.parent.mkdir(parents=True, exist_ok=True)
    worksheet(a.out, completed)
    print(f"{len(completed)} completed observations -> {a.out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    commands = ap.add_subparsers(dest="command",required=True)
    d = commands.add_parser("deck")
    d.add_argument("--exits", action="append", required=True, metavar="LABEL=PARQUET")
    d.add_argument("--book", action="append", required=True, metavar="LABEL=BOOK")
    d.add_argument("--colour", choices=["white","black"],required=True)
    d.add_argument("--count", type=int, default=12)
    d.add_argument("--seed", type=int, default=47)
    d.add_argument("--start", type=date.fromisoformat, default=date.today())
    d.add_argument("--fast", default="600+5", help="PGN time control (seconds)")
    d.add_argument("--slow", default="3600+30")
    d.add_argument("--out",type=Path,required=True)
    r = commands.add_parser("review-games")
    r.add_argument("--pgn", nargs="+",required=True)
    r.add_argument("--player",required=True)
    r.add_argument("--book",action="append",required=True,metavar="COLOUR=BOOK")
    r.add_argument("--out",type=Path,required=True)
    reveal = commands.add_parser("results")
    reveal.add_argument("--deck", required=True, type=Path)
    reveal.add_argument("--out", required=True, type=Path)
    a = ap.parse_args()
    if a.command=="deck":
        deck(a)
    elif a.command == "results":
        practice_results(a)
    else:
        review_games(a)


if __name__=="__main__":
    main()
