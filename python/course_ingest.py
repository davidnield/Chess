"""Opening-course PGNs -> position-keyed tables (stage 1 of the course benchmarks).

Reads every PGN under the courses folder (read-only) and writes, per course, four
tables keyed by the SAME position hash every pipeline stage uses (zobrist_int64),
so courses join directly against the pool, the eval arrays and our books:

  courses   one row per course: folder, category, publisher, year, side hint
  items     one row per PGN game ("item"): headers, structure, and a TYPE
  edges     every move of every item, main line and variations: parent/child
            hash, SAN, ply, variation depth, NAGs, whether a comment follows
  comments  cleaned prose by position (the position the comment follows)

ITEM TYPES. A course mixes repertoire lines with material that is not a line to
learn. The type is decided from the headers and the item's shape, and the rule
that fired is recorded (`type_reason`) so it can be audited and overridden:

  line          a repertoire line (the default)
  quickstarter  a shortened duplicate of full lines (Chessable "Quickstarter")
  model_game    an annotated real game: real players / Elo / decisive result /
                long main line, or a "X vs. Y" / "X - Y, Place 1999" title
  intro         introductions, prefaces, overviews, structures, conclusions
  exercise      tests, quizzes, puzzles, "Position N", or a FEN start that is not
                an opening position
  endgame       a FEN start with few pieces
  trap          traps, tactics, tricks, blunders (still inside the opening tree)
  alternative   explicitly alternative options for the repertoire side

Stage 2 (course_books.py) decides which types feed a course's repertoire; this
stage keeps everything, so the comments of intros and model games stay
searchable by position.

FORMAT QUIRKS HANDLED
  - Forward Chess / Thinkers / Chess Stars / some Quality Chess "ebook" PGNs put
    two blank lines between the header block and the movetext, which splits each
    item into a header-only game and a headerless one, the second parsed from the
    wrong start position when the first has a FEN. The blank-line run is
    collapsed before parsing.
  - The same files restate the parent move inside variations, "2.c4 ( 2.c4 e5 )".
    That is harmless here: positions are extracted move by move, and the restated
    move lands on the same position.
  - Illegal or ambiguous SAN: python-chess skips the rest of that variation and
    records the error; the count is kept per item.
  - Comment commands ([%evp ...], [%cal ...], [%csl ...], [%clk ...]) are removed
    from the prose.

Resumable: each course is written to <work>/parts/<course_id>/ with a _DONE
sentinel last; a rerun skips finished courses. `--phase finalize` concatenates
the parts into <work>/{courses,items,edges,comments}.parquet.

    .venv/Scripts/python.exe python/course_ingest.py --phase all
    .venv/Scripts/python.exe python/course_ingest.py --phase courses --limit 5
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import re
import sys
import time
import traceback
from multiprocessing import Pool
from pathlib import Path

import chess
import chess.pgn
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from zobrist import zobrist_int64  # noqa: E402

ROOT = Path("G:/My Drive/Chess/Books and Courses/1 Openings")
WORK = Path("E:/chess/courses")
START_HASH = zobrist_int64(chess.Board())

NAG_SYMBOL = {1: "!", 2: "?", 3: "!!", 4: "??", 5: "!?", 6: "?!", 7: "□", 10: "=",
              13: "∞", 14: "⩲", 15: "⩱", 16: "±", 17: "∓", 18: "+-", 19: "-+",
              22: "⨀", 32: "⟳", 36: "↑", 40: "→", 132: "⇆", 140: "∆", 146: "N"}

# ── course discovery ────────────────────────────────────────────────────────

def course_dirs(root: Path) -> list[Path]:
    """A course is the folder directly holding PGNs, one level under its
    category (General Opening Books) or subcategory (everything else)."""
    out = set()
    for f in root.rglob("*.pgn"):
        rel = f.relative_to(root).parts
        depth = 2 if rel[0] == "General Opening Books" else 3
        if len(rel) <= depth:            # a PGN directly in a category folder
            out.add(root.joinpath(*rel[:-1]))
        else:
            out.add(root.joinpath(*rel[:depth]))
    return sorted(out)


def course_meta(d: Path, root: Path) -> dict:
    rel = d.relative_to(root).parts
    name = rel[-1]
    pub = re.search(r"\[([^\]]+)\]\s*$", name)
    year = re.search(r"\((\d{4})\)", name)
    side = {"White Repertoires": "white", "Black Repertoires": "black"}.get(rel[0])
    rel_str = "/".join(rel)
    return {"course_id": hashlib.sha1(rel_str.encode("utf-8")).hexdigest()[:12],
            "rel_dir": rel_str, "category": rel[0],
            "subcategory": rel[1] if len(rel) > 2 else None,
            "course_name": name, "publisher": pub.group(1) if pub else None,
            "year": int(year.group(1)) if year else None, "side_hint": side}


# ── parsing ─────────────────────────────────────────────────────────────────

_HDR_GAP = re.compile(r"(\]\s*\n)(?:[ \t]*\r?\n){2,}(?=[^\[\s])")
_COMMANDS = re.compile(r"\[%[^\]]*\]")
_WS = re.compile(r"\s+")


def read_text(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            txt = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    txt = txt.replace("\r\n", "\n")
    # header block, then 2+ blank lines, then movetext: collapse to one blank line
    return _HDR_GAP.sub(r"\1\n", txt)


def clean_comment(c: str | None) -> str:
    if not c:
        return ""
    return _WS.sub(" ", _COMMANDS.sub(" ", c)).strip()


DEFAULTS = {"Event": "?", "Site": "?", "White": "?", "Black": "?", "Round": "?"}


def is_default_headers(h) -> bool:
    return all(h.get(k, "?") in ("?", "") for k in DEFAULTS)


# ── classification ──────────────────────────────────────────────────────────

RX = {
    "quickstarter": re.compile(r"quick ?-?start|quickstarter|short version|in a nutshell", re.I),
    "intro": re.compile(r"\b(introduction|intro|preface|foreword|about this|overview|"
                        r"welcome|how to use|conclusion|summary|bibliography|"
                        r"general (considerations|ideas|remarks)|key ideas|"
                        r"pawn structures?|typical (plans|ideas|structures)|"
                        r"strategic (ideas|overview)|ideas behind|theory overview|"
                        r"what to (study|expect)|series foreword)\b", re.I),
    "exercise": re.compile(r"\b(exercises?|tests?|quiz(zes)?|puzzles?|solutions?|drills?|"
                           r"challenges?|find the|position \d+|positions for|"
                           r"test your|training|workbook)\b", re.I),
    "trap": re.compile(r"\b(traps?|tactics?|tactical|tricks?|blunders?|swindles?|"
                       r"pitfalls?|thematic|motifs?|combinations?|catastroph)\w*", re.I),
    "alternative": re.compile(r"\b(alternatives?|alternative options?|other options?|"
                              r"plan b|instead of|also playable|second option|"
                              r"backup|secondary line)\b", re.I),
    "endgame": re.compile(r"\b(endgames?|endings?)\b", re.I),
    "vs_title": re.compile(r"\bvs\.?\b|^[A-Z][\w'\-]+(?:,? [A-Z][\w.'\-]*)* - "
                           r"[A-Z][\w'\-]+.*\b(1[89]|20)\d\d\b"),
}
PERSON = re.compile(r"^[A-Z][\w'\-]+(?:,\s*[A-Z][\w.'\- ]*)?$|^[A-Z][\w.'\-]+ [A-Z][\w'\-]+$")
CHAPTERISH = re.compile(r"\b(chapter|game|part|section|line|variation|#\d|\d+\.)", re.I)


def classify(h: dict, f: dict) -> tuple[str, str]:
    """(item_type, reason) from headers h and structural features f."""
    title = " | ".join(v for k in ("Event", "Site", "White", "Black", "Round")
                       if (v := h.get(k)) and v not in ("?", ""))
    res = h.get("Result", "*")
    w, b = h.get("White", "?"), h.get("Black", "?")
    if f["start_pieces"] is not None and f["start_pieces"] <= 14:
        return "endgame", f"FEN start with {f['start_pieces']} pieces"
    if f["fen_start"] and not f["opening_like"]:
        return "exercise", "FEN start is not an opening position"
    for t in ("quickstarter", "exercise"):
        m = RX[t].search(title)
        if m:
            return t, f"title '{m.group(0)}'"
    # a real game inside an introduction chapter is still a model game
    m = RX["vs_title"].search(title)
    if m and f["mainline_plies"] >= 30:
        return "model_game", f"game title '{m.group(0)[:30]}'"
    m = RX["intro"].search(title)
    # an intro is short: a long item under an "Intro" chapter name is a real line
    # (measured: "5.Nf3 Intro and 7.Ne1 | 16.a3 a5 7.d4!?" is a 50-ply line)
    if m and f["mainline_plies"] < 20 and not f["n_variations"] > 5:
        return "intro", f"title '{m.group(0)}'"
    elo = h.get("WhiteElo") or h.get("BlackElo")
    real_players = (PERSON.match(w or "") and PERSON.match(b or "")
                    and not CHAPTERISH.search(w + " " + b))
    if elo and res != "*":
        return "model_game", "Elo headers and a result"
    if real_players and res in ("1-0", "0-1", "1/2-1/2") and f["mainline_plies"] >= 30:
        return "model_game", "player names, a result, a long main line"
    if f["mainline_plies"] >= 70 and res in ("1-0", "0-1", "1/2-1/2"):
        return "model_game", "a result and a main line of 70+ plies"
    # "endgame" in a title is NOT excluded: "Philidor endgame with 7.Bd2" is an
    # opening line that reaches a queenless position; only a FEN start with few
    # pieces is an endgame item (above)
    for t in ("trap", "alternative"):
        m = RX[t].search(title)
        if m:
            return t, f"title '{m.group(0)}'"
    return "line", "default"


def opening_like(board: chess.Board) -> bool:
    """A FEN start that could sit inside an opening tree: most material still on
    the board and not deep into the game."""
    return len(board.piece_map()) >= 24 and board.fullmove_number <= 25


# ── extraction ──────────────────────────────────────────────────────────────

def extract_item(game, cid: str, fidx: int, iidx: int):
    """Edges and comments of one item, and its structural features."""
    board = game.board()
    start_hash = zobrist_int64(board)
    fen_start = "FEN" in game.headers
    edges, comments = [], []
    root_c = clean_comment(game.comment)
    if root_c:
        comments.append((cid, fidx, iidx, 0, start_hash, None, board.ply(), root_c))
    n_nodes = n_com = n_var = 0
    # stack: (node, board, node_id, var_depth)
    stack = [(game, board, 0, 0)]
    next_id = 1
    mainline_plies = 0
    node = game
    while node.variations:
        node = node.variations[0]
        mainline_plies += 1
    while stack:
        node, bd, nid, depth = stack.pop()
        ph = zobrist_int64(bd)
        if len(node.variations) > 1:
            n_var += len(node.variations) - 1
        for i, child in enumerate(node.variations):
            mv = child.move
            try:
                san = bd.san(mv)
            except Exception:  # noqa: BLE001 - an illegal move recorded by the parser
                continue
            nb = bd.copy(stack=False)
            nb.push(mv)
            ch = zobrist_int64(nb)
            cid_ = next_id
            next_id += 1
            n_nodes += 1
            d = depth + (1 if i > 0 else 0)
            text = clean_comment(" ".join(x for x in (child.starting_comment, child.comment) if x))
            nags = " ".join(NAG_SYMBOL.get(n, f"${n}") for n in sorted(child.nags))
            edges.append((cid, fidx, iidx, cid_, nid, bd.ply(), d, ph, san, ch,
                          "w" if bd.turn else "b", nags or None, bool(text)))
            if text:
                n_com += 1
                comments.append((cid, fidx, iidx, cid_, ch, san, nb.ply(), text))
            stack.append((child, nb, cid_, d))
    start_board = game.board()
    feats = {"start_hash": start_hash, "start_ply": start_board.ply(),
             "fen_start": fen_start and start_hash != START_HASH,
             "start_pieces": len(start_board.piece_map()) if fen_start else None,
             "opening_like": opening_like(start_board),
             "mainline_plies": mainline_plies, "n_nodes": n_nodes,
             "n_comments": n_com + (1 if root_c else 0), "n_variations": n_var,
             "n_errors": len(game.errors)}
    return edges, comments, feats


EDGE_SCHEMA = {"course_id": pl.Utf8, "file_idx": pl.Int32, "item_idx": pl.Int32,
               "node_id": pl.Int32, "parent_node": pl.Int32, "ply": pl.Int32,
               "var_depth": pl.Int32, "parent_hash": pl.Int64, "move_san": pl.Utf8,
               "child_hash": pl.Int64, "mover": pl.Utf8, "nags": pl.Utf8,
               "has_comment": pl.Boolean}
COMMENT_SCHEMA = {"course_id": pl.Utf8, "file_idx": pl.Int32, "item_idx": pl.Int32,
                  "node_id": pl.Int32, "position_hash": pl.Int64, "move_san": pl.Utf8,
                  "ply": pl.Int32, "text": pl.Utf8}
HEADER_KEYS = ("Event", "Site", "Date", "Round", "White", "Black", "Result", "ECO",
               "Annotator", "WhiteElo", "BlackElo", "FEN")


def process_course(args) -> dict:
    meta, root, work = args
    out = Path(work) / "parts" / meta["course_id"]
    if (out / "_DONE").exists():
        return {**meta, "status": "skipped"}
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    logging.getLogger("chess.pgn").setLevel(logging.CRITICAL)
    try:
        files = sorted(Path(root, meta["rel_dir"]).rglob("*.pgn"))
        items, edges, comments = [], [], []
        for fidx, f in enumerate(files):
            pgn = io.StringIO(read_text(f))
            iidx = 0
            while True:
                try:
                    game = chess.pgn.read_game(pgn)
                except Exception as e:  # noqa: BLE001
                    items.append({"course_id": meta["course_id"], "file_idx": fidx,
                                  "item_idx": iidx, "file": f.name, "item_type": "unreadable",
                                  "type_reason": f"parser: {e}"[:200]})
                    break
                if game is None:
                    break
                h = dict(game.headers)
                if not game.variations and not game.comment and is_default_headers(h):
                    continue                      # empty filler between items
                try:
                    e, c, feats = extract_item(game, meta["course_id"], fidx, iidx)
                except Exception as ex:  # noqa: BLE001 - e.g. a malformed FEN header
                    items.append({"course_id": meta["course_id"], "file_idx": fidx,
                                  "item_idx": iidx, "file": f.name,
                                  **{k.lower(): h.get(k) for k in HEADER_KEYS},
                                  "item_type": "unreadable",
                                  "type_reason": f"extract: {ex}"[:200]})
                    iidx += 1
                    continue
                t, why = classify(h, feats)
                items.append({"course_id": meta["course_id"], "file_idx": fidx,
                              "item_idx": iidx, "file": f.name,
                              **{k.lower(): h.get(k) for k in HEADER_KEYS},
                              **{k: v for k, v in feats.items()},
                              "item_type": t, "type_reason": why})
                edges.extend(e)
                comments.extend(c)
                iidx += 1
        pl.DataFrame(edges, schema=EDGE_SCHEMA, orient="row").write_parquet(out / "edges.parquet")
        pl.DataFrame(comments, schema=COMMENT_SCHEMA, orient="row").write_parquet(out / "comments.parquet")
        pl.DataFrame(items, infer_schema_length=None).write_parquet(out / "items.parquet")
        info = {**meta, "status": "ok", "n_files": len(files), "n_items": len(items),
                "n_edges": len(edges), "n_comments": len(comments),
                "n_errors": int(sum(i.get("n_errors") or 0 for i in items)),
                "seconds": round(time.time() - t0, 1)}
        (out / "course.json").write_text(json.dumps(info, indent=1), encoding="utf-8")
        (out / "_DONE").write_text("ok", encoding="utf-8")
        return info
    except Exception as e:  # noqa: BLE001 - one bad course must not stop the rest
        return {**meta, "status": "error", "error": f"{e}\n{traceback.format_exc()}"[-2000:]}


def reclassify(work: Path) -> None:
    """Re-run classify() over items.parquet (the headers and features it needs are
    stored), so a rule change needs no re-parse. Writes items.parquet in place."""
    items = pl.read_parquet(work / "items.parquet")
    types, reasons = [], []
    for r in items.iter_rows(named=True):
        if r["item_type"] == "unreadable":
            types.append(r["item_type"]); reasons.append(r["type_reason"]); continue
        h = {k: r.get(k.lower()) for k in HEADER_KEYS}
        h = {k: v for k, v in h.items() if v is not None}
        f = {k: r.get(k) for k in ("start_pieces", "fen_start", "opening_like",
                                   "mainline_plies", "n_variations")}
        t, why = classify(h, f)
        types.append(t); reasons.append(why)
    old = items["item_type"]
    items = items.with_columns(pl.Series("item_type", types), pl.Series("type_reason", reasons))
    changed = (old != items["item_type"]).sum()
    tmp = work / "items.parquet.tmp"
    items.write_parquet(tmp, compression="zstd")
    tmp.replace(work / "items.parquet")
    print(f"  reclassified: {changed:,} of {items.height:,} items changed type")


def finalize(work: Path) -> None:
    parts = sorted((work / "parts").glob("*/_DONE"))
    dirs = [p.parent for p in parts]
    courses = pl.DataFrame([json.loads((d / "course.json").read_text(encoding="utf-8")) for d in dirs])
    for name in ("items", "edges", "comments"):
        frames = [pl.read_parquet(d / f"{name}.parquet") for d in dirs
                  if (d / f"{name}.parquet").exists()]
        df = pl.concat(frames, how="diagonal_relaxed")
        tmp = work / f"{name}.parquet.tmp"
        df.write_parquet(tmp, compression="zstd")
        tmp.replace(work / f"{name}.parquet")
        print(f"  {name}: {df.height:,} rows")
    tmp = work / "courses.parquet.tmp"
    courses.write_parquet(tmp)
    tmp.replace(work / "courses.parquet")
    print(f"  courses: {courses.height:,}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=str(ROOT))
    ap.add_argument("--work", default=str(WORK))
    ap.add_argument("--phase", choices=["courses", "finalize", "reclassify", "all"],
                    default="all")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit", type=int, default=0, help="Only the first N courses (testing).")
    ap.add_argument("--match", default=None, help="Only courses whose folder contains this text.")
    a = ap.parse_args()
    root, work = Path(a.root), Path(a.work)
    work.mkdir(parents=True, exist_ok=True)
    if a.phase in ("courses", "all"):
        metas = [course_meta(d, root) for d in course_dirs(root)]
        if a.match:
            metas = [m for m in metas if a.match.lower() in m["rel_dir"].lower()]
        if a.limit:
            metas = metas[:a.limit]
        print(f"{len(metas)} courses -> {work}", flush=True)
        t0 = time.time()
        n_ok = n_err = 0
        with Pool(a.workers) as pool:
            for i, info in enumerate(pool.imap_unordered(
                    process_course, [(m, str(root), str(work)) for m in metas]), 1):
                if info["status"] == "error":
                    n_err += 1
                    print(f"  ERROR {info['course_name']}: {info['error'][:300]}", flush=True)
                else:
                    n_ok += 1
                if i % 25 == 0 or i == len(metas):
                    print(f"  [{i}/{len(metas)}] ok {n_ok}, errors {n_err} "
                          f"({time.time() - t0:,.0f}s)", flush=True)
    if a.phase in ("finalize", "all"):
        finalize(work)
    if a.phase == "reclassify":
        reclassify(work)
    return 0


if __name__ == "__main__":
    sys.exit(main())
