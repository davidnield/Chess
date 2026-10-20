"""Course tables -> course repertoire books (stage 2 of the course benchmarks).

Reads course_ingest.py's tables and writes, per course and repertoire side, a book
in the SAME format as our generated books (position_hash, side_to_move,
best_move, value, event, elo_band), so replay_holdout, the explorer, the opening
namer and the book-value tools work on courses unchanged. Also writes every
candidate move the course gives at each position (course_moves.parquet), which
the comparisons and mix-and-match read.

WHAT COUNTS AS REPERTOIRE (item types from course_ingest.classify):
  line, trap, quickstarter, alternative   every move
  model_game                              moves before --model-ply only (the
                                          opening phase; the rest is middlegame)
  intro, exercise, endgame, unreadable    nothing (their comments stay searchable)

WHICH SIDE. The folder (White/Black Repertoires) decides when present. Otherwise
per chapter: the repertoire side plays about one move per position and the
opponent branches, so the side whose positions branch LESS is the repertoire
side; a chapter with no branching either way takes the course majority. A course
whose chapters split between sides (e.g. a repertoire for Black AND White) yields
two books.

THE RECOMMENDED MOVE at a repertoire-side position is the candidate with the
highest weight, summed over every occurrence:
  main line of a line/trap item 3, its variations 2 (depth 1) or 1 (deeper),
  quickstarter 2, model-game main line 2 / variations 1, alternative item 0.5;
  a "!" or "!!" multiplies by 1.5; "?", "??" or "?!" on OUR move means the course
  is refuting it, weight 0.
Others with positive weight are kept as alternatives; a runner-up at half the
winner's weight or more is flagged as a conflict.

THE BOOK is the tree walked from the start position: at our turn the recommended
move, at the opponent's turn every reply the course covers. Positions the course
reaches only from an unreachable FEN start are counted (orphans) but not booked.
`value` is the engine's expected score of the position (eval arrays, White
perspective), else 0.5; replay_holdout needs a non-null value for membership.

    .venv/Scripts/python.exe python/course_books.py [--courses ID,...]
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from collections import defaultdict
from pathlib import Path

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from zobrist import zobrist_int64  # noqa: E402

WORK = Path("E:/chess/courses")
ROOT_HASH = zobrist_int64(chess.Board())
SCOPE_TYPES = {"line", "trap", "quickstarter", "alternative", "model_game"}
REFUTED = {"?", "??", "?!"}
PRAISED = {"!", "!!"}


def edge_weight(item_type: str, var_depth: int, nags: str | None) -> float:
    if item_type in ("line", "trap"):
        w = 3.0 if var_depth == 0 else (2.0 if var_depth == 1 else 1.0)
    elif item_type == "quickstarter":
        w = 2.0
    elif item_type == "model_game":
        w = 2.0 if var_depth == 0 else 1.0
    elif item_type == "alternative":
        w = 0.5
    else:
        return 0.0
    tags = set((nags or "").split())
    if tags & REFUTED:
        return 0.0
    if tags & PRAISED:
        w *= 1.5
    return w


def weight_expr() -> pl.Expr:
    """edge_weight() as one polars expression (13M edges; a Python UDF per row
    is minutes). _test_course_pipeline holds the two to the same values."""
    it, vd = pl.col("item_type"), pl.col("var_depth")
    base = (pl.when(it.is_in(["line", "trap"]))
            .then(pl.when(vd == 0).then(3.0).when(vd == 1).then(2.0).otherwise(1.0))
            .when(it == "quickstarter").then(2.0)
            .when(it == "model_game").then(pl.when(vd == 0).then(2.0).otherwise(1.0))
            .when(it == "alternative").then(0.5)
            .otherwise(0.0))
    toks = pl.col("nags").fill_null("").str.split(" ")
    refuted = toks.list.eval(pl.element().is_in(list(REFUTED))).list.any()
    praised = toks.list.eval(pl.element().is_in(list(PRAISED))).list.any()
    return (pl.when(refuted).then(0.0)
            .when(praised).then(base * 1.5)
            .otherwise(base))


def chapter_field(items: pl.DataFrame) -> pl.Series:
    """Per item, a chapter key: within each file, the header among (event, site,
    white) that groups items (2+ distinct values, fewer than the items) — the
    publishers put the chapter in different fields; else the file."""
    keys = []
    for (fidx,), grp in items.group_by(["file_idx"], maintain_order=True):
        n = grp.height
        best = None
        for col in ("white", "site", "event"):
            k = grp[col].fill_null("?").n_unique()
            if 2 <= k <= max(2, n // 2):
                best = col
                break
        vals = grp[best].fill_null("?") if best else pl.Series([""] * n)
        keys.append(pl.DataFrame({"course_id": grp["course_id"], "file_idx": grp["file_idx"],
                                  "item_idx": grp["item_idx"],
                                  "chapter": [f"{fidx}|{v}" for v in vals]}))
    return pl.concat(keys)


def infer_sides(ed: pl.DataFrame, hint: str | None) -> dict[str, str]:
    """chapter -> 'w' / 'b'. ed: one course's in-scope edges with a chapter column.

    A folder hint (White/Black Repertoires) decides every chapter: measured on the
    first full build, per-chapter branching split 132 hinted courses into a main
    book plus a spurious 1-3-chapter book of the other colour (~10-100 moves, the
    chapters that discuss the opponent's options). Branching decides only for
    courses without a hint (General Opening Books), which do mix colours."""
    if hint:
        side = "w" if hint == "white" else "b"
        return {ch: side for ch in ed["chapter"].unique().to_list()}
    out, undecided = {}, []
    for (ch,), g in ed.group_by(["chapter"]):
        frac = {}
        for side in ("w", "b"):
            s = g.filter(pl.col("mover") == side).group_by("parent_hash").agg(
                pl.col("move_san").n_unique().alias("k"))
            frac[side] = (s["k"] > 1).mean() if s.height else None
        fw, fb = frac["w"], frac["b"]
        if fw is None or fb is None or (fw == 0 and fb == 0) or abs((fw or 0) - (fb or 0)) < 0.05:
            undecided.append(ch)
        else:
            out[ch] = "w" if fw < fb else "b"
    if hint:
        default = "w" if hint == "white" else "b"
    elif out:
        vals = list(out.values())
        default = max(set(vals), key=vals.count)
    else:
        default = "w"
    for ch in undecided:
        out[ch] = default
    return out


def structure_stats(root: int, our_white: bool, rec: dict, oppm: dict,
                    seen: set) -> dict:
    """Size of a course book over its OWN tree (not the pool's): our moves, the
    number of start-to-end variations and their lengths in our moves. Exact path
    counting by DP over the book DAG; an edge back into the current DFS path (a
    repetition loop) is cut."""
    children = {}
    for h in seen:
        # side to move is not stored per hash here; rec is keyed by our-turn hashes
        if h in rec:
            ch = rec[h][1]
            children[h] = ([ch] if ch in seen else [], True)
        else:
            children[h] = ([c for _m, c in oppm.get(h, ()) if c in seen], False)
    paths, total, longest = {}, {}, {}
    state = {}                      # 1 on the stack, 2 done
    stack = [(root, 0)]
    while stack:
        h, i = stack.pop()
        kids, ours = children.get(h, ([], False))
        if i == 0:
            if state.get(h) == 2:
                continue
            state[h] = 1
        if i < len(kids):
            stack.append((h, i + 1))
            c = kids[i]
            if state.get(c) is None:
                stack.append((c, 0))
            continue
        done = [c for c in kids if state.get(c) == 2]
        step = 1 if ours else 0
        if not done:
            paths[h], total[h], longest[h] = 1, step, step
        else:
            paths[h] = sum(paths[c] for c in done)
            total[h] = sum(total[c] + paths[c] * step for c in done)
            longest[h] = max(longest[c] for c in done) + step
        state[h] = 2
    n = paths.get(root, 0)
    return {"variations": n, "len_mean": (total.get(root, 0) / n) if n else 0.0,
            "len_max": longest.get(root, 0)}


def build_course(cid: str, ed: pl.DataFrame, items: pl.DataFrame, hint: str | None,
                 model_ply: int, evals) -> tuple[list[dict], list[dict], list[dict]]:
    """Books (one per side), the candidate-move table and per-side stats rows."""
    it = items.select("file_idx", "item_idx", "item_type", "chapter")
    e = ed.join(it, on=["file_idx", "item_idx"], how="inner")
    e = e.filter(pl.col("item_type").is_in(list(SCOPE_TYPES)))
    e = e.filter((pl.col("item_type") != "model_game") | (pl.col("ply") < model_ply))
    if not e.height:
        return [], [], []
    sides = infer_sides(e, hint)
    e = e.with_columns(pl.col("chapter").replace_strict(sides, default="w").alias("rep_side"))
    e = e.with_columns(weight_expr().alias("w"))
    books, moves, stats = [], [], []
    for side in sorted(set(sides.values())):
        es = e.filter(pl.col("rep_side") == side)
        if not es.height:
            continue
        ours = es.filter(pl.col("mover") == side)
        opp = es.filter(pl.col("mover") != side)
        cand = (ours.group_by(["parent_hash", "move_san"])
                .agg(pl.col("child_hash").first(), pl.col("w").sum().alias("score"),
                     pl.len().alias("n_occ"),
                     pl.col("nags").drop_nulls().str.join(" ").alias("nags"))
                .sort(["parent_hash", "score", "n_occ", "move_san"],
                      descending=[False, True, True, False]))
        rec, alts, conflict = {}, defaultdict(list), set()
        for ph, grp in cand.group_by("parent_hash", maintain_order=True):
            ph = ph[0] if isinstance(ph, tuple) else ph
            rows = grp.rows(named=True)
            top = rows[0]
            if top["score"] <= 0:
                continue
            rec[ph] = (top["move_san"], top["child_hash"])
            for r in rows[1:]:
                if r["score"] > 0:
                    alts[ph].append(r["move_san"])
                    if r["score"] >= 0.5 * top["score"]:
                        conflict.add(ph)
        omoves = (opp.group_by(["parent_hash", "move_san"])
                  .agg(pl.col("child_hash").first(), pl.len().alias("n_occ")))
        oppm = defaultdict(list)
        for ph, mv, ch, n in omoves.iter_rows():
            oppm[ph].append((mv, ch))
        # walk the book from the start position
        our_white = side == "w"
        seen, book_rows = set(), []
        stack = [(ROOT_HASH, True)]          # (hash, white to move)
        while stack:
            h, white_tm = stack.pop()
            if h in seen:
                continue
            seen.add(h)
            our_turn = white_tm == our_white
            mv = None
            if our_turn:
                if h in rec:
                    mv, ch = rec[h]
                    stack.append((ch, not white_tm))
            else:
                for _mv, ch in oppm.get(h, ()):
                    stack.append((ch, not white_tm))
            if our_turn or oppm.get(h):
                book_rows.append((h, "white" if white_tm else "black", mv))
        # drop opponent-turn leaves with no covered reply and our-turn leaves w/o a move
        keep = [r for r in book_rows if r[2] is not None or r[1] != ("white" if our_white else "black")]
        hashes = np.array([r[0] for r in keep], dtype=np.int64)
        vals = evals(hashes) if len(hashes) else np.array([])
        side_name = "white" if our_white else "black"
        bdf = pl.DataFrame({
            "position_hash": hashes, "side_to_move": [r[1] for r in keep],
            "best_move": [r[2] for r in keep],
            # the course's own child of its move: a course move the pool never
            # recorded still continues the walk (repertoire_stats reads it)
            "best_child": [rec[r[0]][1] if r[2] is not None else None for r in keep],
            "value": [0.5 if (v is None or math.isnan(v)) else float(v) for v in vals],
            "event": "Pooled", "elo_band": 0, "course_id": cid, "rep_side": side_name,
            "alt_moves": [" ".join(alts.get(r[0], [])) or None for r in keep],
            "conflict": [r[0] in conflict for r in keep]})
        books.append({"course_id": cid, "side": side_name, "book": bdf})
        moves.append(cand.with_columns(pl.lit(cid).alias("course_id"),
                                       pl.lit(side_name).alias("rep_side"),
                                       pl.col("parent_hash").is_in(list(seen)).alias("in_book")))
        n_rec_reached = sum(1 for r in keep if r[2] is not None)
        struct = structure_stats(ROOT_HASH, our_white, rec, oppm, seen)
        stats.append({"course_id": cid, "side": side_name,
                      "chapters": sum(1 for s in sides.values() if s == side),
                      "candidate_positions": len(rec),
                      "book_moves": n_rec_reached,
                      "orphan_positions": len(rec) - n_rec_reached,
                      "positions_with_alternatives": len(alts),
                      "conflicts": len(conflict),
                      "opponent_moves_covered": int(omoves.height),
                      **{f"tree_{k}": v for k, v in struct.items()}})
    return books, moves, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", default=str(WORK))
    ap.add_argument("--courses", default=None, help="Comma-separated course_ids (default all).")
    ap.add_argument("--model-ply", type=int, default=30)
    ap.add_argument("--no-evals", action="store_true")
    a = ap.parse_args()
    work = Path(a.work)
    t0 = time.time()
    courses = pl.read_parquet(work / "courses.parquet")
    items = pl.read_parquet(work / "items.parquet")
    edges = pl.read_parquet(work / "edges.parquet",
                            columns=["course_id", "file_idx", "item_idx", "ply", "var_depth",
                                     "parent_hash", "move_san", "child_hash", "mover", "nags"])
    if a.courses:
        keep = a.courses.split(",")
        courses = courses.filter(pl.col("course_id").is_in(keep))
    items = items.join(pl.concat([chapter_field(g) for _, g in items.group_by("course_id")]),
                       on=["course_id", "file_idx", "item_idx"], how="left")
    if a.no_evals:
        evals = lambda hs: np.full(len(hs), np.nan)  # noqa: E731
    else:
        from eval_arrays import MISSING, lookup_evals, open_eval_arrays
        from stage3_backwards_induction import LICHESS_CP_SCALE
        mm = open_eval_arrays()

        def evals(hs):
            cp = lookup_evals(hs, *mm).astype(np.float64)
            es = 1.0 / (1.0 + np.exp(-LICHESS_CP_SCALE * cp))
            return np.where(cp == MISSING, np.nan, es)
    out_dir = work / "books"
    out_dir.mkdir(exist_ok=True)
    all_moves, all_stats = [], []
    by_c = {c: g for (c,), g in edges.group_by(["course_id"])}
    for i, row in enumerate(courses.iter_rows(named=True), 1):
        cid = row["course_id"]
        ed = by_c.get(cid)
        if ed is None:
            continue
        it = items.filter(pl.col("course_id") == cid)
        books, moves, stats = build_course(cid, ed, it, row.get("side_hint"), a.model_ply, evals)
        for b in books:
            p = out_dir / f"{cid}_{b['side']}.parquet"
            b["book"].write_parquet(p)
        all_moves.extend(moves)
        all_stats.extend({**s, "course_name": row["course_name"],
                          "publisher": row["publisher"], "category": row["category"],
                          "subcategory": row["subcategory"]} for s in stats)
        if i % 50 == 0:
            print(f"  [{i}/{courses.height}] ({time.time() - t0:,.0f}s)", flush=True)
    pl.concat(all_moves, how="diagonal_relaxed").write_parquet(work / "course_moves.parquet")
    st = pl.DataFrame(all_stats)
    st.write_parquet(work / "course_books.parquet")
    print(f"{st.height} books from {courses.height} courses in {time.time() - t0:,.0f}s -> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
