"""Which courses have prose on the positions of a book? (course benchmarks, goal 1)

Walks a book (ours or a course's) against the 1800+ pool to get each position's
reach, then joins course_ingest's comments table on the position hash, so a
comment is found however the course reached the position (transpositions
included). Ranks courses by reach-weighted coverage of the book's positions and
writes the matched comments, grouped by position in reach order, to a local
text file (course text stays on this machine).

    .venv/Scripts/python.exe python/course_comments.py --book PATH --colour white \\
        [--out E:/chess/courses/notes_<name>.txt] [--top 25] [--side-match]

--side-match keeps only courses whose repertoire side matches the book's colour
(a White book reads White courses); by default every course counts, since a
Black course's comment on a position can still teach the White player.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from repertoire_stats import POOL, ROOT_HASH, PoolIndex, load_book  # noqa: E402

WORK = Path("E:/chess/courses")


def book_reach(book: dict, colour: str, idx: PoolIndex, min_reach: float = 1e-7,
               max_depth: int = 120) -> tuple[dict[int, float], dict[int, list[str]]]:
    """Reach of every position the book walk visits (summed over paths) and one
    SAN path to each (the first found), for display."""
    our_white = colour == "white"
    moves, child_of, rows = book["moves"], book["child"], book["rows"]
    reach: dict[int, float] = defaultdict(float)
    path_of: dict[int, list[str]] = {}
    stack = [(ROOT_HASH, True, 1.0, [], frozenset([ROOT_HASH]))]
    while stack:
        h, wtm, r, path, onpath = stack.pop()
        reach[h] += r
        path_of.setdefault(h, path)
        if len(onpath) > max_depth:
            continue
        if wtm == our_white:
            m = moves.get(h)
            if m is None:
                continue
            ch = child_of.get(h)
            if ch is None:
                ch = next((c for s, c, _t in idx.edges(h) if s == m), None)
            if ch is not None and ch not in onpath:
                stack.append((ch, not wtm, r, path + [m], onpath | {ch}))
        else:
            es = idx.edges(h)
            tot = sum(t for *_, t in es)
            for s, ch, t in es:
                rr = r * t / tot if tot else 0.0
                if ch in rows and ch not in onpath and rr >= min_reach:
                    stack.append((ch, not wtm, rr, path + [s], onpath | {ch}))
    return dict(reach), path_of


def fmt(path: list[str]) -> str:
    b, out = chess.Board(), []
    for s in path:
        out.append((f"{b.fullmove_number}." if b.turn else f"{b.fullmove_number}...") + s)
        try:
            b.push_san(s)
        except ValueError:
            break
    return " ".join(out) or "(start)"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--book", required=True)
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--work", default=str(WORK))
    ap.add_argument("--pool", default=POOL)
    ap.add_argument("--out", default=None)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--side-match", action="store_true")
    ap.add_argument("--deep-ply", type=int, default=6,
                    help="Rank by coverage of positions at least this many plies in "
                         "(default 6): the first moves are commented by hundreds of "
                         "courses and carry most of the reach, so they say nothing "
                         "about who covers THIS book's lines.")
    a = ap.parse_args()
    work = Path(a.work)
    idx = PoolIndex(a.pool)
    book = load_book(a.book, a.colour)
    reach, path_of = book_reach(book, a.colour, idx)
    total = sum(reach.values())
    pos = pl.DataFrame({"position_hash": list(reach), "reach": list(reach.values()),
                        "book_ply": [len(path_of.get(h, [])) for h in reach]})
    deep_total = sum(r for h, r in reach.items() if len(path_of.get(h, [])) >= a.deep_ply)
    com = (pl.scan_parquet(work / "comments.parquet").drop("ply")
           .join(pos.lazy(), on="position_hash", how="inner").collect())
    courses = pl.read_parquet(work / "courses.parquet").select(
        "course_id", "course_name", "side_hint", "category")
    books = pl.read_parquet(work / "course_books.parquet").select("course_id", "side")
    com = com.join(courses, on="course_id", how="left")
    if a.side_match:
        com = com.join(books, on="course_id", how="inner").filter(pl.col("side") == a.colour)
    if not com.height:
        print("no course comments on this book's positions")
        return 0
    per = (com.group_by("course_id", "course_name")
           .agg(pl.col("position_hash").n_unique().alias("positions"),
                pl.col("text").str.len_chars().sum().alias("chars"))
           .join(com.unique(["course_id", "position_hash"]).group_by("course_id")
                 .agg(pl.col("reach").sum().alias("reach_covered"),
                      pl.col("reach").filter(pl.col("book_ply") >= a.deep_ply).sum()
                      .alias("deep_reach"),
                      (pl.col("book_ply") >= a.deep_ply).sum().alias("deep_positions")),
                 on="course_id")
           .with_columns((pl.col("reach_covered") / total).alias("share_of_book_reach"),
                         (pl.col("deep_reach") / deep_total).alias("share_of_deep_reach"))
           .sort("deep_reach", descending=True))
    print(f"book: {len(reach):,} positions; {com['position_hash'].n_unique():,} of them carry "
          f"course comments from {per.height} courses")
    print(f"ranked by coverage of positions {a.deep_ply}+ plies in (share of their reach)")
    print(f"{'deep':>7} {'deep pos':>8} {'all pos':>8} {'chars':>9}  course")
    for r in per.head(a.top).iter_rows(named=True):
        print(f"{r['share_of_deep_reach'] * 100:6.1f}% {r['deep_positions']:8,} "
              f"{r['positions']:8,} {r['chars']:9,}  "
              f"{r['course_name'][:90]}")
    out = Path(a.out) if a.out else work / f"notes_{Path(a.book).stem}.txt"
    by_pos = defaultdict(list)
    for r in com.sort("reach", descending=True).iter_rows(named=True):
        by_pos[r["position_hash"]].append(r)
    with open(out, "w", encoding="utf-8") as fh:
        for h in sorted(by_pos, key=lambda x: -reach[x]):
            fh.write(f"\n=== {reach[h] * 100:.2f}% of games | {fmt(path_of.get(h, []))}\n")
            for r in by_pos[h]:
                mv = f" after {r['move_san']}" if r["move_san"] else ""
                fh.write(f"  [{r['course_name'][:70]}]{mv}: {r['text']}\n")
    print(f"comments by position -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
