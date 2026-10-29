"""Group duplicate courses by the moves their books share (course benchmarks).

The library holds some material more than once: a Chessable course and its book
edition, a short cut of a full course, an update of an older one. Comparing courses,
or mixing them, should count each body of analysis once. For every pair of
same-side course books, over their (position, move) choices (course_moves rows with
in_book, check marks stripped):

  shared        |A and B|
  jaccard       shared / |A or B|
  containment   shared / min(|A|, |B|)     a short edition inside a long one

Pairs with jaccard >= --dup-jaccard join one duplicate group (connected
components). A pair below that with containment >= --contain is `contained`: the
smaller book is largely an excerpt of the larger, kept as its own course. Writes
<work>/course_overlap.parquet (every pair sharing >= --min-shared moves) and
<work>/course_groups.parquet (course_id, side, group_id, group_size, is_rep -- the
largest book of its group stands for it).

    .venv/Scripts/python.exe python/course_dedup.py [--dup-jaccard 0.6] [--contain 0.8]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import polars as pl

from duck_compat import duck_pl

WORK = Path("E:/chess/courses")


def _p(path) -> str:
    return str(path).replace("\\", "/")


def overlaps(con, course_moves: str, min_shared: int) -> pl.DataFrame:
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bm AS
        SELECT DISTINCT course_id, rep_side AS side, parent_hash,
               rtrim(move_san, '+#') AS m
        FROM read_parquet('{_p(course_moves)}') WHERE in_book""")
    return duck_pl(con, f"""
        WITH sz AS (SELECT course_id, side, count(*) AS n FROM bm GROUP BY ALL),
        pr AS (SELECT x.course_id AS a, y.course_id AS b, x.side, count(*) AS shared
               FROM bm x JOIN bm y ON x.side = y.side AND x.parent_hash = y.parent_hash
                                  AND x.m = y.m AND x.course_id < y.course_id
               GROUP BY ALL HAVING count(*) >= {int(min_shared)})
        SELECT pr.*, sa.n AS n_a, sb.n AS n_b,
               shared / (sa.n + sb.n - shared) AS jaccard,
               shared / least(sa.n, sb.n) AS containment
        FROM pr JOIN sz sa ON sa.course_id = pr.a AND sa.side = pr.side
                JOIN sz sb ON sb.course_id = pr.b AND sb.side = pr.side
        ORDER BY jaccard DESC""")


def groups(sizes: dict[tuple[str, str], int], dup: pl.DataFrame) -> pl.DataFrame:
    parent = {k: k for k in sizes}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for a, b, side in dup.select("a", "b", "side").iter_rows():
        ra, rb = find((a, side)), find((b, side))
        if ra != rb:
            parent[ra] = rb
    members: dict = {}
    for k in sizes:
        members.setdefault(find(k), []).append(k)
    rows = []
    for gi, (_root, ms) in enumerate(sorted(members.items(), key=lambda kv: -len(kv[1]))):
        rep = max(ms, key=lambda k: sizes[k])
        for k in ms:
            rows.append({"course_id": k[0], "side": k[1], "group_id": gi,
                         "group_size": len(ms), "is_rep": k == rep,
                         "book_moves": sizes[k]})
    return pl.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", default=str(WORK))
    ap.add_argument("--dup-jaccard", type=float, default=0.6)
    ap.add_argument("--contain", type=float, default=0.8)
    ap.add_argument("--min-shared", type=int, default=20)
    a = ap.parse_args()
    work = Path(a.work)
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='8GB'")
    ov = overlaps(con, work / "course_moves.parquet", a.min_shared)
    sizes = {(c, s): n for c, s, n in con.execute(
        "SELECT course_id, side, count(*) FROM bm GROUP BY ALL").fetchall()}
    con.close()
    ov = ov.with_columns(pl.when(pl.col("jaccard") >= a.dup_jaccard).then(pl.lit("duplicate"))
                         .when(pl.col("containment") >= a.contain).then(pl.lit("contained"))
                         .otherwise(pl.lit("overlap")).alias("relation"))
    ov.write_parquet(work / "course_overlap.parquet")
    g = groups(sizes, ov.filter(pl.col("relation") == "duplicate"))
    g.write_parquet(work / "course_groups.parquet")
    names = pl.read_parquet(work / "courses.parquet", columns=["course_id", "course_name"])
    nm = dict(names.iter_rows())
    dupg = g.filter(pl.col("group_size") > 1).sort(["group_id", "book_moves"],
                                                     descending=[False, True])
    print(f"{len(sizes)} course books; {ov.height:,} pairs share >= {a.min_shared} moves: "
          + ", ".join(f"{r} {n}" for r, n in ov["relation"].value_counts().iter_rows()))
    print(f"{dupg['group_id'].n_unique()} duplicate groups hold {dupg.height} books "
          f"(jaccard >= {a.dup_jaccard}); {g.filter(pl.col('is_rep')).height} distinct books")
    for gid, grp in dupg.group_by("group_id", maintain_order=True):
        print(f"\n group {gid[0]} ({grp['side'][0]})")
        for r in grp.iter_rows(named=True):
            print(f"   {'*' if r['is_rep'] else ' '} {r['book_moves']:>6,}  "
                  f"{nm.get(r['course_id'], r['course_id'])[:100]}")
    con_ = ov.filter(pl.col("relation") == "contained").head(30)
    if con_.height:
        print(f"\ncontained (smaller book mostly inside the larger), top {con_.height}:")
        for r in con_.iter_rows(named=True):
            small, big = (r["a"], r["b"]) if r["n_a"] <= r["n_b"] else (r["b"], r["a"])
            print(f"   {r['containment']:.2f} of {min(r['n_a'], r['n_b']):,}: "
                  f"{nm.get(small, small)[:60]}  IN  {nm.get(big, big)[:60]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
