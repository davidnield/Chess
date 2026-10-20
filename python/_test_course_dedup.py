"""course_dedup.py on a synthetic course_moves table (2026-10-20).

Four White books and one Black book:
  A  100 moves                      B  A's 100 moves + 10 of its own  (an edition of A)
  C  30 of A's moves + 70 others   D  20 of A's moves only            (a short cut of A)
  E  a Black book with the same (position, move) pairs as A -- other side, never grouped

  A  A and B share 100 moves: jaccard 100/110 >= 0.6 -> one duplicate group, B (the
     larger) represents it
  B  D is 'contained' in A (containment 1.0, jaccard 0.2) and keeps its own group
  C  C overlaps A (jaccard 30/170) without being a duplicate or contained
  D  the Black book is never paired with a White one; rows with in_book false are ignored
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import course_dedup as cd  # noqa: E402

FAIL = 0


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def rows(course, side, hashes, in_book=True):
    return [{"parent_hash": h, "move_san": f"m{h % 7}+", "child_hash": h + 1, "score": 3.0,
             "n_occ": 1, "nags": "", "course_id": course, "rep_side": side,
             "in_book": in_book} for h in hashes]


def main() -> int:
    a = list(range(1000, 1100))
    data = (rows("A", "white", a) + rows("B", "white", a + list(range(5000, 5010)))
            + rows("C", "white", a[:30] + list(range(6000, 6070)))
            + rows("D", "white", a[:20]) + rows("E", "black", a)
            + rows("C", "white", list(range(7000, 7100)), in_book=False))
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "course_moves.parquet"
        pl.DataFrame(data).write_parquet(p)
        con = duckdb.connect()
        ov = cd.overlaps(con, p, min_shared=5)
        sizes = {(c, s): n for c, s, n in con.execute(
            "SELECT course_id, side, count(*) FROM bm GROUP BY ALL").fetchall()}
        con.close()
    ov = ov.with_columns(pl.when(pl.col("jaccard") >= 0.6).then(pl.lit("duplicate"))
                         .when(pl.col("containment") >= 0.8).then(pl.lit("contained"))
                         .otherwise(pl.lit("overlap")).alias("relation"))
    rel = {(a_, b_): r for a_, b_, r in ov.select("a", "b", "relation").iter_rows()}
    g = cd.groups(sizes, ov.filter(pl.col("relation") == "duplicate"))
    gid = {(c, s): (i, rep) for c, s, i, rep in g.select("course_id", "side", "group_id",
                                                         "is_rep").iter_rows()}
    check("A: A and B are one duplicate group, B represents it",
          rel.get(("A", "B")) == "duplicate" and gid[("A", "white")][0] == gid[("B", "white")][0]
          and gid[("B", "white")][1] and not gid[("A", "white")][1], f"{rel}")
    check("B: D is contained in A and keeps its own group",
          rel.get(("A", "D")) == "contained" and gid[("D", "white")][0] != gid[("A", "white")][0])
    check("C: C overlaps A without being grouped", rel.get(("A", "C")) == "overlap"
          and gid[("C", "white")][0] != gid[("A", "white")][0])
    check("D: no cross-side pair; in_book false ignored",
          not any("E" in k for k in rel) and sizes[("C", "white")] == 100, f"{sizes}")
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
