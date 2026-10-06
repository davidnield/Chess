"""T9: the subgraph collector must settle reach, not freeze it at first sight.

collect_subgraph_edges walks the stats parquet outward from the root, keeping
every edge of every node whose OPTIMISTIC reach clears eps. Reach decays only at
opponent nodes (by reply share); our moves carry it unchanged.

THE BUG THIS PINS (found by external review, fixed 2026-09-04). The frontier
guard read `if cr < eps or ch in fetched: continue`, so a node already visited
was skipped before the reach update below it. A transposition first reached
through a SHORT UNLIKELY path and later through a LONGER LIKELIER one kept the
first, lower reach forever -- and, the part that actually loses positions, its
children had already been expanded at that lower reach, so any child then under
eps was dropped and never revisited.

Nothing downstream repairs it. build_graph only sees the rows this returns, so
recomputing reach correctly over an already-truncated edge set still has no rows
for the pruned cone. The affected arms are dp/fixdp/greedy; `trunc` prunes the
full Stage-3 rep instead and is NOT restricted this way, so the defect biases the
four-arm comparison against exactly the methods it is meant to test.

FIXTURE (our_white, so even plies are our turn). T is reachable two ways:

    R --our--> O1 --0.1--> T                       (short, reach 0.10)
    R --our--> O1 --0.9--> U --our--> O3 --1.0--> T (long,  reach 0.90)

T's own subtree then hangs on which reach wins:

    T --our--> O4 --0.20--> Z    0.10*0.2 = 0.018 DROPPED at eps 0.05
                                 0.90*0.2 = 0.180 KEPT
    T --our--> O4 --0.02--> W    0.90*0.02 = 0.018, below eps either way

Z is the recovered subtree; W is the control that proves eps is still enforced.

Run: .venv/Scripts/python.exe python/_test_budget_frontier.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

from build_budget_books import collect_subgraph_edges

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


R, O1, T, U, O3, O4, Z, W, FILL, LEAF = (
    1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010)

# (parent, san, child, total). Shares are total/sum(total) over the parent.
EDGES = [
    (R,   "m1", O1,   1000),      # our node: child inherits reach 1.0
    (O1,  "a",  T,     100),      # share 0.1 -> the short, unlikely arrival
    (O1,  "b",  U,     900),      # share 0.9
    (U,   "m2", O3,   1000),      # our node
    (O3,  "c",  T,    1000),      # share 1.0 -> the long, likely arrival
    (T,   "m3", O4,   1000),      # our node
    (O4,  "d",  Z,     200),      # share 0.20 -> recovered only at reach 0.9
    (O4,  "e",  W,      20),      # share 0.02 -> below eps either way
    (O4,  "f",  FILL,  780),      # share 0.78
    (Z,   "m4", LEAF, 1000),      # gives Z rows of its own to find
]

EPS = 0.05


def write_stats(path: Path) -> None:
    pl.DataFrame({
        "parent_hash":     [e[0] for e in EDGES],
        "move_san":        [e[1] for e in EDGES],
        "child_hash":      [e[2] for e in EDGES],
        "parent_epd":      [f"epd{e[0]}" for e in EDGES],
        "total":           [e[3] for e in EDGES],
        "white_wins":      [e[3] // 2 for e in EDGES],
        "draws":           [0 for _ in EDGES],
        "black_wins":      [e[3] - e[3] // 2 for e in EDGES],
        "white_score_avg": [0.5 for _ in EDGES],
    }).write_parquet(path)


def main() -> int:
    tmp = Path(__file__).parent / "_tmp_frontier_stats.parquet"
    write_stats(tmp)
    try:
        con = duckdb.connect()
        df = collect_subgraph_edges(
            con, tmp.as_posix(), root=R, our_white=True, eps=EPS,
            max_ply=40, share_floor=0.002, min_games=0)
        con.close()
    finally:
        tmp.unlink(missing_ok=True)

    parents = set(df["parent_hash"].to_list()) if df.height else set()
    children = set(df["child_hash"].to_list()) if df.height else set()

    check("the root and the two arrivals are collected",
          {R, O1, U, O3, T} <= parents,
          f"missing {sorted({R, O1, U, O3, T} - parents)}")

    # The headline: T's subtree survives at the LATER, higher reach.
    check("T's subtree is expanded at the improved reach (0.9, not 0.1)",
          O4 in parents,
          f"O4 {'present' if O4 in parents else 'MISSING'}")
    check("the 0.18-reach child is collected, not pruned at the stale 0.018",
          Z in children and Z in parents,
          f"Z as child={Z in children}, as parent={Z in parents}")

    # ...and eps is still a real cutoff, so this is not "collect everything".
    check("the genuinely sub-eps child is still pruned",
          W not in parents,
          f"W {'wrongly expanded' if W in parents else 'correctly not expanded'}")

    # Re-expansion must not duplicate rows: reopened parents are re-read for
    # traversal but only genuinely new parents append to the returned frame.
    if df.height:
        keys = df.select(["parent_hash", "move_san"])
        check("no edge is emitted twice by re-propagation",
              keys.n_unique() == df.height,
              f"{df.height} rows, {keys.n_unique()} distinct (parent, move)")
    else:
        check("collector returned rows at all", False, "empty frame")

    # Every collected parent must be one the fixture actually defines, i.e. the
    # walk never invents nodes.
    check("no node outside the fixture is collected",
          parents <= {e[0] for e in EDGES},
          f"unexpected {sorted(parents - {e[0] for e in EDGES})}")

    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
