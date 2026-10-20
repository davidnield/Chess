"""--allowed-moves (mix and match, 2026-10-20): a budget book may play only the moves
a list allows -- e.g. the moves some course recommends -- and stops elsewhere.

FIXTURE (white book, gates off so every recorded move is a candidate):

    R --e4--> O1 --e5--> X1 --Nf3--> O4 --a--> Z1
                         X1 --Bc4--> O5 --b--> Z2
              O1 --c5--> X2 --Nc3--> O6 --c--> Z3
    R --d4+-> O2 --d5--> X3 --c4---> O7 --d--> Z4
    R --c4--> O3 --e5--> X4

  course_moves rows: R e4 (3, course A), R d4 (2, B), R c4 (0: marked '?'),
  X1 Bc4 (1, B), X1 Nf3 (2, a BLACK course), X3 c4 (2, A)

  A  load_allowed_moves keeps the book side's moves scoring > 0, strips check marks
     ("d4" matches the pool's "d4+"), --allowed-courses narrows it, and
     --allowed-book-only keeps each course's own choice (in_book) only
  B  AllowedAdmit expands only allowed moves, nothing at an unlisted position, and
     intersects with the gates' admission when it wraps one
  C  the collector never expands a disallowed move (c4 at R, Nf3 at X1) and still
     returns every recorded move of a node it reached
  D  restrict_candidates leaves only allowed candidates, empties X2 (no course move
     there), and a price book over the restricted graph books only allowed moves
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
from build_budget_books import (AllowedAdmit, collect_subgraph_edges,  # noqa: E402
                                load_allowed_moves, restrict_candidates)

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


(R, O1, O2, O3, X1, X2, X3, X4, O4, O5, O6, O7,
 Z1, Z2, Z3, Z4) = range(4001, 4017)
EDGES = [(R, "e4", O1, 600), (R, "d4+", O2, 300), (R, "c4", O3, 100),
         (O1, "e5", X1, 400), (O1, "c5", X2, 200), (O2, "d5", X3, 300),
         (O3, "e5", X4, 100),
         (X1, "Nf3", O4, 300), (X1, "Bc4", O5, 100), (X2, "Nc3", O6, 200),
         (X3, "c4", O7, 300),
         (O4, "a", Z1, 300), (O5, "b", Z2, 100), (O6, "c", Z3, 200), (O7, "d", Z4, 300)]
SCORE = {O4: 0.62, O5: 0.55, O6: 0.60, O7: 0.58, O3: 0.70}


def by_parent():
    out = {}
    for p, s, c, t in EDGES:
        out.setdefault(p, []).append({"move_san": s, "child_hash": c, "total": t,
                                      "white_score_avg": SCORE.get(c, 0.5),
                                      "parent_epd": f"e{p}"})
    return out


def main() -> int:
    here = Path(__file__).parent
    cm = here / "_tmp_allowed_course_moves.parquet"
    pl.DataFrame({"parent_hash": [R, R, R, X1, X1, X3],
                  "move_san": ["e4", "d4", "c4", "Bc4", "Nf3", "c4"],
                  "score": [3.0, 2.0, 0.0, 1.0, 2.0, 2.0],
                  "course_id": ["A", "B", "A", "B", "C", "A"],
                  "rep_side": ["white", "white", "white", "white", "black", "white"],
                  "in_book": [True, False, True, True, True, True]}
                 ).write_parquet(cm)
    try:
        allowed = load_allowed_moves(cm.as_posix(), "white")
        only_a = load_allowed_moves(cm.as_posix(), "white", courses=["A"])
        book_only = load_allowed_moves(cm.as_posix(), "white", book_only=True)
    finally:
        cm.unlink(missing_ok=True)
    check("A: side, score > 0 and check marks",
          allowed == {R: {"e4", "d4"}, X1: {"Bc4"}, X3: {"c4"}}, f"{allowed}")
    check("A: --allowed-courses narrows it", only_a == {R: {"e4"}, X3: {"c4"}}, f"{only_a}")
    check("A: --allowed-book-only keeps in_book moves",
          book_only == {R: {"e4"}, X1: {"Bc4"}, X3: {"c4"}}, f"{book_only}")

    bp = by_parent()
    adm = AllowedAdmit(allowed)
    check("B: allowed moves only, nothing at an unlisted position",
          adm(R, bp[R]) == {O1, O2} and adm(X1, bp[X1]) == {O5} and adm(X2, bp[X2]) == set())

    class Gate:
        def prefetch(self, hashes):
            pass

        def __call__(self, ph, rows):
            return {O1}
    check("B: intersected with the gates' admission",
          AllowedAdmit(allowed, Gate())(R, bp[R]) == {O1})

    tmp = here / "_tmp_allowed_stats.parquet"
    pl.DataFrame({
        "parent_hash": [e[0] for e in EDGES], "move_san": [e[1] for e in EDGES],
        "child_hash": [e[2] for e in EDGES], "parent_epd": [f"e{e[0]}" for e in EDGES],
        "total": [e[3] for e in EDGES], "white_wins": [e[3] // 2 for e in EDGES],
        "draws": [0 for _ in EDGES], "black_wins": [e[3] - e[3] // 2 for e in EDGES],
        "white_score_avg": [SCORE.get(e[2], 0.5) for e in EDGES]}).write_parquet(tmp)
    try:
        con = duckdb.connect()
        df = collect_subgraph_edges(con, tmp.as_posix(), root=R, our_white=True,
                                    eps=0.01, max_ply=40, share_floor=0.002,
                                    min_games=0, admit=AllowedAdmit(allowed))
        con.close()
    finally:
        tmp.unlink(missing_ok=True)
    parents = set(df["parent_hash"].to_list())
    check("C: disallowed moves are never expanded",
          O3 not in parents and O4 not in parents and {O1, O2, O5, X1, X3, O7} <= parents,
          f"{sorted(parents)}")
    check("C: every recorded move of a reached node is returned",
          set(df.filter(pl.col("parent_hash") == R)["move_san"].to_list())
          == {"e4", "d4+", "c4"})

    g = bc.build_graph(bp, R, True, eval_ws={}, slice_prior=0.5, eps=0.01,
                       max_ply=40, require_eval=False, gates=False)
    cnt = restrict_candidates(g, allowed)
    cands = {h: sorted(c[0] for c in g.our[h].cands) for h in (R, X1, X2, X3)}
    check("D: only allowed candidates remain; X2 is emptied",
          cands == {R: ["d4+", "e4"], X1: ["Bc4"], X2: [], X3: ["c4"]}
          and cnt["nodes_emptied"] >= 1, f"{cands}, {cnt}")
    res, _info = bc.price_search(g, 4, log=lambda *_: None)
    booked = {h: m.rstrip("+#") for h, m in res["booked"].items()}
    ok = all(m in allowed.get(h, ()) for h, m in booked.items()) and booked
    check("D: a price book over the restricted graph books only allowed moves",
          bool(ok), f"{booked}")

    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
