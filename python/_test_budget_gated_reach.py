"""--gated-reach (2026-10-18): the budget subgraph expands, and optimistic reach
flows, only through moves the gates admit at our nodes.

FIXTURE (our_white, slice prior 0.5, robustness floor 0.1 -> a white move needs
child eval >= 0.4):

    R --good--> O1 (eval 0.55) --0.1--> X --x1--> O3 --1.0--> Z
              \\                --0.9--> Y
               --bad--> O2 (eval 0.20, refuted) --1.0--> X

  A  default (policy-free): X gets reach 1.0 through the refuted move, O2 is a node.
  B  gated: X's reach is 0.1 (through O1 only); O2 is not a node; R's choice set
     is the same in both (only 'good' passes).
  C  the source book's move is always admitted: if the book plays 'bad', gated
     reach flows through it again.
  D  the collector with an admit hook never expands O2, still returns both of R's
     recorded moves (the stop value reads them all), and still reaches X via O1.
  E  gate_candidates is the one definition: build_graph's root candidates equal it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
from build_budget_books import collect_subgraph_edges  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


R, O1, O2, X, Y, O3, Z = 2001, 2002, 2003, 2004, 2005, 2006, 2007
EDGES = [(R, "good", O1, 1000), (R, "bad", O2, 1000),
         (O1, "a", X, 100), (O1, "b", Y, 900),
         (O2, "c", X, 1000),
         (X, "x1", O3, 1000),
         (O3, "d", Z, 1000)]
EVALS = {O1: 0.55, O2: 0.20, O3: 0.50, X: 0.5, Y: 0.5, Z: 0.5, R: 0.52}


def by_parent():
    out = {}
    for p, s, c, t in EDGES:
        out.setdefault(p, []).append({"move_san": s, "child_hash": c, "total": t,
                                      "white_score_avg": 0.5, "parent_epd": f"e{p}"})
    return out


def graph(**kw):
    return bc.build_graph(by_parent(), R, True, eval_ws=EVALS, slice_prior=0.5,
                          eps=0.05, max_ply=40, **kw)


class StubAdmit:
    """The collector's hook, with the real gate over the fixture's evals."""

    def __init__(self, rep_moves=None):
        self.rep_moves = rep_moves or {}
        self.prefetched = 0

    def prefetch(self, hashes):
        self.prefetched += 1

    def __call__(self, ph, rows):
        return {e["child_hash"] for e, _ in bc.gate_candidates(
            ph, rows, self.rep_moves.get(ph), EVALS, True, 0.5)}


def main() -> int:
    g0 = graph()
    check("A: policy-free reach flows through the refuted move",
          abs(g0.reach.get(X, 0) - 1.0) < 1e-12 and O2 in g0.opp,
          f"reach X {g0.reach.get(X)}, O2 node {O2 in g0.opp}")

    g1 = graph(gated_reach=True)
    check("B: gated reach reaches X only through O1 (0.1)",
          abs(g1.reach.get(X, 0) - 0.1) < 1e-12, f"reach X {g1.reach.get(X)}")
    check("B: the refuted move's opponent node is not in the gated graph",
          O2 not in g1.opp and O2 not in g1.reach)
    c0 = [c[0] for c in g0.our[R].cands]
    c1 = [c[0] for c in g1.our[R].cands]
    check("B: the root's choice set is unchanged", c0 == c1 == ["good"], f"{c0} vs {c1}")

    g2 = graph(gated_reach=True, rep_moves={R: "bad"})
    check("C: the source book's move is always admitted",
          O2 in g2.opp and abs(g2.reach.get(X, 0) - 1.0) < 1e-12,
          f"O2 node {O2 in g2.opp}, reach X {g2.reach.get(X)}")

    tmp = Path(__file__).parent / "_tmp_gated_reach_stats.parquet"
    pl.DataFrame({
        "parent_hash": [e[0] for e in EDGES], "move_san": [e[1] for e in EDGES],
        "child_hash": [e[2] for e in EDGES], "parent_epd": [f"e{e[0]}" for e in EDGES],
        "total": [e[3] for e in EDGES], "white_wins": [e[3] // 2 for e in EDGES],
        "draws": [0 for _ in EDGES], "black_wins": [e[3] - e[3] // 2 for e in EDGES],
        "white_score_avg": [0.5 for _ in EDGES]}).write_parquet(tmp)
    try:
        con = duckdb.connect()
        stub = StubAdmit()
        df = collect_subgraph_edges(con, tmp.as_posix(), root=R, our_white=True,
                                    eps=0.05, max_ply=40, share_floor=0.002,
                                    min_games=0, admit=stub)
        df_all = collect_subgraph_edges(con, tmp.as_posix(), root=R, our_white=True,
                                        eps=0.05, max_ply=40, share_floor=0.002,
                                        min_games=0)
        con.close()
    finally:
        tmp.unlink(missing_ok=True)
    parents = set(df["parent_hash"].to_list())
    check("D: the collector never expands the refuted move's node",
          O2 not in parents and O2 in set(df_all["parent_hash"].to_list()))
    check("D: both of R's recorded moves are still returned",
          set(df.filter(pl.col("parent_hash") == R)["move_san"].to_list()) == {"good", "bad"})
    check("D: X is still reached through O1, and its subtree collected",
          {X, O3} <= parents and stub.prefetched > 0)

    gc = bc.gate_candidates(R, by_parent()[R], None, EVALS, True, 0.5)
    check("E: gate_candidates matches build_graph's root choice set",
          [e["move_san"] for e, _ in gc] == c1)

    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
