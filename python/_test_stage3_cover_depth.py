"""Synthetic tests for --cover-leave-cost and --cover-mass-shrink.

Two defects found by decomposing why cover0.12 answered 1.d4 d5 2.Bf4 with ...h5:

1. TRUNCATION IS FREE. A node where our book ends contributed 0 covered depth AND
   0 branches, so a subtree that simply STOPS sooner gets a better depth-per-branch
   ratio. Measured: ...h5 scored 0.1672 on depth 5.24 / 30.4 branches ending at mean
   ply 9.9, while ...c5 scored 0.1298 on 8.82 / 66.9 running to ply 17.1 — cover_eff
   tracked shallowness across all six replies. The "a bare leaf scores 0" guard only
   reaches one ply.

2. THE THIN-DATA CORRECTION DID NOT PROPAGATE. --cover-prior shrinks only the final
   ratio at the node being scored, so deep thin nodes carry raw inflated depth into
   every ancestor. --cover-mass-shrink applies the reply-mass coverage c inside the
   recursion instead, needing no prior.

Fixtures keep values equal so the coverage term alone decides.
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import run_backwards_induction, zobrist_int64

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def board_after(*sans: str) -> chess.Board:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def edge(parent: chess.Board, san: str, score: float, total: int) -> dict:
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": 0}


COMMON = dict(prior_strength=0.0, min_move_games=0, eval_weight=0.0,
              robustness_floor=1.0, gate_rel_floor=1.0, cover_weight=1.0)


def shallow_vs_deep() -> list[dict]:
    """a3 -> book ends after one opponent decision. e4 -> a second decision below.

    Both nodes have the same 2 replies at the same counts, so they differ only in
    how far our preparation continues.
    """
    start = board_after()
    sh_n, dp_n = board_after("a3"), board_after("e4")
    edges = [
        edge(start, "a3", 0.50, 10000),
        edge(start, "e4", 0.50, 10000),
        # SHALLOW: opponent decides once, then nothing recorded -> our book ends
        edge(sh_n, "e5", 0.50, 5000),
        edge(sh_n, "d5", 0.50, 5000),
        # DEEP: opponent decides, we reply, opponent decides again — and that second
        # decision is BUSHY. Bushiness is essential: a narrow continuation would raise
        # the ratio on its own and the bug would not appear. Real deep subtrees are
        # bushy (c5 carried 66.9 branches for 8.82 depth), which is exactly why
        # continuing deeper LOWERED the ratio and shallow lines won.
        edge(dp_n, "e5", 0.50, 5000),
        edge(dp_n, "d5", 0.50, 5000),
        edge(board_after("e4", "e5"), "Nf3", 0.50, 5000),
    ] + [edge(board_after("e4", "e5", "Nf3"), san, 0.50, 625)
         for san in ("Nc6", "d6", "Nf6", "f5", "d5", "Bc5", "Qf6", "a6")]
    return edges, zobrist_int64(start)


def main() -> None:
    edges, sh = shallow_vs_deep()

    free_v, free_bm, *_ = run_backwards_induction(edges, "white", cover_leave_cost=0.0,
                                                  **COMMON)
    paid_v, paid_bm, *_ = run_backwards_induction(edges, "white", cover_leave_cost=1.0,
                                                  **COMMON)
    dflt_v, dflt_bm, *_ = run_backwards_induction(edges, "white", **COMMON)

    check(free_bm.get(sh) == "a3",
          f"leaving book FREE prefers the shallow line — the bug "
          f"(root={free_bm.get(sh)!r}, want 'a3')")
    check(paid_bm.get(sh) == "e4",
          f"--cover-leave-cost 1.0 prefers the DEEPER book "
          f"(root={paid_bm.get(sh)!r}, want 'e4')")
    check(free_v[sh] == dflt_v[sh] and free_bm.get(sh) == dflt_bm.get(sh),
          "omitting --cover-leave-cost is bit-identical to 0.0")

    # --cover-mass-shrink: same tree, but one branch's node loses most of the mass
    # that reached it (games ended / replies fell below the pool floor), so its
    # recorded replies overstate how much of the real distribution we cover.
    start = board_after()
    thin_n, dense_n = board_after("a3"), board_after("e4")
    mass_edges = [
        edge(start, "a3", 0.50, 10000),   # 10,000 games arrive...
        edge(start, "e4", 0.50, 10000),   # ...at each
        edge(thin_n, "e5", 0.50, 500),    # only 500 continue  -> c = 0.05
        edge(dense_n, "e5", 0.50, 10000),  # all 10,000 continue -> c = 1.0
    ]
    m0_v, m0_bm, *_ = run_backwards_induction(mass_edges, "white",
                                              cover_mass_shrink=0.0, **COMMON)
    m1_v, m1_bm, *_ = run_backwards_induction(mass_edges, "white",
                                              cover_mass_shrink=1.0, **COMMON)
    md_v, md_bm, *_ = run_backwards_induction(mass_edges, "white", **COMMON)

    check(m0_bm.get(sh) == m0_bm.get(sh) and m0_bm.get(sh) in ("a3", "e4"),
          f"uncorrected: the two look alike (root={m0_bm.get(sh)!r}) — "
          f"identical shares hide that one node lost 95% of its mass")
    check(m1_bm.get(sh) == "e4",
          f"--cover-mass-shrink 1.0 prefers the FULLY-COVERED node "
          f"(root={m1_bm.get(sh)!r}, want 'e4')")
    check(m0_v[sh] == md_v[sh] and m0_bm.get(sh) == md_bm.get(sh),
          "omitting --cover-mass-shrink is bit-identical to 0.0")

    # Both flags together must not resurrect the shallow line.
    both_v, both_bm, *_ = run_backwards_induction(edges, "white", cover_leave_cost=1.0,
                                                  cover_mass_shrink=1.0, **COMMON)
    check(both_bm.get(sh) == "e4",
          f"both corrections together still prefer the deeper book "
          f"(root={both_bm.get(sh)!r}, want 'e4')")

    bad = [lbl for ok, lbl in _checks if not ok]
    print(f"\n{len(_checks) - len(bad)}/{len(_checks)} checks passed")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
