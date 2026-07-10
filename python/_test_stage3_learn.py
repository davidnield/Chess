"""Synthetic test for the Stage-3 LEARNABILITY tiebreak (--plan-prior /
--plan-reach): among gate-passing candidates within a δ window of the best
selection key, the most HABITUAL idea (highest plan-prior game frequency) is
picked; δ is tight at common nodes and loose at rare ones. Imports the
production run_backwards_induction (template: _test_stage3_relgate.py).

White fixture: root with two leaf moves, both eval-covered, gates disabled.
  e4 -> es 0.60, habitual (global prior 0.9)
  b4 -> es 0.62, unusual  (no prior entry -> freq 0)
  gap 0.02 sits BETWEEN delta_main (0.005) and delta_rare (0.04):
  a rare root prefers the habit, a common root stays sharp.
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import (LEARN_GLOBAL_CTX, idea_token,
                                        run_backwards_induction, zobrist_int64)

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


def main() -> None:
    start = board_after()
    sh = zobrist_int64(start)
    habit_child   = board_after("e4")
    unusual_child = board_after("b4")
    edges_w = [edge(start, "e4", 0.55, 5000), edge(start, "b4", 0.55, 5000)]
    eval_w = {zobrist_int64(habit_child): 0.60,
              zobrist_int64(unusual_child): 0.62}      # b4 is the raw argmax by 0.02
    prior = {(LEARN_GLOBAL_CTX, "e4"): 0.9}
    common = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0,
                  require_eval=True, eval_lookup=eval_w)

    # 1. learn off -> raw argmax
    (_v, bm, *_r) = run_backwards_induction(edges_w, "white", **common)
    check(bm.get(sh) == "b4", f"learn off: raw argmax (root={bm.get(sh)!r}, want 'b4')")

    # 2. rare node (no reach entry -> delta_rare 0.04 >= gap) -> habit wins
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_prior=prior, learn_ctx={}, learn_reach={}, **common)
    check(bm.get(sh) == "e4", f"rare node: habit overrides 0.02 gap (root={bm.get(sh)!r}, want 'e4')")

    # 3. common node (reach 0.05 >= pivot -> delta_main 0.005 < gap) -> stays sharp
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_prior=prior, learn_ctx={}, learn_reach={sh: 0.05}, **common)
    check(bm.get(sh) == "b4", f"common node: 0.02 gap kept sharp (root={bm.get(sh)!r}, want 'b4')")

    # 4. common node, tiny gap (0.003 < delta_main) -> habit wins even in main lines
    eval_tiny = {zobrist_int64(habit_child): 0.60,
                 zobrist_int64(unusual_child): 0.603}
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_prior=prior, learn_ctx={}, learn_reach={sh: 0.05},
        **{**common, "eval_lookup": eval_tiny})
    check(bm.get(sh) == "e4", f"common node: habit wins tiny gap (root={bm.get(sh)!r}, want 'e4')")

    # 5. rare node, huge gap (0.08 > delta_rare) -> performance dominates
    eval_huge = {zobrist_int64(habit_child): 0.60,
                 zobrist_int64(unusual_child): 0.68}
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_prior=prior, learn_ctx={}, learn_reach={},
        **{**common, "eval_lookup": eval_huge})
    check(bm.get(sh) == "b4", f"rare node: 0.08 gap beats habit (root={bm.get(sh)!r}, want 'b4')")

    # 6. ctx-specific prior beats the global fallback: this node's context prefers
    #    b4 (0.95) over globally-habitual e4 (0.9); e4 is the argmax; rare delta
    #    covers the gap -> pick b4 via the ctx row.
    eval_flip = {zobrist_int64(habit_child): 0.62,
                 zobrist_int64(unusual_child): 0.60}
    prior_ctx = {(LEARN_GLOBAL_CTX, "e4"): 0.9, ("d4", "b4"): 0.95}
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_prior=prior_ctx, learn_ctx={sh: "d4"}, learn_reach={},
        **{**common, "eval_lookup": eval_flip})
    check(bm.get(sh) == "b4", f"ctx prior beats global (root={bm.get(sh)!r}, want 'b4')")

    # 7. black-side sign handling: f5 is the raw argmax for black (lower white
    #    score); habitual e5 wins at a rare node within delta_rare.
    e4_node = board_after("e4")
    eh = zobrist_int64(e4_node)
    edges_b = [edge(start, "e4", 0.55, 6000),
               edge(e4_node, "e5", 0.45, 3000), edge(e4_node, "f5", 0.45, 3000)]
    eval_b = {zobrist_int64(board_after("e4", "e5")): 0.40,
              zobrist_int64(board_after("e4", "f5")): 0.38}
    prior_b = {(LEARN_GLOBAL_CTX, "e5"): 0.9}
    (_v, bm, *_r) = run_backwards_induction(
        edges_b, "black", learn_prior=prior_b, learn_ctx={}, learn_reach={},
        **{**common, "eval_lookup": eval_b})
    (_v, bm_off, *_r) = run_backwards_induction(
        edges_b, "black", **{**common, "eval_lookup": eval_b})
    check(bm.get(eh) == "e5", f"black rare node: habit wins (move={bm.get(eh)!r}, want 'e5')")
    check(bm_off.get(eh) == "f5", f"black learn off: raw argmax (move={bm_off.get(eh)!r}, want 'f5')")

    # 8. token sanity for the shared vocabulary
    check(idea_token("Nbd2") == "Nd2" and idea_token("cxd5") == "cxd5",
          "shared idea_token semantics")

    # 9./10. ctx-share shrinkage (the 1.b3 self-reinforcement fix): in a RARE
    #    context the ctx-local habit (b4: 0.6) is shrunk toward the global habit
    #    (e4: 0.8) -> the globally-normal move wins; in a COMMON context the
    #    ctx-local habit stands.
    prior_shrunk = {(LEARN_GLOBAL_CTX, "e4"): 0.8,
                    ("b3", "b4"): 0.6, ("b3", "e4"): 0.3}
    kw = dict(learn_prior=prior_shrunk, learn_ctx={sh: "b3"}, learn_reach={},
              **{**common, "eval_lookup": eval_w})   # b4 argmax by 0.02, rare δ covers
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_ctx_share={"b3": 0.011}, **kw)
    check(bm.get(sh) == "e4",
          f"rare ctx shrinks to global habit (root={bm.get(sh)!r}, want 'e4')")
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_ctx_share={"b3": 0.30}, **kw)
    check(bm.get(sh) == "b4",
          f"common ctx keeps its own habit (root={bm.get(sh)!r}, want 'b4')")

    # 11./12./13. depth gating: with a depth table supplied, the loose δ applies
    #    only to SHALLOW nodes (min_our_depth < horizon); deep nodes and nodes
    #    ABSENT from the walk fall back to the tight δ (the v1 compounding fix).
    kw_d = dict(learn_prior=prior, learn_ctx={}, learn_reach={}, **common)
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_depth={sh: 2}, **kw_d)
    check(bm.get(sh) == "e4",
          f"shallow rare node: loose δ, habit wins (root={bm.get(sh)!r}, want 'e4')")
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_depth={sh: 8}, **kw_d)
    check(bm.get(sh) == "b4",
          f"deep node: tight δ, stays sharp (root={bm.get(sh)!r}, want 'b4')")
    (_v, bm, *_r) = run_backwards_induction(
        edges_w, "white", learn_depth={}, **kw_d)
    check(bm.get(sh) == "b4",
          f"off-walk node: tight δ, stays sharp (root={bm.get(sh)!r}, want 'b4')")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
