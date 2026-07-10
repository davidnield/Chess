"""Synthetic test for the Stage-3 opponent-node eval clamp (--opp-eval-clamp):
at an OPPONENT node whose own position is eval-covered, the empirical mean value
is capped at eval ± margin (mirrored per perspective) — the missing-refutation
guard. Ranking only; gates untouched. Imports the production
run_backwards_induction (template: _test_stage3_relgate.py).

White fixture: root (our move e4) -> node A (opponent). A's two recorded replies
reach leaves evaling 0.80, so A's empirical mean is 0.80; A's OWN eval is 0.50
(the unplayed refutation equalizes). margin 0.2 -> A capped at 0.70.
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


def main() -> None:
    start = board_after()
    sh = zobrist_int64(start)
    a_node = board_after("e4")               # opponent (black to move) for white persp
    ah = zobrist_int64(a_node)
    edges_w = [edge(start, "e4", 0.55, 6000),
               edge(a_node, "e5", 0.55, 3000), edge(a_node, "f5", 0.55, 3000)]
    leaf_evals = {zobrist_int64(board_after("e4", "e5")): 0.80,
                  zobrist_int64(board_after("e4", "f5")): 0.80}
    common = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0)

    # 1. flag off -> pure empirical mean propagates (0.80)
    (v, *_r) = run_backwards_induction(
        edges_w, "white", eval_lookup=dict(leaf_evals), **common)
    check(abs(v[ah] - 0.80) < 1e-9 and abs(v[sh] - 0.80) < 1e-9,
          f"flag off: unclamped mean (A={v[ah]:.3f}, root={v[sh]:.3f}, want 0.800)")

    # 2. clamp 0.2, A's own eval 0.50 -> A capped at 0.70; propagates to root
    ev = dict(leaf_evals); ev[ah] = 0.50
    (v, *_r) = run_backwards_induction(
        edges_w, "white", eval_lookup=ev, opp_eval_clamp=0.2, **common)
    check(abs(v[ah] - 0.70) < 1e-9 and abs(v[sh] - 0.70) < 1e-9,
          f"clamp 0.2: A capped at eval+0.2 (A={v[ah]:.3f}, root={v[sh]:.3f}, want 0.700)")

    # 3. clamp slack when empirical optimism is within the margin (eval 0.75 -> cap 0.95)
    ev = dict(leaf_evals); ev[ah] = 0.75
    (v, *_r) = run_backwards_induction(
        edges_w, "white", eval_lookup=ev, opp_eval_clamp=0.2, **common)
    check(abs(v[ah] - 0.80) < 1e-9,
          f"within margin: untouched (A={v[ah]:.3f}, want 0.800)")

    # 4. A uncovered -> clamp inert even with the flag on
    (v, *_r) = run_backwards_induction(
        edges_w, "white", eval_lookup=dict(leaf_evals), opp_eval_clamp=0.2, **common)
    check(abs(v[ah] - 0.80) < 1e-9,
          f"uncovered node: clamp inert (A={v[ah]:.3f}, want 0.800)")

    # 5. black-side mirror: root (white to move) is the OPPONENT node; empirical
    #    mean 0.30 (great for black), root's own eval 0.55 -> floored at 0.35.
    edges_b = [edge(start, "e4", 0.45, 3000), edge(start, "d4", 0.45, 3000)]
    ev_b = {zobrist_int64(board_after("e4")): 0.30,
            zobrist_int64(board_after("d4")): 0.30, sh: 0.55}
    (v, *_r) = run_backwards_induction(
        edges_b, "black", eval_lookup=ev_b, opp_eval_clamp=0.2, **common)
    check(abs(v[sh] - 0.35) < 1e-9,
          f"black mirror: floored at eval-0.2 (root={v[sh]:.3f}, want 0.350)")
    (v, *_r) = run_backwards_induction(
        edges_b, "black", eval_lookup=ev_b, **common)
    check(abs(v[sh] - 0.30) < 1e-9,
          f"black flag off: unclamped (root={v[sh]:.3f}, want 0.300)")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
