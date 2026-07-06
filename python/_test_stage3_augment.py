"""Synthetic test for --augment-engine (engine-move rescue at forced-losing nodes).

Reproduces the missing-improvement blind spot in miniature (template: _test_gate_eval.py):
at OUR node the ONLY recorded move fails the eval gate, but a legal — never-played —
move reaches a position the FULL eval DB scores acceptably. That good move is absent
from `eval_lookup` (it's outside the input DAG, exactly like the real 6...c6 rescue),
so it is only findable via the full-DB sorted arrays.

Assertions:
  1. augment ON  -> the legal rescue is selected (best_move == the unplayed good move).
  2. augment OFF -> the least-bad recorded move is selected (best_move == the bad move).
  3. augment picks the BEST gate-passing rescue, not just any.
  4. GUARD: at a node whose recorded move already PASSES the gate, augmentation does
     nothing (the recorded move is kept even though a higher-eval unplayed move exists).
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import run_backwards_induction, zobrist_int64

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def edge(parent: chess.Board, san: str, score: float, total: int) -> dict:
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": 0}


def child_hash(parent: chess.Board, san: str) -> int:
    b = parent.copy()
    b.push_san(san)
    return zobrist_int64(b)


def sorted_arrays(pairs: dict[int, float]) -> tuple[np.ndarray, np.ndarray]:
    """Build the (sorted hashes, es) arrays the full-DB augmentation lookup expects."""
    h = np.asarray(list(pairs.keys()), dtype=np.int64)
    e = np.asarray(list(pairs.values()), dtype=np.float32)
    order = np.argsort(h)
    return np.ascontiguousarray(h[order]), np.ascontiguousarray(e[order])


def main() -> None:
    start = chess.Board()

    # ── Case 1: forced-losing node — only recorded move fails the gate ──────────
    # slice_prior ~= 0.50 (one start edge at 0.50); white gate threshold = 0.50-0.25 = 0.25.
    bad_san = "h3"          # recorded, its child eval FAILS the gate
    good_san = "e4"         # legal, UNPLAYED, its child eval PASSES — only in the full DB
    meh_san = "d4"          # legal, UNPLAYED, passes the gate but lower than good
    edges = [edge(start, bad_san, 0.50, 1000)]      # sole recorded continuation
    eval_lookup = {child_hash(start, bad_san): 0.10}  # bad child is engine-lost (fails gate)
    # Full DB: the two unplayed rescues, NOT present in eval_lookup (outside the DAG).
    full_h, full_es = sorted_arrays({
        child_hash(start, good_san): 0.55,   # best rescue (passes: 0.55 >= 0.25)
        child_hash(start, meh_san): 0.30,    # weaker rescue (passes: 0.30 >= 0.25)
    })
    common = dict(prior_strength=0.0, min_move_games=0, eval_lookup=eval_lookup,
                  eval_weight=1.0, require_eval=True, robustness_floor=0.25,
                  gate_metric="eval")
    sh = zobrist_int64(start)

    (_v0, bm_off, *_r0) = run_backwards_induction(edges, "white", **common)
    (_v1, bm_on, *_r1) = run_backwards_induction(
        edges, "white", augment_engine=True,
        full_eval_hashes=full_h, full_eval_es=full_es, **common)

    check(bm_off.get(sh) == bad_san,
          f"augment OFF keeps the least-bad recorded move (root={bm_off.get(sh)!r}, expected {bad_san!r})")
    check(bm_on.get(sh) == good_san,
          f"augment ON selects the unplayed engine rescue (root={bm_on.get(sh)!r}, expected {good_san!r})")
    check(bm_on.get(sh) != meh_san,
          f"augment ON picks the BEST rescue, not the weaker one (root={bm_on.get(sh)!r}, not {meh_san!r})")

    # ── Case 2: GUARD — recorded move passes the gate, augmentation must not fire ─
    ok_san = "d4"           # recorded, child eval PASSES the gate
    tempt_san = "e4"        # unplayed, even higher eval in the full DB — must be IGNORED
    edges2 = [edge(start, ok_san, 0.50, 1000)]
    eval_lookup2 = {child_hash(start, ok_san): 0.60}   # ok child passes (0.60 >= 0.25)
    full_h2, full_es2 = sorted_arrays({child_hash(start, tempt_san): 0.95})
    common2 = dict(common); common2["eval_lookup"] = eval_lookup2
    (_v2, bm_guard, *_r2) = run_backwards_induction(
        edges2, "white", augment_engine=True,
        full_eval_hashes=full_h2, full_eval_es=full_es2, **common2)
    check(bm_guard.get(sh) == ok_san,
          f"GUARD: gate-passing recorded move untouched by augmentation "
          f"(root={bm_guard.get(sh)!r}, expected {ok_san!r}, NOT {tempt_san!r})")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
