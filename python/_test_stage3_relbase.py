"""Synthetic test for --gate-rel-baseline own-eval (full-legal-move baseline for
the relative gate). With us to move, the node's OWN engine eval prices the best
LEGAL move, so a recorded move conceding more than gate_rel_floor vs (own eval -
margin) is gated even when it is the only/best recorded candidate; the emptied
set flows into the --augment-engine rescue, which supplies the unplayed engine
move. Imports the production run_backwards_induction (templates:
_test_stage3_relgate.py, _test_stage3_augment.py).

Fixture (white): start node, sole recorded move h3 whose child evals 0.55 —
passes the ABSOLUTE gate (prior 0.5, floor 0.25). The node's own eval is 0.70:
the engine sees a much better move. Full eval DB carries the unplayed e4 child
at 0.69. own-eval baseline (margin 0.02, rel floor 0.1) -> cutoff 0.58 > 0.55
-> h3 gated -> rescue picks e4.
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
    h = np.asarray(list(pairs.keys()), dtype=np.int64)
    e = np.asarray(list(pairs.values()), dtype=np.float32)
    order = np.argsort(h)
    return np.ascontiguousarray(h[order]), np.ascontiguousarray(e[order])


def main() -> None:
    start = chess.Board()
    sh = zobrist_int64(start)
    edges = [edge(start, "h3", 0.50, 1000)]          # sole recorded move
    ev = {child_hash(start, "h3"): 0.55, sh: 0.70}   # child passes abs gate; own eval high
    full_h, full_es = sorted_arrays({child_hash(start, "e4"): 0.69})
    common = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0,
                  require_eval=True, robustness_floor=0.25, gate_metric="eval",
                  gate_rel_floor=0.1, augment_engine=True,
                  full_eval_hashes=full_h, full_eval_es=full_es)

    # 1. default baseline (candidates): sole recorded move survives its own gate
    (_v, bm, *_r) = run_backwards_induction(edges, "white", eval_lookup=dict(ev), **common)
    check(bm.get(sh) == "h3",
          f"candidates baseline: sole recorded move kept (root={bm.get(sh)!r}, want 'h3')")

    # 2. own-eval baseline: h3 concedes 0.15 > 0.1 vs (0.70 - 0.02) -> gated ->
    #    augmentation supplies the unplayed engine move e4
    (_v, bm, *_r) = run_backwards_induction(
        edges, "white", eval_lookup=dict(ev), gate_rel_baseline="own-eval", **common)
    check(bm.get(sh) == "e4",
          f"own-eval baseline: engine rescue replaces conceding move (root={bm.get(sh)!r}, want 'e4')")

    # 3. own eval within floor of the recorded move -> no cut
    ev3 = dict(ev); ev3[sh] = 0.60          # cutoff 0.60-0.02-0.1 = 0.48 < 0.55
    (_v, bm, *_r) = run_backwards_induction(
        edges, "white", eval_lookup=ev3, gate_rel_baseline="own-eval", **common)
    check(bm.get(sh) == "h3",
          f"own eval within floor: recorded move kept (root={bm.get(sh)!r}, want 'h3')")

    # 4. node's own position UNCOVERED -> baseline never raises -> recorded kept
    ev4 = {child_hash(start, "h3"): 0.55}
    (_v, bm, *_r) = run_backwards_induction(
        edges, "white", eval_lookup=ev4, gate_rel_baseline="own-eval", **common)
    check(bm.get(sh) == "h3",
          f"own eval missing: baseline unchanged (root={bm.get(sh)!r}, want 'h3')")

    # 5. multi-candidate: a non-conceding recorded sibling exists -> the cut keeps
    #    it and NO augmentation is needed (gated stays non-empty)
    edges5 = [edge(start, "h3", 0.50, 1000), edge(start, "a4", 0.50, 1000)]
    ev5 = {child_hash(start, "h3"): 0.55, child_hash(start, "a4"): 0.67, sh: 0.70}
    (_v, bm, *_r) = run_backwards_induction(
        edges5, "white", eval_lookup=ev5, gate_rel_baseline="own-eval", **common)
    check(bm.get(sh) == "a4",
          f"multi-candidate: non-conceding sibling survives the raise (root={bm.get(sh)!r}, want 'a4')")

    # 6. black mirror: after 1.e4, black's sole recorded reply h6 (child 0.45)
    #    concedes vs the node's own eval 0.30 (floor-adjusted cutoff 0.42);
    #    rescue c5 at 0.31 from the full DB.
    e4n = chess.Board(); e4n.push_san("e4")
    eh = zobrist_int64(e4n)
    edges_b = [edge(start, "e4", 0.55, 5000), edge(e4n, "h6", 0.50, 800)]
    # NB: child_hash(start, "e4") IS eh — one entry, the node's own eval.
    ev_b = {child_hash(e4n, "h6"): 0.45, eh: 0.30}
    full_hb, full_esb = sorted_arrays({child_hash(e4n, "c5"): 0.31})
    common_b = dict(common); common_b.update(full_eval_hashes=full_hb, full_eval_es=full_esb)
    (_v, bm, *_r) = run_backwards_induction(
        edges_b, "black", eval_lookup=dict(ev_b), gate_rel_baseline="own-eval", **common_b)
    (_v, bm_off, *_r) = run_backwards_induction(
        edges_b, "black", eval_lookup=dict(ev_b), **common_b)
    check(bm.get(eh) == "c5",
          f"black mirror own-eval: rescue picked (move={bm.get(eh)!r}, want 'c5')")
    check(bm_off.get(eh) == "h6",
          f"black mirror candidates: recorded kept (move={bm_off.get(eh)!r}, want 'h6')")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
