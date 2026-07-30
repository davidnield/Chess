"""Synthetic test for --gate-anchor: the ABSOLUTE refutation gate's reference point.

The gate has always been measured from slice_prior, the slice's empirical white
score. On the real 2019-25 pool that is 0.5183, which makes the gate ASYMMETRIC
BY COLOUR: at --robustness-floor 0.1 White must stay above 0.4183 (-89.5cp)
while Black may go up to 0.6183 (+131.0cp) — Black is allowed to book positions
41.5cp worse than White is. --gate-anchor even puts both colours on 0.5.

Each fixture gives the move under test a HIGH propagated value (via a recorded
reply subtree) but a LOW gate value (its child's engine eval), so value and gate
disagree and the anchor alone decides the pick — otherwise the argmax would
choose on value and the gate would never be exercised. Both colours are covered
because the asymmetry has opposite sign for each: with slice_prior > 0.5,
'even' TIGHTENS Black's gate and LOOSENS White's.

Template: _test_stage3_relgate.py.
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
    common = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0,
                  require_eval=True, robustness_floor=0.1, gate_metric="eval",
                  gate_rel_floor=1.0)

    # ── WHITE ────────────────────────────────────────────────────────────────
    # slice_prior = (0.55*5000 + 0.90*1000)/6000 = 0.6083
    #   slice-prior bar = 0.6083 - 0.1 = 0.5083   |   even bar = 0.5 - 0.1 = 0.40
    # b4 reaches a position the engine scores 0.45 — BELOW the slice-prior bar,
    # ABOVE the even bar — while its recorded subtree propagates 0.90, so it wins
    # the argmax whenever the gate lets it through.
    e4c, b4c = board_after("e4"), board_after("b4")
    edges_w = [edge(start, "e4", 0.55, 5000),
               edge(start, "b4", 0.90, 1000),
               edge(b4c, "e5", 0.90, 1000)]        # subtree -> propagates 0.90
    eval_w = {zobrist_int64(e4c): 0.52,            # clears both bars
              zobrist_int64(b4c): 0.45}            # clears 'even' only
    (_v, bm_sp, *_r) = run_backwards_induction(
        edges_w, "white", eval_lookup=eval_w, gate_anchor="slice-prior", **common)
    (_v2, bm_ev, *_r2) = run_backwards_induction(
        edges_w, "white", eval_lookup=eval_w, gate_anchor="even", **common)
    check(bm_sp.get(sh) == "e4",
          f"white slice-prior anchor (bar 0.508) GATES the 0.45 move "
          f"(root={bm_sp.get(sh)!r}, expected 'e4')")
    check(bm_ev.get(sh) == "b4",
          f"white even anchor (bar 0.400) ADMITS it — 'even' LOOSENS White "
          f"(root={bm_ev.get(sh)!r}, expected 'b4')")

    # ── BLACK (sign flips: bar = anchor + floor, lower white score is better) ──
    # slice_prior = 0.55 (the lone start edge)
    #   slice-prior bar = 0.55 + 0.1 = 0.65   |   even bar = 0.5 + 0.1 = 0.60
    # c5 reaches a position the engine scores 0.62 — INSIDE the slice-prior bar,
    # OUTSIDE the even bar — while its subtree propagates 0.10 (great for Black).
    e4n = board_after("e4")
    eh = zobrist_int64(e4n)
    sound_b, tempt_b = board_after("e4", "e5"), board_after("e4", "c5")
    edges_b = [edge(start, "e4", 0.55, 6000),
               edge(e4n, "e5", 0.45, 5000),
               edge(e4n, "c5", 0.10, 1000),
               edge(tempt_b, "d4", 0.10, 1000)]    # subtree -> propagates 0.10
    eval_b = {zobrist_int64(sound_b): 0.48,        # clears both bars
              zobrist_int64(tempt_b): 0.62}        # clears slice-prior only
    (_v3, bm_bsp, *_r3) = run_backwards_induction(
        edges_b, "black", eval_lookup=eval_b, gate_anchor="slice-prior", **common)
    (_v4, bm_bev, *_r4) = run_backwards_induction(
        edges_b, "black", eval_lookup=eval_b, gate_anchor="even", **common)
    check(bm_bsp.get(eh) == "c5",
          f"black slice-prior anchor (bar 0.650) ADMITS the 0.62 move "
          f"(move={bm_bsp.get(eh)!r}, expected 'c5')")
    check(bm_bev.get(eh) == "e5",
          f"black even anchor (bar 0.600) GATES it — 'even' TIGHTENS Black "
          f"(move={bm_bev.get(eh)!r}, expected 'e5')")

    # ── the default must reproduce the legacy anchor exactly ──────────────────
    (_v5, bm_def, *_r5) = run_backwards_induction(
        edges_w, "white", eval_lookup=eval_w, **common)
    check(bm_def.get(sh) == bm_sp.get(sh) == "e4",
          f"default gate_anchor == 'slice-prior' (legacy) "
          f"(default={bm_def.get(sh)!r}, slice-prior={bm_sp.get(sh)!r})")
    (_v6, bm_bdef, *_r6) = run_backwards_induction(
        edges_b, "black", eval_lookup=eval_b, **common)
    check(bm_bdef.get(eh) == bm_bsp.get(eh) == "c5",
          f"...for black too (default={bm_bdef.get(eh)!r}, slice-prior={bm_bsp.get(eh)!r})")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
