"""Synthetic test for --crush-penalty (COUNTER-CRUSH).

The crush bonus has always been one-sided: it counts how often WE reach a winning
position early and ignores how often the OPPONENT does, so a line that wins fast
30% and loses fast 30% scored the same bonus as one that wins fast 30% and draws
70%. The opposite-colour histogram columns were computed, joined and carried in
the CSR arrays the whole time — nothing read them.

Fixture (White). Two root moves whose OUR-crush is identical (all white_* sums
zero) and whose IMMEDIATE opponent-crush is also identical (black_* sums zero on
both root edges). They differ ONLY one ply deeper, in the opponent-crush of the
reply subtree:

    e4 -> value 0.60, opponent crushes us at rate 0.5 in the subtree
    d4 -> value 0.55, opponent crushes us at rate 0.0 in the subtree

So the penalty can only bite by PROPAGATING crush_pot_opp up from the grandchild;
a local-only term would compute 0 for both and change nothing. The last check
proves exactly that by flattening the subtrees and showing the flip disappears.
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


def edge(parent: chess.Board, san: str, score: float, total: int,
         opp_dfull: float = 0.0, cg: int = 1000) -> dict:
    """One edge. `opp_dfull` is the opponent's discounted early-win sum through it
    (black_dfull_sum for a White-perspective run); every OUR-side crush column is
    held at zero so --crush-weight cannot differentiate the candidates."""
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": 0,
            "white_imm_sum": 0.0, "white_dfull_sum": 0.0,
            "black_imm_sum": 0.0, "black_dfull_sum": opp_dfull,
            "crush_games": cg}


# crush_prior 0 + baseline "zero" makes crush() the identity on sum/n, and
# gamma 1.0 makes gamma_hop 1.0 — so the arithmetic in the docstring is exact.
COMMON = dict(prior_strength=0.0, min_move_games=0, eval_weight=0.0,
              robustness_floor=1.0, gate_rel_floor=1.0,
              crush_mode="relative-propagated", crush_weight=0.1,
              crush_prior=0.0, crush_baseline="zero", crush_gamma=1.0)


def main() -> None:
    start = board_after()
    sh = zobrist_int64(start)
    e4c, d4c = board_after("e4"), board_after("d4")

    # Root edges: identical crush columns (all zero) — only the subtrees differ.
    deep = [
        edge(start, "e4", 0.60, 5000),
        edge(start, "d4", 0.55, 5000),
        # Opponent's replies. These carry the opponent-crush that must propagate up.
        edge(e4c, "e5", 0.60, 5000, opp_dfull=500.0),   # -> o_dfull 0.5
        edge(d4c, "d5", 0.55, 5000, opp_dfull=0.0),     # -> o_dfull 0.0
    ]

    (_v, bm_off, *_r) = run_backwards_induction(deep, "white", crush_penalty=0.0, **COMMON)
    check(bm_off.get(sh) == "e4",
          f"crush_penalty 0 keeps the legacy pick — higher value wins "
          f"(root={bm_off.get(sh)!r}, expected 'e4')")

    (_v2, bm_on, *_r2) = run_backwards_induction(deep, "white", crush_penalty=0.2, **COMMON)
    check(bm_on.get(sh) == "d4",
          f"crush_penalty 0.2 flips to the line the opponent does NOT crush "
          f"(0.05 value edge < 0.2*0.5 penalty) (root={bm_on.get(sh)!r}, expected 'd4')")

    # Default must be the legacy behaviour.
    (_v3, bm_def, *_r3) = run_backwards_induction(deep, "white", **COMMON)
    check(bm_def.get(sh) == "e4",
          f"default crush_penalty is 0 (root={bm_def.get(sh)!r}, expected 'e4')")

    # A penalty too small to cover the value gap must NOT flip it — the term is a
    # weighted trade-off, not a veto.
    (_v4, bm_small, *_r4) = run_backwards_induction(deep, "white", crush_penalty=0.05, **COMMON)
    check(bm_small.get(sh) == "e4",
          f"crush_penalty 0.05 (0.025 < 0.05 value gap) does not flip "
          f"(root={bm_small.get(sh)!r}, expected 'e4')")

    # ── the flip must come from PROPAGATION, not the immediate edge ────────────
    # Same root edges, subtrees removed: both children are now leaves, so
    # opp_line_crush falls back to each ROOT edge's own black_dfull_sum — zero for
    # both. If the penalty still flipped the pick, the term would be reading
    # something local and the two-ply structure above would prove nothing.
    flat = [edge(start, "e4", 0.60, 5000), edge(start, "d4", 0.55, 5000)]
    (_v5, bm_flat, *_r5) = run_backwards_induction(flat, "white", crush_penalty=0.2, **COMMON)
    check(bm_flat.get(sh) == "e4",
          f"with the subtrees removed the penalty has nothing to propagate and "
          f"does NOT flip (root={bm_flat.get(sh)!r}, expected 'e4')")

    # ── black perspective: the colour roles swap ──────────────────────────────
    # For a Black run the opponent is White, so the penalty must read white_*.
    # Mirror the fixture: Black prefers the LOWER white score.
    e4n = board_after("e4")
    eh = zobrist_int64(e4n)
    b_e5, b_c5 = board_after("e4", "e5"), board_after("e4", "c5")

    def bedge(parent, san, score, total, opp_dfull=0.0, cg=1000):
        child = parent.copy()
        child.push_san(san)
        return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
                "move_san": san, "parent_epd": parent.epd(),
                "white_score_avg": score, "total": total, "draws": 0,
                "black_imm_sum": 0.0, "black_dfull_sum": 0.0,
                "white_imm_sum": 0.0, "white_dfull_sum": opp_dfull,  # opponent = White
                "crush_games": cg}

    deep_b = [
        bedge(start, "e4", 0.50, 5000),
        bedge(e4n, "e5", 0.40, 5000),                    # better for Black (0.40)
        bedge(e4n, "c5", 0.45, 5000),
        bedge(b_e5, "Nf3", 0.40, 5000, opp_dfull=500.0),  # ...but White crushes here
        bedge(b_c5, "Nf3", 0.45, 5000, opp_dfull=0.0),
    ]
    (_v6, bm_boff, *_r6) = run_backwards_induction(deep_b, "black", crush_penalty=0.0, **COMMON)
    (_v7, bm_bon, *_r7) = run_backwards_induction(deep_b, "black", crush_penalty=0.2, **COMMON)
    check(bm_boff.get(eh) == "e5",
          f"black, penalty 0: lower white score wins (move={bm_boff.get(eh)!r}, expected 'e5')")
    check(bm_bon.get(eh) == "c5",
          f"black, penalty 0.2: reads white_* as the opponent's crush and flips "
          f"(move={bm_bon.get(eh)!r}, expected 'c5')")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
