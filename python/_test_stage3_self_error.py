"""Synthetic test for --self-error-weight (the symmetric counterpart to --error-weight).

--error-weight rewards lines where the OPPONENT is likely to err. Nothing
penalised lines where WE are, so a +2.0 position needing six only-moves outranked
a +0.8 one that plays itself.

Two structural facts this pins down:

1. The term must PROPAGATE. Our expected error at a node is a property of the
   POSITION, identical for every candidate there, so it cannot influence that
   node's own argmax — what discriminates is the error cost the move leads INTO.
   Hence self_err_pot, read at mv["child"] exactly like memo_pot.

2. It must stay OUT of the SCC convergence gate. Like memo_pot it accumulates
   ADDITIVELY along the chosen chain, so a best-move chain that stays inside a
   cycle grows it without bound; gating on it would report false non-convergence
   forever. The last check runs the real cyclic fixture with the term ON and
   asserts the fixpoint still converges — it fails if anyone adds self_err_pot
   to gate_dicts.

Fixture (White, 3 ply). Both branches reach a position where WE move again:
    e4 -> ... -> our replies score 0.60 / 0.10   (wide spread: easy to go wrong)
    d4 -> ... -> our replies score 0.55 / 0.55   (no spread: plays itself)
e4 is worth 0.05 more, so the penalty must exceed that to flip the pick.
"""
from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import chess

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import run_backwards_induction, zobrist_int64
from _test_stage3_cycles import cyclic_fixture

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


# error_prior 0 makes opponent_error's Bayesian shrink the identity, so the
# 0.25 self-error in the docstring is exact hand arithmetic.
COMMON = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0,
              error_prior=0.0, robustness_floor=1.0, gate_rel_floor=1.0)


def main() -> None:
    start = board_after()
    sh = zobrist_int64(start)
    e4c, d4c = board_after("e4"), board_after("d4")
    e4g, d4g = board_after("e4", "e5"), board_after("d4", "d5")

    edges = [
        edge(start, "e4", 0.60, 5000),
        edge(start, "d4", 0.55, 5000),
        edge(e4c, "e5", 0.60, 5000),        # opponent's forced reply
        edge(d4c, "d5", 0.55, 5000),
        # OUR next decision. The spread between these is the self-error.
        edge(e4g, "Nf3", 0.60, 1000),
        edge(e4g, "Qh5", 0.10, 1000),       # wide spread -> self-error 0.25
        edge(d4g, "Nf3", 0.55, 1000),
        edge(d4g, "c4", 0.55, 1000),        # no spread  -> self-error 0.0
    ]
    ev = {zobrist_int64(board_after("e4", "e5", "Nf3")): 0.60,
          zobrist_int64(board_after("e4", "e5", "Qh5")): 0.10,
          zobrist_int64(board_after("d4", "d5", "Nf3")): 0.55,
          zobrist_int64(board_after("d4", "d5", "c4")): 0.55}

    (_v, bm_off, *_r) = run_backwards_induction(
        edges, "white", eval_lookup=ev, self_error_weight=0.0, **COMMON)
    check(bm_off.get(sh) == "e4",
          f"self_error_weight 0 keeps the legacy pick (root={bm_off.get(sh)!r}, expected 'e4')")

    (_v2, bm_def, *_r2) = run_backwards_induction(edges, "white", eval_lookup=ev, **COMMON)
    check(bm_def.get(sh) == "e4",
          f"default self_error_weight is 0 (root={bm_def.get(sh)!r}, expected 'e4')")

    (_v3, bm_on, *_r3) = run_backwards_induction(
        edges, "white", eval_lookup=ev, self_error_weight=0.4, **COMMON)
    check(bm_on.get(sh) == "d4",
          f"weight 0.4 (0.4*0.25 = 0.10 > 0.05 value gap) prefers the line that "
          f"plays itself (root={bm_on.get(sh)!r}, expected 'd4')")

    (_v4, bm_small, *_r4) = run_backwards_induction(
        edges, "white", eval_lookup=ev, self_error_weight=0.1, **COMMON)
    check(bm_small.get(sh) == "e4",
          f"weight 0.1 (0.025 < 0.05 gap) does not flip — a trade-off, not a veto "
          f"(root={bm_small.get(sh)!r}, expected 'e4')")

    # The deeper node's own pick is by value: its candidates are leaves, so the
    # propagated term is 0 for both and cannot bias it.
    check(bm_on.get(zobrist_int64(e4g)) == "Nf3",
          f"our deep node still picks on value (move={bm_on.get(zobrist_int64(e4g))!r}, "
          f"expected 'Nf3')")

    # ── negative control: with no depth there is nothing to propagate ──────────
    # Root candidates lead straight to leaves, so self_err_pot.get(child) is 0 for
    # every candidate and no weight can flip the pick. Proves the flip above came
    # from propagation rather than from a local term leaking into the argmax.
    flat = [edge(start, "e4", 0.60, 5000), edge(start, "d4", 0.55, 5000)]
    flat_ev = {zobrist_int64(e4c): 0.60, zobrist_int64(d4c): 0.55}
    (_v5, bm_flat, *_r5) = run_backwards_induction(
        flat, "white", eval_lookup=flat_ev, self_error_weight=5.0, **COMMON)
    check(bm_flat.get(sh) == "e4",
          f"one-ply fixture: even weight 5.0 cannot flip (root={bm_flat.get(sh)!r}, "
          f"expected 'e4') — the local term is constant across candidates")

    # ── the SCC guard ─────────────────────────────────────────────────────────
    # Behavioural half: the real 4-node cycle from _test_stage3_cycles, term ON.
    cyc_edges, ch = cyclic_fixture()
    cyc_ev = {ch["start"]: 0.55, ch["p1"]: 0.52, ch["p2"]: 0.52, ch["p3"]: 0.50}
    buf = io.StringIO()
    with redirect_stdout(buf):
        (cv, _cbm, *_cr) = run_backwards_induction(
            cyc_edges, "white", eval_lookup=cyc_ev, self_error_weight=0.4,
            prior_strength=0.0, min_move_games=0, error_prior=0.0)
    log = buf.getvalue()
    check("0 non-converged SCC(s)" in log,
          f"cyclic fixture converges with self-error ON "
          f"({[l.strip() for l in log.splitlines() if 'fixpoint' in l]})")
    check(len(cv) == 4 and all(v == v and abs(v) < 10 for v in cv.values()),
          f"...and all {len(cv)} cycle positions are still finite")

    # Structural half — deliberately white-box, because the behavioural test above
    # CANNOT catch a regression here. Making self_err_pot actually diverge needs a
    # best-move chain that stays inside the cycle with probability 1, and the
    # penalty is self-limiting: accumulating it is exactly what steers selection
    # OUT of the cycle. (In the fixture above the chain exits at the root, so
    # self_err_pot converges and would look fine even if it were gated.) The
    # invariant is therefore asserted against the source: memo_pot and
    # self_err_pot accumulate additively along the chosen chain and must both stay
    # out of gate_dicts; crush_pot / crush_pot_opp are bounded and multiplicative
    # and must both be in it.
    src = (Path(__file__).parent / "stage3_backwards_induction.py").read_text(encoding="utf-8")
    raw = src.split("gate_dicts = [", 1)[1].split("gate_dicts, gate_names", 1)[0]
    # Strip comments — the block explains WHY each term is in or out, so naming a
    # term in prose there must not read as including it.
    gate_block = "\n".join(ln.split("#", 1)[0] for ln in raw.splitlines())
    for name, want_in in (("self_err_pot", False), ("memo_pot", False),
                          ("crush_pot_opp", True), ("crush_pot", True)):
        present = name in gate_block
        check(present == want_in,
              f"{name} {'IS' if want_in else 'is NOT'} in the SCC convergence gate "
              f"(found={present})")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
