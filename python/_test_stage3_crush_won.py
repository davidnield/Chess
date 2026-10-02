"""Synthetic test for --crush-won-cp (winning positions are crush-ABSORBING).

The winpos histogram records the FIRST WINNING POSITION after each edge, not the
first crossing into one. A game already winning at a node that stays winning
therefore credits every move out of that node at bucket 1 -- a STATE, measured at
81.7% of games through edges from >= +300 positions on the shipped t300 histogram.
The recursion LineCrush = imm + (1-imm)*gamma*crush_pot(child) then re-counts those
games up the tree. With the flag, a position winning for a side earns that side
no crush on any move out of it and passes none through it.

crush_prior 0 + baseline "zero" makes crush() the identity sum/n and gamma 1.0
makes gamma_hop 1.0, so every number below is hand-computable. Fixture B needs
slack in (1-imm) to show the re-count, so it uses crush_prior 1000 (1000 games ->
crush 0.5).

  A. ABSORBING, not merely masked. Y (+500) is winning; 'e4' keeps the win
     (Z1 +450, value 0.70), 'd4' dips under the threshold (Z2 +290, value 0.69)
     and the reply below Z2 re-crosses (crush 1.0). Zeroing only imm/dfull
     would give d4 LineCrush = crush_pot(Z2) = 1.0 and pick it
     (0.69 + 0.1 > 0.70) -- a bonus for giving the win back. Absorbing leaves
     both at 0, so value decides: e4.
  B. The crossing keeps its credit, the state behind it goes. S -e4-> X -e5-> G,
     G winning. Off: crush_pot(G) = 0.5 (state), X = 0.5 + 0.5*0.5 = 0.75,
     S = 0.75. On: G = 0, X = 0.5 + 0.5*0 = 0.5 (the crossing alone), S = 0.5.
  C. Boundary and coverage: +300 is winning (>=), +299 is not, and a position
     with no eval is left unmasked.
  D. Black: the mask reads eval <= -300 and black_* columns; a node winning for
     WHITE does not absorb Black's crush.
  E. Counter-crush (--crush-penalty): a node winning for the OPPONENT absorbs the
     opponent's crush, flipping a pick the state credit had been deciding.
  F. No-op: default == 0; no evals, or crush weight and penalty 0, change nothing.
  G. CLI guards: a threshold that disagrees with the histogram's _t<cp> suffix,
     or no --eval-db, fails loudly before any work.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import chess
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import (cp_to_expected_score as ES,
                                        run_backwards_induction, zobrist_int64)

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def close(a, b, tol: float = 1e-12) -> bool:
    return a is not None and b is not None and abs(a - b) <= tol


def board_after(*sans: str) -> chess.Board:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def H(*sans: str) -> int:
    return zobrist_int64(board_after(*sans))


def edge(parent: chess.Board, san: str, score: float, total: int = 1000, *,
         w_imm: float = 0.0, w_dfull: float = 0.0,
         b_imm: float = 0.0, b_dfull: float = 0.0, cg: int = 1000) -> dict:
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": 0,
            "white_imm_sum": w_imm, "white_dfull_sum": w_dfull,
            "black_imm_sum": b_imm, "black_dfull_sum": b_dfull,
            "crush_games": cg}


COMMON = dict(prior_strength=0.0, min_move_games=0, eval_weight=0.0,
              robustness_floor=1.0, gate_rel_floor=1.0,
              crush_mode="relative-propagated", crush_weight=0.1,
              crush_prior=0.0, crush_baseline="zero", crush_gamma=1.0)


def run(edges, persp="white", **kw):
    args = {**COMMON, **kw}
    out = run_backwards_induction(edges, persp, **args)
    return out[0], out[1], out[6]          # values, best_moves, crush_pot


def same(a, b) -> bool:
    """Every returned view identical (NaN-aware) -- the no-op contract."""
    for x, y in zip(a, b):
        if hasattr(x, "arr"):
            if not np.array_equal(x.arr, y.arr, equal_nan=x.arr.dtype.kind == "f"):
                return False
        elif hasattr(x, "lst"):
            if x.lst != y.lst:
                return False
        elif x != y:
            return False
    return True


def fixture_a():
    y = board_after()
    z1, z2 = board_after("e4"), board_after("d4")
    edges = [edge(y, "e4", 0.70, w_imm=1000.0, w_dfull=1000.0),   # keep the win
             edge(y, "d4", 0.69),                                  # dip under it
             edge(z1, "e5", 0.70, w_imm=1000.0, w_dfull=1000.0),  # state credit
             edge(z2, "d5", 0.69, w_imm=1000.0, w_dfull=1000.0)]  # re-crossing
    evals = {H(): ES(500), H("e4"): ES(450), H("d4"): ES(290)}
    return edges, evals


def fixture_b(g_cp: int | None, persp: str = "white"):
    """S -e4-> X -e5-> G -Nf3-> leaf. The crossing is the e5 edge into G."""
    s, x, g = board_after(), board_after("e4"), board_after("e4", "e5")
    col = "w" if persp == "white" else "b"
    kw = lambda v: {f"{col}_imm": v, f"{col}_dfull": v}   # noqa: E731
    edges = [edge(s, "e4", 0.55),
             edge(x, "e5", 0.60, **kw(1000.0)),
             edge(g, "Nf3", 0.60, **kw(1000.0))]
    evals = {H(): ES(20), H("e4"): ES(30)}
    if g_cp is not None:
        evals[H("e4", "e5")] = ES(g_cp)
    return edges, evals


def test_absorbing() -> None:
    print("\nA. winning positions are ABSORBING, not merely masked")
    edges, evals = fixture_a()
    y, z1, z2 = H(), H("e4"), H("d4")
    _v, bm_off, cp_off = run(edges, eval_lookup=evals)
    check(close(cp_off[y], 1.0), "off: the state credit reaches Y (crush_pot 1.0)")
    _v, bm, cp = run(edges, eval_lookup=evals, crush_won_cp=300)
    check(close(cp[z1], 0.0), "on: Z1 (+450, winning) has crush_pot 0")
    check(close(cp[z2], 1.0), "on: Z2 (+290) keeps the re-crossing below it (1.0)")
    check(close(cp[y], 0.0), "on: Y (+500) passes none of it through (crush_pot 0)")
    check(bm[y] == "e4", f"on: value decides at the won node -> keep the win "
          f"(picked {bm[y]!r}; an imm/dfull-only mask would pick 'd4')")


def test_crossing_keeps_credit() -> None:
    print("\nB. the crossing keeps its credit; the state behind it is gone")
    s, x, g = H(), H("e4"), H("e4", "e5")
    edges, evals = fixture_b(500)
    _v, _b, off = run(edges, eval_lookup=evals, crush_prior=1000.0)
    check(close(off[g], 0.5) and close(off[x], 0.75) and close(off[s], 0.75),
          f"off: G 0.5 / X 0.75 / S 0.75 "
          f"(got {off[g]:.4f} / {off[x]:.4f} / {off[s]:.4f})")
    _v, _b, on = run(edges, eval_lookup=evals, crush_prior=1000.0, crush_won_cp=300)
    check(close(on[g], 0.0) and close(on[x], 0.5) and close(on[s], 0.5),
          f"on: G 0 / X 0.5 (the crossing alone) / S 0.5 "
          f"(got {on[g]:.4f} / {on[x]:.4f} / {on[s]:.4f})")


def test_boundary_and_coverage() -> None:
    print("\nC. threshold boundary and eval coverage")
    x = H("e4")
    for g_cp, want, why in ((300, 0.5, "+300 is winning (>=): absorbed"),
                            (299, 0.75, "+299 is not winning: unmasked"),
                            (None, 0.75, "no eval for G: unmasked")):
        edges, evals = fixture_b(g_cp)
        _v, _b, cp = run(edges, eval_lookup=evals, crush_prior=1000.0,
                         crush_won_cp=300)
        check(close(cp[x], want), f"{why} (X crush_pot {cp[x]:.4f}, want {want})")


def test_black() -> None:
    print("\nD. black perspective reads eval <= -threshold and black_* columns")
    x = H("e4")
    edges, evals = fixture_b(-500, persp="black")
    _v, _b, off = run(edges, "black", eval_lookup=evals, crush_prior=1000.0)
    _v, _b, on = run(edges, "black", eval_lookup=evals, crush_prior=1000.0,
                     crush_won_cp=300)
    check(close(off[x], 0.75) and close(on[x], 0.5),
          f"G at -500 absorbs Black's crush (X {off[x]:.4f} -> {on[x]:.4f})")
    edges, evals = fixture_b(500, persp="black")
    _v, _b, on_w = run(edges, "black", eval_lookup=evals, crush_prior=1000.0,
                       crush_won_cp=300)
    check(close(on_w[x], 0.75),
          f"G at +500 (winning for WHITE) leaves Black's crush alone ({on_w[x]:.4f})")


def test_counter_crush() -> None:
    print("\nE. counter-crush: a node winning for the OPPONENT absorbs theirs")
    s = board_after()
    x, x2 = board_after("e4"), board_after("d4")
    g, g2 = board_after("e4", "e5"), board_after("d4", "d5")
    edges = [edge(s, "e4", 0.60), edge(s, "d4", 0.48),
             edge(x, "e5", 0.60, b_imm=1000.0, b_dfull=1000.0),   # their crossing
             edge(g, "Nf3", 0.60, b_imm=1000.0, b_dfull=1000.0),  # their state
             edge(x2, "d5", 0.48), edge(g2, "Nf3", 0.48)]
    evals = {H(): ES(20), H("e4"): ES(30), H("d4"): ES(20),
             H("e4", "e5"): ES(-500), H("d4", "d5"): ES(20)}
    kw = dict(eval_lookup=evals, crush_prior=1000.0, crush_penalty=0.2)
    # off: e4 pays 0.2 * (0.5 + 0.5*0.5) = 0.15 -> 0.45 < 0.48 -> d4
    # on : G is Black's win, absorbing  -> 0.2 * 0.5 = 0.10 -> 0.50 > 0.48 -> e4
    _v, bm_off, _c = run(edges, **kw)
    _v, bm_on, _c = run(edges, crush_won_cp=300, **kw)
    check(bm_off[H()] == "d4" and bm_on[H()] == "e4",
          f"the opponent's state credit stops deciding the root "
          f"({bm_off[H()]!r} -> {bm_on[H()]!r}, want 'd4' -> 'e4')")


def test_noop() -> None:
    print("\nF. no-op contract")
    for name, (edges, evals) in (("A", fixture_a()), ("B", fixture_b(500))):
        base = run_backwards_induction(edges, "white", eval_lookup=evals,
                                       **{**COMMON, "crush_prior": 1000.0})
        zero = run_backwards_induction(edges, "white", eval_lookup=evals,
                                       crush_won_cp=0, **{**COMMON, "crush_prior": 1000.0})
        check(same(base, zero), f"fixture {name}: default == crush_won_cp 0, bit for bit")
        no_ev = run_backwards_induction(edges, "white", **{**COMMON, "crush_prior": 1000.0})
        no_ev_on = run_backwards_induction(edges, "white", crush_won_cp=300,
                                           **{**COMMON, "crush_prior": 1000.0})
        check(same(no_ev, no_ev_on), f"fixture {name}: no evals -> the flag is inert")
        plain = {**COMMON, "crush_weight": 0.0}
        a = run_backwards_induction(edges, "white", eval_lookup=evals, **plain)
        b = run_backwards_induction(edges, "white", eval_lookup=evals,
                                    crush_won_cp=300, **plain)
        check(same(a, b), f"fixture {name}: crush weight and penalty 0 -> inert")


def test_cli_guards() -> None:
    print("\nG. CLI guards fail before any work")
    stage3 = str(Path(__file__).parent / "stage3_backwards_induction.py")
    r = subprocess.run([sys.executable, stage3, "--crush-won-cp", "200",
                        "--eval-db", "unused.parquet",
                        "--crush-db", "crush_hist_relwin_x_t300.parquet"],
                       capture_output=True, text=True)
    check(r.returncode != 0 and "disagrees" in r.stderr,
          "threshold disagreeing with the histogram's _t300 suffix is refused")
    r = subprocess.run([sys.executable, stage3, "--crush-won-cp", "300"],
                       capture_output=True, text=True)
    check(r.returncode != 0 and "needs --eval-db" in r.stderr,
          "--crush-won-cp without --eval-db is refused")


def main() -> None:
    test_absorbing()
    test_crossing_keeps_credit()
    test_boundary_and_coverage()
    test_black()
    test_counter_crush()
    test_noop()
    test_cli_guards()
    bad = [label for ok, label in _checks if not ok]
    print(f"\n{len(_checks) - len(bad)}/{len(_checks)} checks passed")
    if bad:
        print("FAILED: " + "; ".join(bad))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
