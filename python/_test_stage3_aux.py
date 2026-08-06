"""Stage 3 consumes the aux sidecar, and by exactly the right amount.

The defect: an opponent node's value is a mean over its OUTGOING edges, so games
that ended there, replies below the pool floor, and games cut off by the ply cap
contribute nothing. The first is directional — measured, the side to move scores
0.0953 at a terminal node, so excluding them deletes precisely the opponent's
collapses.

Every expectation is computed by hand here rather than snapshotted, so a change
in the blend fails loudly instead of quietly updating a baseline.

  A  --aux-stats omitted is an EXACT no-op
  B  terminations enter at their empirical score, by mass-weighted arithmetic
  C  time forfeits are separable: --no-aux-term-flags drops exactly their mass
  D  the other-moves bucket blends empirical with its eval via effective_eval_weight
  E  a NULL bucket eval falls back to empirical and is NOT read as 0.0
  F  horizon is empirical by default; --aux-horizon eval takes the node's eval
  G  aux mass can FLIP the chosen move — the point of the exercise
  H  OUR node is untouched (its value is prescriptive, not an empirical mean)

Run: .venv/Scripts/python.exe python/_test_stage3_aux.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from stage3_backwards_induction import (effective_eval_weight,
                                        run_backwards_induction, zobrist_int64)

TOL = 1e-9
_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def board_after(*sans: str) -> chess.Board:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def edge(parent: chess.Board, san: str, score: float, total: int, draws: int = 0) -> dict:
    child = parent.copy()
    child.push_san(san)
    return {"parent_hash": zobrist_int64(parent), "child_hash": zobrist_int64(child),
            "move_san": san, "parent_epd": parent.epd(),
            "white_score_avg": score, "total": total, "draws": draws}


GROUPS = ("term_normal", "term_flag", "term_other", "horizon")
AUX_SCHEMA = {
    "position_hash": pl.Int64,
    **{f"{g}_{c}": pl.Int64 for g in GROUPS
       for c in ("total", "white_wins", "draws", "black_wins")},
    "other_total": pl.Int64, "other_white_wins": pl.Int64,
    "other_draws": pl.Int64, "other_black_wins": pl.Int64,
    "other_edges": pl.Int32, "other_eval_mean": pl.Float64,
    "other_eval_min": pl.Float64, "other_eval_max": pl.Float64,
    "other_eval_cov": pl.Float64,
}


def aux_row(position_hash: int, **kw) -> dict:
    r = {"position_hash": position_hash}
    for g in GROUPS:
        for c in ("total", "white_wins", "draws", "black_wins"):
            r[f"{g}_{c}"] = 0
    r.update({"other_total": 0, "other_white_wins": 0, "other_draws": 0,
              "other_black_wins": 0, "other_edges": 0, "other_eval_mean": None,
              "other_eval_min": None, "other_eval_max": None, "other_eval_cov": 0.0})
    r.update(kw)
    return r


COMMON = dict(prior_strength=0.0, min_move_games=0, robustness_floor=1.0,
              gate_rel_floor=1.0)


def go(edges, aux=None, colour="white", **kw):
    adf = pl.DataFrame(aux, schema=AUX_SCHEMA) if aux else None
    v, bm, *_ = run_backwards_induction(edges, colour, aux=adf, **COMMON, **kw)
    return v, bm


def main() -> None:
    print("=" * 70)
    print("STAGE 3 — AUX SIDECAR")
    print("=" * 70)

    start = board_after()
    sh = zobrist_int64(start)
    e4n, d4n = board_after("e4"), board_after("d4")
    opp = zobrist_int64(e4n)

    edges = [
        edge(start, "e4", 0.55, 1000),
        edge(start, "d4", 0.50, 1000),
        edge(e4n, "e5", 0.60, 400),
        edge(e4n, "c5", 0.40, 600),
        edge(d4n, "d5", 0.50, 1000),
    ]
    base = (0.60 * 400 + 0.40 * 600) / 1000

    v0, _ = go(edges)
    check(abs(v0[opp] - base) < TOL,
          f"A: no aux -> plain outgoing mean ({v0[opp]:.9f} == {base:.9f})")

    # B — 200 terminations, White scoring 180/200 (Black to move, Black collapsed)
    aux_b = [aux_row(opp, term_normal_total=200, term_normal_white_wins=180,
                     term_normal_black_wins=20)]
    want_b = (0.60 * 400 + 0.40 * 600 + 180.0) / 1200
    vb, _ = go(edges, aux_b)
    check(abs(vb[opp] - want_b) < TOL,
          f"B: terminations enter at their empirical score "
          f"({vb[opp]:.9f} vs {want_b:.9f}; was {base:.6f})")
    check(vb[opp] > v0[opp],
          "B: they RAISE the node — the collapses that were being deleted")

    # C — flags separable
    aux_c = [aux_row(opp, term_normal_total=200, term_normal_white_wins=180,
                     term_normal_black_wins=20,
                     term_flag_total=100, term_flag_white_wins=90,
                     term_flag_black_wins=10)]
    want_in = (0.60 * 400 + 0.40 * 600 + 180.0 + 90.0) / 1300
    want_out = (0.60 * 400 + 0.40 * 600 + 180.0) / 1200
    vi, _ = go(edges, aux_c, aux_term_flags=True)
    vo, _ = go(edges, aux_c, aux_term_flags=False)
    check(abs(vi[opp] - want_in) < TOL and abs(vo[opp] - want_out) < TOL,
          f"C: --no-aux-term-flags drops exactly the forfeit mass "
          f"({vi[opp]:.9f} -> {vo[opp]:.9f})")

    # D — bucket blend. eval_lookup stays EMPTY so leaf blending cannot fire and
    #     the only eval effect in play is the bucket's own.
    EW = 0.5
    aux_d = [aux_row(opp, other_total=500, other_white_wins=250,
                     other_black_wins=250, other_edges=9, other_eval_mean=0.30,
                     other_eval_cov=1.0)]
    w_o = effective_eval_weight(EW, 0.1, 0.0, 500)
    v_o = (1 - w_o) * 0.5 + w_o * 0.30
    want_d = (0.60 * 400 + 0.40 * 600 + v_o * 500) / 1500
    vd, _ = go(edges, aux_d, eval_weight=EW, eval_weight_min=0.1, eval_weight_k=0.0)
    check(abs(vd[opp] - want_d) < TOL,
          f"D: bucket = (1-w)*emp + w*eval at w={w_o} "
          f"({vd[opp]:.9f} vs {want_d:.9f})")

    # D2 — the eval's trust must rest on the COVERED mass, not the whole bucket.
    #      Identical bucket, identical eval, only coverage differs: at k>0 the
    #      half-covered bucket must trust the engine MORE (less evidence behind
    #      the empirical side of the blend), which passing other_total would miss.
    K = 400.0
    aux_d2 = [aux_row(opp, other_total=500, other_white_wins=250,
                      other_black_wins=250, other_edges=9, other_eval_mean=0.30,
                      other_eval_cov=0.5)]
    w_half = effective_eval_weight(EW, 0.1, K, 500 * 0.5)
    v_half = (1 - w_half) * 0.5 + w_half * 0.30
    want_d2 = (0.60 * 400 + 0.40 * 600 + v_half * 500) / 1500
    vd2, _ = go(edges, aux_d2, eval_weight=EW, eval_weight_min=0.1, eval_weight_k=K)
    check(abs(vd2[opp] - want_d2) < TOL,
          f"D2: blend weight uses covered mass ({vd2[opp]:.9f} vs {want_d2:.9f})")
    w_full = effective_eval_weight(EW, 0.1, K, 500.0)
    check(w_half > w_full,
          f"D2: and half coverage trusts the engine more ({w_half:.4f} > {w_full:.4f})")

    # E — NULL bucket eval must not read as 0.0
    aux_e = [aux_row(opp, other_total=500, other_white_wins=250,
                     other_black_wins=250, other_edges=9, other_eval_mean=None)]
    want_e = (0.60 * 400 + 0.40 * 600 + 0.5 * 500) / 1500
    ve, _ = go(edges, aux_e, eval_weight=EW)
    check(abs(ve[opp] - want_e) < TOL,
          f"E: NULL bucket eval -> empirical, not 0.0 "
          f"({ve[opp]:.9f} vs {want_e:.9f})")

    # F — horizon
    aux_f = [aux_row(opp, horizon_total=300, horizon_white_wins=150,
                     horizon_draws=100, horizon_black_wins=50)]
    want_f = (0.60 * 400 + 0.40 * 600 + (150 + 50.0)) / 1300
    vf, _ = go(edges, aux_f)
    check(abs(vf[opp] - want_f) < TOL,
          f"F: horizon uses its observed results by default "
          f"({vf[opp]:.9f} vs {want_f:.9f})")
    ev = {opp: 0.90}
    want_fe = (0.60 * 400 + 0.40 * 600 + 0.90 * 300) / 1300
    vfe, _ = go(edges, aux_f, aux_horizon="eval", eval_lookup=ev)
    check(abs(vfe[opp] - want_fe) < TOL,
          f"F: --aux-horizon eval substitutes the node's eval "
          f"({vfe[opp]:.9f} vs {want_fe:.9f})")

    # G — aux flips the root choice
    edges_g = [
        edge(start, "e4", 0.55, 1000), edge(start, "d4", 0.50, 1000),
        edge(e4n, "e5", 0.520, 1000),
        edge(d4n, "d5", 0.515, 1000),
    ]
    opp_d4 = zobrist_int64(d4n)
    _, bg0 = go(edges_g)
    aux_g = [aux_row(opp_d4, term_normal_total=1000, term_normal_white_wins=1000)]
    _, bg1 = go(edges_g, aux_g)
    check(bg0.get(sh) == "e4" and bg1.get(sh) == "d4",
          f"G: aux mass flips the chosen move ({bg0.get(sh)!r} -> {bg1.get(sh)!r})")

    # H — at OUR node, games where WE lost are book deviations and must not enter.
    vh0, _ = go(edges)
    aux_h = [aux_row(sh, term_normal_total=5000, term_normal_black_wins=5000)]
    vh1, _ = go(edges, aux_h)
    check(abs(vh0[sh] - vh1[sh]) < TOL,
          f"H: OUR losses at our node are excluded (book deviation) "
          f"({vh0[sh]:.9f} == {vh1[sh]:.9f})")

    # I — but the opponent resigning BEFORE we move is a real collapse and must.
    #     `1.e4 e5 {Black resigns}` ends at a White-to-move node; that mass used to
    #     be discarded entirely. cont = 2,000 games leave the root by e4/d4.
    aux_i = [aux_row(sh, term_normal_total=500, term_normal_white_wins=500)]
    vi2, _ = go(edges, aux_i)
    want_i = (vh0[sh] * 2000 + 1.0 * 500) / 2500
    check(abs(vi2[sh] - want_i) < TOL,
          f"I: opponent resigning on OUR turn IS counted, at score 1.0 "
          f"({vi2[sh]:.9f} vs {want_i:.9f}, was {vh0[sh]:.9f})")

    # J — draws at our node stay out (usually a draw we agreed = deviation, and
    #     the data cannot separate that from stalemate or repetition).
    aux_j = [aux_row(sh, term_normal_total=500, term_normal_draws=500)]
    vj, _ = go(edges, aux_j)
    check(abs(vj[sh] - vh0[sh]) < TOL,
          f"J: draws at our node are excluded ({vj[sh]:.9f} == {vh0[sh]:.9f})")

    # K — it cannot change the pick AT this node (identical for every candidate),
    #     only the node's value, and hence the parent's choice.
    _, bk0 = go(edges)
    _, bk1 = go(edges, aux_i)
    check(bk0.get(sh) == bk1.get(sh),
          f"K: our-node collapse mass does not reorder candidates AT that node "
          f"({bk0.get(sh)!r} == {bk1.get(sh)!r})")

    # L/M — BLACK perspective. Values stay in white-score units, so every sign
    #       flips: our node is Black to move, "they cracked" is black_wins, and a
    #       collapse pulls the value DOWN toward 0. Nothing above exercises this,
    #       and a colour error here would silently invert the correction for half
    #       the repertoire while leaving every white-side test green.
    b_edges = [
        edge(start, "e4", 0.55, 1000),
        edge(e4n, "e5", 0.50, 400),
        edge(e4n, "c5", 0.50, 600),
    ]
    # opp is White-to-move-after-1.e4? No: for a BLACK book the node after 1.e4 is
    # OURS (Black to move); the root (White to move) is the opponent's.
    vb0, _ = go(b_edges, colour="black")
    aux_l = [aux_row(opp, term_normal_total=200, term_normal_black_wins=200)]
    vb1, _ = go(b_edges, aux_l, colour="black")
    want_l = (0.50 * 1000 + 0.0 * 200) / 1200          # our-node collapse, Black wins
    check(abs(vb1[opp] - want_l) < TOL,
          f"L: BLACK book — opponent resigning on our turn pulls value DOWN "
          f"({vb0[opp]:.9f} -> {vb1[opp]:.9f}, want {want_l:.9f})")
    aux_m = [aux_row(opp, term_normal_total=200, term_normal_white_wins=200)]
    vb2, _ = go(b_edges, aux_m, colour="black")
    check(abs(vb2[opp] - vb0[opp]) < TOL,
          f"M: BLACK book — OUR losses at our node still excluded "
          f"({vb2[opp]:.9f} == {vb0[opp]:.9f})")

    print()
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
