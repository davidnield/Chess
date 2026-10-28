"""empirical_stop.build on a hand-made pool: values checked by hand.

Pool (White's view): 1.e4 (100 games) / 1.d4 (50); after 1.e4 the pool replies 1...e5 (60) /
1...c5 (40), and 10 more games ended there, all won by White (aux term bucket); after 1.d4,
1...d5 (10). Weak players at the start: 1.e4 30, 1.d4 10. Engine values (our view) are injected:
after 1.e4 e5 0.60, after 1.e4 c5 0.40, after 1.d4 d5 0.50.

Run:  .venv/Scripts/python.exe python/_test_empirical_stop.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.stdout.reconfigure(encoding="utf-8")

import chess
import numpy as np
import polars as pl

from empirical_stop import build
from zobrist import zobrist_int64

FAILS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAILS
    print(f"  [{'ok ' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
    FAILS += not cond


def pos(*sans):
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return b


def h(*sans):
    return zobrist_int64(pos(*sans))


def edge(parent, san, n):
    b = pos(*parent)
    return {"parent_hash": zobrist_int64(b), "move_san": san, "parent_epd": b.epd(),
            "child_hash": h(*parent, san), "ply": len(parent) + 1, "white_wins": 0, "draws": 0,
            "black_wins": 0, "total": n, "event": "Pooled", "elo_band": 0, "white_score_avg": 0.5}


tmp = Path(tempfile.mkdtemp(prefix="empstop_"))
pool = tmp / "position_stats_t.parquet"
pl.DataFrame([edge((), "e4", 100), edge((), "d4", 50), edge(("e4",), "e5", 60),
              edge(("e4",), "c5", 40), edge(("d4",), "d5", 10)]).write_parquet(pool)
zero = {k: 0 for k in ("term_normal_total", "term_normal_white_wins", "term_normal_draws",
                       "term_normal_black_wins", "term_flag_total", "term_flag_white_wins",
                       "term_flag_draws", "term_flag_black_wins", "term_other_total",
                       "term_other_white_wins", "term_other_draws", "term_other_black_wins",
                       "horizon_total", "horizon_white_wins", "horizon_draws", "horizon_black_wins",
                       "other_total", "other_white_wins", "other_draws", "other_black_wins",
                       "other_edges")}
aux = tmp / "position_stats_aux_t.parquet"
pl.DataFrame([{"position_hash": h("e4"), **zero, "term_normal_total": 10, "term_normal_white_wins": 10,
               "other_eval_mean": None, "other_eval_min": None, "other_eval_max": None,
               "other_eval_cov": 0.0}]).write_parquet(aux)

E = {h("e4", "e5"): 0.60, h("e4", "c5"): 0.40, h("d4", "d5"): 0.50}


def evals(keys, our_turn=True):
    return np.array([E.get(int(k), np.nan) for k in keys], dtype=float)


def weak(n_e4, n_d4):
    p = tmp / f"weak_{n_e4}_{n_d4}.parquet"
    pl.DataFrame({"parent_hash": [h(), h()], "move_san": ["e4", "d4"],
                  "child_hash": [h("e4"), h("d4")], "total": [float(n_e4), float(n_d4)]}).write_parquet(p)
    return str(p)


print("played out with weak moves at our turn and the pool's (plus finished games) at theirs:")
df = build(str(pool), str(aux), weak(30, 10), "white", evals=evals, log=lambda s: None)
V = dict(zip(df["position_hash"].to_list(), df["v"].to_list()))
v_e4 = (60 * 0.60 + 40 * 0.40 + 10 * 1.0) / 110
check("after 1.e4: replies by games, the 10 finished games count as wins", abs(V[h("e4")] - v_e4) < 1e-12,
      f"{V[h('e4')]} vs {v_e4}")
check("after 1.d4: its one reply's engine value", abs(V[h("d4")] - 0.50) < 1e-12)
check("start: weak shares 30:10", abs(V[h()] - (0.75 * v_e4 + 0.25 * 0.50)) < 1e-12, str(V[h()]))
src = dict(zip(df["position_hash"].to_list(), df["source"].to_list()))
check("start marked as played out with weak moves", src[h()] == 0)

print("\nfewer than --min-weak weak games: the pool's own moves:")
df = build(str(pool), str(aux), weak(5, 5), "white", evals=evals, log=lambda s: None)
V = dict(zip(df["position_hash"].to_list(), df["v"].to_list()))
src = dict(zip(df["position_hash"].to_list(), df["source"].to_list()))
check("start uses pool shares 100:50", abs(V[h()] - (100 * v_e4 + 50 * 0.50) / 150) < 1e-12, str(V[h()]))
check("and is marked as such", src[h()] == 2)

print("\nBlack's view flips every value:")
dfb = build(str(pool), str(aux), weak(30, 10), "black",
            evals=lambda k, our_turn=True: 1 - evals(k), log=lambda s: None)
Vb = dict(zip(dfb["position_hash"].to_list(), dfb["v"].to_list()))
check("after 1.e4 Black is to move (our turn), no weak rows there: the pool's replies, our view",
      abs(Vb[h("e4")] - (60 * 0.40 + 40 * 0.60) / 100) < 1e-12, str(Vb[h("e4")]))

print(f"\n{'ALL PASS' if FAILS == 0 else f'{FAILS} FAILURES'}")
sys.exit(1 if FAILS else 0)
