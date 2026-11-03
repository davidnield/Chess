"""Is the referee's fold bootstrap a valid game bootstrap? (follow-up review R2b, 2026-10-31)

The referee's replicates reweight F hash folds of games (convertibility_eval.fold_weights).
Its target is the ordinary game bootstrap: resample the N games, every game moving all of its
edges together. This study checks the approximation where the answer is known, on a synthetic
world small enough to walk exactly:

  world      positions to --max-ply from the start; at every position the TRUE move shares are
             a fixed random draw over three quiet moves, the Maia prior's shares are 3:2:1 over
             the same moves, and each leaf's engine value is a fixed function of its hash
  data       N games sampled from the true shares; our-turn moves go to our table and the
             opponent's to theirs, each game carrying one id (so one fold) in both
  estimator  the referee's exact walk (enumerate_book, tau 0) of an empty book on those
             tables, the counts blended with the prior at --k games
  truth      the estimator's real sampling spread: its SD over --datasets independent datasets

For each dataset every method draws --reps replicates; a method is calibrated when its mean
bootstrap SE matches the true SD and its 90% intervals cover the estimator's mean ~90% of the
time. Methods: per-game Poisson(1) and multinomial (the reference bootstraps), and F-fold
Poisson weights raw (the referee before the fix) and normalised to sum to F (after).

    PYTHONPATH=python .venv/Scripts/python.exe python/bootstrap_validation.py \\
        [--games 200] [--k 100] [--datasets 200] [--reps 200] [--out PATH.json]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
import time
from types import SimpleNamespace

import chess
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import convertibility_eval as cv  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

PREF = ["Nf3", "Nc3", "Nf6", "Nc6", "Ng1", "Nb1", "Ng8", "Nb8", "e3", "e6", "d3", "d6",
        "a3", "a6", "h3", "h6"]


def moves_at(b: chess.Board) -> list[str]:
    legal = {b.san(m) for m in b.legal_moves}
    return [m for m in PREF if m in legal][:3]


def true_shares(h: int, ms: list[str]) -> dict:
    w = [-math.log(1.0 - cv.u01(17, h, i)) for i in range(len(ms))]   # Dirichlet(1)
    return {m: x / sum(w) for m, x in zip(ms, w)}


class PriorMaia:
    def probs(self, epds, se, oe, keys):
        out = []
        for e in epds:
            ms = moves_at(chess.Board(e + " 0 1"))
            w = [3.0, 2.0, 1.0][:len(ms)]
            out.append({m: x / sum(w) for m, x in zip(ms, w)})
        return out


def lookup(hs):
    return np.array([(int(h) % 401) - 200 for h in hs], dtype=np.int64)


class GameCounts:
    """Counts by position as weighted sums over games: self.w (one weight per game) is set
    per replicate; get() ignores `rep` and reads the current weights."""

    def __init__(self, n_games: int):
        self.by: dict[int, dict[str, list[int]]] = {}
        self.w = np.ones(n_games)

    def add(self, h, san, g):
        self.by.setdefault(h, {}).setdefault(san, []).append(g)

    def get(self, hashes, rep=0):
        out = []
        for h in hashes:
            d = self.by.get(h, {})
            out.append({m: float(self.w[gs].sum()) for m, gs in d.items() if self.w[gs].sum() > 0})
        return out


def simulate(n: int, max_ply: int, rng) -> tuple[GameCounts, GameCounts]:
    ours, theirs = GameCounts(n), GameCounts(n)
    for g in range(n):
        b = chess.Board()
        for ply in range(max_ply):
            h = zobrist_int64(b)
            ms = moves_at(b)
            ts = true_shares(h, ms)
            san = rng.choice(ms, p=[ts[m] for m in ms])
            (ours if b.turn == chess.WHITE else theirs).add(h, san, g)
            b.push_san(san)
    return ours, theirs


def estimate(ours, theirs, a) -> float:
    leaves, _, _, _ = cv.enumerate_book({}, True, 0, a, ours, theirs, PriorMaia(), lookup)
    v = cv.value_leaves(leaves, True, lookup, None, ((0.0, 0.004), (0.0, 0.004)))
    return float((v["ps"] * v["uniform"]).sum() / v["ps"].sum())


def weights(method: str, n: int, folds_of: dict, rng) -> np.ndarray:
    if method == "game_poisson":
        return rng.poisson(1.0, n).astype(float)
    if method == "game_multinomial":
        return rng.multinomial(n, np.full(n, 1.0 / n)).astype(float)
    kind, F = method.rsplit("_", 1)
    F = int(F)
    w = rng.poisson(1.0, F).astype(float)
    while w.sum() == 0:
        w = rng.poisson(1.0, F).astype(float)
    if kind == "fold_norm":
        w = w * F / w.sum()
    return w[folds_of[F]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", type=int, default=200)
    ap.add_argument("--k", type=float, default=100.0)
    ap.add_argument("--max-ply", type=int, default=4)
    ap.add_argument("--datasets", type=int, default=200)
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--folds", default="16,64,256")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    opts = SimpleNamespace(tau=0.0, rollouts=0, max_ply=a.max_ply, k=a.k, min_p=0.0,
                           our_elo=1450, opp_elo=2000)
    Fs = [int(x) for x in a.folds.split(",")]
    methods = ["game_poisson", "game_multinomial"] + \
        [f"fold_{k}_{F}" for F in Fs for k in ("raw", "norm")]
    rng = np.random.default_rng(a.seed)
    folds_of = {F: np.array([cv.u01(5, g) * F for g in range(a.games)], dtype=int) for F in Fs}
    est, se, lo, hi = [], {m: [] for m in methods}, {m: [] for m in methods}, {m: [] for m in methods}
    t0 = time.time()
    for d in range(a.datasets):
        ours, theirs = simulate(a.games, a.max_ply, rng)
        ours.w[:] = theirs.w[:] = 1.0
        e0 = estimate(ours, theirs, opts)
        est.append(e0)
        for m in methods:
            reps = []
            for _ in range(a.reps):
                w = weights(m, a.games, folds_of, rng)
                ours.w, theirs.w = w, w               # one weight per game, both tables
                reps.append(estimate(ours, theirs, opts))
            reps = np.array(reps)
            se[m].append(float(reps.std(ddof=1)))
            lo[m].append(float(e0 - 1.6449 * reps.std(ddof=1)))
            hi[m].append(float(e0 + 1.6449 * reps.std(ddof=1)))
        if d % 10 == 9:
            print(f"{d + 1}/{a.datasets} datasets ({time.time() - t0:,.0f}s)", flush=True)
    est = np.array(est)
    sd_true, centre = float(est.std(ddof=1)), float(est.mean())
    res = {"games": a.games, "k": a.k, "max_ply": a.max_ply, "datasets": a.datasets,
           "reps": a.reps, "true_sd": sd_true, "estimator_mean": centre, "methods": {}}
    for m in methods:
        cov = float(np.mean((np.array(lo[m]) <= centre) & (centre <= np.array(hi[m]))))
        res["methods"][m] = {
            "mean_se": float(np.mean(se[m])), "se_ratio": float(np.mean(se[m]) / sd_true),
            "sd_of_se": float(np.std(se[m])), "coverage_90": cov,
            "coverage_mc_se": float(math.sqrt(0.9 * 0.1 / a.datasets))}
    print(json.dumps(res, indent=1))
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=1)


if __name__ == "__main__":
    main()
