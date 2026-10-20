"""Eval-to-score calibration by rating (plan item 8, 2026-10-20).

Every engine valuation in the pipeline turns centipawns into an expected score with
ONE logistic curve, Lichess's 1 / (1 + exp(-0.00368208 * cp)). Lichess fitted it on
2300+ rapid games (lila PR #11148; the author expected the slope near 0.002 at
lower ratings), so it likely overstates how often weaker players convert an
advantage. The weak-us stop model (weak_stop.py) leans on it hardest.

This fits the curve per rating pairing from real games: for a sample of games, the
position at fixed plies is hashed (the replay's incremental hasher), its eval read
from the eval arrays, and the game's result paired with it. Per cell
(side-to-move rating band x opponent rating band x event x ply) it fits

    E[score of the side to move] = 1 / (1 + exp(-(a + k * cp_stm)))

by binomial maximum likelihood with fractional outcomes (draw = 0.5), cp from the
side to move's view, clipped to +-1000 (mates included at the clip). `a` carries the
rating gap (a weaker side to move scores less at equal eval); `k` is the conversion
slope. Output <out>/eval_curves.parquet (one row per cell: a, k, n, log-loss of the
fit and of the uniform curve) and the raw sample <out>/eval_sample.parquet.

Fit on months inside the pool's training period (default 2025-08..2026-07) so the
2026-08..09 holdout stays clean; --check-months scores the fitted curves and the
uniform one on held-out games.

    .venv/Scripts/python.exe python/fit_eval_curve.py [--games-per-file 40000]
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import chess
import numpy as np
import polars as pl
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import discover_source_files  # noqa: E402
from stage1_extract_positions import iter_san_moves  # noqa: E402
from stage3_backwards_induction import LICHESS_CP_SCALE  # noqa: E402
from zobrist import IncrementalZobrist  # noqa: E402

OUT = Path("E:/chess/calibration")
PLIES = (16, 24)
BANDS = [0, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]
CLIP = 1000
# Pooled cells (all events, both plies): (label, side-to-move Elo [lo, hi), opponent
# Elo [lo, hi)). The weak-us model (weak_stop.py) reads the first two: at our turn
# we (1000-1799) are to move against 1800+, at theirs the reverse.
POOLED = (("stm1000-1799_vs_opp1800+", (1000, 1800), (1800, 9999)),
          ("stm1800+_vs_opp1000-1799", (1800, 9999), (1000, 1800)),
          ("stm1800+_vs_opp1800+", (1800, 9999), (1800, 9999)),
          ("stm1000-1799_vs_opp1000-1799", (1000, 1800), (1000, 1800)))


def pooled_cond(stm: tuple[int, int], opp: tuple[int, int]) -> pl.Expr:
    return (pl.col("elo_stm").is_between(stm[0], stm[1], closed="left")
            & pl.col("elo_opp").is_between(opp[0], opp[1], closed="left"))


def band(elo: np.ndarray) -> np.ndarray:
    b = np.zeros(len(elo), dtype=np.int32)
    for edge in BANDS[1:]:
        b = np.where(elo >= edge, edge, b)
    return b


def sample_file(args) -> pl.DataFrame:
    path, event, n_games, seed = args
    pf = pq.ParquetFile(path)
    rng = np.random.default_rng(seed)
    rows = []
    taken = 0
    board = chess.Board()
    hasher = IncrementalZobrist(board)
    for batch in pf.iter_batches(batch_size=50_000,
                                 columns=["movetext", "white_elo", "black_elo", "white_score"]):
        df = pl.from_arrow(batch).filter(pl.col("white_score").is_not_null()
                                         & pl.col("white_elo").is_not_null()
                                         & pl.col("black_elo").is_not_null())
        if not df.height:
            continue
        keep = rng.random(df.height) < max(0.0, (n_games - taken) / max(1, df.height)) * 2
        df = df.filter(pl.Series(keep))
        for mt, we, be, ws in df.iter_rows():
            if taken >= n_games:
                break
            board.reset()
            hasher.reset(board)
            ply = 0
            try:
                for san in iter_san_moves(mt or ""):
                    mv = board.parse_san(san)
                    hasher.push_move(board, mv)
                    ply += 1
                    if ply in PLIES:
                        rows.append((hasher.current(board), ply, int(we), int(be), float(ws)))
                    if ply >= PLIES[-1]:
                        break
            except ValueError:
                pass
            taken += 1
        if taken >= n_games:
            break
    return pl.DataFrame(rows, schema={"position_hash": pl.Int64, "ply": pl.Int32,
                                      "white_elo": pl.Int32, "black_elo": pl.Int32,
                                      "white_score": pl.Float64}, orient="row") \
        .with_columns(pl.lit(event).alias("event"))


def fit_logistic(x: np.ndarray, y: np.ndarray, iters: int = 50) -> tuple[float, float]:
    """MLE of P(y) = sigmoid(a + k x) with fractional y, Newton-Raphson."""
    a, k = 0.0, LICHESS_CP_SCALE
    for _ in range(iters):
        z = np.clip(a + k * x, -30, 30)
        p = 1.0 / (1.0 + np.exp(-z))
        w = np.maximum(p * (1 - p), 1e-9)
        ga, gk = (y - p).sum(), ((y - p) * x).sum()
        haa, hak, hkk = w.sum(), (w * x).sum(), (w * x * x).sum()
        det = haa * hkk - hak * hak
        if det <= 0:
            break
        da = (hkk * ga - hak * gk) / det
        dk = (-hak * ga + haa * gk) / det
        a, k = a + da, k + dk
        if abs(da) < 1e-10 and abs(dk) < 1e-12:
            break
    return float(a), float(k)


def logloss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.clip(p, 1e-9, 1 - 1e-9)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def stm_frame(s: pl.DataFrame) -> pl.DataFrame:
    """One row per sampled position, from the side to move's view."""
    white_tm = (pl.col("ply") % 2 == 0)
    return s.with_columns(
        pl.when(white_tm).then(pl.col("cp")).otherwise(-pl.col("cp")).clip(-CLIP, CLIP).alias("x"),
        pl.when(white_tm).then(pl.col("white_score")).otherwise(1 - pl.col("white_score")).alias("y"),
        pl.when(white_tm).then(pl.col("white_elo")).otherwise(pl.col("black_elo")).alias("elo_stm"),
        pl.when(white_tm).then(pl.col("black_elo")).otherwise(pl.col("white_elo")).alias("elo_opp"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--start", default="2025-08")
    ap.add_argument("--end", default="2026-07")
    ap.add_argument("--check-months", default="2026-08,2026-09")
    ap.add_argument("--games-per-file", type=int, default=40_000)
    ap.add_argument("--events", default="Blitz,Rapid,Classical")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--min-cell", type=int, default=2000)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    events = a.events.split(",")

    def files_for(spans):
        fs = []
        for y0m0, y1m1 in spans:
            y0, m0 = map(int, y0m0.split("-"))
            y1, m1 = map(int, y1m1.split("-"))
            for f, y, m, ev in discover_source_files(y0, y1, None, events):
                if (y0, m0) <= (y, m) <= (y1, m1):
                    fs.append((f, ev))
        return fs

    from eval_arrays import MISSING, lookup_evals, open_eval_arrays
    mm = open_eval_arrays()

    def collect(fs, tag):
        cache = out / f"eval_sample_{tag}.parquet"
        if cache.exists():
            return pl.read_parquet(cache)
        jobs = [(str(f), ev, a.games_per_file, i) for i, (f, ev) in enumerate(fs)]
        with Pool(a.workers) as pool:
            parts = list(pool.imap_unordered(sample_file, jobs))
        s = pl.concat([p for p in parts if p.height])
        cp = lookup_evals(s["position_hash"].to_numpy(), *mm).astype(np.int64)
        s = s.with_columns(pl.Series("cp", cp)).filter(pl.col("cp") != MISSING)
        s.write_parquet(cache)
        print(f"  {tag}: {s.height:,} evaluated positions from {len(fs)} files "
              f"({time.time() - t0:,.0f}s)", flush=True)
        return s

    fit_files = files_for([(a.start, a.end)])
    chk = [m.strip() for m in a.check_months.split(",") if m.strip()]
    chk_files = files_for([(m, m) for m in chk]) if chk else []
    s = stm_frame(collect(fit_files, "fit"))
    s = s.with_columns(pl.Series("b_stm", band(s["elo_stm"].to_numpy())),
                       pl.Series("b_opp", band(s["elo_opp"].to_numpy())))
    rows = []
    for (ev, ply, bs, bo), g in s.group_by(["event", "ply", "b_stm", "b_opp"]):
        if g.height < a.min_cell:
            continue
        x, y = g["x"].to_numpy().astype(float), g["y"].to_numpy()
        aa, kk = fit_logistic(x, y)
        p_fit = 1 / (1 + np.exp(-(aa + kk * x)))
        p_uni = 1 / (1 + np.exp(-LICHESS_CP_SCALE * x))
        rows.append({"event": ev, "ply": ply, "band_stm": bs, "band_opp": bo, "n": g.height,
                     "a": aa, "k": kk, "logloss_fit": logloss(p_fit, y),
                     "logloss_uniform": logloss(p_uni, y),
                     "mean_score": float(y.mean()), "mean_pred_uniform": float(p_uni.mean())})
    cur = pl.DataFrame(rows).sort(["event", "ply", "band_stm", "band_opp"])
    pooled = []
    for label, stm, opp in POOLED:
        g = s.filter(pooled_cond(stm, opp))
        if not g.height:
            continue
        x, y = g["x"].to_numpy().astype(float), g["y"].to_numpy()
        aa, kk = fit_logistic(x, y)
        pooled.append({"cell": label, "n": g.height, "a": aa, "k": kk,
                       "logloss_fit": logloss(1 / (1 + np.exp(-(aa + kk * x))), y),
                       "logloss_uniform": logloss(1 / (1 + np.exp(-LICHESS_CP_SCALE * x)), y)})
    pooled = pl.DataFrame(pooled)
    if chk_files:
        c = stm_frame(collect(chk_files, "check"))
        chk_rows = []
        cells = {lab: (stm, opp) for lab, stm, opp in POOLED}
        for r in pooled.iter_rows(named=True):
            lab = r["cell"]
            g = c.filter(pooled_cond(*cells[lab]))
            x, y = g["x"].to_numpy().astype(float), g["y"].to_numpy()
            chk_rows.append({"cell": lab, "check_n": g.height,
                             "check_logloss_fit": logloss(1 / (1 + np.exp(-(r["a"] + r["k"] * x))), y),
                             "check_logloss_uniform": logloss(1 / (1 + np.exp(-LICHESS_CP_SCALE * x)), y)})
        pooled = pooled.join(pl.DataFrame(chk_rows), on="cell", how="left")
    cur.write_parquet(out / "eval_curves.parquet")
    pooled.write_parquet(out / "eval_curves_pooled.parquet")
    with pl.Config(tbl_rows=60, tbl_width_chars=200):
        print(pooled)
    print(f"done in {time.time() - t0:,.0f}s -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
