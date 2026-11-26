"""Is Stockfish 19's centipawn scale different from Stockfish 18's? (2026-11-22)

The step-2 panel found SF19 depth-24 scores ~1.3-1.5x more extreme than cloud and ChessDB (~1.1-1.2x
fishnet). Version or depth? Re-search 50 panel positions per group with SF18 (engine_path.STOCKFISH, the binary
every tool used until 2026-11-21 and again since 11-22) at the same depth, one thread, fresh engine per position, and compare the slopes.
Output: E:/chess/eval-panel-20261122/scale_sf18_d24.jsonl (resumable) and a printed table.

Usage: python sf_scale_check.py [--per-group 50] [--workers 4] [--depth 24]
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from engine_path import STOCKFISH as SF18  # noqa: E402  (same binary, sha256 c86215fa)

OUT = Path("E:/chess/eval-panel-20261122")


def one(task: dict) -> dict:
    import chess
    import chess.engine
    eng = chess.engine.SimpleEngine.popen_uci(SF18)
    try:
        eng.configure({"Threads": 1, "Hash": 128})
        info = eng.analyse(chess.Board(task["fen"]), chess.engine.Limit(depth=task["depth"]), game=object())
        s = info["score"].white()
        return dict(task, cp_white=max(-2000, min(2000, s.score(mate_score=100000))), engine=eng.id.get("name"))
    finally:
        eng.quit()


def main() -> None:
    import numpy as np
    import pandas as pd
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-group", type=int, default=50)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--depth", type=int, default=24)
    a = ap.parse_args()
    d = pd.read_parquet(OUT / f"panel_scored_d{a.depth}.parquet")
    pick = d.groupby("grp", group_keys=False).apply(lambda x: x.sort_values("id").head(a.per_group))
    res = OUT / f"scale_sf18_d{a.depth}.jsonl"
    done = {json.loads(l)["id"] for l in res.read_text().splitlines() if l.strip()} if res.exists() else set()
    todo = [dict(id=r.id, fen=r.epd + " 0 1", depth=a.depth) for r in pick.itertuples() if r.id not in done]
    print(f"{len(todo)} SF18 searches", flush=True)
    with res.open("a", encoding="utf8") as f, cf.ProcessPoolExecutor(a.workers) as ex:
        it = iter(todo)
        live = {ex.submit(one, t) for t in [next(it) for _ in range(min(a.workers, len(todo)))]}
        while live:
            fin, live = cf.wait(live, return_when=cf.FIRST_COMPLETED)
            for x in fin:
                f.write(json.dumps(x.result()) + "\n")
                f.flush()
                nxt = next(it, None)
                if nxt is not None:
                    live.add(ex.submit(one, nxt))
    r = pd.read_json(res, lines=True).set_index("id").cp_white
    x = pick.assign(sf18=pick.id.map(r)).dropna(subset=["sf18"])
    x = x[(x.sf.abs() <= 1000) & (x.sf18.abs() <= 1000)]
    print(f"engine: {pd.read_json(res, lines=True).engine.iloc[0]}; {len(x)} positions with |score| <= 1000")

    def slope(a_, b_):
        a_, b_ = np.asarray(a_, float), np.asarray(b_, float)
        return (a_ @ b_) / (a_ @ a_)
    print(f"SF19 on SF18 slope {slope(x.sf18, x.sf):.3f}   MAE SF19 vs SF18 {np.abs(x.sf - x.sf18).mean():.1f} cp")
    for s in ("cloud", "fishnet", "chessdb"):
        y = x[x[s].notna()]
        print(f"  {s:8s} n={len(y):3d}  slope SF19/source {slope(y[s], y.sf):.2f}  SF18/source {slope(y[s], y.sf18):.2f}  "
              f"MAE vs SF19 {np.abs(y[s] - y.sf).mean():.1f}  vs SF18 {np.abs(y[s] - y.sf18).mean():.1f}")


if __name__ == "__main__":
    main()
