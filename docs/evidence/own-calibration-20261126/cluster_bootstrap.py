"""Review 2026-11-26 F5, step 1: the confirmation panel's paired errors under a cluster bootstrap.

The panel's positions carry no move history, so "opening family" is approximated by board similarity:
two positions are linked when at least T of the 64 squares hold the same thing (piece or empty), and a
family is a connected component. Several T are reported, from strict to loose, plus a coarse family key
(the two sides' pawn skeletons on their own halves). Clusters are resampled whole; the statistic is the
same stratum-macro mean error difference as the original report, recomputed with weights = how many
times each position's cluster was drawn. Tails as the original: 95% and simultaneous (2 comparisons).

Reads the OPENED confirmation (D:/chess/chessdb-ensemble-confirmation-20261125-v1/report); writes
D:/chess/acceptance/review-20261126/cluster-bootstrap.json. Touches nothing in the experiment folder.
Run with PYTHONPATH=<worktree>/python.
"""
import json
from pathlib import Path

import chess
import numpy as np
import pandas as pd

import chessdb_ensemble_study as study
import chessdb_policy_validation as ref

EXP = Path("D:/chess/chessdb-ensemble-confirmation-20261125-v1")
OUT = Path("D:/chess/acceptance/review-20261126/cluster-bootstrap.json")
REPS, SEED = 20000, 20261126


def squares(epd):
    b = chess.Board(epd + " 0 1")
    return np.array([(p.piece_type * (1 if p.color else -1)) if (p := b.piece_at(s)) else 0
                     for s in chess.SQUARES], np.int8)


def components(link):
    n = len(link)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for i in range(n):
        for j in np.nonzero(link[i])[0]:
            if j > i:
                parent[find(i)] = find(int(j))
    return np.array([find(i) for i in range(n)])


def pawn_key(epd):
    b = chess.Board(epd + " 0 1")
    w = sorted(s for s in b.pieces(chess.PAWN, chess.WHITE) if chess.square_rank(s) <= 3)
    k = sorted(s for s in b.pieces(chess.PAWN, chess.BLACK) if chess.square_rank(s) >= 4)
    return f"{w}|{k}"


def main():
    choice = json.loads((EXP / "choice.json").read_text())["payload"]
    opened = json.loads((EXP / "OPENED.json").read_text())
    d = pd.read_parquet(EXP / "report" / "confirmation-scored.parquet")
    q = d[d.qualified].reset_index(drop=True)
    p = study.predict(q, choice["recipes"])
    err = 100 * abs(p.sub(ref.score(q.sf), axis=0))
    strata = q.stratum.to_numpy()
    su = sorted(set(strata))
    S = np.array([[s == u for s in strata] for u in su], float)          # strata x positions
    boards = np.stack([squares(e) for e in q.epd])
    same = (boards[:, None, :] == boards[None, :, :]).sum(axis=2)
    keys = {f"similarity>={t}": components(same >= t) for t in (60, 56, 52, 48, 44)}
    keys["pawn-skeleton"] = pd.factorize(q.epd.map(pawn_key))[0]
    keys["none (ordinary)"] = np.arange(len(q))
    rng = np.random.default_rng(SEED)
    bases = [b for b in dict.fromkeys(choice["comparators"]) if b != choice["contender"]]
    tail = .025 / max(1, len(bases))
    report = dict(source=str(EXP), opened=opened, n=len(q), reps=REPS, seed=SEED, comparisons={})
    for b in bases:
        delta = (err[choice["contender"]] - err[b]).to_numpy()
        point = float(np.mean(S @ delta / S.sum(axis=1)))
        rows = {}
        for name, cl in keys.items():
            ids, inv = np.unique(cl, return_inverse=True)
            draws = rng.integers(0, len(ids), size=(REPS, len(ids)))
            counts = np.zeros((REPS, len(ids)))
            np.add.at(counts, (np.repeat(np.arange(REPS), len(ids)), draws.ravel()), 1)
            w = counts[:, inv]                                            # reps x positions
            num = w * delta                                               # stratum-macro mean per rep
            per = np.stack([(num * S[k]).sum(1) / np.maximum((w * S[k]).sum(1), 1e-12) for k in range(len(su))], 1)
            ok = (np.stack([(w * S[k]).sum(1) for k in range(len(su))], 1) > 0).all(1)
            m = per[ok].mean(1)
            sizes = np.bincount(inv)
            rows[name] = dict(clusters=int(len(ids)), largest=int(sizes.max()),
                              positions_in_multi=int(sizes[sizes > 1].sum()),
                              ci95=[float(np.quantile(m, .025)), float(np.quantile(m, .975))],
                              simultaneous=[float(np.quantile(m, tail)), float(np.quantile(m, 1 - tail))],
                              reps_used=int(ok.sum()))
        report["comparisons"][f"{choice['contender']} - {b}"] = dict(delta_macro_mae_points=point, by_clustering=rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=1), encoding="utf8")
    for c, r in report["comparisons"].items():
        print(c, f"{r['delta_macro_mae_points']:+.4f}")
        for name, x in r["by_clustering"].items():
            print(f"  {name:18s} clusters {x['clusters']:3d} largest {x['largest']:3d} in-multi {x['positions_in_multi']:3d}"
                  f"  95% [{x['ci95'][0]:+.4f}, {x['ci95'][1]:+.4f}]  simult [{x['simultaneous'][0]:+.4f}, {x['simultaneous'][1]:+.4f}]")


if __name__ == "__main__":
    main()
