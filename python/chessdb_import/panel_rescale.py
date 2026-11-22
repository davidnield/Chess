"""Panel scale analysis (ChatGPT review R4): the computations behind "ChessDB wins after rescaling", saved as a script.

For a scored panel (eval_panel.py report output), per source group and source: median absolute error, MAE on
balanced positions (|ref| <= 300), MAE on all positions, the least-squares slope of the reference on the source
through the origin (|ref| <= 1000), and MAE after multiplying the source by that slope. The slope is fitted and
scored on the SAME rows (in-sample), so the rescaled MAE is an optimistic, descriptive number, not a validated rule.

Usage: python panel_rescale.py E:/chess/eval-panel-20261122/panel_scored_d24.parquet [out.csv]
"""
import sys

import numpy as np
import pandas as pd

d = pd.read_parquet(sys.argv[1])
rows = []
for g, x in d.groupby("grp", sort=False):
    for s in ("current", "cloud", "fishnet", "chessdb", "rule_13nov"):
        y = x[x[s].notna() & x.sf.notna()]
        if len(y) < 20:
            continue
        a, b = y[s].astype(float).values, y.sf.astype(float).values
        e = np.abs(a - b)
        m = np.abs(b) <= 1000
        k = (a[m] @ b[m]) / (a[m] @ a[m]) if m.any() else np.nan
        rows.append(dict(group=g, source=s, n=len(y), median_abs=np.median(e), mae_balanced=e[np.abs(b) <= 300].mean(),
                         mae_all=e.mean(), slope=k, n_slope=int(m.sum()), mae_rescaled=np.abs(k * a[m] - b[m]).mean()))
r = pd.DataFrame(rows).round(2)
print(r.to_string(index=False))
if len(sys.argv) > 2:
    r.to_csv(sys.argv[2], index=False)
