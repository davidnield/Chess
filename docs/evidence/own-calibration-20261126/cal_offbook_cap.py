"""Follow-up to the own calibration (26 Nov): does the observation cap help where contamination would act,
i.e. for OFF-BOOK movers at positions with many training games? Chosen transform (42, no cap) against its
cap-100 and cap-1000 twins, per support bin, on/off book, both splits, opening-family bootstrap.
Writes D:/chess/own-calibration-20261126/run/offbook-cap.json. Run with PYTHONPATH=<release>/python."""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from sweep import own_calibration as oc
from sweep.model_contract import calibration_grid

RUN = Path("D:/chess/own-calibration-20261126/run")
panel = pd.read_parquet(RUN / "panel.parquet")
boards = pd.read_parquet(RUN / "counts.parquet")
boards["legal"] = boards.legal.map(list)
boards["counts"] = boards.counts.map(list)
raw = {(x, y): list(pd.read_parquet(RUN / f"maia-{x}-{y}.parquet").raw.map(list)) for x, y in oc.PAIRS}
grid, nll, n_row, c_row, bi, mi = oc.score(panel, boards, raw, check_native=50)
chosen = json.loads((RUN / "report.json").read_text())["chosen"]
g = grid[chosen]
twins = {h.get("observation_cap"): k for k, h in enumerate(grid)
         if all(h[x] == g[x] for x in ("ratings", "temperature", "strength"))}
# off-book flag exactly as the contamination study defines it
p = panel.assign(n=n_row, c=c_row).sort_values(["game_id", "colour", "ply"])
rare = (np.where(p.n > 0, p.c / np.maximum(p.n, 1), 0.0) < oc.OFF_BOOK_SHARE).astype(int)
off = p.assign(r=rare).groupby(["game_id", "colour"]).r.transform(lambda s: s.shift(fill_value=0).cummax())
off = off.reindex(panel.index).astype(bool).to_numpy()
out = dict(chosen=chosen, recipe=g, comparisons=[])
for split in oc.SPLITS:
    for lo, hi in ((100, 999), (1000, None)):
        sup = (n_row >= lo) & (n_row <= hi if hi else True)
        for flag in (False, True):
            m = (panel.split == split).to_numpy() & sup & (off == flag)
            if not m.any():
                continue
            for cap in (100.0, 1000.0):
                if cap not in twins:
                    continue
                d = nll[m, twins[cap]] - nll[m, twins[None]]     # negative = the cap fits better
                r = oc.family_bootstrap(d, panel.family.to_numpy()[m])
                out["comparisons"].append(dict(split=split, support=f"{lo}-{hi or ''}", off_book=flag, cap=cap, **r))
                print(f"{split:12s} n {lo}-{hi or '':5} off_book={flag!s:5} cap {cap:6}: cap minus none {r['mean']:+.5f} "
                      f"[{r['ci95'][0]:+.5f}, {r['ci95'][1]:+.5f}]  moves {r['moves']:,}")
(RUN / "offbook-cap.json").write_text(json.dumps(out, indent=1))
