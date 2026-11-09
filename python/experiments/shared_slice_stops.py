"""Use the production fit and its own weak-play inputs on a new set of boards."""
from collections import Counter
import hashlib
import json
from pathlib import Path

import chess
import numpy as np
import polars as pl

from budget_core import verified_cp_overrides
from sampled_stop import predict
from weak_stop import EvalSource, weak_stop_values


class Stops:
    def __init__(self, folder, data):
        book = json.loads((folder/"practical_black_b2000.parquet.meta.json").read_text())
        tab = folder/"stop-table.parquet"
        meta = json.loads(Path(str(tab)+".meta.json").read_text())
        self.recipe = meta["dump"]
        dump = folder/"stop-positions.parquet"
        if (self.recipe != json.loads(Path(str(dump)+".meta.json").read_text())
                or Path(book["sampled_stop"]).resolve()!=tab.resolve()
                or self.recipe["colour"]!="black" or self.recipe["horizon"]!=30
                or meta["sim"]["horizon"]!=30 or book.get("weak_eval_curve")):
            raise ValueError("Continuation model does not match the Black ply-30 build")
        self.model = book["sampled_fit"]["model"]
        proposal = Path(book["engine_candidates"])
        with proposal.open("rb") as f:
            if hashlib.file_digest(f,"sha256").hexdigest()!=self.recipe["engine_candidates_sha256"]:
                raise ValueError("Changed engine proposal source")
        cp = verified_cp_overrides(pl.read_parquet(proposal))
        data.cp.update(cp)
        self.source = EvalSource(False,overrides=cp)
        self.data = data
        table = pl.read_parquet(dump,columns=["hash","side","stop","exact","ply"])
        self.indices = {}
        self.samples = {}
        for side in ("x","y"):
            rows = table.filter(pl.col("side")==side).sort("hash")
            self.indices[side] = tuple(rows[c].to_numpy() for c in ("hash","stop","exact"))
            samples = (pl.read_parquet(tab)
                       .filter((pl.col("side")==side) & pl.col("v0").is_not_null())
                       .join(rows.select("hash","ply"),on=["hash","ply"],how="semi"))
            self.samples[side] = {(h,p):(v,se or 0.) for h,p,v,se in
                                  samples.select("hash","ply","v0","se").iter_rows()}
        self.prior = 1-book["slice_prior"]
        self.values = {}
        self.unsupported = set()
        self.counts = Counter()

    def get(self, boards, ply):
        side = "x" if ply%2 else "y"
        fresh = [h for h in boards if (ply,h) not in self.values]
        if not fresh:
            return {h:self.values[ply,h] for h in boards}
        if not 0<ply<30:
            raise ValueError("Slice requires interior continuation positions")
        hashes,proxy,exact = self.indices[side]
        keys = np.array(fresh,dtype=np.int64)
        idx = np.minimum(np.searchsorted(hashes,keys),len(hashes)-1)
        raw,terminal = {},{}
        for h,j in zip(fresh,idx):
            if hashes[j]==h:
                if np.isfinite(proxy[j]):
                    raw[h] = float(proxy[j])
                if np.isfinite(exact[j]):
                    terminal[h] = float(exact[j])
        self.counts["saved_proxy"] += len(raw)
        for h in fresh:
            if h in terminal:
                continue
            end = chess.Board(boards[h]+" 0 1").outcome()
            if end:
                terminal[h] = .5 if end.winner is None else float(end.winner==chess.BLACK)
        missing = set(fresh)-raw.keys()-terminal.keys()
        if missing:
            w,r,_ = weak_stop_values(self.data.db, missing if side=="x" else (),
                                      missing if side=="y" else (), pool=self.recipe["stats"],
                                      weak_pool=self.recipe["weak_pool"],evals=self.source,
                                      min_weak=self.recipe["weak_min_games"],
                                      min_share=self.recipe["weak_min_share"],log=lambda *_:None)
            computed = w if side=="x" else r
            raw.update((h,computed[h]) for h in missing if h in computed)
            self.counts["computed_proxy"] += len(missing.intersection(computed))
        m = self.model[side]
        estimated = predict(m,np.full(len(raw),ply),np.array(list(raw.values())))
        for h,v in zip(raw,estimated):
            sample = self.samples[side].get((h,ply))
            if sample is not None:
                mean,se = sample
                v += m["tau2"]/(m["tau2"]+se*se)*(mean-v)
                self.counts["direct_sample"] += 1
            self.values[ply,h] = float(np.clip(v,0.,1.))
        self.values.update({(ply,h):v for h,v in terminal.items()})
        self.counts["terminal"] += len(terminal)
        for h in missing-raw.keys():
            # No weak proxy or static evaluation. Explicit population-prior exit.
            self.values[ply,h] = self.prior
            self.unsupported.add((ply,h))
            self.counts["no_proxy_prior"] += 1
        return {h:self.values[ply,h] for h in boards}
