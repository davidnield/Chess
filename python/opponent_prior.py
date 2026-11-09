"""Attach a frozen, support-limited opponent model to a budget-builder frontier."""
import hashlib
import json
import math
from pathlib import Path

import polars as pl


class ReplyOverlay:
    def __init__(self, path, weight, pool, aux_path, colour, floor):
        path = Path(path)
        meta = json.loads(Path(str(path)+".json").read_text())
        with path.open("rb") as fp:
            sha = hashlib.file_digest(fp,"sha256").hexdigest()
        if (sha != meta["sha256"] or meta["schema"] != 1 or meta["colour"] != colour
                or Path(meta["source_pool"]).resolve() != Path(pool).resolve()
                or not aux_path or Path(aux_path).resolve() != Path(meta["source_aux"]).resolve()
                or floor != meta["min_games"] or not math.isfinite(weight) or weight < 0):
            raise ValueError("Opponent prior does not match these pool, perspective, or weight settings")
        self.recipe = {"file":str(path.resolve()), "sha256":sha, "K":weight, "scope":meta["scope"]}
        self.rows = pl.read_parquet(path).with_columns(
            (pl.col("reply_observed") + weight*pl.col("maia_p")).alias("reply_mass")
        ).with_columns((pl.col("reply_mass") /
                        (pl.col("reply_mass").sum().over("parent_hash")+pl.col("nonmove")))
                       .alias("reach_share"))
        self.keys = self.rows.select("parent_hash").unique()

    def attach(self, frame):
        # Replace only parents actually reached by this frontier, and only
        # within the frozen support. Our candidate rows keep their true counts.
        cols = frame.columns
        keys = frame.select("parent_hash").unique()
        named = self.rows.join(keys,on="parent_hash",how="semi").filter(pl.col("reply_mass")>0)
        untouched = frame.join(self.keys,on="parent_hash",how="anti").with_columns(
            pl.lit(None,dtype=pl.Float64).alias("reply_mass"),
            pl.lit(None,dtype=pl.Float64).alias("reach_share"))
        return pl.concat([untouched,named.select(*cols,"reply_mass","reach_share")],how="vertical_relaxed")
