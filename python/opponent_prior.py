"""Attach a frozen, support-limited opponent model to a budget-builder frontier."""
import hashlib
import json
import math
from pathlib import Path

import polars as pl


class ReplyOverlay:
    def __init__(self, path, weight, pool, aux_path, colour, floor, keys=None):
        path = Path(path)
        meta = json.loads(Path(str(path)+".json").read_text())
        with path.open("rb") as fp:
            sha = hashlib.file_digest(fp,"sha256").hexdigest()
        if (sha != meta["sha256"] or meta["schema"] not in (1, 2) or meta["colour"] != colour
                or Path(meta["source_pool"]).resolve() != Path(pool).resolve()
                or not aux_path or Path(aux_path).resolve() != Path(meta["source_aux"]).resolve()
                or floor != meta["min_games"] or not math.isfinite(weight) or weight < 0):
            raise ValueError("Opponent prior does not match these pool, perspective, or weight settings")
        self.recipe = {"file":str(path.resolve()), "sha256":sha, "K":weight, "scope":meta["scope"]}
        self.rows = pl.read_parquet(path)
        if keys:
            self.rows = keys.encode(self.rows, "parent_hash", "child_hash")
        mass = pl.col("reply_observed") + weight*pl.col("maia_p")
        if meta["schema"] == 2 and weight:
            # The prior predicts which move is played conditional on a move.
            # Keep the observed chance of termination or censoring identical in both arms.
            observed = pl.col("reply_observed").sum().over("parent_hash")
            mass = mass * (observed / (observed + weight))
        self.rows = self.rows.with_columns(mass.alias("reply_mass")).with_columns((pl.col("reply_mass") /
                        (pl.col("reply_mass").sum().over("parent_hash")+pl.col("nonmove")))
                       .alias("reach_share"))
        self.keys = self.rows.select("parent_hash").unique()
        self.extra_aux = {}
        if meta["schema"] == 2:
            extra = Path(str(path) + ".aux.parquet")
            with extra.open("rb") as fp:
                if hashlib.file_digest(fp, "sha256").hexdigest() != meta["extra_aux_sha256"]:
                    raise ValueError("Expanded reply termination data changed")
            frame = pl.read_parquet(extra)
            if keys:
                frame = keys.encode(frame, "position_hash")
            self.extra_aux = {r["position_hash"]: r for r in frame.to_dicts()}

    def extend_aux(self, aux_frame, parents):
        """The aux sidecar's rows plus the expanded parents' real termination mass (the
        prior's .aux.parquet), for the parents in the graph. A parent may not have both."""
        extra = [self.extra_aux[h] for h in parents if h in self.extra_aux]
        if not extra:
            return aux_frame
        ex = pl.DataFrame(extra, infer_schema_length=None)
        dup = ex.join(aux_frame.select("position_hash"), on="position_hash", how="semi")
        if dup.height:
            raise ValueError(f"Duplicate termination data for expanded parent {dup['position_hash'][0]}")
        return pl.concat([aux_frame, ex], how="diagonal_relaxed")

    def attach(self, frame, frontier):
        # Replace only parents actually reached by this frontier, and only
        # within the frozen support. Our candidate rows keep their true counts.
        keys = pl.DataFrame({"parent_hash": list(frontier)}, schema={"parent_hash": pl.Int64})
        named = self.rows.join(keys,on="parent_hash",how="semi").filter(pl.col("reply_mass")>0)
        retained = frame.select("parent_hash","child_hash").unique().with_columns(pl.lit(True).alias("retained"))
        named = named.join(retained,on=["parent_hash","child_hash"],how="left").with_columns(
            pl.col("retained").is_null().alias("additional_reply"))
        untouched = frame.join(self.keys,on="parent_hash",how="anti").with_columns(
            pl.lit(None,dtype=pl.Float64).alias("reply_mass"),
            pl.lit(None,dtype=pl.Float64).alias("reach_share"),
            pl.lit(False).alias("additional_reply"))
        return pl.concat([untouched,named.select(*untouched.columns)],how="vertical_relaxed")
