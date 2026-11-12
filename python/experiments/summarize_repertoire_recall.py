"""Summarize practice scenarios and paired ply-30 pilot blocks."""
import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import t


def learning(directory):
    schedules = pl.read_parquet(directory / "schedules.parquet")
    (schedules.group_by("label", "start", "curve", "day")
     .agg(pl.col("learned").mean(), pl.col("recalled").mean(),
          pl.col("recalled").std().alias("schedule_sd"))
     .sort("label", "start", "curve", "day").write_csv(directory / "study-summary.csv"))
    strata = []
    for arm in json.loads((directory / "design.json").read_text())["arms"]:
        label = arm["label"]
        cards = pl.read_parquet(directory / f"{label}-cards.parquet")
        profiles = pl.read_parquet(list((directory / label / "scratch/central").glob("*.parquet")))
        profiles = profiles.group_by("position_hash").agg(pl.col("day90").mean())
        cards = cards.join(profiles, on="position_hash", validate="1:1")
        for group, expr in {
            "first_ply": pl.when(pl.col("first_ply") < 0).then(pl.lit("unreached"))
             .when(pl.col("first_ply") < 8).then(pl.lit("0-7"))
             .when(pl.col("first_ply") < 16).then(pl.lit("8-15")).otherwise(pl.lit("16-29")),
            "reference_visits_per_game": pl.when(pl.col("reach") >= .01).then(pl.lit(">=0.01"))
             .when(pl.col("reach") >= .001).then(pl.lit("0.001 to <0.01")).otherwise(pl.lit("<0.001"))
        }.items():
            strata.append(cards.with_columns(expr.alias("band")).group_by("band").agg(
                pl.len().alias("cards"),pl.col("reach").sum().alias("visits_per_game"),
                (pl.col("reach")*pl.col("day90")).sum().alias("remembered_visits_per_game"))
                .with_columns(pl.lit(label).alias("label"),pl.lit(group).alias("group")))
    (pl.concat(strata).with_columns(pl.when(pl.col("visits_per_game") > 0)
        .then(pl.col("remembered_visits_per_game")/pl.col("visits_per_game")).alias("recall"))
     .sort("label", "group", "band").write_csv(directory / "day90-by-depth-and-frequency.csv"))


def paired(scores):
    paths = sorted(scores.glob("seed-*.parquet"))
    if not paths:
        return
    data = pl.read_parquet(paths)
    data.group_by("label","model").agg(pl.col("score").mean(),pl.col("recalled").mean(),
        pl.col("unaided_book_visits").mean(),pl.col("found").mean(),pl.len().alias("games")
        ).sort("label","model").write_csv(scores/"scores.csv")
    blocks = data.group_by("label","model","seed").agg(pl.col("score").mean())
    cells = {(label,model):dict(zip(f["seed"],f["score"]))
             for (label,model),f in blocks.group_by("label","model")}
    rows = []
    def compare(a,b):
        if cells[a].keys() != cells[b].keys():
            raise ValueError(f"Incomplete paired blocks for {a} vs {b}")
        x = np.array([cells[a][s]-cells[b][s] for s in sorted(cells[a])])
        half = t.ppf(.975,len(x)-1)*x.std(ddof=1)/np.sqrt(len(x)) if len(x)>1 else np.nan
        rows.append(dict(label=a[0],model=a[1],baseline=b[0],baseline_model=b[1],
            blocks=len(x),delta=x.mean(),lower=x.mean()-half,upper=x.mean()+half))
    for label,model in sorted(cells):
        if model != "perfect" and (label,"perfect") in cells:
            compare((label,model),(label,"perfect"))
        if model.startswith("switch-"):
            _, curve, day = model.split("-")
            baseline = f"scratch-{curve}-{day}" if day == "0" else f"switch-{curve}-0"
            if (label,baseline) in cells:
                compare((label,model),(label,baseline))
        colour,gate = label.split("-")
        if gate != "30" and (f"{colour}-30",model) in cells:
            compare((label,model),(f"{colour}-30",model))
    if rows:
        pl.DataFrame(rows).write_csv(scores / "paired.csv")


if __name__ == "__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("profiles",type=Path)
    p.add_argument("--scores",type=Path)
    a=p.parse_args()
    learning(a.profiles)
    if a.scores:
        paired(a.scores)
