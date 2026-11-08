"""Score shrinkage against actual moves in disjoint development game folds."""
import argparse
import json
import math
import os
from pathlib import Path

import duckdb
import polars as pl

import convertibility_eval as cv
import run_practical_rebuild as r

KS = (10, 30, 100, 300, 1000)


def grade(counts, held, prior, k):
    # Rescale 48 training folds to the deployment table size. No validation moves enter the fit.
    train = {s:n*64/48 for s,n in counts.items()}
    dense = cv.blend(train,prior,k,0.)
    deployed = cv.blend(train,prior,k,.005)
    n = sum(held.values())
    loss = -sum(v*math.log(max(dense.get(s,0.),1e-12)) for s,v in held.items())
    pruned = sum(v for s,v in held.items() if s not in deployed)
    return n,loss,pruned


def run(out, limit):
    out.mkdir(parents=True,exist_ok=True)
    books = {c:r.incumbent(c) for c in ("white","black")}
    plan = {"books":{c:r.digest(p) for c,p in books.items()}, "limit":limit,"ks":list(KS),
            "train_folds":"0..47", "validation_folds":"48..63","script":r.digest(__file__),
            "identity":cv.identity(),"cpu_only":os.environ.get("CUDA_VISIBLE_DEVICES")=="-1"}
    rows = []
    for repertoire,book in books.items():
        data = pl.read_parquet(book).select("position_hash","position_epd","side_to_move").drop_nulls().unique()
        for side in ("white","black"):
            weak = repertoire==side
            band, elo, opp = ("1000",1450,2000) if weak else ("1800",2000,1450)
            root = r.HX/f"movers{band}_{side}_f64_2026_08_09"
            params = cv.table_params(str(root))
            if not params or params["folds"]!=64 or params["our_colour"]!=side:
                raise ValueError(f"Fold/cohort mismatch: {root}")
            group = f"{repertoire}-{side}"
            config = {**plan,"table":cv.table_manifest(str(root)),"params":params,"elo":[elo,opp]}
            path = out/f"{group}.json"
            if path.exists():
                saved = json.loads(path.read_text())
                if saved["config"] != json.loads(json.dumps(config,default=str)):
                    raise ValueError("Shrinkage screen inputs changed")
            else:
                boards = data.filter(pl.col("side_to_move")==side).sort("position_hash")
                boards = boards.sample(n=min(limit,boards.height),seed=42).sort("position_hash")
                keys = dict(boards.select("position_hash","position_epd").iter_rows())
                with duckdb.connect() as con:
                    con.execute("SET threads=2")
                    con.register("boards",boards.to_arrow())
                    values = con.execute("""SELECT parent_hash,move_san,
                        SUM(CASE WHEN fold<48 THEN total ELSE 0 END),
                        SUM(CASE WHEN fold>=48 THEN total ELSE 0 END) FROM read_parquet(?)
                        SEMI JOIN boards ON parent_hash=position_hash GROUP BY 1,2""",
                        [str(root/"month=*/bkt=*/*.parquet")]).fetchall()
                train, held = {}, {}
                for h,s,n,v in values:
                    s=s.rstrip("+#")
                    for dest,count in ((train,n),(held,v)):
                        d=dest.setdefault(h,{})
                        d[s]=d.get(s,0)+int(count)
                ids=[h for h in keys if sum(held.get(h,{}).values())]
                maia=cv.Maia()
                try:
                    priors=maia.probs([keys[h] for h in ids],elo,opp,ids)
                finally:
                    maia.close()
                saved={"config":config,"positions":[{"h":h,"train":train[h],"held":held[h],"prior":p}
                                                    for h,p in zip(ids,priors)]}
                r.publish(path,saved)
            for k in KS:
                scores=[grade(s["train"],s["held"],s["prior"],k) for s in saved["positions"]]
                n=sum(x[0] for x in scores)
                if not n:
                    raise ValueError("Empty validation sample")
                rows.append(dict(repertoire=repertoire,side=side,k=k,positions=len(scores),moves=n,
                                 logloss=sum(x[1] for x in scores)/n,
                                 position_mean_logloss=sum(x[1]/x[0] for x in scores)/len(scores),
                                 pruned_move_mass=sum(x[2] for x in scores)/n))
            print(f"{group}: {len(saved['positions'])} positions",flush=True)
    r.publish(out/"results.json",rows)


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out",type=Path,required=True)
    parser.add_argument("--positions-per-group",type=int,default=1200)
    a=parser.parse_args()
    run(a.out,a.positions_per_group)
