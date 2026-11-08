"""Raise an existing pool's edge floor while moving the dropped results into its aux table."""
import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa

from eval_arrays import MISSING, lookup_evals, open_eval_arrays
from run_practical_rebuild import digest, publish
from stage3_backwards_induction import LICHESS_CP_SCALE


def write_metadata(out,design,meta,ameta):
    kept,side=out/"stats.parquet",out/"aux.parquet"
    for path,info in ((kept,meta),(side,ameta)):
        # Population and evaluator identity carry over; origin counts, times,
        # gate verdicts, paths and source floor describe the old files only.
        facts={k:info[k] for k in ("population","max_ply","term_reasons","event","elo_band",
                "book_meta_sha256","collisions_sha256","eval_arrays_fingerprint","role") if k in info}
        publish(Path(str(path)+".meta.json"),{**facts,"producer":"rebucket_pool","min_games":design["floor"],
                "file":str(path.resolve()),"pool_file":str(kept.resolve()),"rebucketed_from":design,
                "conserved":["total","white_wins","draws","black_wins"]})
    publish(out/"complete.json",{"design":design,"outputs":{p.name:digest(p) for p in
            (kept,side,Path(str(kept)+".meta.json"),Path(str(side)+".meta.json"))}})


def build(stats,aux,floor,out):
    out.mkdir(parents=True,exist_ok=True)
    meta = json.loads(Path(str(stats)+".meta.json").read_text())
    ameta = json.loads(Path(str(aux)+".meta.json").read_text())
    if floor <= meta["min_games"] or ameta["min_games"] != meta["min_games"] or meta["population"] != ameta["population"]:
        raise ValueError("Require a matching stats/aux pair and a higher floor")
    design = {"stats":digest(stats),"aux":digest(aux),"floor":floor,"script":digest(__file__)}
    done = out/"complete.json"
    if done.exists():
        saved=json.loads(done.read_text())
        if any(digest(out/p)!=v for p,v in saved["outputs"].items()):
            raise RuntimeError("Rebucketed pool changed")
        if saved["design"]==design:
            return
        old={**design,"script":"72a0efa128e350015eeef8b44e8bf7f1cd14eafa2847b6c2d4ce5cf2eb1f46ec"}
        if saved["design"]==old:
            write_metadata(out,design,meta,ameta)
            return
        raise RuntimeError("Rebucketing contract changed")
    mm=open_eval_arrays()
    def evaluate(keys):
        valid=np.array(keys.is_valid())
        vals=lookup_evals(np.array(keys.fill_null(0),dtype=np.int64),*mm)
        ws=1/(1+np.exp(-LICHESS_CP_SCALE*vals.astype(float)))
        return pa.array(ws,mask=(~valid)|(vals==MISSING))
    kept=out/"stats.parquet"
    side=out/"aux.parquet"
    with duckdb.connect() as con:
        con.execute("SET threads=2")
        con.execute("SET memory_limit='5GB'")
        con.execute("SET temp_directory=?",[str(out/"spill")])
        con.create_function("pos_eval",evaluate,["BIGINT"],"DOUBLE",type="arrow",null_handling="special")
        con.read_parquet(str(stats)).create_view("edges")
        con.read_parquet(str(aux)).create_view("aux")
        con.sql(f"SELECT * FROM edges WHERE total>={floor}").write_parquet(str(kept),compression="zstd")
        con.read_parquet(str(kept)).create_view("kept")
        con.execute("""CREATE TEMP TABLE removed AS WITH e AS MATERIALIZED
            (SELECT *,pos_eval(child_hash) ev FROM edges WHERE total<?)
            SELECT parent_hash h,sum(total)::BIGINT n,sum(white_wins)::BIGINT w,
            sum(draws)::BIGINT d,sum(black_wins)::BIGINT b,count(*)::INTEGER cnt,
            sum(CASE WHEN ev IS NOT NULL THEN total ELSE 0 END)::BIGINT cov,
            sum(total*ev) weighted,min(ev) lo,max(ev) hi FROM e GROUP BY 1""",[floor])
        con.sql("""WITH x AS (SELECT a.*, r.*,
            round(a.other_total*coalesce(a.other_eval_cov,0))::BIGINT old_cov
            FROM aux a SEMI JOIN kept k ON a.position_hash=k.parent_hash
            LEFT JOIN removed r ON a.position_hash=r.h)
            SELECT * EXCLUDE (h,n,w,d,b,cnt,cov,weighted,lo,hi,old_cov) REPLACE (
            other_total+coalesce(n,0) AS other_total, other_white_wins+coalesce(w,0) AS other_white_wins,
            other_draws+coalesce(d,0) AS other_draws,other_black_wins+coalesce(b,0) AS other_black_wins,
            other_edges+coalesce(cnt,0) AS other_edges,
            (coalesce(other_eval_mean,0)*old_cov+coalesce(weighted,0))/nullif(old_cov+coalesce(cov,0),0) AS other_eval_mean,
            least(other_eval_min,lo) AS other_eval_min,greatest(other_eval_max,hi) AS other_eval_max,
            coalesce((old_cov+coalesce(cov,0))/nullif(other_total+coalesce(n,0),0),0) AS other_eval_cov)
            FROM x ORDER BY position_hash""").write_parquet(str(side),compression="zstd")
        con.read_parquet(str(side)).create_view("new_aux")
        for col in ("total","white_wins","draws","black_wins"):
            mismatches=con.sql(f"""WITH old AS (SELECT parent_hash,sum({col}) v FROM edges GROUP BY 1),
                new AS (SELECT parent_hash,sum({col}) v FROM kept GROUP BY 1)
                SELECT count(*) FROM new JOIN old USING(parent_hash)
                LEFT JOIN aux a ON new.parent_hash=a.position_hash
                LEFT JOIN new_aux b ON new.parent_hash=b.position_hash
                WHERE b.position_hash IS NULL OR a.position_hash IS NULL
                OR old.v+a.other_{col}<>new.v+b.other_{col}""").fetchone()[0]
            if mismatches:
                raise ValueError(f"{col} conservation failed at {mismatches} parents")
    write_metadata(out,design,meta,ameta)


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    for k in ("stats","aux","out"):
        parser.add_argument("--"+k,type=Path,required=True)
    parser.add_argument("--floor",type=int,required=True)
    build(**vars(parser.parse_args()))
