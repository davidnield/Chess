"""One-factor tuning builds, paired low-rollout screening and calibrated finalists.

The human evaluator stays fixed across construction pools and gates. All stages
write to the tuning directory and may resume. Historical canonical books are read-only.
"""
import argparse
from contextlib import contextmanager
import json
import msvcrt
from pathlib import Path
import shutil

import polars as pl
import nomination_trial as nt
import run_practical_rebuild as r

ROOT=nt.TRIAL/"tuning"
OLD_CACHE=Path("E:/chess/calibration/convertibility_sf_cache_sampled.parquet")


@contextmanager
def workspace():
    ROOT.mkdir(parents=True,exist_ok=True)
    with (ROOT/"screen.lock").open("a+b") as lock:
        lock.seek(0)
        msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
        old=r.OUT,r.STATUS_FILE
        r.OUT,r.STATUS_FILE=ROOT,ROOT/"status.json"
        (ROOT/"logs").mkdir(exist_ok=True)
        try:
            yield
        finally:
            r.OUT,r.STATUS_FILE=old


def variants(colour):
    s1800=Path(r.POOL["white"])
    s2000=Path(r.POOL["black"])
    p50=nt.TRIAL/"ge2000-floor50"
    def pair(stats):
        return stats,stats.with_name(stats.name.replace("position_stats_pooled","position_stats_aux_pooled"))
    native=pair(s2000 if colour=="black" else s1800)
    options={"base":(*native,.10,.10),"absolute-tight":(*native,.06,.10),
             "absolute-loose":(*native,.14,.10),"relative-tight":(*native,.10,.06),
             "relative-loose":(*native,.10,.14),"2000-floor50":(p50/"stats.parquet",p50/"aux.parquet",.10,.10),
             "2000-floor5":(*pair(Path(str(s2000).replace("mg20","mg5"))),.10,.10)}
    if colour=="black":
        options["1800-floor50"]=(*pair(s1800),.10,.10)
    else:
        options["2000-floor20"]=(*pair(s2000),.10,.10)
    return options


def args_for(colour,settings):
    stats,aux,absolute,relative=settings
    args=r.recipe(colour)
    for flag,v in [("--stats",stats),("--aux-stats",aux),("--mem","8GB"),("--threads","2")]:
        args[args.index(flag)+1]=str(v)
    return [*args,"--robustness-floor",absolute,"--gate-rel-floor",relative]


def book_for(colour,name,settings,calibration=None):
    base=args_for(colour,settings)
    tag=name if calibration is None else f"{name}-k{calibration}"
    work=ROOT/colour/tag
    work.mkdir(parents=True,exist_ok=True)
    label=f"tune-{colour}-{tag}"
    args=list(base)
    if calibration is not None:
        for kind in ("stop","other"):
            dump=work/f"{kind}-positions.parquet"
            r.stage(label+"-dump-"+kind,r.cmd(nt.HERE/"build_budget_books.py",*base,"--out-prefix",work/"dump",
                    "--dump-"+kind+"-positions",dump),nt.files(dump))
            table=work/f"{kind}-table.parquet"
            opts=["--top",12000,"--chunk",2000,"--pool",settings[0]] if kind=="stop" else ["--nodes",2000]
            outputs=nt.files(table)+([Path(str(table)+".boundary.parquet")] if kind=="stop" else [])
            r.stage(label+"-sample-"+kind,r.cmd(r.SELECTION/f"sampled_{kind}.py","--dump",dump,"--out",table,
                    "--colour",colour,"--k",calibration,"--games",32,"--max-ply",30,"--sf-procs",3,
                    "--our-moves",r.HX/"movers1000_{colour}_f64_2026_08_09","--opp-moves",
                    r.HX/"movers1800_{colour}_f64_2026_08_09","--sf-cache",ROOT/"sf.parquet",*opts),outputs)
            args+=["--sampled-"+kind,table]
    prefix=work/"book"
    for attempt in ("first","exact"):
        book=prefix.with_name(prefix.name+"_b2000.parquet")
        r.stage(label+"-"+attempt,r.cmd(nt.HERE/"build_budget_books.py",*args,"--force","--out-prefix",prefix),nt.files(book))
        f=pl.read_parquet(book,columns=["position_hash","side_to_move","best_move"])
        f=f.filter((pl.col("side_to_move")==colour)&pl.col("best_move").is_not_null())
        if f.height==f["position_hash"].n_unique()==2000:
            return book
        args+=["--polish"]
        prefix=work/"exact"
    raise ValueError(f"Cannot compare unequal footprint: {book}")


def score(colour,label,books,seeds,rollouts):
    path=ROOT/colour/(label+".parquet")
    other="white" if colour=="black" else "black"
    args=["--compare-to","CANON","--our-moves",r.HX/f"movers1000_{colour}_f64_2026_08_09",
          "--opp-moves",r.HX/f"movers1800_{other}_f64_2026_08_09","--max-ply",30,"--seed",seeds,
          "--rollouts",rollouts,"--tau",".0001","--k",100,"--sf-procs",2,"--sf-cache",ROOT/"sf.parquet","--out",path]
    for k,p in books.items():
        args+=["--book",f"{k}={colour}={p}"]
    r.stage(f"{colour}-{label}",r.cmd(r.DEPTH/"convertibility_eval.py",*args),[path])
    return pl.read_parquet(path)


def screen(only=None):
    with workspace():
        code={name:r.digest(nt.HERE/name) for name in ("build_budget_books.py","budget_core.py","screen_repertoires.py",
                                                       "validate_book.py","rebucket_pool.py")}
        design={"code":code,"gate_values":[.06,.10,.14],"paired_screen_seeds":[1,2],"rollouts":1,
                "fixed_referee_k":100,"pool_source":"fixed source repertoire within color",
                "ranking":"mean uniform expected score; tie by name","screen_stop_model":"weak unsampled"}
        plan=ROOT/"design.json"
        if plan.exists() and json.loads(plan.read_text())!=design:
            raise RuntimeError("Tuning design changed; use a versioned output")
        r.publish(plan,design)
        if only:
            colour,name=only
            return book_for(colour,name,variants(colour)[name])
        for src,dst in zip(nt.files(OLD_CACHE),nt.files(ROOT/"sf.parquet")):
            if not dst.exists():
                tmp=dst.with_suffix(".copying")
                shutil.copy2(src,tmp)
                tmp.replace(dst)
        results=pl.DataFrame(json.loads((nt.TRIAL/"maia-development/results.json").read_text()))
        ks=results.group_by("k").agg(pl.col("position_mean_logloss").mean().alias("loss")).sort("loss","k")
        shortlist=sorted({100,ks["k"][0]})
        for colour in ("black","white"):
            grid=variants(colour)
            books={"CANON":r.incumbent(colour)}
            for name,settings in grid.items():
                books[name]=book_for(colour,name,settings)
            scores=score(colour,"development-screen",books,"1,2",1)
            order=scores.filter(pl.col("label")!="CANON").group_by("label").agg(pl.col("uniform").mean()).sort(
                ["uniform","label"],descending=[True,False])
            best=order["label"][0]
            finalists={"CANON":books["CANON"]}
            for name in sorted({best,"base"}):
                for k in shortlist:
                    finalists[f"{name}-k{k}"]=book_for(colour,name,grid[name],k)
            final=score(colour,"development-finalists",finalists,"1,2,3,4",2)
            r.publish(ROOT/colour/"results.json",{"screen":order.to_dicts(),"maia_k":shortlist,
                      "paired_finalist_rows":final.select("label","seed","uniform").to_dicts()})


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--one",nargs=2,metavar=("COLOUR","VARIANT"))
    a=p.parse_args()
    screen(a.one)
