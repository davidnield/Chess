"""Fresh common-position safety checks and paired evidence for the two nomination methods."""
import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path

import chess
import chess.engine
import duckdb
import numpy as np
import polars as pl

import cloud_feedback as fb
import engine_candidates as ec
import run_practical_rebuild as r
from nomination_trial import BROAD, TRIAL, MIN_REACH, edges_for
from zobrist import zobrist_int64

MARGIN = .002


def policy(book,colour):
    b=pl.read_parquet(book).filter((pl.col("side_to_move")==colour)&pl.col("best_move").is_not_null())
    choices={}
    for h,epd,m in b.select("position_hash","position_epd","best_move").iter_rows():
        board=chess.Board(epd+" 0 1")
        if zobrist_int64(board)!=h:
            raise ValueError("Book EPD mismatch")
        board.push_san(m)
        choices[h]=(epd,m,zobrist_int64(board))
    return choices


def visits(choices,stats,aux,min_games,colour):
    root=zobrist_int64(chess.Board())
    parents={child for _,_,child in choices.values()} | {root}
    opponent=defaultdict(list)
    totals=defaultdict(int)
    for h,_,child,n in edges_for(stats,parents,min_games):
        totals[h]+=n
        if child in choices:
            opponent[h].append((child,n))
    with duckdb.connect() as con:
        con.register("wanted",pl.DataFrame({"h":sorted(parents)},schema={"h":pl.Int64}).to_arrow())
        for h,other in con.execute("""SELECT position_hash,other_total+horizon_total+term_flag_total+
                term_normal_total+term_other_total FROM read_parquet(?) SEMI JOIN wanted ON position_hash=h""",
                [str(aux)]).fetchall():
            totals[h]+=other
    frontier={root:1.}
    result=defaultdict(float)
    for ply in range(30):
        nxt=defaultdict(float)
        our_turn=(ply%2==0)==(colour=="white")
        for h,mass in frontier.items():
            if our_turn and h in choices:
                result[h]+=mass
                nxt[choices[h][2]]+=mass
            elif not our_turn:
                for child,n in opponent.get(h,[]):
                    nxt[child]+=mass*n/totals[h]
        frontier=nxt
    return result


def safety(broad,target,out):
    books={"broad":broad,"targeted":target}
    paths={"broad":BROAD/"black/audit-NEW.rows.parquet","targeted":TRIAL/"black/audit-NEW.rows.parquet"}
    config={"books":{k:r.digest(p) for k,p in books.items()},"audit":{k:r.digest(p) for k,p in paths.items()},
            "engine":r.digest(ec.SF),"nodes":30_000_000,"retry_nodes":60_000_000,"threads":6,
            "script":r.digest(__file__),"limit_cp":30}
    folder=out/"checks"
    folder.mkdir(parents=True,exist_ok=True)
    design=folder/"design.json"
    if design.exists() and json.loads(design.read_text())!=config:
        raise ValueError("Frozen finalist audit inputs changed")
    r.publish(design,config)
    choices={k:policy(p,"black") for k,p in books.items()}
    reach={}
    flags=set()
    for k,p in books.items():
        meta=json.loads(Path(str(p)+".meta.json").read_text())
        reach[k]=visits(choices[k],meta["stats"],meta["aux_stats"],meta["min_games"],"black")
        for row in pl.read_parquet(paths[k]).iter_rows(named=True):
            loss=row.get("sf_loss")
            if loss is None:
                loss=row["cached_loss"]
            if (row["status"] in ("unknown","fallback","outside_set") or row["flagged"]
                    or row.get("unstable") or (loss is not None and abs(loss-30)<=15)):
                flags.add(row["position_hash"])
    all_pos=set(choices["broad"]) | set(choices["targeted"])
    sample=sorted(all_pos,key=lambda h:hashlib.sha256(f"nomination-control-v1/{h}".encode()).digest())
    for h in all_pos:
        if (choices["broad"].get(h)!=choices["targeted"].get(h) and
                max(rh.get(h,0.) for rh in reach.values())>=MIN_REACH):
            flags.add(h)
    flags.update([h for h in sample if h not in flags][:32])
    engine=None
    rows=[]
    try:
        for h in sorted(flags):
            entry={k:d[h] for k,d in choices.items() if h in d}
            epds={row[0] for row in entry.values()}
            if len(epds)!=1:
                raise ValueError(f"Position collision {h}")
            epd=epds.pop()
            path=folder/f"{h}.json"
            if not path.exists():
                if engine is None:
                    engine=chess.engine.SimpleEngine.popen_uci(ec.SF)
                    engine.configure({"Threads":6,"Hash":1024})
                board=chess.Board(epd+" 0 1")
                moves={row[1] for row in entry.values()}
                result=fb.score_moves(engine,board,moves,30_000_000,30)
                if result["unstable"]:
                    result=fb.score_moves(engine,board,moves|set(result["scores"]),60_000_000,30)
                r.publish(path,{"epd":epd,"result":result})
            saved=json.loads(path.read_text())
            if saved["epd"]!=epd:
                raise ValueError("Cached audit EPD changed")
            result=saved["result"]
            best=result["scores"][result["best"]]["cp"]
            for k,(_,move,_) in entry.items():
                rows.append({"arm":k,"hash":h,"move":move,"reach":reach[k].get(h,0.),
                             "loss_cp":result["scores"][move]["cp"]-best, "unstable":result["unstable"]})
    finally:
        if engine:
            engine.quit()
    r.publish(out/"safety.json",{"design":config,"positions":len(flags),"rows":rows})


def summarize(score,boot,safety_report):
    def pairs(frame, keys):
        groups=frame.partition_by(keys,as_dict=True)
        values=[]
        for group in groups.values():
            d=dict(group.select("label","uniform").iter_rows())
            if len(d)!=group.height or not {"TARGET","BROAD"}<=d.keys():
                raise ValueError("Missing or duplicate paired score")
            values.append(d["TARGET"]-d["BROAD"])
        return np.array(values)
    baseline=score.filter(pl.col("replicate")==0)
    seeds=pairs(baseline,"seed")
    folds=pairs(boot.filter(pl.col("replicate")>0),["replicate","seed"])
    if set(baseline["seed"])!={1,2,3,4} or set(boot["replicate"])!=set(range(17)) or set(boot["seed"])!={1}:
        raise ValueError("Incomplete paired design")
    avg=float(seeds.mean())
    # Center the one-seed game-fold perturbations on the four-seed point estimate.
    # Combine conservative separate MC and fold bounds for this development decision.
    fold_center=pairs(boot.filter(pl.col("replicate")==0),"seed")[0]
    mc=2.353*float(seeds.std(ddof=1))/math.sqrt(4)
    lower=avg-mc+min(0.,float(np.quantile(folds-fold_center,.05)))
    upper=avg+mc+max(0.,float(np.quantile(folds-fold_center,.95)))
    rows=safety_report["rows"]
    def problems(arm):
        return {r["hash"] for r in rows if r["arm"]==arm and r["reach"]>=MIN_REACH and
                r["loss_cp"]>30 and not r["unstable"]}
    extra=problems("targeted")-problems("broad")
    same_positions={r["hash"]:r for r in rows if r["arm"]=="broad"}
    extra.update(r["hash"] for r in rows if r["arm"]=="targeted" and r["reach"]>=MIN_REACH
                 and not r["unstable"] and r["loss_cp"]>30 and r["hash"] in same_positions
                 and r["loss_cp"]-same_positions[r["hash"]]["loss_cp"]>15)
    unstable={r["hash"] for r in rows if r["unstable"] and r["reach"]>=MIN_REACH}
    method="targeted" if lower>=-MARGIN and not extra and not unstable else "broad" if upper< -MARGIN or extra else "inconclusive"
    return {"method":method,"target_minus_broad":avg,"seed_differences":seeds.tolist(),
            "development_lower":lower,"development_upper":upper,"margin":MARGIN,
            "additional_common_risk_positions":sorted(extra),"unstable_common_positions":sorted(unstable),
            "fold_replicates":len(folds)}


if __name__=="__main__":
    p=argparse.ArgumentParser()
    for k in ("broad","target","out"):
        p.add_argument("--"+k,type=Path,required=True)
    safety(**vars(p.parse_args()))
