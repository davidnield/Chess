"""Batch independent searches across frozen provisional books.

Plans are read-only. A pass unions the complete comparison requests at each full
position key, uses the existing restricted-search caches, and publishes one new
overlay. Every book must be rebuilt/rechecked on that same overlay before closure.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import chess
import polars as pl
import rare_verify as rv
import cloud_feedback as fb
from transposition_search import resolve
from consumer_keys import PAIR, checked_board, stored_key
from .stages import atomic_json,file_sha
from .spec import digest

def combine(plans):
    grouped={}
    for plan in plans:
        policy=plan["policy"]
        if policy["limit"] is None: raise ValueError("joint verification requires score gates")
        for row in plan["requests"]:
            pair=(row["key"]["lo"],row["key"]["hi"])
            b=checked_board(row["epd"],row["key"])
            if b.turn!=(plan["colour"]=="white"): raise ValueError("request side differs")
            epd=" ".join(b.fen(en_passant="fen").split()[:4])
            g=grouped.setdefault(pair,dict(key=row["key"],epd=epd,moves=set(),done=set(),
                    menu={},previous_nodes=0,policies=[],books=set(),reasons=set()))
            if epd!=g["epd"]: raise ValueError("full-key request has a different board")
            for item in row["menu"]:
                san=item["san"]
                child=b.copy(stack=False);child.push_san(san)
                if item["child"]!=stored_key(child,True):
                    raise ValueError("comparison menu child does not match legal replay")
                old=g["menu"].get(san)
                if old and old!=item:
                    raise ValueError("books use different candidate counts/evidence at a shared parent")
                g["menu"][san]=item
            g["moves"].update(row["moves"]);g["done"].update(row["done"])
            g["previous_nodes"]=max(g["previous_nodes"],row["previous_nodes"])
            if policy not in g["policies"]: g["policies"].append(policy)
            g["books"].add(plan["book_sha256"]);g["reasons"].add(row["reason"])
    return [dict(g,moves=sorted(g["moves"]),done=sorted(g["done"]),
                 books=sorted(g["books"]),reasons=sorted(g["reasons"]))
            for _,g in sorted(grouped.items())]

def verify(plans, *, cache, out, prior=None, nodes=5_000_000, deep_nodes=20_000_000,
           threads=2, searches_factory=rv.Searches, resolver=resolve):
    out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
    prior_sha=file_sha(prior) if prior else None
    if any(p["overlay_sha256"]!=prior_sha for p in plans):
        raise ValueError("all books must use the same frozen verification overlay")
    groups=combine(plans)
    identity=digest(dict(plans=plans,prior=prior_sha,nodes=nodes,deep_nodes=deep_nodes,threads=threads))
    plan_path=Path(str(out)+".requests.json")
    if plan_path.exists() and json.loads(plan_path.read_text(encoding="utf8"))["identity"]!=identity:
        raise ValueError("verification pass already has a different request set")
    atomic_json(plan_path,dict(identity=identity,groups=groups))
    if prior and Path(prior).resolve()==out.resolve():
        raise ValueError("verification output must not overwrite frozen evidence")
    schema={**fb.SCHEMA,"position_hash":PAIR,"child_hash":PAIR}
    previous=pl.read_parquet(prior) if prior else pl.DataFrame(schema=schema)
    old_parents={}
    for row in previous.to_dicts():
        if row.get("verification_nodes"):
            k=(row["position_hash"]["lo"],row["position_hash"]["hi"])
            old_parents.setdefault(k,[]).append(row)
    for g in groups:
        for old in old_parents.get((g["key"]["lo"],g["key"]["hi"]),[]):
            san=rv.bare(old["move_san"])
            g["previous_nodes"]=max(g["previous_nodes"],old["verification_nodes"])
            if san not in g["done"]: g["done"].append(san)
            g["menu"].setdefault(san,dict(san=san,child=old["child_hash"],total=0,stored_cp=old["child_cp_white"]))
    base=searches_factory(Path(cache),nodes,threads,wide=True)
    if previous.height:
        checked=previous.filter(pl.col("verification_nodes").is_not_null())
        if checked.height and (checked["verification_engine_sha256"].null_count()
                 or checked.filter(pl.col("verification_engine_sha256")!=base.contract["engine"]).height):
            base.close()
            raise ValueError("independent evidence uses another engine")

    deeper={}
    records=[];report=[]
    try:
        for g in groups:
            h=g["key"];epd=g["epd"]
            board=checked_board(epd,h);sign=1 if board.turn else -1
            # comparison_set needs scalar local child IDs, never persisted as position keys.
            menu={san:dict(move_san=san,child_hash=i,total=e["total"]) for i,(san,e) in enumerate(sorted(g["menu"].items()))}
            stored={menu[san]["child_hash"]:e["stored_cp"] for san,e in g["menu"].items() if e["stored_cp"] is not None}
            use=set(g["moves"])|set(g["done"])
            first=[True]
            def search(moves,unused):
                saved=base.get(h,epd,moves,count=first[0]);first[0]=False;return saved
            while True:
                before=set(use)
                for policy in g["policies"]:
                    extra,saved=rv.comparison_set(search,use,g["done"],menu,stored,0,
                                                  policy["limit"],sign,score_units=True)
                    use.update(extra)
                if before==use: break
            # Ensure cached responses from an unrelated arm cannot quietly enlarge the reference.
            saved=base.get(h,epd,sorted(use),count=False)
            boundary=any(rv.boundary(saved,p["limit"],moves=use,score_units=True,
                                    score_floor=p["floor"],score_band=p["band"]) for p in g["policies"])
            budget=max(nodes,g["previous_nodes"],deep_nodes if boundary else 0)
            deep=None
            if budget>nodes:
                worker=deeper.setdefault(budget,None)
                if worker is None:
                    worker=deeper[budget]=searches_factory(Path(cache)/f"deep-{budget}",budget,threads,free=False,wide=True)
                deep=worker.get(h,epd,sorted(use))
            external={san:dict(move_san=san,child_hash=e["child"],total=e["total"]) for san,e in g["menu"].items()}
            nominated=[]
            for san in sorted(use-set(external)):
                child=board.copy(stack=False);move=child.parse_san(san)
                full_san=child.san(move);child.push(move)
                external[san]=dict(move_san=full_san,child_hash=stored_key(child,True),total=0)
                nominated.append(full_san)
            p=g["policies"][0]
            new=list(rv.rows_for(h,saved,external,p["limit"],nodes,base.contract["engine"],
                         "; ".join(g["reasons"]),deep,budget if deep else None,use,True,p["floor"]))
            # 'unstable' means any participating gate changes, not merely the first arm.
            for row in new:
                if deep:
                    base_best=max(sign*saved["moves"][m]["cp"] for m in use)
                    deep_best=max(sign*deep["moves"][m]["cp"] for m in use)
                    san=rv.bare(row["move_san"])
                    row["verification_unstable"]=any(
                        rv.bc.fails_engine_gate(base_best,sign*saved["moves"][san]["cp"],q["limit"],True,q["floor"])
                        !=rv.bc.fails_engine_gate(deep_best,sign*deep["moves"][san]["cp"],q["limit"],True,q["floor"])
                        for q in g["policies"])
            records.extend(new)
            report.append(dict(key=h,moves=sorted(use),nodes=budget,boundary=boundary,books=g["books"],
                               engine_nominations=nominated))
    finally:
        base.close()
        for worker in deeper.values(): worker.close()
    if records:
        update=pl.DataFrame(records,schema=schema)
        # Preserve any independently verified stronger incoming evidence, including on an
        # unrequested move at a shared parent. Such rows force the caller to replan at that tier.
        paired=update.select("position_hash","move_san","verification_nodes").join(
            previous.select("position_hash","move_san","verification_nodes"),on=["position_hash","move_san"],
            how="inner",suffix="_old")
        if paired.filter(pl.col("verification_nodes_old")>pl.col("verification_nodes")).height:
            raise ValueError("verification pass would downgrade evidence")
        keep=previous.join(update.select("position_hash","move_san"),on=["position_hash","move_san"],how="anti")
        roots=update.select("position_hash","parent_cp_white").unique().rename({"parent_cp_white":"_new_root"})
        keep=keep.join(roots,on="position_hash",how="left").with_columns(
             pl.coalesce("_new_root","parent_cp_white").alias("parent_cp_white")).drop("_new_root")
        result=pl.concat([keep,update],how="diagonal_relaxed").sort("position_hash","move_san")
    else:
        result=previous
    # Re-searching every incoming edge must not erase a prior direct-child result.
    if "transposition_cp_white" in previous.columns:
        direct=previous.select("child_hash","transposition_cp_white").drop_nulls().unique()
        if direct["child_hash"].n_unique()!=direct.height:
            raise ValueError("conflicting prior direct-child evidence")
        result=result.drop("transposition_cp_white",strict=False).join(direct,on="child_hash",how="left")
    transpositions=None
    if groups:
        result,transpositions=resolver(result,Path(cache)/"transpositions")
    tmp=Path(str(out)+".tmp");result.write_parquet(tmp,compression="zstd");tmp.replace(out)
    info=dict(identity=identity,requested_parents=len(groups),overlay_sha256=file_sha(out),
              input_overlay_sha256=prior_sha,searches=base.stats,
              deeper={str(n):worker.stats for n,worker in deeper.items()},
              parents=report,transpositions=transpositions,closed=not groups,
              # Any changed transposed child can affect another book's allocation.
              rebuild_books=[p["book"] for p in plans] if groups else [])
    atomic_json(str(out)+".meta.json",info)
    return info

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--books",type=Path,required=True,help="JSON list of book and colour")
    p.add_argument("--cache",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    p.add_argument("--prior",type=Path)
    p.add_argument("--threads",type=int,default=2)
    p.add_argument("--plan-only",action="store_true")
    a=p.parse_args()
    plans=[rv.request_plan(r["book"],r["colour"]) for r in json.loads(a.books.read_text(encoding="utf8"))]
    if a.plan_only:
        atomic_json(a.out,dict(plans=plans,groups=combine(plans)))
    else:
        print(json.dumps(verify(plans,cache=a.cache,out=a.out,prior=a.prior,threads=a.threads),indent=2))

if __name__=="__main__": main()
