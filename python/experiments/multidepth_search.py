"""Run a multi-depth Black study-budget comparison twice using persistent inputs."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time

from multidepth_allocation import Allocator
from multidepth_graph import build
from multidepth_panel import Panel
from multidepth_replay import replay_inputs
from repertoire_input_cache import Cache, Inputs
from root_pilot_data import Maia, successors
from root_search_pilot import KS
from run_practical_rebuild import publish


def run_one(a,kind,run,db):
    start = time.perf_counter()
    cache = Cache(db)
    try:
        inputs = Inputs(cache,a.inputs)
        maia = Maia(a.maia)
        try:
            panel,key,hit = Panel.load(a.cache_dir,inputs,maia,a.anchor,a.widths,a.candidates)
        finally:
            maia.close()
        setup = time.perf_counter()-start
        print(f"{run}/{kind} panel {key[:12]}, setup {setup:.1f}s",flush=True)
        graphs,errors,phases = build(panel,inputs,a.anchor,a.budget,a.tolerance,kind=="full",a.candidates)
        ready = time.perf_counter()
        result = dict(panel=key,panel_hit=hit,coverage=panel.coverage,anchor=a.anchor,widths=a.widths,
                      actual_layers=list(map(len,panel.layers)),candidate_cap=a.candidates,budget=a.budget,
                      tolerance=a.tolerance,setup_s=setup,build_s=ready-start-setup,phases=phases,
                      cache=dict(cache.stats),maia_misses=maia.inferred,arms={})
        books,allocs = {},{}
        for k in KS:
            allocs[k] = Allocator(graphs[k],panel.weights[k])
            book,upper = allocs[k].optimize(a.budget)
            books[k] = book
            replay = allocs[k].replay(book)
            if replay["cards"]>a.budget:
                raise ArithmeticError("Overspent study budget")
            result["arms"][k] = dict(replay=replay,price_upper=upper,input_error_bound=errors[k])
            print(f"{run}/{kind} K={k}: {len(book)} cards, {replay['value']:.9f}",flush=True)
        result["solve_s"] = time.perf_counter()-ready
        result["total_s"] = time.perf_counter()-start
        # The LP and cross-replay are audits, timed separately from producing the book.
        if kind=="full":
            for k in KS:
                ub = allocs[k].lp_bound(a.budget)
                arm = result["arms"][k]
                arm["lp_upper"] = ub
                if ub is not None and ub+1e-8<arm["replay"]["value"]:
                    raise ArithmeticError("Budget LP bound contradicted by feasible book")
        result["audit_s"] = time.perf_counter()-start-result["total_s"]
        dest = a.out/run/kind
        publish(dest/"result.json",result)
        raw = {k:[[h,m] for h,m in sorted(b.items())] for k,b in books.items()}
        publish(dest/"choices.json",raw)
        # The two K arms share most moves, so convert SAN just once per position.
        named = {}
        for h in set().union(*books.values()):
            legal = successors(panel.boards[h])
            named[h] = {m:legal[m][0] for b in books.values() if (m:=b.get(h)) is not None}
        publish(dest/"books.json",{k:[dict(hash=h,epd=panel.boards[h],san=named[h][m]) for h,m in sorted(b.items())] for k,b in books.items()})
        return result,books,allocs
    finally:
        cache.db.close()


def compare(a,run,results,policies,ref):
    comparison = {}
    for k in KS:
        s,f = (policies[kind][k] for kind in ("selective","full"))
        scored = {kind:ref[k].replay(policies[kind][k]) for kind in policies}
        sel = results["selective"]["arms"][k]
        if not sel["replay"]["value"]-1e-8<=scored["selective"]["value"]<=sel["replay"]["value"]+sel["input_error_bound"]+1e-8:
            raise ArithmeticError("Independent full replay exceeds input-error bound")
        arm = results["full"]["arms"][k]
        upper = min(arm["price_upper"],arm["lp_upper"] if arm["lp_upper"] is not None else 1.)
        if max(r["value"] for r in scored.values())>upper+1e-8:
            raise ArithmeticError("Full-data upper bound contradicted")
        comparison[k] = dict(scores=scored,common_positions=len(s.keys()&f.keys()),
                             same_cards=len(s.items()&f.items()),
                             reference_minus_selective=scored["full"]["value"]-scored["selective"]["value"],
                             full_optimum_upper=upper,selective_regret_bound=upper-scored["selective"]["value"])
    publish(a.out/run/"comparison.json",comparison)
    return comparison


def audit_inputs(args,db):
    cache = Cache(db)
    try:
        inputs = Inputs(cache,args.inputs)
        maia = Maia(args.maia)
        try:
            panel,key,_ = Panel.load(args.cache_dir,inputs,maia,args.anchor,args.widths,args.candidates)
        finally:
            maia.close()
        saved = json.loads((args.out/"warm/comparison.json").read_text())
        report = {}
        for kind in ("full","selective"):
            dest = args.out/"warm"/kind
            if json.loads((dest/"result.json").read_text())["panel"]!=key:
                raise ValueError("Cannot audit books against a different panel")
            books = json.loads((dest/"choices.json").read_text())
            report[kind] = {}
            for k in KS:
                check = replay_inputs(panel,inputs,args.anchor,dict(books[str(k)]),k,args.candidates)
                if abs(check["score"]-saved[str(k)]["scores"][kind]["value"])>1e-8:
                    raise ArithmeticError("Source-level replay differs from matrix score")
                report[kind][k] = check
        publish(args.out/"verified-exits.json",report)
    finally:
        cache.db.close()


def main():
    base = Path("outputs/rebuild-2026-11-06")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out",type=Path,required=True)
    ap.add_argument("--cache-dir",type=Path,help="Reuse shared inputs across separate experiment output directories")
    ap.add_argument("--inputs",type=Path,default=base/"black/depth-policy/round-00")
    ap.add_argument("--maia",type=Path,default=base/"root-search-pilot")
    ap.add_argument("--anchor",type=int,default=7)
    ap.add_argument("--widths",type=int,nargs="+",default=[320,1600,3600,6500])
    ap.add_argument("--candidates",type=int,default=2)
    ap.add_argument("--budget",type=int,default=2000)
    ap.add_argument("--tolerance",type=float,default=.001)
    ap.add_argument("--audit-only",action="store_true",help="Replay finished books against board-level inputs")
    args = ap.parse_args()
    if (len(args.widths)<3 or min(args.widths)<1 or args.anchor%2!=1 or args.anchor<1
            or args.anchor+2*(len(args.widths)-1)+1>=30 or min(args.candidates,args.budget)<1
            or not 0<args.tolerance<1):
        ap.error("Use at least three positive widths, an interior odd anchor, positive caps and a tolerance in (0,1)")
    args.out.mkdir(parents=True,exist_ok=True)
    args.cache_dir = args.cache_dir or args.out
    args.cache_dir.mkdir(parents=True,exist_ok=True)
    db = args.cache_dir/"inputs.sqlite"
    if args.audit_only:
        audit_inputs(args,db)
        return
    common = args.out/"cold-setup.json"
    t = time.perf_counter()
    cache = Cache(db)
    inputs = Inputs(cache,args.inputs)
    maia = Maia(args.maia)
    try:
        _,key,hit = Panel.load(args.cache_dir,inputs,maia,args.anchor,args.widths,args.candidates)
        if not common.exists() or json.loads(common.read_text()).get("key")!=key:
            publish(common,dict(key=key,hit=hit,seconds=time.perf_counter()-t,cache=dict(cache.stats),maia_misses=maia.inferred))
    finally:
        maia.close()
        cache.db.close()
    del inputs,_
    separate = args.out/"selective-inputs.sqlite"
    if not separate.exists():
        # Identical position-input state for each logical cold run.
        with tempfile.NamedTemporaryFile(dir=args.out,delete=False) as f:
            name = Path(f.name)
        source,target = sqlite3.connect(db),sqlite3.connect(name)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        os.replace(name,separate)
    first = None
    for run,order in (("first",("selective","full")),("warm",("full","selective"))):
        results,policies = {},{}
        ref = None
        for kind in order:
            path = separate if kind=="selective" and run=="first" else db
            results[kind],policies[kind],allocs = run_one(args,kind,run,path)
            if kind=="full":
                ref = allocs
            del allocs
        compare(args,run,results,policies,ref)
        if first is not None and policies!=first:
            raise ArithmeticError("Cold and warm policies disagree")
        first = policies
    audit_inputs(args,db)
    print(f"Verified multi-depth results in {args.out}",flush=True)


if __name__=="__main__":
    main()
