"""Receipted joint build/verify/rebuild epochs. Dry-run by default.

Every arm is rebuilt on the same immutable evidence after a changed pass, even
when only another arm requested the search. This deliberately uses full rebuilds;
it does not claim an incremental graph solver. A round cap is resumable, never
verification closure. The production matrix additionally needs a pilot acceptance
artifact; closure alone does not grant that approval.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
import polars as pl
import run_rare_book as runner
from consumer_keys import PAIR
from .builder import command
from .spec import digest, manifest
from .stages import artifacts, atomic_json, file_sha
from .reports import pilot_acceptance
from .bindings import check_arm

def check_book(path, arm, overlay, *, final=False):
    rows=pl.read_parquet(path)
    if rows.schema["position_hash"]!=PAIR: raise ValueError("book needs full keys")
    ours=rows.filter((pl.col("side_to_move")==arm["recipe"]["colour"]) &
                     pl.col("best_move").is_not_null())
    budget=arm["recipe"]["budget"]
    if ours.height!=ours["position_hash"].n_unique() or ours.height>budget:
        raise ValueError("invalid distinct prescription count")
    if final and ours.height!=budget:
        raise ValueError("verification closed but book did not fill its prescribed budget")
    meta=json.loads(Path(str(path)+".meta.json").read_text(encoding="utf8"))
    if meta.get("engine_candidates_sha256")!=(file_sha(overlay) if overlay else None):
        raise ValueError("book did not consume this epoch's frozen evidence")
    if meta.get("verification_policy")!="quality": raise ValueError("book is not quality-reviewed")
    expected=arm["recipe"].get("evaluation_binding_sha256")
    if expected and (meta.get("evaluation_binding") or {}).get("binding_sha256")!=expected:
        raise ValueError("book did not consume its declared evaluation binding")
    return dict(book=str(path),distinct_prescriptions=ours.height,sha256=file_sha(path))

def ordered(entries):
    if not entries: raise ValueError("joint run needs at least one arm")
    pending={e["arm"]["id"]:e for e in entries}; result=[]
    if len(pending)!=len(entries): raise ValueError("duplicate arm")
    while pending:
        available=[k for k,e in pending.items() if not (set(e["arm"].get("depends_on",())) & set(pending))]
        if not available: raise ValueError("cyclic arm dependency")
        for k in sorted(available): result.append(pending.pop(k))
    return result

def preflight(config):
    if config["schema"] not in (1,2): raise ValueError("unsupported joint build config")
    m=manifest(); known={a["id"]:a for a in m["primary"]+m["sensitivities"]}
    entries=ordered(config["arms"])
    paired=config["schema"]==2
    if paired:
        from .scenarios import check_pair
        referee=check_pair(config,known[m["pilot"]])
    for e in ([] if paired else entries):
        a=e["arm"]
        if a["id"] not in known or a["recipe"]!=known[a["id"]]["recipe"]:
            raise ValueError("arm is not in the approved manifest")
        if a.get("depends_on",[])!=known[a["id"]]["depends_on"]:
            raise ValueError("arm dependencies differ from the approved manifest")
    ids={e["arm"]["id"] for e in entries}
    for e in entries:
        # the bound files must be the populations the recipe names (pool, weak, opponent policy)
        check_arm(e["arm"]["recipe"],e["inputs"])
    for e in entries:
        r=e["arm"]["recipe"]
        if r["mode"]=="reduced" and r.get("allowed_parent",r["master"]) not in ids:
            raise ValueError("a reduced arm requires its parent in the shared evidence epoch")
    pinned=artifacts(config["artifacts"])
    required={str(Path(v).resolve()) for e in entries for k,v in e["inputs"].items()
              if v and k not in ("engine_overlay",)}
    import engine_candidates as ec
    required.update(str(Path(p).resolve()) for p in (config.get("python",sys.executable),ec.SF))
    for e in entries:
        if e["inputs"].get("engine_overlay") not in (None,config.get("seed_overlay")):
            raise ValueError("use one shared seed_overlay, not per-arm engine evidence")
        for selective in [e["inputs"].get(k) for k in ("opponent_policy_config","continuation_config")]:
            if not selective:continue
            cfg=json.loads(Path(selective).read_text(encoding="utf8"))
            for item in cfg["artifacts"]:
                path=str(Path(item["path"]).resolve());required.add(path)
                if pinned.get(path)!=item["sha256"]: raise ValueError("policy source is not pinned to the same generation")
        for k in ("source","stats","aux","weak"):
            path=Path(str(e["inputs"][k])+".meta.json")
            if path.exists(): required.add(str(path.resolve()))
        from eval_binding import read_binding
        if e["inputs"].get("eval_binding"):
            binding=read_binding(e["inputs"]["eval_binding"])
            lookup=binding["lookup"]
        else:
            path=Path(e["inputs"]["stats"]+".meta.json")
            meta=json.loads(path.read_text(encoding="utf8"))
            lookup=meta.get("eval_lookup128")
        if not lookup: raise ValueError("candidate pool has no full-key lookup receipt")
        required.add(str((Path(lookup)/"lookup.json").resolve()))
        if e["inputs"].get("opponent_overlay"):
            required.add(str(Path(e["inputs"]["opponent_overlay"]+".json").resolve()))
    if config.get("seed_overlay"): required.add(str(Path(config["seed_overlay"]).resolve()))
    if paired:
        required.update((referee["binding_path"],str((Path(referee["lookup"])/"lookup.json").resolve())))
    if not required<=set(pinned): raise ValueError("not all bound inputs/sidecars are pinned")
    if not paired and [e["arm"]["id"] for e in entries]!=[m["pilot"]]:
        report=config.get("pilot_report")
        if not report or str(Path(report).resolve()) not in pinned:
            raise ValueError("matrix needs a pinned Black pilot acceptance report")
        if not pilot_acceptance(json.loads(Path(report).read_text(encoding="utf8")))["ready_for_matrix"]:
            raise ValueError("Black pilot has not passed all release checks")
    return entries

def drive(config, out, *, max_rounds=8, source=None, build_fn=None, verify_fn=None, audit_fn=None):
    """Hooks isolate engine/process I/O in tests; the receipt and closure loop is real."""
    if max_rounds<1: raise ValueError("need a positive round cap")
    out=Path(out).resolve(); out.mkdir(parents=True,exist_ok=True)
    source=Path(source or Path(__file__).resolve().parents[1])
    code=runner.code_sha(runner.code_files(source))
    kept=runner.snapshot(out,code,source)
    identity=dict(config=config,code=code)
    settings=out/"recipe.json"
    if settings.exists() and json.loads(settings.read_text(encoding="utf8"))!=identity:
        raise runner.StaleRound("joint run recipe/code changed; use a new output directory")
    atomic_json(settings,identity)
    def guard():
        if runner.code_sha(runner.code_files(kept))!=code:
            raise runner.StaleRound("joint run code snapshot changed")
        artifacts(config["artifacts"])
    guard()
    py=config.get("python",sys.executable)
    def default_build(cmd, log):
        runner.run(cmd,log,kept)
    def default_verify(books, dest, prior, log):
        listing=dest.parent/"books.json"; atomic_json(listing,books)
        cmd=[py,"-X","utf8","-u","-m","sweep.verify","--books",str(listing),
             "--cache",config["searches"],"--out",str(dest),"--threads",str(config.get("threads",2))]
        if prior: cmd+=["--prior",str(prior)]
        runner.run(cmd,log,kept)
    def default_audit(book, arm, dest):
        prefix=dest.with_suffix("")
        runner.run([py,"-X","utf8",str(kept/"validate_book.py"),"--book",str(book),
                    "--colour",arm["recipe"]["colour"],"--out",str(prefix)],dest.with_suffix(".log"),kept)
        report=json.loads(dest.read_text(encoding="utf8"))
        if report["flagged"] or set(report["status"])-{"in_set"}:
            raise ValueError("closed book failed the independent gate audit")
    build_fn=build_fn or default_build
    verify_fn=verify_fn or default_verify
    audit_fn=audit_fn or default_audit
    entries=ordered(config["arms"]); overlay=config.get("seed_overlay"); history=[]
    for epoch in range(max_rounds):
        rd=out/f"round-{epoch:02d}";rd.mkdir(exist_ok=True)
        books=[]; reports=[]; built_by_id={}
        atomic_json(out/"status.json",dict(state="building",round=epoch))
        for e in entries:
            arm=e["arm"]; folder=rd/arm["id"];folder.mkdir(exist_ok=True)
            inputs=dict(e["inputs"],engine_overlay=overlay)
            if arm["recipe"]["mode"]=="reduced":
                dependency=arm["recipe"].get("allowed_parent",arm["recipe"]["master"])
                if dependency in built_by_id: inputs["allowed_book"]=str(built_by_id[dependency])
            prefix=folder/"book"; budget=arm["recipe"]["budget"]
            book=Path(str(prefix)+f"_b{budget}.parquet")
            cmd=command(arm,python=py,code=kept,inputs=inputs,out_prefix=prefix,tmp=folder/"tmp")
            outputs=[book,Path(str(book)+".meta.json"),Path(str(book)+".rare.json"),
                     Path(str(prefix)+".quality-frontier.json")]
            if inputs.get("opponent_policy_config"):
                outputs += [Path(str(prefix)+".opponent.parquet"),Path(str(prefix)+".opponent.parquet.json")]
            if inputs.get("continuation_config"):
                outputs += [Path(str(prefix)+".continuation.json")]
            if arm["recipe"]["lookahead_floor"]:
                outputs += [Path(str(book)+s) for s in
                            (".lookahead-inputs.parquet",".lookahead.json",".lookahead-audit.json")]
            def work():
                # A missing receipt invalidates partial output; the native builder must not reuse it.
                build_fn(cmd,folder/"build.log")
                check_book(book,arm,overlay)
                if arm["recipe"]["lookahead_floor"]:
                    runner.run([py,"-X","utf8",str(kept/"lookahead_audit.py"),str(book),
                        str(book)+".lookahead-audit.json","--depth",str(arm["recipe"]["lookahead_floor"])],
                        folder/"lookahead.log",kept)
            receipt,reused=runner.stage(folder/"build.receipt.json",
                 dict(run=digest(identity),command=cmd,overlay=file_sha(overlay) if overlay else None,
                      allowed=file_sha(inputs["allowed_book"]) if inputs.get("allowed_book") else None),
                 outputs,work,guard)
            reports.append(dict(arm=arm["id"],reused=reused,seconds=receipt["seconds"]))
            books.append(dict(book=str(book),colour=arm["recipe"]["colour"]))
            built_by_id[arm["id"]]=book
        dest=rd/"evidence.parquet"
        atomic_json(out/"status.json",dict(state="verifying",round=epoch))
        receipt,reused=runner.stage(rd/"verify.receipt.json",
            dict(run=digest(identity),books=[file_sha(b["book"]) for b in books],
                 build_receipts=[file_sha(rd/e["arm"]["id"]/"build.receipt.json") for e in entries],
                 prior=file_sha(overlay) if overlay else None),
            [dest,Path(str(dest)+".meta.json"),Path(str(dest)+".requests.json")],
            lambda:verify_fn(books,dest,overlay,rd/"verify.log"),guard)
        info=json.loads(Path(str(dest)+".meta.json").read_text(encoding="utf8"))
        if info["input_overlay_sha256"]!=(file_sha(overlay) if overlay else None):
            raise runner.StaleRound("verifier consumed different evidence")
        if info["overlay_sha256"]!=file_sha(dest): raise runner.StaleRound("corrupt verification output")
        history.append(dict(round=epoch,builds=reports,requested=info["requested_parents"],
                            verification_reused=reused,verification_seconds=receipt["seconds"]))
        atomic_json(out/"rounds.json",history)
        if not info["requested_parents"]:
            if info.get("closed") is not True: raise ValueError("inconsistent verification closure")
            final_books=[]
            for e in entries:
                arm=e["arm"];book=built_by_id[arm["id"]]
                rec=check_book(book,arm,overlay,final=True)
                audit=book.parent/"gate-audit.json"
                runner.stage(book.parent/"audit.receipt.json",
                     dict(run=digest(identity),book=file_sha(book),overlay=file_sha(overlay) if overlay else None),
                     [audit,audit.with_suffix(".rows.parquet")],
                     lambda:audit_fn(book,arm,audit),guard)
                final_books.append(dict(rec,arm=arm["id"],audit_sha256=file_sha(audit)))
            final=dict(state="quality-closed",round=epoch,books=final_books,code=code,
                       # Closure names evidence actually APPLIED, not a no-op rewritten parquet.
                       overlay=overlay,applied_overlay_sha256=file_sha(overlay) if overlay else None,
                       production_release=False,remaining_checks=["pilot acceptance","clean rebuild parity"],
                       recipe_sha256=file_sha(settings))
            atomic_json(out/"final.json",final);atomic_json(out/"status.json",final)
            return final
        overlay=str(dest)
    status=dict(state="round-limit",round=max_rounds-1,next_overlay=overlay,
                resumable=True,quality_closed=False)
    atomic_json(out/"status.json",status)
    return status

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    p.add_argument("--max-rounds",type=int,default=8)
    p.add_argument("--execute",action="store_true")
    a=p.parse_args(); cfg=json.loads(a.config.read_text(encoding="utf8"))
    preflight(cfg)
    try:
        result=(drive(cfg,a.out,max_rounds=a.max_rounds) if a.execute else
                dict(state="ready",arms=[e["arm"]["id"] for e in ordered(cfg["arms"])],
                     starts_jobs=False,max_rounds=a.max_rounds))
    except Exception as exc:
        if a.execute: atomic_json(a.out/"status.json",dict(state="failed",error=str(exc),
                                  requires_new_directory=isinstance(exc,runner.StaleRound)))
        raise
    print(json.dumps(result,indent=2))

if __name__=="__main__": main()
