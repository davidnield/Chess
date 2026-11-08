"""Resume the Black cost experiment, choose the White search schedule, and score both colors."""
import json
import msvcrt
from pathlib import Path
import time
from contextlib import contextmanager
import errno
import shutil
import subprocess
import sys
import tempfile

import polars as pl
import psutil

import nomination_trial as nt
import run_practical_rebuild as r


@contextmanager
def exclusive(path):
    with path.open("a+b") as f:
        f.seek(0)
        msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
        yield


def broad_build(colour):
    """Use the original stage paths and pins; recover a skipped DP footprint symmetrically."""
    old=(r.OUT,r.STATUS_FILE,r.provisional)
    r.OUT,r.STATUS_FILE,r.provisional=nt.BROAD,nt.BROAD/"status.json",nt.exact_provisional
    try:
        with exclusive(nt.BROAD/"run.lock"):
            books=r.build(colour)
            r.audit(colour,books)
            r.status(state=f"{colour}-broad-complete",book=books["NEW"])
            return books["NEW"]
    finally:
        r.OUT,r.STATUS_FILE,r.provisional=old


def broad_active():
    try:
        with exclusive(nt.BROAD/"run.lock"):
            return False
    except OSError as exc:
        if exc.errno!=errno.EACCES:
            raise
        return True


def sampler_still_running():
    adoption=nt.BROAD/"black-only-adoption.json"
    if not adoption.exists():
        return False
    info=json.loads(adoption.read_text())
    try:
        proc=psutil.Process(info["worker"])
        return proc.create_time()==info["worker_created"] and proc.cmdline()==info["original"]["command"]
    except psutil.NoSuchProcess:
        return False


def cost():
    arms={}
    for arm,root in [("broad",nt.BROAD),("targeted",nt.TRIAL)]:
        if arm=="broad":
            files=list((root/"black/depth-policy").glob("round-*/hybrid-proposals-*.parquet"))
            n=sum(pl.scan_parquet(p).filter(pl.col("engine")!="Lichess cloud PV").select(pl.len()).collect().item() for p in files)
            verify=root/"black/depth-policy/verification-cache"
        else:
            files=list((root/"black").glob("round-*/local.parquet.meta.json"))
            n=sum(json.loads(p.read_text())["requested"] for p in files)
            verify=root/"black/verification-cache"
        stages={p.stem:json.loads(p.read_text())["seconds"] for p in (root/"stages").glob("black*.json")}
        other_seconds=sum(sec for name,sec in stages.items() if "-hybrid-" not in name and
                          not name.endswith(("-local","audit-CANON","audit-LEARNED")))
        # Old hybrid receipts span only their final resume; time their cloud parts separately.
        # The targeted seed reuses the original pre-nomination graph and owns its cloud lookup.
        dirs=([Path(str(p)+".parts") for p in files] if arm=="broad" else
              [nt.TRIAL/"black/seed/cloud.parquet.parts"])
        cloud_seconds=0.
        for parts in dirs:
            matches=list(parts.glob("cloud-*.parquet" if arm=="broad" else "[0-9]*.parquet"))
            if matches:
                cloud_seconds+=max(p.stat().st_mtime for p in matches)-min(p.stat().st_mtime for p in matches)
        if arm=="targeted":
            other_seconds+=json.loads((nt.BROAD/"stages/black-targets-01.json").read_text())["seconds"]
        verified=[json.loads(p.read_text()) for p in verify.glob("*.json") if p.name!="config.json"]
        arms[arm]={"local_nomination_calls":n,"nominal_nomination_nodes":n*10_000_000,
                   "local_nomination_hours_at_1428_per_hour":n/1428,
                   "estimated_cold_build_hours":n/1428+(other_seconds+cloud_seconds)/3600,
                   "nonlocal_build_stage_hours":other_seconds/3600,"cloud_part_span_hours":cloud_seconds/3600,
                   "verified_roots":len(verified),
                   "retained_verification_nodes":sum(v["free"]["nodes"]+(
                        sum(s["nodes"] for s in v["scores"].values()) if len(v["scores"])>1 else 0) for v in verified),
                   "completed_stage_seconds":stages}
    return arms


def compare(books):
    from nomination_evidence import summarize
    dest=nt.TRIAL/"comparison-black"
    dest.mkdir(exist_ok=True)
    for label,path in books.items():
        if not path.is_file():
            raise ValueError(f"Missing finalist {label}: {path}")
    frozen={k:{"path":str(p),"sha256":r.digest(p)} for k,p in books.items()}
    manifest=dest/"books.json"
    if manifest.exists() and json.loads(manifest.read_text())!=frozen:
        raise RuntimeError("Finalists changed during comparison")
    r.publish(manifest,frozen)
    for part,labels,seeds,reps in [("baseline",list(books),"1,2,3,4",0),("folds",["BROAD","TARGET"],"1",16)]:
        out=dest/(part+".parquet")
        args=["--our-moves",r.HX/"movers1000_black_f64_2026_08_09","--opp-moves",r.HX/"movers1800_white_f64_2026_08_09",
              "--tau",".0001","--max-ply",30,"--rollouts",2,"--sf-procs",2,"--seed",seeds,
              "--replicates",reps,"--compare-to","BROAD","--sf-cache",nt.TRIAL/"training-sf.parquet","--out",out]
        for label in labels:
            args+=["--book",f"{label}=black={books[label]}"]
        r.stage("comparison-"+part,r.cmd("convertibility_eval.py",*args),[out])
    r.stage("comparison-fresh-audit",r.cmd(nt.HERE/"nomination_evidence.py","--broad",books["BROAD"],
            "--target",books["TARGET"],"--out",dest),[dest/"safety.json"])
    decision=summarize(pl.read_parquet(dest/"baseline.parquet"),pl.read_parquet(dest/"folds.parquet"),
                       json.loads((dest/"safety.json").read_text()))
    decision["cost"]=cost()
    if (decision["method"]=="targeted" and decision["cost"]["targeted"]["estimated_cold_build_hours"]>=
            decision["cost"]["broad"]["estimated_cold_build_hours"]):
        decision["method"]="inconclusive"
    r.publish(dest/"decision.json",decision)
    return decision


def workflow():
    root=nt.TRIAL
    root.mkdir(exist_ok=True)
    with exclusive(root/"workflow.lock"):
        snapshot=json.loads((nt.HERE/"trial-runtime.json").read_text())
        source={p.name:r.digest(p) for p in nt.HERE.glob("*.py")}
        if source!=snapshot["files"]:
            raise ValueError("Immutable method snapshot changed")
        design={"scripts":source,"selection_pin":r.digest(r.SELECTION/"source.json"),
                "local_frontier_budget":128,"frontier_min_optimistic_reach":.001,
                "development_margin":.002,"fresh_audit_nodes":30_000_000,"fold_replicates":16,
                "baseline_seeds":[1,2,3,4],"reserved_months_used":[]}
        design_file=root/"design.json"
        if design_file.exists() and json.loads(design_file.read_text())!=design:
            raise ValueError("Trial protocol changed; use a versioned experiment")
        r.publish(design_file,design)
        original_stage=r.stage
        def checked_stage(label,command,outputs):
            # A completed screen launched before the pin has identical code and
            # inputs, but its command names the original checkout. Verify the
            # existing receipt and checksums without rerunning working-tree code.
            done=r.OUT/"stages"/(label+".json")
            if done.exists() and label.startswith("tune-") and Path(command[3]).parent==nt.HERE:
                old=json.loads(done.read_text())["command"]
                args=list(map(str,command))
                if (Path(old[3])==r.ROOT/"python"/Path(args[3]).name and args[:3]+args[4:]==old[:3]+old[4:]):
                    command=old
            return original_stage(label,command,outputs)
        r.stage=checked_stage
        r.publish(root/"queue.json",{"state":"waiting-for-broad-black","since":r.now(),
                  "next":["targeted-black","paired-score-and-fresh-audit","choose-white-method","white","tuning","scenarios"]})
        while True:
            state=json.loads((nt.BROAD/"status.json").read_text())
            if ((nt.BROAD/"black/verified-book.json").exists() and
                    (nt.BROAD/"stages/black-audit-LEARNED.json").exists()):
                break
            if not broad_active() and not sampler_still_running():
                r.setup()
                r.setup_selection()
                broad_build("black")
                break
            time.sleep(30)
        nt.setup()
        r.publish(root/"queue.json",{"state":"targeted-black","time":r.now()})
        target=nt.build("black")
        broad=nt.BROAD/"black/practical_black_b2000.parquet"
        prior=Path("E:/chess/repertoire/_candidates_v3")
        books={"BROAD":broad,"TARGET":target,"CANON":r.incumbent("black"),
               "FREE":prior/"free_black_b2000.parquet","ENGINE2001":prior/"freeengine_black_b2000.parquet"}
        r.publish(root/"queue.json",{"state":"paired-comparison","time":r.now()})
        decision=compare(books)
        # A tied/inconclusive experiment can build provisional White cheaply. Only positive
        # evidence for broad search releases White's exhaustive local queue.
        method=decision["method"]
        r.publish(root/"white-plan.json",{"method":"broad" if method=="broad" else "targeted",
                  "black_evidence":method,"time":r.now()})
        r.publish(root/"queue.json",{"state":"building-white","time":r.now()})
        white=broad_build("white") if method=="broad" else nt.build("white")
        from screen_repertoires import screen
        r.publish(root/"queue.json",{"state":"tuning","time":r.now()})
        screen()
        import practical_scenarios as ps
        r.publish(root/"queue.json",{"state":"scenario-comparisons","time":r.now()})
        for colour,book in [("black",broad if method=="broad" else target),("white",white)]:
            arms={"CANON":r.incumbent(colour),"NEW":book,"LEARNED":r.incumbent(colour,True)}
            for name,scenario in ps.SCENARIOS.items():
                r.compare(colour,arms,{},name,scenario)
        r.publish(root/"queue.json",{"state":"complete","time":r.now()})


def launch():
    pin=nt.TRIAL/"method-code"
    if nt.HERE==pin:
        workflow()
        return
    nt.TRIAL.mkdir(parents=True,exist_ok=True)
    if not pin.exists():
        with tempfile.TemporaryDirectory(dir=nt.TRIAL) as tmp:
            candidate=Path(tmp)/"method-code"
            candidate.mkdir()
            for p in nt.HERE.glob("*.py"):
                shutil.copy2(p,candidate/p.name)
            r.publish(candidate/"trial-runtime.json",{"checkout":str(r.ROOT),
                      "files":{p.name:r.digest(p) for p in candidate.glob("*.py")}})
            candidate.rename(pin)
    subprocess.run([sys.executable,"-u",str(pin/Path(__file__).name)],cwd=r.ROOT,check=True)


if __name__=="__main__":
    try:
        launch()
    except Exception as exc:
        r.publish(nt.TRIAL/"queue-error.json",{"time":r.now(),"error":str(exc)})
        raise
