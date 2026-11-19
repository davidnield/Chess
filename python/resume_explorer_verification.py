"""Resume completed explorer publication checks, then the existing eval launcher.

Runs checks as separately receipted stages under the original migration root lock.
Never reruns extraction/merge or modifies their outputs. The input book and archive
manifests, this release, and each successful stage are pinned. A failed or corrupt
stage never produces the upstream 'training explorer verified' status.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time
from month_archive import root_lock,read_json,write_json,sha

def stage(name,flags,*,root,identity,command,guard,execute=subprocess.run):
    folder=root/name;folder.mkdir(parents=True,exist_ok=True)
    receipt=folder/"receipt.json";log=folder/"run.log"
    spec=dict(identity=identity,flags=flags,command=command)
    guard()
    if receipt.exists():
        saved=read_json(receipt)
        if saved["spec"]!=spec or not log.is_file() or sha(log)!=saved["log_sha256"]:
            raise ValueError(f"stale verification receipt: {receipt}")
        return dict(name=name,reused=True,seconds=saved["seconds"])
    start=time.monotonic()
    write_json(root/"status.json",dict(stage=name,state="running",time=dt.datetime.now().isoformat()))
    with log.open("w",encoding="utf8") as output:
        execute([*command,*flags],stdout=output,stderr=subprocess.STDOUT,check=True)
    guard()
    saved=dict(spec=spec,log_sha256=sha(log),seconds=time.monotonic()-start)
    write_json(receipt,saved)
    return dict(name=name,reused=False,seconds=saved["seconds"])

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for n in ("book","months-root","ops"):
        p.add_argument("--"+n,type=Path,required=True)
    p.add_argument("--mem",default="32GB")
    p.add_argument("--threads",type=int,default=2)
    p.add_argument("--resume-evals",type=Path)
    p.add_argument("--reserve-gb",type=float,default=150.)
    a=p.parse_args()
    pin=Path(__file__).resolve().parent
    release=read_json(pin/"pin.json")
    root=a.ops/"verification-recovery-20261119"
    root.mkdir(parents=True,exist_ok=True)
    with root_lock(a.ops):
        try:
            params=read_json(a.book/"_merge_params.json")
            expected={f"{y}_{m}" for y in range(2013,2027) for m in range(1,13) if (y,m)<=(2026,7)}
            if (params["schema"]!="book-v4" or set(params["months"])!=expected or
                params["input_params"]["max_ply"]!=51 or not params["input_params"]["term_slices"]
                or not params["input_params"].get("key_bits")==128
                or Path(params["months_root"]).resolve()!=a.months_root.resolve()):
                raise ValueError("not the approved complete training explorer generation")
            done=a.book/"_done"
            if {p.name for p in done.glob("bkt*.DONE")}!={f"bkt{i:03d}.DONE" for i in range(512)}:
                raise ValueError("incomplete bucket publication")
            manifests=list(sorted((a.book/"_manifest").glob("*.parquet")))
            if {p.name for p in manifests}!={f"bkt{i:03d}.parquet" for i in range(512)}:
                raise ValueError("incomplete book manifest publication")
            inputs=[a.book/n for n in ("_book.meta.json","_merge_params.json","_BOOK.DONE")]
            inputs+=manifests
            inputs+=[a.months_root/"_manifest"/f"month={m}.parquet" for m in sorted(expected)]
            if a.resume_evals: inputs+=[a.resume_evals,a.resume_evals.parent/"pin.json"]
            inputs+=[Path(sys.executable)]
            hashes={str(f.resolve()):sha(f) for f in inputs}
            identity=dict(inputs=hashes,release=release)
            def guard():
                for name,digest in release["files"].items():
                    if sha(pin/name)!=digest: raise ValueError(f"verification release changed: {name}")
                for name,digest in hashes.items():
                    if sha(Path(name))!=digest: raise ValueError(f"verification input changed: {name}")
                if shutil.disk_usage(a.ops.anchor).free<a.reserve_gb*10**9:
                    raise ValueError("verification free-space reserve exhausted")
            guard()
            command=[sys.executable,"-X","utf8","-u",str(pin/"verify_book.py"),str(a.book),str(a.months_root),
                     "--threads",str(a.threads),"--workers","2","--mem",a.mem,
                     "--tmp-dir",str(root/"tmp")]
            previous=root/"previous-status.json"
            if not previous.exists() and (a.ops/"status.json").exists():
                write_json(previous,read_json(a.ops/"status.json"))
            write_json(a.ops/"status.json",dict(stage="recovering explorer verification",time=dt.datetime.now().isoformat(),
                       recovery=str(root),book=str(a.book),mem=a.mem))
            results=[]
            # First prove the failed operation, then rerun the full originally requested suite.
            checks=[(f"duplicates-{b:03d}",["--dup-buckets",str(b)]) for b in (0,251,511)]
            checks += [(f"digest-{b:03d}",["--digest-buckets",str(b)]) for b in (0,251,511)]
            checks += [("sums",["--sums"]),("terminal-digest",["--term-digest"]),
                       ("collisions",["--collisions"]),("sample",["--sample","3000"])]
            for name,flags in checks:
                record=stage(name,flags,root=root,identity=identity,command=command,guard=guard)
                results.append(record);write_json(root/"progress.json",results);print(json.dumps(record),flush=True)
            guard()
            receipt=dict(identity=identity,checks=results,book_meta_sha256=sha(a.book/"_book.meta.json"),
                         completed=dt.datetime.now().isoformat())
            write_json(root/"verified.json",receipt)
            write_json(root/"status.json",dict(stage="verification complete",time=dt.datetime.now().isoformat()))
            write_json(a.ops/"status.json",dict(stage="training explorer verified",book=str(a.book),
                       meta_sha256=receipt["book_meta_sha256"],verification_receipt=str(root/"verified.json"),
                       verification_receipt_sha256=sha(root/"verified.json"),time=dt.datetime.now().isoformat()))
        except Exception as exc:
            write_json(root/"status.json",dict(stage="stopped",error=str(exc),time=dt.datetime.now().isoformat()))
            write_json(a.ops/"status.json",dict(stage="stopped",error=str(exc),recovery=str(root),
                       time=dt.datetime.now().isoformat()))
            raise
    if a.resume_evals:
        # Its existing pin validation, all-child pilot, disk forecast and reserve checks remain intact.
        print("Explorer checks passed; resuming the pinned eval workflow.",flush=True)
        with root_lock(root/"eval-launch"):
            subprocess.run([sys.executable,"-X","utf8","-u",str(a.resume_evals)],check=True)

if __name__=="__main__": main()
