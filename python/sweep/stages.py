"""Bounded, receipt-checked shared-input stages; no background queue or shell execution."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from .spec import digest

def file_sha(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f,"sha256").hexdigest()

def atomic_json(path,value):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name(path.name+".tmp")
    tmp.write_text(json.dumps(value,indent=2,allow_nan=False)+"\n",encoding="utf8")
    os.replace(tmp,path)

def artifacts(items):
    """Path+SHA, not existence or a batch DONE marker, is the identity."""
    result={}
    for item in items:
        path=Path(item["path"]).resolve(strict=True)
        if not path.is_file() or file_sha(path)!=item["sha256"]:
            raise ValueError(f"artifact changed: {path}")
        result[str(path)]=item["sha256"]
    return result

def stage(spec, root, *, execute=False):
    root=Path(root).resolve()
    if not spec["id"].replace("-","").replace("_","").isalnum():
        raise ValueError("invalid stage ID")
    folder=root/spec["id"]
    paths=[Path(p).resolve() for p in spec["outputs"]]
    if not paths or len(paths)!=len(set(paths)):
        raise ValueError("missing or duplicate stage outputs")
    # This runner can publish only inside its explicitly named output workspace.
    if any(not p.is_relative_to(root) for p in paths):
        raise ValueError("stage output escapes its workspace")
    command=spec["command"]
    if not isinstance(command,list) or not command or not all(isinstance(s,str) and s for s in command):
        raise ValueError("command must be a nonempty argument array")
    inputs=artifacts(spec["inputs"])
    identity=digest({"spec":spec,"inputs":inputs})
    receipt=folder/"receipt.json"
    if receipt.exists():
        saved=json.loads(receipt.read_text(encoding="utf8"))
        if saved["identity"]!=identity: raise ValueError("stale stage recipe")
        artifacts(saved["outputs"])
        return dict(state="reused",receipt=str(receipt))
    if not spec.get("validate") or not isinstance(spec["validate"], list):
        raise ValueError("stage needs an explicit validator argument array")
    if not execute: return dict(state="ready",identity=identity,command=command)
    folder.mkdir(parents=True,exist_ok=True)
    start=time.monotonic()
    atomic_json(folder/"status.json",dict(state="running",identity=identity))
    try:
        with (folder/"run.log").open("a",encoding="utf8") as log:
            subprocess.run(command,cwd=spec["cwd"],stdout=log,stderr=subprocess.STDOUT,
                           check=True,shell=False)
        artifacts(spec["inputs"])  # refuse publication if any pinned input changed during work
        # A stage validator must be pinned as an input like the producer.
        if not spec.get("validate"): raise ValueError("no stage validator configured")
        with (folder/"validation.log").open("a",encoding="utf8") as log:
            subprocess.run(spec["validate"],cwd=spec["cwd"],stdout=log,stderr=subprocess.STDOUT,
                           check=True,shell=False)
        outputs=[dict(path=str(p),sha256=file_sha(p)) for p in paths]
        artifacts(outputs)
        artifacts(spec["inputs"])
        saved=dict(identity=identity,outputs=outputs,seconds=time.monotonic()-start,
                   command=command,validation=spec["validate"])
        atomic_json(receipt,saved)
        atomic_json(folder/"status.json",dict(state="complete",identity=identity))
        return dict(state="complete",receipt=str(receipt))
    except BaseException as exc:
        atomic_json(folder/"status.json",dict(state="failed",identity=identity,error=str(exc)))
        raise

def pool_command(recipe, *, python, code, explorer, lookup, root, workers=1, threads=2):
    """Reuse the existing resumable export, including its per-bucket acceptance checks."""
    if recipe["min_games"]!=1 or recipe["key_bits"]!=128:
        raise ValueError("sweep pools require uncut full keys")
    code,root=Path(code),Path(root)
    return [str(python),"-X","utf8","-u",str(code/"python/pool_from_book.py"),
            "--book",str(explorer),"--eval-lookup128",str(lookup),
            "--events",*recipe["events"],"--elo-bands",*map(str,recipe["elo_bands"]),
            "--max-ply",str(recipe["max_ply"]),"--min-games","1","--tag",recipe["id"],
            "--work",str(root/recipe["id"]/"work"),"--out-dir",str(root/recipe["id"]/"pool"),
            "--tmp-dir",str(root/recipe["id"]/"tmp"),"--workers",str(workers),
            "--threads",str(threads),"--mem","8GB","--phase","all"]
