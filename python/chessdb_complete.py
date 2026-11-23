"""Final acceptance and immutable evidence snapshot after a candidate lookup build.

python chessdb_complete.py --lookup PATH --old PATH --ops PATH --evidence NEW_PATH
Waits for the existing build's verified status, then independently hashes every
output, checks original comparisons, and snapshots evidence. Never starts a pool,
changes bindings, deletes data, or edits a running release.
"""
import argparse
import datetime as dt
import time
import uuid
import psutil
from pathlib import Path
from chessdb_integrity import fingerprint
from chessdb_evidence import create
from month_archive import read_json,write_json,sha,file_lock

def verify_outputs(root,old,expected=range(512)):
    root,old=Path(root),Path(old);expected=list(expected)
    meta=read_json(root/"lookup.json")
    if read_json(root/"_identity.json") != meta["identity"]:raise ValueError("global identity changed")
    names={f"bkt{b:03d}.npy" for b in expected}
    if set(meta["buckets"])!=names or {p.name for p in root.glob("bkt*.npy")}!=names:
        raise ValueError("wrong output bucket set")
    if {p.name for p in (root/"_done").glob("*.json")}!={f"{b:03d}.json" for b in expected}:
        raise ValueError("wrong receipt bucket set")
    old_names={p.name for p in (old/"_done").glob("*.json")}
    if not old_names<={f"{b:03d}.json" for b in expected}:raise ValueError("unexpected old receipt bucket")
    compared=0;totals={k:0 for k in ("rows","base_rows","gap_added","legacy_replaced")}
    for b in expected:
        rec=meta["buckets"][f"bkt{b:03d}.npy"]
        if read_json(root/"_done"/f"{b:03d}.json")!=rec:raise ValueError(f"receipt changed: {b}")
        if rec.get("identity")!=meta["identity"] or rec.get("bucket")!=b:raise ValueError(f"wrong receipt identity: {b}")
        fingerprint(root/f"bkt{b:03d}.npy",rec["sha256"])
        prior=old/"_done"/f"{b:03d}.json"
        if prior.exists():
            original=read_json(prior);comparison=rec.get("old_comparison",{})
            if not comparison.get("matched") or comparison.get("receipt_sha256")!=sha(prior):
                raise ValueError(f"missing/stale old comparison: {b}")
            fingerprint(old/f"bkt{b:03d}.npy",original["sha256"])
            if original["sha256"]!=rec["sha256"]:raise ValueError(f"old/new bytes differ: {b}")
            compared+=1
        for k in totals:totals[k]+=rec[k]
    return dict(verified_buckets=len(expected),old_comparisons=compared,totals=totals,
                lookup_manifest_sha256=sha(root/"lookup.json"),producer=meta["identity"]["producer"])

def complete(lookup,old,ops,evidence):
    lookup,old,ops,evidence=map(Path,(lookup,old,ops,evidence))
    status=ops/"completion-status.json"
    with file_lock(ops/"_completion.lock"):
        while True:
            current=read_json(lookup/"_status.json")
            if current["state"]=="failed":
                write_json(status,dict(state="blocked-by-build-failure",error=current.get("error")))
                raise RuntimeError("lookup coordinator failed; no acceptance published")
            if current["state"]=="verified":break
            pid=current.get("pid")
            try: live=pid is not None and psutil.Process(pid).is_running() and any("chessdb_lookup_run.py" in s for s in psutil.Process(pid).cmdline())
            except (psutil.NoSuchProcess,psutil.AccessDenied):live=False
            if not live:
                write_json(status,dict(state="build-process-gone",pid=pid))
                raise RuntimeError("build process disappeared; restart the locked pipeline")
            write_json(status,dict(state="waiting-for-build",completed=current["completed"],total=current["total"]))
            time.sleep(30)
        try:
            write_json(status,dict(state="verifying-output-bytes"))
            with file_lock(lookup/"_coordinator.lock",wait=True):
                proof=verify_outputs(lookup,old)
                proof.update(schema="chessdb-lookup-acceptance-v1",
                    completed_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                    verifier_sha256=sha(Path(__file__)),bindings_changed=False)
                write_json(ops/"lookup-acceptance.json",proof)
                evidence=Path(str(evidence)+"-"+proof["lookup_manifest_sha256"][:12])
                if evidence.exists():evidence=Path(str(evidence)+"-"+uuid.uuid4().hex[:8])
                bundle=create("D:/chess/acceptance/chessdb-import-20261122",evidence,lookup,ops/"validation-v2")
            write_json(status,dict(state="complete",lookup_acceptance=str(ops/"lookup-acceptance.json"),
                        evidence=str(evidence),manifest_sha256=bundle["manifest_sha256"],
                        adoption="candidate only; base bindings unchanged"))
            print(proof,flush=True)
        except BaseException as exc:
            write_json(status,dict(state="failed",error=f"{type(exc).__name__}: {exc}"))
            raise

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for n in ("lookup","old","ops","evidence"):p.add_argument("--"+n,required=True,type=Path)
    a=p.parse_args();complete(a.lookup,a.old,a.ops,a.evidence)
