"""Resume the isolated K=30 Black challenger and its paired ply-30 comparisons.

Run with the project venv. Wait for the broad Black selection, reuse its final
nominees and independent Stockfish checks, then verify every changed decision.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import msvcrt
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid


def digest(f):
    with f.open("rb") as stream:
        return hashlib.file_digest(stream,"sha256").hexdigest()


def copy_once(src,dest):
    if dest.exists():
        return
    dest.parent.mkdir(parents=True,exist_ok=True)
    tmp = dest.with_name(dest.name+".tmp")
    shutil.copyfile(src,tmp)
    tmp.replace(dest)


@contextmanager
def lock(path):
    with path.open("a+b") as fp:
        fp.seek(0)
        msvcrt.locking(fp.fileno(),msvcrt.LK_NBLCK,1)
        try:
            yield
        finally:
            fp.seek(0)
            msvcrt.locking(fp.fileno(),msvcrt.LK_UNLCK,1)


def pin(checkout,dest,extra_scripts=()):
    pin_dir = dest/"code"
    if not pin_dir.exists():
        temp = dest/("pin-"+uuid.uuid4().hex)
        temp.mkdir()
        for source in (checkout/"python").glob("*.py"):
            shutil.copy2(source,temp/source.name)
        shutil.copy2(__file__,temp/"run_opponent_prior_challenger.py")
        for script in extra_scripts:
            shutil.copy2(script,temp/script.name)
        checks = {f.name:digest(f) for f in temp.glob("*.py")}
        (temp/"sources.json").write_text(json.dumps(checks,indent=2))
        temp.rename(pin_dir)
    for file,checksum in json.loads((pin_dir/"sources.json").read_text()).items():
        if digest(pin_dir/file)!=checksum:
            raise ValueError(f"Challenger source snapshot changed: {file}")
    return pin_dir


def workflow(checkout, base, out, code):
    sys.path.insert(0,str(code))
    import polars as pl
    import run_practical_rebuild as r
    import nomination_trial as nt
    import practical_scenarios as scenarios
    r.ROOT,r.OUT,r.STATUS_FILE = checkout,out,out/"status.json"
    r.SELECTION,r.DEPTH,r.CODE = code,code,code
    for folder in ("stages","logs","duckdb-temp"):
        (out/folder).mkdir(exist_ok=True)
    prior = base/"opponent-prior-pilot/challenger-replies.parquet"
    prior_meta = json.loads(Path(str(prior)+".json").read_text())
    if digest(prior)!=prior_meta["sha256"]:
        raise ValueError("Challenger prior changed")
    selection = base/"black/depth-policy/selected.json"
    if not selection.exists():
        r.status(state="waiting-for-verified-baseline-selection",dependency=selection,
                 K=30,prior_positions=prior_meta["positions"])
        while not selection.exists():
            time.sleep(30)
    chosen = json.loads(selection.read_text())
    base_book,props = Path(chosen["book"]),Path(chosen["verified_proposals"])
    if digest(base_book)!=chosen["book_sha256"] or digest(props)!=chosen["verified_proposals_sha256"]:
        raise ValueError("Baseline inputs changed after selection")
    design = {"reference":chosen,"prior":prior_meta,"K":30,"budget":2000,
              "candidate_source":"Freeze broad Black nominations. Recheck and feed back challenger decisions.",
              "comparisons":list(scenarios.SCENARIOS),"horizon":30,"seeds":r.SEEDS,
              "named_rare_control":"Empirical rare replies now have explicit successors. Any book gain measures this change together with shrinkage."}
    manifest=out/"design.json"
    if manifest.exists() and json.loads(manifest.read_text())!=design:
        raise ValueError("Frozen challenger design or baseline changed")
    r.publish(manifest,design)
    ref_root=base/"black/depth-policy"
    root=out/"black/depth-policy"
    # Seed independent folders, then allow both experiments to update their own copies.
    for cache in ("verification-cache","transposition-cache-v2"):
        for src in (ref_root/cache).glob("*.json"):
            copy_once(src,root/cache/src.name)
    for suffix in ("",".meta.json"):
        source=base/("training-sf.parquet"+suffix)
        copy_once(source,out/source.name)
    args=[*r.recipe("black"),"--opponent-prior",str(prior),"--opponent-pseudogames","30"]
    for epoch in range(100):
        label=f"black-prior-{epoch:02d}"
        work=root/f"round-{epoch:02d}"
        work.mkdir(parents=True,exist_ok=True)
        book=nt.exact_provisional("black",work,label,args,props)
        fresh=work/"verified-proposals.parquet"
        receipt=Path(str(fresh)+".meta.json")
        r.stage(label+"-verify",r.cmd(code/"cloud_feedback.py","--book",book,"--colour","black",
                "--proposals",props,"--cache",root/"verification-cache","--out",fresh),[fresh,receipt])
        if not json.loads(receipt.read_text())["searched_positions"]:
            if digest(fresh)!=digest(props):
                raise ValueError("A stable feedback round changed the proposals")
            break
        props=r.resolve_proposals("black",label+"-feedback",fresh)
    else:
        raise RuntimeError("Challenger verification failed to converge")
    r.audit("black",{"NEW":book})
    r.publish(root/"selected.json",{"round":epoch,"book":book,"book_sha256":digest(book),
              "verified_proposals":props,"verified_proposals_sha256":digest(props)})
    base_receipt=base/"black/verified-book.json"
    if not base_receipt.exists():
        r.status(state="waiting-for-baseline-audit",dependency=base_receipt)
        while not base_receipt.exists():
            time.sleep(30)
    books={"CANON":r.incumbent("black"),"BASE":base_book,"MAIA30":book}
    audits={k:json.loads(f.read_text()) for k,f in {
        "CANON":base/"black/audit-CANON.json","BASE":base/"black/audit-NEW.json",
        "MAIA30":out/"black/audit-NEW.json"}.items()}
    for k,file in books.items():
        if audits[k]["book_sha256"]!=digest(file):
            raise ValueError(f"Audit describes a different {k} book")
    for name,scenario in scenarios.SCENARIOS.items():
        r.compare("black",books,audits,name,scenario,private_tables=out/"cohorts")
        dest=out/"black/comparison"/name
        paired=scenarios.paired_summary(pl.read_parquet(dest/"scores.parquet"),"BASE",
                        {k:scenarios.decisions(f,"black") for k,f in books.items()},audits)
        r.publish(dest/"paired-vs-baseline.json",paired)
    r.status(state="complete",book=book,comparison=out/"comparison.csv")


if __name__ == "__main__":
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--checkout",type=Path,default=Path(__file__).resolve().parents[2])
    ap.add_argument("--pinned",action="store_true")
    args=ap.parse_args()
    checkout=args.checkout.resolve()
    broad=checkout/"outputs/rebuild-2026-11-06"
    dest=broad/"opponent-prior-challenger"
    dest.mkdir(exist_ok=True)
    if args.pinned:
        with lock(dest/"run.lock"):
            try:
                workflow(checkout,broad,dest,Path(__file__).parent)
            except Exception as exc:
                import run_practical_rebuild as r
                r.publish(dest/"status.json",{"state":"failed","error":str(exc),"updated_utc":r.now()})
                raise
    else:
        with lock(dest/"pin.lock"):
            code=pin(checkout,dest)
        raise SystemExit(subprocess.call([sys.executable,"-X","utf8","-u",str(code/Path(__file__).name),
                                         "--checkout",str(checkout),"--pinned"],cwd=checkout))
