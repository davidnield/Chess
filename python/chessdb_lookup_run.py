"""Verified ChessDB step-1 lookup coordinator (one process owns each output root).

python chessdb_lookup_run.py --base PATH --eval-db PATH --menus PATH --out NEW_PATH
    [--workers 2] [--buckets 0 1] [--old-lookup OLD_PATH]

The production CLI requires _MENUS.COMPLETE.json. A single invocation covers the
available complete input generation; there is no 30-minute rehash/relaunch loop.
Every resumed bucket rehashes its actual inputs and output, even if timestamps
are unchanged. One fresh process per bucket limits retained allocator memory.
Workers never write global identity/status/publication files.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import datetime as dt
import os
import statistics
import time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from chessdb_integrity import fingerprint, unchanged
from month_archive import read_json, write_json, sha, file_lock
from eval_lookup128 import SCHEMA

VERSION = "chessdb-step1-input-bound-v2"
RECEIPT_VERSION = 2

def producer():
    here=Path(__file__).parent
    return {n:sha(here/n) for n in ("chessdb_lookup_run.py","eval_lookup_cdb.py","chessdb_integrity.py","month_archive.py","chessdb_publication.py","eval_lookup128.py")}

def context(base, db, menus, out, old=None, require_publication=False):
    roots={n:Path(p).resolve() for n,p in dict(base=base,db=db,menus=menus,out=out).items()}
    if any(roots["out"]==p or p in roots["out"].parents or roots["out"] in p.parents for n,p in roots.items() if n!="out"):
        raise ValueError("output must be disjoint from every source")
    if old is not None:
        old=Path(old).resolve()
        if old==roots["out"] or old in roots["out"].parents or roots["out"] in old.parents:
            raise ValueError("old and new output directories must be disjoint")
    paths={"base_manifest":roots["base"]/"lookup.json","db_meta":roots["db"]/"_build.meta.json",
           "db_manifest":roots["db"]/"_manifest.parquet","menus_params":roots["menus"]/"_params.json"}
    globals_={n:fingerprint(p) for n,p in paths.items()}
    bm=read_json(paths["base_manifest"]); mp=read_json(paths["menus_params"])
    if bm["schema"]!=SCHEMA: raise ValueError("base is not an eval-lookup-128-v1")
    if bm["source"]["build_meta_sha256"]!=globals_["db_meta"]["sha256"]:
        raise ValueError("base lookup was not built from this eval DB")
    if bm["book_meta_sha256"]!=mp["book_meta_sha256"]:
        raise ValueError("ChessDB menus were built on another explorer book")
    dm=pq.read_table(paths["db_manifest"],columns=["bucket","sha256"]).to_pylist()
    if len({int(r["bucket"]) for r in dm})!=len(dm): raise ValueError("duplicate eval DB manifest bucket")
    db_hash={int(r["bucket"]):r["sha256"] for r in dm}
    menu_entries = None
    if require_publication:
        from chessdb_publication import MARKER, SCHEMA as MENU_SCHEMA
        p=roots["menus"]/MARKER
        publication=read_json(p)
        if publication.get("schema")!=MENU_SCHEMA or set(publication.get("buckets",{}))!={str(b) for b in range(512)}:
            raise ValueError("menus are not fully published")
        if publication["params"]["sha256"]!=globals_["menus_params"]["sha256"]:
            raise ValueError("menu publication params changed")
        menu_entries = {int(b): {k: r[k]["sha256"] for k in ("receipt", "positions")} for b,r in publication["buckets"].items()}
        paths["menus_publication"]=p; globals_["menus_publication"]=fingerprint(p)
    ident=dict(variant=VERSION,receipt_version=RECEIPT_VERSION,producer=producer(),
               runtime=dict(numpy=np.__version__,pyarrow=pa.__version__),
               inputs={n:r["sha256"] for n,r in globals_.items()},
               roots={n:str(p) for n,p in roots.items() if n!="out"},
               old_lookup=str(old) if old is not None else None)
    return dict(roots={n:str(p) for n,p in roots.items()},old=str(old) if old else None,
                base_meta=bm,db_hash=db_hash,identity=ident,menu_entries=menu_entries,
                globals={n:dict(path=str(paths[n]),fingerprint=r) for n,r in globals_.items()})

def bucket_input(ctx,b):
    roots={n:Path(p) for n,p in ctx["roots"].items()}
    rp=roots["menus"]/"_done"/f"bkt{b:03d}.json"
    if not rp.is_file(): raise ValueError(f"ChessDB bucket {b} not imported yet")
    ri=fingerprint(rp); r=read_json(rp)
    if r.get("bucket",b)!=b: raise ValueError("wrong menu receipt bucket")
    if ctx["menu_entries"] is not None:
        published = ctx["menu_entries"][b]
        if published["receipt"] != ri["sha256"] or published["positions"] != r["positions_sha256"]:
            raise ValueError(f"menu publication no longer matches bucket {b}")
    bp=roots["base"]/f"bkt{b:03d}.npy"; dp=roots["db"]/f"bkt{b:03d}.parquet"
    mp=roots["menus"]/"positions"/f"bkt{b:03d}.parquet"
    try:
        bh=ctx["base_meta"]["buckets"][bp.name]["sha256"]; dh=ctx["db_hash"][b]
    except KeyError as e: raise ValueError(f"missing source manifest bucket {b}") from e
    ids={"base":fingerprint(bp,bh),"db":fingerprint(dp,dh),
         "menu_receipt":ri,"menus":fingerprint(mp,r["positions_sha256"])}
    paths={"base":bp,"db":dp,"menu_receipt":rp,"menus":mp}
    unchanged(rp,ri)
    return paths,ids

def old_comparison(ctx,b,digest):
    if ctx["old"] is None:return None
    root=Path(ctx["old"]); receipt=root/"_done"/f"{b:03d}.json"
    if not receipt.exists():return None
    ri=fingerprint(receipt); old=read_json(receipt)
    oi=fingerprint(root/f"bkt{b:03d}.npy",old["sha256"])
    unchanged(receipt,ri)
    if oi["sha256"]!=digest:raise ValueError(f"old/new lookup bytes disagree at bucket {b}")
    return dict(path=str(root),sha256=digest,receipt_sha256=ri["sha256"],matched=True)

def hashes(ids):
    return {k:v["sha256"] for k,v in ids.items()}

def bucket_work(ctx,b):
    from eval_lookup_cdb import make_bucket
    pa.set_cpu_count(2)
    root=Path(ctx["roots"]["out"]); start=time.monotonic()
    with file_lock(root/"_bucket-locks"/f"{b:03d}.lock"):
        dest=root/f"bkt{b:03d}.npy"; ck=root/"_done"/f"{b:03d}.json"
        paths,ids=bucket_input(ctx,b)
        hash_s=time.monotonic()-start
        if ck.exists():
            rec=read_json(ck)
            if rec.get("receipt_version")!=RECEIPT_VERSION or rec.get("bucket")!=b or rec.get("identity")!=ctx["identity"]:
                raise ValueError(f"legacy or incompatible receipt for bucket {b}")
            if hashes(rec["inputs"])!=hashes(ids):
                raise ValueError(f"input generation changed at bucket {b}; use a new output directory")
            oi=fingerprint(dest,rec["sha256"])
            rec.update(inputs=ids,output=oi)
        else:
            values,stats=make_bucket(paths["base"],paths["db"],paths["menus"],b)
            for n,p in paths.items():unchanged(p,ids[n])
            tmp=dest.with_name(dest.name+f".{os.getpid()}.tmp")
            with tmp.open("wb") as f:
                np.save(f,values,allow_pickle=False); f.flush(); os.fsync(f.fileno())
            tmp.replace(dest)
            oi=fingerprint(dest)
            rec=dict(**stats,bucket=b,receipt_version=RECEIPT_VERSION,identity=ctx["identity"],
                     inputs=ids,output=oi,sha256=oi["sha256"])
        comparison=old_comparison(ctx,b,rec["sha256"])
        if comparison is not None:rec["old_comparison"]=comparison
        for n,p in paths.items():unchanged(p,ids[n])
        rec["last_verified_utc"]=dt.datetime.now(dt.timezone.utc).isoformat()
        rec["seconds"]=time.monotonic()-start; rec["input_hash_seconds"]=hash_s
        try:
            import psutil
            mem=psutil.Process().memory_info()
            rec["peak_rss_bytes"]=getattr(mem,"peak_wset",mem.rss)
        except ImportError:pass
        write_json(ck,rec)
        return rec

def receipt_ids(root):
    ids=set()
    for p in (root/"_done").glob("*.json"):
        if not p.stem.isdigit() or p.name!=f"{int(p.stem):03d}.json" or not 0<=int(p.stem)<512:
            raise ValueError(f"invalid checkpoint bucket filename: {p}")
        ids.add(int(p.stem))
    return ids

def verify_end(ctx,records):
    root=Path(ctx["roots"]["out"])
    for g in ctx["globals"].values():unchanged(Path(g["path"]),g["fingerprint"])
    for b,rec in records.items():
        ck=root/"_done"/f"{b:03d}.json"
        if read_json(ck)!=rec:raise ValueError(f"checkpoint changed during run: {b}")
        unchanged(root/f"bkt{b:03d}.npy",rec["output"])
        source=ctx["roots"]
        for n,p in dict(base=Path(source["base"])/f"bkt{b:03d}.npy",
                        db=Path(source["db"])/f"bkt{b:03d}.parquet",
                        menus=Path(source["menus"])/"positions"/f"bkt{b:03d}.parquet",
                        menu_receipt=Path(source["menus"])/"_done"/f"bkt{b:03d}.json").items():
            unchanged(p,rec["inputs"][n])

def run(base,db,menus,out,buckets=None,workers=1,old=None,require_publication=False):
    out=Path(out); out.mkdir(parents=True,exist_ok=True)
    wanted=list(range(512)) if buckets is None else sorted(set(buckets))
    if workers<1 or not wanted or any(not 0<=b<512 for b in wanted):raise ValueError("invalid workers/buckets")
    with file_lock(out/"_coordinator.lock"):
        ctx=context(base,db,menus,out,old,require_publication)
        ip=out/"_identity.json"
        if ip.exists():
            if read_json(ip)!=ctx["identity"]:raise ValueError("other inputs or code: use a new output directory")
        elif any((out/"_done").glob("*.json")):
            raise ValueError("unidentified existing receipts; do not invent provenance")
        write_json(ip,ctx["identity"])
        existing=receipt_ids(out)
        # If a partial invocation completes the last buckets, revalidate all before publication.
        if existing|set(wanted)==set(range(512)):wanted=list(range(512))
        prior=out/"lookup.json"
        if prior.exists():prior.unlink()  # own derived publication; restore only after current verification
        start=time.monotonic(); records={}; timings=[]
        def progress(state="running",error=None):
            elapsed=time.monotonic()-start
            pending=len(wanted)-len(records)
            eta=(statistics.median(timings)*pending/workers) if timings else None
            info=dict(state=state,pid=os.getpid(),workers=workers,completed=len(records),total=len(wanted),
                      elapsed_seconds=elapsed,estimated_remaining_seconds=eta,error=error)
            write_json(out/"_status.json",info)
        progress()
        try:
            # Set child thread counts before creating worker processes.
            for n in ("OMP_NUM_THREADS","OPENBLAS_NUM_THREADS","MKL_NUM_THREADS"):
                os.environ[n]="1"
            with cf.ProcessPoolExecutor(max_workers=workers,max_tasks_per_child=1) as pool:
                todo = iter(wanted)
                futures = {pool.submit(bucket_work,ctx,b): b for b in [next(todo) for _ in range(min(workers,len(wanted)))]}
                while futures:
                    finished, _ = cf.wait(futures, return_when=cf.FIRST_COMPLETED)
                    for f in finished:
                        b=futures.pop(f); rec=f.result(); records[b]=rec; timings.append(rec["seconds"])
                        progress()
                        print(f"bucket {b:03d} verified ({len(records)}/{len(wanted)}), {rec['seconds']:.1f}s, old_match={bool(rec.get('old_comparison'))}",flush=True)
                        nxt=next(todo,None)
                        if nxt is not None:futures[pool.submit(bucket_work,ctx,nxt)] = nxt
            verify_end(ctx,records)
            all_done=receipt_ids(out)
            if all_done==set(range(512)) and set(records)==all_done:
                files={p.name for p in out.glob("bkt*.npy")}
                if files!={f"bkt{b:03d}.npy" for b in range(512)}:raise ValueError("unexpected output bucket files")
                meta={k:v for k,v in ctx["base_meta"].items() if k!="buckets"}
                meta.update(variant=VERSION,identity=ctx["identity"],
                            buckets={f"bkt{b:03d}.npy":records[b] for b in sorted(records)})
                write_json(prior,meta)
                progress("verified")
            else:progress("partial-verified")
            return {f"bkt{b:03d}.npy":r for b,r in records.items()}
        except BaseException as e:
            progress("failed",f"{type(e).__name__}: {e}")
            raise

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for n in ("base","eval-db","menus","out"):p.add_argument("--"+n,type=Path,required=True)
    p.add_argument("--workers",type=int,default=2)
    p.add_argument("--buckets",type=int,nargs="+")
    p.add_argument("--old-lookup",type=Path)
    a=p.parse_args()
    run(a.base,a.eval_db,a.menus,a.out,a.buckets,a.workers,a.old_lookup,True)

if __name__=="__main__":main()
