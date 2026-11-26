"""Complete compact policy-count pools, independent of evaluation/ending sidecars.

Python orchestrates the existing native DuckDB aggregation engine. Every source
position/move through the declared cap is materialized; this is not a selective
explorer query cache. Finished ranges survive interruption, and publication is
refused until every range of all 512 buckets has a checked receipt.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from .spec import digest
from .stages import atomic_json, file_sha

SCHEMA = "complete-policy-counts-v1"
SUMS = ("white_wins", "draws", "black_wins", "total")


def stamp(path):
    p = Path(path); s = p.stat()
    return dict(path=str(p.resolve()), bytes=s.st_size, mtime_ns=s.st_mtime_ns)


def freeze_source(book):
    """Capture the full published inventory, including evidence for absent slices."""
    from pool_from_book import check_cap, check_population
    book = Path(book).resolve()
    meta = json.loads((book / "_book.meta.json").read_text())
    params = json.loads((book / "_merge_params.json").read_text())
    if meta["schema"] != "book-v4" or params["input_params"]["key_bits"] != 128:
        raise ValueError("requires the published compact full-key explorer")
    check_cap(book, 30)
    check_population(book, ["Blitz", "Rapid", "Classical"], [0,1000,1200,1400,1600,1800,2000,2200,2500])
    paths = sorted((book / "ps").glob("event=*/elo_band=*/bkt*.parquet"))
    if len(paths) != meta["totals"]["files"] or sum(p.stat().st_size for p in paths) != meta["totals"]["bytes"]:
        raise ValueError("published explorer inventory does not match totals")
    return dict(book=str(book), manifests={n:file_sha(book/n) for n in
        ("_book.meta.json", "_merge_params.json", "_BOOK.DONE", "_slices.parquet")},
        files={p.relative_to(book).as_posix():stamp(p) for p in paths})


def inputs(source, recipe, bucket):
    book = Path(source["book"])
    for name, sha in source["manifests"].items():
        if file_sha(book/name) != sha: raise ValueError("explorer publication changed")
    selected=[]
    for event in recipe["events"]:
        for band in recipe["elo_bands"]:
            rel=f"ps/event={event}/elo_band={band}/bkt{bucket:03d}.parquet"
            path=book/rel; expected=source["files"].get(rel)
            if expected is None:
                if path.exists():raise ValueError("source inventory gained a file")
            else:
                if not path.is_file() or stamp(path)!=expected:raise ValueError("source missing or changed: "+rel)
                selected.append(str(path))
    return selected


def sql_literal(value): return "'" + str(value).replace("\\", "/").replace("'", "''") + "'"


def aggregate(db, files, output, *, cap, bucket, part, parts):
    """One disjoint signed-low range; full 128-bit keys always remain in GROUP BY."""
    from pool_from_book import _read
    width=(1<<64)//parts; low=-(1<<63)+part*width; high=low+width-1
    cols="parent_hash, move_san, child_hash, ply, " + ", ".join(SUMS)
    src=_read(files,cols,f"ply <= {cap}",True,(low,high))
    expected=db.execute("SELECT " + ",".join(f"coalesce(sum({c}),0)" for c in SUMS) +
        ", count(*) FILTER (WHERE parent_hash IS NULL OR child_hash IS NULL OR move_san IS NULL OR ply IS NULL "
        "OR ply<1 OR total IS NULL OR white_wins IS NULL OR draws IS NULL OR black_wins IS NULL "
        "OR total<1 OR white_wins<0 OR draws<0 OR black_wins<0 OR total<>white_wins+draws+black_wins "
        f"OR ((parent_hash.lo%512)+512)%512<>{bucket}) FROM {src}").fetchone()
    if expected[-1]:raise ValueError("invalid source keys/counts/partition")
    db.execute(f"""CREATE TEMP TABLE counts AS SELECT parent_hash, move_san,
        MIN(child_hash) child_hash, MAX(child_hash) child_max, MIN(ply)::INTEGER ply,
        {', '.join(f'SUM({c})::BIGINT {c}' for c in SUMS)} FROM {src}
        GROUP BY parent_hash,move_san""")
    if db.execute("SELECT count(*) FROM counts WHERE child_hash<>child_max").fetchone()[0]:
        raise ValueError("source move has inconsistent full child keys")
    # Child XOR is lossless and compresses far better than storing a second random key.
    db.execute(f"""COPY (SELECT parent_hash,move_san,
        xor(parent_hash.lo,child_hash.lo) move_xor_lo,
        xor(parent_hash.hi,child_hash.hi) move_xor_hi,ply,{', '.join(SUMS)}
        FROM counts ORDER BY parent_hash,move_san)
        TO {sql_literal(output)} (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 6,
                                  ROW_GROUP_SIZE 65536)""")
    actual=db.execute("SELECT count(*),"+ ",".join(f"coalesce(sum({c}),0)" for c in SUMS)+
        " FROM read_parquet(?)",[str(output)]).fetchone()
    db.execute("DROP TABLE counts")
    if list(actual[1:])!=list(expected[:-1]):raise ValueError("output count conservation failed")
    return dict(rows=actual[0],mass=dict(zip(SUMS,actual[1:])),low=low,high=high)


def check_receipt(root, receipt, identity):
    r=json.loads(Path(receipt).read_text())
    if r["identity"]!=identity:raise ValueError("range recipe changed")
    p=Path(root)/r["file"]
    if p.stat().st_size!=r["bytes"] or file_sha(p)!=r["sha256"]:
        raise ValueError("completed range changed")
    return r


def run_bucket(cfg, recipe, bucket):
    from pool_from_book import _duck
    root=Path(recipe["output"]);root.mkdir(parents=True,exist_ok=True)
    identity=digest(dict(recipe=recipe,source=cfg["source"],producer=cfg["producer"]))
    files=inputs(cfg["source"],recipe,bucket)
    for part in range(cfg["parts"]):
        name=f"b{bucket:03d}-r{part:02d}.parquet"; path=root/name
        receipt=root/(name+".receipt.json")
        unit=digest(dict(identity=identity,bucket=bucket,part=part,parts=cfg["parts"]))
        if receipt.exists():check_receipt(root,receipt,unit);continue
        if Path(cfg["stop"]).exists():return False
        # Range boundaries make both interruption and space exhaustion bounded.
        if shutil.disk_usage(root).free < recipe["reserve_bytes"]+cfg["range_headroom_bytes"]:
            raise RuntimeError("output disk reserve reached")
        if shutil.disk_usage(cfg["temp"]).free < cfg["temp_reserve_bytes"]+cfg["spill_bytes"]:
            raise RuntimeError("spill disk reserve reached")
        tmp=path.with_suffix(".parquet.tmp")
        tmp.unlink(missing_ok=True)  # only this producer's unreceipted range
        t=time.monotonic()
        db=_duck(cfg["threads"],cfg["memory"],Path(cfg["temp"]))
        try:
            db.execute(f"SET max_temp_directory_size='{cfg['spill_bytes']}B'")
            stats=aggregate(db,files,tmp,cap=recipe["max_ply"],bucket=bucket,part=part,parts=cfg["parts"])
        finally:db.close()
        inputs(cfg["source"],recipe,bucket)  # refuses input drift during computation
        tmp.replace(path)
        atomic_json(receipt,dict(identity=unit,file=name,bucket=bucket,part=part,
            bytes=path.stat().st_size,sha256=file_sha(path),seconds=time.monotonic()-t,**stats))
        print(json.dumps(dict(pool=recipe["id"],bucket=bucket,part=part,**stats)),flush=True)
    return True


def publish(cfg, recipe):
    root=Path(recipe["output"])
    identity=digest(dict(recipe=recipe,source=cfg["source"],producer=cfg["producer"]))
    records=[]
    for bucket in range(512):
        inputs(cfg["source"],recipe,bucket)
        for part in range(cfg["parts"]):
            name=f"b{bucket:03d}-r{part:02d}.parquet"
            r=check_receipt(root,root/(name+".receipt.json"),digest(dict(
                identity=identity,bucket=bucket,part=part,parts=cfg["parts"])))
            records.append(r)
    manifest=dict(schema=SCHEMA,complete=True,key_bits=128,min_games=1,
        max_ply=recipe["max_ply"],population={k:recipe[k] for k in ("events","elo_bands")},
        recipe=recipe,source_identity=digest(cfg["source"]),identity=identity,
        buckets=512,parts=cfg["parts"],files=records,
        scope="all observed moves; policy counts only; no endings, evals or candidate EPDs")
    atomic_json(root/"pool.json.meta.json",{k:v for k,v in manifest.items() if k!="files"})
    atomic_json(root/"pool.json",manifest)  # completion published last


def capacity(cfg, recipe):
    root=Path(recipe["output"])
    sizes=[];done=0
    for b in range(512):
        receipts=list(root.glob(f"b{b:03d}-r*.parquet.receipt.json"))
        if len(receipts)==cfg["parts"]:
            sizes.append(sum(json.loads(p.read_text())["bytes"] for p in receipts));done+=1
    if len(sizes)<len(cfg["pilot_buckets"]):raise ValueError("capacity pilot incomplete")
    remaining=int(max(sizes)*1.25*(512-done))
    free=shutil.disk_usage(root).free
    return dict(completed=done,remaining_bytes_estimate_with_margin=remaining,free_bytes=free,
        reserve_bytes=recipe["reserve_bytes"],fits=free>=remaining+recipe["reserve_bytes"])


def control(cfg):
    """Compare a real range against a previously accepted, full-schema adult pool."""
    from pool_from_book import _duck
    c=cfg["control"];path=Path(c["output"]);path.parent.mkdir(parents=True,exist_ok=True)
    reference=Path(c["reference"])
    if file_sha(reference)!=c["sha256"]:raise ValueError("accepted reference changed")
    recipe=dict(events=["Blitz","Rapid","Classical"],elo_bands=[1800,2000,2200,2500])
    files=inputs(cfg["source"],recipe,c["bucket"])
    db=_duck(cfg["threads"],cfg["memory"],Path(cfg["temp"]))
    try:
        db.execute(f"SET max_temp_directory_size='{cfg['spill_bytes']}B'")
        path.unlink(missing_ok=True)
        result=aggregate(db,files,path,cap=30,bucket=c["bucket"],part=0,parts=cfg["parts"])
        db.execute("CREATE TEMP VIEW candidate AS SELECT parent_hash,move_san, "
            "struct_pack(lo:=xor(parent_hash.lo,move_xor_lo),hi:=xor(parent_hash.hi,move_xor_hi)) child_hash, "
            f"ply,{','.join(SUMS)} FROM read_parquet({sql_literal(path)})")
        db.execute(f"CREATE TEMP VIEW reference AS SELECT parent_hash,move_san,child_hash,ply,{','.join(SUMS)} "
            f"FROM read_parquet({sql_literal(reference)}) WHERE parent_hash.lo BETWEEN {result['low']} AND {result['high']}")
        differences=[db.execute(f"SELECT count(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b})").fetchone()[0]
            for a,b in [("candidate","reference"),("reference","candidate")]]
        if differences!=[0,0]:raise ValueError("real-data reference parity failed: "+str(differences))
        inputs(cfg["source"],recipe,c["bucket"])
        atomic_json(path.with_suffix(".verified.json"),dict(differences=differences,rows=result["rows"],
            reference=c,source_identity=digest(cfg["source"]),producer=cfg["producer"],sha256=file_sha(path)))
    finally:db.close()


def run(cfg_path):
    cfg=json.loads(Path(cfg_path).read_text());status=Path(cfg["status"])
    for path,h in cfg["producer"].items():
        if file_sha(path)!=h:raise ValueError("frozen producer changed")
    def state(**kw):atomic_json(status,dict(utc=dt.datetime.now(dt.timezone.utc).isoformat(),**kw))
    def unit(recipe,bucket):
        state(state="running",pool=recipe["id"],bucket=bucket)
        result=subprocess.run([sys.executable,"-B","-u","-m","sweep.count_pool","--config",str(cfg_path),
            "--bucket",str(bucket),"--pool",recipe["id"]],check=False)
        if result.returncode:raise RuntimeError(f"bucket child failed: {result.returncode}")
        return not Path(cfg["stop"]).exists()
    # No second queue may concurrently mutate these publications.
    import msvcrt
    lock=open(str(status)+".lock","a+b")
    if not lock.tell():lock.write(b"0");lock.flush()
    lock.seek(0);msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
    try:
        state(state="reference-check")
        subprocess.run([sys.executable,"-B","-u","-m","sweep.count_pool","--config",str(cfg_path),"--control"],check=True)
        for bucket in cfg["pilot_buckets"]:
            for recipe in cfg["pools"]:
                if not unit(recipe,bucket):state(state="paused");return
        checks={r["id"]:capacity(cfg,r) for r in cfg["pools"]}
        atomic_json(status.with_name("capacity.json"),checks)
        if not all(c["fits"] for c in checks.values()):
            state(state="capacity-hold",capacity=checks);return
        for recipe in cfg["pools"]:
            for bucket in range(512):
                if bucket in cfg["pilot_buckets"]:continue
                if not unit(recipe,bucket):state(state="paused");return
            state(state="verifying",pool=recipe["id"])
            publish(cfg,recipe)
        state(state="complete",pools=[r["output"] for r in cfg["pools"]])
    except BaseException as e:
        state(state="failed",error=str(e));raise
    finally:lock.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config",type=Path,required=True)
    p.add_argument("--bucket",type=int);p.add_argument("--pool")
    p.add_argument("--control",action="store_true")
    a=p.parse_args()
    if a.control:control(json.loads(a.config.read_text()))
    elif a.bucket is None:run(a.config)
    else:
        cfg=json.loads(a.config.read_text())
        if not 0<=a.bucket<512:raise ValueError("bucket out of range")
        run_bucket(cfg,next(r for r in cfg["pools"] if r["id"]==a.pool),a.bucket)


if __name__=="__main__":main()
