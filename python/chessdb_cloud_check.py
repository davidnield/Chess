"""Verify cloud menu bytes and reproduce the 16-bucket top-PV comparison.

python chessdb_cloud_check.py --menus PATH --db PATH --keys PATH --out NEW_JSON
Checks every cloud bucket checksum/row count, then compares cloud cp/mate/depth
with the accepted eval DB for the deterministic buckets 5,37,...,485.
"""
import argparse
import concurrent.futures as cf
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
from chessdb_integrity import exact_buckets,fingerprint,unchanged
from month_archive import read_json,write_json,sha

def check(menus,db,keys,out):
    menus,db,keys,out=map(Path,(menus,db,keys,out))
    if out.exists():raise ValueError("output exists; choose a new receipt path")
    names=[f"bkt{b:03d}" for b in range(512)]
    exact_buckets(menus/"cloud_done","*.json",[n+".json" for n in names])
    exact_buckets(menus/"cloud_moves","*.parquet",[n+".parquet" for n in names])
    def one(b):
        rp=menus/"cloud_done"/f"bkt{b:03d}.json";ri=fingerprint(rp);r=read_json(rp)
        if r["bucket"]!=b:raise ValueError("wrong cloud receipt bucket")
        p=menus/"cloud_moves"/f"bkt{b:03d}.parquet";data=fingerprint(p,r["sha256"])
        if pq.ParquetFile(p).metadata.num_rows!=r["moves"]:raise ValueError("cloud row count mismatch")
        unchanged(rp,ri);unchanged(p,data)
        return dict(bucket=b,receipt=ri,data=data,moves=r["moves"],positions=r["positions"])
    with cf.ThreadPoolExecutor(max_workers=4) as pool:cloud=list(pool.map(one,range(512)))
    print("all 512 cloud files match original receipts",flush=True)
    dbm=pq.read_table(db/"_manifest.parquet",columns=["bucket","sha256"]).to_pylist()
    hashes={int(r["bucket"]):r["sha256"] for r in dbm}
    c=duckdb.connect();c.execute("SET threads=2");c.execute("SET memory_limit='2GB'")
    rows=[]
    for b in range(5,512,32):
        cp=menus/"cloud_moves"/f"bkt{b:03d}.parquet";ep=db/f"bkt{b:03d}.parquet";kp=keys/f"bkt{b:03d}.parquet"
        ei=fingerprint(ep,hashes[b]);ki=fingerprint(kp)
        n,s,miss=c.execute("""
        WITH ev AS (SELECT DISTINCT position_hash AS lo,position_hash_hi AS hi,cloud_cp,cloud_mate,cloud_depth,
             split_part(cloud_line,' ',1) AS uci FROM read_parquet(?) WHERE cloud_depth IS NOT NULL AND NOT hash_ambiguous),
        evk AS (SELECT ev.* FROM ev SEMI JOIN read_parquet(?) kk USING(lo,hi))
        SELECT count(*),count(*) FILTER (c.cp_white IS NOT DISTINCT FROM evk.cloud_cp AND
          c.mate_white IS NOT DISTINCT FROM evk.cloud_mate AND c.depth=evk.cloud_depth),
          count(*) FILTER (c.lo IS NULL)
        FROM evk LEFT JOIN read_parquet(?) c USING(lo,hi,uci)
        """,[str(ep),str(kp),str(cp)]).fetchone()
        if n!=s or miss:raise ValueError(f"bucket {b}: total={n}, identical={s}, missing={miss}")
        unchanged(ep,ei);unchanged(kp,ki);unchanged(cp,cloud[b]["data"])
        rows.append(dict(bucket=b,total=n,identical=s,missing=miss,db_input=ei,key_input=ki))
        print(f"bucket {b:03d}: {s}/{n} identical; missing={miss}",flush=True)
    result=dict(schema="cloud-menu-validation-v1",scope="all cloud byte checks; 16-bucket eval top-PV sample",
       producer_sha256=sha(Path(__file__)),db_manifest_sha256=sha(db/"_manifest.parquet"),
       cloud=cloud,comparisons=rows,total=sum(r["total"] for r in rows),identical=sum(r["identical"] for r in rows),
       moves=sum(r["moves"] for r in cloud),positions=sum(r["positions"] for r in cloud))
    write_json(out,result)
    print({k:result[k] for k in ("total","identical","moves","positions")},flush=True)
if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for n in ("menus","db","keys","out"):p.add_argument("--"+n,required=True,type=Path)
    a=p.parse_args();check(a.menus,a.db,a.keys,a.out)
