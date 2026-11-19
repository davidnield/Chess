"""Bounded lookup benchmark; never launches builds or changes a source pool."""
import argparse
import json
from pathlib import Path
import random
import time
import duckdb
import polars as pl
from polars.testing import assert_frame_equal
from consumer_keys import PAIR
from .keyed_parquet import KeyedParquet
from .stages import atomic_json,file_sha

def benchmark(path,column,batch_size=16,repeats=3):
    reader=KeyedParquet(path,column);groups=len(reader.groups)
    if not groups:raise ValueError("benchmark needs a nonempty source")
    keys=[];rng=random.Random(1937)
    for i in rng.sample(range(groups),min(batch_size,groups)):
        rows=reader.reader.read_row_group(i,columns=[column]).column(column).to_pylist()
        if rows:keys.append(rows[rng.randrange(len(rows))])
    # Tiny files can have only one group. Fill the requested batch from that group.
    if len(keys)<batch_size:
        rows=reader.reader.read_row_group(0,columns=[column]).column(column).to_pylist()
        keys.extend(rng.sample(rows,min(batch_size-len(keys),len(rows))))
    values={"indexed":[],"full_join":[]}
    with duckdb.connect(config={"threads":2,"memory_limit":"2GB"}) as db:
        db.register("benchmark_keys",pl.DataFrame({"h":keys},schema={"h":PAIR}).to_arrow())
        for repeat in range(repeats):
            got={}
            # Alternate order. This is an OS-warm microbenchmark, not cold-disk timing.
            for name in (("indexed","full_join") if repeat%2 else ("full_join","indexed")):
                t=time.perf_counter()
                frame=(reader.fetch(db,keys) if name=="indexed" else
                       pl.from_arrow(db.execute(f"SELECT s.* FROM read_parquet(?) s SEMI JOIN benchmark_keys k ON s.{column}=k.h",[str(path)]).to_arrow_table()))
                values[name].append(time.perf_counter()-t);got[name]=frame
            # Ignore ordering, retain exact dtypes and values.
            assert_frame_equal(got["indexed"],got["full_join"],check_row_order=False)
    result=dict(source=str(Path(path).resolve()),source_sha256=file_sha(path),
                bytes=Path(path).stat().st_size,rows=reader.reader.metadata.num_rows,
                requested=len(keys),returned=len(got["indexed"]),seconds=values,
                selected=reader.stats,exact_equal=True,
                caveat="OS-warm bounded lookup benchmark; not a production speed estimate")
    reader.close();return result

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source",type=Path,required=True)
    p.add_argument("--column",default="parent_hash",choices=["parent_hash","position_hash"])
    p.add_argument("--batch-size",type=int,default=16)
    p.add_argument("--repeats",type=int,default=3)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    if a.batch_size<1 or a.repeats<1:p.error("positive batch size and repeat count required")
    result=benchmark(a.source,a.column,a.batch_size,a.repeats)
    atomic_json(a.out,result);print(json.dumps(result,indent=2))

if __name__=="__main__":main()
