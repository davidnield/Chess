"""Exact full-key lookup with conservative Parquet row-group pruning.

Uses the signed low-half min/max metadata only to exclude impossible groups.
Missing statistics always retain a group. The final join compares both halves;
a low-half collision cannot return another board. No new large index is built.
"""
from bisect import bisect_left
from pathlib import Path
import pyarrow.parquet as pq
import polars as pl
from consumer_keys import PAIR

class KeyedParquet:
    def __init__(self,path,column):
        self.path=str(Path(path).resolve());self.column=column
        self.partitioned=None
        if Path(path).suffix==".json":
            from .count_pool_reader import CountPoolReader
            self.partitioned=CountPoolReader(path,column)
            self.stats=self.partitioned.stats
            return
        self.reader=pq.ParquetFile(self.path)
        if pl.read_parquet_schema(self.path)[column]!=PAIR:
            self.reader.close();raise ValueError(f"expected full struct keys: {self.path}")
        meta=self.reader.metadata
        self.groups=[]
        for i in range(meta.num_row_groups):
            group=meta.row_group(i);bounds=None
            for j in range(group.num_columns):
                c=group.column(j)
                if c.path_in_schema==column+".lo":
                    s=c.statistics
                    if s is not None and s.has_min_max: bounds=(s.min,s.max)
                    break
            self.groups.append((bounds,group.num_rows,
                                sum(group.column(j).total_compressed_size for j in range(group.num_columns))))
        self.stats=dict(queries=0,total_groups=0,selected_groups=0,selected_rows=0,
                        selected_compressed_bytes=0,total_compressed_bytes=0,unpruned_queries=0)

    def select_groups(self,keys):
        lows=sorted({int(k["lo"]) for k in keys})
        if not lows:return []
        result=[]
        for i,(bounds,_,_) in enumerate(self.groups):
            if bounds is None:
                result.append(i);continue
            low,high=bounds;j=bisect_left(lows,low)
            if j<len(lows) and lows[j]<=high:result.append(i)
        return result

    def fetch(self,db,keys):
        if self.partitioned is not None:return self.partitioned.fetch(db,keys)
        keys=list(keys);groups=self.select_groups(keys)
        self.stats["queries"]+=1;self.stats["total_groups"]+=len(self.groups)
        self.stats["selected_groups"]+=len(groups)
        self.stats["selected_rows"]+=sum(self.groups[i][1] for i in groups)
        self.stats["selected_compressed_bytes"]+=sum(self.groups[i][2] for i in groups)
        self.stats["total_compressed_bytes"]+=sum(g[2] for g in self.groups)
        if not groups:
            return pl.from_arrow(self.reader.schema_arrow.empty_table())
        db.register("selective_keys",pl.DataFrame({"h":keys},schema={"h":PAIR}).to_arrow())
        batch=None
        try:
            if len(groups)==len(self.groups):
                # For tiny files or broad requests the native reader avoids Arrow overhead.
                self.stats["unpruned_queries"]+=1
                return pl.from_arrow(db.execute(f"SELECT s.* FROM read_parquet(?) s "
                     f"SEMI JOIN selective_keys k ON s.{self.column}=k.h",[self.path]).to_arrow_table())
            import pyarrow as pa
            batch=pa.RecordBatchReader.from_batches(self.reader.schema_arrow,
                     self.reader.iter_batches(batch_size=65536,row_groups=groups,use_threads=False))
            db.register("selective_rows",batch)
            return pl.from_arrow(db.execute(f"SELECT s.* FROM selective_rows s "
                     f"SEMI JOIN selective_keys k ON s.{self.column}=k.h").to_arrow_table())
        finally:
            if batch is not None:db.unregister("selective_rows");batch.close()
            db.unregister("selective_keys")

    def close(self):
        if self.partitioned is not None:self.partitioned.close()
        else:self.reader.close()
