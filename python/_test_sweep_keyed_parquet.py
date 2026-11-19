"""Exact lookup, low-half collisions, boundary duplicates and absent metadata."""
from pathlib import Path
import tempfile
import unittest
import duckdb
import polars as pl
import pyarrow.parquet as pq
from polars.testing import assert_frame_equal
from consumer_keys import PAIR
from sweep.keyed_parquet import KeyedParquet

class Lookup(unittest.TestCase):
    def test_group_pruning_matches_full_join_with_collisions_and_missing_statistics(self):
        with tempfile.TemporaryDirectory() as td,duckdb.connect(config={"threads":1}) as db:
            root=Path(td)
            # Same low half, different high halves; duplicate keys span group boundaries.
            keys=[dict(lo=lo,hi=hi) for lo in range(-10,11) for hi in (-2,2) for _ in range(3)]
            frame=pl.DataFrame({"parent_hash":keys,"payload":list(range(len(keys)))},schema={"parent_hash":PAIR,"payload":pl.Int64})
            for statistics in (True,False):
                path=root/(str(statistics)+".parquet")
                pq.write_table(frame.to_arrow(),path,row_group_size=7,write_statistics=statistics)
                reader=KeyedParquet(path,"parent_hash")
                for selected in ([dict(lo=-10,hi=2),dict(lo=0,hi=-2),dict(lo=10,hi=2)],
                                 [dict(lo=0,hi=9)],[],[dict(lo=999,hi=2)],
                                 [dict(lo=-10,hi=2),dict(lo=-10,hi=2)]):
                    db.register("requested",pl.DataFrame({"h":selected},schema={"h":PAIR}).to_arrow())
                    expected=pl.from_arrow(db.execute("SELECT s.* FROM read_parquet(?) s SEMI JOIN requested k ON s.parent_hash=k.h",[str(path)]).to_arrow_table())
                    actual=reader.fetch(db,selected)
                    assert_frame_equal(expected.sort("payload"),actual.sort("payload"))
                if statistics:self.assertLess(reader.stats["selected_groups"],reader.stats["total_groups"]//2)
                else:self.assertEqual(len(reader.select_groups([dict(lo=999,hi=2)])),len(reader.groups))
                reader.close()

if __name__=="__main__":unittest.main()
