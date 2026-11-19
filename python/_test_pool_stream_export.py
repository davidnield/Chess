"""Bounded export parity, full-key uniqueness, and interrupted publication."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import duckdb
import polars as pl
import pyarrow.parquet as pq
import pool_from_book as pool


class ExportTests(unittest.TestCase):
    def test_exact_types_order_counts_and_recipe(self):
        for wide in (False, True):
            with self.subTest(wide=wide), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                key = (lambda lo, hi: {'lo': lo, 'hi': hi}) if wide else (lambda lo, hi: lo * 10 + hi)
                rows = [dict(parent_hash=key(lo, hi), child_hash=key(hi, lo), move_san=san,
                             parent_epd='fixture', ply=ply, white_wins=w, draws=d, black_wins=b,
                             total=w+d+b) for lo,hi,san,ply,w,d,b in
                        [(7,2,'e4',1,5,3,2),(-9,4,'d4',3,2,1,2),(7,1,'e4',5,7,0,1),
                         (-9,4,'c4',3,0,2,3),(7,2,'Nf3',1,1,1,1)]]
                src=pl.DataFrame(rows).with_columns(pl.col('ply').cast(pl.Int32)).select([n for n,_ in pool.POOL_SCHEMA[:-3]])
                for i,frame in enumerate((src[:2],src[2:],src[:0])):
                    frame.write_parquet(root/f'{i}.parquet')
                expected=(src.with_columns(pl.lit('Pooled').alias('event'),pl.lit(0,dtype=pl.Int64).alias('elo_band'),
                    ((pl.col('white_wins')+.5*pl.col('draws'))/pl.col('total')).alias('white_score_avg'))
                    .select([n for n,_ in pool.POOL_SCHEMA]).sort(['parent_hash','move_san']))
                con=duckdb.connect(config={'threads':1,'memory_limit':'32MB'})
                try:
                    source=str(root/'*.parquet').replace('\\','/').replace("'","''")
                    query=f"""SELECT *, 'Pooled' AS event, 0::BIGINT AS elo_band,
                        (white_wins::DOUBLE+.5::DOUBLE*draws::DOUBLE)/total::DOUBLE AS white_score_avg
                        FROM read_parquet('{source}') ORDER BY parent_hash,move_san"""
                    schema=pool.output_schema(pool.POOL_SCHEMA,wide)
                    recipe={'pool_recipe':'test-recipe'} if wide else None
                    out=root/'out.parquet'
                    n=pool.write_sorted_export(con,query,out,schema,recipe,batch_rows=2)
                    self.assertEqual(n,5)
                    self.assertEqual(pl.read_parquet(out).to_dicts(),expected.to_dicts())
                    self.assertEqual(pool._schema_list(out),schema)
                    self.assertTrue(all(f.nullable for f in pq.read_schema(out)))
                    if wide:self.assertEqual(pq.read_metadata(out).metadata[b'pool_recipe'],b'test-recipe')
                    self.assertEqual(pq.ParquetFile(out).num_row_groups,3)
                    empty=root/'empty.parquet'
                    # Enumerate just the source shards; the first output now exists too.
                    empty_query="SELECT * FROM read_parquet('"+str(out).replace('\\','/')+"') LIMIT 0"
                    self.assertEqual(pool.write_sorted_export(con,empty_query,empty,schema,recipe),0)
                    self.assertEqual(pool._schema_list(empty),schema)
                finally:con.close()

    def test_full_key_duplicates_across_batches_and_atomic_failure(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);con=duckdb.connect()
            columns=[('position_hash','struct<lo: int64, hi: int64>')]
            try:
                twin="SELECT struct_pack(lo := 3::BIGINT, hi := i::BIGINT) AS position_hash FROM range(2) t(i) ORDER BY position_hash"
                out=root/'aux.parquet'
                self.assertEqual(pool.write_sorted_export(con,twin,out,columns,unique='position_hash',batch_rows=1),2)
                before=out.read_bytes()
                dup="SELECT struct_pack(lo := 3::BIGINT, hi := 1::BIGINT) AS position_hash FROM range(2) ORDER BY position_hash"
                with self.assertRaisesRegex(ValueError,'duplicate'):
                    pool.write_sorted_export(con,dup,out,columns,unique='position_hash',batch_rows=1)
                self.assertEqual(out.read_bytes(),before)
                self.assertFalse(Path(str(out)+'.tmp').exists())
                with patch.object(pool,'check_export',side_effect=RuntimeError('injected failure')):
                    with self.assertRaisesRegex(RuntimeError,'injected'):
                        pool.write_sorted_export(con,twin,out,columns)
                self.assertEqual(out.read_bytes(),before)
                self.assertFalse(Path(str(out)+'.tmp').exists())
                null="SELECT struct_pack(lo := NULL::BIGINT, hi := 1::BIGINT) AS position_hash"
                with self.assertRaisesRegex(ValueError,'null'):
                    pool.write_sorted_export(con,null,out,columns,unique='position_hash')
            finally:con.close()


if __name__=='__main__':unittest.main()
