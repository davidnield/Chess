"""Exact bucket parity, including 128-bit twins, floors, loops and terminal guards."""
import functools, json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import duckdb
import pyarrow.parquet as pq
import pool_from_book as pool
from _test_pool128 import make_inputs

class BucketOptimizations(unittest.TestCase):
    def test_full_bucket_equivalence(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);book,lookup=make_inputs(root)
            args=['pool','--book',str(book),'--work',str(root/'source'),
                  '--out-dir',str(root/'out'),'--eval-lookup128',str(lookup),
                  '--events','Rapid','--elo-bands','1800','2000','--max-ply','5',
                  '--min-games','1','--tag','optimizations','--phase','arrivals',
                  '--tmp-dir',str(root/'spill')]
            with patch.object(pool,'N_BUCKETS',2),patch.object(pool,'GROUP_SIZE',1), \
                 patch.object(pool,'_pool_run',lambda fn,tasks,*a:[fn(t) for t in tasks]),patch('sys.argv',args):
                pool.main()
                for floor in (1,5,1000):
                    for bucket in (0,1):
                        results=[];paths=[]
                        for optimized in (False,True):
                            work=root/f'f{floor}-b{bucket}-opt{optimized}';(work/'buckets').mkdir(parents=True)
                            task=(bucket,str(book),['Rapid'],[1800,2000],5,floor,str(lookup),str(work),str(root/'source'/'arrivals'),[],None,2,'256MB',str(work/'spill'),False)
                            _,st=pool._bucket_task(task,wide=True,sparse_endings=optimized,release_edges=optimized,edge_partitions=4 if optimized else 1)
                            results.append({k:v for k,v in st.items() if k not in ('secs','eval_lookup_s')});paths.append(pool.bucket_paths(work,bucket))
                        self.assertEqual(*results)
                        for kind in ('edges','aux'):
                            left,right=[pq.read_table(p[kind]) for p in paths]
                            self.assertEqual(left.schema,right.schema)
                            key=lambda r:json.dumps(r,sort_keys=True)
                            self.assertEqual(sorted(left.to_pylist(),key=key),sorted(right.to_pylist(),key=key))

    def test_sparse_summary_omits_only_all_zero_cells(self):
        with tempfile.TemporaryDirectory() as td:
            con=duckdb.connect()
            con.execute('CREATE TABLE en(x BIGINT,p INTEGER,white_wins BIGINT,draws BIGINT,black_wins BIGINT,total BIGINT)')
            con.execute('INSERT INTO en VALUES (1,1,0,0,0,0),(2,1,0,2,0,2),(2,3,0,0,0,0),(3,2,1,0,0,0)')
            pool.split_endings(con,td,0,[],[],5,sparse=True)
            self.assertEqual(con.execute('SELECT x,term_other_white_wins,term_other_draws FROM terminal ORDER BY x').fetchall(),[(2,0,2),(3,1,0)])
            self.assertEqual(con.execute('SELECT COUNT(*) FROM en').fetchone()[0],4)
            con.execute('INSERT INTO en VALUES (4,1,-1,0,0,-1)')
            with self.assertRaisesRegex(RuntimeError,'NEGATIVE'):pool.check_ended_nonnegative(con,'sparse guard')
            con.close()

    def test_classified_excess_still_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);book,_=make_inputs(root);con=duckdb.connect()
            con.execute('CREATE TABLE en(x STRUCT(lo BIGINT, hi BIGINT),p INTEGER,white_wins BIGINT,draws BIGINT,black_wins BIGINT,total BIGINT)')
            with self.assertRaisesRegex(ValueError,'terminal counts exceed'):
                pool.split_endings(con,book,1,['Rapid'],[1800],5,True,sparse=True)
            con.close()

if __name__=='__main__':unittest.main()
