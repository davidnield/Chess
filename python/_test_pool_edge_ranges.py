import importlib.util,json,sys,tempfile,unittest
from pathlib import Path
import duckdb,pyarrow as pa,pyarrow.parquet as pq
import pool_from_book as p

class RangeTests(unittest.TestCase):
    def fixture(self,root,null=None):
        lows=[-(1<<63),-(1<<62),-1,0,0,1,(1<<62),(1<<63)-1]
        fields={'parent_hash':lows,'parent_hash_hi':[1,2,3,-7,7,8,9,10],
                'child_hash':lows[:],'child_hash_hi':[12]*8,'ply':[1]*8}
        if null:fields[null][0]=None
        f=root/'input.parquet';pq.write_table(pa.table({k:pa.array(v,type=pa.int64()) for k,v in fields.items()}),f);return f
    def test_full_signed_domain_and_high_half_twins(self):
        with tempfile.TemporaryDirectory() as td:
            f=self.fixture(Path(td));con=duckdb.connect();cols='parent_hash, child_hash, ply'
            expected=con.execute('SELECT * FROM '+p._read([str(f)],cols,'ply <= 2',True)).fetchall()
            rows=[]
            for i in range(8):
                lo=-(1<<63)+i*(1<<61);hi=lo+(1<<61)-1
                rows+=con.execute('SELECT * FROM '+p._read([str(f)],cols,'ply <= 2',True,(lo,hi))).fetchall()
            key=lambda x:json.dumps(x,sort_keys=True)
            self.assertEqual(sorted(expected,key=key),sorted(rows,key=key));con.close()
    def test_invalid_halves_are_not_filtered_away(self):
        for col in ('parent_hash','parent_hash_hi','child_hash','child_hash_hi'):
            with self.subTest(column=col),tempfile.TemporaryDirectory() as td:
                f=self.fixture(Path(td),null=col);con=duckdb.connect()
                with self.assertRaisesRegex(duckdb.InvalidInputException,'incomplete 128-bit'):
                    con.execute('SELECT * FROM '+p._read([str(f)],'parent_hash, child_hash','ply <= 2',True,(-(1<<63),-(1<<63)+(1<<61)-1))).fetchall()
                con.close()

if __name__=='__main__':unittest.main()
