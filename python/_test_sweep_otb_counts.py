"""Native/Python full-key parity and frozen OTB count invariants."""
import os,random,tempfile,unittest,logging
from pathlib import Path
import chess,pyarrow as pa,pyarrow.parquet as pq
from sweep.otb_counts import Native,reference,compare_reference,replay_table,connection,ledger_views,master_query,pool_query,MASTER_SCHEMA,POOL_SCHEMA,atomic_parquet
from pool_from_book import write_sorted_export
from sweep.verify_otb_counts import verify_ledger,verify_master,verify_pool,verify_sample
DLL=Path(os.environ.get('OTB_REPLAY_DLL','D:/chess/build/otb-counts-rust-20261119/release/otb_counts_replay.dll'))

class CountsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not DLL.exists():raise unittest.SkipTest('build the OTB replay DLL first')
        cls.native=Native(DLL)
        logging.getLogger('chess.pgn').setLevel(logging.CRITICAL)
    def test_parity_special_moves_comments_horizon(self):
        lines=['{source}\n\n1.e4 (1.d4 (1.c4)) e5 $1 2.Nf3!? Nc6 1-0',
            '1.f3 e5 2.g4 Qh4#','1.e4 a6 2.e5 d5 3.exd6',
            '1.a4 h5 2.a5 h4 3.a6 h3 4.axb7 hxg2 5.bxa8=Q gxh1=Q',
            '1.e4 e5 2.Nf3 Nc6 3.Bc4 Bc5 4.O-O Nf6',
            '1.d4 d5\n\n2.Nc3 Nf6', '1.e4 ; line comment\ne5']
        rng=random.Random(3721)
        for _ in range(100):
            b=chess.Board();sans=[]
            for ply in range(60):
                if b.is_game_over():break
                m=rng.choice(list(b.legal_moves));sans.append(b.san(m));b.push(m)
            lines.append(' '.join(sans))
        rows,lens=self.native.replay(lines,51)
        for i,line in enumerate(lines):
            with self.subTest(i=i):
                self.assertGreater(lens[i],0);compare_reference(rows[i],int(lens[i]),line,51)
    def test_bad_prefix_is_not_partial_evidence(self):
        lines=['1.e4 e5 2.Bh6','1.e4 --','{no score}','1.e4 (1.d4','1.e4 {unfinished']
        _,lens=self.native.replay(lines,51)
        self.assertEqual(lens.tolist(),[-2,-3,-4,-1,-1])
        rows,lens=self.native.replay(['1.e4 {unfinished'],1)
        self.assertEqual(lens.tolist(),[1]);compare_reference(rows[0],1,'1.e4',1)
    def test_mover_rating_roles_transpositions_and_exact_export(self):
        rows=[dict(game_id=i,role=role,white_elo=w,black_elo=b,movetext=text) for i,role,w,b,text in [
            (1,'training',1999,2200,'1.Nf3 Nf6 2.Nc3 Nc6'),
            (2,'training',1500,1799,'1.Nc3 Nc6 2.Nf3 Nf6'),
            (3,'validation',1400,1800,'1.e4 e5'),(4,'test',2000,1700,'1.d4 d5'),
            (5,'quarantine',1200,1500,'1.e4 e5'),(6,'training',1500,1600,'1.e4 e5 2.Bh6')]]
        table=pa.Table.from_pylist(rows);edges,states,checks=replay_table(table,self.native,51)
        self.assertEqual(len(edges),12)
        self.assertEqual(states['replayed_plies'].to_pylist(),[4,4,2,2,-10,-2])
        # Move-order transpositions share BOTH key halves.
        e=edges.to_pylist();a=next(x for x in e if x['game_id']==1 and x['ply']==4);b=next(x for x in e if x['game_id']==2 and x['ply']==4)
        self.assertEqual((a['child_lo'],a['child_hi']),(b['child_lo'],b['child_hi']))
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);(root/'chunks').mkdir();atomic_parquet(edges,root/'chunks/0.edges.parquet');atomic_parquet(states,root/'chunks/0.games.parquet');atomic_parquet(table,root/'frozen.parquet')
            with connection(root) as c:
                ledger_views(c,root,root/'frozen.parquet')
                self.assertEqual(verify_ledger(c)['qualifying_moves'],9)
                self.assertEqual(verify_sample(c,8)['mismatches'],0)
                for role,total in [('training',6),('validation',2),('test',1)]:
                    out=root/f'{role}.parquet';write_sorted_export(c,master_query(role)+' ORDER BY parent_hash,move_san,ply,mover_band',out,MASTER_SCHEMA,batch_rows=2)
                    self.assertEqual(sum(pq.read_table(out)['total'].to_pylist()),total)
                    pooled=root/f'{role}-p30.parquet';write_sorted_export(c,pool_query(out,30)+' ORDER BY parent_hash,move_san',pooled,POOL_SCHEMA,batch_rows=2)
                    self.assertEqual(sum(pq.read_table(pooled)['total'].to_pylist()),total)
                    self.assertEqual(str(pq.read_schema(pooled).field('parent_hash').type),'struct<lo: int64, hi: int64>')
                    self.assertEqual(verify_master(c,out,role)['moves'],total)
                    self.assertEqual(verify_pool(c,pooled,out,30)['moves'],total)
                    bad=pq.read_table(pooled).to_pylist();bad[0]['total']+=1
                    atomic_parquet(pa.Table.from_pylist(bad,schema=pq.read_schema(pooled)),root/f'{role}-corrupt.parquet')
                    with self.assertRaisesRegex(ValueError,'pooled count mismatch'):verify_pool(c,root/f'{role}-corrupt.parquet',out,30)
if __name__=='__main__':unittest.main()

