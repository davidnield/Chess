import hashlib,json,tempfile,unittest,io
from contextlib import redirect_stdout
from unittest.mock import patch
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
from pool_from_book import write_sorted_export
from sweep.freeze_otb import reconcile,eligible,source_prefix,START,GAME_SCHEMA
from sweep.verify_otb_split import validate_tables,replay_sample,main as verify_main
from sweep.stages import file_sha
from sweep.spec import digest

class FrozenSplitTests(unittest.TestCase):
    def connection(self):
        c=duckdb.connect()
        c.execute('CREATE TABLE historical(game_id BIGINT,rid BIGINT,split BIGINT)')
        c.executemany('INSERT INTO historical VALUES (?,?,?)',[(10,1,0),(20,3,1),(30,4,2),(40,5,0),(50,6,4),(60,7,1)])
        c.execute('CREATE TABLE membership(rid BIGINT,game_id BIGINT)')
        c.executemany('INSERT INTO membership VALUES (?,?)',[(0,10),(1,10),(2,20),(3,30),(4,40),(5,50),(6,60)])
        c.execute('CREATE TABLE game_records(record_id BIGINT,game_id BIGINT,source VARCHAR,source_file VARCHAR,source_record BIGINT)')
        c.executemany('INSERT INTO game_records VALUES (?,?,?,?,?)',[(rid,g,'s','file',rid) for rid,g in [(1,100),(2,101),(3,101),(4,102),(5,103),(6,104),(7,104)]])
        c.execute('CREATE TABLE archived_features AS SELECT record_id-1 rid,source,source_file,source_record FROM game_records')
        return c

    def test_full_ancestry_split_and_cross_role_quarantine(self):
        with self.connection() as c:
            stats=reconcile(c,7)
            self.assertEqual(stats['ancestral_records'],7)
            self.assertEqual(stats['historical_games_split'],1)
            roles=dict(c.execute('SELECT game_id,role FROM existing_assignments').fetchall())
            self.assertEqual(roles,{100:'quarantine',101:'quarantine',102:'training',103:'test',104:'quarantine'})
            self.assertEqual(c.execute('SELECT count(DISTINCT component_id) FROM existing_assignments WHERE game_id IN (100,101)').fetchone()[0],1)
            # Test-only representative 100 must inherit the conflict at sibling 101.
            self.assertEqual(c.execute('SELECT fold FROM existing_assignments WHERE game_id=103').fetchone()[0],0)

    def test_missing_record_and_changed_locator_rejected(self):
        for mutation,message in [("DELETE FROM game_records WHERE record_id=2",'lost'),
                                  ("UPDATE game_records SET source_file='wrong' WHERE record_id=2",'locator changed'),
                                  ("UPDATE membership SET game_id=99 WHERE rid=0",'anchor differs')]:
            with self.subTest(mutation=mutation),self.connection() as c:
                c.execute(mutation)
                with self.assertRaisesRegex(ValueError,message):reconcile(c,7)

    def test_new_games_eligibility_and_persisted_old_fold(self):
        with self.connection() as c:
            reconcile(c,7)
            c.execute('''CREATE TABLE opening_analysis(game_id BIGINT,representative_record_id BIGINT,data_source VARCHAR,
             WhiteElo INTEGER,BlackElo INTEGER,WhiteRatingSystem VARCHAR,BlackRatingSystem VARCHAR,GameDate DATE,
             clock_screen VARCHAR,movetext VARCHAR,Result VARCHAR,mainline_plies INTEGER,Variant VARCHAR,FEN VARCHAR)''')
            for gid,date,wr,br in [(100,'2026-01-01',1800,2100),(102,'2026-01-01',1500,1600),
                                  (103,'2026-01-01',1500,1600),(999,'2026-01-01',1500,1600),
                                  (1000,'2012-01-01',1500,1600),(1001,'2100-01-01',1500,1600),(1002,'2026-01-01',2000,2100)]:
                c.execute("INSERT INTO opening_analysis VALUES (?,?,?, ?,?,NULL,NULL,?, 'unknown','1.e4 e5','1-0',2,'standard',?)",[gid,gid,'s',wr,br,date,START])
            counts=eligible(c,'2026-11-19')
            with tempfile.TemporaryDirectory() as directory:
                path=Path(directory)/'games.parquet'
                query='SELECT '+','.join(name for name,_ in GAME_SCHEMA)+' FROM assignments ORDER BY game_id'
                self.assertEqual(write_sorted_export(c,query,path,GAME_SCHEMA,batch_rows=2),4)
                self.assertEqual(str(pq.read_schema(path).field('game_date').type),'date32[day]')
                self.assertEqual(pq.read_table(path).column('game_id').to_pylist(),[100,102,103,999])
            self.assertEqual(sum(counts.values()),4)
            c.execute('CREATE VIEW labels AS SELECT * FROM existing_assignments')
            c.execute('CREATE VIEW frozen_games AS SELECT * FROM assignments')
            verified=validate_tables(c,7,'2026-11-19')
            self.assertEqual(verified['historical_role_crossings'],0)
            self.assertEqual(replay_sample(c,4)['malformed_prefixes'],[])
            c.execute("UPDATE assignments SET role='training' WHERE game_id=100")
            with self.assertRaisesRegex(ValueError,'frozen assignment'):validate_tables(c,7,'2026-11-19')
            c.execute("UPDATE assignments SET role='quarantine' WHERE game_id=100")
            c.execute("UPDATE assignments SET fold=NULL WHERE game_id=999")
            with self.assertRaisesRegex(ValueError,'null identity'):validate_tables(c,7,'2026-11-19')
            self.assertEqual(c.execute('SELECT role,fold FROM assignments WHERE game_id=103').fetchone(),('test',0))
            self.assertEqual(c.execute('SELECT assignment_origin FROM assignments WHERE game_id=999').fetchone(),('new-family',))

    def test_source_annotation_before_blank_line_is_not_a_lost_game(self):
        with duckdb.connect() as c:
            c.execute('CREATE TABLE frozen_games(game_id BIGINT,movetext VARCHAR,role VARCHAR)')
            c.executemany('INSERT INTO frozen_games VALUES (?,?,?)',[
                (1,'{ source annotation }\n\n1. e4 e5','training'),
                (2,'{ annotation only }','training'),
                (3,'{ source }\n\n1. e5','training')])
            result=replay_sample(c,4)
            self.assertEqual(result['per_role']['training'],dict(sampled=3,valid=1))
            self.assertEqual({x['game_id'] for x in result['malformed_prefixes']},{2,3})
            self.assertEqual(result['positions'],2)

    def test_verifier_archives_inputs_and_rejects_corrupted_outputs(self):
        with self.connection() as c,tempfile.TemporaryDirectory() as directory:
            reconcile(c,7)
            c.execute('CREATE VIEW labels AS SELECT * FROM existing_assignments')
            c.execute(f"""CREATE TABLE frozen_games AS SELECT game_id,component_id,fold,role,
                'preserved-ancestry' assignment_origin,game_id representative_record_id,
                1500 white_elo,1800 black_elo,DATE '2026-01-01' game_date,
                '{START}' start_fen,'1.e4 e5' movetext FROM existing_assignments""")
            checks=validate_tables(c,7,'2026-11-19')
            root=Path(directory);old=root/'old';old.mkdir();inputs=[];outputs=[]
            for table,name in [('historical','games.parquet'),('membership','membership.parquet')]:
                path=old/name;c.execute(f"COPY {table} TO '{path.as_posix()}' (FORMAT PARQUET)")
                inputs.append(dict(path=str(path),sha256=file_sha(path)))
            for table,name in [('ancestry','ancestry.parquet'),('lineage','lineage.parquet'),
                               ('labels','historical-assignments.parquet'),('frozen_games','games.parquet')]:
                path=root/name;c.execute(f"COPY {table} TO '{path.as_posix()}' (FORMAT PARQUET)")
                outputs.append(dict(path=str(path),sha256=file_sha(path)))
            recipe=dict(inputs=inputs,historical_rows=7,as_of='2026-11-19')
            split=root/'split.json'
            split.write_text(json.dumps(dict(schema='otb-split-v2',identity=digest(recipe),recipe=recipe,
                checks=checks,outputs=outputs,split_verified=True)),encoding='utf8')
            with patch('sys.argv',['verify','--split',str(split),'--sample-per-role','2']),redirect_stdout(io.StringIO()):
                verify_main()
                (old/'games.parquet').unlink()  # Verification remains reproducible after source refresh.
                verify_main()
                report=json.loads((root/'verified.json').read_text(encoding='utf8'))
                self.assertFalse(report['training_counts_ready'])
                self.assertEqual(report['legal_replay_sample']['malformed_prefixes'],[])
                (root/'games.parquet').write_bytes(b'corrupt')
                with self.assertRaises(ValueError):verify_main()

    def test_historical_source_digest_and_complete_prefix(self):
        s=[dict(path='D:/data/s/a.parquet',base=0,rows=7,sha256='a'),dict(path='D:/data/new/b.parquet',base=7,rows=3,sha256='b')]
        value=hashlib.sha256(json.dumps([('s','a.parquet',0,7,'a')]).encode()).hexdigest()
        self.assertEqual(source_prefix(s,7,value),s[:1])
        with self.assertRaisesRegex(ValueError,'complete source prefix'):source_prefix(s,8,value)
        with self.assertRaisesRegex(ValueError,'snapshot mismatch'):source_prefix(s,7,'bad')

if __name__=='__main__':unittest.main()
