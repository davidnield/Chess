import tempfile
import unittest
from pathlib import Path
import duckdb
import polars as pl
from sweep.audit_otb_split import audit

class SplitAuditTests(unittest.TestCase):
    def test_merges_quarantined_and_missing_or_changed_sources_exposed(self):
        with tempfile.TemporaryDirectory() as td:
            old=Path(td)/'old.parquet'
            pl.DataFrame(dict(game_id=[10,11,12,13,14,15,16],rid=[100,101,102,103,104,105,106],
                              split=[0,2,3,0,4,1,2],source=['s']*7)).write_parquet(old)
            c=duckdb.connect()
            try:
                c.execute('CREATE TABLE game_records(record_id INTEGER,game_id INTEGER,source VARCHAR)')
                c.executemany('INSERT INTO game_records VALUES (?,?,?)',[(100,20,'s'),(101,20,'s'),(102,21,'s'),
                    (103,22,'s'),(104,23,'different'),(105,24,'s')])
                result=audit(c,old)
                self.assertEqual(result['test_training_families'],1)
                self.assertEqual(result['test_training_old_games'],2)
                self.assertEqual(result['missing_records'],1)
                self.assertEqual(result['source_mismatches'],1)
                self.assertEqual(c.execute('SELECT current_game_id FROM split_candidates WHERE candidate_role=\'training\' ORDER BY 1').fetchall(),[(21,),(23,)])
                self.assertEqual(c.execute('SELECT candidate_role,proposed_fold FROM split_candidates WHERE current_game_id=20').fetchall(),[('quarantine',0)])
                self.assertEqual(c.execute('SELECT candidate_role FROM split_candidates WHERE current_game_id=22').fetchall(),[('test',)])
            finally:c.close()

    def test_duplicate_old_game_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            old=Path(td)/'old.parquet'
            pl.DataFrame(dict(game_id=[10,10],rid=[100,101],split=[0,2],source=['s','s'])).write_parquet(old)
            c=duckdb.connect()
            try:
                with self.assertRaisesRegex(ValueError,'duplicate'):audit(c,old)
            finally:c.close()

if __name__=='__main__':unittest.main()
