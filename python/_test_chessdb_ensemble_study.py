import json
from pathlib import Path
import tempfile
import unittest

import chess
import numpy as np
import pandas as pd
import chessdb_ensemble_study as s
from month_archive import write_json, sha
from zobrist128 import zobrist128


class StudyTests(unittest.TestCase):
    def scratch(self):
        p=Path(__file__).resolve().parents[1]/'scratch'/'ensemble-tests'
        p.mkdir(parents=True,exist_ok=True)
        return tempfile.TemporaryDirectory(dir=p)

    def frame(self):
        return pd.DataFrame(dict(cloud=[0.,np.nan,100.],fishnet=[300.,100.,-300.],
             chessdb=[600.,200.,50.],cloud_depth=[25.,np.nan,20.],
             fishnet_tier=['nnue','nnue','classical'],fishnet_n_tier=[1,2,3],current=[0.,100.,100.]))

    def test_catalog_pov_and_missing_sources(self):
        recipes=s.catalog();self.assertEqual(len(recipes),135)
        self.assertEqual(len({x['name'] for x in recipes}),135)
        d=self.frame();p=s.predict(d,recipes)
        self.assertAlmostEqual(p.loc[0,'cp_equal3'],s.ref.score(300.))
        self.assertAlmostEqual(p.loc[0,'score_equal3'],np.mean(s.ref.score([0,300,600])))
        self.assertNotAlmostEqual(p.loc[0,'cp_equal3'],p.loc[0,'score_equal3'])
        self.assertTrue(np.isnan(p.loc[1,'cp_10_0_0']))
        pp=s.predict(d,recipes,production=True)
        self.assertAlmostEqual(pp.loc[1,'cp_10_0_0'],s.ref.score(200))
        self.assertAlmostEqual(pp.loc[2,'cp_0_10_0'],s.ref.score(50))
        self.assertAlmostEqual(pp.loc[2,'cp_equal3'],s.ref.score(75))

    def test_nullable_scores_and_integer_identity(self):
        d=pd.DataFrame(dict(lo=[9007199254740993],hi=[-9007199254740995],
            cloud_cp=pd.array([None],dtype='Int32'),cloud_mate=pd.array([None],dtype='Int32'),
            cp=pd.array([2],dtype='Int32'),mate=pd.array([None],dtype='Int32'),
            fishnet_cp=pd.array([None],dtype='Int32'),fishnet_mate=pd.array([-3],dtype='Int32'),
            fishnet_tier=['nnue'],fishnet_n_tier=[2],cloud_depth=pd.array([None],dtype='Int16'),
            best_score=[30001],white_to_move=[False]))
        d=s.normalize(d)
        self.assertEqual(d.id.iloc[0],'9007199254740993:-9007199254740995')
        self.assertEqual(d.fishnet.iloc[0],-2000)
        self.assertEqual(d.chessdb.iloc[0],-2000)
        self.assertEqual(d.stratum.iloc[0],'no_cloud_repeated')
        self.assertTrue(np.isnan(d.cloud.iloc[0]))

    def test_canonical_exclusion_and_gate_perspective(self):
        b=chess.Board();b.push_uci('e2e4')
        self.assertEqual(s.canon(b.fen(en_passant='fen')),s.canon(b.fen(en_passant='legal')))
        self.assertEqual(s.canon(b.fen()),s.canon(b.mirror().fen()))
        gates=s.gate_counts(np.array([.6]),np.array([.5]),{'test':dict(white=.05,black=.05)},dict(white=.5,black=.5))
        self.assertEqual(gates[0]['false_exclusions'],0)
        self.assertEqual(gates[1]['false_exclusions'],1)

    def test_real_native_sampler_and_source_receipt(self):
        with self.scratch() as td:
            out=Path(td);db=out/'db';menu=out/'menus';db.mkdir();(menu/'positions').mkdir(parents=True)
            board=chess.Board();ev=[];positions=[]
            for move in ['e2e4','e7e5','g1f3','b8c6']:
                board.push_uci(move);lo,hi=zobrist128(board)
                ev.append(dict(position_hash=lo,position_hash_hi=hi,epd=board.epd(),source='cloud',
                   cp=10,mate=None,cloud_depth=25,cloud_cp=10,cloud_mate=None,fishnet_cp=20,
                   fishnet_mate=None,fishnet_tier='nnue',fishnet_n_tier=1,hash_ambiguous=False))
                positions.append(dict(lo=lo,hi=hi,games=10,first_ply=len(board.move_stack),
                   white_to_move=board.turn,best_score=30,found=True))
            # Duplicate input identity is rejected by eligibility, never any_value-picked.
            ev.append(dict(ev[0]))
            pd.DataFrame(ev).to_parquet(db/'bkt000.parquet',index=False)
            pos=menu/'positions'/'bkt000.parquet';pd.DataFrame(positions).to_parquet(pos,index=False)
            write_json(menu/'_MENUS.COMPLETE.json',{'buckets':{'0':{'positions':{'sha256':sha(pos)}}}})
            cfg=dict(eval_db=str(db),menus=str(menu),thinning=1,per_bucket_keep=450,settings=dict(disk_reserve_gib=0))
            r=s.sample_bucket(out,cfg,0)
            self.assertEqual(r['rows'],3)
            d=pd.read_parquet(out/'sample-buckets'/'b000.parquet')
            self.assertEqual(set(d.stratum),{'deep_once'})
            self.assertNotIn(positions[0]['lo'],set(d.lo))
            pd.DataFrame(positions[:2]).to_parquet(pos,index=False)
            with self.assertRaisesRegex(ValueError,'publication'):
                s.sample_bucket(out,cfg,0)

    def test_checkpoint_resume_and_no_interim_holdout_report(self):
        with self.scratch() as td:
            out=Path(td);engine=out/'fake-engine';engine.write_text('test')
            cfg=dict(code=s.code_identity(),sources={},settings={})
            write_json(out/'definition.json',cfg);write_json(out/'finalists.json',{})
            write_json(out/'freeze.json',dict(definition=sha(out/'definition.json'),finalists=sha(out/'finalists.json')))
            tasks=[dict(id=str(i),kind='parent',minimum_depth=26) for i in range(2)]
            m=dict(code=cfg['code'],engine=str(engine),engine_sha256=sha(engine),tasks=tasks,
                   sources={},settings=dict(workers=1))
            write_json(out/'manifest.json',m)
            calls=[]
            def fake(t,m):
                calls.append(t['id']);return dict(cp_white=0,depth=28,bound=False,seconds=.1,engine_sha256=m['engine_sha256'])
            self.assertFalse(s.reference_run(out,max_new=1,search=fake))
            with self.assertRaisesRegex(ValueError,'no interim'):
                s.report(out)
            self.assertTrue(s.reference_run(out,search=fake))
            self.assertTrue(s.reference_run(out,search=fake))
            self.assertEqual(calls,['0','1'])
            write_json(out/'finalists.json',dict(tampered=True))
            with self.assertRaisesRegex(ValueError,'frozen'):
                s.check_definition(out)


if __name__=='__main__':unittest.main()
