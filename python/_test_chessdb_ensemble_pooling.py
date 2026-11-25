from pathlib import Path
import copy
import json
import unittest
from unittest.mock import patch

import chess
import numpy as np
import pandas as pd
import chessdb_ensemble_pooling as p
from _test_chessdb_ensemble_confirmation import ConfirmationTests
from month_archive import read_json, write_json, sha
from zobrist128 import zobrist128


class PoolingTests(unittest.TestCase):
    def setUp(self):
        self.helper=ConfirmationTests()
        self.rows=self.helper.data()
        self.old=self.rows.head(8).copy();self.old['sf']=15.
        self.new=self.rows.tail(8).copy()
        self.refs={r.id:dict(id=r.id,fen=r.epd+' 0 1',cp_white=r.sf,depth=24,engine='Stockfish 18')
                   for r in self.old.itertuples()}
        self.fresh={r.id:self.result(r) for r in self.new.itertuples()}

    @staticmethod
    def result(r,depth=28,qualified=True,cp=25,kind='parent'):
        return dict(cp_white=cp,depth=depth,nodes=100,qualified=qualified,
                    task=dict(kind=kind,fen=r.epd+' 0 1',target_depth=28))

    def test_upgrades_replace_only_qualified_parents_without_double_counting(self):
        rows=list(self.old.itertuples())
        upgrades={rows[0].id:self.result(rows[0],cp=40),
                  rows[1].id:self.result(rows[1],depth=25,qualified=False,cp=500),
                  rows[2].id:self.result(rows[2],depth=25,qualified=True,cp=600)}
        new_refs=copy.deepcopy(self.fresh);new_refs[self.new.id.iloc[0]]['qualified']=False
        got,audit=p.annotate(self.old,self.new,self.refs,upgrades,new_refs,set())
        self.assertEqual(len(got),15);self.assertEqual(len(audit),16)
        x=got.set_index('id')
        self.assertEqual(x.loc[rows[0].id,'sf'],40)
        self.assertEqual(x.loc[rows[1].id,'sf'],15)
        self.assertEqual(x.loc[rows[2].id,'sf'],15)
        self.assertTrue(np.isnan(x.loc[rows[1].id,'reference_actual_depth']))
        self.assertEqual(x.loc[rows[1].id,'reference_requested_depth'],24)
        self.assertEqual(int((audit.upgrade_status=='replaced-old').sum()),1)
        self.assertTrue(any(abs(int(v))>2**53 for v in got.lo))
        upgrades[rows[0].id]['task']['kind']='child'
        with self.assertRaisesRegex(ValueError,'matching parent'):
            p.annotate(self.old,self.new,self.refs,upgrades,new_refs,set())

    def test_deduplication_prefers_deeper_not_score_agreement(self):
        new=pd.concat([self.old.drop(columns='sf').head(1),self.new],ignore_index=True)
        fresh={r.id:self.result(r,cp=900) for r in new.itertuples()}
        got,audit=p.annotate(self.old,new,self.refs,{},fresh,set())
        self.assertEqual(len(got),16);self.assertEqual(len(audit),17)
        same=got[got.id==self.old.id.iloc[0]].iloc[0]
        self.assertEqual(same.sf,900);self.assertEqual(same.source_panel,'new')
        # A color-mirrored spelling is the same canonical evidence unit.
        b=chess.Board(self.old.epd.iloc[1]).mirror();lo,hi=zobrist128(b)
        new.loc[0,['id','lo','hi','epd','white_to_move']]=[f'{lo}:{hi}',lo,hi,b.epd(),b.turn]
        fresh={r.id:self.result(r,cp=800) for r in new.itertuples()}
        got,audit=p.annotate(self.old,new,self.refs,{},fresh,set())
        self.assertEqual(len(got),16);self.assertEqual(len(set(got.canonical)),16)

    def test_exclusions_and_reference_identity_fail_closed(self):
        with self.assertRaisesRegex(ValueError,'confirmation board'):
            p.annotate(self.old,self.new,self.refs,{},self.fresh,{p.study.canon(self.new.epd.iloc[0])})
        bad=self.old.copy();bad.loc[0,'lo']=0;bad.loc[0,'id']=f'0:{bad.hi.iloc[0]}'
        with self.assertRaisesRegex(ValueError,'key/FEN'):
            p.annotate(bad,self.new,self.refs,{},self.fresh,set())
        refs=copy.deepcopy(self.refs);refs[self.old.id.iloc[0]]['cp_white']=999
        with self.assertRaisesRegex(ValueError,'score differs'):
            p.annotate(self.old,self.new,refs,{},self.fresh,set())

    def test_report_weighting_and_old_reference_sensitivity(self):
        first=next(self.old.itertuples())
        d,_=p.annotate(self.old,self.new,self.refs,{first.id:self.result(first,cp=100)},self.fresh,set())
        cfg=dict(catalog=p.study.catalog(),gates={'strict':dict(white=.05,black=.05),
                 'permissive':dict(white=.11,black=.075)},
                 baselines=dict(white=.5183137990703003,black=.5195669627673518))
        with self.helper.scratch() as td:
            out=Path(td);table=p.score_panels(d,cfg,out)
            errors=100*abs(p.study.predict(d,cfg['catalog']).sub(p.ref.score(d.sf),axis=0))
            expected=errors.groupby(d.stratum).mean().mean()
            np.testing.assert_allclose(table['pooled-best__macro_mae_points'].reindex(expected.index),expected)
            equal=errors.groupby([d.stratum,d.source_panel]).mean().groupby(level=0).mean().mean()
            np.testing.assert_allclose(table['equal-panel-within-stratum__macro_mae_points'].reindex(equal.index),equal)
            self.assertTrue((table['old-best__macro_mae_points']!=table['old-original__macro_mae_points']).any())
            self.assertTrue((out/'reference-requested-depth24-actual-unknown-gates.csv').exists())
            # Distinguish unequal panel sizes from equal panel weighting.
            d=pd.concat([d,d[d.source_panel=='new']],ignore_index=True)
            t=p.score_panels(d,cfg,out)
            self.assertFalse(np.allclose(t['pooled-best__macro_mae_points'],t['equal-panel-within-stratum__macro_mae_points']))

    def fixture(self,root):
        conf,base,rationale=self.helper.fixture(root)
        out=root/'pool';out.mkdir()
        cfg=dict(protocol=p.PROTOCOL,code=p.code_identity(),sources={},runtime={},confirmation=str(conf),
                 catalog=p.study.catalog())
        write_json(out/'definition.json',cfg)
        write_json(out/'freeze.json',dict(definition_sha256=sha(out/'definition.json')))
        write_json(conf/p.MARKER,dict(protocol=p.PROTOCOL,pooling_output=str(out),definition_sha256=sha(out/'definition.json')))
        # The main-only fixture chooses ChessDB. Pooled fixture chooses Fishnet.
        pd.DataFrame([dict(cohort='all3_nnue',policy=n,macro_mae_points=e)
                      for n,e in zip(p.confirmation.SINGLES,[2.,.5,1.])]).to_csv(out/'pooled-best-metrics.csv',index=False)
        receipt=dict(protocol=p.PROTOCOL,definition_sha256=sha(out/'definition.json'),inputs={},
                     outputs={str(out/'pooled-best-metrics.csv'):sha(out/'pooled-best-metrics.csv')})
        write_json(out/'COMPLETE.json',receipt)
        return out,conf,rationale

    def test_requires_completed_studies_before_reading_references(self):
        with self.helper.scratch() as td:
            root=Path(td);a=root/'main';b=root/'target';a.mkdir();b.mkdir()
            write_json(a/'status.json',dict(state='searching'));write_json(b/'status.json',dict(state='complete'))
            with patch.object(p.ref,'load_results',side_effect=AssertionError('reference leak')):
                with self.assertRaisesRegex(ValueError,'no interim'):
                    p.completed_inputs(dict(study=str(a),targeted=str(b)))

    def test_pooled_choice_lock_is_blind_immutable_and_hash_bound(self):
        with self.helper.scratch() as td:
            out,conf,rationale=self.fixture(Path(td))
            with patch.object(p.ref,'load_results',side_effect=AssertionError('sealed leak')):
                p.lock_choice(out,'score_equal3',rationale)
            choice=p.confirmation.checked_choice(conf)
            self.assertEqual(choice['best_single'],'cp_0_10_0')
            self.assertEqual(choice['selection_protocol'],p.PROTOCOL)
            self.assertFalse(choice['selection_uses_confirmation_labels'])
            with self.assertRaisesRegex(ValueError,'already locked'):
                p.lock_choice(out,'proposal',rationale)
            (out/'pooled-best-metrics.csv').write_text('changed')
            with self.assertRaisesRegex(ValueError,'evidence changed'):
                p.confirmation.checked_choice(conf)
            with self.assertRaisesRegex(ValueError,'checksum'):
                p.verify_complete(out)

    def test_pooled_lock_to_unchanged_confirmation_report_end_to_end(self):
        with self.helper.scratch() as td:
            out,conf,rationale=self.fixture(Path(td))
            with patch.object(p.ref,'load_results',side_effect=AssertionError('sealed leak')):
                with self.assertRaisesRegex(ValueError,'lock-choice'):
                    p.report_confirmation(out)
            p.confirmation.search_sealed(conf,search=self.helper.fake)
            p.lock_choice(out,'score_equal3',rationale)
            p.report_confirmation(out)
            summary=read_json(conf/'report'/'summary.json')
            self.assertFalse(summary['adopted']);self.assertEqual(summary['qualified'],8)
            table=pd.read_csv(conf/'report'/'confirmation-metrics.csv')
            self.assertEqual(set(table.policy),{'score_equal3','proposal','cp_0_10_0'})

    def test_legacy_choice_refused_by_new_report(self):
        with self.helper.scratch() as td:
            out,conf,rationale=self.fixture(Path(td))
            p.confirmation.lock_choice(conf,'score_equal3',rationale)
            with patch.object(p.confirmation,'report',side_effect=AssertionError('sealed leak')):
                with self.assertRaisesRegex(ValueError,'pooled selection'):
                    p.report_confirmation(out)

    def test_complete_build_receipts_counts_and_child_exclusion(self):
        with self.helper.scratch() as td:
            root=Path(td);out,conf,_=self.fixture(root)
            panel=root/'panel';panel.mkdir();target=root/'target';target.mkdir();base=root/'base'
            self.old.to_parquet(panel/'panel_scored_sf18_d24.parquet',index=False)
            self.new.to_parquet(base/'holdout-inputs.parquet',index=False)
            (panel/'search_sf18_d24.jsonl').write_text('\n'.join(json.dumps(r) for r in self.refs.values()))
            # Disjoint exclusion position, without any engine label.
            pd.DataFrame([dict(epd=chess.Board().epd())]).to_parquet(conf/'confirmation-inputs.parquet',index=False)
            cfg=read_json(out/'definition.json')
            cfg.update(study=str(base),targeted=str(target),panel=str(panel),
                       gates={'strict':dict(white=.05,black=.05),'permissive':dict(white=.11,black=.075)},
                       baselines=dict(white=.5183137990703003,black=.5195669627673518))
            write_json(out/'definition.json',cfg)
            write_json(out/'freeze.json',dict(definition_sha256=sha(out/'definition.json')))
            write_json(conf/p.MARKER,dict(protocol=p.PROTOCOL,pooling_output=str(out),definition_sha256=sha(out/'definition.json')))
            row=next(self.old.itertuples())
            upgraded={row.id:self.result(row),row.id+':1':self.result(row,kind='child')}
            with patch.object(p,'completed_inputs',return_value=(self.fresh,upgraded)):
                p.build(out,cfg)
            _,receipt=p.verify_complete(out)
            summary=read_json(out/'summary.json')
            self.assertEqual(summary['retained'],16)
            self.assertEqual(summary['qualified_old_upgrades'],1)
            self.assertEqual(summary['nnue_all_three'],16)
            self.assertFalse(summary['confirmation_scores_read'])
            self.assertIn(str(out/'pooled-positions.parquet'),receipt['outputs'])
            self.assertEqual(read_json(out/'status.json')['state'],'complete')


if __name__=='__main__':unittest.main()
