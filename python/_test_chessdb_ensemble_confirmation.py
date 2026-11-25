from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import chess
import pandas as pd
import chessdb_ensemble_confirmation as c
from month_archive import read_json,write_json,sha
from zobrist128 import zobrist128


class ConfirmationTests(unittest.TestCase):
    def scratch(self):
        p=Path(__file__).resolve().parents[1]/'scratch'/'confirmation-tests'
        p.mkdir(parents=True,exist_ok=True)
        return tempfile.TemporaryDirectory(dir=p)

    def data(self):
        b=chess.Board();rows=[]
        moves='e4 e5 Nf3 Nc6 Bc4 Bc5 c3 Nf6 d3 d6 O-O O-O Re1 a6 Bb3 Ba7'.split()
        groups=list(c.QUOTAS)
        for i,m in enumerate(moves):
            b.push_san(m);lo,hi=zobrist128(b);group=groups[i%4]
            rows.append(dict(id=f'{lo}:{hi}',lo=lo,hi=hi,epd=b.epd(),white_to_move=b.turn,
                stratum=group,fishnet_tier='nnue',cloud=10.,fishnet=20.,chessdb=30.,current=10.,
                cloud_depth=25 if group.startswith('deep') else 18,
                fishnet_n_tier=1 if group.endswith('once') else 2,
                sample_priority=i,first_ply=i+1,games=10,bucket=0))
        return pd.DataFrame(rows)

    def fixture(self,root):
        out=root/'run';out.mkdir();base=root/'base';base.mkdir();engine=root/'engine';engine.write_text('fake')
        d=self.data().head(8);d.to_parquet(out/'confirmation-inputs.parquet',index=False)
        cfg=dict(code=c.code_identity(),sources={},runtime={},base_study=str(base),catalog=c.study.catalog(),
            settings=dict(workers=1,bootstrap=20),quotas={g:2 for g in c.QUOTAS},
            engine=str(engine),engine_sha256=sha(engine),
            gates={'strict':dict(white=.05,black=.05),'permissive':dict(white=.11,black=.075)},
            baselines=dict(white=.5183137990703003,black=.5195669627673518))
        write_json(out/'definition.json',cfg)
        write_json(out/'freeze.json',dict(definition_sha256=sha(out/'definition.json')))
        tasks=[dict(id=r.id,parent=r.id,kind='parent',fen=r.epd+' 0 1',target_depth=28,minimum_depth=26) for r in d.itertuples()]
        manifest=dict(code=cfg['code'],engine=str(engine),engine_sha256=sha(engine),settings=cfg['settings'],
                      tasks=tasks,sources={str(out/'confirmation-inputs.parquet'):sha(out/'confirmation-inputs.parquet')})
        write_json(out/'manifest.json',manifest)
        write_json(base/'status.json',dict(state='complete'))
        pd.DataFrame([dict(cohort='all3_nnue',policy=n,macro_mae_points=e) for n,e in zip(c.SINGLES,[2.,1.5,1.])]).to_csv(base/'holdout-metrics.csv',index=False)
        for name in ('summary.json','holdout-gates.csv','holdout-paired.csv','reference-qualification.csv'):
            (base/name).write_text('{}')
        rationale=root/'rationale.md';rationale.write_text('Selected using preceding study error, tail and gate evidence.')
        return out,base,rationale

    @staticmethod
    def fake(t,m):
        return dict(cp_white=15,depth=28,bound=False,seconds=.1,nodes=100,engine_sha256=m['engine_sha256'])

    def test_selection_excludes_known_boards_and_mirrors(self):
        d=self.data();b=chess.Board(d.epd.iloc[0])
        blocked={c.study.canon(b.mirror().fen())}
        got=c.select_panel(d,blocked,{g:2 for g in c.QUOTAS})
        self.assertEqual(len(got),8)
        self.assertEqual(set(got.stratum),set(c.QUOTAS))
        self.assertNotIn(d.id.iloc[0],set(got.id))
        self.assertEqual(len(set(got.canonical)),8)
        self.assertTrue(any(abs(int(x))>2**53 for x in got.lo))
        d.loc[1,'fishnet_tier']='early'
        with self.assertRaisesRegex(ValueError,'all three sources'):
            c.select_panel(d,blocked,{g:2 for g in c.QUOTAS})

    def test_no_sealed_access_before_choice_and_resume(self):
        with self.scratch() as td:
            out,base,rationale=self.fixture(Path(td));calls=[]
            def search(t,m):calls.append(t['id']);return self.fake(t,m)
            self.assertFalse(c.search_sealed(out,max_new=1,search=search))
            with patch.object(c.ref,'load_results',side_effect=AssertionError('leaked sealed access')):
                with self.assertRaisesRegex(ValueError,'lock-choice'):
                    c.report(out)
            self.assertTrue(c.search_sealed(out,search=search))
            self.assertEqual(len(calls),8)
            self.assertTrue(c.search_sealed(out,search=search));self.assertEqual(len(calls),8)
            status=read_json(out/'status.json')
            self.assertEqual(status['state'],'sealed-complete');self.assertFalse(status['scores_opened'])
            self.assertFalse((out/'report').exists())
            self.assertFalse((out/'results').exists())

    def test_choice_requires_finished_prior_and_is_immutable(self):
        with self.scratch() as td:
            out,base,rationale=self.fixture(Path(td))
            write_json(base/'status.json',dict(state='searching'))
            with self.assertRaisesRegex(ValueError,'not complete'):
                c.lock_choice(out,'score_equal3',rationale)
            write_json(base/'status.json',dict(state='complete'))
            choice=c.lock_choice(out,'score_equal3',rationale)
            self.assertEqual(choice['best_single'],'cp_0_0_10')
            self.assertFalse(choice['selection_uses_confirmation_labels'])
            self.assertEqual(len(choice['recipes']),3)
            with self.assertRaisesRegex(ValueError,'already locked'):
                c.lock_choice(out,'proposal',rationale)
            (base/'holdout-gates.csv').write_text('changed')
            with self.assertRaisesRegex(ValueError,'evidence changed'):
                c.checked_choice(out)

    def test_locked_comparison_end_to_end_and_corruption(self):
        with self.scratch() as td:
            out,base,rationale=self.fixture(Path(td))
            c.search_sealed(out,search=self.fake)
            c.lock_choice(out,'score_equal3',rationale)
            c.report(out)
            summary=read_json(out/'report'/'summary.json')
            self.assertFalse(summary['adopted']);self.assertEqual(summary['qualified'],8)
            self.assertEqual(len(summary['paired']),2)
            table=pd.read_csv(out/'report'/'confirmation-metrics.csv')
            self.assertEqual(set(table.policy),{'score_equal3','proposal','cp_0_0_10'})
            self.assertTrue((out/'OPENED.json').exists())
            p=next((out/'sealed'/'results').glob('*.json'));r=read_json(p)
            r['payload']['cp_white']=500;write_json(p,r)
            with self.assertRaisesRegex(ValueError,'checksum'):
                c.report(out)


if __name__=='__main__':unittest.main()
