import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd
import chessdb_policy_validation as v
from month_archive import write_json
from sweep.spec import BASELINES, GATES


class PolicyFollowup(unittest.TestCase):
    def test_gate_selection_and_full_keys(self):
        lo = 9007199254740993
        d = pd.DataFrame([dict(id=f'{lo}:9', lo=lo, hi=9, grp='cloud_deep',
                               step1=-10., proposal=-70.),
                          dict(id='2:8', lo=2, hi=8, grp='nnue_once', step1=60., proposal=100.)])
        diagnostics = [('cdb-regret-1pp', pd.DataFrame([dict(lo=lo, hi=9, score_points=2.)]), 'score_points', 1.)]
        reasons, menu = v.selection(d, diagnostics, GATES, BASELINES)
        self.assertIn('absolute-flip:stricter:white', reasons[f'{lo}:9'])
        self.assertIn('absolute-flip:stricter:black', reasons['2:8'])
        self.assertEqual(menu, {f'{lo}:9'})
        self.assertEqual((reasons, menu), v.selection(d, diagnostics, GATES, BASELINES))

    def test_quality_never_treats_bounded_or_shallow_search_as_deeper(self):
        task = dict(minimum_depth=26)
        self.assertFalse(v.qualified(task, dict(depth=25)))
        self.assertFalse(v.qualified(task, dict(depth=28, bound=True)))
        self.assertTrue(v.qualified(task, dict(depth=26, bound=False)))
        self.assertTrue(v.qualified(task, dict(depth=0, terminal=True)))

    def test_score_records_do_not_inherit_stale_bounds_or_deeper_depth(self):
        from chess.engine import AnalysisResult
        bounded = dict(score=99, depth=26, nodes=1, lowerbound=True, pv=['a2a4'])
        exact = dict(score=42, depth=26, nodes=2, pv=['a2a3'])
        aggregate = AnalysisResult()
        aggregate.post(bounded)
        aggregate.post(exact)
        self.assertTrue(aggregate.info['lowerbound'])  # Reproduce aggregate trap.
        deeper_bound = dict(score=90, depth=28, nodes=3, upperbound=True, pv=['a2a4'])
        chosen, details = v.completed_reference([bounded, exact, deeper_bound, dict(nodes=4)])
        self.assertEqual(chosen, exact)
        self.assertEqual(details['maximum_reported_depth'], 28)
        self.assertEqual(details['bound_updates'], 2)
        self.assertTrue(v.qualified(dict(minimum_depth=26), dict(depth=chosen['depth'], bound=False)))
        self.assertFalse(v.qualified(dict(minimum_depth=27), dict(depth=chosen['depth'], bound=False)))
        chosen, details = v.completed_reference([bounded])
        self.assertEqual(chosen, bounded)
        self.assertEqual(details['reference_kind'], 'bound-only')
        with self.assertRaisesRegex(ValueError, 'no scored'):
            v.completed_reference([dict(nodes=1)])

    def test_checkpoint_resume_and_recipe_corruption(self):
        scratch = Path(__file__).resolve().parents[1]/'scratch'/'policy-followup-tests'
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as td:
            out = Path(td)
            parents, tasks = [], []
            for i in range(2):
                ident = f'{9007199254740993+i}:8'
                parents.append(dict(id=ident, lo=9007199254740993+i, hi=8, grp='gap',
                    white_to_move=True, epd='test-only', reasons=['control'],
                    step1=5., proposal=0., old_sf=10., chessdb=0., menu_test=False))
                tasks.append(dict(id=ident, parent=ident, kind='parent', fen='test-only',
                                  target_depth=28, minimum_depth=26))
            m = dict(schema=1, engine_sha256='a'*64, settings=dict(workers=4),
                     gates=GATES, baselines=BASELINES, parents=parents, tasks=tasks,
                     interpretation='synthetic')
            write_json(out/'manifest.json', m)
            calls = []
            def fake(t, manifest):
                calls.append(t['id'])
                return dict(cp_white=0, depth=28, nodes=10, seconds=.1, bound=False,
                            engine_sha256=manifest['engine_sha256'])
            v.run(out, workers=1, max_new=1, search=fake, check_inputs=False)
            self.assertEqual(len(calls), 1)
            v.run(out, workers=1, search=fake, check_inputs=False)
            self.assertEqual(len(calls), 2)
            v.run(out, workers=1, search=fake, check_inputs=False)
            self.assertEqual(len(calls), 2)
            self.assertEqual(v.read_json(out/'summary.json')['completed'], 2)
            saved = next((out/'results').glob('*.json'))
            original = saved.read_bytes()
            r = v.read_json(saved); r['payload']['cp_white'] = 100
            write_json(saved, r)
            with self.assertRaisesRegex(ValueError, 'checkpoint'):
                v.load_results(out, m)
            saved.write_bytes(original)
            m['settings']['workers'] = 3
            with self.assertRaisesRegex(ValueError, 'checkpoint'):
                v.load_results(out, m)


if __name__ == '__main__':
    unittest.main()
