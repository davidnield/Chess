import unittest
from .exposure_recall import RecallCurve, solve, fixed_recall_exposure
from .model import Graph,Node,Edge,Config


class ExposureRecallTests(unittest.TestCase):
    def test_user_curve_and_invalid_inputs(self):
        curve=RecallCurve()
        for f,r in [(.01,100/101),(.001,.5),(.0001,1/101),(0,0)]: self.assertAlmostEqual(curve(f),r)
        for f in [-1,float('nan'),2]:
            with self.assertRaises(ValueError): curve(f)

    def test_merged_routes_and_upstream_recall_determine_exposure(self):
        g=Graph('r',{'r':Node('us',(Edge('learn','a',.2,0),Edge('other','b',.8,0)),cp=0),
            'a':Node('opp',(Edge('next','s',1),),cp=0),'b':Node('opp',(Edge('next','s',.25),Edge('end','t',.75)),cp=0),
            's':Node('us',(Edge('win','w',.1,400),Edge('miss','t',.9,0)),cp=0),
            'w':Node('terminal',cp=400,wdl=(1,0,0)),'t':Node('terminal',cp=0,wdl=(0,1,0))})
        result=solve(g,{'r':'learn','s':'win'},Config(horizon=3),.001)
        self.assertAlmostEqual(result['recall']['r'],.5)
        self.assertAlmostEqual(result['frequencies']['s'],.001*(.6+.4*.25))
        self.assertAlmostEqual(result['recall']['s'],RecallCurve()(.0007))
        oracle=fixed_recall_exposure(g,{'r':'learn','s':'win'},Config(horizon=3),.001,result['recall'])
        self.assertEqual(oracle,result['frequencies'])

    def test_repeated_position_counts_once_and_ending_blocks_reach(self):
        g=Graph('r',{'r':Node('us',(Edge('go','a',1,0),),cp=0),'a':Node('opp',(Edge('back','r',1),),cp=0)})
        self.assertEqual(fixed_recall_exposure(g,{'r':'go'},Config(horizon=8),.01,{'r':.5}),{'r':.01})
        self.assertAlmostEqual(solve(g,{'r':'go'},Config(horizon=8),.01)['recall']['r'],100/101,places=8)

    def test_ambiguous_feedback_is_not_silently_accepted(self):
        g=Graph('r',{'r':Node('opp',(Edge('a','a',.5),Edge('b','b',.5)),cp=0),
            'a':Node('us',(Edge('continue','b',0,0),Edge('end','t',1,0)),cp=0),
            'b':Node('us',(Edge('continue','a',0,0),Edge('end','t',1,0)),cp=0),
            't':Node('terminal',cp=0)})
        with self.assertRaisesRegex(ValueError,'multiple'):
            solve(g,{'a':'continue','b':'continue'},Config(horizon=4),curve=RecallCurve(.75,12))
