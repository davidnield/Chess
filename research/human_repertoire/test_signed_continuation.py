import unittest
from .model import Graph, Node, Edge
from .signed_continuation import intervention, flow_bounds


class SignedContinuationTests(unittest.TestCase):
    def graph(self, shared=False):
        return Graph('r',{'r':Node('us',(Edge('Be3','a',.4),Edge('other','a' if shared else 'b',.6)),cp=0),
            'a':Node('opp',(Edge('win','w',.5),Edge('uncertain','u',.5)),cp=0),
            'b':Node('opp',(Edge('join','u',1),),cp=0),
            'w':Node('terminal',cp=400),'u':Node('frontier',cp=0)})

    def test_signed_cancellation_matches_exhaustive_unknown_values(self):
        g=self.graph(); bound=intervention(g,horizon=3,discount=1)
        # Be3 - fallback: .6*(.5+.5*u-u) = .3-.3*u.
        self.assertAlmostEqual(bound['interval'][0],0)
        self.assertAlmostEqual(bound['interval'][1],.3)
        for u in [0,.1,.5,1]:
            v=.3-.3*u
            self.assertLessEqual(bound['interval'][0]-1e-12,v)
            self.assertGreaterEqual(bound['interval'][1]+1e-12,v)

    def test_shared_unknown_subtree_cancels_exactly(self):
        result=intervention(self.graph(shared=True),horizon=3)
        self.assertEqual(result['interval'],[0,0])

    def test_different_remaining_horizons_do_not_cancel(self):
        g=Graph('r',{'r':Node('us',(Edge('Be3','u',.5),Edge('other','x',.5)),cp=0),
            'x':Node('opp',(Edge('later','u',1),),cp=0),'u':Node('frontier',cp=0)})
        result=intervention(g,horizon=2,discount=1)
        self.assertEqual(result['interval'],[0,.5])

    def test_unknown_before_known_hit_keeps_uncertainty(self):
        g=Graph('r',{'r':Node('us',(Edge('Be3','u',.5),Edge('other','t',.5)),cp=0),
            'u':Node('opp',(Edge('win','w',1),),cp=None),'w':Node('terminal',cp=400),
            't':Node('terminal',cp=0)})
        self.assertEqual(intervention(g,horizon=3,discount=1)['interval'],[0,.5])

    def test_resolved_advantage_and_ending_hazard(self):
        g=Graph('r',{'r':Node('us',(Edge('Be3','w',.2),Edge('other','t',.8)),cp=0,end_probability=.1),
            'w':Node('terminal',cp=400),'t':Node('terminal',cp=-400)})
        result=intervention(g,horizon=1,discount=.9)
        self.assertAlmostEqual(result['interval'][0],.8*.9*.9)
        self.assertEqual(result['interval'][0],result['interval'][1])

    def test_invalid_boundary_rejected(self):
        g=Graph('r',{'r':Node('us',(Edge('Be3','t',1),),cp=None),'t':Node('terminal',cp=0)})
        with self.assertRaises(ValueError): intervention(g)
