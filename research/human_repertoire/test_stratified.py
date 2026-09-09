from dataclasses import replace
import unittest
from .fixtures import demanding, shared_reentry
from .model import Config, Graph, Node, Edge
from .oracle import path_evaluate
from .trajectory import GraphBackend, score_trace
from .stratified import enumerate_prefixes, continue_trace, weighted_summary


class StratifiedTests(unittest.TestCase):
    def test_exhaustive_prefixes_match_independent_book_oracle(self):
        for graph, book in [(demanding(), {'root':'demanding','d1':'remember1'}), (shared_reentry(), {'shared':'find'})]:
            for recall in [.4, 1]:
                cfg = Config(horizon=6, recall=recall)
                backend = GraphBackend(graph); ps = enumerate_prefixes(backend,'x',book,cfg,depth=6)
                exact = path_evaluate(graph,book,cfg).report(); scores = {k:n.cp for k,n in graph.nodes.items()}
                rows = [(p['probability'],score_trace(continue_trace(backend,'x',book,cfg,'x',0,p),scores,cfg)) for p in ps]
                for metric in ['early_advantage','advantage','disadvantage','win','draw','loss']:
                    self.assertAlmostEqual(sum(w*r[metric] for w,r in rows),exact[metric])

    def test_prefix_history_preserves_repetition_and_hazard_priority(self):
        graph=Graph('r',{'r':Node('us',(Edge('go','a',1),),cp=0), 'a':Node('opp',(Edge('back','r',1),),cp=0)})
        cfg=Config(horizon=8); backend=GraphBackend(graph)
        p=enumerate_prefixes(backend,'x',{},cfg)[0]
        t=continue_trace(backend,'x',{},cfg,'x',0,p)
        self.assertEqual(t['keys'],['r','a','r','a','r']); self.assertEqual(t['reason'],'repetition')
        hazard=Graph('r',{'r':Node('us',(Edge('go','a',1),),cp=0,end_probability=.25,end_wdl=(1,0,0)), 'a':Node('terminal',cp=400,wdl=(1,0,0))})
        ps=enumerate_prefixes(GraphBackend(hazard),'x',{},cfg)
        self.assertEqual([p['probability'] for p in ps],[.25,.75])
        self.assertEqual(enumerate_prefixes(GraphBackend(hazard),'x',{},replace(cfg,horizon=0))[0]['reason'],'horizon')

    def test_weighted_estimator_keeps_rare_branch_small(self):
        r=weighted_summary({'rare':[1,1],'common':[0,0]}, {'rare':.01,'common':.99}, difference=True)
        self.assertAlmostEqual(r['mean'],.01); self.assertEqual(r['mc_se'],0)
        self.assertGreater(r['hoeffding_margin'],0)
        with self.assertRaises(ValueError): weighted_summary({'x':[0,1]},{'x':.5})
        with self.assertRaises(ValueError): enumerate_prefixes(GraphBackend(demanding()),'x',{},Config(reentry=False))
        with self.assertRaises(ValueError): enumerate_prefixes(GraphBackend(demanding()),'x',{},Config(),cap=1)
