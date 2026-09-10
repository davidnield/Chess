from dataclasses import replace
from itertools import product
import unittest
from .budget_search import DagScorer, CommonScorer, search
from .context_recall import solve
from .fixtures import demanding, shared_reentry
from .model import Config, Graph,Node,Edge,eligible
from .oracle import enumerate_books,path_evaluate


class BudgetSearchTests(unittest.TestCase):
    def test_compiled_score_matches_context_and_path_oracle(self):
        for graph in [demanding(),shared_reentry()]:
            cfg=Config(horizon=8)
            anchors={graph.root:.99} if graph.nodes[graph.root].side in ('us','opp') else {}
            scorer=DagScorer(graph,cfg,.01,anchors,4)
            for book in enumerate_books(graph,cfg,3):
                expected=solve(graph,book,cfg,.01,anchors=anchors,root_ply=4)
                actual=scorer.score(book)
                oracle=path_evaluate(graph,book,replace(cfg,recall_by_position=expected['recall']))
                self.assertAlmostEqual(actual['score'],oracle.certain_early_advantage)
                self.assertAlmostEqual(actual['unresolved'],oracle.unresolved_first_hit_mass)

    def test_connected_packet_crosses_single_card_barrier(self):
        g=Graph('r',{'r':Node('us',(Edge('go','s',0,0),Edge('safe','t',1,0)),cp=0),
            's':Node('us',(Edge('win','w',0,400),Edge('miss','t',1,0)),cp=0),
            'w':Node('terminal',cp=400),'t':Node('terminal',cp=0)})
        cfg=Config(horizon=2); scorer=DagScorer(g,cfg,.1)
        candidates={'r':['go'],'s':['win']}
        results=search(scorer,candidates,[{'r':'go','s':'win'}],[1,2],max_evaluations=100)
        self.assertEqual(results['1']['book'],{})
        self.assertEqual(results['2']['book'],{'r':'go','s':'win'})
        all_books=list(enumerate_books(g,cfg,2,candidates))
        self.assertAlmostEqual(results['2']['selection_metrics']['score'],max(scorer.score(b)['score'] for b in all_books))

    def test_budget_caps_and_seed_preserved(self):
        g=demanding(); cfg=Config(horizon=8); s=DagScorer(g,cfg,.01,{'root':.99})
        candidates={k:eligible(g,cfg,k) for k,n in g.nodes.items() if n.side=='us'}
        reference={'root':'easy'}
        result=search(s,candidates,[],[1,2,3],reference,max_evaluations=2)
        for b,row in result.items():
            self.assertLessEqual(row['cost'],int(b))
            self.assertGreaterEqual(row['selection_metrics']['score'],s.score(reference)['score'])
            self.assertTrue(all(m in candidates[k] for k,m in row['book'].items()))

    def test_unknown_endings_and_shared_position(self):
        g=Graph('r',{'r':Node('opp',(Edge('a','a',.4),Edge('b','b',.6)),cp=0,end_probability=.1),
            'a':Node('opp',(Edge('s','s',1),),cp=0),'b':Node('opp',(Edge('s','s',1),),cp=0),
            's':Node('us',(Edge('good','w',.1,400),Edge('unknown','u',.9,0)),cp=0),
            'w':Node('terminal',cp=400),'u':Node('frontier',cp=None)})
        cfg=Config(horizon=5); s=DagScorer(g,cfg,.001,{'a':.99})
        for book in [{},{'s':'good'}]:
            expected=solve(g,book,cfg,.001,anchors={'a':.99})['conditional_metrics']
            self.assertAlmostEqual(s.score(book)['score'],expected['certain_early_advantage'])
            self.assertAlmostEqual(s.score(book)['unresolved'],expected['unresolved_first_hit_mass'])

    def test_perfect_selection_is_separate_from_common_recall(self):
        g=demanding(); cfg=Config(horizon=8); scorer=DagScorer(g,cfg,.001)
        for book in enumerate_books(g,cfg,3):
            expected=path_evaluate(g,book,replace(cfg,recall=1,recall_by_position={}))
            self.assertAlmostEqual(scorer.score(book,True)['score'],expected.certain_early_advantage)

    def test_pgn_prescriptions_round_trip_and_detect_missing_marks(self):
        from .repertoire_builder import Builder,key
        from .hashed_board import HashedBoard
        from .io import output_path
        from .verify_builder import verify_pgn
        board=HashedBoard(); board.push_san('e4'); board.push_san('e5'); k=key(board)
        builder=Builder(); builder.prefix='builder/test_export/'
        builder.read=lambda n:{k:['e5']}
        row={'book':{k:'Nf3'},'cost':1,'contexts':{'Blitz':{'recall':{k:.9}}}}
        builder.export('test',1,row)
        text=output_path(builder.prefix+'test_1.pgn').read_text(encoding='utf-8')
        self.assertEqual(verify_pgn(text,row['book']),1)
        with self.assertRaises(AssertionError): verify_pgn(text.replace('STUDY:','MISSING:'),row['book'])
