"""Independent path-sum verification of the real exposure comparison."""
from dataclasses import replace
import math
import chess
from python.zobrist import zobrist_int64
from .exposure_real import verified,read,save,PREFIX
from .io import output_path,sha256
from .model import Config,graph_from_dict
from .exposure_recall import fixed_recall_exposure,RecallCurve
from .oracle import path_evaluate


def main():
    p=verified(); data=read('graphs'); results=read('results'); empirical=read('empirical'); fens=read('fens')
    assert results['manifest_sha256']==sha256(output_path(PREFIX+'manifest.json'))
    assert results['identity']==p['identity']
    checks=0; legal_edges=0; cfg=Config(horizon=3,recall=.85)
    for event,raw in data.items():
        g=graph_from_dict(raw); g.validate(); policies=read('policies')[event]
        for key,node in g.nodes.items():
            if key.startswith('unresolved:'): continue
            board=chess.Board(fens[key]); assert str(zobrist_int64(board))==key
            if node.side=='us':
                expected={board.san(chess.Move.from_uci(u)):v for u,v in policies[key].items()}
                assert set(policies[key])=={m.uci() for m in board.legal_moves}
                assert all(abs(e.probability-expected[e.move])<1e-12 for e in node.edges)
            if node.side=='opp':
                rows=empirical['stats'][key]; a=empirical['aux'][key]
                n=sum(r['total'] for r in rows)+a['other_total']+a['horizon_total']
                ends=sum(a[k+'_total'] for k in ['term_normal','term_flag','term_other'])
                assert abs(node.end_probability-ends/(n+ends))<1e-12
                probs={e.move:e.probability for e in node.edges}
                for r in rows: assert abs(probs[r['move_san']]-r['total']/n)<1e-12
                assert abs(probs.get('__unresolved__',0)-(a['other_total']+a['horizon_total'])/n)<1e-12
            for edge in node.edges:
                if edge.move=='__unresolved__': continue
                child=board.copy(); child.push_san(edge.move)
                assert str(zobrist_int64(child))==edge.child; legal_edges+=1
        for entry in p['entries']:
            for name,book in p['books'].items():
                r=results['results'][event][str(entry)][name]; dynamic=r['exposure']
                assert dynamic['method']=='exact_dag'
                counts=fixed_recall_exposure(g,book,cfg,entry,dynamic['recall'])
                for key in book:
                    assert abs(counts[key]-dynamic['frequencies'][key])<1e-12
                    assert abs(RecallCurve(**p['curve'])(counts[key])-dynamic['recall'][key])<1e-12; checks+=2
                for label,recall in [('exposure',dynamic['recall']),('flat_85',{k:.85 for k in book})]:
                    exact=path_evaluate(g,book,replace(cfg,recall_by_position=recall)).report()
                    saved=dynamic['conditional_metrics'] if label=='exposure' else r[label]
                    for metric,value in exact.items():
                        if value is not None: assert abs(value-saved[metric])<1e-10,(event,name,metric); checks+=1
                    assert abs(exact['advantage']+exact['disadvantage']+exact['no_hit']-1)<1e-10
                    assert abs(exact['win']+exact['draw']+exact['loss']-1)<1e-10
                assert abs(dynamic['per_game_early_advantage_contribution']-entry*dynamic['conditional_metrics']['early_advantage'])<1e-12
        for phase,gates in p['gates'].items():
            for gate in gates: assert gate['passed'] and gate['child_cp']>=-100 and gate['parent_cp']-gate['child_cp']<=150
    out={'status':'passed','scenario_books':sum(len(v) for e in results['results'].values() for v in e.values()),
        'legal_graph_edges_checked':legal_edges,'independent_scalar_checks':checks,
        'results_sha256':sha256(output_path(PREFIX+'results.json')),'verifier_sha256':sha256(__file__)}
    save('verification',out); print(out)


if __name__=='__main__': main()
