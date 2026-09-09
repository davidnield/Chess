"""Independent paths, legal boards, empirical mass and recall checks."""
from dataclasses import asdict, replace
from math import isclose
import chess
from . import followthrough as run
from .context_recall import ContextCurve
from .followthrough_data import PREFIX, read, save
from .io import output_path, sha256
from .model import Config, book_cost
from .oracle import path_evaluate
from .verify_context import path_context
from .hashed_board import HashedBoard


def main():
    p=run.verified(); result=read('results'); engine=read('engine')
    assert result['identity']=={'manifest_sha256':sha256(output_path(PREFIX+'manifest.json')),
                               'engine_sha256':sha256(output_path(PREFIX+'engine.json'))}
    assert engine['complete']
    for phase,nodes,seconds in [('base',100000,2),('deeper',500000,5)]:
        assert set(engine[phase])==set(p['targets'][phase])
        for k,row in engine[phase].items():
            board=HashedBoard(row['fen']); assert run.key(board)==k
            assert row['limits']=={'nodes':nodes,'seconds':seconds}
            for uci in row['pv']: board.push_uci(uci)
    fens=read('fens'); empirical=read('empirical'); aliases=read('aliases'); policies=read('policies')
    cfg=Config(horizon=8,max_states=2000000); counts=0; cases=0; legal=0
    for event,g in run.scored_graphs(p,engine).items():
        g.validate()
        for k,node in g.nodes.items():
            if node.side not in ('us','opp'): continue
            board=HashedBoard(fens[k]); assert run.key(board)==k
            rows=empirical['stats'][k]; aux=empirical['aux'][k]
            continuation=sum(row['total'] for row in rows)+aux['other_total']+aux['horizon_total']
            endings=sum(aux[t+'_total'] for t in ['term_normal','term_flag','term_other'])
            assert isclose(node.end_probability,endings/(continuation+endings),abs_tol=1e-12)
            if node.side=='us':
                expected={board.san(chess.Move.from_uci(m)):v for m,v in policies[event][k].items()}
                assert set(policies[event][k])=={m.uci() for m in board.legal_moves}
            else:
                expected={row['move_san']:row['total']/continuation for row in rows}
                if aux['other_total']+aux['horizon_total']:
                    expected['__unresolved__']=(aux['other_total']+aux['horizon_total'])/continuation
            assert {e.move for e in node.edges}==set(expected)
            for edge in node.edges:
                assert isclose(edge.probability,expected[edge.move],abs_tol=1e-12)
                if edge.move=='__unresolved__': continue
                child=board.copy(); child.push_san(edge.move)
                assert run.key(child)==aliases.get(edge.child,edge.child); legal+=1
        for entry,books in result['results'][event].items():
            for name,row in books.items():
                book=p['books'][name]
                assert row['cost']==book_cost(g,book)
                independently_eligible=True
                for k,move in book.items():
                    child=HashedBoard(fens[k]); child.push_san(move); ck=run.key(child)
                    for phase in ['base','deeper']:
                        parent_cp=engine[phase][k]['cp']; child_cp=engine[phase][ck]['cp']
                        independently_eligible &= child_cp>=-100 and parent_cp-child_cp<=150
                assert row['eligible']==independently_eligible
                if not row['eligible']: continue
                r=row['contextual']; exposure,anchor,support,recall=path_context(g,book,cfg,float(entry),
                    r['recall'],{p['root']:.99},4,asdict(ContextCurve()))
                for k in book:
                    for value,expected in [(r['frequencies'][k],exposure[k]),(r['recall'][k],recall[k]),
                                           (r['components'][k]['mean_support'],support[k])]:
                        assert isclose(value,expected,rel_tol=1e-9,abs_tol=1e-12); counts+=1
                oracle=path_evaluate(g,book,replace(cfg,recall_by_position=r['recall'])).report()
                for metric,value in oracle.items():
                    expected=r['conditional_metrics'][metric]
                    assert value is None and expected is None or value is not None and isclose(value,expected,abs_tol=1e-9)
                    counts+=1
                assert isclose(oracle['win']+oracle['draw']+oracle['loss'],1,abs_tol=1e-9)
                assert isclose(oracle['advantage']+oracle['disadvantage']+oracle['no_hit'],1,abs_tol=1e-9)
                assert isclose(r['per_game_early_advantage_contribution'],float(entry)*oracle['early_advantage'],abs_tol=1e-12)
                cases+=1
        print(event,'independent verification complete',flush=True)
    out={'passed':True,'eligible_cases_verified':cases,'legal_edges':legal,'scalar_checks':counts,
         'results_sha256':sha256(output_path(PREFIX+'results.json')),'verifier_sha256':sha256(__file__)}
    save('verification',out); print(out)


if __name__=='__main__': main()
