"""Independent path-atom reconstruction of Be3 intervention intervals."""
from collections import defaultdict
from math import isclose
import chess
from . import be3_focus as run
from .hashed_board import HashedBoard
from .io import output_path,sha256
from .model import Config
from .oracle import path_evaluate


def atoms(graph, root_policy):
    root=graph.nodes[graph.root]; stack=[]; fixed=0.; uncertain=defaultdict(float)
    for edge in root.edges: stack.append((edge.child,1,(1-root.end_probability)*root_policy[edge.move]))
    count=0
    while stack:
        key,ply,mass=stack.pop(); count+=1
        if count>2000000: raise ValueError('independent path cap')
        if mass==0: continue
        n=graph.nodes[key]
        if n.cp is None:
            uncertain[key,ply]+=mass; continue
        if n.cp>=300: fixed+=mass*.99**ply; continue
        if n.cp<=-300: continue
        if n.side=='frontier' and ply<4: uncertain[key,ply]+=mass; continue
        if n.side not in ('us','opp') or ply==4: continue
        for edge in n.edges: stack.append((edge.child,ply+1,mass*(1-n.end_probability)*edge.probability))
    return fixed,dict(uncertain)


def interval(fixed, coefficients):
    return [fixed+sum(min(0,c)*.99**ply for (_,ply),c in coefficients.items()),
            fixed+sum(max(0,c)*.99**ply for (_,ply),c in coefficients.items())]


def main():
    p=run.verified(); results=run.read('results'); engine=run.read('engine'); aliases=run.read('aliases')
    assert results['identity']=={'manifest_sha256':sha256(output_path(run.PREFIX+'manifest.json')),
                                'engine_sha256':sha256(output_path(run.PREFIX+'engine.json'))}
    assert set(engine['records'])==set(p['targets']) and engine['complete']
    for k,row in engine['records'].items():
        board=HashedBoard(row['fen']); assert run.previous.key(board)==k
        assert row['limits']=={'nodes':100000,'seconds':2}
        for move in row['pv']: board.push_uci(move)
    fens=run.read('fens'); empirical=run.read('empirical'); policies=run.read('policies')
    prior=run.previous.read('results'); manifest=run.previous.verified()
    assert sha256(output_path(run.previous.PREFIX+'results.json'))==run.previous.read('verification')['results_sha256']
    assert manifest['books']['sequence3']=={**manifest['books']['exd4_Nxd4'],run.CARD:'Be3'}
    assert prior['gates']['exd4_Nxd4']['passed'] and prior['gates']['sequence3']['passed']
    checks=0; legal=0
    for event,graph in run.scored_graphs().items():
        for k,n in graph.nodes.items():
            if n.side not in ('us','opp'): continue
            board=HashedBoard(fens[k]); assert run.previous.key(board)==k
            rows=empirical['stats'][k]; aux=empirical['aux'][k]
            continuation=sum(r['total'] for r in rows)+aux['other_total']+aux['horizon_total']
            endings=sum(aux[t+'_total'] for t in ['term_normal','term_flag','term_other'])
            assert isclose(n.end_probability,endings/(continuation+endings),abs_tol=1e-12)
            if n.side=='us':
                expected={board.san(chess.Move.from_uci(m)):v for m,v in policies[event][k].items()}
                assert set(policies[event][k])=={m.uci() for m in board.legal_moves}
            else:
                expected={r['move_san']:r['total']/continuation for r in rows}
                if aux['other_total']+aux['horizon_total']: expected['__unresolved__']=(aux['other_total']+aux['horizon_total'])/continuation
            assert {e.move for e in n.edges}==set(expected)
            for e in n.edges:
                assert isclose(e.probability,expected[e.move],abs_tol=1e-12)
                if e.move=='__unresolved__': continue
                child=board.copy(); child.push_san(e.move)
                assert run.previous.key(child)==aliases.get(e.child,e.child); legal+=1
        q={e.move:e.probability for e in graph.nodes[run.CARD].edges}
        fixed_fallback,atoms_fallback=atoms(graph,q)
        fixed_move,atoms_move=atoms(graph,{m:float(m=='Be3') for m in q})
        delta_atoms={k:atoms_move.get(k,0)-atoms_fallback.get(k,0) for k in atoms_move.keys()|atoms_fallback.keys()}
        expected_delta=interval(fixed_move-fixed_fallback,delta_atoms)
        for x,y in zip(expected_delta,results['results'][event]['raw_intervention']['interval']):
            assert isclose(x,y,abs_tol=1e-11); checks+=1
        for entry,case in results['results'][event]['scenarios'].items():
            source=prior['results'][event][entry]['sequence3']['contextual']
            assert case['recall']==source['recall'][run.CARD]
            assert isclose(case['prefix']['total'],source['frequencies'][run.CARD]/float(entry),abs_tol=1e-12)
            assert isclose(case['prefix']['clean_at_ply4'],case['prefix']['total'],abs_tol=1e-12)
            assert isclose(case['scale'],case['prefix']['clean_at_ply4']*case['recall']*.99**4,abs_tol=1e-12)
            checks+=4
            r=case['recall']; mixed={m:(1-r)*v+r*(m=='Be3') for m,v in q.items()}
            fixed,coefficients=atoms(graph,mixed)
            expected=interval(fixed,coefficients)
            for x,y in zip(expected,case['prepared_local_bounds']['interval']):
                assert isclose(x,y,abs_tol=1e-11); checks+=1
            for book,cfg in [({},Config(horizon=4)),({run.CARD:'Be3'},Config(horizon=4,recall=r))]:
                metrics=path_evaluate(graph,book,cfg)
                assert isclose(metrics.win+metrics.draw+metrics.loss,1,abs_tol=1e-9)
                assert isclose(metrics.advantage+metrics.disadvantage+metrics.no_hit,1,abs_tol=1e-9)
                bound=expected if book else interval(fixed_fallback,atoms_fallback)
                assert bound[0]-1e-10<=metrics.early_advantage<=bound[1]+1e-10; checks+=3
            for x,y in zip(expected_delta,case['opening_conditional_difference_interval']):
                assert isclose(case['scale']*x,y,abs_tol=1e-12); checks+=1
            for x,y in zip(case['opening_conditional_difference_interval'],case['all_games_difference_interval']):
                assert isclose(float(entry)*x,y,abs_tol=1e-12); checks+=1
            difference=case['opening_conditional_difference_interval']
            assert case['resolved_at_fixed_boundary']==(difference[0]>0 or difference[1]<0)
        print(event,'verified',flush=True)
    out={'passed':True,'scenarios':6,'legal_edges':legal,'independent_checks':checks,
         'results_sha256':sha256(output_path(run.PREFIX+'results.json')),'verifier_sha256':sha256(__file__)}
    run.save('verification',out); print(out)


if __name__=='__main__': main()
