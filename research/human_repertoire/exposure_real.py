"""Exact shallow empirical book comparison with frequency-dependent recall."""
import argparse
from dataclasses import replace
import json
from pathlib import Path
import time
import chess
import numpy as np
from python.zobrist import zobrist_int64
from python.eval_arrays import verify_eval_arrays, open_eval_arrays, lookup_evals, MISSING
from . import maia_stratified as previous
from .maia_policy import MaiaPolicy
from .hashed_board import HashedBoard
from .io import ROOT,output_path,write_json,sha256,fingerprint
from .model import Graph,Node,Edge,Config,evaluate,graph_dict,graph_from_dict
from .exposure_recall import solve,RecallCurve,fixed_recall_exposure
from .extract_empirical_positions import PREFIX,main as verify_snapshot


def read(n): return json.loads(output_path(PREFIX+n+'.json').read_text())
def save(n,v): return write_json(PREFIX+n+'.json',v)


def identity():
    return {'code':{n:sha256(ROOT/'research/human_repertoire'/n) for n in ['exposure_real.py','exposure_recall.py','maia_policy.py','model.py','hashed_board.py']},
        'plan_sha256':sha256(ROOT/'docs/human_repertoire/exposure_plan.md'),
        'empirical_sha256':sha256(output_path(PREFIX+'empirical.json')),
        'maia_config_sha256':sha256(ROOT/'docs/human_repertoire/maia_config.json')}


def empirical_edges(rows, aux):
    known=sum(r['total'] for r in rows)
    other=aux.get('other_total',0); horizon=aux.get('horizon_total',0)
    endings=sum(aux.get(k+'_total',0) for k in ['term_normal','term_flag','term_other'])
    continuation=known+other+horizon; total=continuation+endings
    if not total or not continuation: raise ValueError('no supported continuation')
    if any(v<0 for v in [known,other,horizon,endings]): raise ValueError('negative mass')
    wdl=tuple(sum(aux.get(k+'_'+field,0) for k in ['term_normal','term_flag','term_other'])/endings for field in ['white_wins','draws','black_wins']) if endings else (0,1,0)
    return {'denominator':total,'stored':known,'other':other,'horizon':horizon,'endings':endings,
        'end_probability':endings/total,'end_wdl':wdl,'probabilities':{r['move_san']:r['total']/continuation for r in rows},
        'unresolved_probability':(other+horizon)/continuation}


def prepare():
    verify_snapshot(); empirical=read('empirical')
    if output_path(PREFIX+'manifest.json').exists():
        verified(); print('Verified existing exposure preparation'); return
    start=time.monotonic(); policy=MaiaPolicy(); graphs={}; fens={}; policies={}; accounting={}; inference_calls=0
    root,books,gates,gate_fens=previous.equal.packages(previous.config()['cards'])
    books['d4_only']={str(zobrist_int64(root)):'d4'}; fens.update(gate_fens)
    for event in ['Blitz','Rapid']:
        nodes={}; queue=[(HashedBoard(root.fen()),0)]; visited=set(); policies[event]={}
        while queue:
            if time.monotonic()-start>360: raise TimeoutError('bounded graph preparation guard')
            b,ply=queue.pop(); key=str(zobrist_int64(b))
            if key in visited: continue
            visited.add(key); fens[key]=b.fen()
            if len(visited)>35000: raise ValueError('graph position cap')
            if b.outcome(claim_draw=False) is not None:
                outcome=b.outcome(claim_draw=False)
                nodes[key]=Node('terminal',cp=(100000 if outcome.winner else -100000) if outcome.winner is not None else 0,
                    wdl=(1,0,0) if outcome.winner else (0,0,1) if outcome.winner is not None else (0,1,0)); continue
            if ply==3:
                nodes[key]=Node('frontier',wdl=(0,1,0)); continue
            rows=empirical['stats'].get(key,[]); aux=empirical['aux'].get(key,{})
            accounting[key]=empirical_edges(rows,aux) if rows else None
            a=accounting[key]; endings=a['end_probability'] if a else 0; end_wdl=a['end_wdl'] if a else (0,1,0)
            if b.turn:
                inference_calls+=1
                if inference_calls>900: raise ValueError('Maia inference cap')
                ps=policy.probabilities(b,event,2000); policies[event][key]=ps
                moves={b.san(chess.Move.from_uci(u)):p for u,p in ps.items()}
            else:
                if not a: raise ValueError('opponent empirical position absent')
                moves=a['probabilities']
            edges=[]
            for san,p in moves.items():
                child=b.copy(); child.push_san(san); ck=str(zobrist_int64(child))
                if ck in fens and fens[ck].split()[:4]!=child.fen().split()[:4]: raise ValueError('FEN drift')
                fens.setdefault(ck,child.fen()); edges.append(Edge(san,ck,p)); queue.append((child,ply+1))
            if not b.turn and a['unresolved_probability']:
                unknown='unresolved:'+key; nodes[unknown]=Node('frontier',wdl=(0,1,0))
                edges.append(Edge('__unresolved__',unknown,a['unresolved_probability']))
            nodes[key]=Node('us' if b.turn else 'opp',tuple(edges),wdl=(0,1,0),end_probability=endings,end_wdl=end_wdl)
        if sum(len(n.edges) for n in nodes.values())>100000: raise ValueError('edge cap')
        graphs[event]=Graph(str(zobrist_int64(root)),nodes)
        print(event,'graph',len(nodes),'nodes',flush=True)
    array_dir=Path('E:/chess/eval_arrays'); db=Path('E:/chess/unified_eval_db.parquet')
    verification=verify_eval_arrays(array_dir,db,adopt=False)
    sources={str(p):fingerprint(p) for p in [db,array_dir/'eval_hash.npy',array_dir/'eval_cp.npy',array_dir/'eval_arrays.meta.json']}
    keys=sorted(fens)
    if len(keys)>35000: raise ValueError('eval key cap')
    mmh,mmc=open_eval_arrays(array_dir); values=lookup_evals(np.array([int(k) for k in keys],dtype=np.int64),mmh,mmc)
    scores={k:int(cp) for k,cp in zip(keys,values) if cp!=MISSING}; del mmh,mmc
    _,_,_,prior=previous.source(previous.config()); previous.verified(previous.config()); audit=previous.read('engine_audit')
    prior['base'].update(audit['base']); prior['deeper'].update(audit['deeper'])
    overlays={s:{k:v for k,v in rs.items() if k in fens} for s,rs in prior.items()}
    scores.update({k:r['cp'] for k,r in overlays['base'].items()})
    gate_checks={s:previous.panel.check_gates(gates,{k:r['cp'] for k,r in rs.items()},Config()) for s,rs in overlays.items()}
    if any(not g['passed'] for gs in gate_checks.values() for g in gs): raise ValueError('uncertified book gate')
    for event,g in graphs.items():
        nodes={k:replace(n,cp=n.cp if n.side=='terminal' else scores.get(k),edges=tuple(replace(e,sound_cp=scores.get(e.child)) for e in n.edges)) for k,n in g.nodes.items()}
        graphs[event]=Graph(g.root,nodes); graphs[event].validate()
    for p,fp in sources.items():
        if fingerprint(p)!=fp: raise ValueError('eval source changed')
    snapshots={n:sha256(save(n,v)) for n,v in {'graphs':{e:graph_dict(g) for e,g in graphs.items()},'policies':policies,'fens':fens,
        'scores':scores,'overlay':overlays,'accounting':accounting}.items()}
    save('manifest',{'identity':identity(),'books':books,'gates':gate_checks,'eval_sources':sources,'eval_verification':verification,
        'snapshots':snapshots,'inference_calls':inference_calls,'seconds':time.monotonic()-start,
        'entries':[.1,.01,.001],'horizon':3,'curve':{'midpoint':.001,'power':2}})
    print({'positions':len(fens),'scored':len(scores),'inference_calls':inference_calls,'seconds':time.monotonic()-start},flush=True)


def verified():
    p=read('manifest')
    if p['identity']!=identity() or any(sha256(output_path(PREFIX+n+'.json'))!=h for n,h in p['snapshots'].items()): raise ValueError('exposure frozen input drift')
    return p


def compare():
    p=verified(); results={}; cfg=Config(horizon=3,recall=.85,max_states=200000)
    for event,data in read('graphs').items():
        graph=graph_from_dict(data); results[event]={}
        for entry in p['entries']:
            results[event][str(entry)]={}
            for name,book in p['books'].items():
                dynamic=solve(graph,book,cfg,entry,RecallCurve(**p['curve']))
                flat=evaluate(graph,book,cfg).report()
                for metrics in [dynamic['conditional_metrics'],flat]:
                    metrics['early_interval']=[metrics['certain_early_advantage'],min(1,metrics['certain_early_advantage']+metrics['unresolved_first_hit_mass'])]
                results[event][str(entry)][name]={'exposure':dynamic,'flat_85':flat}
            print(event,entry,'complete',flush=True)
    save('results',{'identity':identity(),'manifest_sha256':sha256(output_path(PREFIX+'manifest.json')),'results':results})


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('phase',choices=('prepare','compare'))
    globals()[p.parse_args().phase]()
