"""Bounded local intervention study for the fixed 5.Be3 prescription."""
import argparse
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
import time
import chess
import chess.engine
import numpy as np
from python.eval_arrays import verify_eval_arrays,open_eval_arrays,lookup_evals,MISSING
from . import followthrough as previous
from .followthrough_data import sources
from .board_benchmark import isolated
from .hashed_board import HashedBoard
from .maia_policy import MaiaPolicy
from .continuation_audit import analyse_position
from .exposure_recall import topological
from .model import Graph,Node,Edge,graph_dict,graph_from_dict
from .io import ROOT,output_path,write_json,sha256,fingerprint
from .signed_continuation import intervention,flow_bounds
import json

PREFIX='be3_focus/'
CARD='-1436759193943917468'


def read(n): return json.loads(output_path(PREFIX+n+'.json').read_text())
def save(n,v): return write_json(PREFIX+n+'.json',v)


def identity():
    return {'code':{n:sha256(ROOT/'research/human_repertoire'/n) for n in
        ['be3_focus.py','signed_continuation.py','followthrough_data.py','board_benchmark.py']},
        'previous_manifest':sha256(output_path(previous.PREFIX+'manifest.json')),
        'previous_engine':sha256(output_path(previous.PREFIX+'engine.json')),
        'previous_results':sha256(output_path(previous.PREFIX+'results.json')),
        'plan':sha256(ROOT/'docs/human_repertoire/be3_focus_plan.md')}


def prepare():
    if output_path(PREFIX+'manifest.json').exists(): verified(); print('Verified preparation'); return
    started=time.monotonic(); old=previous.verified(); stamp=identity(); source_identity=sources()
    if source_identity!=old['sources']: raise ValueError('original empirical source changed')
    loader=isolated('followthrough_data.py',previous.PREFIX,PREFIX+'queries/')
    empirical=previous.read('empirical'); policies=previous.read('policies')
    fens={CARD:previous.read('fens')[CARD]}; nodes={e:{} for e in policies}; depths={CARD:0}
    mass={CARD:1.}; envelope=dict(mass); aliases={}; reasons={}; layers=[]; batches=0; calls=0; model=None
    for ply,cap in enumerate([1,64,256,1024]):
        if time.monotonic()-started>240: raise TimeoutError('preparation session cap; caches retained')
        selected=sorted(mass,key=lambda k:(-mass[k],k))[:cap]
        missing=sorted(k for k in selected if k not in empirical['stats'] or k not in empirical['aux'])
        for offset in range(0,len(missing),64):
            batches+=1
            if batches>24: raise ValueError('query batch cap')
            query={k:fens[k] for k in missing[offset:offset+64]}
            extra=loader.layer_snapshot(f'{ply}_{offset//64}',query,source_identity)
            empirical['stats'].update(extra['stats']); empirical['aux'].update(extra['aux'])
        cache_name=f'policies_{ply}'; cache_stamp={'identity':stamp,'selected':{k:fens[k] for k in selected}}
        if output_path(PREFIX+cache_name+'.json').exists():
            cache=read(cache_name)
            if cache['identity']!=cache_stamp: raise ValueError('policy cache drift')
            for e in policies: policies[e].update(cache['policies'][e])
        following=defaultdict(float)
        for k in mass:
            if k not in selected: reasons[k]='cap'
        for k in selected:
            board=HashedBoard(fens[k]); outcome=board.outcome(claim_draw=False)
            if outcome is not None:
                for event in nodes: nodes[event][k]=Node('terminal',cp=0 if outcome.winner is None else 100000 if outcome.winner else -100000,
                    wdl=(0,1,0) if outcome.winner is None else (1,0,0) if outcome.winner else (0,0,1))
                continue
            rows=empirical['stats'].get(k,[]); aux=empirical['aux'].get(k)
            if not rows or aux is None: reasons[k]='empirical_absent'; continue
            a=previous.previous.empirical_edges(rows,aux); probabilities={}
            for event in nodes:
                if board.turn:
                    if k not in policies[event]:
                        if model is None: model=MaiaPolicy()
                        calls+=1
                        if calls>512: raise ValueError('Maia inference cap')
                        policies[event][k]=model.probabilities(board,event,2000)
                    probabilities[event]={board.san(chess.Move.from_uci(m)):p for m,p in policies[event][k].items()}
                else: probabilities[event]=a['probabilities']
            children={}
            for move in sorted(probabilities['Blitz']):
                child=board.copy(); child.push_san(move); ck=previous.key(child)
                if ck in fens and fens[ck].split()[:4]!=child.fen().split()[:4]: raise ValueError('hash/FEN collision')
                fens.setdefault(ck,child.fen())
                if ck in depths and depths[ck]<=ply:
                    target=f'cut:{ply}:{k}:{ck}'; aliases[target]=ck; reasons[target]='earlier_layer'
                else:
                    target=ck; depths.setdefault(ck,ply+1)
                    probability=max(abs(float(move=='Be3')-ps[move]) for ps in probabilities.values()) if ply==0 else max(ps[move] for ps in probabilities.values())
                    following[ck]+=mass[k]*(1-a['end_probability'])*probability
                    envelope[ck]=following[ck]
                children[move]=target
            for event in nodes:
                edges=[Edge(m,children[m],p) for m,p in probabilities[event].items()]
                if not board.turn and a['unresolved_probability']:
                    u='unresolved:'+k; reasons[u]='empirical_tail'; edges.append(Edge('__unresolved__',u,a['unresolved_probability']))
                nodes[event][k]=Node('us' if board.turn else 'opp',tuple(edges),wdl=(0,1,0),
                    end_probability=a['end_probability'],end_wdl=a['end_wdl'])
        save(cache_name,{'identity':cache_stamp,'policies':{e:{k:policies[e][k] for k in selected if k in policies[e]} for e in policies}})
        layers.append({'ply':ply,'selected':len(selected),'arrivals':len(mass),'next':len(following)})
        print('layer',ply,'expanded',len(selected),'next',len(following),'Maia',calls,flush=True)
        mass=dict(following)
    for k in mass: reasons[k]='horizon'
    if len(fens)>60000: raise ValueError('position cap')
    directory=Path('E:/chess/eval_arrays'); db=Path('E:/chess/unified_eval_db.parquet')
    eval_verification=verify_eval_arrays(directory,db,adopt=False)
    eval_sources={str(p):fingerprint(p) for p in [db,directory/'eval_hash.npy',directory/'eval_cp.npy',directory/'eval_arrays.meta.json']}
    keys=sorted(fens); hashes,cps=open_eval_arrays(directory)
    values=lookup_evals(np.array([int(k) for k in keys],dtype=np.int64),hashes,cps)
    scores={k:int(v) for k,v in zip(keys,values) if v!=MISSING}; del hashes,cps
    scores.update({k:v for k,v in previous.read('scores').items() if k in fens})
    scores.update({k:r['cp'] for k,r in previous.read('engine')['base'].items() if k in fens})
    targets=sorted((k for k in fens if k not in scores),key=lambda k:(-envelope.get(k,0),k))[:128]
    graphs={}
    for event,ns in nodes.items():
        for node in list(ns.values()):
            for edge in node.edges:
                ns.setdefault(edge.child,Node('frontier',wdl=(0,1,0)))
        ns={k:replace(n,cp=n.cp if n.side=='terminal' else scores.get(aliases.get(k,k)),
            edges=tuple(replace(e,sound_cp=scores.get(aliases.get(e.child,e.child))) for e in n.edges)) for k,n in ns.items()}
        graph=Graph(CARD,ns); graph.validate()
        if topological(graph) is None or len(ns)>60000 or sum(len(n.edges) for n in ns.values())>150000: raise ValueError('graph bound/acyclic failure')
        graphs[event]=graph_dict(graph)
    snapshots={n:sha256(save(n,v)) for n,v in {'graphs':graphs,'fens':fens,'scores':scores,'empirical':empirical,
        'policies':policies,'aliases':aliases,'reasons':reasons,'envelope':envelope}.items()}
    if identity()!=stamp or sources()!=source_identity or any(fingerprint(p)!=fp for p,fp in eval_sources.items()): raise ValueError('source drift')
    save('manifest',{'identity':stamp,'snapshots':snapshots,'sources':source_identity,'eval_sources':eval_sources,
        'eval_verification':eval_verification,'layers':layers,'targets':targets,'new_maia_calls':calls,'query_batches':batches,
        'seconds':time.monotonic()-started,'engine_path':old['engine_path'],'engine_binary':old['engine_binary']})
    print('prepared',len(fens),'positions',len(scores),'scores',flush=True)


def verified():
    previous.verified(); p=read('manifest')
    if p['identity']!=identity() or any(sha256(output_path(PREFIX+n+'.json'))!=h for n,h in p['snapshots'].items()): raise ValueError('input drift')
    return p


def engine():
    p=verified(); stamp=sha256(output_path(PREFIX+'manifest.json'))
    cache=read('engine') if output_path(PREFIX+'engine.json').exists() else {'manifest_sha256':stamp,'records':{},'complete':False}
    if cache['manifest_sha256']!=stamp or fingerprint(p['engine_path'],full=True)!=p['engine_binary']: raise ValueError('engine drift')
    if cache['complete']: print('Verified engine cache'); return
    fens=read('fens'); started=time.monotonic()
    with chess.engine.SimpleEngine.popen_uci(p['engine_path'],timeout=10) as worker:
        worker.configure({'Threads':1,'Hash':32,'UCI_LimitStrength':False,'Skill Level':20,'UCI_ShowWDL':True,'SyzygyPath':''})
        cache['uci_id']=worker.id
        for index,k in enumerate(p['targets'],1):
            if k in cache['records']: continue
            if time.monotonic()-started>180: raise TimeoutError('engine session cap')
            cache['records'][k]=analyse_position(worker,fens[k],100000,2); save('engine',cache)
            if index%32==0: print('engine',index,'/',len(p['targets']),flush=True)
    if fingerprint(p['engine_path'],full=True)!=p['engine_binary']: raise ValueError('engine changed')
    cache['complete']=True; cache['seconds']=time.monotonic()-started; save('engine',cache)


def scored_graphs():
    audit=read('engine')
    if not audit['complete'] or audit['manifest_sha256']!=sha256(output_path(PREFIX+'manifest.json')): raise ValueError('engine incomplete/stale')
    scores=read('scores'); scores.update({k:r['cp'] for k,r in audit['records'].items()}); aliases=read('aliases')
    return {event:Graph(raw['root'],{k:replace(n,cp=n.cp if n.side=='terminal' else scores.get(aliases.get(k,k)),
        edges=tuple(replace(e,sound_cp=scores.get(aliases.get(e.child,e.child))) for e in n.edges))
        for k,n in graph_from_dict(raw).nodes.items()}) for event,raw in read('graphs').items()}


def prefix_stats(graph,book,recall):
    states={(graph.root,False,False):1.}; arrival=defaultdict(float); unresolved=0.
    for ply in range(9):
        following=defaultdict(float)
        for (k,hit,unknown),mass in states.items():
            n=graph.nodes[k]; unknown|=n.cp is None; hit|=n.cp is not None and abs(n.cp)>=300
            if k==CARD:
                arrival['total']+=mass
                if not hit and not unknown and ply==4: arrival['clean_at_ply4']+=mass
                elif not hit: arrival['other_nohit_arrival']+=mass
                continue
            if n.side=='frontier' and ply<=6 and not hit: unresolved+=mass
            if n.side not in ('us','opp') or ply==8: continue
            for edge in n.edges:
                p=edge.probability
                if n.side=='us' and k in book: p=(1-recall[k])*p+recall[k]*(edge.move==book[k])
                following[edge.child,hit,unknown]+=mass*(1-n.end_probability)*p
        states=following
    return {**arrival,'unresolved_prefix_upper_mass':unresolved}


def compare():
    p=verified(); old=previous.read('results'); old_manifest=previous.verified(); results={}
    assert old['gates']['exd4_Nxd4']['passed'] and old['gates']['sequence3']['passed']
    old_graphs=previous.scored_graphs(old_manifest,previous.read('engine'))
    for event,graph in scored_graphs().items():
        delta=intervention(graph); probabilities={e.move:e.probability for e in graph.nodes[CARD].edges}
        baseline=flow_bounds(graph,probabilities); results[event]={'raw_intervention':delta,'fallback_bounds':baseline,'scenarios':{}}
        for entry in old_manifest['entries']:
            prior=old['results'][event][str(entry)]; prepared=prior['sequence3']['contextual']; baseline_prior=prior['exd4_Nxd4']['contextual']
            prefix=prefix_stats(old_graphs[event],old_manifest['books']['exd4_Nxd4'],baseline_prior['recall'])
            recall=prepared['recall'][CARD]; scale=prefix.get('clean_at_ply4',0)*recall*.99**4
            interval=[scale*v for v in delta['interval']]
            mixed=flow_bounds(graph,{m:(1-recall)*q+recall*(m=='Be3') for m,q in probabilities.items()})
            prior_uncertainty=prefix['unresolved_prefix_upper_mass']+prefix.get('other_nohit_arrival',0)
            results[event]['scenarios'][str(entry)]={'recall':recall,'prefix':prefix,'scale':scale,
                'opening_conditional_difference_interval':interval,'all_games_difference_interval':[entry*x for x in interval],
                'including_prefix_uncertainty_interval':[interval[0]-prior_uncertainty,interval[1]+prior_uncertainty],
                'prepared_local_bounds':mixed,'resolved_at_fixed_boundary':interval[0]>0 or interval[1]<0}
        print(event,delta,flush=True)
    save('results',{'identity':{'manifest_sha256':sha256(output_path(PREFIX+'manifest.json')),'engine_sha256':sha256(output_path(PREFIX+'engine.json'))},'results':results})


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('phase',choices=['prepare','engine','compare'])
    globals()[parser.parse_args().phase]()
