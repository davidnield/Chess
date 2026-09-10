"""Configurable bounded repertoire generation: prepare, engine, build, verify, all.

Run all phases in the isolated Maia environment, or prepare there and the other
phases in the ordinary research environment. No historical manifests required.
"""
import argparse
from collections import defaultdict
from dataclasses import asdict,replace
import json
from pathlib import Path
import re
import time
import chess
import chess.engine
import chess.pgn
import numpy as np
from python.zobrist import zobrist_int64
from python.eval_arrays import verify_eval_arrays,open_eval_arrays,lookup_evals,MISSING
from .builder_data import source_identity,snapshot,accounting
from .budget_search import DagScorer,CommonScorer,search
from .context_recall import solve,ContextCurve
from .continuation_audit import analyse_position
from .exposure_recall import topological
from .hashed_board import HashedBoard
from .io import ROOT,output_path,write_json,sha256,fingerprint
from .maia_policy import MaiaPolicy
from .model import Config,Graph,Node,Edge,graph_dict,graph_from_dict,book_cost,eligible

DEFAULT_CONFIG=ROOT/'docs/human_repertoire/builder_config.json'


def key(board): return str(zobrist_int64(board))


class Builder:
    def __init__(self, config=DEFAULT_CONFIG):
        self.config_path=Path(config); self.c=json.loads(self.config_path.read_text())
        if not re.fullmatch('[a-z0-9_]+',self.c['run_name']): raise ValueError('invalid run name')
        if self.c['events']!=['Blitz','Rapid'] or not 0<=self.c['entry_probability']<=1: raise ValueError('unsupported profile')
        if any(type(v) is not int or v<=0 for v in self.c['budgets']): raise ValueError('invalid budgets')
        self.prefix='builder/'+self.c['run_name']+'/'
        self.root=HashedBoard()
        for move in self.c['root_moves']: self.root.push_san(move)
        if self.root.turn: raise ValueError('initial implementation expects Black to move after a White opening move')
        self.root_ply=len(self.c['root_moves']); self.cfg=Config(horizon=self.c['horizon'],max_states=self.c['max_states'])
        self.curve=ContextCurve()

    def read(self,n): return json.loads(output_path(self.prefix+n+'.json').read_text())
    def save(self,n,v): return write_json(self.prefix+n+'.json',v)
    def identity(self):
        return {'config':self.c,'code':{n:sha256(ROOT/'research/human_repertoire'/n) for n in
            ['repertoire_builder.py','builder_data.py','budget_search.py','context_recall.py','model.py',
             'maia_policy.py','hashed_board.py','exposure_recall.py','continuation_audit.py','extract_empirical_positions.py']},
            'maia_config_sha256':sha256(ROOT/'docs/human_repertoire/maia_config.json'),
            'plan_sha256':sha256(ROOT/'docs/human_repertoire/builder_plan.md')}

    def prepare(self):
        if output_path(self.prefix+'manifest.json').exists(): self.verified(); print('Verified existing builder preparation'); return
        c=self.c; started=time.monotonic(); stamp=self.identity()
        identities={kind:source_identity(c[kind]) for kind in ['source_stats','source_aux','reference']}
        metadata_path=Path(c['reference']+'.meta.json'); metadata={'identity':fingerprint(metadata_path,full=True),'contents':json.loads(metadata_path.read_text())}
        fens={key(self.root):self.root.fen()}; paths={key(self.root):[]}; depths={key(self.root):0}
        policy={e:{} for e in c['events']}; nodes={e:{} for e in c['events']}; data={'stats':{},'aux':{},'reference':{}}
        mass={key(self.root):1.}; envelope=dict(mass); route_mass=dict(mass); aliases={}; reasons={}; layers=[]; model=None; calls=0
        for ply in range(c['horizon']):
            if time.monotonic()-started>c['preparation_seconds']: raise TimeoutError('preparation session cap; query/policy caches retained')
            selected=sorted(mass,key=lambda k:(-mass[k],k))[:c['expanded_per_ply']]
            for offset in range(0,len(selected),64):
                query={k:fens[k] for k in selected[offset:offset+64]}
                extra=snapshot(self.prefix,f'{ply}_{offset//64}',query,c,identities)
                for kind in data: data[kind].update(extra[kind])
            cache_name=f'policies_{ply}'; cache_stamp={'identity':stamp,'selected':{k:fens[k] for k in selected}}
            if output_path(self.prefix+cache_name+'.json').exists():
                cached=self.read(cache_name)
                if cached['identity']!=cache_stamp: raise ValueError('policy cache drift')
                for e in policy: policy[e].update(cached['policy'][e])
            following=defaultdict(float)
            for k in mass:
                if k not in selected: reasons[k]='expansion_cap'
            for k in selected:
                board=HashedBoard(fens[k]); outcome=board.outcome(claim_draw=False)
                if outcome is not None:
                    for e in nodes: nodes[e][k]=Node('terminal',cp=0 if outcome.winner is None else 100000 if outcome.winner else -100000,
                        wdl=(0,1,0) if outcome.winner is None else (1,0,0) if outcome.winner else (0,0,1))
                    continue
                rows=data['stats'].get(k,[]); aux=data['aux'].get(k)
                if not rows or aux is None: reasons[k]='empirical_absent'; continue
                a=accounting(rows,aux); ps={}
                for event in nodes:
                    if board.turn:
                        if k not in policy[event]:
                            if model is None: model=MaiaPolicy()
                            calls+=1
                            if calls>c['max_inference_calls']: raise ValueError('inference cap')
                            policy[event][k]=model.probabilities(board,event,2000)
                        ps[event]={board.san(chess.Move.from_uci(m)):p for m,p in policy[event][k].items()}
                    else: ps[event]=a['probabilities']
                children={}
                reference=data['reference'].get(k,{}).get('best_move')
                for move in sorted(ps['Blitz']):
                    child=board.copy(); child.push_san(move); ck=key(child)
                    if ck in fens and fens[ck].split()[:4]!=child.fen().split()[:4]: raise ValueError('hash/FEN drift')
                    fens.setdefault(ck,child.fen()); paths.setdefault(ck,paths[k]+[move])
                    if ck in depths and depths[ck]<=ply:
                        target=f'cut:{ply}:{k}:{ck}'; aliases[target]=ck; reasons[target]='earlier_layer'
                    else:
                        target=ck; depths.setdefault(ck,ply+1)
                        p=max(ps[e][move] for e in ps)
                        if board.turn and move==reference: p=1.
                        flow=mass[k]*(1-a['end_probability'])*p; following[ck]+=flow; envelope[ck]=following[ck]
                        if flow>route_mass.get(ck,0): paths[ck]=paths[k]+[move]; route_mass[ck]=flow
                    children[move]=target
                for event in nodes:
                    edges=[Edge(m,children[m],p) for m,p in ps[event].items()]
                    if not board.turn and a['tail']:
                        target='unresolved:'+k; reasons[target]='empirical_tail'; edges.append(Edge('__unresolved__',target,a['tail']))
                    nodes[event][k]=Node('us' if board.turn else 'opp',tuple(edges),wdl=(0,1,0),end_probability=a['end_probability'],end_wdl=a['end_wdl'])
            self.save(cache_name,{'identity':cache_stamp,'policy':{e:{k:policy[e][k] for k in selected if k in policy[e]} for e in policy}})
            layers.append({'ply':ply,'selected':len(selected),'arrivals':len(mass),'next':len(following)})
            mass=dict(following); print('builder layer',ply,'expanded',len(selected),'next',len(mass),'Maia',calls,flush=True)
        for k in mass: reasons[k]='horizon'
        if len(fens)>c['max_positions']: raise ValueError('position cap')
        candidate_keys=sorted((k for k,n in nodes['Blitz'].items() if n.side=='us'),key=lambda k:(-envelope.get(k,0),k))[:c['candidate_positions']]
        proposals={}
        for k in candidate_keys:
            board=HashedBoard(fens[k]); moves=[e.move for e in nodes['Blitz'][k].edges]
            mean={m:sum(next(e.probability for e in nodes[event][k].edges if e.move==m) for event in nodes)/len(nodes) for m in moves}
            reference=data['reference'].get(k,{}).get('best_move')
            proposals[k]=list(dict.fromkeys(([reference] if reference else [])+sorted(moves,key=lambda m:(-mean[m],m))))[:c['moves_per_position']]
        targets=list(candidate_keys)
        for slot in range(c['moves_per_position']):
            for k,moves in proposals.items():
                if slot<len(moves):
                    b=HashedBoard(fens[k]); b.push_san(moves[slot]); targets.append(key(b))
        targets=list(dict.fromkeys(targets))[:c['max_engine_targets']]
        directory=Path(c['eval_arrays']); verification=verify_eval_arrays(directory,Path(c['eval_db']),adopt=False)
        eval_ids={str(p):fingerprint(p) for p in [Path(c['eval_db']),directory/'eval_hash.npy',directory/'eval_cp.npy',directory/'eval_arrays.meta.json']}
        keys=sorted(fens); hashes,cps=open_eval_arrays(directory)
        values=lookup_evals(np.array([int(k) for k in keys],dtype=np.int64),hashes,cps)
        scores={k:int(v) for k,v in zip(keys,values) if v!=MISSING}; del hashes,cps
        graphs={}
        for event,ns in nodes.items():
            for node in list(ns.values()):
                for edge in node.edges: ns.setdefault(edge.child,Node('frontier',wdl=(0,1,0)))
            ns={k:replace(n,cp=n.cp if n.side=='terminal' else scores.get(aliases.get(k,k)),
                edges=tuple(replace(e,sound_cp=scores.get(aliases.get(e.child,e.child))) for e in n.edges)) for k,n in ns.items()}
            graph=Graph(key(self.root),ns); graph.validate()
            if topological(graph) is None or len(ns)>c['max_positions'] or sum(len(n.edges) for n in ns.values())>c['max_edges']: raise ValueError('graph cap/DAG contract')
            graphs[event]=graph_dict(graph)
        snapshots={n:sha256(self.save(n,v)) for n,v in {'graphs':graphs,'fens':fens,'paths':paths,'scores':scores,'data':data,
            'policies':policy,'aliases':aliases,'reasons':reasons,'envelope':envelope}.items()}
        if stamp!=self.identity() or any(source_identity(c[k])!=fp for k,fp in identities.items()) or any(fingerprint(p)!=fp for p,fp in eval_ids.items()): raise ValueError('source drift')
        self.save('manifest',{'identity':stamp,'sources':identities,'reference_metadata':metadata,'eval_sources':eval_ids,'eval_verification':verification,
            'snapshots':snapshots,'candidate_proposals':proposals,'engine_targets':targets,'engine_binary':fingerprint(c['engine_path'],full=True),
            'layers':layers,'new_maia_calls':calls,'seconds':time.monotonic()-started})
        print('builder prepared',len(fens),'positions',len(proposals),'candidate positions',flush=True)

    def verified(self):
        p=self.read('manifest')
        if p['identity']!=self.identity() or any(sha256(output_path(self.prefix+n+'.json'))!=h for n,h in p['snapshots'].items()): raise ValueError('builder input drift')
        return p

    def engine(self):
        p=self.verified(); stamp=sha256(output_path(self.prefix+'manifest.json')); c=self.c
        cache=self.read('engine') if output_path(self.prefix+'engine.json').exists() else {'manifest_sha256':stamp,'records':{},'complete':False}
        if cache['manifest_sha256']!=stamp or fingerprint(c['engine_path'],full=True)!=p['engine_binary']: raise ValueError('engine drift')
        if cache['complete']: print('Verified builder engine cache'); return
        fens=self.read('fens'); started=time.monotonic()
        with chess.engine.SimpleEngine.popen_uci(c['engine_path'],timeout=10) as worker:
            worker.configure({'Threads':1,'Hash':32,'UCI_LimitStrength':False,'Skill Level':20,'UCI_ShowWDL':True,'SyzygyPath':''}); cache['uci_id']=worker.id
            for index,k in enumerate(p['engine_targets'],1):
                if k in cache['records']: continue
                if time.monotonic()-started>c['engine_session_seconds']: raise TimeoutError('engine session cap; resume cached targets')
                cache['records'][k]=analyse_position(worker,fens[k],c['engine_nodes'],c['engine_position_seconds']); self.save('engine',cache)
                if index%32==0 or index==len(p['engine_targets']): print('builder engine',index,'/',len(p['engine_targets']),flush=True)
        if fingerprint(c['engine_path'],full=True)!=p['engine_binary']: raise ValueError('engine changed')
        cache.update(complete=True,seconds=time.monotonic()-started); self.save('engine',cache)

    def graphs(self):
        p=self.verified(); audit=self.read('engine')
        if not audit['complete'] or audit['manifest_sha256']!=sha256(output_path(self.prefix+'manifest.json')): raise ValueError('engine incomplete/stale')
        scores=self.read('scores'); scores.update({k:r['cp'] for k,r in audit['records'].items()}); aliases=self.read('aliases')
        return {event:Graph(raw['root'],{k:replace(n,cp=n.cp if n.side=='terminal' else scores.get(aliases.get(k,k)),
            edges=tuple(replace(e,sound_cp=scores.get(aliases.get(e.child,e.child))) for e in n.edges))
            for k,n in graph_from_dict(raw).nodes.items()}) for event,raw in self.read('graphs').items()}

    def candidate_set(self,graphs):
        p=self.read('manifest'); records=self.read('engine')['records']; fens=self.read('fens'); data=self.read('data'); paths=self.read('paths')
        candidates={}; excluded=[]
        for k,moves in p['candidate_proposals'].items():
            approved=[]
            for move in moves:
                b=HashedBoard(fens[k]); b.push_san(move); child=key(b)
                # Both sides of a candidate gate must have this run's base-budget audit.
                if k not in records or child not in records: excluded.append({'position':k,'move':move,'reason':'gate_not_audited'}); continue
                if move in eligible(graphs['Blitz'],self.cfg,k): approved.append(move)
                else: excluded.append({'position':k,'move':move,'reason':'soundness_gate'})
            if approved: candidates[k]=approved
        frequencies=defaultdict(float)
        for graph in graphs.values():
            states={graph.root:self.c['entry_probability']}
            for ply in range(self.cfg.horizon+1):
                following=defaultdict(float)
                for k,mass in states.items():
                    if k in candidates: frequencies[k]+=mass/len(graphs)
                    n=graph.nodes[k]
                    if n.side not in ('us','opp') or ply==self.cfg.horizon: continue
                    for e in n.edges: following[e.child]+=mass*(1-n.end_probability)*e.probability
                states=following
        order=sorted(candidates,key=lambda k:(-frequencies[k],k))
        reference={k:data['reference'][k]['best_move'] for k in order if k in data['reference'] and data['reference'][k]['best_move'] in candidates[k]}
        packets=[]
        for k,moves in candidates.items():
            for move in moves:
                packet={k:move}; board=self.root.copy()
                for san in paths[k]:
                    ancestor=key(board)
                    if ancestor in candidates and san in candidates[ancestor]: packet[ancestor]=san
                    board.push_san(san)
                packets.append(packet)
        return {'candidates':candidates,'reference':reference,'reference_order':list(reference),'packets':packets,'fallback_frequencies':dict(frequencies),'excluded':excluded}

    def build(self):
        graphs=self.graphs(); stamp={'manifest':sha256(output_path(self.prefix+'manifest.json')),'engine':sha256(output_path(self.prefix+'engine.json'))}
        if output_path(self.prefix+'results.json').exists():
            if self.read('results')['identity']!=stamp: raise ValueError('result drift')
            print('Verified existing books'); return
        c=self.c; pool=self.candidate_set(graphs); self.save('candidates',pool)
        anchors={k:v for k,v in c['anchors'].items() if k in graphs['Blitz'].nodes and graphs['Blitz'].nodes[k].side in ('us','opp')}
        scorers=CommonScorer([DagScorer(g,self.cfg,c['entry_probability'],anchors,self.root_ply) for g in graphs.values()])
        search_results={}
        for name,perfect in [('human',False),('perfect_selection',True)]:
            cached_path=output_path(self.prefix+'search_'+name+'.json')
            if cached_path.exists():
                cached=self.read('search_'+name)
                if cached['identity']!=stamp: raise ValueError('search cache drift')
                rows=cached['results']
            else:
                rows=search(scorers,pool['candidates'],pool['packets'],c['budgets'],pool['reference'],perfect=perfect,
                    max_evaluations=c['max_evaluations_per_budget'],max_seconds=c['search_seconds_per_budget'],proposal_width=c['proposal_width'])
                self.save('search_'+name,{'identity':stamp,'results':rows})
            search_results[name]=rows
            print(name,[(b,r['cost'],r['stopped']) for b,r in rows.items()],flush=True)
        results={}
        for budget in c['budgets']:
            b=str(budget); ref=dict(list(pool['reference'].items())[:budget])
            books={'reference':ref,**{name:rows[b]['book'] for name,rows in search_results.items()}}
            results[b]={}
            for method,book in books.items():
                contexts={e:solve(g,book,self.cfg,c['entry_probability'],anchors=anchors,root_ply=self.root_ply) for e,g in graphs.items()}
                for result in contexts.values():
                    m=result['conditional_metrics']; m['early_interval']=[m['certain_early_advantage'],min(1,m['certain_early_advantage']+m['unresolved_first_hit_mass'])]
                row={'book':book,'cost':book_cost(graphs['Blitz'],book),'contexts':contexts,
                     'mean_human_score':sum(r['conditional_metrics']['certain_early_advantage'] for r in contexts.values())/len(contexts)}
                self.export(method,budget,row)
                results[b][method]=row
                print('exported',method,budget,'cost',row['cost'],'score',row['mean_human_score'],flush=True)
        self.save('results',{'identity':stamp,'anchors_used':anchors,'anchors_unavailable':sorted(set(c['anchors'])-set(anchors)),
            'results':results,'search':search_results,'candidate_positions':len(pool['candidates']),
            'candidate_moves':sum(map(len,pool['candidates'].values())),'reference_positions':len(pool['reference'])})

    def export(self,method,budget,row):
        paths=self.read('paths'); games=[]; book=row['book']
        for k,move in sorted(book.items(),key=lambda kv:(len(paths[kv[0]]),paths[kv[0]],kv[1])):
            game=chess.pgn.Game(); game.headers.update({'Event':f'{method} budget {budget}','White':'Prepared player','Black':'Empirical opponent',
                'Result':'*','CardPosition':k,'Budget':str(budget),'BookCost':str(row['cost'])})
            game.comment='Illustrative route to this card. Unmarked White moves are context, not additional prescriptions.'
            node=game; board=HashedBoard()
            for san in self.c['root_moves']+paths[k]+[move]:
                position=key(board); m=board.parse_san(san); node=node.add_variation(m)
                if book.get(position)==san:
                    recall=sum(r['recall'][position] for r in row['contexts'].values())/len(row['contexts'])
                    node.comment=f'STUDY: {san}. Model recall {100*recall:.1f}%.'
                board.push(m)
            games.append(str(game))
        output_path(self.prefix+f'{method}_{budget}.pgn').write_text('\n\n'.join(games)+'\n',encoding='utf-8')

    def verify(self):
        from .verify_builder import verify
        verify(self)


def main():
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('phase',choices=['prepare','engine','build','verify','all'])
    parser.add_argument('--config',default=str(DEFAULT_CONFIG)); args=parser.parse_args(); builder=Builder(args.config)
    for phase in ['prepare','engine','build','verify'] if args.phase=='all' else [args.phase]: getattr(builder,phase)()


if __name__=='__main__': main()
