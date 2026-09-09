"""Fixed-budget empirical continuation comparison, with explicit truncation.

Phases: prepare (isolated Maia environment), engine, compare.
All outputs confined to _artifacts/followthrough/. See prospective plan.
"""
import argparse
from collections import defaultdict
from dataclasses import replace
import json
from pathlib import Path
import time
import chess
import chess.engine
import chess.pgn
import numpy as np
from python.zobrist import zobrist_int64
from python.eval_arrays import verify_eval_arrays, open_eval_arrays, lookup_evals, MISSING
from . import exposure_real as previous
from .context_recall import solve
from .exposure_recall import topological
from .continuation_audit import analyse_position
from .followthrough_data import PREFIX, read, save, sources, layer_snapshot
from .hashed_board import HashedBoard
from .io import ROOT, output_path, sha256, fingerprint
from .maia_policy import MaiaPolicy
from .model import Config, Graph, Node, Edge, graph_dict, graph_from_dict, book_cost


def key(board): return str(zobrist_int64(board))


def packages():
    root = HashedBoard()
    for move in ['e4', 'e5', 'Nf3', 'Nc6']: root.push_san(move)
    books = {'d4_only': {key(root): 'd4'}}
    for name, reply, move in [('exd4_Nxd4','exd4','Nxd4'),('Nf6_Nc3','Nf6','Nc3'),('d6_Nc3','d6','Nc3')]:
        board = root.copy(); board.push_san('d4'); board.push_san(reply)
        books[name] = {key(root): 'd4', key(board): move}
    books['breadth4'] = {k: v for name in ['exd4_Nxd4', 'Nf6_Nc3', 'd6_Nc3'] for k, v in books[name].items()}
    sequence = ['d4', 'exd4', 'Nxd4', 'Bc5', 'Be3', 'Qf6', 'c3']
    forced = defaultdict(set); forced[0].add(key(root)); fens = {key(root): root.fen()}
    chain = {}; board = root.copy()
    for ply, move in enumerate(sequence):
        forced[ply].add(key(board)); fens[key(board)] = board.fen()
        if board.turn:
            chain[key(board)] = move
            if len(chain) == 3: books['sequence3'] = dict(chain)
        board.push_san(move); fens[key(board)] = board.fen()
    books['sequence4'] = chain
    forced[len(sequence)].add(key(board))
    for reply in ['Nf6', 'd6']:
        board = root.copy()
        for ply, move in enumerate(['d4', reply, 'Nc3']):
            forced[ply].add(key(board)); fens[key(board)] = board.fen(); board.push_san(move)
        fens[key(board)] = board.fen()
        forced[3].add(key(board))
    return root, books, dict(forced), fens


def identity():
    return {'code': {n: sha256(ROOT/'research/human_repertoire'/n) for n in
        ['followthrough.py', 'followthrough_data.py', 'context_recall.py', 'exposure_recall.py',
         'model.py', 'maia_policy.py', 'hashed_board.py', 'continuation_audit.py']},
        'plan_sha256': sha256(ROOT/'docs/human_repertoire/followthrough_plan.md'),
        'previous_manifest_sha256': sha256(output_path(previous.PREFIX+'manifest.json')),
        'maia_config_sha256': sha256(ROOT/'docs/human_repertoire/maia_config.json')}


def prepare():
    if output_path(PREFIX+'manifest.json').exists(): verified(); print('Verified preparation'); return
    started = time.monotonic(); old = previous.verified(); source_identity = sources()
    root, books, forced, gate_fens = packages(); fens = {key(root): root.fen()}
    if any(books[name] != book for name,book in old['books'].items()): raise ValueError('original book drift')
    graph_nodes = {event: {} for event in ['Blitz', 'Rapid']}
    policies = previous.read('policies'); empirical = previous.read('empirical')
    if empirical['sources'] != source_identity: raise ValueError('original empirical source changed')
    model = None; calls = 0; depths = {key(root): 0}; mass = {key(root): 1.}
    envelope = dict(mass); layers = []; aliases = {}; reasons = {}; accounting = {}
    prescribed = defaultdict(set)
    for book in books.values():
        for k, move in book.items(): prescribed[k].add(move)
    for ply in range(8):
        if time.monotonic()-started > 600: raise TimeoutError('preparation time cap')
        mandatory = forced.get(ply, set())
        if not mandatory <= set(mass): raise ValueError('required line not reached')
        selected = sorted(mandatory)+[k for k in sorted(mass, key=lambda k: (-mass[k], k)) if k not in mandatory][:64-len(mandatory)]
        queries = {k: fens[k] for k in selected if k not in empirical['stats'] or k not in empirical['aux']}
        if queries:
            extra = layer_snapshot(ply, queries, source_identity)
            empirical['stats'].update(extra['stats']); empirical['aux'].update(extra['aux'])
        cache_name = f'layer_{ply}_policies'; cache_path = output_path(PREFIX+cache_name+'.json')
        cache_identity = {'identity': identity(), 'selected': {k: fens[k] for k in selected}}
        if cache_path.exists():
            cached = read(cache_name)
            if cached['identity'] != cache_identity: raise ValueError('policy cache drift')
            for event in policies: policies[event].update(cached['policies'][event])
        following = defaultdict(float)
        for k in mass:
            if k not in selected: reasons[k] = 'beam'
        for k in selected:
            board = HashedBoard(fens[k]); outcome = board.outcome(claim_draw=False)
            if outcome is not None:
                cp = 0 if outcome.winner is None else 100000 if outcome.winner else -100000
                for event in graph_nodes: graph_nodes[event][k] = Node('terminal', cp=cp,
                    wdl=(0,1,0) if outcome.winner is None else (1,0,0) if outcome.winner else (0,0,1))
                continue
            rows = empirical['stats'].get(k, []); aux = empirical['aux'].get(k)
            if not rows or aux is None:
                reasons[k] = 'empirical_absent'; continue
            a = previous.empirical_edges(rows, aux); accounting[k] = a
            probabilities = {}
            for event in graph_nodes:
                if board.turn:
                    if k not in policies[event]:
                        if model is None: model = MaiaPolicy()
                        calls += 1
                        if calls > 600: raise ValueError('inference cap')
                        policies[event][k] = model.probabilities(board, event, 2000)
                    probabilities[event] = {board.san(chess.Move.from_uci(m)): p for m,p in policies[event][k].items()}
                else: probabilities[event] = a['probabilities']
            moves = sorted(probabilities['Blitz']); children = {}
            for move in moves:
                child = board.copy(); child.push_san(move); ck = key(child)
                if ck in fens and fens[ck].split()[:4] != child.fen().split()[:4]: raise ValueError('hash/FEN collision')
                fens.setdefault(ck, child.fen())
                if ck in depths and depths[ck] <= ply:
                    target = f'cut:{ply}:{k}:{ck}'; aliases[target] = ck; reasons[target] = 'earlier_layer'
                else:
                    target = ck; depths.setdefault(ck, ply+1)
                    p = max(probabilities[e][move] for e in probabilities)
                    if board.turn and move in prescribed[k]: p = 1.
                    following[ck] += mass[k]*(1-a['end_probability'])*p
                    envelope[ck] = following[ck]
                children[move] = target
            for event in graph_nodes:
                edges = [Edge(m, children[m], p) for m,p in probabilities[event].items()]
                if not board.turn and a['unresolved_probability']:
                    unknown = 'unresolved:'+k; reasons[unknown] = 'empirical_tail'
                    edges.append(Edge('__unresolved__', unknown, a['unresolved_probability']))
                graph_nodes[event][k] = Node('us' if board.turn else 'opp', tuple(edges),
                    wdl=(0,1,0), end_probability=a['end_probability'], end_wdl=a['end_wdl'])
        save(cache_name, {'identity': cache_identity, 'policies':
            {e: {k: policies[e][k] for k in selected if k in policies[e]} for e in policies}})
        layers.append({'ply': ply, 'expanded_selection': len(selected), 'arrivals': len(mass),
                       'mandatory': sorted(mandatory), 'selected': selected, 'next_positions': len(following)})
        print('layer', ply, 'selected',len(selected),'next',len(following),'new Maia',calls,flush=True)
        mass = dict(following)
    for k in mass: reasons[k] = 'horizon'
    fens.update(gate_fens)
    if len(fens) > 35000: raise ValueError('graph/eval position cap')
    array_dir = Path('E:/chess/eval_arrays'); db = Path('E:/chess/unified_eval_db.parquet')
    eval_verified = verify_eval_arrays(array_dir, db, adopt=False)
    eval_sources = {str(p): fingerprint(p) for p in [db,array_dir/'eval_hash.npy',array_dir/'eval_cp.npy',array_dir/'eval_arrays.meta.json']}
    keys = sorted(fens); hashes, cps = open_eval_arrays(array_dir)
    values = lookup_evals(np.array([int(k) for k in keys], dtype=np.int64), hashes, cps)
    scores = {k:int(cp) for k,cp in zip(keys,values) if cp != MISSING}; del hashes,cps
    overlay = previous.read('overlay')['base']; scores.update({k:r['cp'] for k,r in overlay.items() if k in fens})
    gates = {}
    for book in books.values():
        for k,move in book.items():
            board = HashedBoard(fens[k]); board.push_san(move); ck = key(board); fens[ck] = board.fen()
            gates[k+' '+move] = {'parent': k, 'move': move, 'child': ck}
    gate_keys = {g[part] for g in gates.values() for part in ['parent','child']}
    missing = sorted((k for k in fens if k not in scores), key=lambda k: (-envelope.get(k,0),k))[:64]
    for event,nodes in graph_nodes.items():
        for parent in list(nodes.values()):
            for edge in parent.edges:
                if edge.child not in nodes: nodes[edge.child] = Node('frontier',wdl=(0,1,0))
        nodes = {k: replace(n,cp=n.cp if n.side=='terminal' else scores.get(aliases.get(k,k)),
                 edges=tuple(replace(e,sound_cp=scores.get(aliases.get(e.child,e.child))) for e in n.edges)) for k,n in nodes.items()}
        graph = Graph(key(root),nodes); graph.validate()
        if topological(graph) is None: raise ValueError('unexpected topology cycle')
        if len(nodes)>35000 or sum(len(n.edges) for n in nodes.values())>100000: raise ValueError('graph cap')
        graph_nodes[event] = graph_dict(graph)
    snapshots = {n:sha256(save(n,value)) for n,value in {
        'graphs':graph_nodes,'fens':fens,'scores':scores,'policies':policies,'empirical':empirical,
        'accounting':accounting,'aliases':aliases,'reasons':reasons,'envelope':envelope}.items()}
    if sources()!=source_identity or any(fingerprint(p)!=fp for p,fp in eval_sources.items()):
        raise ValueError('source changed during preparation')
    engine_path = 'C:/Users/David/AppData/Local/Microsoft/WinGet/Links/stockfish.exe'
    save('manifest',{'identity':identity(),'sources':source_identity,'eval_sources':eval_sources,'eval_verification':eval_verified,
        'books':books,'gates':list(gates.values()),'snapshots':snapshots,'layers':layers,'new_maia_calls':calls,
        'engine_path':engine_path,'engine_binary':fingerprint(engine_path,full=True),
        'targets':{'base':sorted(gate_keys|set(missing)),'deeper':sorted(gate_keys)},
        'seconds':time.monotonic()-started,'root':key(root),'root_ply':4,'horizon':8,'entries':[.1,.01,.001]})
    print('prepared',len(fens),'positions',len(scores),'scores',flush=True)


def verified():
    p=read('manifest')
    if p['identity']!=identity() or any(sha256(output_path(PREFIX+n+'.json'))!=h for n,h in p['snapshots'].items()):
        raise ValueError('follow-through input drift')
    return p


def engine():
    p=verified(); stamp={'manifest_sha256':sha256(output_path(PREFIX+'manifest.json')),'binary':p['engine_binary']}
    cache=read('engine') if output_path(PREFIX+'engine.json').exists() else {'identity':stamp,'base':{},'deeper':{},'complete':False}
    if cache['identity']!=stamp: raise ValueError('engine cache drift')
    if fingerprint(p['engine_path'],full=True)!=p['engine_binary']: raise ValueError('engine binary drift')
    if cache['complete']: print('Verified engine cache'); return
    fens=read('fens'); started=time.monotonic()
    with chess.engine.SimpleEngine.popen_uci(p['engine_path'],timeout=10) as worker:
        worker.configure({'Threads':1,'Hash':32,'UCI_LimitStrength':False,'Skill Level':20,'UCI_ShowWDL':True,'SyzygyPath':''})
        cache['uci_id']=worker.id
        for phase,nodes,seconds in [('base',100000,2),('deeper',500000,5)]:
            for index,k in enumerate(p['targets'][phase],1):
                if k in cache[phase]: continue
                if time.monotonic()-started>240: raise TimeoutError('engine session cap; cached records retained')
                cache[phase][k]=analyse_position(worker,fens[k],nodes,seconds); save('engine',cache)
                if index%16==0 or index==len(p['targets'][phase]): print(phase,index,'/',len(p['targets'][phase]),flush=True)
    if fingerprint(p['engine_path'],full=True)!=p['engine_binary']: raise ValueError('engine binary changed')
    cache['complete']=True; cache['seconds']=time.monotonic()-started; save('engine',cache)


def scored_graphs(p,audit):
    scores=read('scores'); scores.update({k:r['cp'] for k,r in audit['base'].items()}); aliases=read('aliases')
    return {event:Graph(raw['root'],{k:replace(n,cp=n.cp if n.side=='terminal' else scores.get(aliases.get(k,k)),
        edges=tuple(replace(e,sound_cp=scores.get(aliases.get(e.child,e.child))) for e in n.edges))
        for k,n in graph_from_dict(raw).nodes.items()}) for event,raw in read('graphs').items()}


def gate_checks(p,audit):
    checks={}
    for name,book in p['books'].items():
        rows=[]
        for g in p['gates']:
            if book.get(g['parent'])!=g['move']: continue
            for phase in ['base','deeper']:
                parent=audit[phase][g['parent']]['cp']; child=audit[phase][g['child']]['cp']
                rows.append({**g,'phase':phase,'parent_cp':parent,'child_cp':child,'passed':child>=-100 and parent-child<=150})
        checks[name]={'passed':all(g['passed'] for g in rows),'rows':rows}
    return checks


def compare():
    p=verified(); audit=read('engine')
    if not audit['complete'] or audit['identity']['manifest_sha256']!=sha256(output_path(PREFIX+'manifest.json')):
        raise ValueError('engine incomplete or stale')
    graphs=scored_graphs(p,audit); gates=gate_checks(p,audit); results={}
    cfg=Config(horizon=8,max_states=2000000)
    for event,graph in graphs.items():
        results[event]={}
        for entry in p['entries']:
            rows={}; results[event][str(entry)]=rows
            for name,book in p['books'].items():
                if not gates[name]['passed']: rows[name]={'eligible':False,'cost':book_cost(graph,book)}; continue
                result=solve(graph,book,cfg,entry,anchors={p['root']:.99},root_ply=4)
                metrics=result['conditional_metrics']; lower=metrics['certain_early_advantage']
                metrics['early_interval']=[lower,min(1,lower+metrics['unresolved_first_hit_mass'])]
                rows[name]={'eligible':True,'cost':book_cost(graph,book),'contextual':result}
            print(event,entry,'evaluated',flush=True)
    contrasts={}
    pairs=[('breadth4','sequence4'),('exd4_Nxd4','d4_only'),('sequence3','exd4_Nxd4'),('sequence4','sequence3')]
    for event,entries in results.items():
        contrasts[event]={}
        for entry,rows in entries.items():
            contrasts[event][entry]={}
            for a,b in pairs:
                if not rows[a]['eligible'] or not rows[b]['eligible']: continue
                x=rows[a]['contextual']['conditional_metrics']; y=rows[b]['contextual']['conditional_metrics']
                contrasts[event][entry][a+' minus '+b]={'observed_difference':x['certain_early_advantage']-y['certain_early_advantage'],
                    'coverage_interval':[x['early_interval'][0]-y['early_interval'][1],x['early_interval'][1]-y['early_interval'][0]]}
    save('results',{'identity':{'manifest_sha256':sha256(output_path(PREFIX+'manifest.json')),
        'engine_sha256':sha256(output_path(PREFIX+'engine.json'))},'gates':gates,'results':results,'contrasts':contrasts})


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('phase',choices=['prepare','engine','compare'])
    globals()[parser.parse_args().phase]()
