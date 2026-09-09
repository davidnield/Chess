"""Rare-branch allocation experiment with exact prefix weights."""
import argparse
from dataclasses import replace
import json
import time
import chess
from . import maia_equal as equal, paired_panel as panel
from .board_benchmark import isolated
from .io import ROOT, output_path, sha256, write_json
from .maia_policy import MaiaPolicy, MaiaFallbackKernels
from .model import Config
from .hashed_board import HashedBoard
from .trajectory import ChessBackend, uniform, score_trace
from .stratified import enumerate_prefixes, conditional_prefix, continue_trace, weighted_summary

PREFIX = 'maia_stratified/'
STRATA = ['exd4_Nxd4', 'Nf6_Nc3', 'd6_Nc3', 'rest']


def config():
    return {**json.loads(equal.CONFIG.read_text()), 'pairs': 32, 'deeper_pairs': 4,
        'prefix_cap': 2000, 'seed': 'human-repertoire-maia-stratified-v1'}


def read(n): return json.loads(output_path(PREFIX+n+'.json').read_text())
def save(n, v): return write_json(PREFIX+n+'.json', v)


def source(c):
    ec = json.loads(equal.CONFIG.read_text()); p = equal.verified(ec)
    _, c6, base, records = equal.source(c); audit = equal.read('engine_audit')
    if not audit['complete'] or audit['identity'] != {'case_manifest_sha256':sha256(output_path(equal.PREFIX+'case_manifest.json')), 'engine':p['engine_identity']}:
        raise ValueError('equal-budget source drift')
    for phase,nodes,seconds in [('base',c['engine_nodes'],c['engine_time_seconds']),('deeper',c['deeper_nodes'],c['deeper_time_seconds'])]:
        records[phase].update(panel.checked_records(audit,phase,nodes,seconds))
    return p,c6,base,records


def identity(c):
    return {'config':c, 'code_sha256':sha256(__file__), 'sampler_sha256':sha256(ROOT/'research/human_repertoire/stratified.py'),
        'hashed_board_sha256':sha256(ROOT/'research/human_repertoire/hashed_board.py'),
        'plan_sha256':sha256(ROOT/'docs/human_repertoire/maia_stratified_plan.md'),
        'equal_identity':equal.identity(json.loads(equal.CONFIG.read_text())),
        'equal_manifest_sha256':sha256(output_path(equal.PREFIX+'case_manifest.json')),
        'equal_engine_sha256':sha256(output_path(equal.PREFIX+'engine_audit.json'))}


def verified(c):
    p=read('case_manifest')
    if p['identity'] != identity(c) or any(sha256(output_path(PREFIX+k+'.json')) != h for k,h in p['snapshots'].items()):
        raise ValueError('stratified frozen input drift')
    return p


def partition(prefixes,c):
    groups={s:[] for s in STRATA}
    for p in prefixes:
        label=next((b for b,(reply,_) in c['cards'].items() if p['moves']==['d4',reply]),'rest')
        groups[label].append(p)
    weights={s:sum(p['probability'] for p in ps) for s,ps in groups.items()}
    if abs(sum(weights.values())-1)>1e-10 or any(w<=0 for w in weights.values()): raise ValueError('invalid strata')
    return groups,weights


def selection(paths,fens,gates,records,c,cfg):
    policies=[p+'_'+s for p in c['policies'] for s in STRATA]
    flattened={p+'_'+s+'/'+b:paths[p+'/'+s+'/'+b] for p in c['policies'] for s in STRATA for b in c['cards']}
    return equal.selection(flattened,fens,gates,records,{**c,'policies':policies},cfg)


def prepare(c):
    if output_path(PREFIX+'case_manifest.json').exists(): verified(c); return
    old,c6,base,records=source(c); c4,_,train,fitted,specs,_,_=base
    root,books,gates,fens=equal.packages(c['cards']); cfg=Config(**old['evaluator_config'])
    paths,tables,prefixes,weights={},{},{},{}; policy=MaiaPolicy(); start=time.monotonic()
    for name in c['policies']:
        backend=ChessBackend(root.fen(),fitted,specs,train,c4,c['max_positions'])
        backend.initial=HashedBoard(root.fen())
        backend.kernels=MaiaFallbackKernels(policy,'Blitz' if name=='maia_blitz' else 'Rapid',chess.WHITE,c['opponent_rating_input'],backend.kernels,'shared_position',cap=c['max_positions'])
        ps=enumerate_prefixes(backend,'maia1400',books[c['reference']],cfg,cap=c['prefix_cap'])
        for book in books.values():
            if enumerate_prefixes(backend,'maia1400',book,cfg,cap=c['prefix_cap']) != ps: raise ValueError('unequal prefix distribution')
        groups,weights[name]=partition(ps,c); prefixes[name]=ps
        for s in STRATA:
            seed=c['seed']+'/'+s
            for book in books:
                rows=[]
                for i in range(c['pairs']):
                    if time.monotonic()-start>c['sampling_seconds']: raise TimeoutError('sampling guard')
                    prefix=conditional_prefix(groups[s],uniform(seed,i,0,'prefix'))
                    rows.append(continue_trace(backend,'maia1400',books[book],cfg,seed,i,prefix))
                paths[name+'/'+s+'/'+book]=rows
        tables[name]=backend.kernels.cache
        for key,fen in backend.fens.items():
            if key in fens and fens[key].split()[:4]!=fen.split()[:4]: raise ValueError('FEN drift')
            fens.setdefault(key,fen)
        print(name,'complete',len(backend.fens),'positions',weights[name],flush=True)
    targets=selection(paths,fens,{g[k] for g in gates for k in ('position','child')},records,c,old['evaluator_config'])
    prior={s:{k:records[s][k] for k in targets[s]['reused']} for s in records}
    snapshots={n:sha256(save(n,v)) for n,v in {'traces':paths,'policy_tables':tables,'prefixes':prefixes,'weights':weights,'fens':fens,'engine_selection':targets,'inherited_records':prior}.items()}
    save('case_manifest',{'identity':identity(c),'snapshots':snapshots,'books':books,'gates':gates,
        'evaluator_config':old['evaluator_config'],'sampling_seconds':time.monotonic()-start,
        **{k:old[k] for k in ('engine_identity','engine_path','engine_options')}})
    print({s:len(t['targets']) for s,t in targets.items()},flush=True)


def engine(c):
    module=isolated('calibration_experiment.py','m7/',PREFIX); module.verified_cases=lambda unused:verified(c); module.engine(c)


def compare(c):
    p=verified(c); audit=read('engine_audit'); prior=read('inherited_records'); targets=read('engine_selection')
    if not audit['complete'] or audit['identity']!={'case_manifest_sha256':sha256(output_path(PREFIX+'case_manifest.json')),'engine':p['engine_identity']}: raise ValueError('engine drift/incomplete')
    maps={}
    for phase,nodes,seconds in [('base',c['engine_nodes'],c['engine_time_seconds']),('deeper',c['deeper_nodes'],c['deeper_time_seconds'])]:
        fresh=panel.checked_records(audit,phase,nodes,seconds)
        if set(fresh)!=set(targets[phase]['targets']): raise ValueError('incomplete coverage')
        maps[phase]={k:r['cp'] for k,r in {**prior[phase],**fresh}.items()}
    paths=read('traces'); weights=read('weights'); cfg=Config(**p['evaluator_config']); scored={}; contexts={}
    alpha=c['alpha']/(len(c['policies'])*(len(c['cards'])-1))
    for phase in maps:
        n=c['pairs'] if phase=='base' else c['deeper_pairs']
        for policy in c['policies']:
            rows={b:{s:[score_trace(t,maps[phase],cfg) for t in paths[policy+'/'+s+'/'+b][:n]] for s in STRATA} for b in c['cards']}
            if any(r['unknown_eval_mass'] or r['frontier_mass'] for ss in rows.values() for rs in ss.values() for r in rs): raise ValueError('missing score')
            arms={b:{m:weighted_summary({s:[r[m] for r in rs] for s,rs in ss.items()},weights[policy],c['alpha']) for m in ['early_advantage','advantage','disadvantage','points']} for b,ss in rows.items()}
            contrasts={b:{m:weighted_summary({s:[a[m]-z[m] for a,z in zip(ss[s],rows[c['reference']][s])] for s in STRATA},weights[policy],alpha,True) for m in ['early_advantage','disadvantage','points']} for b,ss in rows.items() if b!=c['reference']}
            contexts[phase+'/'+policy]={'arms':arms,'contrasts':contrasts}; scored[phase+'/'+policy]=rows
    gates={s:panel.check_gates(p['gates'],m,cfg) for s,m in maps.items()}
    eligible={b:all(g['passed'] for gs in gates.values() for g in gs if book.get(g['position'])==g['move']) for b,book in p['books'].items()}
    save('results',{'identity':identity(c),'contexts':contexts,'weights':weights,'gates':gates,'eligible':eligible,
        'engine_sha256':sha256(output_path(PREFIX+'engine_audit.json')),'scored_sha256':sha256(save('scored',scored)),'contrast_alpha':alpha})
    print(json.dumps({k:v['contrasts'] for k,v in contexts.items() if k.startswith('base/')},indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('phase',choices=('prepare','engine','compare'))
    args=parser.parse_args(); globals()[args.phase](config())
