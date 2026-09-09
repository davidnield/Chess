"""Independent prefix-mass and weighted-score audit plus frozen path replay."""
import math
import chess
from python.zobrist import zobrist_int64
from . import maia_stratified as run
from .io import output_path,sha256
from .model import Config
from .hashed_board import HashedBoard
from .trajectory import ChessBackend,uniform
from .stratified import enumerate_prefixes,conditional_prefix,continue_trace


def main():
    c=run.config(); p=run.verified(c); _,_,base,old=run.source(c); c4,_,train,fitted,specs,_,_=base
    paths,tables,fens,prefixes,weights,targets,prior,audit,result,scored=[run.read(n) for n in
        ['traces','policy_tables','fens','prefixes','weights','engine_selection','inherited_records','engine_audit','results','scored']]
    root,books,gates,_=run.equal.packages(c['cards']); cfg=Config(**p['evaluator_config'])
    assert books==p['books'] and gates==p['gates'] and result['identity']==p['identity']
    assert result['weights']==weights
    assert result['scored_sha256']==sha256(output_path(run.PREFIX+'scored.json'))
    assert result['engine_sha256']==sha256(output_path(run.PREFIX+'engine_audit.json'))
    assert audit['complete'] and audit['identity']=={'case_manifest_sha256':sha256(output_path(run.PREFIX+'case_manifest.json')),'engine':p['engine_identity']}
    count=moves_count=prefix_checks=0
    for policy in c['policies']:
        class FrozenKernel:
            def get(self,key,fen):
                moves,ps,core=tables[policy][key]; b=chess.Board(fen)
                assert ' '.join(core)==' '.join(fen.split()[:4])
                assert set(moves.values())=={m.uci() for m in b.legal_moves}
                assert all(b.parse_san(s).uci()==u for s,u in moves.items())
                assert set(ps['maia1400'])==set(moves) and all(x>0 for x in ps['maia1400'].values())
                assert abs(sum(ps['maia1400'].values())-1)<1e-10
                return moves,ps
        backend=ChessBackend(root.fen(),fitted,specs,train,c4,c['max_positions']); backend.kernels=FrozenKernel()
        backend.initial=HashedBoard(root.fen())
        for book in books.values(): assert enumerate_prefixes(backend,'maia1400',book,cfg,cap=c['prefix_cap'])==prefixes[policy]
        for prefix in prefixes[policy]:
            board=root.copy(); probability=1.; book=books[c['reference']]
            for move in prefix['moves']:
                view=backend.view(board,'maia1400'); node=view.node
                edge=next(e for e in node.edges if e.move==move)
                r=cfg.recall_by_position.get(view.key,cfg.recall) if node.side=='us' and view.key in book else 0
                probability *= (1-node.end_probability)*((1-r)*edge.probability+r*(book.get(view.key)==move))
                board.push_san(move)
            end=backend.view(board,'maia1400').node
            if prefix['reason']=='human_end' and end.side in ('us','opp'): probability*=end.end_probability
            assert abs(probability-prefix['probability'])<1e-12; prefix_checks+=1
        groups,ws=run.partition(prefixes[policy],c); assert ws==weights[policy]
        for s in run.STRATA:
            seed=c['seed']+'/'+s
            for book in books:
                traces=paths[policy+'/'+s+'/'+book]
                assert [t['index'] for t in traces]==list(range(c['pairs']))
                for trace in traces:
                    pre=conditional_prefix(groups[s],uniform(seed,trace['index'],0,'prefix'))
                    assert continue_trace(backend,'maia1400',books[book],cfg,seed,trace['index'],pre)==trace
                    b=HashedBoard(root.fen())
                    for i,key in enumerate(trace['keys']):
                        assert str(zobrist_int64(b))==key and b.fen().split()[:4]==fens[key].split()[:4]
                        if i<len(trace['moves']): b.push_san(trace['moves'][i]); moves_count+=1
                    count+=1
    assert count==len(c['policies'])*len(run.STRATA)*len(books)*c['pairs']
    assert run.selection(paths,fens,{g[k] for g in gates for k in ('position','child')},old,c,p['evaluator_config'])==targets
    maps={}; work={}
    for phase,nodes,seconds in [('base',c['engine_nodes'],c['engine_time_seconds']),('deeper',c['deeper_nodes'],c['deeper_time_seconds'])]:
        assert set(prior[phase])==set(targets[phase]['reused']) and set(audit[phase])==set(targets[phase]['targets'])
        assert all(r==old[phase][k] for k,r in prior[phase].items())
        records={**prior[phase],**audit[phase]}; assert set(records)==set(targets[phase]['required'])
        for key,r in records.items():
            assert r['limits']=={'nodes':nodes,'seconds':seconds}
            b=HashedBoard(r['fen']); assert str(zobrist_int64(b))==key and b.fen().split()[:4]==fens[key].split()[:4]
            for u in r['pv']:
                m=chess.Move.from_uci(u); assert m in b.legal_moves; b.push(m)
        maps[phase]={k:r['cp'] for k,r in records.items()}
        work[phase]={'required':len(records),'reused':len(prior[phase]),'new':len(audit[phase]),'seconds':sum(r['seconds'] for r in audit[phase].values())}
    checks=0; matched={}
    for context,rows in scored.items():
        phase,policy=context.split('/'); n=c['pairs'] if phase=='base' else c['deeper_pairs']
        for book,strata in rows.items():
            for s,values in strata.items():
                assert len(values)==n
                for row,trace in zip(values,paths[policy+'/'+s+'/'+book]):
                    hit=0; at=None
                    for ply,key in enumerate(trace['keys']):
                        cp=trace['exact_cp'] if ply==len(trace['keys'])-1 and trace['exact_cp'] is not None else maps[phase][key]
                        if abs(cp)>=cfg.threshold_cp: hit,at=(1 if cp>0 else -1),ply; break
                    assert row['advantage']==(hit==1) and row['disadvantage']==(hit==-1)
                    assert row['early_advantage']==(cfg.discount**at if hit==1 else 0)
                    assert row['unknown_eval_mass']==row['frontier_mass']==0; checks+=5
            for metric,summary in result['contexts'][context]['arms'][book].items():
                mean=sum(weights[policy][s]*sum(r[metric] for r in rs)/n for s,rs in strata.items())
                assert abs(mean-summary['mean'])<1e-12
        for book,metrics in result['contexts'][context]['contrasts'].items():
            for metric,summary in metrics.items():
                means={}; variance=0
                for s in run.STRATA:
                    ds=[a[metric]-z[metric] for a,z in zip(rows[book][s],rows[c['reference']][s])]
                    means[s]=sum(ds)/n
                    variance+=weights[policy][s]**2*sum((v-means[s])**2 for v in ds)/(n-1)/n
                mean=sum(weights[policy][s]*v for s,v in means.items())
                margin=math.sqrt(2*math.log(2/result['contrast_alpha'])*sum(w*w/n for w in weights[policy].values()))
                assert abs(mean-summary['mean'])<1e-12 and abs(math.sqrt(variance)-summary['mc_se'])<1e-12
                assert abs(margin-summary['hoeffding_margin'])<1e-12
        if phase=='deeper':
            matched[policy]={}
            for book in books:
                if book==c['reference']: continue
                matched[policy][book]={budget:sum(weights[policy][s]*sum(a['early_advantage']-z['early_advantage'] for a,z in zip(scored[budget+'/'+policy][book][s][:n],scored[budget+'/'+policy][c['reference']][s][:n]))/n for s in run.STRATA) for budget in ['base','deeper']}
    for phase,gs in result['gates'].items():
        for g in gs:
            a,b=maps[phase][g['position']],maps[phase][g['child']]
            assert (a,b)==(g['parent_cp'],g['child_cp'])
            assert g['passed']==(b>=cfg.soundness_floor_cp and b>=a-cfg.max_engine_loss_cp)
    assert result['eligible']=={b:all(g['passed'] for gs in result['gates'].values() for g in gs if book.get(g['position'])==g['move']) for b,book in books.items()}
    out={'status':'passed','paths_replayed':count,'legal_moves':moves_count,'prefix_mass_checks':prefix_checks,'scalar_checks':checks,
        'engine_work':work,'matched_deeper_contrasts':matched,'results_sha256':sha256(output_path(run.PREFIX+'results.json')),'verifier_sha256':sha256(__file__)}
    run.save('verification',out); print(out)


if __name__=='__main__': main()
