"""Attribute the exact bounded-policy cutoff mass without new model work."""
from collections import defaultdict
from math import isclose
from . import followthrough as run
from .followthrough_data import PREFIX, read, save
from .io import sha256, output_path


def main():
    p=run.verified(); results=read('results'); reasons=read('reasons'); out={}
    for event,graph in run.scored_graphs(p,read('engine')).items():
        out[event]={}
        for name,row in results['results'][event]['0.01'].items():
            if not row['eligible']: continue
            book=p['books'][name]; recall=row['contextual']['recall']
            states={(graph.root,0):1.}; by_reason=defaultdict(float); by_ply=defaultdict(float)
            cutoffs=[]; representatives={graph.root:[]}
            for ply in range(9):
                following=defaultdict(float)
                for (k,_),mass in states.items():
                    node=graph.nodes[k]
                    if node.side=='frontier' and ply<8:
                        reason=reasons[k]; by_reason[reason]+=mass; by_ply[str(ply)]+=mass
                        cutoffs.append({'position':k,'ply':ply,'mass':mass,'reason':reason,
                                        'representative_continuation':representatives.get(k,[])})
                    if node.side not in ('us','opp') or ply==8: continue
                    for edge in node.edges:
                        probability=edge.probability
                        if node.side=='us' and k in book:
                            probability=(1-recall[k])*probability+recall[k]*(edge.move==book[k])
                        following[edge.child,ply+1]+=mass*(1-node.end_probability)*probability
                        representatives.setdefault(edge.child,representatives.get(k,[])+[edge.move])
                states=following
            assert isclose(sum(by_reason.values()),row['contextual']['conditional_metrics']['frontier_before_horizon_mass'],abs_tol=1e-10)
            out[event][name]={'by_reason':dict(by_reason),'by_ply':dict(by_ply),
                             'largest_cutoffs':sorted(cutoffs,key=lambda x:-x['mass'])[:10]}
    save('cutoff_diagnostics',{'results_sha256':sha256(output_path(PREFIX+'results.json')),
         'code_sha256':sha256(__file__),'diagnostics':out})
    for event,books in out.items():
        print(event, 'sequence4', books['sequence4'])


if __name__=='__main__': main()
