"""Corrected native rollout adapter with explicit uncertainty, utility and eval binding.

Start from the initial board for complete actual rollout history; arbitrary endpoint
starts have only history since that endpoint and are labeled accordingly by callers.
This adapter deliberately does not use legacy value_leaves' neutral-value fallback.
"""
import numpy as np
from book_builder_rs import continuation_utility
from convertibility_eval import rollouts


def play(starts,book,*,white,args,seed,policies,lookup=None):
    if getattr(args,"position_keys",None) is not policies.keys:
        raise ValueError("native referee and policies must share full-key IDs")
    binding=None
    if lookup is None:
        binding=getattr(policies.keys,"binding",None)
        if not binding:raise ValueError("referee requires an explicit fixed evaluation binding")
        lookup=policies.keys.scores
    leaves,book_mass=rollouts(starts,book,white,None,args,seed,None,None,None,lookup,policies=policies)
    ids=np.array([r[1] for r in leaves],np.int64)
    cp=np.asarray(lookup(ids),np.float64)
    score=continuation_utility(cp.tolist(),white)
    rows=[]
    for leaf,c,s in zip(leaves,cp,score):
        exact=leaf[4]
        if exact is not None:lo=hi=exact;origin="exact-terminal"
        elif np.isfinite(s):lo=hi=s;origin="direct-engine"
        else:lo,hi=0.,1.;origin="unresolved"
        # Ancestor values are diagnostics, not an evaluation of the missing leaf.
        rows.append(dict(mass=leaf[0],key=leaf[1],lower=lo,upper=hi,origin=origin,
                         cp=float(c) if np.isfinite(s) else None,ancestor_cp=leaf[2],
                         ply=leaf[6],start=leaf[8],trajectory=leaf[9]))
    lower=sum(r["mass"]*r["lower"] for r in rows)
    upper=sum(r["mass"]*r["upper"] for r in rows)
    return dict(schema="shared-native-referee-v1",lower=lower,upper=upper,
                unresolved_mass=upper-lower,mass=sum(r["mass"] for r in rows),book_mass=book_mass,leaves=rows,
                evaluation_binding=binding,test_lookup_override=binding is None)
