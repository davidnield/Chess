"""Corrected native rollout adapter with explicit uncertainty, utility and eval binding.

Start from the initial board for complete actual rollout history; arbitrary endpoint
starts have only history since that endpoint and are labeled accordingly by callers.
This adapter deliberately does not use legacy value_leaves' neutral-value fallback.

The referee's evaluation is separate from the books' construction sources (review
2026-11-26, F1). Pass ``referee_binding``, one frozen eval binding, to score every arm's
leaves with the SAME lookup; paired source arms must, or each book would be judged by its
own construction source. Without it the arm's own construction binding scores its leaves,
recorded as ``referee_role="construction"`` (single-arm diagnostics only).
"""
import numpy as np
from book_builder_rs import continuation_utility
from convertibility_eval import rollouts


def referee_lookup(policies, referee_binding):
    """A lookup over the referee binding, keyed by the policies' full-key IDs."""
    from consumer_keys import ConsumerKeys
    from eval_binding import read_binding
    binding = read_binding(referee_binding)
    evals = ConsumerKeys(binding["lookup"], expected_sha=binding["lookup_sha256"], verify_hashes=True).evals
    keys = policies.keys

    def lookup(ids):
        return evals.lookup(*keys.halves(np.asarray(ids, np.int64)))[0]
    return lookup, binding


def play(starts,book,*,white,args,seed,policies,referee_binding=None,lookup=None):
    if getattr(args,"position_keys",None) is not policies.keys:
        raise ValueError("native referee and policies must share full-key IDs")
    if referee_binding is not None and lookup is not None:
        raise ValueError("pass a referee binding or a test lookup, not both")
    binding=None;role="test-override"
    if referee_binding is not None:
        lookup,binding=referee_lookup(policies,referee_binding);role="fixed-referee"
    elif lookup is None:
        binding=getattr(policies.keys,"binding",None)
        if not binding:raise ValueError("referee requires an explicit fixed evaluation binding")
        lookup=policies.keys.scores;role="construction"
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
    return dict(schema="shared-native-referee-v2",lower=lower,upper=upper,
                unresolved_mass=upper-lower,mass=sum(r["mass"] for r in rows),book_mass=book_mass,leaves=rows,
                evaluation_binding=binding,referee_role=role,test_lookup_override=role=="test-override")
