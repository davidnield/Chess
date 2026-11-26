"""I/O driver for native, mass-preserving W/R; no Python game tree or value solver."""
import numpy as np
from book_builder_rs import RsContinuation
from .model_contract import validate


def values(xs,ys,*,policies,keys,lookup,recipe):
    validate(recipe)
    xs,ys=list(xs),list(ys);roots=[(h,2) for h in xs]+[(h,3) for h in ys]
    result=[];states=0
    for start in range(0,len(roots),recipe["batch_roots"]):
        batch=roots[start:start+recipe["batch_roots"]]
        boards=[policies.opponent.board(h) for h,_ in batch]
        c=RsContinuation([" ".join(b.fen(en_passant="fen").split()[:4]) for b in boards],
            [d for _,d in batch],policies.opponent.colour=="white",keys.native,
            recipe["max_states"],recipe["min_expand"])
        while True:
            pending=c.needed()
            if not pending:break
            for ours in (True,False):
                rows=[r for r in pending if r[2]==ours]
                if not rows:continue
                hs=[r[0] for r in rows];epds=[r[1] for r in rows]
                distributions=policies.native_batch(ours,hs,epds)
                for h,p in zip(hs,distributions):c.expand(h,p)
        leaves=c.leaves()
        if leaves:c.set_cp(leaves,np.asarray(lookup(np.array(leaves,np.int64)),np.float64))
        part=c.results()
        if [r[0] for r in part]!=[h for h,_ in batch]:raise ValueError("continuation root key differs")
        result.extend(part);states+=c.state_count()
    rows=[dict(key=h,lower=lo,upper=hi,unresolved_mass=hi-lo,side="x" if i<len(xs) else "y")
          for i,(h,lo,hi) in enumerate(result)]
    # A scalar-only legacy optimizer cannot safely consume unresolved intervals.
    # Refuse rather than fill them with equality or normalize away their mass.
    known=[r for r in rows if r["upper"]-r["lower"]<=1e-12]
    W={r["key"]:r["lower"] for r in known if r["side"]=="x"}
    R={r["key"]:r["lower"] for r in known if r["side"]=="y"}
    return W,R,dict(schema="continuation-result-v1",states=states,roots=len(roots),
                   unresolved_roots=len(rows)-len(known),bounds=rows,own=dict(policies.own.stats))
