"""Consequential missing/boundary evidence on the collected own-move frontier.

This is independent of the selected book and historical move counts. A missing
child may open a previously absent branch; solving again must permit expansion.
"""
import math
import numpy as np
import polars as pl
import budget_core as bc
from eval_arrays import MISSING
from .quality import score

def requests(edges,reach,lookup,*,white,floor,limit,verified_frame=None,overrides=None,band=.01):
    sign=1 if white else -1
    ids=sorted(set(edges["parent_hash"])|set(edges["child_hash"].drop_nulls()))
    cps={h:int(c) for h,c in zip(ids,lookup(np.asarray(ids,np.int64))) if c!=MISSING}
    cps.update(overrides or {})
    done={}
    if verified_frame is not None:
        for row in verified_frame.to_dicts():
            if row.get("verification_nodes"):
                done[row["position_hash"],row["move_san"].rstrip("+#")]=row["verification_nodes"]
    out=[]
    for (h,),group in edges.group_by("parent_hash",maintain_order=True):
        if reach.get(h,0)<=0: continue
        items=group.to_dicts()
        child=[sign*cps[r["child_hash"]] for r in items if r["child_hash"] in cps]
        reference=max(child+([sign*cps[h]] if h in cps else []),default=None)
        moves=[];reasons=set()
        for r in items:
            san=r["move_san"].rstrip("+#");budget=done.get((h,san),0)
            cp=cps.get(r["child_hash"])
            reason=None
            if cp is None or reference is None:
                if budget<5_000_000: reason="missing-candidate-evidence"
            elif budget<20_000_000:
                value=score(sign*cp);loss=score(reference)-value
                if (abs(loss-limit)<=band+bc.SCORE_EPS and value>=floor-band-bc.SCORE_EPS
                    or abs(value-floor)<=band+bc.SCORE_EPS and loss<=limit+band+bc.SCORE_EPS):
                    reason="candidate-score-boundary"
                if h in cps and child and score(max(child))-score(sign*cps[h])>band:
                    reason="inconsistent-parent-child"
            if reason: moves.append(san);reasons.add(reason)
        if moves:
            out.append(dict(position_hash=h,epd=items[0]["parent_epd"],moves=sorted(set(moves)),
                            reason="; ".join(sorted(reasons)),reach=reach[h]))
    return out
