"""Full-key, arbitrary-size equal-practice recall scenarios for unseen books.

This is a scenario model, not a claim about an individual learner's retention.
Reach orders new cards and reviews. Optional real-game exposures reinforce only
the prescriptions that were actually retrieved successfully.
"""
from __future__ import annotations
import math
from .rollout import uniform
from .reports import key

RETENTION={"low":(2.,1.6),"central":(5.,2.),"high":(10.,2.4)}

def schedules(cards, *, seed, daily=30, curve="central", days=(30,90,240),
              history=None, game_visits=None):
    """Yield (day, profiles, effort). Card fields: position_hash, move, reach, ply.
    history is explicitly verified (full key, move)->[(day<=0, success)].
    game_visits(day) yields actual consecutive own booked keys for each practice game.
    """
    if daily<1 or not days or min(days)<0: raise ValueError("invalid study schedule")
    initial,growth=RETENTION[curve]
    data={}
    for c in cards:
        k=key(c["position_hash"])
        if k in data: raise ValueError("duplicate card")
        if not math.isfinite(c["reach"]) or c["reach"]<0 or c["ply"]<0:
            raise ValueError("invalid reach/ply")
        data[k]=c
    order=sorted(data,key=lambda k:(-data[k]["reach"],data[k]["ply"],k))
    strength={k:0. for k in data}; last={k:0 for k in data}
    for k,c in data.items():
        for day,good in sorted((history or {}).get((k,c["move"]),())):
            if day>0: raise ValueError("future trainer evidence")
            strength[k]=min(365.,strength[k]*growth) if good and strength[k] else initial
            last[k]=day
    presentations=retrievals=0
    for day in range(max(days)+1):
        prob={k:2**(-(day-last[k])/strength[k]) if strength[k] else 0. for k in data}
        if day in days:
            yield day,{k:dict(move=data[k]["move"],recall=prob[k],introduced=bool(strength[k]))
                       for k in data},dict(presentations=presentations,game_retrievals=retrievals)
        if day==max(days): break
        fresh=[k for k in order if not strength[k]]
        known=[k for k in order if strength[k]]
        n_new=min(len(fresh),max(daily//3,daily-len(known)))
        review=sorted(known,key=lambda k:(-data[k]["reach"]*(1-prob[k]),k))[:daily-n_new]
        for k in review:
            good=uniform(seed,day,0,f"study:{k}:{data[k]['move']}")<prob[k]
            strength[k]=min(365.,strength[k]*growth) if good else initial
            last[k]=day
        for k in fresh[:n_new]:
            strength[k]=initial; last[k]=day
        presentations+=len(review)+n_new
        for game,visits in enumerate(game_visits(day) if game_visits else ()):
            for ply,k in enumerate(visits):
                k=key(k)
                if k not in data or not strength[k]: break
                elapsed=day-last[k]
                if uniform(seed,day,ply,f"game:{game}:{k}")>=2**(-elapsed/strength[k]): break
                if elapsed>=1:
                    strength[k]=min(365.,strength[k]*growth); last[k]=day; retrievals+=1
