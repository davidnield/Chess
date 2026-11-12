"""Replay a saved book from board-level inputs, independently of the solver matrices."""
from collections import Counter

def replay_inputs(panel,inputs,anchor,policy,k,limit):
    mass = dict(panel.weights[k])
    score = fallback = exited = 0.
    paid = set()
    exit_plies = Counter()
    def stop(boards,arrivals,ply):
        nonlocal score,exited,fallback
        vals = inputs.values(boards,ply)
        for h,p in arrivals.items():
            score += p*vals[h]
            exited += p
            exit_plies[ply] += p
            if (ply,h) in inputs.unsupported:
                fallback += p
    for d,xs in enumerate(panel.layers):
        ply = anchor+2*d
        booked = {h:p for h,p in mass.items() if h in policy}
        out = {h:p for h,p in mass.items() if h not in policy}
        stop({h:panel.boards[h] for h in out},out,ply)
        paid.update(booked)
        mids = Counter()
        if d==len(panel.layers)-1:
            legal = inputs.candidates({h:panel.boards[h] for h in booked})
            boards = {}
            for h,p in booked.items():
                mid = policy[h]
                if mid not in list(legal[h])[:limit]:
                    raise ValueError("Book failed its final-ply gate")
                boards[mid] = legal[h][mid][1]
                mids[mid] += p
            stop(boards,mids,ply+1)
            break
        for h,p in booked.items():
            mid = policy[h]
            if mid not in panel.menu[h]:
                raise ValueError("Book move outside the common menu")
            mids[mid] += p
        counts = inputs.exact_counts({h:panel.boards[h] for h in mids})
        nxt,out,no_reply = Counter(),Counter(),Counter()
        active = set(panel.layers[d+1])
        for h,p in mids.items():
            total = sum(counts[h].values())
            if not total:
                no_reply[h] += p
                continue
            for child,q,_ in panel.white[h][1]:
                share = (counts[h].get(child,0)+k*q)/(total+k)
                if share:
                    (nxt if child in active else out)[child] += p*share
        stop({h:panel.boards[h] for h in no_reply},no_reply,ply+1)
        stop({h:panel.boards[h] for h in out},out,ply+2)
        mass = nxt
    if abs(exited-1.)>1e-8 or paid!=policy.keys():
        raise ValueError("Direct replay lost exit mass or contains unreachable cards")
    return dict(score=score,prior_exit_mass=fallback,exit_mass=exited,exit_plies=dict(exit_plies))
