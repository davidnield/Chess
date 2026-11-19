"""Batched paired rollouts with full-key books, arbitrary horizon and pluggable policies.

Callbacks supply predictions, recall probabilities and independent leaf scores.
History-aware policies receive the actual root-to-position move sequence. No
model/backend or rating-panel choice is silently made by this evaluator.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib
import math
import chess
from zobrist128 import zobrist128

@dataclass
class Game:
    arm: str
    colour: str
    number: int
    board: chess.Board
    recalled: int=0
    lapses: int=0
    recovered: int=0

def uniform(seed,game,ply,stream):
    data=f"{seed}:{game}:{ply}:{stream}".encode()
    return (int.from_bytes(hashlib.blake2b(data,digest_size=8).digest(),"big") >> 11)/2**53

def request(game,ply):
    b=game.board
    return dict(arm=game.arm,colour=game.colour,game=game.number,ply=ply,
                fen=b.fen(),position_hash=zobrist128(b),
                history=tuple(m.uci() for m in b.move_stack),
                legal=tuple(sorted(m.uci() for m in b.legal_moves)),
                ours=b.turn==(game.colour=="white"))

def draw(dist, legal, u):
    if set(dist)!=set(legal) or any(not math.isfinite(p) or p<0 for p in dist.values()):
        raise ValueError("human policy must supply the complete legal distribution")
    if not math.isclose(sum(dist.values()),1.,abs_tol=1e-9):
        raise ValueError("human policy is not normalized")
    cum=0.
    for move in sorted(dist):
        cum+=dist[move]
        if u<cum: return move
    return max(dist,key=lambda m: (dist[m]>0,m)) # only floating residue reaches this

def play(arms, *, games, seed, horizon, predict, recall, value_leaves):
    """arms: {id, colour, moves:{(lo,hi):UCI}}.
    predict receives a batch of requests; recall returns P(remember) for an own
    booked visit; value_leaves returns own-side scores for nonterminal requests.
    Exact terminal outcomes never go through engine/model conversion.
    """
    if games<1 or horizon<1 or not arms: raise ValueError("invalid rollout dimensions")
    by_id={a["id"]:a for a in arms}
    if len(by_id)!=len(arms): raise ValueError("duplicate arm IDs")
    live=[Game(a["id"],a["colour"],i,chess.Board()) for a in arms for i in range(games)]
    done=[]
    for ply in range(horizon):
        need=[]; humans=[]; moves=[]
        for g in live:
            q=request(g,ply)
            booked=by_id[g.arm]["moves"].get(q["position_hash"]) if q["ours"] else None
            if booked is not None and booked not in q["legal"]:
                raise ValueError(f"illegal prescription in {g.arm}: {booked}")
            p=recall(q) if booked else 0.
            if not math.isfinite(p) or not 0<=p<=1: raise ValueError("invalid recall probability")
            if booked and uniform(seed,g.number,ply,"recall")<p:
                g.recalled+=1; moves.append((g,booked))
            else:
                if booked: g.lapses+=1
                need.append(q); humans.append((g,booked))
        predictions=predict(need) if need else []
        if len(predictions)!=len(need): raise ValueError("policy batch size mismatch")
        for (g,booked),q,dist in zip(humans,need,predictions):
            move=draw(dist,q["legal"],uniform(seed,g.number,ply,"human"))
            if booked==move: g.recovered+=1
            moves.append((g,move))
        live=[]
        for g,move in moves:
            g.board.push_uci(move)
            if g.board.is_game_over(claim_draw=False) or ply+1==horizon: done.append(g)
            else: live.append(g)
        if not live: break
    terminal={}; need=[]; indices=[]
    for i,g in enumerate(done):
        out=g.board.outcome(claim_draw=False)
        if out:
            terminal[i]=.5 if out.winner is None else float(out.winner==(g.colour=="white"))
        else:
            need.append(request(g,g.board.ply())); indices.append(i)
    values=value_leaves(need) if need else []
    if len(values)!=len(need) or any(not math.isfinite(v) or not 0<=v<=1 for v in values):
        raise ValueError("leaf valuation incomplete or invalid")
    terminal.update(zip(indices,values))
    return [dict(arm=g.arm,colour=g.colour,game=g.number,seed=seed,score=terminal[i],
                 ply=g.board.ply(),recalled=g.recalled,lapses=g.lapses,recovered=g.recovered)
            for i,g in enumerate(done)]
