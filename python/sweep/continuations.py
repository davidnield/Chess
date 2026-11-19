"""Resumable history-aware continuation expansion and greedy package selection.

No depth or opponent-frequency cutoff. A work limit writes an OPEN checkpoint.
The search follows independently analysed engine continuations (or fixed master
moves), retaining connecting prescriptions. Package choice is a reported greedy
heuristic, not a globally optimal allocation certificate.
"""
from __future__ import annotations
import json
import math
from pathlib import Path
import chess
from consumer_keys import stored_key
from .extensions import Route,severity,package_value,unresolved_bound,residual_exposure,residual_package_value
from .quality import score
from .spec import digest, DEFAULT_EXTENSION_METRIC
from .stages import atomic_json

def token(board):
    k=stored_key(board,True)
    return f"{k['lo']}:{k['hi']}"

def choose(routes,risks,base,threshold,metric=DEFAULT_EXTENSION_METRIC):
    value=package_value if metric=="covered-maximum" else residual_package_value
    """Consider each prefix, plus unions reaching the same transposed endpoint."""
    packages=set(); ends={}
    for route in routes:
        prefix=set()
        for k in route.positions:
            if k not in base: prefix.add(k)
            if prefix:
                packages.add(frozenset(prefix));ends.setdefault(k,set()).update(prefix)
    packages.update(frozenset(p) for p in ends.values())
    selected=set(); log=[]
    while packages:
        ranked=[]
        for p in packages:
            v=value(routes,risks,p,selected,base)
            if v["new_positions"] and v["density"]>=threshold:
                ranked.append((v["density"],v["marginal_exposure"],tuple(sorted(p)),v))
        if not ranked: break
        _,_,p,v=max(ranked,key=lambda r:(r[0],r[1],r[2]))
        selected.update(p);log.append(dict(positions=list(p),**v))
        packages.discard(frozenset(p))
    return selected,log

class Search:
    def __init__(self, *, base, white, analyse, reply, contract, threshold=.00003,
                 checkpoint=None, allowed=None, root_fen=chess.STARTING_FEN,metric=DEFAULT_EXTENSION_METRIC):
        if not math.isfinite(threshold) or not 0<threshold<1: raise ValueError("invalid extension threshold")
        if metric not in ("covered-maximum","remaining-maximum"): raise ValueError("unknown extension metric")
        self.metric=metric
        self.base=dict(base);self.white=white;self.analyse=analyse;self.reply=reply
        self.threshold=threshold;self.allowed=allowed;self.checkpoint=Path(checkpoint) if checkpoint else None
        self.identity=digest(dict(base=base,white=white,contract=contract,threshold=threshold,
                                  allowed=allowed,root_fen=root_fen,metric=metric,algorithm="prefix-union-greedy-v1"))
        self.root_fen=root_fen
        self.state=dict(identity=self.identity,expanded=0,risks={k:0. for k in base},decisions={},
                        analysis={},policies={},model_only_visits=0.,opponent_visits=0.,leaves=[dict(history=[],mass=1.,positions=[],complete=False,blocked=None)])
        if self.checkpoint and self.checkpoint.exists():
            state=json.loads(self.checkpoint.read_text(encoding="utf8"))
            if state["identity"]!=self.identity: raise ValueError("continuation checkpoint recipe changed")
            self.state=state
        self.contract=contract

    def board(self,history):
        b=chess.Board(self.root_fen)
        for u in history: b.push_uci(u)
        return b

    def result(self):
        routes=[Route(r["mass"],tuple(r["positions"]),r["complete"]) for r in self.state["leaves"]]
        selected,packages=choose(routes,self.state["risks"],self.base,self.threshold,self.metric)
        remaining=(unresolved_bound(routes,self.state["risks"],selected,self.base) if self.metric=="covered-maximum"
                   else sum(r.probability for r in routes if not r.complete))
        # This bounds unexplored exposure; it is not a proof of global package optimality.
        closed=remaining<self.threshold
        return dict(metric=self.metric,known_unbooked_exposure=residual_exposure(routes,self.state["risks"],selected,self.base),
                    state="bounded" if closed else "open",closed=closed,identity=self.identity,
                    additional={k:self.state["decisions"][k] for k in sorted(selected)},
                    additional_distinct=len(selected),base_distinct=len(self.base),
                    packages=packages,unresolved_exposure_bound=remaining,
                    blocked_mass=sum(r["mass"] for r in self.state["leaves"] if r["blocked"]),
                    expanded=self.state["expanded"],routes=len(routes),
                    deepest_ply=max(len(r["history"]) for r in self.state["leaves"]),
                    selection_method="greedy-prefix-and-transposition-union-packages",
                    global_optimality_certified=False,
                    opponent_visits=self.state["opponent_visits"],model_only_opponent_visits=self.state["model_only_visits"])

    def save(self):
        if self.checkpoint: atomic_json(self.checkpoint,self.state)

    def step(self):
        pending=[(r["mass"],-len(r["history"]),i) for i,r in enumerate(self.state["leaves"])
                 if not r["complete"] and not r["blocked"]]
        if not pending: return False
        _,_,i=max(pending); leaf=self.state["leaves"][i]
        b=self.board(leaf["history"])
        if b.is_game_over(claim_draw=False):
            leaf["complete"]=True;self.state["expanded"]+=1;return True
        children=[]
        def child(move,mass,positions):
            if mass>0:
                children.append(dict(history=leaf["history"]+[move],mass=mass,positions=positions,
                                     complete=False,blocked=None))
        if b.turn==self.white:
            k=token(b);positions=leaf["positions"]+[k]
            if k in self.base:
                move=self.base[k]
            else:
                if self.allowed is not None and k not in self.allowed:
                    leaf["blocked"]="missing-from-extended-master";return False
                if k not in self.state["analysis"]:
                    required=self.allowed[k] if self.allowed is not None else None
                    # The callback MUST certify top-three coverage and independent evidence.
                    a=self.analyse(b.copy(stack=False),required)
                    legal=b.legal_moves.count();top=a["top"]
                    if (a.get("independent") is not True or a.get("nodes",0)<20_000_000
                        or len(top)!=min(3,legal) or len({r["uci"] for r in top})!=len(top)
                        or not a.get("receipt")):
                        raise ValueError("extension gaps require independent top-three evidence")
                    sign=1 if self.white else -1
                    cp=[sign*r["cp_white"] for r in top]
                    if any(not math.isfinite(c) for c in cp): raise ValueError("invalid engine scores")
                    if cp!=sorted(cp,reverse=True): raise ValueError("engine top moves are not ranked")
                    for r in top: b.parse_uci(r["uci"])
                    move=required or top[0]["uci"]
                    owncp=(next((sign*r["cp_white"] for r in top if r["uci"]==move),None))
                    if owncp is None:
                        owncp=sign*a["extra"][move]
                    if not math.isfinite(owncp): raise ValueError("invalid prescribed engine score")
                    if score(owncp)<self.contract["absolute_floor"]-1e-12 or score(cp[0])-score(owncp)>self.contract["relative_limit"]+1e-12:
                        leaf["blocked"]="engine-gate";return False
                    self.state["analysis"][k]=a
                    self.state["risks"][k]=severity([score(c) for c in cp],legal)["severity"]
                    self.state["decisions"][k]=move
                move=self.state["decisions"][k]
            b.parse_uci(move)
            child(move,leaf["mass"],positions)
        else:
            k=token(b)
            if k not in self.state["policies"]:
                self.state["policies"][k]=self.reply(b.copy(stack=False))
            p=self.state["policies"][k]
            moves=p["moves"];stop=p.get("stop",0.)
            if (set(moves)!={m.uci() for m in b.legal_moves}
                or any(not math.isfinite(v) or v<0 for v in [stop,*moves.values()])
                or not math.isclose(stop+sum(moves.values()),1.,abs_tol=1e-9)):
                raise ValueError("continuation policy must account for every legal move and nonmove mass")
            self.state["opponent_visits"]+=leaf["mass"]
            support=p.get("support")
            if support is not None and support["broad"]+support["otb"]==0:
                self.state["model_only_visits"]+=leaf["mass"]
            for u,mass in sorted(moves.items()): child(u,leaf["mass"]*mass,leaf["positions"])
            if stop:
                children.append(dict(leaf,mass=leaf["mass"]*stop,complete=True,blocked=None))
        self.state["leaves"][i:i+1]=children
        self.state["expanded"]+=1
        return True

    def run(self, steps, checkpoint_every=25):
        if steps<0 or checkpoint_every<1: raise ValueError("invalid work limit")
        for n in range(steps):
            # Selection is quadratic in route count; evaluate at checkpoint boundaries.
            if n%checkpoint_every==0 and self.result()["closed"]: break
            if not self.step():
                if all(r["complete"] or r["blocked"] for r in self.state["leaves"]): break
            if (n+1)%checkpoint_every==0: self.save()
        self.save()
        result=self.result()
        if not result["closed"]: result["stop_reason"]="needs-analysis-or-master-revision"
        return result
