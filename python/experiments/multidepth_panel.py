"""Root-led finite comparison panel. The same legal decision set serves both arms."""
from collections import Counter, defaultdict
import gzip
import json
import os
from pathlib import Path
import tempfile

import chess

from repertoire_input_cache import digest, sha
from root_pilot_data import successors, zobrist_int64
from root_search_pilot import KS, chance_inputs, discovery_policy
from shared_budget_slice import exact_policy


def save_panel(path,obj):
    with tempfile.NamedTemporaryFile(dir=path.parent,delete=False) as f:
        name = f.name
    try:
        with gzip.open(name,"wt",encoding="utf8",compresslevel=1) as out:
            json.dump(obj,out,separators=(",",":"),allow_nan=False)
        os.replace(name,path)
    finally:
        Path(name).unlink(missing_ok=True)


class Panel:
    def __init__(self):
        self.boards = {}
        self.white = {}
        self.layers = []
        self.menu = {}

    def remember(self,h,epd):
        if self.boards.setdefault(h,epd)!=epd:
            raise ValueError(f"Hash collision on panel: {h}")

    def actions(self,xs,inputs,limit):
        cs = inputs.candidates({h:self.boards[h] for h in xs if h not in self.menu})
        for h,moves in cs.items():
            self.menu[h] = list(moves)[:limit]
            for mid in self.menu[h]:
                self.remember(mid,moves[mid][1])

    def chances(self,mids,inputs,maia):
        new = sorted(set(mids)-self.white.keys())
        for i in range(0,len(new),384):
            nodes = {}
            chance_inputs(inputs,maia,{h:self.boards[h] for h in new[i:i+384]},nodes)
            for h,n in nodes.items():
                c,total = inputs.coarse[h]
                replies = []
                for ch,(_,epd) in n.legal.items():
                    self.remember(ch,epd)
                    replies.append([ch,n.q[ch],c.get(ch,0)])
                self.white[h] = (total,replies)

    def ranking(self,mass):
        return sorted(set().union(*mass.values()),key=lambda h:(-max(v.get(h,0.) for v in mass.values()),h))

    def discover(self,inputs,maia,anchor,widths,limit):
        starting = chess.Board()
        start = zobrist_int64(starting)
        prefix = {}
        chance_inputs(inputs,maia,{start:starting.epd()},prefix)
        node = prefix[start]
        raw = inputs.exact_counts({start:starting.epd()})[start]
        mass = {k:exact_policy(raw,node.q,k) for k in KS}
        roots = list(node.legal)
        for h,(_,e) in node.legal.items():
            self.remember(h,e)
        for ply in range(1,anchor,2):
            choices = inputs.candidates({h:self.boards[h] for h in roots})
            fixed = {h:next(iter(cs)) for h,cs in choices.items() if cs}
            boards = {mid:choices[h][mid][1] for h,mid in fixed.items()}
            prefix = {}
            chance_inputs(inputs,maia,boards,prefix)
            exact = inputs.exact_counts(boards)
            nxt = {k:Counter() for k in KS}
            for h,mid in fixed.items():
                for k in KS:
                    for ch,p in exact_policy(exact[mid],prefix[mid].q,k).items():
                        if p:
                            self.remember(ch,prefix[mid].legal[ch][1])
                            nxt[k][ch] += mass[k].get(h,0.)*p
            mass = nxt
            roots = self.ranking(mass)[:widths[0]]
        self.coverage = {k:sum(mass[k].get(h,0.) for h in roots) for k in KS}
        self.weights = {k:{h:mass[k].get(h,0.)/self.coverage[k] for h in roots} for k in KS}
        optimistic = self.weights
        for i,width in enumerate(widths):
            if i:
                roots = self.ranking(optimistic)[:width]
            self.layers.append(roots)
            print(f"Discovering ply {anchor+2*i}: {len(roots)} Black boards",flush=True)
            if i==len(widths)-1:
                break
            self.actions(roots,inputs,limit)
            self.chances((m for h in roots for m in self.menu[h]),inputs,maia)
            nxt = {k:Counter() for k in KS}
            for h in roots:
                for k in KS:
                    best = Counter()
                    for m in self.menu[h]:
                        n,edges = self.white[m]
                        named = sum(c for _,_,c in edges)
                        missing_q = sum(q for _,q,c in edges if not c)
                        for ch,q,c in edges:
                            # Used for panel membership only. The solver bounds the unnamed mass.
                            tail = (n-named)*q/missing_q if n and missing_q and not c else 0.
                            p = (c+k*q+tail)/(n+k) if n else q
                            best[ch] = max(best[ch],p)
                    for ch,p in best.items():
                        nxt[k][ch] += optimistic[k].get(h,0.)*p
            optimistic = nxt
        # Close every incoming route to any eligible card, including later/earlier-ply aliases.
        allowed = set().union(*self.layers)
        for i in range(len(widths)-1):
            xs = self.layers[i]
            self.actions(xs,inputs,limit)
            self.chances((m for h in xs for m in self.menu[h]),inputs,maia)
            child = {ch for h in xs for m in self.menu[h] for ch,_,_ in self.white[m][1] if ch in allowed}
            self.layers[i+1] = sorted(child)
        return self

    @classmethod
    def load(cls,folder,inputs,maia,anchor,widths,limit):
        key = digest([sha(__file__),sha(Path(__file__).with_name("root_search_pilot.py")),
                      sha(Path(__file__).with_name("shared_budget_slice.py")),inputs.coarse_key,
                      inputs.raw_key,inputs.candidate_key,maia.identity,KS,anchor,widths,limit])
        path = folder/f"panel-{key}.json.gz"
        if not path.exists():
            p = cls().discover(inputs,maia,anchor,widths,limit)
            save_panel(path,dict(key=key,boards=list(p.boards.items()),white=list(p.white.items()),
                                 menu=list(p.menu.items()),layers=p.layers,coverage=p.coverage,
                                 weights={k:list(v.items()) for k,v in p.weights.items()}))
            return p,key,False
        with gzip.open(path,"rt",encoding="utf8") as f:
            obj = json.load(f)
        if obj["key"]!=key:
            raise ValueError("Panel recipe mismatch")
        p = cls()
        p.boards,p.white,p.menu = (dict(obj[v]) for v in ("boards","white","menu"))
        p.layers = obj["layers"]
        p.coverage = {int(k):v for k,v in obj["coverage"].items()}
        p.weights = {int(k):dict(v) for k,v in obj["weights"].items()}
        return p,key,True
