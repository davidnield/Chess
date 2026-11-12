"""Persistent, recipe-scoped inputs for root-led repertoire experiments."""
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import chess

from root_pilot_data import ARCHIVE, AUX, POOL, PYTHON, Data
from eval_arrays import DEFAULT_ARRAY_DIR, META_NAME


def digest(obj):
    return hashlib.sha256(json.dumps(obj,sort_keys=True,separators=(",",":")).encode()).hexdigest()


def stamp(path):
    path = Path(path)
    st = path.stat()
    return [str(path.resolve()),st.st_size,st.st_mtime_ns]


def sha(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f,"sha256").hexdigest()


class Cache:
    def __init__(self,path):
        path.parent.mkdir(parents=True,exist_ok=True)
        self.db = sqlite3.connect(path,timeout=120)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS inputs (kind TEXT, recipe TEXT, ply INTEGER, h INTEGER, epd TEXT, payload TEXT, PRIMARY KEY(kind,recipe,ply,h)) WITHOUT ROWID")
        self.stats = Counter()

    def batch(self,kind,recipe,ply,boards,compute,chunk=2048):
        result = {}
        keys = sorted(boards)
        for i in range(0,len(keys),400):
            hs = keys[i:i+400]
            qs = ",".join("?" for _ in hs)
            rows = self.db.execute(f"SELECT h,epd,payload FROM inputs WHERE kind=? AND recipe=? AND ply=? AND h IN ({qs})",
                                   [kind,recipe,ply,*hs])
            for h,epd,payload in rows:
                if epd!=boards[h]:
                    raise ValueError(f"Position identity mismatch in {kind} at {h}")
                result[h] = json.loads(payload)
        self.stats[kind+"_hits"] += len(result)
        missing = [h for h in keys if h not in result]
        for i in range(0,len(missing),chunk):
            batch = {h:boards[h] for h in missing[i:i+chunk]}
            new = compute(batch)
            if new.keys()!=batch.keys():
                raise ValueError(f"Incomplete {kind} input batch")
            with self.db:
                for h,item in new.items():
                    payload = json.dumps(item,separators=(",",":"),allow_nan=False)
                    cur = self.db.execute("INSERT OR IGNORE INTO inputs VALUES (?,?,?,?,?,?)",
                                          (kind,recipe,ply,h,boards[h],payload))
                    if not cur.rowcount:
                        epd,payload = self.db.execute("SELECT epd,payload FROM inputs WHERE kind=? AND recipe=? AND ply=? AND h=?",
                                                      (kind,recipe,ply,h)).fetchone()
                        if epd!=boards[h]:
                            raise ValueError("Conflicting concurrent cache write")
                        item = json.loads(payload)
                    result[h] = item
            self.stats[kind+"_misses"] += len(batch)
        return result


class Inputs:
    def __init__(self,cache,folder):
        from shared_slice_stops import Stops
        self.cache = cache
        self.data = Data()
        self.stops = Stops(folder,self.data)
        core = PYTHON/"experiments"
        common = [sha(core/"root_pilot_data.py"),sha(PYTHON/"zobrist.py"),chess.__version__]
        self.coarse_key = digest([common,stamp(POOL),stamp(AUX),self.data.pool_meta,sha(ARCHIVE/"_collisions.parquet")])
        params = self.data.pool_meta["params"]
        stamps = []
        for event in params["events"]:
            for band in params["elo_bands"]:
                directory = ARCHIVE/"ps"/f"event={event}"/f"elo_band={band}"
                with os.scandir(directory) as entries:
                    stamps.extend((event,band,e.name,e.stat().st_size,e.stat().st_mtime_ns) for e in entries if e.name.endswith(".parquet"))
        self.raw_key = digest([self.coarse_key,sorted(stamps),sha(PYTHON/"pool_from_book.py")])
        engine = [sha(DEFAULT_ARRAY_DIR/META_NAME),*[stamp(DEFAULT_ARRAY_DIR/f) for f in ("eval_cp.npy","eval_hash.npy")],
                  self.stops.recipe["engine_candidates_sha256"],sha(PYTHON/"eval_arrays.py")]
        self.candidate_key = digest([common,engine,sha(PYTHON/"budget_core.py"),sha(PYTHON/"stage3_backwards_induction.py")])
        stop_sources = [engine,self.stops.recipe,self.stops.model,self.stops.prior,
                                *[stamp(self.stops.recipe[k]) for k in ("stats","weak_pool")],
                                *[sha(folder/f) for f in ("stop-table.parquet","stop-positions.parquet")],
                                *[sha(p) for p in (core/"shared_slice_stops.py",PYTHON/"sampled_stop.py",PYTHON/"weak_stop.py")]]
        self.stop_key = digest([*stop_sources,chess.__version__,
                                *[sha(PYTHON/f) for f in ("budget_core.py","stage3_backwards_induction.py","duck_compat.py")]])
        self.unsupported = set()

    def populate(self,boards):
        def create(batch):
            self.data.populate(batch)
            return {h:[list(self.data.coarse[h][0].items()),self.data.coarse[h][1]] for h in batch}
        fresh = {h:e for h,e in boards.items() if h not in self.data.coarse}
        rows = self.cache.batch("coarse",self.coarse_key,0,fresh,create)
        self.data.coarse.update({h:(dict(cs),n) for h,(cs,n) in rows.items()})

    @property
    def coarse(self):
        return self.data.coarse

    def exact_counts(self,boards):
        self.populate(boards)
        def create(batch):
            return {h:list(c.items()) for h,c in self.data.exact_counts(batch).items()}
        complete = {h:dict(c) for h in boards for c,n in [self.coarse[h]] if n is not None and n==sum(c.values())}
        rows = self.cache.batch("raw",self.raw_key,0,{h:e for h,e in boards.items() if h not in complete},create,4096)
        return complete | {h:dict(items) for h,items in rows.items()}

    def candidates(self,boards):
        def create(batch):
            cs = self.data.candidates(batch)
            return {h:[[ch,s,e] for ch,(s,e) in sorted(moves.items(),key=lambda kv:(self.data.cp[kv[0]],kv[1][0]))] for h,moves in cs.items()}
        rows = self.cache.batch("moves",self.candidate_key,0,boards,create)
        return {h:{ch:(s,e) for ch,s,e in moves} for h,moves in rows.items()}

    def values(self,boards,ply):
        def create(batch):
            vals = self.stops.get(batch,ply)
            return {h:[v,(ply,h) in self.stops.unsupported] for h,v in vals.items()}
        rows = self.cache.batch("stop",self.stop_key,ply,boards,create,8192)
        self.unsupported.update((ply,h) for h,(_,flag) in rows.items() if flag)
        return {h:v for h,(v,_) in rows.items()}
