"""Local data sources for the bounded root-search pilot. No repertoire determines eligibility."""
from collections import Counter, defaultdict
from contextlib import redirect_stdout
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import chess
import numpy as np

PYTHON = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PYTHON))
from zobrist import zobrist_int64

STATS = Path("E:/chess/position-stats")
POOL = STATS/"position_stats_pooled_ge2000_2013_2026_brc_p30_mg5.parquet"
AUX = STATS/POOL.name.replace("_pooled", "_aux_pooled")
WEAK = STATS/"weak_edges_b1000_1800_on_ge1800.parquet"
ARCHIVE = STATS/"explorer_banded_2013_2026"


def successors(epd):
    board = chess.Board(epd+" 0 1")
    children = {}
    for move in list(board.legal_moves):
        san = board.san(move)
        board.push(move)
        children[zobrist_int64(board)] = (san,board.epd())
        board.pop()
    return children


def worker():
    """Keep the GPU resident across frontier batches in the existing Maia interpreter."""
    import torch
    import maia_batch as mb
    torch.set_num_threads(2)
    with redirect_stdout(sys.stderr):
        cfg, model = mb.load("79m","D:/chess/maia/hf_cache/hub")
    with open(cfg.checkpoint_path,"rb") as f:
        checkpoint = hashlib.file_digest(f,"sha256").hexdigest()
    identity = dict(checkpoint=checkpoint, encoder=hashlib.sha256(Path(mb.__file__).read_bytes()).hexdigest(),
                    ratings=[2000,1450],history="current position repeated",version=1)
    print(json.dumps(identity),flush=True)
    for line in sys.stdin:
        recs = json.loads(line)
        policies = {h:{} for h,_ in recs}
        for i in range(0,len(recs),128):
            hs, tokens, moves = mb.prep(recs[i:i+128])
            if moves:
                p,_ = mb.run_batch(cfg,model,tokens,moves,2000,1450)
                for (row,_,s),v in zip(moves,p):
                    policies[hs[row]][s] = float(v)
        print(json.dumps(policies,allow_nan=False),flush=True)


class Maia:
    def __init__(self,out):
        out.mkdir(parents=True,exist_ok=True)
        self.sql = sqlite3.connect(out/"maia.sqlite")
        self.proc = subprocess.Popen(["D:/chess/maia/.venv/Scripts/python.exe","-X","utf8","-u",__file__],
                                     stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,encoding="utf8")
        identity = self.proc.stdout.readline()
        if not identity:
            raise RuntimeError("Maia failed to initialize")
        self.identity = json.loads(identity)
        self.sql.execute("CREATE TABLE IF NOT EXISTS policy(model TEXT,h INTEGER,epd TEXT,probs TEXT, PRIMARY KEY(model,h))")
        self.key = json.dumps(self.identity,sort_keys=True)
        self.used = set()
        self.inferred = 0

    def get(self, boards):
        cached,missing = {},[]
        for h,epd in sorted(boards.items()):
            row = self.sql.execute("SELECT epd,probs FROM policy WHERE model=? AND h=?",(self.key,h)).fetchone()
            if row is None:
                missing.append((h,epd))
            elif row[0] == epd:
                cached[h] = json.loads(row[1])
            else:
                raise ValueError(f"Cache identity mismatch at {h}")
        if missing:
            self.proc.stdin.write(json.dumps(missing)+"\n")
            self.proc.stdin.flush()
            output = json.loads(self.proc.stdout.readline())
            with self.sql:
                for h,epd in missing:
                    probs = output[str(h)]
                    if probs and not np.isclose(sum(probs.values()),1):
                        raise ValueError("Invalid Maia distribution")
                    self.sql.execute("INSERT INTO policy VALUES(?,?,?,?)",(self.key,h,epd,json.dumps(probs)))
                    cached[h] = probs
            self.inferred += len(missing)
        self.used.update(boards)
        return cached

    def close(self):
        self.proc.stdin.close()
        self.proc.wait()
        self.sql.close()


class Data:
    def __init__(self):
        import duckdb
        from eval_arrays import open_eval_arrays
        self.db = duckdb.connect(config={"threads":2,"memory_limit":"2GB"})
        self.arrays = open_eval_arrays()
        self.pool_meta = json.loads(Path(str(POOL)+".meta.json").read_text())
        if (self.pool_meta["min_games"] != 5 or self.pool_meta["max_ply"] != 30
                or self.pool_meta["book_meta_sha256"] != hashlib.sha256((ARCHIVE/"_book.meta.json").read_bytes()).hexdigest()):
            raise ValueError("Pilot requires the matching five-game pool and ply-30 source archive")
        self.collisions = {h for (h,) in self.db.execute("SELECT DISTINCT parent_hash FROM read_parquet(?)",
                                                      [str(ARCHIVE/"_collisions.parquet")]).fetchall()}
        self.coarse = {}
        self.weak = {}
        self.cp = {}

    def rows(self,path,key,cols,keys):
        ids = sorted(set(map(int,keys)))
        if not ids:
            return []
        where = ",".join(map(str,ids))
        return self.db.execute(f"SELECT {cols} FROM read_parquet(?,hive_partitioning=false) WHERE {key} IN ({where})",
                               [str(path)]).fetchall()

    def populate(self,boards):
        keys = set(boards)-self.coarse.keys()
        if keys.intersection(self.collisions):
            raise ValueError("Collision in pilot graph")
        counts = defaultdict(Counter)
        for h,ch,n in self.rows(POOL,"parent_hash","parent_hash,child_hash,total",keys):
            counts[h][ch] += n
        aux = dict(self.rows(AUX,"position_hash","position_hash,other_total",keys))
        if counts.keys()!=aux.keys():
            raise ValueError("Five-game sidecar does not match retained parents")
        for h in keys:
            self.coarse[h] = (counts[h],sum(counts[h].values())+aux[h]) if h in aux else ({},None)

    def weak_counts(self,keys):
        keys = set(keys)-self.weak.keys()
        self.weak.update({h:Counter() for h in keys})
        for h,ch,n in self.rows(WEAK,"parent_hash","parent_hash,child_hash,total",keys):
            self.weak[h][ch] += n

    def evals(self,keys):
        from eval_arrays import lookup_evals,MISSING
        ids = np.array(sorted(set(keys)-self.cp.keys()),dtype=np.int64)
        for h,v in zip(ids,lookup_evals(ids,*self.arrays) if len(ids) else ()):
            self.cp[int(h)] = int(v) if v!=MISSING else None

    def candidates(self,boards):
        from budget_core import gate_candidates
        from stage3_backwards_induction import cp_to_expected_score
        by_h = {h:successors(epd) for h,epd in boards.items()}
        keys = set(boards).union(*(set(cs) for cs in by_h.values()))
        self.evals(keys)
        ev = {h:cp_to_expected_score(self.cp[h]) for h in keys if self.cp[h] is not None}
        out = {}
        for h,cs in by_h.items():
            rows = [{"child_hash":ch,"move_san":s} for ch,(s,_) in cs.items()]
            gated = gate_candidates(h,rows,None,ev,False,.5195669627673518,.1,.1,.02,True,True,30)
            out[h] = {r["child_hash"]:cs[r["child_hash"]] for r,_ in gated}
        return out

    def exact_counts(self,boards):
        """Each Parquet slice is sorted by parent hash. Read only matched row groups."""
        from pool_from_book import slice_files
        import pyarrow.parquet as pq
        keys = defaultdict(list)
        for h in boards:
            keys[h%512].append(h)
        out = {h:Counter() for h in boards}
        for bucket,hs in sorted(keys.items()):
            target = np.array(sorted(hs),dtype=np.int64)
            params = self.pool_meta["params"]
            files = slice_files(ARCHIVE,params["events"],params["elo_bands"],bucket)
            if len(files)!=9:
                raise ValueError("Incomplete archive slices")
            for path in files:
                f = pq.ParquetFile(path)
                hi = f.schema_arrow.names.index("parent_hash")
                for g in range(f.num_row_groups):
                    stat = f.metadata.row_group(g).column(hi).statistics
                    ids = target[target.searchsorted(stat.min):target.searchsorted(stat.max,"right")]
                    if not len(ids):
                        continue
                    col = f.read_row_group(g,columns=["parent_hash"]).column(0).to_numpy()
                    lo,high = col.searchsorted(ids),col.searchsorted(ids,"right")
                    if not np.any(high>lo):
                        continue
                    payload = f.read_row_group(g,columns=["parent_epd","child_hash","total","ply"])
                    for h,l,r in zip(ids,lo,high):
                        if r>l:
                            for epd,ch,n,ply in zip(*(c.to_pylist() for c in payload.slice(l,r-l).columns)):
                                if epd!=boards[h] or ply>30:
                                    raise ValueError(f"Unexpected raw position at {h}")
                                out[h][ch] += n
        for h,counts in out.items():
            retained,total = self.coarse[h]
            if (total is not None and sum(counts.values())!=total) or any(counts[c]<n for c,n in retained.items()):
                raise ValueError(f"Archive and five-game pool disagree at {h}")
        return out


if __name__=="__main__":
    worker()
