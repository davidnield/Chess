"""Batched own/opponent adapters shared by short continuation and native rollouts."""
import json
from pathlib import Path
import chess
import duckdb
import polars as pl
from consumer_keys import stored_key, checked_board
from .keyed_parquet import KeyedParquet
from .policy import PolicyCache, _vector
from .spec import digest
from .stages import artifacts, file_sha


class WeakCounts:
    def __init__(self,path):
        self.reader=KeyedParquet(path,"parent_hash")
        self.db=duckdb.connect(config={"threads":2,"memory_limit":"2GB"})
    def fetch(self,boards):
        keys=[stored_key(b,True) for b in boards]
        rows=self.reader.fetch(self.db,keys)
        groups={}
        for row in rows.to_dicts():
            k=row["parent_hash"];groups.setdefault((k["lo"],k["hi"]),[]).append(row)
        return [groups.get((k["lo"],k["hi"]),[]) for k in keys]
    def close(self):
        self.reader.close();self.db.close()


def count_vector(board,records):
    """Sorted legal UCI moves and their weak counts at a board, full key and child replay checked."""
    legal=sorted(m.uci() for m in board.legal_moves)
    if not legal:raise ValueError("terminal board has no own policy")
    counts=dict.fromkeys(legal,0.)
    seen=set();key=stored_key(board,True)
    for row in records:
        if row["parent_hash"]!=key:raise ValueError("weak count full key differs")
        move=board.parse_san(row["move_san"]);u=move.uci()
        if u in seen:raise ValueError("duplicate weak move")
        seen.add(u);child=board.copy(stack=False);child.push(move)
        if stored_key(child,True)!=row["child_hash"]:raise ValueError("weak child differs from legal replay")
        counts[u]=row["total"]
    return legal,_vector("weak counts",list(counts.values()),len(legal))


class OwnProvider:
    def __init__(self,*,counts,predict,cache,recipe,identity):
        self.counts,self.predict,self.recipe=counts,predict,recipe
        self.identity=digest(dict(recipe=recipe,inputs=identity,implementation=1))
        Path(cache).parent.mkdir(parents=True,exist_ok=True)
        self.cache=PolicyCache(cache,dict(identity=self.identity))
        self.stats=dict(boards=0,hits=0,weak_observations=0,predicted=0)
    def distributions(self,boards):
        from book_builder_rs import own_policy
        # Raw counts are read in one batch, independently of any model probability.
        rows=self.counts.fetch(boards)
        if len(rows)!=len(boards):raise ValueError("weak count batch differs")
        results=[None]*len(boards);pending=[]
        for i,(board,records) in enumerate(zip(boards,rows)):
            legal,n=count_vector(board,records)
            self.stats["weak_observations"]+=sum(n);self.stats["boards"]+=1
            fen=" ".join(board.fen(en_passant="fen").split()[:4])
            # Delay inference until the persistent content cache actually misses.
            def need():raise LookupError
            try:
                p,hit=self.cache.get_or_compute(fen,legal,n,need)
                results[i]=dict(zip(legal,p));self.stats["hits"]+=int(hit)
            except LookupError:pending.append((i,fen,legal,n,board))
        answers=self.predict([v[4] for v in pending]) if pending else []
        if len(answers)!=len(pending):raise ValueError("own Maia batch differs")
        if pending:
            with self.cache.db:
                self.cache.db.execute("BEGIN")
                for (i,fen,legal,n,_),q in zip(pending,answers):
                    if set(q)!=set(legal):raise ValueError("own Maia must cover all legal moves")
                    p,_=self.cache.get_or_compute(fen,legal,n,
                        lambda:own_policy(n,[q[u] for u in legal],self.recipe["strength"],
                                          self.recipe.get("observation_cap")),commit=False)
                    results[i]=dict(zip(legal,p));self.stats["predicted"]+=1
        return results
    def close(self):
        self.cache.close();self.counts.close()
        close=getattr(self.predict,"close",None)
        if close:close()


class SharedPolicies:
    def __init__(self,own,opponent,keys):
        self.own,self.opponent,self.keys=own,opponent,keys
    def distributions(self,ours,boards):
        return self.own.distributions(boards) if ours else self.opponent.conditional(boards)
    def native_batch(self,ours,ids,epds):
        boards=[]
        for h,epd in zip(ids,epds):
            key=self.keys.external(pl.Series("h",[h],dtype=pl.Int64))[0]
            boards.append(checked_board(epd,key))
        result=self.distributions(ours,boards)
        if len(result)!=len(boards):raise ValueError("shared policy batch differs")
        out=[]
        for board,p in zip(boards,result):
            legal={m.uci() for m in board.legal_moves}
            if set(p)!=legal:raise ValueError("shared policy is not a full legal vector")
            _vector("shared policy",list(p.values()),len(legal),True)
            out.append(sorted((board.san(board.parse_uci(u)),v) for u,v in p.items()))
        return out


def from_config(path,*,keys,opponent,weak_pool,candidate_cap):
    from .model_contract import validate
    from .maia_policy import MaiaPolicy,runtime_identity
    cfg=json.loads(Path(path).read_text(encoding="utf8"));recipe=cfg["recipe"];validate(recipe)
    if recipe["candidate_cap"]!=candidate_cap:raise ValueError("continuation construction cap differs")
    if Path(cfg["weak"]).resolve()!=Path(weak_pool).resolve():raise ValueError("weak source differs")
    if opponent.semantics!=recipe["nonmove"]:raise ValueError("opponent ending semantics differ")
    meta_path=Path(str(weak_pool)+".meta.json")
    meta=json.loads(meta_path.read_text(encoding="utf8"))
    pop=meta.get("population",{})
    if (meta.get("key_bits")!=128 or meta.get("min_games")!=1 or meta.get("max_ply")!=recipe["count_cap"]
        or sorted(pop.get("elo_bands",[]))!=[1000,1200,1400,1600]
        or sorted(pop.get("events",[]))!=["Blitz","Classical","Rapid"]):
        raise ValueError("weak source population/depth differs from contract")
    pinned=artifacts(cfg["artifacts"])
    required=[weak_pool,meta_path,cfg["model"]["checkpoint"],cfg["model"]["python"]]
    # The deeper evidence cap is a property of actual bound count files, never
    # inferred from the repertoire cap or the upstream explorer's advertised cap.
    for role in ("broad","adult","rapid","classical"):
        path=Path(opponent.counts.sources[role]+".meta.json")
        source=json.loads(path.read_text(encoding="utf8"))
        if source.get("max_ply")!=recipe["count_cap"]:
            raise ValueError(f"opponent {role} count horizon differs")
        required.append(path)
    if Path(opponent.counts.sources["otb"]).name!=f"training-p{recipe['count_cap']}.parquet":
        raise ValueError("opponent OTB role/count horizon differs")
    if not {str(Path(p).resolve()) for p in required}<=set(pinned):raise ValueError("own policy inputs not pinned")
    runtime=runtime_identity(cfg["model"]["python"])
    code=Path(__file__).resolve().parents[1]
    identity=dict(inputs=pinned,runtime=runtime,code={p.name:file_sha(p) for p in
        [Path(__file__),Path(__file__).with_name("maia_policy.py"),Path(__file__).with_name("policy.py"),
         Path(__file__).with_name("keyed_parquet.py"),Path(__file__).with_name("count_pool_reader.py"),
         Path(__file__).with_name("count_pool.py"),Path(__file__).with_name("prediction_cache.py"),
         Path(__file__).with_name("partitioned_pool.py"),Path(__file__).with_name("partitioned_pool_reader.py"),
         code/"book_builder_rs.pyd",code/"maia_server.py",code/"maia_batch.py"]})
    model=dict(cfg["model"])
    model.setdefault("raw_cache",str(Path(cfg["cache"]).parent/"maia-raw-v1.sqlite"))
    own=OwnProvider(counts=WeakCounts(weak_pool),predict=MaiaPolicy(model,recipe["own"],runtime=runtime),
                    cache=cfg["cache"],recipe=recipe["own"],identity=identity)
    return SharedPolicies(own,opponent,keys),recipe
