"""Selective, full-key OTB policy materialization for the native collector.

Counts and predictions are fetched only for opponent frontiers. The cache stores
one immutable record per position and complete source/model recipe. Every used
record is frozen into a normal schema-3 overlay before a book is published.
"""
from __future__ import annotations
import json
import math
from pathlib import Path
import sqlite3
import time
import chess
import polars as pl
import duckdb
from consumer_keys import PAIR, checked_board, stored_key
from .policy import ensemble, overlay_rows, validate_overlay_rows, write_overlay
from .spec import POLICY, digest
from .stages import artifacts, file_sha, atomic_json

def pair(k):
    return k["lo"],k["hi"]

def token(k):
    return f"{k[0]}:{k[1]}"

class Counts:
    """Batched keyed reads. Source pools must already be uncut, full-key, and frozen."""
    names=("broad","adult","rapid","classical","otb")
    def __init__(self, sources, aux, threads=2):
        self.sources={n:str(Path(sources[n]).resolve()) for n in self.names}
        self.aux=str(Path(aux).resolve())
        self.db=duckdb.connect(config={"threads":threads,"memory_limit":"4GB"})
        self.queries=0
        for path,column in [(p,"parent_hash") for p in self.sources.values()]+[(self.aux,"position_hash")]:
            if pl.read_parquet_schema(path)[column]!=PAIR:
                raise ValueError(f"policy counts require full struct keys: {path}")

    def fetch(self, boards):
        ks=[stored_key(b,True) for b in boards]
        query=pl.DataFrame({"h":ks},schema={"h":PAIR})
        self.db.register("policy_keys",query.to_arrow())
        out={pair(k):{n:[] for n in self.names}|{"aux":{}} for k in ks}
        try:
            for name,path in self.sources.items():
                self.queries+=1
                rows=pl.from_arrow(self.db.execute(
                                 "SELECT s.* FROM read_parquet(?) s SEMI JOIN policy_keys k ON s.parent_hash=k.h",[path]).to_arrow_table())
                for row in rows.to_dicts():
                    out[pair(row["parent_hash"])][name].append(row)
            self.queries+=1
            rows=pl.from_arrow(self.db.execute(
                "SELECT s.* FROM read_parquet(?) s SEMI JOIN policy_keys k ON s.position_hash=k.h",[self.aux]).to_arrow_table())
            for row in rows.to_dicts():
                target=out[pair(row["position_hash"])]
                if target["aux"]: raise ValueError("duplicate aux position")
                target["aux"]=row
        finally:
            self.db.unregister("policy_keys")
        return out

    def close(self): self.db.close()

def vectors(board, record):
    legal=sorted(m.uci() for m in board.legal_moves)
    counts={n:{u:0 for u in legal} for n in Counts.names}
    outcomes={}
    expected=stored_key(board,True)
    for name in Counts.names:
        seen=set()
        for row in record[name]:
            if row["parent_hash"]!=expected: raise ValueError("count lookup returned another full key")
            move=board.parse_san(row["move_san"]); u=move.uci()
            if u in seen: raise ValueError("duplicate move in a pooled count table")
            seen.add(u)
            n=row["total"]
            if not math.isfinite(n) or n<1 or int(n)!=n: raise ValueError("not an uncut observed-move count")
            board.push(move)
            if row["child_hash"]!=stored_key(board,True): raise ValueError("count child differs from legal replay")
            board.pop()
            counts[name][u]=n
            if name=="adult":
                result=tuple(row[c] for c in ("white_wins","draws","black_wins"))
                if sum(result)!=n or any(v<0 or int(v)!=v for v in result):
                    raise ValueError("invalid outcome counts")
                outcomes[u]=result
    aux=record["aux"]
    if aux.get("other_total",0)!=0: raise ValueError("cutoff-generated Other mass in an uncut pool")
    nonmove=sum(aux.get(group+"_total",0) or 0 for group in ("term_normal","term_flag","term_other","horizon"))
    if nonmove<0 or not math.isfinite(nonmove): raise ValueError("invalid nonmove mass")
    b,a=[list(counts[n].values()) for n in ("broad","adult")]
    r=[counts["rapid"][u]+counts["classical"][u] for u in legal]
    o=list(counts["otb"].values())
    return legal,b,a,r,o,outcomes,nonmove

class FrontierOverlay:
    def __init__(self, *, colour, keys, counts, predict, cache, recipe, input_artifacts,
                 pool, aux, verify_artifacts=True, cache_components=None):
        if keys is None: raise ValueError("selective OTB policy requires full keys")
        if recipe!=POLICY: raise ValueError("unexpected OTB ensemble recipe")
        if verify_artifacts: artifacts(input_artifacts)
        self.colour,self.keys,self.counts,self.predict=colour,keys,counts,predict
        self.policy_recipe=recipe
        self.inputs=input_artifacts
        self.pool,self.aux=pool,aux
        self.identity=digest(dict(recipe=recipe,inputs=cache_components or input_artifacts,implementation=2))
        Path(cache).parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(cache,timeout=60)
        self.db.execute("CREATE TABLE IF NOT EXISTS replies(recipe TEXT, key TEXT, value TEXT, PRIMARY KEY(recipe,key))")
        self.origins={keys.board(chess.Board()):chess.Board().fen(en_passant="fen")}
        self.used=set()
        self.recipe={"scope":"selective-otb-frontier","policy_recipe":recipe,"inputs":input_artifacts}
        self.stats=dict(requested=0,cache_hits=0,predicted=0,query_seconds=0.,model_seconds=0.)
        self.extra_aux={}

    def remember(self, frame):
        # Lazy child replay avoids parsing moves whose child never reaches the next frontier.
        for h,epd in frame.select("parent_hash","parent_epd").unique("parent_hash").iter_rows():
            self.origins[h]=epd
        for h,ch,san in frame.select("parent_hash","child_hash","move_san").iter_rows():
            if ch is not None: self.origins.setdefault(ch,(h,san))

    def board(self,h):
        origin=self.origins.get(h)
        if origin is None: raise ValueError(f"reached position has no replay origin: {h}")
        ext=self.keys.external(pl.Series("h",[h],dtype=pl.Int64))[0]
        if isinstance(origin,tuple):
            par,san=origin
            b=self.board(par); b.push_san(san)
            if stored_key(b,True)!=ext: raise ValueError("frontier child does not match full key")
        else:
            b=checked_board(origin,ext)
        self.origins[h]=b.fen(en_passant="fen")
        return b.copy(stack=False)

    def rows(self, ids):
        boards=[self.board(h) for h in ids]
        # A position-only DAG cannot infer a historical repetition or fifty-move claim.
        pairs=[pair(stored_key(b,True)) for b in boards]
        found={}; missing=[]
        for h,k,b in zip(ids,pairs,boards):
            self.used.add(token(k)); self.stats["requested"]+=1
            row=self.db.execute("SELECT value FROM replies WHERE recipe=? AND key=?",(self.identity,token(k))).fetchone()
            if row:
                found[k]=json.loads(row[0]); self.stats["cache_hits"]+=1
            else: missing.append((h,k,b))
        if missing:
            t=time.monotonic()
            data=self.counts.fetch([b for _,_,b in missing])
            self.stats["query_seconds"]+=time.monotonic()-t
            t=time.monotonic()
            priors=self.predict([b for _,_,b in missing])
            self.stats["model_seconds"]+=time.monotonic()-t
            if len(priors)!=len(missing): raise ValueError("Maia batch size mismatch")
            for (_,k,b),prior in zip(missing,priors):
                legal,bc,ac,rc,oc,outcomes,nonmove=vectors(b,data[k])
                if set(prior)!=set(legal): raise ValueError("Maia must cover every legal move")
                prob=ensemble(legal,bc,ac,rc,oc,[prior[u] for u in legal])
                observed=sum(ac)
                # Zero empirical arrivals means a model-only continuation, not a made-up result.
                mass=observed if observed or nonmove else 1.
                frame=overlay_rows(b,dict(zip(legal,prob)),empirical=outcomes,nonmove=nonmove,move_mass=mass)
                frame=frame.with_columns(pl.lit(sum(bc)).alias("support_broad"),
                      pl.lit(sum(ac)).alias("support_adult"),pl.lit(sum(rc)).alias("support_target"),
                      pl.lit(sum(oc)).alias("support_otb"))
                validate_overlay_rows(frame,self.colour)
                value=frame.to_dicts()
                with self.db:
                    self.db.execute("INSERT OR IGNORE INTO replies VALUES(?,?,?)",
                                    (self.identity,token(k),json.dumps(value,allow_nan=False)))
                # First writer wins if two arms inferred this board concurrently.
                found[k]=json.loads(self.db.execute("SELECT value FROM replies WHERE recipe=? AND key=?",
                                                   (self.identity,token(k))).fetchone()[0])
                self.stats["predicted"]+=1
        result=pl.DataFrame([r for k in pairs for r in found[k]],infer_schema_length=None)
        validate_overlay_rows(result,self.colour)
        return result

    def attach(self,frame,frontier):
        self.remember(frame)
        opponents=[h for h in frontier if self.board(h).turn!=(self.colour=="white")
                   and not self.board(h).is_game_over(claim_draw=False)]
        untouched=frame.with_columns(pl.lit(None,dtype=pl.Float64).alias("reply_mass"),
                    pl.lit(None,dtype=pl.Float64).alias("reach_share"),pl.lit(False).alias("additional_reply"))
        if not opponents: return untouched
        rows=self.keys.encode(self.rows(opponents),"parent_hash","child_hash")
        rows=rows.with_columns((pl.col("policy_p")*pl.col("move_mass")).alias("reply_mass"))
        rows=rows.with_columns((pl.col("reply_mass")/
                 (pl.col("reply_mass").sum().over("parent_hash")+pl.col("nonmove"))).alias("reach_share"))
        retained=frame.select("parent_hash","child_hash").unique().with_columns(pl.lit(True).alias("_old"))
        rows=rows.join(retained,on=["parent_hash","child_hash"],how="left").with_columns(
             pl.col("_old").is_null().alias("additional_reply"))
        self.remember(rows)
        rows=rows.filter(pl.col("reply_mass")>0)
        untouched=untouched.filter(~pl.col("parent_hash").is_in(opponents))
        return pl.concat([untouched,rows.select(untouched.columns)],how="vertical_relaxed")

    def extend_aux(self,frame,parents): return frame

    def publish(self,path):
        rows=[]
        for k in sorted(self.used):
            rows.extend(json.loads(self.db.execute("SELECT value FROM replies WHERE recipe=? AND key=?",
                                                  (self.identity,k)).fetchone()[0]))
        if not rows: raise ValueError("no reached opponent policy to publish")
        write_overlay(path,pl.DataFrame(rows,infer_schema_length=None),colour=self.colour,pool=self.pool,
                      aux=self.aux,input_artifacts=self.inputs,recipe=self.policy_recipe,scope="selective-otb-frontier")
        self.recipe=dict(file=str(Path(path).resolve()),sha256=file_sha(path),K=0,
                         scope="selective-otb-frontier",policy_recipe=self.policy_recipe,inputs=self.inputs,
                         cache_identity=self.identity,materialization=dict(self.stats))
        meta_path=Path(str(path)+".json")
        meta=json.loads(meta_path.read_text(encoding="utf8"))
        meta.update(cache_identity=self.identity,materialization=dict(self.stats))
        atomic_json(meta_path,meta)
        return self.recipe

    def close(self):
        self.db.close()
        self.counts.close()
        close=getattr(self.predict,"close",None)
        if close: close()

def from_config(path,*,colour,keys,pool,aux):
    from .maia_policy import MaiaPolicy,runtime_identity
    cfg=json.loads(Path(path).read_text(encoding="utf8"))
    if cfg["schema"]!=1 or cfg["recipe"]!=POLICY: raise ValueError("unsupported selective policy config")
    if Path(cfg["sources"]["adult"]).resolve()!=Path(pool).resolve() or Path(cfg["aux"]).resolve()!=Path(aux).resolve():
        raise ValueError("outcome pool or aux differs from the ensemble's adult component")
    pinned=artifacts(cfg["artifacts"])
    needed={str(Path(p).resolve()) for p in [*cfg["sources"].values(),aux,cfg["model"]["checkpoint"],cfg["model"]["python"]]}
    if not needed<=set(pinned): raise ValueError("selective policy sources/model are not all pinned")
    # Pin inference and preprocessing code too; checkpoint identity alone is insufficient.
    bound=list(cfg["artifacts"])
    code=Path(__file__).resolve().parents[1]
    for p in (Path(__file__),Path(__file__).with_name("maia_policy.py"),Path(__file__).with_name("policy.py"),
              code/"maia_server.py",code/"maia_batch.py"):
        bound.append(dict(path=str(p),sha256=file_sha(p)))
    runtime=runtime_identity(cfg["model"]["python"])
    runtime_path=Path(cfg["cache"]).parent/("runtime-"+digest(runtime)+".json")
    atomic_json(runtime_path,runtime)
    bound.append(dict(path=str(runtime_path.resolve()),sha256=file_sha(runtime_path)))
    cache_components=dict(sources={n:pinned[str(Path(p).resolve())] for n,p in cfg["sources"].items()},
         aux=pinned[str(Path(aux).resolve())],checkpoint=pinned[str(Path(cfg["model"]["checkpoint"]).resolve())],
         runtime=runtime,code={Path(r["path"]).name:r["sha256"] for r in bound[len(cfg["artifacts"]):-1]})
    counts=Counts(cfg["sources"],aux)
    model=MaiaPolicy(cfg["model"],POLICY)
    return FrontierOverlay(colour=colour,keys=keys,counts=counts,predict=model,cache=cfg["cache"],
                           recipe=POLICY,input_artifacts=bound,pool=pool,aux=aux,
                           verify_artifacts=False,cache_components=cache_components)
