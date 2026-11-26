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
from .policy import ensemble, needs_maia, overlay_rows, validate_overlay_rows, write_overlay
from .spec import POLICY, digest
from .keyed_parquet import KeyedParquet
from .stages import artifacts, file_sha, atomic_json

def pair(k):
    return k["lo"],k["hi"]

def token(k):
    return f"{k[0]}:{k[1]}"

class Counts:
    """Batched keyed reads. Source pools must already be uncut, full-key, and frozen."""
    names=("broad","adult","rapid","classical","otb")
    def __init__(self, sources, aux, threads=2, outcome_pool=None):
        self.sources={n:str(Path(sources[n]).resolve()) for n in self.names}
        self.aux=str(Path(aux).resolve())
        self.db=duckdb.connect(config={"threads":threads,"memory_limit":"4GB"})
        self.queries=0
        self.readers={n:KeyedParquet(p,"parent_hash") for n,p in self.sources.items()}
        if outcome_pool is not None:
            self.readers["outcome"]=KeyedParquet(outcome_pool,"parent_hash")
        self.readers["aux"]=KeyedParquet(self.aux,"position_hash")

    def fetch(self, boards):
        ks=[stored_key(b,True) for b in boards]
        out={pair(k):{n:[] for n in self.readers if n!="aux"}|{"aux":{}} for k in ks}
        for name,reader in self.readers.items():
            self.queries+=1
            for row in reader.fetch(self.db,ks).to_dicts():
                target=out[pair(row[reader.column])]
                if name=="aux":
                    if target["aux"]:raise ValueError("duplicate aux position")
                    target["aux"]=row
                else:target[name].append(row)
        return out

    def metrics(self):return {n:dict(r.stats) for n,r in self.readers.items()}

    def close(self):
        self.db.close()
        for r in self.readers.values():r.close()


def continuation_mass(observed, nonmove, other_support):
    """The outcome pool's continuing mass at a board: its observed moves. With none, a
    model-only continuation gets mass 1 when nothing ended there either; when only endings
    were recorded but another source (broad or OTB) saw moves, one pseudo-continuation keeps
    the ensemble's replies (review 2026-11-19); a board where every source only ended keeps 0."""
    if observed:
        return float(observed)
    return 1. if (not nonmove or other_support) else 0.


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
            if name=="adult" and "outcome" not in record:
                result=tuple(row[c] for c in ("white_wins","draws","black_wins"))
                if sum(result)!=n or any(v<0 or int(v)!=v for v in result):
                    raise ValueError("invalid outcome counts")
                outcomes[u]=result
    if "outcome" in record:
        for row in record["outcome"]:
            if row["parent_hash"]!=expected:raise ValueError("outcome pool full key differs")
            move=board.parse_san(row["move_san"]);u=move.uci()
            if u in outcomes:raise ValueError("duplicate outcome-pool move")
            child=board.copy(stack=False);child.push(move)
            if row["child_hash"]!=stored_key(child,True):raise ValueError("outcome child differs from legal replay")
            result=tuple(row[c] for c in ("white_wins","draws","black_wins"))
            if (sum(result)!=row["total"] or row["total"]<1 or
                any(not math.isfinite(v) or v<0 or int(v)!=v for v in result)):
                raise ValueError("invalid outcome-pool counts")
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
                 pool, aux, verify_artifacts=True, cache_components=None, semantics="legacy-nonmove-v1"):
        if keys is None: raise ValueError("selective OTB policy requires full keys")
        if recipe!=POLICY: raise ValueError("unexpected OTB ensemble recipe")
        if verify_artifacts: artifacts(input_artifacts)
        self.colour,self.keys,self.counts,self.predict=colour,keys,counts,predict
        self.policy_recipe=recipe
        self.inputs=input_artifacts
        self.pool,self.aux=pool,aux
        if semantics not in ("legacy-nonmove-v1","conditional-board-v1"):
            raise ValueError("unknown nonmove semantics")
        self.semantics=semantics
        self.identity=digest(dict(recipe=recipe,inputs=cache_components or input_artifacts,implementation=4,semantics=semantics))
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
            pr=self.policy_recipe
            gate=dict(maia_support_gate=pr.get("maia_support_gate"),backup_discount=pr["backup_discount"],
                      broad_discount=pr["broad_discount"])
            vecs=[vectors(b,data[k]) for _,k,b in missing]
            # Only boards below the support gate reach the model (otb-ensemble-v2).
            ask=[i for i,v in enumerate(vecs) if needs_maia(v[1],v[2],v[3],v[4],**gate)]
            t=time.monotonic()
            asked=self.predict([missing[i][2] for i in ask]) if ask else []
            self.stats["model_seconds"]+=time.monotonic()-t
            if len(asked)!=len(ask): raise ValueError("Maia batch size mismatch")
            priors=[None]*len(missing)
            for i,prior in zip(ask,asked): priors[i]=prior
            self.stats["maia_skipped"]=self.stats.get("maia_skipped",0)+len(missing)-len(ask)
            writes=[]
            for (_,k,b),prior,v in zip(missing,priors,vecs):
                legal,bc,ac,rc,oc,outcomes,nonmove=v
                observed_nonmove=nonmove
                if self.semantics=="conditional-board-v1":nonmove=0.
                if prior is not None and set(prior)!=set(legal): raise ValueError("Maia must cover every legal move")
                prob=ensemble(legal,bc,ac,rc,oc,None if prior is None else [prior[u] for u in legal],
                              otb_strength=pr["otb_strength"],empirical_strength=pr["empirical_strength"],
                              maia_strength=pr["maia_strength"],**gate)
                observed=sum(sum(v) for v in outcomes.values())
                # Zero empirical arrivals means a model-only continuation, not a made-up result.
                # The outcome pool can record only endings at a board other sources saw continue
                # (review 2026-11-19): keep one pseudo-continuation there, so the ensemble's
                # replies are not all dropped as zero mass.
                other_support=sum(bc)+sum(oc)
                mass=continuation_mass(observed,nonmove,other_support)
                if not observed and nonmove and other_support:
                    self.stats["endings_only_pseudo"]=self.stats.get("endings_only_pseudo",0)+1
                unseen=[p for p,bb,oo in zip(prob,bc,oc) if bb==0 and oo==0]
                self.stats["model_only_replies"]=self.stats.get("model_only_replies",0)+len(unseen)
                self.stats["model_only_probability"]=self.stats.get("model_only_probability",0.)+sum(unseen)
                frame=overlay_rows(b,dict(zip(legal,prob)),empirical=outcomes,nonmove=nonmove,move_mass=mass)
                frame=frame.with_columns(pl.lit(sum(bc)).alias("support_broad"),
                      pl.lit(sum(ac)).alias("support_adult"),pl.lit(sum(rc)).alias("support_target"),
                      pl.lit(sum(oc)).alias("support_otb"),
                      pl.lit(observed_nonmove).alias("observed_nonmove"),
                      pl.lit(data[k]["aux"].get("horizon_total",0) or 0).alias("observed_censored"))
                validate_overlay_rows(frame,self.colour)
                value=frame.to_dicts()
                writes.append((self.identity,token(k),json.dumps(value,allow_nan=False)))
                self.stats["predicted"]+=1
            with self.db:
                self.db.executemany("INSERT OR IGNORE INTO replies VALUES(?,?,?)",writes)
            for _,k,_ in missing:
                # First writer wins if two arms inferred this board concurrently.
                found[k]=json.loads(self.db.execute("SELECT value FROM replies WHERE recipe=? AND key=?",
                                                   (self.identity,token(k))).fetchone()[0])
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

    def extend_aux(self,frame,parents):
        if self.semantics=="legacy-nonmove-v1":return frame
        # A board-conditional policy continues until an actual board terminal.
        # Preserve raw flow data on disk and observed/censored diagnostics in the
        # policy receipt. Historical resignation/flags are not board transitions.
        cols=[c for c in frame.columns if c.startswith(("term_normal_","term_flag_","term_other_","horizon_"))]
        return frame.with_columns([pl.lit(0).cast(frame.schema[c]).alias(c) for c in cols])

    def conditional(self,boards):
        if self.semantics!="conditional-board-v1":
            raise ValueError("shared continuation requires conditional-board semantics")
        ids=[]
        for b in boards:
            h=self.keys.board(b);ids.append(h)
            self.origins[h]=" ".join(b.fen(en_passant="fen").split()[:4])
        if not ids:return []
        rows=self.rows(list(dict.fromkeys(ids)))
        by_key={}
        for row in rows.to_dicts():
            k=pair(row["parent_hash"]);by_key.setdefault(k,[]).append(row)
        result=[]
        for b in boards:
            records=by_key[pair(stored_key(b,True))]
            # Duplicated query boards must not duplicate their probabilities.
            result.append({b.parse_san(r["move_san"]).uci():r["policy_p"] for r in records})
        return result

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
                         cache_identity=self.identity,materialization=dict(self.stats),nonmove_semantics=self.semantics)
        meta_path=Path(str(path)+".json")
        meta=json.loads(meta_path.read_text(encoding="utf8"))
        meta.update(cache_identity=self.identity,materialization=dict(self.stats),nonmove_semantics=self.semantics,
                    count_lookup=self.counts.metrics() if hasattr(self.counts,"metrics") else {})
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
    separate=Path(cfg["sources"]["adult"]).resolve()!=Path(pool).resolve()
    if ((separate and cfg.get("nonmove_semantics")!="conditional-board-v1")
        or Path(cfg["aux"]).resolve()!=Path(aux).resolve()):
        raise ValueError("outcome pool or aux differs from the ensemble's adult component")
    pinned=artifacts(cfg["artifacts"])
    needed={str(Path(p).resolve()) for p in [*cfg["sources"].values(),pool,aux,cfg["model"]["checkpoint"],cfg["model"]["python"]]}
    if not needed<=set(pinned): raise ValueError("selective policy sources/model are not all pinned")
    # Pin inference and preprocessing code too; checkpoint identity alone is insufficient.
    bound=list(cfg["artifacts"])
    code=Path(__file__).resolve().parents[1]
    for p in (Path(__file__),Path(__file__).with_name("maia_policy.py"),Path(__file__).with_name("policy.py"),Path(__file__).with_name("keyed_parquet.py"),
              code/"maia_server.py",code/"maia_batch.py",code/"book_builder_rs.pyd"):
        bound.append(dict(path=str(p),sha256=file_sha(p)))
    runtime=runtime_identity(cfg["model"]["python"])
    runtime_path=Path(cfg["cache"]).parent/("runtime-"+digest(runtime)+".json")
    atomic_json(runtime_path,runtime)
    bound.append(dict(path=str(runtime_path.resolve()),sha256=file_sha(runtime_path)))
    cache_components=dict(sources={n:pinned[str(Path(p).resolve())] for n,p in cfg["sources"].items()},
         outcome=pinned[str(Path(pool).resolve())],aux=pinned[str(Path(aux).resolve())],checkpoint=pinned[str(Path(cfg["model"]["checkpoint"]).resolve())],
         runtime=runtime,code={Path(r["path"]).name:r["sha256"] for r in bound[len(cfg["artifacts"]):-1]})
    counts=Counts(cfg["sources"],aux,outcome_pool=pool if separate else None)
    model=MaiaPolicy(cfg["model"],POLICY)
    return FrontierOverlay(colour=colour,keys=keys,counts=counts,predict=model,cache=cfg["cache"],
                           recipe=POLICY,input_artifacts=bound,pool=pool,aux=aux,
                           verify_artifacts=False,cache_components=cache_components,
                           semantics=cfg.get("nonmove_semantics","legacy-nonmove-v1"))
