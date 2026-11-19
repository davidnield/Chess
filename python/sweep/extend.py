"""Bounded work on unlimited extensions, with frozen engine and selective OTB inputs.

Default is readiness only. --execute permits at most --steps expansions per call;
a repeated call resumes its checkpoint. OPEN results are never published as a
finished repertoire. This entrypoint produces an extension prescription artifact;
PGN/ladder publication remains a separate, audited step.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys
import chess
import chess.engine
import polars as pl
from consumer_keys import ConsumerKeys,checked_board,stored_key
import engine_candidates as ec
from .continuations import Search,token
from .frontier import from_config
from .spec import digest
from .stages import file_sha,atomic_json,artifacts

def load_book(path,colour):
    result={}
    for r in pl.read_parquet(path).filter((pl.col("side_to_move")==colour)&
                                          pl.col("best_move").is_not_null()).to_dicts():
        b=checked_board(r["position_epd"],r["position_hash"])
        if b.turn!=(colour=="white"): raise ValueError("book colour differs from board turn")
        move=b.parse_san(r["best_move"]).uci();k=token(b)
        if k in result and result[k]!=move: raise ValueError("conflicting book prescriptions")
        result[k]=move
    return result

class Engine:
    """Independent 20M MultiPV top three; normalized DAG counters, persistent cache."""
    def __init__(self,cache,nodes=20_000_000):
        self.engine=None;self.cache=Path(cache);self.cache.mkdir(parents=True,exist_ok=True)
        self.contract=dict(engine_sha256=file_sha(ec.SF),nodes=nodes,threads=1,hash_mb=256,
                           kind="multipv3-position-only-v1",mate_cp=2000)
        self.identity=digest(self.contract)
    def __call__(self,board,required=None):
        # Full-key position model: no route-dependent engine counters or repetition state.
        b=chess.Board(board.epd()+" 0 1")
        key=stored_key(b,True);path=self.cache/(digest(dict(key=key,required=required,contract=self.contract))+".json")
        if path.exists():
            saved=json.loads(path.read_text(encoding="utf8"))
            if saved["epd"]!=b.epd() or saved["contract"]!=self.contract:
                raise ValueError("extension analysis cache mismatch")
            a=dict(saved["analysis"]);receipt=a.pop("receipt")
            if digest(dict(key=key,analysis=a))!=receipt: raise ValueError("extension analysis cache corrupted")
            return saved["analysis"]
        if self.engine is None:
            self.engine=chess.engine.SimpleEngine.popen_uci(ec.SF)
            self.engine.configure({"Threads":1,"Hash":256})
        self.engine.configure({"Clear Hash":None})
        raw=self.engine.analyse(b,chess.engine.Limit(nodes=self.contract["nodes"]),
                                multipv=min(3,b.legal_moves.count()),game=path.name)
        top=[dict(uci=x["pv"][0].uci(),cp_white=x["score"].white().score(mate_score=2000)) for x in raw]
        top.sort(key=lambda r:((1 if b.turn else -1)*r["cp_white"],r["uci"]),reverse=True)
        extra={}
        if required and required not in {r["uci"] for r in top}:
            self.engine.configure({"Clear Hash":None})
            x=self.engine.analyse(b,chess.engine.Limit(nodes=self.contract["nodes"]),
                                  root_moves=[b.parse_uci(required)],game=path.name+"extra")
            extra[required]=x["score"].white().score(mate_score=2000)
        payload=dict(top=top,extra=extra,independent=True,nodes=self.contract["nodes"],
                     actual_nodes=max(x.get("nodes",0) for x in raw),
                     depth=min(x.get("depth",0) for x in raw),contract=self.contract)
        payload["receipt"]=digest(dict(key=key,analysis=payload))
        atomic_json(path,dict(epd=b.epd(),contract=self.contract,analysis=payload))
        return payload
    def close(self):
        if self.engine: self.engine.quit();self.engine=None

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--book",type=Path,required=True)
    p.add_argument("--policy-config",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    p.add_argument("--engine-cache",type=Path,required=True)
    p.add_argument("--allowed-book",type=Path)
    p.add_argument("--threshold",type=float,default=.00003)
    p.add_argument("--metric",choices=["covered-maximum","remaining-maximum"],default="covered-maximum",
                   help="remaining-maximum is the proposed comparison; see the progress log")
    p.add_argument("--steps",type=int,default=1000)
    p.add_argument("--execute",action="store_true")
    a=p.parse_args()
    meta_path=Path(str(a.book)+".meta.json");meta=json.loads(meta_path.read_text(encoding="utf8"))
    if meta.get("verification_policy")!="quality" or meta.get("key_bits")!=128:
        p.error("extensions need a full-key quality book")
    # Require actual closure rather than interpreting quality mode as completed verification.
    from rare_verify import request_plan
    if request_plan(a.book,meta["perspective"])["requests"]:
        p.error("base book is not independently verification-closed")
    cfg=json.loads(a.policy_config.read_text(encoding="utf8"));artifacts(cfg["artifacts"])
    pinned=[dict(path=str(p.resolve()),sha256=file_sha(p)) for p in
            (a.book,meta_path,a.policy_config,*([a.allowed_book] if a.allowed_book else []))]
    pinned+=cfg["artifacts"]
    inputs=dict(book=file_sha(a.book),policy=file_sha(a.policy_config),recipe=cfg["recipe"],
                sources=cfg["artifacts"],code={str(p):file_sha(p) for p in Path(__file__).parent.glob("*.py")})
    contract=dict(inputs=inputs,absolute_floor=meta["absolute_score_floor"],relative_limit=meta["max_score_loss"])
    if not a.execute:
        print(json.dumps(dict(state="ready",starts_jobs=False,contract=contract),indent=2));return
    a.out.mkdir(parents=True,exist_ok=True)
    keys=ConsumerKeys(meta["eval_lookup128"],expected_sha=meta["eval_lookup_sha256"])
    prior=from_config(a.policy_config,colour=meta["perspective"],keys=keys,pool=meta["stats"],aux=meta["aux_stats"])
    engine=Engine(a.engine_cache);contract["engine"]=engine.contract
    def reply(b):
        h=keys.board(b);prior.origins.setdefault(h,b.fen())
        frame=prior.rows([h])
        mass=frame["move_mass"][0];nonmove=frame["nonmove"][0];total=mass+nonmove
        return dict(moves={b.parse_san(r["move_san"]).uci():r["policy_p"]*mass/total for r in frame.to_dicts()},
                    stop=nonmove/total,
                    support={name:frame["support_"+name][0] for name in ("broad","adult","target","otb")})
    try:
        search=Search(base=load_book(a.book,meta["perspective"]),white=meta["perspective"]=="white",
                      analyse=engine,reply=reply,contract=contract,threshold=a.threshold,
                      allowed=load_book(a.allowed_book,meta["perspective"]) if a.allowed_book else None,
                      checkpoint=a.out/"checkpoint.json",metric=a.metric)
        report=search.run(a.steps)
        # Preserve every policy used over earlier resumptions as well as this invocation.
        # The immutable per-position cache and pinned recipe reproduce earlier records.
        if prior.used: prior.publish(a.out/"opponent-last-pass.parquet")
        artifacts(pinned)
        report.update(contract=contract,production_release=False,
                      checkpoint_sha256=file_sha(a.out/"checkpoint.json"))
        atomic_json(a.out/"status.json",report)
        if report["closed"]:
            atomic_json(a.out/"extensions.json",dict(report,base=search.base))
        print(json.dumps({k:v for k,v in report.items() if k not in ("contract","packages","additional")},indent=2))
    finally: engine.close();prior.close()

if __name__=="__main__": main()
