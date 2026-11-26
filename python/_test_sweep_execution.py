"""Selective-policy and production-workflow regressions on tiny full-key fixtures."""
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import chess
import duckdb
import polars as pl
import numpy as np
from consumer_keys import ConsumerKeys, PAIR, stored_key
import book_builder_rs
from build_budget_books import collect_subgraph_edges_rs
from sweep.frontier import Counts, FrontierOverlay, vectors
from sweep.spec import POLICY
from sweep.stages import file_sha
from opponent_prior import ReplyOverlay

def board(path=""):
    b=chess.Board()
    for san in path.split(): b.push_san(san)
    return b

def edge(path,san,n):
    b=board(path); parent=stored_key(b,True); epd=b.epd()
    b.push_san(san)
    return dict(parent_hash=parent,parent_epd=epd,child_hash=stored_key(b,True),move_san=san,
                total=n,white_wins=n,draws=0,black_wins=0,white_score_avg=1.)

def keys():
    k=ConsumerKeys.__new__(ConsumerKeys);k.native=book_builder_rs.RsPositionIds();return k

class Model:
    def __init__(self): self.seen=[]
    def __call__(self,boards):
        self.seen.extend(b.fen() for b in boards)
        return [{m.uci():1/b.legal_moves.count() for m in b.legal_moves} for b in boards]
    def close(self): pass

class SelectivePolicy(unittest.TestCase):
    def prepare(self,root):
        adult=pl.DataFrame([edge("","e4",10),edge("","d4",10),edge("e4","e5",6),edge("d4","d5",6)])
        sources={}
        for name,scale in (("broad",10),("adult",1),("rapid",0),("classical",0),("otb",0)):
            frame=adult if scale==1 else adult.with_columns(
                (pl.col("total")*scale).alias("total"),(pl.col("white_wins")*scale).alias("white_wins"))
            if not scale: frame=frame.clear()
            path=root/(name+".parquet"); frame.write_parquet(path);sources[name]=str(path)
        aux=root/"aux.parquet"
        pl.DataFrame([dict(position_hash=stored_key(board(),True),term_normal_total=5,
                          term_normal_white_wins=5,other_total=0)]).write_parquet(aux)
        inputs=[dict(path=str(p),sha256=file_sha(p)) for p in [*map(Path,sources.values()),aux]]
        return sources,aux,inputs

    def test_native_collection_cache_and_frozen_replay(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);sources,aux,inputs=self.prepare(root);k=keys();model=Model()
            provider=FrontierOverlay(colour="black",keys=k,counts=Counts(sources,aux),predict=model,
                         cache=root/"answers.sqlite",recipe=POLICY,input_artifacts=inputs,
                         pool=sources["adult"],aux=aux)
            with duckdb.connect(config={"threads":1}) as db:
                dynamic=collect_subgraph_edges_rs(db,sources["adult"],k.board(board()),False,
                         .00001,2,0.,1,reply_overlay=provider,keys=k)
            self.assertTrue(model.seen)
            self.assertTrue(all(chess.Board(f).turn for f in model.seen))
            initial=dynamic.filter(pl.col("parent_hash")==k.board(board()))
            self.assertEqual(len(initial),20)
            self.assertAlmostEqual(initial["reach_share"].sum(),.8)
            self.assertEqual(initial.filter(pl.col("move_san")=="e4")["total"].item(),10)
            self.assertEqual(initial.filter(pl.col("move_san")=="Nf3")["total"].item(),0)
            frozen=root/"frozen.parquet";provider.publish(frozen)
            n=provider.stats["predicted"];self.assertGreater(n,0);provider.close()
            second_model=Model()
            second=FrontierOverlay(colour="black",keys=k,counts=Counts(sources,aux),predict=second_model,
                        cache=root/"answers.sqlite",recipe=POLICY,input_artifacts=inputs,
                        pool=sources["adult"],aux=aux)
            with duckdb.connect(config={"threads":1}) as db:
                resumed=collect_subgraph_edges_rs(db,sources["adult"],k.board(board()),False,
                         .00001,2,0.,1,reply_overlay=second,keys=k)
            self.assertEqual(second_model.seen,[])
            self.assertEqual(second.counts.queries,0)
            self.assertTrue(dynamic.equals(resumed))
            second.close()
            overlay=ReplyOverlay(frozen,0,sources["adult"],aux,"black",1,keys=k)
            with duckdb.connect(config={"threads":1}) as db:
                replay=collect_subgraph_edges_rs(db,sources["adult"],k.board(board()),False,
                         .00001,2,0.,1,reply_overlay=overlay,keys=k)
            from polars.testing import assert_frame_equal
            assert_frame_equal(dynamic,replay,check_exact=True)
            # Changed source identity must not reuse an earlier recipe's probabilities.
            changed=list(inputs);changed[0]=dict(changed[0],sha256="0"*64)
            with self.assertRaisesRegex(ValueError,"artifact changed"):
                FrontierOverlay(colour="black",keys=k,counts=Counts(sources,aux),predict=Model(),
                    cache=root/"answers.sqlite",recipe=POLICY,input_artifacts=changed,pool=sources["adult"],aux=aux)

    def test_full_builder_freezes_selective_policy_for_both_colours(self):
        import sys
        from _test_pool128 import make_inputs,TWINS
        from _test_full_key_book import add_scores,run
        import build_budget_books as builder
        import pool_from_book as pool
        from sweep.builder import empty_source
        from sweep.stages import atomic_json
        import rare_verify
        with tempfile.TemporaryDirectory() as td,patch.dict(TWINS,{},clear=True):
            root=Path(td);_,lookup=make_inputs(root)
            sources,aux,inputs=self.prepare(root)
            adult=pl.read_parquet(sources["adult"])
            positions=adult["parent_hash"].to_list()+adult["child_hash"].to_list()
            add_scores(lookup,{(k["lo"],k["hi"]):0 for k in positions})
            # Complete production aux schema, including explicit zero counts.
            schema={**{n:{"int64":pl.Int64,"int32":pl.Int32,"double":pl.Float64}[t] for n,t in pool.AUX_SCHEMA},"position_hash":PAIR}
            counts={c:0 for c in schema if c!="position_hash"}
            rows=[dict(counts,position_hash=k) for k in adult["parent_hash"].unique()]
            pl.DataFrame(rows,schema=schema).write_parquet(aux)
            inputs=[dict(path=i["path"],sha256=file_sha(i["path"])) for i in inputs]
            atomic_json(sources["adult"]+".meta.json",dict(key_bits=128,min_games=1,
                        eval_lookup128=str(lookup),eval_lookup_sha256=file_sha(lookup/"lookup.json")))
            source=root/"source.parquet";empty_source(source,prior=.5195669627673518)
            checkpoint=root/"model";checkpoint.write_bytes(b"fixture-model")
            inputs += [dict(path=str(x),sha256=file_sha(x)) for x in (checkpoint,Path(sys.executable))]
            cfg=root/"policy.json";atomic_json(cfg,dict(schema=1,recipe=POLICY,sources=sources,
                aux=str(aux),cache=str(root/"answers.sqlite"),artifacts=inputs,
                model=dict(checkpoint=str(checkpoint),python=sys.executable)))
            def weak_fixture(con,xs,ys,**kw):
                # Make book decisions useful in this flow fixture; weak-stop correctness
                # is exercised separately, not inferred from these synthetic outcomes.
                pos=kw["positions"]
                early={pos.board(board(x)) for x in ("","e4","d4")}
                return {h:(.1 if h in early else .5) for h in xs},{h:.5 for h in ys},{}
            with patch("sweep.maia_policy.MaiaPolicy",side_effect=lambda *a,**kw:Model()), \
                    patch("sweep.maia_policy.runtime_identity",return_value=dict(fixture=True)), \
                    patch("weak_stop.weak_stop_values",new=weak_fixture):
                for colour,budget in (("black",2),("white",1)):
                    run(builder.main,"--rep",source,"--stats",sources["adult"],"--aux-stats",aux,
                        "--weak-pool",sources["adult"],"--weak-min-games",1,"--opponent-policy-config",cfg,
                        "--perspective",colour,"--budget",budget,"--max-ply",2,"--min-games",1,
                        "--max-score-loss",.06,"--robustness-floor",.05,"--slice-prior",.5195669627673518,
                        "--verification-policy","quality","--gated-reach","--out-prefix",root/colour,
                        "--tmp-dir",root/"spill")
                    book=root/f"{colour}_b{budget}.parquet"
                    meta=json.loads(Path(str(book)+".meta.json").read_text())
                    frozen=Path(meta["opponent_prior"]["file"])
                    self.assertTrue(frozen.exists())
                    self.assertEqual(meta["opponent_prior"]["sha256"],file_sha(frozen))
                    proof=json.loads(Path(str(frozen)+".json").read_text())
                    self.assertGreater(proof["materialization"]["requested"],0)
                    requests=rare_verify.request_plan(book,colour)
                    self.assertEqual(len(requests["requests"]),budget)
            import gc
            gc.collect()

    def test_explicit_checkpoint_temperature_protocol(self):
        from sweep.maia_policy import MaiaPolicy
        with tempfile.TemporaryDirectory() as td:
            ckpt=Path(td)/"model";ckpt.write_bytes(b"fixture")
            client=MaiaPolicy(dict(checkpoint=str(ckpt),python="unused"),POLICY)
            b=board();p={b.san(m):1. for m in b.legal_moves};p["e4"]=16.
            total=sum(p.values());p={k:v/total for k,v in p.items()}
            class Worker:
                stdin=io.StringIO()
                stdout=io.StringIO(json.dumps(dict(probs=[p]))+"\n")
            client.process=Worker()
            result=client([b])[0]
            ratio=result["e2e4"]/result["d2d4"]
            self.assertAlmostEqual(ratio,16**(1/1.35))
            req=json.loads(client.process.stdin.getvalue())
            self.assertEqual(req["self"],[2600]);self.assertEqual(req["oppo"],[2600])


class JointVerification(unittest.TestCase):
    def test_shared_requests_warm_resume_and_deep_preservation(self):
        from sweep.verify import verify
        from sweep.stages import file_sha
        b=board();h=stored_key(b,True)
        def menu(sans):
            return [dict(san=m,child=edge("",m,10)["child_hash"],total=10,
                         stored_cp={"e4":-65,"d4":-100,"Nc3":0}[m]) for m in sans]
        def plan(label,moves,limit=.06,prior=None,previous_nodes=0):
            return dict(book=label,book_sha256=label,colour="white",overlay_sha256=prior,
                 policy=dict(limit=limit,floor=.45,band=.01),
                 requests=[dict(key=h,epd=b.epd(),moves=moves,reason="fixture",done=[],
                                menu=menu(["e4","d4"]+([] if "Nc3" not in moves else ["Nc3"])),
                                previous_nodes=previous_nodes)])
        memory={}; searched=[]
        class Searches:
            def __init__(self,cache,nodes,threads,free=True,wide=True):
                self.nodes=nodes;self.contract={"engine":"fixture-engine"}
                self.stats=dict(restricted_searched=0,restricted_cache_hits=0,nodes=0)
            def get(self,h,epd,moves,count=True):
                k=(h["lo"],h["hi"],self.nodes)
                saved=memory.setdefault(k,dict(epd=epd,engine="fixture",date="fixed",
                    free=dict(san="Nf3",cp=0),moves={}))
                for m in sorted(set(moves)|{"Nf3"}):
                    if m not in saved["moves"]:
                        cp={"Nf3":0,"Nc3":0,"e4":-65 if self.nodes==5_000_000 else -80,"d4":-100}[m]
                        saved["moves"][m]=dict(cp=cp,nodes=self.nodes,depth=25)
                        self.stats["restricted_searched"]+=1;searched.append((k,m))
                    else: self.stats["restricted_cache_hits"]+=1
                return saved
            def close(self): pass
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);out=root/"pass.parquet"
            plans=[plan("strict",["e4"]),plan("loose",["d4"],.095)]
            # Two source arms share real counts/children but disagree on stored CP.
            plans[1]["requests"][0]["menu"][0]["stored_cp"]=-60
            info=verify(plans,cache=root/"cache",out=out,searches_factory=Searches)
            refs=json.loads(Path(str(out)+".requests.json").read_text())["groups"][0]["references"]
            self.assertEqual([r["stored"]["e4"] for r in refs],[-65,-60])
            self.assertEqual(info["requested_parents"],1)
            self.assertEqual(len(searched),6) # three comparisons once at each tier
            self.assertIn("Nf3",pl.read_parquet(out)["move_san"].to_list())
            self.assertEqual(info["parents"][0]["engine_nominations"],["Nf3"])
            self.assertEqual(set(pl.read_parquet(out)["verification_nodes"]),{20_000_000})
            again=root/"again.parquet"
            before=len(searched)
            verify(plans,cache=root/"cache",out=again,searches_factory=Searches)
            self.assertEqual(len(searched),before)
            from polars.testing import assert_frame_equal
            assert_frame_equal(pl.read_parquet(out),pl.read_parquet(again),check_exact=True)
            # All incoming rows at this parent are about to be replaced. A direct-child
            # verdict must still survive even if the new PV scores no longer disagree.
            prior_frame=pl.read_parquet(out).with_columns(pl.when(pl.col("move_san")=="e4")
                        .then(pl.lit(7)).otherwise(pl.col("transposition_cp_white")).alias("transposition_cp_white"))
            prior_frame.write_parquet(out)
            changed=[plan("new",["Nc3"],.095,file_sha(out))]
            next_out=root/"next.parquet"
            info=verify(changed,cache=root/"cache",out=next_out,prior=out,searches_factory=Searches)
            self.assertEqual(set(pl.read_parquet(next_out)["verification_nodes"]),{20_000_000})
            self.assertEqual(pl.read_parquet(next_out).filter(pl.col("move_san")=="e4")["transposition_cp_white"].item(),7)
            closed=[dict(changed[0],overlay_sha256=file_sha(next_out),requests=[])]
            self.assertTrue(verify(closed,cache=root/"cache",out=root/"closed.parquet",
                                 prior=next_out,searches_factory=Searches)["closed"])

    def test_missing_unselected_candidate_is_not_hidden_by_gate(self):
        from sweep.candidates import requests
        from eval_arrays import MISSING
        edges=pl.DataFrame([dict(parent_hash=1,parent_epd="fixture",child_hash=2,move_san="e4"),
                            dict(parent_hash=1,parent_epd="fixture",child_hash=3,move_san="d4")])
        scores={1:0,2:0}
        got=requests(edges,{1:1.},lambda ids:np.array([scores.get(h,MISSING) for h in ids],np.int16),
                     white=True,floor=.45,limit=.06)
        self.assertEqual(got[0]["moves"],["d4"])
        self.assertIn("missing",got[0]["reason"])

if __name__=="__main__": unittest.main()

