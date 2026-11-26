"""Regression cases from the November 25 implementation review, with native replay."""
import copy
import json
import math
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import chess
import numpy as np
import polars as pl
import book_builder_rs as rs
from consumer_keys import ConsumerKeys,stored_key
from sweep.stages import atomic_json,file_sha
from sweep.model_contract import validate,calibration_grid
from sweep.human_providers import OwnProvider,SharedPolicies
from sweep.continuation import values
from sweep import policy
from _test_sweep_execution import keys,board,edge,Model


def recipe(**kw):
    return dict(schema="human-continuation-v1",utility="lichess-source-clip2000-v1",
        nonmove="conditional-board-v1",candidate_cap=30,count_cap=50,source_cap=51,
        evaluation_horizon=50,beyond_source="model-only-explicit",status="engineering",
        own=calibration_grid()[0],max_states=100000,batch_roots=8,min_expand=0.,**kw)


class Fixed:
    """All legal moves named, one chosen. No hidden renormalization in consumers."""
    def __init__(self,move=None):self.move=move;self.seen=[];self.stats={}
    def distributions(self,boards):
        self.seen.extend(b.fen() for b in boards)
        out=[]
        for b in boards:
            chosen=b.parse_san(self.move) if self.move and self.move in [b.san(m) for m in b.legal_moves] else next(iter(b.legal_moves))
            out.append({m.uci():float(m==chosen) for m in b.legal_moves})
        return out


class NativePolicies(unittest.TestCase):
    def test_formula_randomized_parity_and_support_boundary(self):
        rng=np.random.default_rng(13)
        for gate in (None,300.):
            for _ in range(80):
                r=rng.integers(0,10,30);a=r+rng.integers(0,40,30);b=a+rng.integers(0,100,30)
                o=rng.integers(0,10,30);q=rng.dirichlet(np.ones(30))
                args=[list(range(30)),b.tolist(),a.tolist(),r.tolist(),o.tolist(),q.tolist()]
                np.testing.assert_allclose(policy.ensemble(*args,maia_support_gate=gate),
                    policy.ensemble_reference(*args,maia_support_gate=gate),atol=1e-15,rtol=0)
        for n in (0,1,19,20,21):
            self.assertAlmostEqual(rs.own_policy([n,0],[.8,.2],30)[0],(n+24)/(n+30))
        with self.assertRaises(ValueError):rs.own_policy([19,0],[.8,.1],30)

    def test_no_probability_loss(self):
        self.assertEqual(rs.bounded_expectation([.991,.009],[1.,0.],[1.,0.]),(.991,.991))
        self.assertEqual(rs.bounded_expectation([.5,.5],[.8,0.],[.8,1.]),(.4,.9))
        self.assertEqual(len(calibration_grid()),36)
        validate(recipe())
        with self.assertRaises(ValueError):validate(recipe()|dict(status="production"))
        with self.assertRaises(ValueError):validate(recipe()|dict(count_cap=29))

    def test_own_provider_keeps_19_observations_and_reuses_content_cache(self):
        class Counts:
            def fetch(self,boards):return [[edge("","e4",19)] for b in boards]
            def close(self):pass
        with tempfile.TemporaryDirectory() as td:
            model=Model();r=recipe()["own"]|dict(strength=30.)
            own=OwnProvider(counts=Counts(),predict=model,cache=Path(td)/"p.sqlite",recipe=r,identity="fixture")
            p=own.distributions([board()])[0]
            self.assertAlmostEqual(p["e2e4"],(19+30/20)/49)
            self.assertGreater(p["d2d4"],0)
            self.assertEqual(own.distributions([board()])[0],p)
            self.assertEqual(len(model.seen),1);own.close()
            model=Model();own=OwnProvider(counts=Counts(),predict=model,cache=Path(td)/"p.sqlite",recipe=r,identity="fixture")
            self.assertEqual(own.distributions([board()])[0],p);self.assertEqual(model.seen,[]);own.close()


class Continuation(unittest.TestCase):
    def test_builder_uses_real_shared_continuation_without_legacy_fallback(self):
        self._builder_fixture(False)

    def test_builder_consumes_complete_compact_broad_and_weak_pools(self):
        self._builder_fixture(True)

    def test_builder_consumes_partitioned_adult_and_compact_count_pools(self):
        self._builder_fixture(True,partitioned=True)

    def _builder_fixture(self,compact,partitioned=False):
        import sys
        import io
        from contextlib import redirect_stdout
        from _test_sweep_execution import SelectivePolicy
        from _test_pool128 import make_inputs,TWINS
        from _test_full_key_book import add_scores,run
        from consumer_keys import PAIR
        from sweep.builder import empty_source
        from sweep.spec import POLICY
        import pool_from_book as pool
        import build_budget_books as builder
        with tempfile.TemporaryDirectory() as td,patch.dict(TWINS,{},clear=True):
            root=Path(td);_,lookup=make_inputs(root)
            sources,aux,inputs=SelectivePolicy.prepare(None,root)
            otb=root/"training-p2.parquet"
            pl.read_parquet(sources["otb"]).write_parquet(otb)
            sources["otb"]=str(otb)
            inputs=[dict(path=str(p),sha256=file_sha(p)) for p in [*map(Path,sources.values()),aux]]
            adult=pl.read_parquet(sources["adult"])
            schema={**{n:{"int64":pl.Int64,"int32":pl.Int32,"double":pl.Float64}[t] for n,t in pool.AUX_SCHEMA},"position_hash":PAIR}
            zero={c:0 for c in schema if c!="position_hash"}
            pl.DataFrame([dict(zero,position_hash=k) for k in adult["parent_hash"].unique()],schema=schema).write_parquet(aux)
            inputs=[dict(path=i["path"],sha256=file_sha(i["path"])) for i in inputs]
            atomic_json(sources["adult"]+".meta.json",dict(key_bits=128,min_games=1,
                max_ply=2,eval_lookup128=str(lookup),eval_lookup_sha256=file_sha(lookup/"lookup.json")))
            for role in ("broad","rapid","classical"):
                atomic_json(sources[role]+".meta.json",dict(key_bits=128,min_games=1,max_ply=2))
            inputs += [dict(path=sources[role]+".meta.json",sha256=file_sha(sources[role]+".meta.json"))
                       for role in ("broad","adult","rapid","classical")]
            source=root/"source.parquet";empty_source(source,prior=.5195669627673518)
            checkpoint=root/"model";checkpoint.write_bytes(b"fixture-model")
            inputs += [dict(path=str(p),sha256=file_sha(p)) for p in (checkpoint,Path(sys.executable))]
            cfg=root/"policy.json"
            atomic_json(cfg,dict(schema=1,recipe=POLICY,sources=sources,aux=str(aux),cache=str(root/"op.sqlite"),
                artifacts=inputs,model=dict(checkpoint=str(checkpoint),python=sys.executable),nonmove_semantics="conditional-board-v1"))
            weak=root/"weak.parquet";adult.write_parquet(weak)
            weakmeta=Path(str(weak)+".meta.json")
            atomic_json(weakmeta,dict(key_bits=128,min_games=1,max_ply=2,
                population=dict(elo_bands=[1000,1200,1400,1600],events=["Blitz","Rapid","Classical"])))
            if compact:
                from _test_sweep_count_pool import compact_fixture
                weak=compact_fixture(root/"weak-full",adult,json.loads(weakmeta.read_text()))
                weakmeta=Path(str(weak)+".meta.json")
                broad=compact_fixture(root/"broad-full",pl.read_parquet(sources["broad"]),
                    json.loads(Path(sources["broad"]+".meta.json").read_text()))
                sources["broad"]=str(broad)
                inputs += [dict(path=str(p),sha256=file_sha(p)) for p in (broad,Path(str(broad)+".meta.json"))]
                atomic_json(cfg,dict(schema=1,recipe=POLICY,sources=sources,aux=str(aux),cache=str(root/"op.sqlite"),
                    artifacts=inputs,model=dict(checkpoint=str(checkpoint),python=sys.executable),nonmove_semantics="conditional-board-v1"))
            if partitioned:
                from _test_partitioned_inputs import published_fixture
                stats,aux,_,_=published_fixture(root/"adult-full",adult.to_dicts(),
                    json.loads(Path(sources["adult"]+".meta.json").read_text()))
                sources["adult"]=str(stats)
                inputs += [dict(path=str(p),sha256=file_sha(p)) for p in (stats,aux,Path(str(stats)+".meta.json"))]
                atomic_json(cfg,dict(schema=1,recipe=POLICY,sources=sources,aux=str(aux),cache=str(root/"op.sqlite"),
                    artifacts=inputs,model=dict(checkpoint=str(checkpoint),python=sys.executable),nonmove_semantics="conditional-board-v1"))
            owncfg=root/"own.json";contract=recipe()|dict(candidate_cap=2,count_cap=2,max_states=500000)
            atomic_json(owncfg,dict(recipe=contract,weak=str(weak),cache=str(root/"own.sqlite"),
                model=dict(checkpoint=str(checkpoint),python=sys.executable),artifacts=inputs+
                [dict(path=str(p),sha256=file_sha(p)) for p in (weak,weakmeta)]))
            binding=root/"eval.json"
            atomic_json(binding,dict(schema="eval-binding-v1",key_bits=128,lookup=str(lookup),lookup_sha256=file_sha(lookup/"lookup.json")))
            with patch("sweep.maia_policy.MaiaPolicy",side_effect=lambda *a,**kw:Model()), \
                 patch("sweep.maia_policy.runtime_identity",return_value=dict(fixture=True)), \
                 patch("weak_stop.weak_stop_values",side_effect=AssertionError("legacy continuation called")), \
                 patch.object(ConsumerKeys,"scores",lambda self,ids:np.full(len(ids),12.75)),redirect_stdout(io.StringIO()):
                run(builder.main,"--rep",source,"--stats",sources["adult"],"--aux-stats",aux,
                    "--weak-pool",weak,"--opponent-policy-config",cfg,"--continuation-config",owncfg,
                    "--eval-binding",binding,"--perspective","black","--budget",2,"--max-ply",2,
                    "--min-games",1,"--max-score-loss",.06,"--robustness-floor",.05,
                    "--slice-prior",.5195669627673518,"--verification-policy","quality","--gated-reach",
                    "--out-prefix",root/"new","--tmp-dir",root/"spill")
            meta=json.loads((root/"new_b2.parquet.meta.json").read_text())
            self.assertEqual(meta["continuation_contract"],contract)
            report=json.loads((root/"new.continuation.json").read_text())
            self.assertEqual(report["unresolved_roots"],0)
            self.assertGreater(report["own"]["predicted"],0)
            self.assertEqual(meta["evaluation_binding"]["binding_sha256"],file_sha(binding))
            frozen=pl.read_parquet(root/"new.opponent.parquet")
            self.assertGreater(frozen["parent_hash"].n_unique(),3) # graph plus off-graph W/R queries

    def policies(self,white):
        k=keys();own=Fixed("e4" if white else "e5");op=Fixed("e5" if white else "e4")
        op.colour="white" if white else "black";op.conditional=op.distributions
        op.origins={};op.board=lambda h:chess.Board(op.origins[h])
        return SharedPolicies(own,op,k),k

    def test_both_colours_same_opponent_in_w_r_and_native_referee(self):
        import convertibility_eval as cv
        from sweep.referee import play
        for white in (True,False):
            p,k=self.policies(white)
            x=board("" if white else "e4");y=board("e4" if white else "")
            for b in (x,y):p.opponent.origins[k.board(b)]=b.fen()
            lookup=lambda ids:np.full(len(ids),12.75)
            W,R,report=values([k.board(x)],[k.board(y)],policies=p,keys=k,lookup=lookup,recipe=recipe())
            want=rs.continuation_utility([12.75],white)[0]
            self.assertAlmostEqual(W[k.board(x)],want);self.assertAlmostEqual(R[k.board(y)],want)
            self.assertEqual(report["unresolved_roots"],0)
            a=SimpleNamespace(rollouts=2,max_ply=4,position_keys=k,our_elo=1450,opp_elo=2200,recall=1.)
            starts=[(k.board(board()),board().epd(),1.,0,12.75,0.,1.)]
            def fail(*a):raise AssertionError("legacy count/Maia path called")
            with patch.object(cv,"blend",fail):
                result=play(starts,{},white=white,args=a,seed=1,policies=p,lookup=lookup)
            self.assertAlmostEqual(result["lower"],want)
            self.assertEqual(result["mass"],1.)
            self.assertEqual(result["leaves"][0]["ancestor_cp"],12.75)
            self.assertGreater(len(p.opponent.seen),2)

    def test_missing_eval_and_expansion_cap_have_bounds_not_neutral_values(self):
        from sweep.referee import play
        p,k=self.policies(True);b=board();h=k.board(b);p.opponent.origins[h]=b.fen()
        missing=lambda ids:np.full(len(ids),-32768.)
        W,R,report=values([h],[],policies=p,keys=k,lookup=missing,recipe=recipe())
        self.assertEqual(W,{})
        self.assertEqual(report["bounds"][0]["upper"],1.)
        self.assertEqual(report["bounds"][0]["lower"],0.)
        a=SimpleNamespace(rollouts=1,max_ply=1,position_keys=k,our_elo=1450,opp_elo=2200)
        result=play([(h,b.epd(),1.,0,None,0.,1.)],{},white=True,args=a,seed=1,policies=p,lookup=missing)
        self.assertEqual((result["lower"],result["upper"]),(0.,1.))
        limited=rs.RsContinuation([b.epd()],[2],True,k.native,1,0.)
        limited.expand(h,p.native_batch(True,[h],[b.epd()])[0])
        self.assertEqual(limited.results(),[(h,0.,1.)])

    def test_actual_mate_and_data_horizon_are_different(self):
        b=board("f3 e5 g4 Qh4#");k=keys()
        c=rs.RsContinuation([b.epd()],[3],False,k.native,10,0.)
        self.assertEqual(c.needed(),[]);self.assertEqual(c.results()[0][1:],(1.,1.))
        b=board();c=rs.RsContinuation([b.epd()],[0],True,k.native,10,0.)
        self.assertEqual(c.results()[0][1:],(0.,1.))

    def test_live_frontier_conditional_drops_no_legal_reply_at_censored_board(self):
        from _test_sweep_execution import SelectivePolicy
        from sweep.frontier import FrontierOverlay,Counts
        from sweep.spec import POLICY
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);sources,aux,inputs=SelectivePolicy.prepare(None,root);k=keys()
            f=pl.read_parquet(aux).with_columns(pl.lit(1000).alias("horizon_total"));f.write_parquet(aux)
            inputs=[dict(path=i["path"],sha256=file_sha(i["path"])) for i in inputs]
            prior=FrontierOverlay(colour="black",keys=k,counts=Counts(sources,aux),predict=Model(),
                cache=root/"cache.sqlite",recipe=POLICY,input_artifacts=inputs,pool=sources["adult"],aux=aux,
                semantics="conditional-board-v1")
            p=prior.conditional([board()])[0]
            self.assertAlmostEqual(sum(p.values()),1.)
            rows=prior.rows([k.board(board())]);self.assertEqual(rows["nonmove"][0],0.)
            self.assertEqual(rows["observed_censored"][0],1000)
            self.assertEqual(prior.conditional([board(),board()]),[p,p])
            self.assertEqual(prior.extend_aux(f,[])["horizon_total"][0],0)
            self.assertEqual(pl.read_parquet(aux)["horizon_total"][0],1000)
            prior.close()

    def test_deeper_policy_counts_do_not_replace_candidate_outcome_counts(self):
        from _test_sweep_execution import SelectivePolicy
        from sweep.frontier import FrontierOverlay,Counts
        from sweep.spec import POLICY
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);sources,aux,inputs=SelectivePolicy.prepare(None,root);k=keys()
            deeper=root/"adult50.parquet"
            frame=pl.read_parquet(sources["adult"])
            frame.with_columns((pl.col("total")*2).alias("total"),(pl.col("white_wins")*2).alias("white_wins")).write_parquet(deeper)
            counts=Counts(dict(sources,adult=str(deeper)),aux,outcome_pool=sources["adult"])
            inputs.append(dict(path=str(deeper),sha256=file_sha(deeper)))
            prior=FrontierOverlay(colour="black",keys=k,counts=counts,predict=Model(),cache=root/"p.sqlite",
                recipe=POLICY,input_artifacts=inputs,pool=sources["adult"],aux=aux,semantics="conditional-board-v1")
            rows=prior.rows([k.board(board())])
            self.assertEqual(rows["support_adult"][0],40)
            self.assertEqual(rows["total"].sum(),20)
            self.assertEqual(rows["move_mass"][0],20)
            prior.close()


class Bindings(unittest.TestCase):
    def test_release_requires_content_bound_model_evidence(self):
        from sweep.reports import pilot_acceptance
        r=dict(full_keys=True,candidate_menu_complete=True,mass_conserved=True,gate_audit=True,
            lookahead_audit=True,quality_closed=True,evidence_monotone=True,clean_rebuild_parity=True,
            distinct_prescriptions=2000,receipt_sha256="fixture")
        self.assertFalse(pilot_acceptance(r)["ready_for_matrix"])
        with tempfile.TemporaryDirectory() as td:
            r.update(schema="pilot-acceptance-v2",model_contract_sha256="a"*64,model_evidence={})
            names=("policy_continuity","own_calibration","uncertainty_accounting","horizon_semantics",
                   "evaluation_binding","native_referee","effective_objective","input_cost")
            for name in names:
                p=Path(td)/(name+".json")
                atomic_json(p,dict(schema="model-check-v1",check=name,passed=True,model_contract_sha256="a"*64))
                r["model_evidence"][name]=dict(path=str(p),sha256=file_sha(p))
            self.assertTrue(pilot_acceptance(r)["ready_for_matrix"])
            Path(r["model_evidence"][names[0]]["path"]).write_text("changed")
            self.assertIn(names[0],pilot_acceptance(r)["missing"])

    def test_explicit_provider_does_not_rewrite_count_metadata(self):
        from _test_pool128 import make_inputs
        from _test_full_key_book import add_scores
        from eval_binding import read_binding
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);_,lookup=make_inputs(root)
            add_scores(lookup,{(42,100):17})
            path=root/"counts.parquet"
            pl.DataFrame([dict(parent_hash=dict(lo=42,hi=100))]).write_parquet(path)
            meta=Path(str(path)+".meta.json")
            atomic_json(meta,dict(key_bits=128,eval_lookup128="absent-historical",eval_lookup_sha256="old"))
            before=meta.read_bytes()
            binding=root/"binding.json"
            atomic_json(binding,dict(schema="eval-binding-v1",key_bits=128,lookup=str(lookup),lookup_sha256=file_sha(lookup/"lookup.json")))
            k=ConsumerKeys.for_pool(path,eval_binding=binding)
            self.assertEqual(k.scores(np.array([k.native.encode_one(42,100)],np.int64))[0],17)
            self.assertEqual(meta.read_bytes(),before)
            k.evals.mapped.clear()
            changed=json.loads((lookup/"lookup.json").read_text());changed["changed"]=True
            atomic_json(lookup/"lookup.json",changed)
            with self.assertRaisesRegex(ValueError,"manifest changed"):read_binding(binding)

    def test_paired_scenario_cannot_change_counts_or_solver(self):
        from sweep.spec import manifest
        from sweep.scenarios import source_arm,check_pair
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);m=manifest();pilot=next(a for a in m["primary"] if a["id"]==m["pilot"])
            bindings=[]
            for i in (0,1):
                lookup=root/str(i);lookup.mkdir();atomic_json(lookup/"lookup.json",dict(id=i))
                path=root/f"binding{i}.json"
                atomic_json(path,dict(schema="eval-binding-v1",key_bits=128,lookup=str(lookup),lookup_sha256=file_sha(lookup/"lookup.json")))
                bindings.append(path)
            config=dict(arms=[dict(arm=source_arm(pilot,p),inputs=dict(stats="same",eval_binding=str(p))) for p in bindings],
                        referee_eval_binding=str(bindings[0]))
            check_pair(config,pilot)
            bad=copy.deepcopy(config);bad["arms"][1]["inputs"]["stats"]="different"
            with self.assertRaisesRegex(ValueError,"share all count"):check_pair(bad,pilot)
            bad=copy.deepcopy(config);bad["arms"][1]["arm"]["recipe"]["max_score_loss"]=.9
            with self.assertRaisesRegex(ValueError,"changes the approved pilot"):check_pair(bad,pilot)

    def test_shared_counts_with_distinct_stored_scores_are_preserved(self):
        from sweep.verify import combine
        b=board();ch=b.copy();ch.push_san("e4")
        item=dict(san="e4",child=stored_key(ch,True),total=28,stored_cp=40)
        req=dict(key=stored_key(b,True),epd=b.epd(),menu=[item],moves=["e4"],done=[],previous_nodes=0,reason="fixture")
        plan=dict(policy=dict(limit=.06,floor=.4,band=.01),colour="white",requests=[req],book_sha256="a")
        other=copy.deepcopy(plan);other["book_sha256"]="b";other["requests"][0]["menu"][0]["stored_cp"]=60
        group=combine([plan,other])[0]
        self.assertEqual([r["stored"]["e4"] for r in group["references"]],[40,60])
        other["requests"][0]["menu"][0]["total"]=29
        with self.assertRaisesRegex(ValueError,"different candidate counts"):combine([plan,other])

if __name__=="__main__":unittest.main()
