"""Integration regressions for sweep preparation. No production inputs or engine jobs."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import chess
import polars as pl

from sweep import spec, stages, policy, quality, extensions, reports, rollout, recall
from opponent_prior import ReplyOverlay
import rare_verify as rv

class Specification(unittest.TestCase):
    def test_matrix_and_fixed_baselines(self):
        m=spec.manifest()
        self.assertEqual(len(m["primary"]),20)
        self.assertEqual(len(m["sensitivities"]),20)
        self.assertEqual(sum(a["recipe"]["mode"]=="independent" for a in m["primary"]),12)
        for color in ("white","black"):
            for gate in spec.GATES:
                arms=[a for a in m["primary"] if a["recipe"]["colour"]==color and a["recipe"]["gate"]==gate]
                self.assertEqual(len(arms),5)
                r={a["recipe"]["budget"]:a for a in arms if a["recipe"]["mode"]=="reduced"}
                self.assertEqual(r[500]["depends_on"],[r[1000]["id"]])
        self.assertEqual(spec.evaluation_horizon(41),52)
        self.assertEqual(spec.evaluation_horizon(68),78)
        m["primary"][0]["recipe"]["slice_prior"]+=.01
        with self.assertRaises(ValueError): spec.validate(m)

    def test_horizon_sensitivity_uses_matching_2000_position_control(self):
        m=spec.manifest()
        primary={a["id"]:a for a in m["primary"]}
        horizons=[a for a in m["sensitivities"] if a["family"]=="horizon"]
        self.assertEqual(len(horizons),4)
        self.assertEqual({(a["recipe"]["colour"],a["recipe"]["gate"]) for a in horizons},
                         {(c,g) for c in ("white","black") for g in spec.GATES})
        for a in horizons:
            with self.subTest(colour=a["recipe"]["colour"],gate=a["recipe"]["gate"]):
                self.assertEqual(a["recipe"]["budget"],2000)
                self.assertEqual(a["recipe"]["mode"],"independent")
                self.assertEqual(len(a["depends_on"]),1)
                control=primary[a["depends_on"][0]]["recipe"]
                expected=dict(control,max_ply=50,candidate_pool="brc1800-p50")
                self.assertEqual(a["recipe"],expected)
                self.assertEqual(control["max_ply"],30)
        self.assertTrue(all(a["recipe"]["budget"]==1000 for a in m["sensitivities"]
                            if a["family"] in ("weight","policy")))

    def test_horizon_validator_rejects_wrong_budget_and_control(self):
        # Recompute the ID so a generic recipe-hash check cannot mask the defect.
        for defect in ("budget","control","colour","gate","weight","missing","duplicate"):
            with self.subTest(defect=defect):
                m=spec.manifest()
                a=next(a for a in m["sensitivities"] if a["family"]=="horizon")
                r=a["recipe"]
                if defect=="budget": r["budget"]=1000
                elif defect=="control":
                    a["depends_on"]=[next(p["id"] for p in m["primary"] if
                        p["recipe"]["colour"]==r["colour"] and p["recipe"]["gate"]==r["gate"]
                        and p["recipe"]["budget"]==1000 and p["recipe"]["mode"]=="independent")]
                elif defect in ("colour","gate"):
                    a["depends_on"]=[next(p["id"] for p in m["primary"] if
                        p["recipe"][defect]!=r[defect] and p["recipe"]["budget"]==2000
                        and p["recipe"]["mode"]=="independent")]
                elif defect=="weight": r["eval_weight"]=.5
                elif defect=="missing": m["sensitivities"].remove(a)
                else: m["sensitivities"].append(dict(a))
                a["id"]=spec.digest(r)[:20]
                with self.assertRaises(ValueError):spec.validate(m)

    def test_extension_default_and_paired_queue(self):
        m=spec.manifest();q=m["extension_comparison"]
        self.assertEqual(m["extensions"]["metric"],"remaining-maximum")
        # 24 metric runs (both metrics) + 12 player-severity runs at the default metric (2026-11-21)
        self.assertEqual(len(q["runs"]),36)
        self.assertEqual(len({r["id"] for r in q["runs"]}),36)
        anchors={a["id"] for a in m["primary"] if a["recipe"]["mode"]=="independent" and a["recipe"]["budget"]==2000}
        self.assertEqual(len(anchors),4)
        for anchor in anchors:
            for threshold in (1e-5,3e-5,1e-4):
                group=[r for r in q["runs"] if r["recipe"]["base_arm"]==anchor and r["recipe"]["extensions"]["threshold"]==threshold]
                pair=[r for r in group if r["role"]!="player-severity"]
                self.assertEqual({r["recipe"]["extensions"]["metric"] for r in pair},{"remaining-maximum","covered-maximum"})
                self.assertTrue(all(r["depends_on"]==[anchor] for r in group))
                (player,)=[r for r in group if r["role"]=="player-severity"]
                control=next(r for r in pair if r["role"]=="default-calibration")
                ext=player["recipe"]["extensions"]
                self.assertEqual(ext["metric"],"remaining-maximum")
                self.assertEqual(ext["severity"]["combine"],"max(engine-severity,player-loss)")
                # the player run differs from its default control by the severity setting alone
                self.assertEqual({k:v for k,v in ext.items() if k!="severity"},
                                 {k:v for k,v in control["recipe"]["extensions"].items() if k!="severity"})
                self.assertEqual(player["cli_args"][-2:],["--severity","engine-or-player"])
        q["runs"][0]["cli_args"][1]="covered-maximum"
        with self.assertRaisesRegex(ValueError,"comparison queue drifted"):spec.validate(m)

    def test_stage_resume_failure_and_tampering(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            source=root/"producer.py"
            source.write_text("from pathlib import Path\nPath('out').write_text('correct')\n",encoding="utf8")
            validator=root/"validate.py"
            validator.write_text("from pathlib import Path\nassert Path('out').read_text()=='correct'\n",encoding="utf8")
            s=dict(id="fixture",inputs=[dict(path=str(p),sha256=stages.file_sha(p)) for p in (source,validator)],
                   command=[sys.executable,str(source)],validate=[sys.executable,str(validator)],
                   cwd=str(root),outputs=[str(root/"out")])
            self.assertEqual(stages.stage(s,root)["state"],"ready")
            self.assertFalse((root/"out").exists())
            self.assertEqual(stages.stage(s,root,execute=True)["state"],"complete")
            self.assertEqual(stages.stage(s,root,execute=True)["state"],"reused")
            (root/"out").write_text("wrong")
            with self.assertRaises(ValueError): stages.stage(s,root,execute=True)
            s["id"]="failed"
            s["command"]=[sys.executable,"-c","raise SystemExit(2)"]
            with self.assertRaises(Exception): stages.stage(s,root,execute=True)
            self.assertFalse((root/"failed/receipt.json").exists())

class Ensemble(unittest.TestCase):
    def test_formula_support_and_overlap(self):
        q=[.8,.2]
        self.assertEqual(policy.ensemble(["a","b"],[0,0],[0,0],[0,0],[0,0],q),q)
        p=policy.ensemble(["a","b"],[100,0],[10,0],[1,0],[0,30],q)
        self.assertAlmostEqual(sum(p),1.)
        self.assertGreater(p[1],.75)
        with self.assertRaisesRegex(ValueError,"subtraction"):
            policy.ensemble(["a","b"],[10,0],[1,1],[0,0],[0,0],q)

    def test_cache_and_separate_nonmove(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            board=chess.Board()
            legal=sorted(m.uci() for m in board.legal_moves)
            q=[1/len(legal)]*len(legal)
            cache=policy.PolicyCache(root/"cache.sqlite",spec.POLICY)
            calls=[]
            def compute(): calls.append(1); return q
            cache.get_or_compute(board.fen(),legal,{"snapshot":"one"},compute)
            _,hit=cache.get_or_compute(board.fen(),legal,{"snapshot":"one"},compute)
            self.assertTrue(hit)
            cache.get_or_compute(board.fen(),legal,{"snapshot":"two"},compute)
            self.assertEqual(len(calls),2)
            cache.close()
            rows=policy.overlay_rows(board,dict(zip(legal,q)),empirical={"e2e4":(4,0,2)},nonmove=2,wide=False)
            file=root/"prior.parquet"; rows.write_parquet(file)
            pool,aux=root/"pool",root/"aux"
            pool.write_text("fixture"); aux.write_text("fixture")
            meta=dict(schema=3,colour="black",source_pool=str(pool),source_aux=str(aux),
                      min_games=1,scope="legal-fixture",sha256=stages.file_sha(file),
                      policy_recipe=spec.POLICY,input_artifacts=[
                          dict(path=str(p),sha256=stages.file_sha(p)) for p in (pool,aux)])
            stages.atomic_json(str(file)+".json",meta)
            overlay=ReplyOverlay(file,0,pool,aux,"black",1)
            self.assertAlmostEqual(overlay.rows["reply_mass"].sum(),6)
            self.assertAlmostEqual(overlay.rows["reach_share"].sum(),.75)
            self.assertEqual(overlay.rows.height,20)
            self.assertEqual(overlay.rows.filter(pl.col("move_san")=="e4")["total"].item(),6)
            with self.assertRaisesRegex(ValueError,"pseudogames"):
                ReplyOverlay(file,30,pool,aux,"black",1)
            pool.write_text("changed")
            with self.assertRaisesRegex(ValueError,"artifact changed"):
                ReplyOverlay(file,0,pool,aux,"black",1)

class Quality(unittest.TestCase):
    def test_depth_and_count_independence(self):
        book=pl.DataFrame(dict(position_hash=[1],position_epd=["fixture"],side_to_move=["white"],best_move=["e4"]))
        meta=dict(rare_floor=0,verification_policy="quality",max_score_loss=.06,
                  slice_prior=.5183137990703003,robustness_floor=.05,verification_score_band=.01)
        for count in (1,10000):
            edges={1:[dict(move_san="e4",child_hash=2,total=count)]}
            want=rv.plan(book,meta,None,"white",{1:0,2:0},edges,[])
            self.assertIn("independent-final",want[1][0])
            verified=pl.DataFrame([dict(position_hash=1,move_san="e4",verification_nodes=20_000_000,
                        verified=True,parent_cp_white=0,child_cp_white=0)])
            # Verified move parser identifies restricted evidence by verification_nodes.
            self.assertFalse(rv.plan(book,meta,verified,"white",{1:0,2:0},edges,[]))
        e=quality.Evidence(-80,5_000_000,True)
        r=quality.review(0,e,absolute_floor=.45,relative_limit=quality.score(0)-quality.score(-80))
        self.assertEqual(r["required_nodes"],20_000_000)
        self.assertIn("score-boundary",r["reasons"])
        old=dict(engine_contract="A",key=[1,2],move="e2e4",nodes=20_000_000)
        self.assertIs(quality.prefer(old,dict(old,nodes=5_000_000)),old)

    def test_union_comparison_menus_and_full_keys(self):
        one=dict(key=[7,1],epd="one",engine_contract="same",nodes=5_000_000,
                 moves=["e2e4"],arms=["strict"],reasons=["cloud"])
        two=dict(one,nodes=20_000_000,moves=["d2d4"],arms=["loose"])
        twin=dict(one,key=[7,2],epd="two")
        result=quality.coalesce([one,two,twin])
        self.assertEqual(len(result),2)
        self.assertEqual(result[0]["nodes"],20_000_000)
        self.assertEqual(result[0]["moves"],["d2d4","e2e4"])


class ExtensionsAndReports(unittest.TestCase):
    def test_long_connector_package_and_shared_risk(self):
        keys=tuple((7,i) for i in range(9))
        risks={k:0. for k in keys}; risks[keys[-1]]=.20
        routes=[extensions.Route(.001,keys)]
        value=extensions.package_value(routes,risks,keys)
        self.assertEqual(value["new_positions"],9)  # no four-move cap
        self.assertAlmostEqual(value["density"],.0002/9)
        self.assertEqual(extensions.exposure(routes,risks,[keys[-1]]),0.) # connector needed
        risks[keys[-2]]=.15
        self.assertAlmostEqual(extensions.exposure(routes,risks,keys),.0002) # no double counting
        transposed=[extensions.Route(.4,keys),extensions.Route(.6,keys)]
        self.assertEqual(extensions.package_value(transposed,risks,keys)["new_positions"],9)
        with self.assertRaisesRegex(ValueError,"unresolved"):
            extensions.require_closed_frontier([extensions.Route(.1,keys,False)],risks,.00003)
        self.assertEqual(extensions.severity([.5],1)["severity"],0)
        with self.assertRaises(ValueError): extensions.severity([.5],3)

    def test_ladder_and_stability_denominators(self):
        a,b=(1,2),(1,3)
        self.assertEqual(reports.check_ladder({a:"e4"},{a:"e4",b:"d4"},{a:"e4",b:"d4"})["conflicts"],0)
        with self.assertRaises(ValueError): reports.check_ladder({a:"e4"},{a:"d4"},{a:"d4"})
        inherited,cost=reports.inherit_extensions({a:"e4"},{b:"d4"},{a:"e4",b:"d4"})
        self.assertEqual(cost["inherited_additional"],1)
        choice=dict(position_hash={"lo":1,"hi":2},move="e4",selected=True,eligible=True,reachable=True,reach=.4)
        books=[dict(id="a",family="primary",mode="independent",colour="white",choices=[choice]),
               dict(id="reduced",family="primary",mode="reduced",colour="white",choices=[choice]),
               dict(id="b",family="primary",mode="independent",colour="white",
                    choices=[dict(choice,selected=False,eligible=False)])]
        row=reports.stability(books)[0]
        self.assertEqual((row["selected"],row["eligible"],row["reachable"]),(1,1,1))
        self.assertEqual(row["selection_rate"],1)
        self.assertFalse(reports.pilot_acceptance({})["ready_for_matrix"])
        with self.assertRaisesRegex(ValueError,"unresolved"):
            reports.evaluation_contract([],model={},policy={},seeds=[1],panel_receipt="fixture")


class Rollout(unittest.TestCase):
    def test_paired_histories_recall_and_exact_terminal(self):
        root=rollout.zobrist128(chess.Board())
        arms=[dict(id="a",colour="white",moves={root:"f2f3"}),
              dict(id="b",colour="white",moves={root:"f2f3"})]
        def predict(reqs):
            answer=[]
            for r in reqs:
                self.assertEqual(len(r["history"]),r["ply"])
                self.assertEqual(len(r["position_hash"]),2)
                san=["f3","e5","g4","Qh4#"][r["ply"]]
                b=chess.Board(r["fen"]); uci=b.parse_san(san).uci()
                answer.append({m:float(m==uci) for m in r["legal"]})
            return answer
        def leaves(reqs): raise AssertionError("a mate must never be engine-valued")
        rows=rollout.play(arms,games=3,seed=7,horizon=70,predict=predict,
                          recall=lambda r:1.,value_leaves=leaves)
        self.assertEqual(len(rows),6)
        self.assertTrue(all(r["score"]==0 and r["ply"]==4 and r["recalled"]==1 for r in rows))
        rows=rollout.play(arms,games=3,seed=7,horizon=2,predict=predict,
                          recall=lambda r:0.,value_leaves=lambda reqs:[.4]*len(reqs))
        self.assertTrue(all(r["lapses"]==1 and r["recovered"]==1 for r in rows))


class Recall(unittest.TestCase):
    def test_equal_effort_full_keys_and_frequency(self):
        cards=[dict(position_hash=dict(lo=9,hi=i),move="e2e4",reach=1/(i+1),ply=2*i)
               for i in range(12)]
        first=list(recall.schedules(cards,seed=1,daily=3,days=(0,1,4,8)))
        again=list(recall.schedules(cards,seed=1,daily=3,days=(0,1,4,8)))
        self.assertEqual(first,again)
        self.assertEqual(first[0][2]["presentations"],0)
        self.assertTrue(all(c["recall"]==0 for c in first[0][1].values()))
        self.assertEqual(first[1][2]["presentations"],3)
        self.assertTrue(first[1][1][(9,0)]["introduced"])
        self.assertFalse(first[1][1][(9,11)]["introduced"])
        self.assertEqual(first[-1][2]["presentations"],24)
        self.assertGreater(first[-1][1][(9,0)]["recall"],first[-1][1][(9,11)]["recall"])

    def test_nonempty_full_key_veto_receipt(self):
        from _test_horizon_lookahead import build, row, key as legacy_key
        import lookahead_policy as lp
        import lookahead_audit as la
        root=legacy_key("")
        frame=pl.DataFrame([row("","e4",50),row("","d4",50)])
        with tempfile.TemporaryDirectory() as td:
            path=Path(td); evidence=path/"book.parquet.lookahead-inputs.parquet"
            bans,stats,_=build(frame,{root:"d4"},{root:.425,legacy_key("e4"):.425,
                         legacy_key("d4"):.415},root,True,5,99,evidence)
            self.assertEqual(bans,{root:["d4"]})
            proof=pl.read_parquet(evidence)
            for column in ("parent","child"):
                proof=proof.with_columns(pl.when(pl.col(column).is_not_null())
                      .then(pl.struct(lo=pl.lit(42,dtype=pl.Int64),hi=pl.col(column))).alias(column))
            proof.write_parquet(evidence)
            book=path/"book.parquet"
            pl.DataFrame([dict(position_hash=dict(lo=42,hi=root),side_to_move="white",best_move="e4")]).write_parquet(book)
            class Keys:
                def records(self,rows,columns):
                    return [dict(r,position_hash=dict(lo=42,hi=r["position_hash"])) for r in rows]
            veto_sha=lp.write_veto(book,bans,{root:1.},keys=Keys())
            meta=dict(perspective="white",slice_prior=.52,robustness_floor=.1,max_ply=5,
                      lookahead_floor=dict(stats,evidence_sha256=lp.sha256(evidence),veto_sha256=veto_sha))
            self.assertEqual(la.audit(book,meta,check_receipt=False)["verified_vetoes"],1)

if __name__=="__main__": unittest.main()
