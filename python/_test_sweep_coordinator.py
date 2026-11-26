"""The epoch loop's interrupted-stage, shared-evidence and closure contracts."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import polars as pl
from consumer_keys import PAIR
from sweep.spec import arm,recipe
from sweep.stages import atomic_json,file_sha
from sweep.coordinator import drive
import run_rare_book as runner

class Epochs(unittest.TestCase):
    def test_preflight_requires_pinned_inputs_and_pilot_release(self):
        from sweep.coordinator import preflight
        from sweep.spec import manifest
        import engine_candidates as ec
        import sys
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);m=manifest()
            pilot=next(a for a in m["primary"] if a["id"]==m["pilot"])
            from sweep.spec import POLICY, pool_recipes
            pools={r["id"]:r for r in pool_recipes()}
            inputs={k:str(root/k) for k in ("source","stats","aux","weak","opponent_overlay")}
            for v in inputs.values(): Path(v).write_text("fixture",encoding="utf8")
            lookup=root/"lookup";lookup.mkdir();atomic_json(lookup/"lookup.json",{})
            def pool_meta(path,pool_id,**extra):
                r=pools[pool_id]
                atomic_json(path+".meta.json",dict(population=dict(events=r["events"],elo_bands=r["elo_bands"]),
                            max_ply=r["max_ply"],min_games=r["min_games"],key_bits=r["key_bits"],**extra))
            pool_meta(inputs["stats"],"brc1800-p30",eval_lookup128=str(lookup))
            pool_meta(inputs["weak"],"weak1000-1799-p30")
            atomic_json(inputs["opponent_overlay"]+".json",dict(schema=3,policy_recipe=POLICY,source_pool=inputs["stats"]))
            engine=root/"engine";engine.write_bytes(b"fixture")
            paths=[*map(Path,inputs.values()),Path(inputs["stats"]+".meta.json"),Path(inputs["weak"]+".meta.json"),
                   Path(inputs["opponent_overlay"]+".json"),lookup/"lookup.json",engine,Path(sys.executable)]
            pins=[dict(path=str(p),sha256=file_sha(p)) for p in paths]
            cfg=dict(schema=1,arms=[dict(arm=pilot,inputs=inputs)],artifacts=pins)
            with patch.object(ec,"SF",engine):
                self.assertEqual(preflight(cfg)[0]["arm"]["id"],m["pilot"])
                from sweep.scenarios import source_arm
                alternatives=[];extra=[]
                for n in (0,1):
                    look=lookup if n==0 else root/"alternative-lookup"
                    if n:look.mkdir();atomic_json(look/"lookup.json",dict(alternative=True));extra.append(look/"lookup.json")
                    binding=root/f"binding-{n}.json"
                    atomic_json(binding,dict(schema="eval-binding-v1",key_bits=128,lookup=str(look),lookup_sha256=file_sha(look/"lookup.json")))
                    extra.append(binding)
                    alternatives.append(dict(arm=source_arm(pilot,binding),inputs=dict(inputs,eval_binding=str(binding))))
                paired=dict(cfg,schema=2,arms=alternatives,referee_eval_binding=str(root/"binding-0.json"),
                    artifacts=pins+[dict(path=str(p),sha256=file_sha(p)) for p in extra])
                self.assertEqual(len(preflight(paired)),2)
                # the bound data must be what the recipe names (review 2026-11-19, finding 4)
                wrong=dict(inputs,stats=str(root/"stats50"));Path(wrong["stats"]).write_text("x")
                pool_meta(wrong["stats"],"brc1800-p50",eval_lookup128=str(lookup))
                with self.assertRaisesRegex(ValueError,"is not brc1800-p30.*max_ply 50"):
                    preflight(dict(cfg,arms=[dict(arm=pilot,inputs=wrong)]))
                weak=dict(inputs,weak=str(root/"weakbrc"));Path(weak["weak"]).write_text("x")
                pool_meta(weak["weak"],"brc1800-p30")
                with self.assertRaisesRegex(ValueError,"own post-book policy"):
                    preflight(dict(cfg,arms=[dict(arm=pilot,inputs=weak)]))
                other_policy=dict(POLICY,temperature=1.)
                bad=dict(inputs,opponent_overlay=str(root/"ov2"));Path(bad["opponent_overlay"]).write_text("x")
                atomic_json(bad["opponent_overlay"]+".json",dict(schema=3,policy_recipe=other_policy,source_pool=inputs["stats"]))
                with self.assertRaisesRegex(ValueError,"not a frozen ensemble of this recipe"):
                    preflight(dict(cfg,arms=[dict(arm=pilot,inputs=bad)]))
                horizon=next(a for a in m["sensitivities"] if a["family"]=="horizon"
                             and a["recipe"]["colour"]=="black" and a["recipe"]["gate"]=="stricter")
                with self.assertRaisesRegex(ValueError,"is not brc1800-p50"):
                    from sweep.bindings import check_arm
                    check_arm(horizon["recipe"],inputs)  # the ply-30 pool under a horizon-50 label
                controls=[a for a in m["sensitivities"] if a["family"]=="policy"
                          and a["recipe"]["colour"]=="black" and a["recipe"]["gate"]=="stricter"]
                ctl=dict(inputs,opponent_overlay=str(root/"ctl"));Path(ctl["opponent_overlay"]).write_text("x")
                atomic_json(ctl["opponent_overlay"]+".json",dict(schema=2,source_pool=inputs["stats"],reply_pool=inputs["stats"]))
                check_arm(next(a for a in controls if a["recipe"]["opponent_policy"]["name"].startswith("brc1800"))["recipe"],ctl)
                with self.assertRaisesRegex(ValueError,"brc2200-maia30 reply pool is not brc2200-p30"):
                    check_arm(next(a for a in controls if a["recipe"]["opponent_policy"]["name"].startswith("brc2200"))["recipe"],ctl)
                atomic_json(ctl["opponent_overlay"]+".json",dict(schema=2,source_pool=inputs["stats"]))
                with self.assertRaisesRegex(ValueError,"does not declare its reply_pool"):
                    check_arm(controls[0]["recipe"],ctl)
                with self.assertRaisesRegex(ValueError,"not all bound inputs"):
                    preflight(dict(cfg,artifacts=pins[:-1]))
                other=next(a for a in m["primary"] if a["recipe"]["colour"]=="white" and a["recipe"]["budget"]==2000)
                with self.assertRaisesRegex(ValueError,"pilot acceptance"):
                    preflight(dict(cfg,arms=[dict(arm=other,inputs=inputs)]))
                report=root/"pilot.json"
                atomic_json(report,dict(full_keys=True)) # incomplete must never release the matrix
                cfg=dict(cfg,arms=[dict(arm=other,inputs=inputs)],pilot_report=str(report),
                         artifacts=pins+[dict(path=str(report),sha256=file_sha(report))])
                with self.assertRaisesRegex(ValueError,"not passed"):
                    preflight(cfg)

    def test_cap_resume_rebuilds_every_arm_and_names_applied_evidence(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);source=root/"code";source.mkdir()
            (source/"placeholder.py").write_text("# fixture\n")
            entries=[dict(arm=arm(recipe("black",g,2)),inputs=dict(
                       source="source",stats="stats",aux="aux",weak="weak",opponent_overlay="policy"))
                     for g in ("stricter","permissive")]
            config=dict(arms=entries,artifacts=[],searches=str(root/"searches"))
            calls=[]
            def build(cmd,log):
                prefix=Path(cmd[cmd.index("--out-prefix")+1]);book=Path(str(prefix)+"_b2.parquet")
                prior=cmd[cmd.index("--engine-candidates")+1] if "--engine-candidates" in cmd else None
                calls.append((str(prefix),prior))
                # Missing evidence leaves an undersized provisional book, which must reach verification.
                n=2 if prior else 1
                pl.DataFrame([dict(position_hash=dict(lo=i,hi=i+2),side_to_move="black",best_move="e5")
                              for i in range(n)],schema_overrides={"position_hash":PAIR}).write_parquet(book)
                atomic_json(str(book)+".meta.json",dict(verification_policy="quality",
                             engine_candidates_sha256=file_sha(prior) if prior else None))
                atomic_json(str(book)+".rare.json",[])
                atomic_json(str(prefix)+".quality-frontier.json",[])
            def verify(books,dest,prior,log):
                # A no-op output intentionally differs, to test closure's APPLIED overlay identity.
                pl.DataFrame(dict(epoch=[1 if prior else 0])).write_parquet(dest)
                atomic_json(str(dest)+".requests.json",dict(books=books))
                atomic_json(str(dest)+".meta.json",dict(input_overlay_sha256=file_sha(prior) if prior else None,
                             overlay_sha256=file_sha(dest),requested_parents=0 if prior else 1,closed=bool(prior)))
            def audit(book,arm,dest):
                atomic_json(dest,dict(status="fixture-passed"))
                pl.DataFrame(dict(passed=[True])).write_parquet(dest.with_suffix(".rows.parquet"))
            kw=dict(source=source,build_fn=build,verify_fn=verify,audit_fn=audit)
            state=drive(config,root/"run",max_rounds=1,**kw)
            self.assertEqual(state["state"],"round-limit")
            self.assertFalse((root/"run/final.json").exists());self.assertEqual(len(calls),2)
            final=drive(config,root/"run",max_rounds=3,**kw)
            self.assertEqual(final["state"],"quality-closed");self.assertEqual(len(calls),4)
            self.assertEqual(final["applied_overlay_sha256"],file_sha(root/"run/round-00/evidence.parquet"))
            self.assertNotEqual(final["applied_overlay_sha256"],file_sha(root/"run/round-01/evidence.parquet"))
            drive(config,root/"run",max_rounds=3,**kw);self.assertEqual(len(calls),4)
            self.assertTrue(all(prior is not None for _,prior in calls[2:]))
            # A corrupt output with a receipt is never silently reused or overwritten.
            Path(final["books"][0]["book"]).write_bytes(b"broken")
            with self.assertRaises(runner.StaleRound):
                drive(config,root/"run",max_rounds=3,**kw)

if __name__=="__main__": unittest.main()
