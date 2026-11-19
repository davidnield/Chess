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
            inputs={k:str(root/k) for k in ("source","stats","aux","weak","opponent_overlay")}
            for v in inputs.values(): Path(v).write_text("fixture",encoding="utf8")
            lookup=root/"lookup";lookup.mkdir();atomic_json(lookup/"lookup.json",{})
            atomic_json(inputs["stats"]+".meta.json",dict(eval_lookup128=str(lookup)))
            atomic_json(inputs["opponent_overlay"]+".json",{})
            engine=root/"engine";engine.write_bytes(b"fixture")
            paths=[*map(Path,inputs.values()),Path(inputs["stats"]+".meta.json"),
                   Path(inputs["opponent_overlay"]+".json"),lookup/"lookup.json",engine,Path(sys.executable)]
            pins=[dict(path=str(p),sha256=file_sha(p)) for p in paths]
            cfg=dict(schema=1,arms=[dict(arm=pilot,inputs=inputs)],artifacts=pins)
            with patch.object(ec,"SF",engine):
                self.assertEqual(preflight(cfg)[0]["arm"]["id"],m["pilot"])
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
