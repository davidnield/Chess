"""Publication is stronger than successful merge markers or extant metadata."""
import copy,json,tempfile,unittest
from pathlib import Path
from sweep.publications import validate
from sweep.stages import atomic_json,file_sha

class Publication(unittest.TestCase):
    def test_overlay_requires_full_bound_generation_and_acceptance_evidence(self):
        with tempfile.TemporaryDirectory() as td:
            r=Path(td); book=r/'book'; lookup=r/'lookup'; base=r/'base'; db=r/'db'
            bm=book/'_book.meta.json'; lm=lookup/'lookup.json'; dm=db/'_build.meta.json'; fallback=base/'lookup.json'
            atomic_json(bm,dict(schema='book-v4')); (book/'_BOOK.DONE').touch()
            atomic_json(dm,dict(inputs=dict(book_meta_sha256=file_sha(bm))))
            common=dict(terminal=True,book_meta_sha256=file_sha(bm),
                source=dict(source=str(db),build_meta_sha256=file_sha(dm)),
                buckets={f'bkt{i:03d}.npy':{} for i in range(512)})
            atomic_json(fallback,dict(common,schema='eval-lookup-128-v1'))
            meta=dict(common,schema='eval-lookup-128-score-overlay-v1',complete=True,
                policy='cloud-nnue-chessdb-score-334-v1',base=dict(root=str(base),sha256=file_sha(fallback)),
                identity=dict(producer=dict(native='native-sha')))
            atomic_json(lm,meta)
            ex=r/'explorer.json'; ev=r/'evals.json'; proof=r/'policy.json'
            atomic_json(ex,dict(stage='training explorer verified',book=str(book),meta_sha256=file_sha(bm)))
            event=dict(stage='eval DB and lookup verified',lookup=str(lookup),db=str(db),
                       lookup_sha256=file_sha(lm),db_meta_sha256=file_sha(dm))
            atomic_json(ev,event)
            paths=[bm,lm,dm,fallback,ex,ev]
            def bind():return dict(explorer=str(book),eval_lookup=str(lookup),
                publications=dict(explorer=str(ex),eval_lookup=str(ev)),
                artifacts=[dict(path=str(p),sha256=file_sha(p)) for p in paths])
            with self.assertRaisesRegex(ValueError,'acceptance receipt'):validate(bind())
            evidence={}
            for name in ('native_tests','real_source_check','white_review','materialization'):
                p=r/(name+'.json');atomic_json(p,dict(pass_check=True));paths.append(p)
                evidence[name]=dict(path=str(p),sha256=file_sha(p))
            receipt=dict(schema='eval-policy-pilot-ready-v1',policy=meta['policy'],
                lookup_sha256=file_sha(lm),fallback_sha256=file_sha(fallback),native_sha256='native-sha',
                checks={n:True for n in evidence},evidence=evidence)
            atomic_json(proof,receipt);paths.append(proof)
            atomic_json(ev,dict(event,policy_verification_receipt=str(proof)))
            validate(bind())
            changed=copy.deepcopy(receipt);changed['checks']['white_review']=False
            atomic_json(proof,changed)
            with self.assertRaisesRegex(ValueError,'acceptance evidence'):validate(bind())
            atomic_json(proof,receipt)
            paths.remove(fallback)
            with self.assertRaisesRegex(ValueError,'not pinned'):validate(bind())
            paths.append(fallback)
            meta['complete']=False;atomic_json(lm,meta)
            atomic_json(ev,dict(event,lookup_sha256=file_sha(lm),policy_verification_receipt=str(proof)))
            with self.assertRaisesRegex(ValueError,'unpublished'):validate(bind())

    def test_completed_matching_generations_required(self):
        with tempfile.TemporaryDirectory() as td:
            r=Path(td);book=r/"book";lookup=r/"lookup";db=r/"db"
            bm=book/"_book.meta.json";lm=lookup/"lookup.json";dm=db/"_build.meta.json"
            atomic_json(bm,dict(schema="book-v4"));(book/"_BOOK.DONE").touch()
            atomic_json(dm,dict(inputs=dict(book_meta_sha256=file_sha(bm))))
            atomic_json(lm,dict(schema="eval-lookup-128-v1",terminal=True,book_meta_sha256=file_sha(bm),
                     source=dict(source=str(db),build_meta_sha256=file_sha(dm)),
                     buckets={f"bkt{i:03d}.npy":{} for i in range(512)}))
            ex=r/"explorer.json";ev=r/"evals.json"
            atomic_json(ex,dict(stage="training explorer verified",book=str(book),meta_sha256=file_sha(bm)))
            atomic_json(ev,dict(stage="eval DB and lookup verified",lookup=str(lookup),db=str(db),
                               lookup_sha256=file_sha(lm),db_meta_sha256=file_sha(dm)))
            paths=[bm,lm,dm,ex,ev]
            def bind():return dict(explorer=str(book),eval_lookup=str(lookup),
                            publications=dict(explorer=str(ex),eval_lookup=str(ev)),
                            artifacts=[dict(path=str(p),sha256=file_sha(p)) for p in paths])
            self.assertEqual(validate(bind())["book_meta_sha256"],file_sha(bm))
            bad=bind();bad.pop("publications")
            with self.assertRaisesRegex(ValueError,"receipts"):validate(bad)
            saved=json.loads(ex.read_text(encoding="utf8"))
            atomic_json(ex,dict(saved,stage="recovering explorer verification"))
            with self.assertRaisesRegex(ValueError,"not verification-complete"):validate(bind())
            atomic_json(ex,saved)
            # A named eval verification receipt must be pinned, unchanged, a pass, and of this DB.
            rc=r/"verification-receipt.json"
            def receipt(db_sha,all_pass=True,where=db):
                atomic_json(rc,dict(schema="eval-verification-receipt-v1",result=dict(all_pass=all_pass),
                                    identity=dict(db=str(where),db_hashes={"_build.meta.json":db_sha})))
                e=json.loads(ev.read_text(encoding="utf8"))
                atomic_json(ev,dict(e,verification_receipt=str(rc),verification_receipt_sha256=file_sha(rc)))
            def bind_rc():
                b=bind();b["artifacts"].append(dict(path=str(rc),sha256=file_sha(rc)));return b
            receipt(file_sha(dm))
            self.assertEqual(validate(bind_rc())["db_meta_sha256"],file_sha(dm))
            with self.assertRaisesRegex(ValueError,"not pinned"):validate(bind())
            for args in ((("0"*64),),(file_sha(dm),False),(file_sha(dm),True,r/"otherdb")):
                receipt(*args)
                with self.assertRaisesRegex(ValueError,"verification receipt"):validate(bind_rc())
            m=json.loads(lm.read_text(encoding="utf8"));m["book_meta_sha256"]="different";atomic_json(lm,m)
            e=json.loads(ev.read_text(encoding="utf8"));e["lookup_sha256"]=file_sha(lm);atomic_json(ev,e)
            with self.assertRaisesRegex(ValueError,"generations disagree"):validate(bind())

if __name__=="__main__":unittest.main()
