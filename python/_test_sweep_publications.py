"""Publication is stronger than successful merge markers or extant metadata."""
import copy,json,tempfile,unittest
from pathlib import Path
from sweep.publications import validate
from sweep.stages import atomic_json,file_sha

class Publication(unittest.TestCase):
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
            m=json.loads(lm.read_text(encoding="utf8"));m["book_meta_sha256"]="different";atomic_json(lm,m)
            e=json.loads(ev.read_text(encoding="utf8"));e["lookup_sha256"]=file_sha(lm);atomic_json(ev,e)
            with self.assertRaisesRegex(ValueError,"generations disagree"):validate(bind())

if __name__=="__main__":unittest.main()
