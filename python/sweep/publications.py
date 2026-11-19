"""Require completed independent publication receipts before shared pool exports."""
import json
from pathlib import Path
from .stages import artifacts,file_sha

def validate(bindings):
    pinned=artifacts(bindings["artifacts"])
    receipts=bindings.get("publications",{})
    if set(receipts)!={"explorer","eval_lookup"}:
        raise ValueError("completed explorer and eval publication receipts are required")
    def checked(path):
        path=Path(path).resolve()
        if str(path) not in pinned:raise ValueError(f"publication dependency is not pinned: {path}")
        return json.loads(path.read_text(encoding="utf8"))
    book=Path(bindings["explorer"]).resolve();lookup=Path(bindings["eval_lookup"]).resolve()
    ex=checked(receipts["explorer"]);ev=checked(receipts["eval_lookup"])
    if ex.get("stage")!="training explorer verified":raise ValueError("explorer is not verification-complete")
    if ev.get("stage")!="eval DB and lookup verified":raise ValueError("eval lookup is not verification-complete")
    if Path(ex["book"]).resolve()!=book or Path(ev["lookup"]).resolve()!=lookup:
        raise ValueError("publication paths name another generation")
    checked(book/"_book.meta.json")
    book_sha=pinned[str(book/"_book.meta.json")]
    if ex["meta_sha256"]!=book_sha:raise ValueError("explorer publication metadata changed")
    if not (book/"_BOOK.DONE").is_file():raise ValueError("explorer completion marker missing")
    meta=checked(lookup/"lookup.json")
    if ev["lookup_sha256"]!=pinned[str(lookup/"lookup.json")]:raise ValueError("lookup publication metadata changed")
    db=Path(ev["db"]).resolve();dbmeta=checked(db/"_build.meta.json")
    if ev["db_meta_sha256"]!=pinned[str(db/"_build.meta.json")]:raise ValueError("eval DB metadata changed")
    if (meta.get("schema")!="eval-lookup-128-v1" or meta.get("terminal") is not True
        or meta.get("book_meta_sha256")!=book_sha or dbmeta["inputs"]["book_meta_sha256"]!=book_sha
        or Path(meta["source"]["source"]).resolve()!=db
        or meta["source"]["build_meta_sha256"]!=ev["db_meta_sha256"]):
        raise ValueError("lookup, eval DB and explorer generations disagree")
    if set(meta["buckets"])!={f"bkt{i:03d}.npy" for i in range(512)}:
        raise ValueError("incomplete full-key lookup publication")
    # Recovery publishes the independent check bundle explicitly. Older verified
    # coordinators have only the completed status receipt, also pinned above.
    if "verification_receipt" in ex:
        proof=checked(ex["verification_receipt"])
        if (file_sha(ex["verification_receipt"])!=ex["verification_receipt_sha256"]
            or proof["book_meta_sha256"]!=book_sha):
            raise ValueError("explorer check bundle changed")
    return dict(book_meta_sha256=book_sha,lookup_sha256=ev["lookup_sha256"],
                db_meta_sha256=ev["db_meta_sha256"])
