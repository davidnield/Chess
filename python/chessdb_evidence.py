"""Create a new immutable acceptance snapshot; never refresh an existing directory.

python chessdb_evidence.py --prior PATH --out NEW_PATH --lookup PATH --analysis PATH
The prior manifest is verified, and any previously unlisted files are explicitly
recorded. Final snapshots can be made after lookup completion in a NEW directory.
"""
import argparse
import datetime as dt
import json
import shutil
from pathlib import Path
from month_archive import sha,write_json,disjoint

def verify(root):
    root=Path(root); listed=set(); total=0
    for line in (root/"MANIFEST.tsv").read_text(encoding="utf8").splitlines():
        name,size,digest=line.split("\t");p=root/name
        if not p.resolve().is_relative_to(root.resolve()):raise ValueError("manifest path escapes root")
        if not p.is_file() or p.stat().st_size!=int(size) or sha(p)!=digest:
            raise ValueError(f"evidence mismatch: {p}")
        listed.add(name);total+=int(size)
    extras=sorted(p.relative_to(root).as_posix() for p in root.rglob("*")
                  if p.is_file() and p.relative_to(root).as_posix() not in listed and p.name not in ("MANIFEST.tsv","MANIFEST.sha256"))
    return dict(files=len(listed),bytes=total,manifest_sha256=sha(root/"MANIFEST.tsv"),previously_unlisted=extras)

def create(prior,out,lookup,analysis):
    prior,out,lookup,analysis=map(Path,(prior,out,lookup,analysis))
    disjoint(prior,out)
    if out.exists():raise ValueError("immutable snapshot path already exists; choose a new one")
    origin=verify(prior)
    shutil.copytree(prior,out)
    (out/"provenance").mkdir(exist_ok=True)
    shutil.copy2(prior/"MANIFEST.tsv",out/"provenance/prior-MANIFEST.tsv")
    shutil.copy2(prior/"MANIFEST.sha256",out/"provenance/prior-MANIFEST.sha256")
    write_json(out/"provenance/snapshot-origin.json",dict(source=str(prior),created=dt.datetime.now(dt.timezone.utc).isoformat(),**origin))
    def tree(src,rel):
        src=Path(src)
        if src.is_dir():shutil.copytree(src,out/rel,dirs_exist_ok=True)
        elif src.is_file():
            (out/rel).parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,out/rel)
        else:raise FileNotFoundError(src)
    menus=Path("E:/chess/chessdb-menus-20260702")
    for n in ("_params.json","_MENUS.COMPLETE.json","_done","_verification","README.md"):
        tree(menus/n,"menus/"+n)
    for n in ("_identity.json","_done","_status.json","lookup.json"):
        if (lookup/n).exists():tree(lookup/n,"lookup_v2/"+n)
    tree(analysis,"analysis/takeover-validation")
    tree(Path(__file__),"code/takeover/chessdb_evidence.py")
    tree(Path(__file__).with_name("chessdb_validation.py"),"code/takeover/chessdb_validation.py")
    ops=Path("D:/chess/takeover-20261122")
    for p in ops.iterdir():
        if p.is_file() and p.suffix in (".json",".log",".err",".py",".bat",".zip"):
            tree(p,"ops/takeover/"+p.name)
    repo=Path(__file__).resolve().parents[1]
    for name in ("repertoire-takeover-plan-2026-11-22.md","claude-response-to-chatgpt-review-2026-11-22.md"):
        tree(repo/"docs"/name,"docs/"+name)
    for pattern in ("chessdb_*.py", "_test_chessdb*.py"):
        for p in (repo/"python").glob(pattern):tree(p,"code/takeover/"+p.name)
    tree(repo/"python/chessdb_import","code/maintained-chessdb-import")
    state=dict(menus_verified=True,lookup=str(lookup),lookup_published=(lookup/"lookup.json").is_file(),
               snapshot_complete=True,nas_archive_verified=False,source_cleanup_permitted=False)
    write_json(out/"SNAPSHOT.json",state)
    rows=[]
    for p in sorted(out.rglob("*")):
        if not p.is_file() or p in (out/"MANIFEST.tsv",out/"MANIFEST.sha256"):continue
        rows.append(f"{p.relative_to(out).as_posix()}\t{p.stat().st_size}\t{sha(p)}")
    (out/"MANIFEST.tsv").write_text("\n".join(rows)+"\n",encoding="utf8",newline="\n")
    (out/"MANIFEST.sha256").write_text(sha(out/"MANIFEST.tsv")+"  MANIFEST.tsv\n",encoding="ascii")
    result=verify(out)
    if result["previously_unlisted"]:raise ValueError("new snapshot omitted files")
    print(json.dumps(result,indent=2))
    return result

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for n in ("prior","out","lookup","analysis"):p.add_argument("--"+n,required=True,type=Path)
    a=p.parse_args();create(a.prior,a.out,a.lookup,a.analysis)
