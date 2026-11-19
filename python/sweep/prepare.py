"""Plan or run one receipted shared-pool export. Default is read-only readiness."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
from . import spec, stages

def pool_stage(recipe, bindings, root):
    """Production paths are supplied only after migration publication; no guessed ready markers."""
    code=Path(bindings["code"]).resolve()
    python=bindings.get("python",sys.executable)
    root=Path(root).resolve()
    cmd=stages.pool_command(recipe,python=python,code=code,explorer=bindings["explorer"],
                            lookup=bindings["eval_lookup"],root=root)
    folder=root/recipe["id"]
    output=folder/"pool"
    pool=output/f"position_stats_pooled_{recipe['id']}.parquet"
    aux=output/f"position_stats_aux_pooled_{recipe['id']}.parquet"
    inputs=list(bindings["artifacts"])
    required={str((Path(bindings["explorer"])/"_book.meta.json").resolve()),
              str((Path(bindings["eval_lookup"])/"lookup.json").resolve())}
    if not required<={str(Path(x["path"]).resolve()) for x in inputs}:
        raise ValueError("pin the published explorer and lookup manifests")
    # Pin every imported Python module, not just the top-level launcher.
    for p in sorted((code/"python").glob("*.py")):
        if not p.name.startswith("_test_"):
            inputs.append(dict(path=str(p),sha256=stages.file_sha(p)))
    for p in sorted((code/"python/sweep").glob("*.py")):
        inputs.append(dict(path=str(p),sha256=stages.file_sha(p)))
    validator=[str(python),"-X","utf8",str(code/"python/sweep/validate_pool.py"),
               "--work",str(folder/"work"),"--out-dir",str(output),
               "--tmp-dir",str(folder/"tmp")]
    return dict(id=recipe["id"],command=cmd,cwd=str(code),inputs=inputs,
                outputs=[str(pool),str(aux),str(pool)+".meta.json",str(aux)+".meta.json",
                         str(folder/"work/_gates.json")],validate=validator)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest",type=Path,required=True)
    p.add_argument("--bindings",type=Path)
    p.add_argument("--workspace",type=Path,required=True)
    p.add_argument("--pool",help="one pool ID from the manifest")
    p.add_argument("--execute",action="store_true",help="run this one pool, preserving its bucket checkpoints")
    a=p.parse_args()
    m=json.loads(a.manifest.read_text(encoding="utf8")); spec.validate(m)
    if not a.bindings:
        if a.execute: p.error("--execute needs frozen --bindings and one --pool")
        print(json.dumps(dict(state="waiting-for-publication",pools=[r["id"] for r in m["inputs"]],
                              required_bindings=m["required_bindings"]),indent=2))
        return
    bindings=json.loads(a.bindings.read_text(encoding="utf8"))
    if not a.pool: p.error("--bindings needs one --pool to check")
    recipes={r["id"]:r for r in m["inputs"]}
    if a.pool not in recipes: p.error("pool is not in this sweep")
    plan=pool_stage(recipes[a.pool],bindings,a.workspace)
    print(json.dumps(stages.stage(plan,a.workspace,execute=a.execute),indent=2))

if __name__=="__main__": main()
