"""A pinned copy of python/ imports its OWN modules (review of R1-R6, #7).

Long runs launch from a snapshot (D:/chess/bin/...) with the working directory at the repo root.
Scripts that put "python" on sys.path imported the WORKING TREE's modules from there, so editing
the tree could change a running job. This copies python/ to a temporary snapshot, imports every
entry point from it with the repo as the working directory, and asserts that every first-party
module came from the snapshot. Also: no script may use a working-directory-relative import path.

Run: .venv/Scripts/python.exe python/_test_snapshot_imports.py
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENTRY = ["convertibility_eval", "side_explorer", "extend_book", "validate_book", "stop_alignment",
         "bootstrap_validation", "band_contamination", "engine_candidates", "sampled_stop",
         "build_budget_books", "budget_core"]
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


bad = [f.name for f in HERE.glob("*.py") if f.name != Path(__file__).name
       and re.search(r"""sys\.path\.insert\(0, ["']python["']\)""", f.read_text(encoding="utf-8"))]
check("no script puts the working directory's python/ on its path", not bad, str(bad))

with tempfile.TemporaryDirectory() as td:
    snap = Path(td) / "snap"
    snap.mkdir()
    for f in HERE.glob("*.py"):
        if not f.name.startswith("_test_"):
            shutil.copy(f, snap / f.name)
    first_party = {f.stem for f in HERE.glob("*.py")}
    probe = snap / "_probe.py"
    probe.write_text(f"""
import importlib, json, sys
sys.path.insert(0, {str(snap)!r})
out = {{}}
for name in {ENTRY!r}:
    importlib.import_module(name)
for name, mod in list(sys.modules.items()):
    if name in {sorted(first_party)!r} and getattr(mod, "__file__", None):
        out[name] = mod.__file__
print(json.dumps(out))
""", encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": str(snap)}
    r = subprocess.run([sys.executable, str(probe)], cwd=HERE.parent, env=env, capture_output=True,
                       text=True, timeout=300)
    if r.returncode:
        print(r.stderr[-2000:])
    check("every entry point imports", r.returncode == 0)
    mods = json.loads(r.stdout.strip().splitlines()[-1]) if r.returncode == 0 else {}
    outside = {k: v for k, v in mods.items() if Path(v).resolve().parent != snap.resolve()}
    check(f"all {len(mods)} first-party modules come from the snapshot", mods and not outside, str(outside))

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
