"""Every name the budget builder and the sweep read is bound somewhere in its module.

A crude, module-wide check (it cannot see scope), but it catches the class that broke the
2026-11-20 census: 8a9e7be dropped the 64-bit pool's ``mm_hash, mm_cp`` binding, and the two
lookups that used them sat on branches no test reaches (a 64-bit pool with source-only children
or quality requests), so the builder died with a NameError after 30 minutes of collection.
"""
import ast
import builtins
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODULES = ["build_budget_books.py", "budget_core.py", "budget_rs.py", "budget_compiled.py",
           "sampled_stop.py", "sampled_other.py", "weak_stop.py", "consumer_keys.py",
           *sorted(p.relative_to(HERE).as_posix() for p in (HERE / "sweep").glob("*.py"))]
IMPLICIT = set(dir(builtins)) | {"__file__", "__name__", "__doc__", "__spec__"}


def unbound(path):
    tree = ast.parse(path.read_text(encoding="utf8"))
    bound, loads = set(IMPLICIT), []
    for n in ast.walk(tree):
        if isinstance(n, ast.Name):
            if isinstance(n.ctx, ast.Load):
                loads.append((n.id, n.lineno))
            else:
                bound.add(n.id)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(n.name)
        elif isinstance(n, ast.arg):
            bound.add(n.arg)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            bound.update((a.asname or a.name).split(".")[0] for a in n.names)
        elif isinstance(n, ast.ExceptHandler) and n.name:
            bound.add(n.name)
        elif isinstance(n, (ast.Global, ast.Nonlocal)):
            bound.update(n.names)
        elif isinstance(n, ast.MatchAs) and n.name:
            bound.add(n.name)
    return sorted({(name, line) for name, line in loads if name not in bound}, key=lambda x: x[1])


def main():
    bad = 0
    for rel in MODULES:
        path = HERE / rel
        if not path.is_file():
            print(f"FAIL  {rel}: listed but missing")
            bad += 1
            continue
        names = unbound(path)
        print(f"{'FAIL' if names else 'PASS'}  {rel}" + (f": {names}" if names else ""))
        bad += bool(names)
    # The check itself must catch the regression it was written for.
    probe = "def f(keys):\n    a = g(1, mm_hash)\n"
    tree_names = [n.id for n in ast.walk(ast.parse(probe)) if isinstance(n, ast.Name)]
    assert "mm_hash" in tree_names
    tmp = HERE / "__bound_names_probe.py"
    try:
        tmp.write_text(probe, encoding="utf8")
        caught = sorted(n for n, _ in unbound(tmp))
    finally:
        tmp.unlink(missing_ok=True)
    if caught != ["g", "mm_hash"]:
        print(f"FAIL  self-check: expected ['g', 'mm_hash'], got {caught}")
        bad += 1
    print("ALL PASS" if not bad else f"{bad} FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
