"""Every path CLAUDE.md documents still exists.

CLAUDE.md's data-paths table is the most load-bearing thing in the file — it is
what tells a session which tree is canonical, which one is archive-only, and
which drive must never take DuckDB spill. It is also the part most likely to rot,
because it describes the filesystem rather than the code, and nothing fails when
the filesystem moves on.

That is not hypothetical. On 2026-08-12 the table still listed
`E:/chess/position-moves-*` (deleted 08-08) and `E:/chess/crush-per-game-v2/`
(deleted that morning) as live data. Length was never the problem with that file;
drift was. This turns "someone notices eventually" into a suite failure.

Scope, deliberately narrow: this asserts EXISTENCE of documented paths, not that
their descriptions are accurate. A path can exist and still be described wrongly,
and no test catches that.

Portability: the pipeline's data lives on D:/E:/F: on one machine. On any other
checkout those drives are absent, which is not a documentation bug, so a missing
DRIVE is skipped while a missing path on a PRESENT drive fails. Glob patterns are
satisfied by any match.

Run: .venv/Scripts/python.exe python/_test_docs_paths.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

DOC = Path(__file__).resolve().parent.parent / "CLAUDE.md"
# Backticked paths inside the data-paths table rows: absolute, drive-lettered.
PATH_RE = re.compile(r"`([A-Za-z]:/[^`]+)`")
# Hive partition placeholders and shell/glob wildcards are patterns, not paths.
PLACEHOLDER = re.compile(r"(year=Y|month=M|event=E|<[^>]+>|%[A-Z_]+%)")

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def resolve(spec: str) -> tuple[str, bool]:
    """(verdict, ok). Placeholders are stripped to their fixed prefix; globs are
    satisfied by any match."""
    if PLACEHOLDER.search(spec):
        spec = PLACEHOLDER.split(spec)[0].rstrip("/")
    p = Path(spec)
    drive = Path(p.drive + "/")
    if not drive.exists():
        return f"SKIP (drive {p.drive} not mounted)", True
    if "*" in spec:
        parent = Path(spec).parent
        if not parent.exists():
            return "MISSING (parent of glob)", False
        return ("ok (glob matched)", True) if any(parent.glob(Path(spec).name)) \
            else ("MISSING (glob matched nothing)", False)
    return ("ok", True) if p.exists() else ("MISSING", False)


def main() -> int:
    print("=" * 70)
    print("CLAUDE.md documented paths still exist")
    print("=" * 70)
    if not DOC.exists():
        print(f"FATAL: no CLAUDE.md at {DOC}")
        return 1

    specs: list[str] = []
    for line in DOC.read_text(encoding="utf-8").splitlines():
        # Table rows only — prose mentions paths that are deliberately gone
        # (the deleted `standard-chess-games` trap is documented BECAUSE it is
        # missing, so asserting it exists would invert the point).
        if not line.startswith("|"):
            continue
        for m in PATH_RE.finditer(line):
            if m.group(1) not in specs:
                specs.append(m.group(1))

    check(len(specs) >= 4,
          f"found {len(specs)} documented path(s) in the data-paths table")
    n_bad = 0
    for spec in specs:
        verdict, ok = resolve(spec)
        n_bad += not ok
        print(f"  {'PASS' if ok else 'FAIL'}  {spec}  ->  {verdict}")
    _checks.append((n_bad == 0, "all documented paths resolve"))

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print()
    if n_fail:
        print(f"{n_fail} FAILED — CLAUDE.md documents a path that no longer "
              f"exists. Fix the table, don't relax the test.")
    else:
        print(f"ALL PASS ({len(specs)} paths checked)")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
