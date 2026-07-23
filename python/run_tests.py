"""Run every _test_*.py in this directory and report a pass/fail summary.

Each test file is a standalone script (no pytest): it prints PASS/FAIL lines
and exits 0 (all checks passed) or 1 (at least one failure). This runner just
subprocess-invokes each one with the project's venv interpreter and collects
the exit codes, so "did I break anything" is one command instead of N.

Usage:
    .venv/Scripts/python.exe python/run_tests.py
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> None:
    tests = sorted(HERE.glob("_test_*.py"))
    if not tests:
        sys.exit("No _test_*.py files found.")

    results: list[tuple[str, bool, float]] = []
    for t in tests:
        t0 = time.time()
        proc = subprocess.run([sys.executable, str(t)], capture_output=True, text=True)
        elapsed = time.time() - t0
        ok = proc.returncode == 0
        results.append((t.name, ok, elapsed))
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {t.name}  ({elapsed:.1f}s)")
        if not ok:
            print(f"  --- {t.name} output ---")
            print((proc.stdout + proc.stderr).rstrip())
            print(f"  --- end {t.name} ---")

    n_fail = sum(1 for _, ok, _ in results if not ok)
    total = sum(e for _, _, e in results)
    print(f"\n{len(results) - n_fail}/{len(results)} test files passed in {total:.1f}s total.")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
