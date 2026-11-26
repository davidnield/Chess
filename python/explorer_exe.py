"""Which explorer-extract the exe-dependent tests run, and whether it can run them.

The tests take $EXPLORER_EXTRACT_EXE, else the default cargo target below. That default is whatever was last
built there, which can be far older than the checkout (on 2026-11-26 it was an Oct-30 build of c865f9b, with
no --key128): tests then died with KeyError 'parent_hash_hi', or failed checks of behaviour added since
(_test_verify_evals' dropped-row check), and read as code defects.

`unusable(exe, {subcommand: [flags]})` gives the reason to skip, or None:
  * the exe is missing;
  * a `<subcommand> --help` does not list a needed flag;
  * the exe's build commit (from `selftest`) does not contain the last commit that changed the crate in this
    checkout, i.e. the binary predates the Rust source under test. Checked only where git can answer; a
    commit git does not know is stale too.
Build the branch's own exe (rust/explorer_extract: `cargo build --release --locked`, CARGO_TARGET_DIR on D:)
and point EXPLORER_EXTRACT_EXE at it to run them.
"""
from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

DEFAULT = "D:/rust-target/explorer_extract/release/explorer-extract.exe"
REPO = Path(__file__).resolve().parent.parent
CRATE = "rust/explorer_extract"
FIX = "set EXPLORER_EXTRACT_EXE to a build of this checkout"


def resolve() -> Path:
    return Path(os.environ.get("EXPLORER_EXTRACT_EXE", DEFAULT))


def _git(*args: str) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, errors="replace")
    except OSError:
        return None


def build_commit(exe: Path) -> str | None:
    """The commit `selftest` says the exe was built from (None if it does not say)."""
    p = subprocess.run([str(exe), "selftest"], capture_output=True, text=True, errors="replace")
    m = re.search(r"\(commit ([0-9a-f]{7,40})", p.stdout + p.stderr)
    return m.group(1) if m else None


def stale(exe: Path) -> str | None:
    """Why the exe predates this checkout's crate, or None (also None when git cannot tell)."""
    last = _git("log", "-1", "--format=%H", "--", CRATE)
    if last is None or last.returncode or not last.stdout.strip():
        return None
    last = last.stdout.strip()
    built = build_commit(exe)
    if built is None:
        return f"{exe} does not report its build commit ({FIX})"
    known = _git("cat-file", "-e", f"{built}^{{commit}}")
    if known is None or known.returncode:
        return f"{exe} was built from {built}, which this repo does not have ({FIX})"
    anc = _git("merge-base", "--is-ancestor", last, built)
    if anc is not None and anc.returncode == 1:
        return f"{exe} was built from {built[:12]}, before {last[:12]} last changed {CRATE} ({FIX})"
    return None


def unusable(exe: Path, needs: dict[str, list[str]]) -> str | None:
    """None when `exe` exists, lists every needed flag and is no older than the crate; else why not."""
    if not exe.is_file():
        return f"no explorer-extract at {exe} (set EXPLORER_EXTRACT_EXE)"
    for sub, flags in needs.items():
        p = subprocess.run([str(exe), sub, "--help"], capture_output=True, text=True, errors="replace")
        if p.returncode != 0:
            return f"{exe} has no `{sub}` subcommand ({FIX})"
        missing = [f for f in flags if f not in p.stdout + p.stderr]
        if missing:
            return f"{exe} `{sub}` lacks {', '.join(missing)} ({FIX})"
    return stale(exe)
