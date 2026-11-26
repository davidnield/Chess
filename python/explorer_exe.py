"""Which explorer-extract the exe-dependent tests run, and whether it can run them.

The tests take $EXPLORER_EXTRACT_EXE, else the default cargo target below. That default is whatever was last
built there, which can be a build from before the flags a test exercises (the Oct-29 build there has no
--key128, so a test that asked for 128-bit columns died with KeyError 'parent_hash_hi'). A test calls
`unusable(exe, {subcommand: [flags]})` first: a missing exe, or one whose `<subcommand> --help` does not list
every flag the test needs, is a reason to skip, printed with the fix. Build the branch's own exe
(rust/explorer_extract, `cargo build --release --locked`) and point EXPLORER_EXTRACT_EXE at it to run them.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

DEFAULT = "D:/rust-target/explorer_extract/release/explorer-extract.exe"


def resolve() -> Path:
    return Path(os.environ.get("EXPLORER_EXTRACT_EXE", DEFAULT))


def unusable(exe: Path, needs: dict[str, list[str]]) -> str | None:
    """None when `exe` exists and every `<subcommand> --help` names each needed flag; else why not."""
    if not exe.is_file():
        return f"no explorer-extract at {exe} (set EXPLORER_EXTRACT_EXE)"
    for sub, flags in needs.items():
        p = subprocess.run([str(exe), sub, "--help"], capture_output=True, text=True, errors="replace")
        if p.returncode != 0:
            return f"{exe} has no `{sub}` subcommand: a stale build (set EXPLORER_EXTRACT_EXE to a current one)"
        text = p.stdout + p.stderr
        missing = [f for f in flags if f not in text]
        if missing:
            return (f"{exe} `{sub}` lacks {', '.join(missing)}: a build older than this branch "
                    f"(set EXPLORER_EXTRACT_EXE to a current one)")
    return None
