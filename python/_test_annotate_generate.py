"""Unit test: run_claude() error taxonomy in annotate.generate_annotations.

The resumable full run must survive a transient claude blip. This pins the
classification run_claude() hands the caller:
  - missing CLI            -> ClaudeUnavailable  (caller aborts the whole run)
  - usage/quota/rate limit -> UsageLimit         (caller saves + stops to resume)
  - any other CLI failure  -> RuntimeError        (caller skips the chunk, continues)
  - clean JSON envelope    -> returns result text

No real `claude` CLI or fixtures — subprocess.run is monkeypatched.
"""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from annotate import generate_annotations as gen

_passed = 0
_failed = 0


def check(name: str, cond: bool) -> None:
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"PASS: {name}")
    else:
        _failed += 1
        print(f"FAIL: {name}")


def _fake_run(returncode=0, stdout="", stderr="", raise_exc=None):
    def _run(*a, **k):
        if raise_exc is not None:
            raise raise_exc
        return types.SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)
    return _run


def outcome(**run_kw):
    """Return (kind, payload) for a run_claude call under a faked subprocess."""
    gen.subprocess.run = _fake_run(**run_kw)
    try:
        return ("return", gen.run_claude("prompt", "sonnet", timeout=5))
    except gen.ClaudeUnavailable as e:
        return ("ClaudeUnavailable", str(e))
    except gen.UsageLimit as e:
        return ("UsageLimit", str(e))
    except RuntimeError as e:
        return ("RuntimeError", str(e))


def main() -> None:
    # 1. Missing CLI -> ClaudeUnavailable (fatal), NOT a generic RuntimeError.
    kind, _ = outcome(raise_exc=FileNotFoundError())
    check("missing CLI -> ClaudeUnavailable", kind == "ClaudeUnavailable")

    # 2. Non-zero exit carrying a usage-limit marker -> UsageLimit.
    kind, _ = outcome(returncode=1, stdout="Error: usage limit exceeded, try later")
    check("exit!=0 + usage marker -> UsageLimit", kind == "UsageLimit")

    # 3. Non-zero exit with no marker -> transient RuntimeError (caller continues).
    kind, _ = outcome(returncode=1, stdout="segfault boom")
    check("exit!=0 generic -> RuntimeError", kind == "RuntimeError")

    # 4. Clean JSON envelope -> result text returned.
    kind, payload = outcome(returncode=0, stdout=json.dumps({"result": "hello"}))
    check("clean envelope -> returns result", kind == "return" and payload == "hello")

    # 5. is_error envelope with a quota marker -> UsageLimit.
    kind, _ = outcome(returncode=0,
                      stdout=json.dumps({"is_error": True, "error": "quota exhausted"}))
    check("is_error + quota marker -> UsageLimit", kind == "UsageLimit")

    # 6. Exit 0 but unparseable stdout -> RuntimeError (transient).
    kind, _ = outcome(returncode=0, stdout="not json at all")
    check("exit0 unparseable envelope -> RuntimeError", kind == "RuntimeError")

    # 7. ClaudeUnavailable must be distinct from RuntimeError so the caller's
    #    except-order (abort vs skip) actually separates them.
    check("ClaudeUnavailable is not a RuntimeError",
          not issubclass(gen.ClaudeUnavailable, RuntimeError))

    print(f"\n{_passed} passed, {_failed} failed")
    sys.exit(1 if _failed else 0)


if __name__ == "__main__":
    main()
