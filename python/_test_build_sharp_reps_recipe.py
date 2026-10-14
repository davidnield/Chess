"""Pin the canonical sharp recipe (build_sharp_reps.common_flags) and its
provenance hash (2026-10-08).

  A  the default Stage-3 argv is EXACTLY the locked recipe below -- a change here
     changes every canonical rep, so it must be deliberate (edit this pin too).
  B  opt-in flags are appended only when on: the default argv carries none of
     them (the exact-no-op rule for new flags).
  C  recipe_hash ignores input/output paths, but changes with any setting.
  D  without aux, reply-shrink is restored to 1.0 (the pre-sidecar recipe).
  E  single pass: the learnability pass-2 is retired (2026-10-14).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import build_sharp_reps as b  # noqa: E402

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


PINNED = ["--eval-weight", "0.5", "--require-eval", "--robustness-floor", "0.1",
          "--gate-metric", "eval", "--gate-rel-floor", "0.1",
          "--gate-rel-baseline", "own-eval", "--gate-rel-own-margin", "0.02",
          "--min-move-games", "0", "--memo-weight", "0", "--reply-shrink", "0.0"]


def main() -> None:
    S, E, A = Path("pool.parquet"), Path("evals"), Path("aux.parquet")
    f = b.common_flags(S, E, A)
    check(f == ["--input", str(S), "--aux-stats", str(A), "--eval-db", str(E)] + PINNED,
          "A: default argv is the pinned recipe")

    on = b.common_flags(S, E, A, eval_weight=1.0, gate_fallback="strict",
                        other_cov_blend=True)
    check(not any(x in f for x in ("--gate-fallback", "--other-cov-blend")),
          "B: default argv carries no opt-in flag")
    check(on[-3:] == ["--gate-fallback", "strict", "--other-cov-blend"]
          and "1" in on[on.index("--eval-weight") + 1],
          f"B: opt-ins appended when on ({on[-3:]})")

    h = b.recipe_hash(f)
    check(h == b.recipe_hash(b.common_flags(Path("x"), Path("y"), Path("z"))),
          "C: hash is path-independent")
    check(h != b.recipe_hash(on), "C: hash changes with the recipe")
    check(h != b.recipe_hash(f + ["--plan-prior", "p", "--learn-delta-rare", "0.04"]),
          "C: a learnability setting would be part of the recipe (paths excluded)")
    check(not hasattr(b, "learn_flags") and not hasattr(b, "LEARN"),
          "E: the driver is single-pass (learnability pass retired 2026-10-14)")

    nf = b.common_flags(S, E, None)
    check(nf[nf.index("--reply-shrink") + 1] == str(b.REPLY_SHRINK) == "1.0",
          "D: --no-aux restores reply-shrink 1.0")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
