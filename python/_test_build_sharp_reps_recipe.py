"""Pin the canonical sharp recipe (build_sharp_reps.common_flags) and its
provenance hash (2026-10-08).

  A  the default Stage-3 argv is EXACTLY the locked recipe below -- a change here
     changes every canonical rep, so it must be deliberate (edit this pin too).
     Recipe of 2026-10-17: eval weight 0.75, gate fallback strict, other-cov blend.
  B  a Stage-3 flag is passed only when it differs from Stage 3's own default, so
     the legacy settings (gate-fallback legacy, no cov blend, margin 0) produce an
     argv carrying none of the opt-in flags.
  C  recipe_hash ignores input/output paths, but changes with any setting.
  D  without aux, reply-shrink is restored to 1.0 (the pre-sidecar recipe).
  E  single pass: the learnability pass-2 is retired (2026-10-14).
  G  rebuild stickiness: --stick-margin is a recipe setting, the incumbent path is not.
  J  a canonical rebuild is sticky (0.01) against the pair it replaces by default.
  H  --prior-strength is passed only when it differs from Stage 3's 500; a meta without
     the key reads as 500, so existing reps are not refused.
  I  --force-move / --avoid-move specs go only to the colour whose turn they are.
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


PINNED = ["--eval-weight", "0.75", "--require-eval", "--robustness-floor", "0.1",
          "--gate-metric", "eval", "--gate-rel-floor", "0.1",
          "--gate-rel-baseline", "own-eval", "--gate-rel-own-margin", "0.02",
          "--min-move-games", "0", "--memo-weight", "0", "--reply-shrink", "0.0",
          "--gate-fallback", "strict", "--other-cov-blend"]


def main() -> None:
    S, E, A = Path("pool.parquet"), Path("evals"), Path("aux.parquet")
    f = b.common_flags(S, E, A)
    check(f == ["--input", str(S), "--aux-stats", str(A), "--eval-db", str(E)] + PINNED,
          "A: default argv is the pinned recipe")

    on = b.common_flags(S, E, A, eval_weight=1.0, gate_fallback="strict",
                        other_cov_blend=True)
    leg = b.common_flags(S, E, A, **b.LEGACY_RECIPE)
    check(not any(x in leg for x in ("--gate-fallback", "--other-cov-blend",
                                     "--select-margin")),
          "B: the legacy settings carry no opt-in flag (Stage 3's own defaults)")
    check(on[-3:] == ["--gate-fallback", "strict", "--other-cov-blend"]
          and "1" in on[on.index("--eval-weight") + 1],
          f"B: opt-ins appended when on ({on[-3:]})")
    check(b.LEGACY_RECIPE["eval_weight"] == 0.5
          and b.LEGACY_RECIPE["gate_fallback"] == "legacy"
          and not b.LEGACY_RECIPE["other_cov_blend"],
          "F: a meta without recipe keys is read as the pre-2026-10-17 recipe")

    h = b.recipe_hash(f)
    check(h == b.recipe_hash(b.common_flags(Path("x"), Path("y"), Path("z"))),
          "C: hash is path-independent")
    check(h != b.recipe_hash(on), "C: hash changes with the recipe")
    check(h != b.recipe_hash(f + ["--plan-prior", "p", "--learn-delta-rare", "0.04"]),
          "C: a learnability setting would be part of the recipe (paths excluded)")
    check(not hasattr(b, "learn_flags") and not hasattr(b, "LEARN"),
          "E: the driver is single-pass (learnability pass retired 2026-10-14)")

    st = b.common_flags(S, E, A, stick_margin=0.01)
    check(st[-2:] == ["--stick-margin", "0.01"] and "--stick-margin" not in f,
          "G: --stick-margin is passed only when on")
    check(b.recipe_hash(st + ["--incumbent", "a.parquet"])
          == b.recipe_hash(st + ["--incumbent", "b.parquet"]) != h,
          "G: the incumbent PATH is not part of the recipe hash; the margin is")

    ps = b.common_flags(S, E, A, prior_strength=1500)
    check(ps[-2:] == ["--prior-strength", "1500"] and "--prior-strength" not in f
          and b.LEGACY_RECIPE["prior_strength"] == 500.0,
          "H: --prior-strength is passed only when it differs from 500")

    rc = {**b.RECIPE_DEFAULTS, "force_moves": ["1.e4 c5: Nf3"],
          "avoid_moves": ["1.e4: e5", ": d4"]}
    check(b.constraint_flags(rc, "white") == ["--force-move", "1.e4 c5: Nf3",
                                              "--avoid-move", ": d4"]
          and b.constraint_flags(rc, "black") == ["--avoid-move", "1.e4: e5"]
          and b.common_flags(S, E, A, **rc) == f,
          "I: constraints route to their colour and stay out of the shared argv")

    import tempfile
    with tempfile.TemporaryDirectory() as td:
        can, other = Path(td) / "can", Path(td) / "other"
        can.mkdir()
        other.mkdir()
        ri = lambda d, inc=None, no=False, m=None: b.resolve_incumbent(d, inc, no, m, can)
        check(ri(can) == (None, 0.0), "J: no pair in the canonical dir -> fresh build")
        for t in ("white", "black"):
            (can / f"repertoire_pooled_{t}_sharp.parquet").touch()
        check(ri(can) == (can, b.STICK_MARGIN_CANONICAL) and b.STICK_MARGIN_CANONICAL == 0.01,
              "J: a canonical rebuild sticks to the pair there at 0.01 by default")
        check(ri(can, no=True) == (None, 0.0), "J: --no-incumbent gives a fresh build")
        check(ri(other) == (None, 0.0), "J: an A/B out-dir is fresh by default")
        check(ri(other, inc=str(can)) == (can, 0.01) and ri(can, m=0.02) == (can, 0.02),
              "J: an explicit incumbent / margin wins")
    check(b.RECIPE_DEFAULTS["stick_margin"] == 0.0,
          "J: RECIPE_DEFAULTS keeps 0 (the margin of a build without an incumbent)")

    nf = b.common_flags(S, E, None)
    check(nf[nf.index("--reply-shrink") + 1] == str(b.REPLY_SHRINK) == "1.0",
          "D: --no-aux restores reply-shrink 1.0")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
