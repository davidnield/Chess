"""Build the canonical SHARP repertoire pair (White + Black) -- one Stage-3 run per
colour over the eval-blend + relative-eval-gate recipe.

Recipe (single pooled slice, event='Pooled', elo_band=0):
  - eval/empirical blend:  --eval-weight 0.75 --require-eval   (leaf value blends engine
                           eval with the Beta-Binomial-shrunk empirical score. ADOPTED
                           2026-10-17 from an OUT-OF-SAMPLE holdout: arms 0.5 / 0.75 / 1.0
                           built on the 2013..2026-07 pool, replayed against 29.1M
                           2026-08..09 games per colour, judged as a symmetric A/B (no
                           status-quo threshold), one value for both colours. Faithful-game
                           score, the objective, tied 0.5 vs 0.75 (colour average 0.590 vs
                           0.588, ~0.7 SE) while 1.0 lost ~6 SE for White; 0.75 kept more
                           games in book in BOTH colours (faithful games +2.2% W / +3.2% B,
                           ply-16 survival +15% W / +5% B). White's faithful score falls
                           monotonically with the weight, so 0.75 is the cautious end. The
                           line still truncates where the eval DB's coverage ends.
                           Reports: E:/chess/repertoire/_holdout_2026_08_09_<colour>.txt)
  - refutation gate:       --robustness-floor 0.1 --gate-metric eval   (gate on the engine
                           eval of the reached position)
  - relative gate:         --gate-rel-floor 0.1 --gate-rel-baseline own-eval
                           --gate-rel-own-margin 0.02   (reject a candidate that concedes
                           > 0.1 expected-score vs the baseline, even if it clears the
                           absolute floor — stops booking moves that squander an advantage
                           because opponents usually misplay them, e.g. 8...Nd4 giving back
                           -2.1 -> +0.1 in the Bxf7+ Italian trap line. own-eval baseline
                           (2026-07): the baseline is raised to the node's OWN engine eval
                           minus the margin whenever that exceeds the best candidate — so a
                           node whose only recorded replies all concede too much vs the
                           position's true value gets gated even with no non-conceding
                           sibling, falling through to engine augmentation. Uniform A/B
                           improvement both colors — see .meta.json for the swept
                           alternative)
  - no traffic floor:      --min-move-games 0
  - no crush:              until 2026-10-02 the recipe added 0.1 x a relative-propagated
                           winpos crush potential (an early-win bonus). The owner retired
                           the idea; the histograms and Stage 3's crush code are gone.
  - no memorization cost:  --memo-weight 0
  - reply-mass shrinkage:  --reply-shrink 1.0 ONLY WITHOUT the aux sidecar (--no-aux).
                           The canonical recipe supplies the sidecar, which forces it to 0
                           (see common_flags). (Adopted 2026-08-03. An opponent node's
                           value is the mean over the replies that CLEAR the pool's
                           min_games, renormalised to 100% — so when the alternatives
                           fragment below the threshold the survivors get modelled as
                           certainties. Measured: after 1.e4 c5 2.Nf3 d6 3.c3 Nf6 4.Ng5
                           h6 5.Nf3, 124 games reached the node but only 5...Nxe4 (50
                           games, +294cp) survived, so a reply played ~40% of the time
                           carried the whole subtree and its leaf eval propagated three
                           plies undiluted — the line's value equalled that leaf to six
                           decimals, beating 4.Be2 despite Ng5 being 60cp worse. The fix
                           blends the mean toward the node's OWN engine eval by the
                           fraction of reply mass that is missing: coverage
                           c = surviving mass / mass that reached the node (clamped to
                           1.0 for transpositions), weight w = 1 - strength*(1-c). At
                           strength 1 this is exactly one pseudo-reply carrying the
                           missing games at the position's own eval. Symmetric — it
                           raises a node's value as readily as it lowers one. Moves
                           6.2%/6.3% of White's and 8.5%/10.7% of Black's pass-1/pass-2
                           decisions, ~90% of it decisive rather than re-broken ties)
  - no learnability pass:  RETIRED 2026-10-14. The build used to be two-pass: pass 1, then
                           plan_consistency_report.py measured an idea-token plan prior from
                           it, then pass 2 re-broke near-ties toward the most habitual idea.
                           The bootstrap stability run (2026-10-14, Poisson-redrawn pools)
                           showed pass 2 roughly DOUBLES noise sensitivity -- reach-weighted
                           flip rate 4.8-6.9% vs 3.0-4.6% for pass 1 -- because its prior is
                           measured from a noisy pass-1 book, and the owner had not found it
                           made the book easier to learn. One pass halves build time. Stage
                           3's --plan-prior/--plan-reach still exist; this driver no longer
                           passes them. plan_consistency_report.py remains as a report.

Only ONE White (no forced first move) + ONE Black are built — the explorer's candidate
table (gold/silver/bronze) lets you inspect the e4 / d4 / etc. subtrees without separate
forced-root reps.

Per colour: one Stage-3 run -> repertoire_pooled_{tag}_sharp.parquet, skip-gated on the
output existing with a matching meta (--force rebuilds). A rep left by the retired two-pass
driver (its meta has a "learnability" block) is refused rather than taken as current.

Prerequisites (defaults, the canonical recipe): position_stats_pooled_ge1800_2013_2026_brc.parquet
(build_pooled_stats.py --phase merge --no-prune) with its aux sidecar
position_stats_aux_pooled_ge1800_2013_2026_brc.parquet, and the eval arrays D:/chess/eval_arrays_full (python/eval_arrays.py, from the explorer
book's eval DB D:/chess/eval_full). Override the inputs with --input / --aux-stats
(--no-aux for the pre-sidecar recipe) / --eval-db to build on a different dataset. A
<rep>.parquet.meta.json provenance sidecar is written next to each rep recording the
inputs, the full Stage-3 flag list, a hash of the recipe and the git commit; the skip
gate refuses a rep whose recipe differs.

Recipe overrides for A/B builds, which must go to their own --out-dir: --eval-weight,
--gate-fallback legacy, --no-other-cov-blend (the pre-2026-10-17 behaviours), and
--select-margin (off; did not reduce noise flips). Rebuild stickiness (item 5,
2026-10-17): --incumbent-dir <dir of the previous pair> --stick-margin X keeps each
previous move unless a new one beats it by more than X.

Usage:
    .venv/Scripts/python.exe python/build_sharp_reps.py            # skip-gated, new pooled inputs
    .venv/Scripts/python.exe python/build_sharp_reps.py --force    # rebuild both reps
"""
from __future__ import annotations
import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

from eval_arrays import DEFAULT_ARRAY_DIR

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
PY = sys.executable
SD = Path("E:/chess/position-stats")
REP_DIR = Path("E:/chess/repertoire")
LOG_DIR = PROJECT / "logs" / "sharp_reps"

# Canonical inputs default to the combined 2013-2026 mean_elo>=1800 --no-prune pooled
# build (build_pooled_stats.py) with its aux sidecar and the eval arrays of
# D:/chess/eval_full (eval_arrays.py). Override with --input / --aux-stats (--no-aux) /
# --eval-db.
DEFAULT_STATS     = SD / "position_stats_pooled_ge1800_2013_2026_brc.parquet"
DEFAULT_AUX       = SD / "position_stats_aux_pooled_ge1800_2013_2026_brc.parquet"
DEFAULT_EVAL_DB   = DEFAULT_ARRAY_DIR

# Reply-mass shrinkage strength (adopted 2026-08-03, see the recipe note above). 1.0 =
# assign the entire missing reply mass the node's own engine eval; 0.0 = the legacy
# renormalise-over-survivors behaviour. Recorded in .meta.json for provenance.
REPLY_SHRINK = 1.0

REPS = [("white", ["--perspective", "white"]), ("black", ["--perspective", "black"])]


# The canonical recipe (adopted 2026-10-17): eval weight 0.75 from the out-of-sample
# holdout; the two item-2 correctness fixes on. --select-margin stays off: it did not
# reduce noise flips (a hard band only moves the coin flip to its edge).
RECIPE_DEFAULTS = {"eval_weight": 0.75, "gate_fallback": "strict", "other_cov_blend": True,
                   "select_margin": 0.0, "stick_margin": 0.0}
# What a meta written BEFORE a key existed was built with. The skip gate must read a
# missing key as these, never as today's RECIPE_DEFAULTS, or an old rep would pass for a
# current one the day a default changes.
LEGACY_RECIPE = {"eval_weight": 0.5, "gate_fallback": "legacy", "other_cov_blend": False,
                 "select_margin": 0.0, "stick_margin": 0.0}


def common_flags(stats: Path, eval_db: Path, aux: Path | None = None,
                 eval_weight: float = RECIPE_DEFAULTS["eval_weight"],
                 gate_fallback: str = RECIPE_DEFAULTS["gate_fallback"],
                 other_cov_blend: bool = RECIPE_DEFAULTS["other_cov_blend"],
                 select_margin: float = RECIPE_DEFAULTS["select_margin"],
                 stick_margin: float = RECIPE_DEFAULTS["stick_margin"]) -> list[str]:
    """The locked sharp recipe (eval blend + relative gate; no crush since 2026-10-02),
    parameterized by input paths.

    `aux` supplies the termination / other-moves / horizon sidecar. When present
    it also forces --reply-shrink to 0: both corrections cover overlapping
    missing mass, and the sidecar measures exactly what reply-shrink could only
    infer from coverage, so running them together shrinks the same games twice.
    """
    reply = 0.0 if aux else REPLY_SHRINK
    return [
        "--input", str(stats),
        *(["--aux-stats", str(aux)] if aux else []),
        "--eval-db", str(eval_db), "--eval-weight", f"{eval_weight:g}",
        "--require-eval",
        "--robustness-floor", "0.1", "--gate-metric", "eval",
        "--gate-rel-floor", "0.1",
        "--gate-rel-baseline", "own-eval", "--gate-rel-own-margin", "0.02",
        "--min-move-games", "0",
        "--memo-weight", "0",
        "--reply-shrink", str(reply),
        # opt-in fixes: appended only when on, so the default argv is byte-identical
        *(["--gate-fallback", gate_fallback] if gate_fallback != "legacy" else []),
        *(["--other-cov-blend"] if other_cov_blend else []),
        *(["--select-margin", f"{select_margin:g}"] if select_margin > 0 else []),
        # stickiness: the per-colour --incumbent path is added by main() (it is a path,
        # so it stays out of the recipe hash); the margin is part of the recipe.
        *(["--stick-margin", f"{stick_margin:g}"] if stick_margin > 0 else []),
    ]


_PATH_FLAGS = {"--input", "--aux-stats", "--eval-db", "--output",
               "--plan-prior", "--plan-reach", "--incumbent"}


def recipe_hash(flags: list[str]) -> str:
    """Hash of the recipe with input/output PATHS removed: two builds share a hash
    iff they ran Stage 3 with the same settings. Paths are compared separately
    (the input-drift check in build_color)."""
    out, skip = [], False
    for f in flags:
        if skip:
            skip = False
            continue
        if f in _PATH_FLAGS:
            skip = True
            continue
        out.append(f)
    return hashlib.sha256("\x1f".join(out).encode()).hexdigest()[:16]


def git_commit() -> str:
    # A pinned copy (D:/chess/bin/<name>/python, no .git) records its commit beside it.
    pinned = Path(__file__).resolve().parent / "PINNED_COMMIT"
    if pinned.exists():
        return pinned.read_text(encoding="utf-8").strip()
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                              check=True, cwd=PROJECT).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def out_path(tag: str) -> Path:
    return REP_DIR / f"repertoire_pooled_{tag}_sharp.parquet"


def meta_path(out: Path) -> Path:
    # Provenance sidecar: "<rep>.parquet.meta.json" (the explorer reads this convention).
    return out.with_name(out.name + ".meta.json")


def write_meta(out: Path, stats: Path, eval_db: Path, tag: str,
               aux: Path | None = None, recipe: dict | None = None,
               flags: list[str] | None = None) -> None:
    """Record how the rep was built: the inputs and the recipe settings the explorer and
    trainer read back. `eval_source` fingerprints the
    eval DB (a parquet's size/mtime/rows, or an eval-arrays directory's meta and verify
    status) -- a path alone cannot tell two builds of the eval DB apart."""
    from eval_arrays import describe_eval_source
    recipe = {**RECIPE_DEFAULTS, **(recipe or {})}
    meta = {**recipe, "gate_rel_baseline": "own-eval",
            "reply_shrink": 0.0 if aux else REPLY_SHRINK,
            "aux_stats": str(aux) if aux else None,
            "input": str(stats), "eval_db": str(eval_db),
            "eval_source": describe_eval_source(eval_db),
            "stage3_flags": flags,
            "recipe_hash": recipe_hash(flags) if flags else None,
            "git_commit": git_commit(),
            "built": time.strftime("%Y-%m-%d %H:%M:%S")}
    meta_path(out).write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run(name: str, cmd: list[str]) -> bool:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = LOG_DIR / f"{name}.log"
    t0 = time.time()
    with open(log, "w", encoding="utf-8") as f:
        f.write("  " + " ".join(cmd) + "\n\n"); f.flush()
        rc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT).returncode
        f.write(f"\n=== exit {rc}, {(time.time()-t0)/60:.1f} min ===\n")
    print(f"  {name}: {'OK' if rc == 0 else 'FAIL'} ({(time.time()-t0)/60:.1f} min)", flush=True)
    return rc == 0


def build_color(tag: str, extra: list[str], flags: list[str],
                stats: Path, eval_db: Path, force: bool,
                aux: Path | None = None, recipe: dict | None = None) -> bool:
    """One Stage-3 run for one colour. Returns True on success."""
    out = out_path(tag)
    stage3 = str(PROJECT / "python/stage3_backwards_induction.py")

    # The skip gate tests existence, so a rep built from OTHER inputs (an older pool, the
    # retired eval DB) or another recipe would be "skipped" and look current. Its meta
    # records them: a mismatch refuses rather than reporting a stale rep as built.
    if not force and out.exists() and meta_path(out).exists():
        old = json.loads(meta_path(out).read_text(encoding="utf-8"))
        # crush_db stays in the comparison: a rep built with the retired crush term
        # records one, so it is refused rather than mistaken for a current build.
        now = {"input": stats, "crush_db": None, "eval_db": eval_db, "aux_stats": aux}
        drift = [k for k, v in now.items()
                 if (old.get(k) or None) != (str(v) if v is not None else None)]
        # Recipe drift: a missing key means the rep predates it (LEGACY_RECIPE).
        want = {**RECIPE_DEFAULTS, **(recipe or {})}
        drift += [k for k, v in want.items()
                  if old.get(k, LEGACY_RECIPE[k]) != v]
        # The retired two-pass driver's output: same flags plus a learnability pass.
        if old.get("learnability"):
            drift.append("learnability")
        if drift:
            print(f"  REFUSING {tag}: {out.name} was built differently "
                  f"({', '.join(f'{k}: {old.get(k)}' for k in drift)}). "
                  f"Rebuild with --force.", flush=True)
            return False

    if force or not out.exists():
        cmd_flags = flags + extra
        if not run(f"sharp_{tag}", [PY, stage3, "--output", str(out)] + cmd_flags):
            return False
        write_meta(out, stats, eval_db, tag, aux, recipe, cmd_flags)
    else:
        print(f"  Skipping {tag} (exists: {out.name}). Use --force to rebuild.")
        if not meta_path(out).exists():
            write_meta(out, stats, eval_db, tag, aux, recipe, flags + extra)
    return True


def main() -> None:
    global REP_DIR, LOG_DIR
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true",
                    help="Rebuild both reps even if they exist.")
    ap.add_argument("--input", default=str(DEFAULT_STATS),
                    help=f"Pooled position-stats parquet (default: {DEFAULT_STATS.name}).")
    ap.add_argument("--eval-db", default=str(DEFAULT_EVAL_DB),
                    help=f"Stockfish eval source: a (position_hash, eval_cp) parquet or an "
                         f"eval-arrays directory from eval_arrays.py (default: {DEFAULT_EVAL_DB}).")
    ap.add_argument("--out-dir", default=str(REP_DIR),
                    help=f"Where the reps go (default: {REP_DIR}, the canonical "
                         f"pair). A/B builds must point elsewhere; logs then go to "
                         f"logs/sharp_reps/<out-dir name>/.")
    ap.add_argument("--aux-stats", default=str(DEFAULT_AUX),
                    help="position_stats_aux_*.parquet sidecar. Adds the mass an "
                         "opponent node's outgoing edges cannot see (terminations, "
                         "the other-moves bucket, ply-cap horizon). Supplying it "
                         "forces --reply-shrink to 0, since the two corrections "
                         f"overlap (default: {DEFAULT_AUX.name}).")
    ap.add_argument("--no-aux", action="store_true",
                    help="Build without the aux sidecar: the pre-sidecar recipe, "
                         "with --reply-shrink restored.")
    ap.add_argument("--eval-weight", type=float, default=RECIPE_DEFAULTS["eval_weight"],
                    help="Leaf eval/empirical blend weight (default 0.75, adopted "
                         "2026-10-17 from the 2026-08..09 holdout). Recorded in the meta.")
    ap.add_argument("--gate-fallback", choices=["legacy", "strict"],
                    default=RECIPE_DEFAULTS["gate_fallback"],
                    help="Stage 3 --gate-fallback (default strict since 2026-10-17). "
                         "strict never books a move that failed the absolute gate when a "
                         "passing one exists, and books the least-losing move by engine "
                         "eval otherwise; legacy is the pre-fix behaviour, for A/B arms.")
    ap.add_argument("--other-cov-blend", action=argparse.BooleanOptionalAction,
                    default=RECIPE_DEFAULTS["other_cov_blend"],
                    help="Stage 3 --other-cov-blend: blend the other-moves bucket's eval "
                         "only over its eval-covered share (default ON since 2026-10-17; "
                         "--no-other-cov-blend for the legacy rule).")
    ap.add_argument("--incumbent-dir", default=None,
                    help="Rebuild stickiness: a directory holding the PREVIOUS pair "
                         "(repertoire_pooled_<colour>_sharp.parquet). Each colour keeps the "
                         "previous book's move unless the best candidate beats it by more "
                         "than --stick-margin. Default: none (a fresh build).")
    ap.add_argument("--stick-margin", type=float, default=RECIPE_DEFAULTS["stick_margin"],
                    help="Key advantage a new move needs to replace the incumbent's "
                         "(expected-score units). Needs --incumbent-dir. Default 0.")
    ap.add_argument("--select-margin", type=float, default=RECIPE_DEFAULTS["select_margin"],
                    help="Stage 3 --select-margin: near-ties within this many expected-"
                         "score units go to the most-played move (default 0 = off).")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    if out_dir.resolve() != REP_DIR.resolve():
        LOG_DIR = LOG_DIR / out_dir.name
    REP_DIR = out_dir

    stats, eval_db = Path(args.input), Path(args.eval_db)
    for p in (stats, eval_db):
        if not p.exists():
            hint = ("  — build it with build_pooled_stats.py --phase merge, or pass "
                    "--input to point at another pool"
                    if p == stats else
                    "  — build it with python/eval_arrays.py")
            sys.exit(f"FATAL: missing prerequisite {p}{hint}")
    REP_DIR.mkdir(parents=True, exist_ok=True)
    aux = None if args.no_aux or not args.aux_stats else Path(args.aux_stats)
    if aux and not aux.exists():
        sys.exit(f"FATAL: --aux-stats not found: {aux}")
    recipe = {"eval_weight": args.eval_weight, "gate_fallback": args.gate_fallback,
              "other_cov_blend": bool(args.other_cov_blend),
              "select_margin": args.select_margin,
              "stick_margin": args.stick_margin}
    flags = common_flags(stats, eval_db, aux, **recipe)
    inc_dir = Path(args.incumbent_dir) if args.incumbent_dir else None
    if args.stick_margin > 0 and inc_dir is None:
        sys.exit("FATAL: --stick-margin needs --incumbent-dir")
    if inc_dir is not None:
        for tag, _ in REPS:
            ip = inc_dir / f"repertoire_pooled_{tag}_sharp.parquet"
            if not ip.exists():
                sys.exit(f"FATAL: incumbent not found: {ip}")
        print(f"Incumbent: {inc_dir}  (stick margin {args.stick_margin})")
    off = {k: v for k, v in recipe.items() if v != RECIPE_DEFAULTS[k]}
    if off:
        print(f"Recipe overrides: {off}")
        if out_dir.resolve() == Path("E:/chess/repertoire").resolve():
            sys.exit("FATAL: recipe overrides must not write the canonical pair; "
                     "pass --out-dir.")
    if aux:
        print(f"Aux stats: {aux.name}  (reply-shrink forced to 0 — see common_flags)")
    t_all = time.time()
    failures = []
    for tag, extra in REPS:
        if inc_dir is not None:
            extra = extra + ["--incumbent",
                             str(inc_dir / f"repertoire_pooled_{tag}_sharp.parquet")]
        if not build_color(tag, extra, flags, stats, eval_db, args.force, aux,
                           recipe=recipe):
            failures.append(tag)
    print(f"\nDone in {(time.time()-t_all)/60:.1f} min.")
    if failures:
        sys.exit(f"FAILED: {', '.join(failures)}")
    print(f"Sharp reps in {REP_DIR}: " + ", ".join(out_path(t).name for t, _ in REPS))


if __name__ == "__main__":
    main()
