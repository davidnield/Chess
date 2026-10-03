"""Build the canonical SHARP repertoire pair (White + Black) — two-pass learnability
build over the winpos-crush + relative-eval-gate recipe (2026-07).

Recipe (single pooled slice, event='Pooled', elo_band=0):
  - eval/empirical blend:  --eval-weight 0.5 --require-eval   (leaf value blends engine eval
                           with the Beta-Binomial-shrunk empirical score; swept 2026-07 —
                           0.5 Pareto-dominated 1.0/0.75/0.25 and a dynamic n-weighted
                           variant on empirical pick quality, eval steering, AND ply-16
                           coverage simultaneously, both colors. The line still truncates
                           where the eval DB's coverage ends)
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
  - crush (sharpness):     relative-propagated over the WINPOS histogram (mate/resignation
                           OR eval >= +300cp achieved — see CLAUDE.md's crush-metric
                           section), γ=0.99, imm-window 2,
                           --crush-weight 0.1 --crush-prior 5000 --crush-baseline zero
                           (zero baseline: a thin line earns NO crush until proven; winpos
                            rates sit on a different scale than the resignation-proxy
                            histogram, hence the lower weight)
  - no memorization cost:  --memo-weight 0
  - reply-mass shrinkage:  --reply-shrink 1.0   (adopted 2026-08-03. An opponent node's
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
  - learnability tiebreak: TWO-PASS build (v3 calibration, LEARN below). Pass 1 builds the
                           recipe above; plan_consistency_report.py measures the idea-token
                           plan prior + per-node reach/context/depth from it; pass 2
                           rebuilds with --plan-prior/--plan-reach so near-equal candidates
                           (within a δ window of the best selection key) collapse onto the
                           most habitual idea. δ is loose only at SHALLOW-but-RARE nodes
                           (offbeat openings), tight on main lines and deep nodes; rare
                           contexts shrink toward the GLOBAL habits. Costs ~0.6-0.8%
                           effectiveness for the consistency gains (adopted 2026-07).

Only ONE White (no forced first move) + ONE Black are built — the explorer's candidate
table (gold/silver/bronze) lets you inspect the e4 / d4 / etc. subtrees without separate
forced-root reps.

Per color the chain is: pass-1 rep -> _plan/pass1_{tag}.parquet, plan exports ->
_plan/{tag}_pass1_{prior,reach}.parquet, final rep -> repertoire_pooled_{tag}_sharp.parquet.
Each step has its own existence skip gate; once a step actually runs, every later step in
the chain reruns too (its inputs changed). --force reruns the whole chain.

Prerequisites (defaults, the canonical recipe): position_stats_pooled_ge1800_2013_2026_brc.parquet
(build_pooled_stats.py --phase merge --no-prune) with its aux sidecar
position_stats_aux_pooled_ge1800_2013_2026_brc.parquet, the t300 winpos histogram
crush_hist_relwin_pooled_ge1800_2013_2026_brc_t300.parquet (the extract's fused winpos pass),
and the eval arrays D:/chess/eval_arrays_full (python/eval_arrays.py, from the explorer
book's eval DB D:/chess/eval_full). Override the inputs with --input / --aux-stats
(--no-aux for the pre-sidecar recipe) / --crush-db / --eval-db to build on a different dataset. A
<rep>.parquet.meta.json provenance sidecar is written next to each rep recording the
crush weight, learnability settings and inputs (the explorer reads it back).

Usage:
    .venv/Scripts/python.exe python/build_sharp_reps.py            # skip-gated, new pooled inputs
    .venv/Scripts/python.exe python/build_sharp_reps.py --force    # rebuild the whole chain
"""
from __future__ import annotations
import argparse
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
PLAN_DIR = REP_DIR / "_plan"          # pass-1 reps + plan-prior/reach exports
LOG_DIR = PROJECT / "logs" / "sharp_reps"

# Canonical inputs default to the combined 2013-2026 mean_elo>=1800 --no-prune pooled
# build (build_pooled_stats.py) with its aux sidecar, the t300 winpos crush histogram and
# the eval arrays of D:/chess/eval_full (eval_arrays.py). Override with --input /
# --aux-stats (--no-aux) / --crush-db / --eval-db.
DEFAULT_STATS     = SD / "position_stats_pooled_ge1800_2013_2026_brc.parquet"
DEFAULT_AUX       = SD / "position_stats_aux_pooled_ge1800_2013_2026_brc.parquet"
DEFAULT_CRUSH_REL = SD / "crush_hist_relwin_pooled_ge1800_2013_2026_brc_t300.parquet"
DEFAULT_EVAL_DB   = DEFAULT_ARRAY_DIR

# Crush selection weight. Surfaced as a constant because the explorer reads it back (via
# the .meta.json sidecar) to reconstruct its selection-key column — keep it in sync with
# the --crush-weight passed in common_flags().
CRUSH_WEIGHT = 0.1

# Reply-mass shrinkage strength (adopted 2026-08-03, see the recipe note above). 1.0 =
# assign the entire missing reply mass the node's own engine eval; 0.0 = the legacy
# renormalise-over-survivors behaviour. Recorded in .meta.json for provenance.
REPLY_SHRINK = 1.0

# Learnability tiebreak calibration ("v3", adopted 2026-07): loose δ only at nodes that are
# both SHALLOW (within our first depth_horizon moves) and RARE (reach below reach_pivot);
# everything deep/off-walk stays at delta_main. Contexts rarer than ctx_pivot shrink toward
# the global habits. Recorded in the .meta.json for provenance.
LEARN = {"delta_main": 0.005, "delta_rare": 0.04, "reach_pivot": 0.02,
         "ctx_pivot": 0.05, "depth_horizon": 6}

REPS = [("white", ["--perspective", "white"]), ("black", ["--perspective", "black"])]


def common_flags(stats: Path, crush_db: Path, eval_db: Path,
                 aux: Path | None = None) -> list[str]:
    """The locked sharp recipe (winpos + relative gate, 2026-07), parameterized by
    input paths.

    `aux` supplies the termination / other-moves / horizon sidecar. When present
    it also forces --reply-shrink to 0: both corrections cover overlapping
    missing mass, and the sidecar measures exactly what reply-shrink could only
    infer from coverage, so running them together shrinks the same games twice.
    """
    reply = 0.0 if aux else REPLY_SHRINK
    return [
        "--input", str(stats),
        *(["--aux-stats", str(aux)] if aux else []),
        "--eval-db", str(eval_db), "--eval-weight", "0.5",
        "--require-eval",
        "--robustness-floor", "0.1", "--gate-metric", "eval",
        "--gate-rel-floor", "0.1",
        "--gate-rel-baseline", "own-eval", "--gate-rel-own-margin", "0.02",
        "--min-move-games", "0",
        "--crush-mode", "relative-propagated",
        "--crush-db", str(crush_db),
        "--crush-gamma", "0.99", "--crush-imm-window", "2",
        "--crush-weight", str(CRUSH_WEIGHT), "--crush-prior", "5000", "--crush-baseline", "zero",
        "--memo-weight", "0",
        "--reply-shrink", str(reply),
    ]


def learn_flags(prior: Path, reach: Path) -> list[str]:
    """Pass-2 learnability flags (v3 calibration, LEARN). Passed explicitly — the blessed
    flag set lives here, not in stage3's defaults."""
    return [
        "--plan-prior", str(prior), "--plan-reach", str(reach),
        "--learn-delta-main", str(LEARN["delta_main"]),
        "--learn-delta-rare", str(LEARN["delta_rare"]),
        "--learn-reach-pivot", str(LEARN["reach_pivot"]),
        "--learn-ctx-pivot", str(LEARN["ctx_pivot"]),
        "--learn-depth-horizon", str(LEARN["depth_horizon"]),
    ]


def out_path(tag: str) -> Path:
    return REP_DIR / f"repertoire_pooled_{tag}_sharp.parquet"


def pass1_path(tag: str) -> Path:
    return PLAN_DIR / f"pass1_{tag}.parquet"


def plan_paths(tag: str) -> tuple[Path, Path]:
    prefix = PLAN_DIR / f"{tag}_pass1"
    return (prefix.with_name(prefix.name + "_prior.parquet"),
            prefix.with_name(prefix.name + "_reach.parquet"))


def meta_path(out: Path) -> Path:
    # Provenance sidecar: "<rep>.parquet.meta.json" (the explorer reads this convention).
    return out.with_name(out.name + ".meta.json")


def write_meta(out: Path, stats: Path, crush_db: Path, eval_db: Path, tag: str,
               aux: Path | None = None) -> None:
    """Record how the rep was built so the explorer can recover the crush weight (and the
    inputs) without the user re-specifying --crush-weight. `eval_source` fingerprints the
    eval DB (a parquet's size/mtime/rows, or an eval-arrays directory's meta and verify
    status) -- a path alone cannot tell two builds of the eval DB apart."""
    from eval_arrays import describe_eval_source
    prior, reach = plan_paths(tag)
    meta = {"crush_weight": CRUSH_WEIGHT, "crush_mode": "relative-propagated",
            "eval_weight": 0.5, "gate_rel_baseline": "own-eval",
            "reply_shrink": 0.0 if aux else REPLY_SHRINK,
            "aux_stats": str(aux) if aux else None,
            "input": str(stats), "crush_db": str(crush_db), "eval_db": str(eval_db),
            "eval_source": describe_eval_source(eval_db),
            "learnability": {**LEARN, "plan_prior": str(prior), "plan_reach": str(reach)},
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
                stats: Path, crush_db: Path, eval_db: Path, force: bool,
                aux: Path | None = None, pass1_only: bool = False) -> bool:
    """Pass-1 -> measure -> pass-2 chain for one color. Returns True on success.
    `rerun` cascades: once any step actually executes, every later step reruns too
    (its inputs just changed), regardless of its own output existing."""
    p1 = pass1_path(tag)
    prior, reach = plan_paths(tag)
    out = out_path(tag)
    stage3 = str(PROJECT / "python/stage3_backwards_induction.py")
    rerun = force

    # The skip gates test existence only, so a rep built from OTHER inputs (an older pool,
    # the retired eval DB) would be "skipped" and look current. Its meta records the
    # inputs: a mismatch refuses rather than reporting a stale rep as built.
    if not force and out.exists() and meta_path(out).exists():
        old = json.loads(meta_path(out).read_text(encoding="utf-8"))
        now = {"input": stats, "crush_db": crush_db, "eval_db": eval_db, "aux_stats": aux}
        drift = [k for k, v in now.items()
                 if (old.get(k) or None) != (str(v) if v is not None else None)]
        if drift:
            print(f"  REFUSING {tag}: {out.name} was built from different inputs "
                  f"({', '.join(f'{k}: {old.get(k)}' for k in drift)}). "
                  f"Rebuild with --force.", flush=True)
            return False

    # Step 1: pass-1 rep (recipe without the plan prior).
    if rerun or not p1.exists():
        if not run(f"sharp_{tag}_pass1",
                   [PY, stage3, "--output", str(p1)] + flags + extra):
            return False
        rerun = True
    else:
        print(f"  Skipping {tag} pass-1 (exists: {p1.name}).")
    if pass1_only:
        return True

    # Step 2: measure the plan prior + node reach/ctx/depth from the pass-1 rep.
    if rerun or not (prior.exists() and reach.exists()):
        if not run(f"plan_{tag}",
                   [PY, str(PROJECT / "python/plan_consistency_report.py"),
                    "--repertoire", str(p1), "--perspective", tag,
                    "--stats", str(stats),
                    "--export-prefix", str(PLAN_DIR / f"{tag}_pass1")]):
            return False
        rerun = True
    else:
        print(f"  Skipping {tag} plan export (exists: {prior.name}, {reach.name}).")

    # Step 3: pass-2 rep (recipe + learnability tiebreak) -> canonical output.
    if rerun or not out.exists():
        if not run(f"sharp_{tag}",
                   [PY, stage3, "--output", str(out)]
                   + flags + extra + learn_flags(prior, reach)):
            return False
        write_meta(out, stats, crush_db, eval_db, tag, aux)
    else:
        print(f"  Skipping {tag} pass-2 (exists: {out.name}). Use --force to rebuild.")
        if not meta_path(out).exists():
            write_meta(out, stats, crush_db, eval_db, tag, aux)
    return True


def main() -> None:
    global REP_DIR, PLAN_DIR, LOG_DIR
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true",
                    help="Rerun the whole pass-1 -> measure -> pass-2 chain even if "
                         "outputs exist.")
    ap.add_argument("--input", default=str(DEFAULT_STATS),
                    help=f"Pooled position-stats parquet (default: {DEFAULT_STATS.name}).")
    ap.add_argument("--crush-db", default=str(DEFAULT_CRUSH_REL),
                    help=f"Relative crush histogram parquet (default: {DEFAULT_CRUSH_REL.name}).")
    ap.add_argument("--eval-db", default=str(DEFAULT_EVAL_DB),
                    help=f"Stockfish eval source: a (position_hash, eval_cp) parquet or an "
                         f"eval-arrays directory from eval_arrays.py (default: {DEFAULT_EVAL_DB}).")
    ap.add_argument("--out-dir", default=str(REP_DIR),
                    help=f"Where the reps (and _plan/) go (default: {REP_DIR}, the canonical "
                         f"pair). A/B builds must point elsewhere; logs then go to "
                         f"logs/sharp_reps/<out-dir name>/.")
    ap.add_argument("--pass1-only", action="store_true",
                    help="Stop after each color's pass-1 rep (_plan/pass1_<color>.parquet): the "
                         "cheap no-op check against the canonical pass-1 reps.")
    ap.add_argument("--aux-stats", default=str(DEFAULT_AUX),
                    help="position_stats_aux_*.parquet sidecar. Adds the mass an "
                         "opponent node's outgoing edges cannot see (terminations, "
                         "the other-moves bucket, ply-cap horizon). Supplying it "
                         "forces --reply-shrink to 0, since the two corrections "
                         f"overlap (default: {DEFAULT_AUX.name}).")
    ap.add_argument("--no-aux", action="store_true",
                    help="Build without the aux sidecar: the pre-sidecar recipe, "
                         "with --reply-shrink restored.")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    if out_dir.resolve() != REP_DIR.resolve():
        LOG_DIR = LOG_DIR / out_dir.name
    REP_DIR, PLAN_DIR = out_dir, out_dir / "_plan"

    stats, crush_db, eval_db = Path(args.input), Path(args.crush_db), Path(args.eval_db)
    for p in (stats, crush_db, eval_db):
        if not p.exists():
            hint = ("  — build it with build_pooled_stats.py --phase merge, or pass "
                    "--input/--crush-db to point at another pool"
                    if p in (stats, crush_db) else
                    "  — build it with python/eval_arrays.py")
            sys.exit(f"FATAL: missing prerequisite {p}{hint}")
    REP_DIR.mkdir(parents=True, exist_ok=True)
    PLAN_DIR.mkdir(parents=True, exist_ok=True)
    aux = None if args.no_aux or not args.aux_stats else Path(args.aux_stats)
    if aux and not aux.exists():
        sys.exit(f"FATAL: --aux-stats not found: {aux}")
    flags = common_flags(stats, crush_db, eval_db, aux)
    if aux:
        print(f"Aux stats: {aux.name}  (reply-shrink forced to 0 — see common_flags)")
    t_all = time.time()
    failures = []
    for tag, extra in REPS:
        if not build_color(tag, extra, flags, stats, crush_db, eval_db, args.force, aux,
                           pass1_only=args.pass1_only):
            failures.append(tag)
    print(f"\nDone in {(time.time()-t_all)/60:.1f} min.")
    if failures:
        sys.exit(f"FAILED: {', '.join(failures)}")
    if args.pass1_only:
        print("Pass-1 reps: " + ", ".join(str(pass1_path(t)) for t, _ in REPS))
    else:
        print(f"Sharp reps in {REP_DIR}: " + ", ".join(out_path(t).name for t, _ in REPS))


if __name__ == "__main__":
    main()
