"""Build the canonical SHARP repertoire pair (White + Black) — two-pass learnability
build over the winpos-crush + relative-eval-gate recipe (2026-07).

Recipe (single pooled slice, event='Pooled', elo_band=0):
  - eval-only objective:   --eval-weight 1.0 --require-eval   (pure engine eval at leaves;
                           the line truncates where the eval DB's coverage ends)
  - refutation gate:       --robustness-floor 0.1 --gate-metric eval   (gate on the engine
                           eval of the reached position)
  - relative gate:         --gate-rel-floor 0.1   (also reject a candidate that concedes
                           > 0.1 expected-score vs the BEST recorded/augmented candidate at
                           the node, even if it clears the absolute floor — stops booking
                           moves that squander an advantage because opponents usually
                           misplay them, e.g. 8...Nd4 giving back -2.1 -> +0.1 in the
                           Bxf7+ Italian trap line)
  - no traffic floor:      --min-move-games 0
  - crush (sharpness):     relative-propagated over the WINPOS histogram (mate/resignation
                           OR eval >= +300cp achieved — see CLAUDE.md's crush-metric
                           section), γ=0.99, imm-window 2,
                           --crush-weight 0.1 --crush-prior 5000 --crush-baseline zero
                           (zero baseline: a thin line earns NO crush until proven; winpos
                            rates sit on a different scale than the resignation-proxy
                            histogram, hence the lower weight)
  - no memorization cost:  --memo-weight 0
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

Prerequisites (defaults): position_stats_pooled_ge1800_2019_2025_brc.parquet
(build_pooled_stats.py --phase merge), crush_hist_relwin_pooled_ge1800_2019_2025_brc.parquet
(build_crush_winpos.py phase 2), unified_eval_db.parquet (build_fishnet_eval_db.py — cloud
eval DB unioned with the fishnet-evals dump). Override the inputs with
--input / --crush-db / --eval-db to build on a different dataset. A
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

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
PY = sys.executable
SD = Path("E:/chess/position-stats")
REP_DIR = Path("E:/chess/repertoire")
PLAN_DIR = REP_DIR / "_plan"          # pass-1 reps + plan-prior/reach exports
LOG_DIR = PROJECT / "logs" / "sharp_reps"

# Canonical inputs default to the combined 2019-2025 mean_elo>=1800 pooled build
# (build_pooled_stats.py) with the winpos crush histogram and the unified (cloud+fishnet)
# eval DB. Override with --input / --crush-db / --eval-db.
DEFAULT_STATS     = SD / "position_stats_pooled_ge1800_2019_2025_brc.parquet"
DEFAULT_CRUSH_REL = SD / "crush_hist_relwin_pooled_ge1800_2019_2025_brc.parquet"
DEFAULT_EVAL_DB   = Path("E:/chess/unified_eval_db.parquet")

# Crush selection weight. Surfaced as a constant because the explorer reads it back (via
# the .meta.json sidecar) to reconstruct its selection-key column — keep it in sync with
# the --crush-weight passed in common_flags().
CRUSH_WEIGHT = 0.1

# Learnability tiebreak calibration ("v3", adopted 2026-07): loose δ only at nodes that are
# both SHALLOW (within our first depth_horizon moves) and RARE (reach below reach_pivot);
# everything deep/off-walk stays at delta_main. Contexts rarer than ctx_pivot shrink toward
# the global habits. Recorded in the .meta.json for provenance.
LEARN = {"delta_main": 0.005, "delta_rare": 0.04, "reach_pivot": 0.02,
         "ctx_pivot": 0.05, "depth_horizon": 6}

REPS = [("white", ["--perspective", "white"]), ("black", ["--perspective", "black"])]


def common_flags(stats: Path, crush_db: Path, eval_db: Path) -> list[str]:
    """The locked sharp recipe (winpos + relative gate, 2026-07), parameterized by
    input paths."""
    return [
        "--input", str(stats),
        "--eval-db", str(eval_db), "--eval-weight", "1.0",
        "--require-eval",
        "--robustness-floor", "0.1", "--gate-metric", "eval",
        "--gate-rel-floor", "0.1",
        "--min-move-games", "0",
        "--crush-mode", "relative-propagated",
        "--crush-db", str(crush_db),
        "--crush-gamma", "0.99", "--crush-imm-window", "2",
        "--crush-weight", str(CRUSH_WEIGHT), "--crush-prior", "5000", "--crush-baseline", "zero",
        "--memo-weight", "0",
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


def write_meta(out: Path, stats: Path, crush_db: Path, eval_db: Path, tag: str) -> None:
    """Record how the rep was built so the explorer can recover the crush weight (and the
    inputs) without the user re-specifying --crush-weight."""
    prior, reach = plan_paths(tag)
    meta = {"crush_weight": CRUSH_WEIGHT, "crush_mode": "relative-propagated",
            "input": str(stats), "crush_db": str(crush_db), "eval_db": str(eval_db),
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
                stats: Path, crush_db: Path, eval_db: Path, force: bool) -> bool:
    """Pass-1 -> measure -> pass-2 chain for one color. Returns True on success.
    `rerun` cascades: once any step actually executes, every later step reruns too
    (its inputs just changed), regardless of its own output existing."""
    p1 = pass1_path(tag)
    prior, reach = plan_paths(tag)
    out = out_path(tag)
    stage3 = str(PROJECT / "python/stage3_backwards_induction.py")
    rerun = force

    # Step 1: pass-1 rep (recipe without the plan prior).
    if rerun or not p1.exists():
        if not run(f"sharp_{tag}_pass1",
                   [PY, stage3, "--output", str(p1)] + flags + extra):
            return False
        rerun = True
    else:
        print(f"  Skipping {tag} pass-1 (exists: {p1.name}).")

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
        write_meta(out, stats, crush_db, eval_db, tag)
    else:
        print(f"  Skipping {tag} pass-2 (exists: {out.name}). Use --force to rebuild.")
        if not meta_path(out).exists():
            write_meta(out, stats, crush_db, eval_db, tag)
    return True


def main() -> None:
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
                    help=f"Stockfish eval DB parquet (default: {DEFAULT_EVAL_DB.name}).")
    args = ap.parse_args()

    stats, crush_db, eval_db = Path(args.input), Path(args.crush_db), Path(args.eval_db)
    for p in (stats, crush_db, eval_db):
        if not p.exists():
            hint = ("  — build it with build_pooled_stats.py --phase merge, or pass "
                    "--input/--crush-db to point at the previous "
                    "position_stats_pooled_1650_1900_2200_brc dataset"
                    if p in (stats, crush_db) else "")
            sys.exit(f"FATAL: missing prerequisite {p}{hint}")
    REP_DIR.mkdir(parents=True, exist_ok=True)
    PLAN_DIR.mkdir(parents=True, exist_ok=True)
    flags = common_flags(stats, crush_db, eval_db)
    t_all = time.time()
    failures = []
    for tag, extra in REPS:
        if not build_color(tag, extra, flags, stats, crush_db, eval_db, args.force):
            failures.append(tag)
    print(f"\nDone in {(time.time()-t_all)/60:.1f} min.")
    if failures:
        sys.exit(f"FAILED: {', '.join(failures)}")
    print("Canonical sharp reps: " + ", ".join(out_path(t).name for t, _ in REPS))


if __name__ == "__main__":
    main()
