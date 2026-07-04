"""Build the canonical SHARP repertoire pair (White + Black) — the winner of the
crush_baseline=zero K×W×λ sweep (2026-06).

Recipe (single pooled slice, event='Pooled', elo_band=0):
  - eval-only objective:   --eval-weight 1.0 --require-eval   (pure engine eval at leaves;
                           the line truncates where the eval DB's coverage ends)
  - refutation gate:       --robustness-floor 0.09 --gate-metric worst   (gate on value_worst:
                           our PREPARED book vs the opponent's best defence at every node, not
                           hypothetical best-play-by-both. Keeps sound gambits like the
                           Blackmar-Diemer / Danish; drops lines refuted by best defence)
  - no traffic floor:      --min-move-games 0
  - crush (sharpness):     relative-propagated, γ=0.9, imm-window 2,
                           --crush-weight 25 --crush-prior 5000 --crush-baseline zero
                           (zero baseline: a thin line earns NO crush until proven — fixes
                            the thin-sample 9.f4-over-9.Qg6+ artifact; K=5000 keeps the
                            well-sampled gambits)
  - memorization:          --memo-weight 15 --memo-leave-cost 0.10
                           (charges leaving book; trims expensive sidelines, keeps the
                            marquee gambits, lowers maintenance burden)

Only ONE White (no forced first move) + ONE Black are built — the explorer's candidate
table (gold/silver/bronze) lets you inspect the e4 / d4 / etc. subtrees without separate
forced-root reps.

Prerequisites (defaults): position_stats_pooled_ge1800_2019_2025_brc.parquet +
crush_hist_rel_pooled_ge1800_2019_2025_brc.parquet (build_pooled_stats.py --phase merge),
lichess_eval_db.parquet (build_lichess_eval_db.py). Override the inputs with
--input / --crush-db / --eval-db to build on a different dataset (e.g. the previous
_1650_1900_2200 pool). A <rep>.parquet.meta.json provenance sidecar is written next to
each rep recording the crush weight + inputs (the explorer reads it back).

Usage:
    .venv/Scripts/python.exe python/build_sharp_reps.py            # skip-gated, new pooled inputs
    .venv/Scripts/python.exe python/build_sharp_reps.py --force    # rebuild
    .venv/Scripts/python.exe python/build_sharp_reps.py \\
        --input E:/chess/position-stats/position_stats_pooled_1650_1900_2200_brc.parquet \\
        --crush-db E:/chess/position-stats/crush_hist_rel_pooled_1650_1900_2200_brc.parquet
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
LOG_DIR = PROJECT / "logs" / "sharp_reps"

# Canonical inputs default to the combined 2019-2025 mean_elo>=1800 pooled build
# (build_pooled_stats.py). Override with --input / --crush-db / --eval-db.
DEFAULT_STATS     = SD / "position_stats_pooled_ge1800_2019_2025_brc.parquet"
DEFAULT_CRUSH_REL = SD / "crush_hist_rel_pooled_ge1800_2019_2025_brc.parquet"
DEFAULT_EVAL_DB   = Path("E:/chess/lichess_eval_db.parquet")

# Crush selection weight. Surfaced as a constant because the explorer reads it back (via
# the .meta.json sidecar) to reconstruct its selection-key column — keep it in sync with
# the --crush-weight passed in common_flags().
CRUSH_WEIGHT = 25

REPS = [("white", ["--perspective", "white"]), ("black", ["--perspective", "black"])]


def common_flags(stats: Path, crush_db: Path, eval_db: Path) -> list[str]:
    """The locked sharp recipe (sweep winner, 2026-06), parameterized by input paths."""
    return [
        "--input", str(stats),
        "--eval-db", str(eval_db), "--eval-weight", "1.0",
        "--require-eval",
        "--robustness-floor", "0.09", "--gate-metric", "worst",
        "--min-move-games", "0",
        "--crush-mode", "relative-propagated",
        "--crush-db", str(crush_db),
        "--crush-gamma", "0.9", "--crush-imm-window", "2",
        "--crush-weight", str(CRUSH_WEIGHT), "--crush-prior", "5000", "--crush-baseline", "zero",
        "--memo-weight", "15", "--memo-leave-cost", "0.10",
    ]


def out_path(tag: str) -> Path:
    return REP_DIR / f"repertoire_pooled_{tag}_sharp.parquet"


def meta_path(out: Path) -> Path:
    # Provenance sidecar: "<rep>.parquet.meta.json" (the explorer reads this convention).
    return out.with_name(out.name + ".meta.json")


def write_meta(out: Path, stats: Path, crush_db: Path, eval_db: Path) -> None:
    """Record how the rep was built so the explorer can recover the crush weight (and the
    inputs) without the user re-specifying --crush-weight."""
    meta = {"crush_weight": CRUSH_WEIGHT, "crush_mode": "relative-propagated",
            "input": str(stats), "crush_db": str(crush_db), "eval_db": str(eval_db),
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


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true", help="Rebuild even if outputs exist.")
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
    flags = common_flags(stats, crush_db, eval_db)
    t_all = time.time()
    failures = []
    for tag, extra in REPS:
        out = out_path(tag)
        if out.exists() and not args.force:
            print(f"  Skipping {tag} (exists: {out.name}). Use --force to rebuild.")
            if not meta_path(out).exists():
                write_meta(out, stats, crush_db, eval_db)
            continue
        cmd = ([PY, str(PROJECT / "python/stage3_backwards_induction.py"),
                "--output", str(out)] + flags + extra)
        if run(f"sharp_{tag}", cmd):
            write_meta(out, stats, crush_db, eval_db)
        else:
            failures.append(tag)
    print(f"\nDone in {(time.time()-t_all)/60:.1f} min.")
    if failures:
        sys.exit(f"FAILED: {', '.join(failures)}")
    print("Canonical sharp reps: " + ", ".join(out_path(t).name for t, _ in REPS))


if __name__ == "__main__":
    main()
