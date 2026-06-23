"""
Build the current canonical repertoire set: the POOLED (1900+2200 x Blitz/Rapid/
Classical) slice with the crush@15 + robustness-gate Stage 3, for three forced
White first moves (1.e4 / 1.d4 / 1.Nf3) and a Black repertoire.

This is the single runnable definition of the "current" repertoire. It replaces
the ad-hoc command lines that previously lived only in logs/pool_final2/*.log.
The legacy orchestrator run_2025_combined.py builds the OLD forcing-weight grid
(no crush / no gate / per-(event,elo) slices) and is NOT this pipeline.

Pipeline (each step resumable via output skip-gate):

  position_stats_2024_2025.parquet + crush_hist_2024_2025.parquet
     │  build_combined_slice.py   (pool the 6 sets into ONE synthetic slice)
     ▼
  position_stats_pooled_1900_2200_brc.parquet
  crush_hist_pooled_1900_2200_brc.parquet
     │  stage3_backwards_induction.py  x4  (crush@15, robustness gate, eval blend)
     ▼
  repertoire_pooled_{white_e4,white_d4,white_nf3,black}_crush15.parquet

Prerequisites (built by earlier one-shot scripts, not re-run here):
  - position_stats_2024_2025.parquet   (merge_stats_2024_2025.py)
  - crush_hist_2024_2025.parquet       (extract_crush_per_game.py -> build_crush_stats.py)
  - lichess_eval_db.parquet            (build_lichess_eval_db.py)

Usage:
    .venv/Scripts/python.exe python/build_pooled_reps.py            # build missing
    .venv/Scripts/python.exe python/build_pooled_reps.py --force    # rebuild all reps
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT   = Path(__file__).resolve().parent.parent
PY        = sys.executable  # the venv python that launched us
STATS_DIR = Path("E:/chess/position-stats")
REP_DIR   = Path("E:/chess/repertoire")
LOG_DIR   = PROJECT / "logs" / "pool_reps"

# ── Inputs / outputs ────────────────────────────────────────────────────────
POOLED_STATS = STATS_DIR / "position_stats_pooled_1900_2200_brc.parquet"
POOLED_CRUSH = STATS_DIR / "crush_hist_pooled_1900_2200_brc.parquet"
EVAL_DB      = Path("E:/chess/lichess_eval_db.parquet")

# Stage 3 knobs shared by all four reps. Tuned values (see crush-metric-design
# memory / CLAUDE.md): crush_prior=20000 stops the small-sample spiral while
# keeping well-sampled gambits; robustness_floor=0.04 is the refutation gate.
COMMON_STAGE3 = [
    "--input",            str(POOLED_STATS),
    "--eval-db",          str(EVAL_DB),
    "--eval-weight",      "0.5",
    "--crush-db",         str(POOLED_CRUSH),
    "--crush-weight",     "10",
    "--crush-horizon",    "15",
    "--crush-prior",      "20000",
    "--robustness-floor", "0.04",
]

# (tag, perspective-and-force flags). Output = repertoire_pooled_<tag>_crush15.parquet
REPS: list[tuple[str, list[str]]] = [
    ("white_e4",  ["--perspective", "white", "--force-root-move", "e4"]),
    ("white_d4",  ["--perspective", "white", "--force-root-move", "d4"]),
    ("white_nf3", ["--perspective", "white", "--force-root-move", "Nf3"]),
    ("black",     ["--perspective", "black"]),
]


def run(name: str, cmd: list[str]) -> bool:
    """Run a child command, teeing a header/footer + exit status into a log file."""
    logfile = LOG_DIR / f"{name}.log"
    print(f"\n{'='*70}\n  {name}\n  log: {logfile}\n{'='*70}", flush=True)
    t0 = time.time()
    with open(logfile, "w", encoding="utf-8") as f:
        f.write(f"=== {name} @ {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        f.write("  " + " ".join(cmd) + "\n\n")
        f.flush()
        rc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT).returncode
        f.write(f"\n=== exit {rc}, {(time.time()-t0)/60:.1f} min ===\n")
    print(f"  {'OK' if rc == 0 else 'FAILED'}  ({(time.time()-t0)/60:.1f} min)", flush=True)
    return rc == 0


def rep_path(tag: str) -> Path:
    return REP_DIR / f"repertoire_pooled_{tag}_crush15.parquet"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true",
                    help="Rebuild reps even if their output already exists.")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REP_DIR.mkdir(parents=True, exist_ok=True)

    # Prereqs that this script does NOT build.
    for p in (STATS_DIR / "position_stats_2024_2025.parquet",
              STATS_DIR / "crush_hist_2024_2025.parquet", EVAL_DB):
        if not p.exists():
            sys.exit(f"FATAL: missing prerequisite {p}")

    t_all = time.time()

    # ── Step 1: pool the 6 sets into the single synthetic slice (skip-gated). ──
    print(f"\n{'#'*70}\n# Step 1: build pooled slice\n{'#'*70}")
    if POOLED_STATS.exists() and POOLED_CRUSH.exists():
        print(f"  Pooled inputs already present — skipping build_combined_slice.")
    elif not run("build_combined_slice",
                 [PY, str(PROJECT / "python/build_combined_slice.py")]):
        sys.exit("FATAL: build_combined_slice failed")
    for p in (POOLED_STATS, POOLED_CRUSH):
        if not p.exists():
            sys.exit(f"FATAL: pooled input not produced: {p}")

    # ── Step 2: the four Stage 3 reps. ───────────────────────────────────────
    print(f"\n{'#'*70}\n# Step 2: Stage 3 reps (crush@15 + robustness gate)\n{'#'*70}")
    failures = []
    for tag, extra in REPS:
        out = rep_path(tag)
        if out.exists() and not args.force:
            print(f"  Skipping {tag} (exists: {out.name}). Use --force to rebuild.")
            continue
        cmd = ([PY, str(PROJECT / "python/stage3_backwards_induction.py"),
                "--output", str(out)] + COMMON_STAGE3 + extra)
        if not run(f"stage3_{tag}", cmd):
            failures.append(tag)

    # ── Summary: recommended first move + start value per rep. ───────────────
    print(f"\n{'#'*70}\n# Done in {(time.time()-t_all)/60:.1f} min\n{'#'*70}")
    try:
        import polars as pl
        import chess, chess.polyglot
        INT64_MAX, RANGE = 2**63 - 1, 2**64
        b = chess.Board()
        h = chess.polyglot.zobrist_hash(b)
        start = h - RANGE if h > INT64_MAX else h
        for tag, _ in REPS:
            out = rep_path(tag)
            if not out.exists():
                print(f"  {tag:<10} (missing)"); continue
            r = (pl.read_parquet(out, columns=["position_hash", "best_move",
                                               "value", "value_robust"])
                 .filter(pl.col("position_hash") == start))
            if r.height:
                row = r.row(0, named=True)
                print(f"  {tag:<10} first={row['best_move'] or '-':<5} "
                      f"value={row['value']:.4f} robust={row['value_robust']:.4f}")
    except Exception as e:
        print(f"  (summary skipped: {e})")

    if failures:
        sys.exit(f"FAILED reps: {', '.join(failures)}")
    print("\nAll pooled reps present.")


if __name__ == "__main__":
    main()
