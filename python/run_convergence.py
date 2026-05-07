"""
Convergence analysis orchestrator.

Runs the full pipeline for multiple datasets and a grid of Stage 3 weights.
Designed to run unattended for 16+ hours on Windows.

Datasets:
  - April 2025 (Stage 1 → Stage 2 → Stage 3 grid)
  - January 2025 (Stage 1 → Stage 2 → Stage 3 grid)
  - 2016 (Stage 3 grid only — stats already exist)

Stage 3 weight grid:
  forcing_weight: 0.00, 0.30
  eval_weight:    0.00, 0.50, 1.00
  → 6 variants × 2 perspectives = 12 repertoire files per dataset

Usage:
    .venv/Scripts/python.exe python/run_convergence.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT   = Path("C:/Users/David/Documents/Chess")
PY        = str(PROJECT / ".venv/Scripts/python.exe")
LOG_DIR   = PROJECT / "logs" / "convergence"
EVAL_DB   = Path("E:/chess/lichess_eval_db.parquet")
REP_DIR   = Path("E:/chess/repertoire")

LOG_DIR.mkdir(parents=True, exist_ok=True)


def run(name: str, cmd: list[str]) -> bool:
    """Run a command, logging stdout+stderr to LOG_DIR/{name}.log. Returns True on success."""
    logfile = LOG_DIR / f"{name}.log"
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"  log: {logfile}")
    print(f"{'='*70}")
    t0 = time.time()

    with open(logfile, "w", encoding="utf-8") as f:
        f.write(f"=== {name} started at {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        f.flush()
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
        elapsed = time.time() - t0
        f.write(f"\n=== {name} finished at {time.strftime('%Y-%m-%d %H:%M:%S')} "
                f"(exit {result.returncode}, {elapsed/60:.1f} min) ===\n")

    status = "OK" if result.returncode == 0 else "FAILED"
    print(f"  {status}  ({elapsed/60:.1f} min)")
    return result.returncode == 0


def run_stage1(tag: str, output_dir: str, year: int, months: list[int]) -> bool:
    """Run Stage 1 for a specific year/month(s)."""
    cmd = [
        PY, str(PROJECT / "python/stage1_run_all.py"),
        "--output", output_dir,
        "--year-range", str(year), str(year),
        "--months", *[str(m) for m in months],
        "--max-ply", "30",
    ]
    return run(f"stage1_{tag}", cmd)


def run_stage2(tag: str, input_dir: str, output_path: str) -> bool:
    """Run Stage 2 aggregation."""
    cmd = [
        PY, str(PROJECT / "python/stage2_aggregate.py"),
        "--input", input_dir,
        "--output", output_path,
    ]
    return run(f"stage2_{tag}", cmd)


def run_stage3(dataset_tag: str, stats_path: str,
               perspective: str, fw: float, ew: float) -> bool:
    """Run one Stage 3 variant."""
    fsfx = f"f{int(fw*100):03d}"
    esfx = f"e{int(ew*100):03d}"
    vtag = f"{fsfx}_{esfx}"
    outfile = REP_DIR / f"repertoire_{dataset_tag}_{perspective}_{vtag}.parquet"

    if outfile.exists():
        print(f"  Skipping {dataset_tag}_{perspective}_{vtag} (exists)")
        return True

    cmd = [
        PY, str(PROJECT / "python/stage3_backwards_induction.py"),
        "--input", stats_path,
        "--output", str(outfile),
        "--perspective", perspective,
        "--forcing-weight", str(fw),
        "--eval-db", str(EVAL_DB),
        "--eval-weight", str(ew),
    ]
    return run(f"stage3_{dataset_tag}_{perspective}_{vtag}", cmd)


def run_stage3_grid(dataset_tag: str, stats_path: str) -> bool:
    """Run the full Stage 3 weight grid for a dataset."""
    all_ok = True
    for perspective in ["white", "black"]:
        for fw in [0.00, 0.30]:
            for ew in [0.00, 0.50, 1.00]:
                ok = run_stage3(dataset_tag, stats_path, perspective, fw, ew)
                if not ok:
                    all_ok = False
                    print(f"  WARNING: Stage 3 {dataset_tag} {perspective} "
                          f"fw={fw} ew={ew} failed — continuing with rest")
    return all_ok


def main():
    t_start = time.time()
    print(f"Convergence analysis starting at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Weight grid: forcing=[0.00, 0.30] × eval=[0.00, 0.50, 1.00]")
    print(f"Datasets: April 2025, January 2025, 2016")

    # ── April 2025 ──────────────────────────────────────────────────────
    apr_moves = "E:/chess/position-moves-2025-apr"
    apr_stats = "E:/chess/position-stats/position_stats_2025_apr.parquet"

    print(f"\n{'#'*70}")
    print(f"# APRIL 2025")
    print(f"{'#'*70}")

    if not Path(apr_stats).exists():
        # Check if Stage 1 output exists (may already be running/done)
        apr_dir = Path(apr_moves)
        expected_files = 6
        existing = list(apr_dir.glob("*.parquet")) if apr_dir.exists() else []

        if len(existing) < expected_files:
            print(f"\n  Stage 1: {len(existing)}/{expected_files} partitions exist, running...")
            if not run_stage1("2025_apr", apr_moves, 2025, [4]):
                print("FATAL: Stage 1 (Apr 2025) failed")
                sys.exit(1)
        else:
            print(f"\n  Stage 1: all {len(existing)} partitions exist, skipping")

        if not run_stage2("2025_apr", apr_moves, apr_stats):
            print("FATAL: Stage 2 (Apr 2025) failed")
            sys.exit(1)
    else:
        print(f"\n  Stats file exists: {apr_stats} — skipping Stage 1+2")

    run_stage3_grid("2025_apr", apr_stats)

    # ── January 2025 ────────────────────────────────────────────────────
    jan_moves = "E:/chess/position-moves-2025-jan"
    jan_stats = "E:/chess/position-stats/position_stats_2025_jan.parquet"

    print(f"\n{'#'*70}")
    print(f"# JANUARY 2025")
    print(f"{'#'*70}")

    if not Path(jan_stats).exists():
        jan_dir = Path(jan_moves)
        existing = list(jan_dir.glob("*.parquet")) if jan_dir.exists() else []

        if len(existing) < 6:
            print(f"\n  Stage 1: {len(existing)}/6 partitions exist, running...")
            if not run_stage1("2025_jan", jan_moves, 2025, [1]):
                print("FATAL: Stage 1 (Jan 2025) failed")
                sys.exit(1)
        else:
            print(f"\n  Stage 1: all {len(existing)} partitions exist, skipping")

        if not run_stage2("2025_jan", jan_moves, jan_stats):
            print("FATAL: Stage 2 (Jan 2025) failed")
            sys.exit(1)
    else:
        print(f"\n  Stats file exists: {jan_stats} — skipping Stage 1+2")

    run_stage3_grid("2025_jan", jan_stats)

    # ── 2016 (Stage 3 only) ────────────────────────────────────────────
    stats_2016 = "E:/chess/position-stats/position_stats_2016.parquet"

    print(f"\n{'#'*70}")
    print(f"# 2016 (Stage 3 grid only)")
    print(f"{'#'*70}")

    run_stage3_grid("2016", stats_2016)

    # ── Summary ─────────────────────────────────────────────────────────
    elapsed = time.time() - t_start
    print(f"\n{'#'*70}")
    print(f"# COMPLETE")
    print(f"# Total time: {elapsed/3600:.1f} hours")
    print(f"# Finished at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*70}")

    # List all generated repertoire files
    rep_files = sorted(REP_DIR.glob("repertoire_20*.parquet"))
    print(f"\nRepertoire files ({len(rep_files)}):")
    for f in rep_files:
        print(f"  {f.name}  ({f.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    main()
