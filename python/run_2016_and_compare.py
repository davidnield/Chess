"""
Run Stage 3 grid for 2016 (using existing stats file) then run convergence
comparison between 2016 and April 2025.

Run while January 2025 Stage 1 is still in progress.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT  = Path("C:/Users/David/Documents/Chess")
PY       = str(PROJECT / ".venv/Scripts/python.exe")
LOG_DIR  = PROJECT / "logs" / "convergence"
EVAL_DB  = Path("E:/chess/lichess_eval_db.parquet")
REP_DIR  = Path("E:/chess/repertoire")
STATS_2016 = Path("E:/chess/position-stats/position_stats_2016.parquet")

LOG_DIR.mkdir(parents=True, exist_ok=True)
REP_DIR.mkdir(parents=True, exist_ok=True)


def run(name: str, cmd: list[str]) -> bool:
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


def run_stage3(perspective: str, fw: float, ew: float) -> bool:
    fsfx = f"f{int(fw*100):03d}"
    esfx = f"e{int(ew*100):03d}"
    vtag = f"{fsfx}_{esfx}"
    outfile = REP_DIR / f"repertoire_2016_{perspective}_{vtag}.parquet"

    if outfile.exists():
        print(f"  Skipping 2016_{perspective}_{vtag} (exists)")
        return True

    cmd = [
        PY, str(PROJECT / "python/stage3_backwards_induction.py"),
        "--input",           str(STATS_2016),
        "--output",          str(outfile),
        "--perspective",     perspective,
        "--forcing-weight",  str(fw),
        "--eval-db",         str(EVAL_DB),
        "--eval-weight",     str(ew),
    ]
    return run(f"stage3_2016_{perspective}_{vtag}", cmd)


def main():
    t0 = time.time()
    print(f"2016 Stage 3 grid + convergence comparison")
    print(f"Starting at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Stats file: {STATS_2016}")

    if not STATS_2016.exists():
        print(f"ERROR: Stats file not found: {STATS_2016}")
        sys.exit(1)

    # Run Stage 3 grid for 2016
    all_ok = True
    for perspective in ["white", "black"]:
        for fw in [0.00, 0.30]:
            for ew in [0.00, 0.50, 1.00]:
                ok = run_stage3(perspective, fw, ew)
                if not ok:
                    print(f"  WARNING: Stage 3 2016 {perspective} fw={fw} ew={ew} failed")
                    all_ok = False

    elapsed_stage3 = time.time() - t0
    print(f"\nStage 3 grid complete in {elapsed_stage3/60:.1f} min")

    # List what we produced
    new_files = sorted(REP_DIR.glob("repertoire_2016_*_f???_e???.parquet"))
    print(f"2016 repertoire files ({len(new_files)}):")
    for f in new_files:
        print(f"  {f.name}  ({f.stat().st_size/1e6:.1f} MB)")

    # Run convergence comparison (Apr 2025 vs 2016)
    report_path = Path("E:/chess/convergence_report_apr2025_vs_2016.txt")
    print(f"\nRunning convergence comparison -> {report_path}")
    ok = run(
        "convergence_compare_apr_2016",
        [
            PY, str(PROJECT / "python/convergence_compare.py"),
            "--repertoire-dir", str(REP_DIR),
            "--output", str(report_path),
        ],
    )
    if not ok:
        print("WARNING: Convergence comparison failed — check log for details")

    total = time.time() - t0
    print(f"\nAll done in {total/60:.1f} min")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
