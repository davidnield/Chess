"""
Stage 3 grid on the combined 2024+2025 stats file.

Variants: e=0.50 fixed; f in {0.00, 0.10, 0.20, 0.30}; perspectives {white, black}
= 8 repertoires, named repertoire_2024_2025_<persp>_f<fw>_e050.parquet.

Resumable (skips existing outputs). Logs to logs/combined_2425/.
Designed to be launched detached + windowless (pythonw) so it survives the
session; stdout/stderr go to per-variant log files.

Usage: python run_combined_2425_grid.py
"""
from __future__ import annotations
import subprocess
import sys
import time
from pathlib import Path
sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path("C:/Users/David/Documents/Chess")
PY = str(PROJECT / ".venv/Scripts/pythonw.exe")
STAGE3 = str(PROJECT / "python/stage3_backwards_induction.py")
STATS = "E:/chess/position-stats/position_stats_2024_2025.parquet"
EVAL_DB = "E:/chess/lichess_eval_db.parquet"
REP_DIR = Path("E:/chess/repertoire")
LOG_DIR = PROJECT / "logs" / "combined_2425"

FW = [0.00, 0.10, 0.20, 0.30]
EW = 0.50
PERSP = ["white", "black"]


def run(name, cmd):
    logfile = LOG_DIR / f"{name}.log"
    print(f"\n{'='*70}\n  {name}\n  log: {logfile}\n{'='*70}", flush=True)
    t0 = time.time()
    with open(logfile, "w", encoding="utf-8") as f:
        f.write(f"=== {name} started {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        f.flush()
        r = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
        f.write(f"\n=== {name} finished {time.strftime('%Y-%m-%d %H:%M:%S')} "
                f"(exit {r.returncode}, {(time.time()-t0)/60:.1f} min) ===\n")
    print(f"  {'OK' if r.returncode==0 else 'FAILED'} ({(time.time()-t0)/60:.1f} min)",
          flush=True)
    return r.returncode == 0


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if not Path(STATS).exists():
        print(f"FATAL: combined stats not found: {STATS}")
        sys.exit(1)
    REP_DIR.mkdir(parents=True, exist_ok=True)
    t_all = time.time()
    ok_all = True
    for persp in PERSP:
        for fw in FW:
            ftag = f"f{int(fw*100):03d}"
            etag = f"e{int(EW*100):03d}"
            out = REP_DIR / f"repertoire_2024_2025_{persp}_{ftag}_{etag}.parquet"
            name = f"stage3_2024_2025_{persp}_{ftag}_{etag}"
            if out.exists():
                print(f"  SKIP {name} (exists)", flush=True)
                continue
            cmd = [PY, STAGE3, "--input", STATS, "--output", str(out),
                   "--perspective", persp,
                   "--forcing-weight", str(fw),
                   "--eval-db", EVAL_DB, "--eval-weight", str(EW)]
            if not run(name, cmd):
                ok_all = False
                print(f"  WARNING: {name} failed — continuing", flush=True)
    print(f"\nGRID DONE in {(time.time()-t_all)/60:.1f} min. all_ok={ok_all}",
          flush=True)


if __name__ == "__main__":
    main()
