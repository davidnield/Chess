"""
Watchdog for the vacation pipeline orchestrator.

Designed to be run on a schedule (Windows Task Scheduler) every few hours.
Each invocation:
  1. Checks whether the orchestrator process is alive
  2. Checks whether the pipeline is already complete (skip everything)
  3. If incomplete AND no orchestrator running, relaunches it (resumable via
     internal skip gates)
  4. Emits a one-line status to stdout AND emails via notify.py
  5. Self-disables (deletes its own scheduled task) once complete

The pipeline output that signals completion is:
  - E:\\chess\\position-stats\\position_stats_2024.parquet exists, AND
  - All 12 E:\\chess\\repertoire\\repertoire_2024_*_f*_e*.parquet files exist
    (white/black × 6 fw/ew combinations)

Usage:
    .venv\\Scripts\\python.exe python\\pipeline_watchdog.py        # one-shot check
    .venv\\Scripts\\python.exe python\\pipeline_watchdog.py --dry-run

Exit codes:
    0  pipeline complete OR healthy
    1  fatal error in the watchdog itself
    2  pipeline was dead and got relaunched
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT      = Path("C:/Users/David/Documents/Chess")
# pythonw.exe (Windows-subsystem) instead of python.exe (Console-subsystem):
# the latter is launched windowless via CREATE_NO_WINDOW, but the venv shim
# re-execs the uv-managed real interpreter without that flag, so the
# grandchild gets a fresh Windows Terminal / PowerShell window that, if
# closed, takes the whole pipeline down via CTRL_CLOSE_EVENT. pythonw.exe
# is a GUI-subsystem app and cannot allocate a console at all.
PYTHON_EXE   = PROJECT / ".venv" / "Scripts" / "pythonw.exe"
ORCH_SCRIPT  = PROJECT / "python" / "run_2025_combined.py"
LOG_DIR      = PROJECT / "logs" / "2025_combined"
WATCHDOG_LOG = LOG_DIR / "watchdog.log"

# Completion gates
FINAL_STATS_2024 = Path("E:/chess/position-stats/position_stats_2024.parquet")
REP_DIR          = Path("E:/chess/repertoire")
REP_GLOB_2024    = "repertoire_2024_*_f*_e*.parquet"
EXPECTED_REP_CNT = 12  # 2 perspectives × 2 fw × 3 ew

# Allow notify import
sys.path.insert(0, str(PROJECT / "python"))


def log(msg: str) -> None:
    """Write to watchdog log file AND stdout."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(WATCHDOG_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def find_orchestrator_pid() -> int | None:
    """
    Find the PID of any running orchestrator.

    Uses WMIC-style query via PowerShell to match python.exe OR pythonw.exe
    processes whose command line includes `run_2025_combined.py`. Returns
    the PID of the first match, or None.

    NOTE: The filter MUST match both python.exe and pythonw.exe. We switched
    the launch path to pythonw.exe on 2026-05-25 (to avoid stray PowerShell
    windows being closed and killing the pipeline) but originally forgot to
    update this detection function — every watchdog tick then incorrectly
    reported "no orchestrator alive" and spawned a duplicate. Duplicate
    orchestrators race on Stage 2 tier-file writes (.tmp → .parquet
    renames) and produce FileNotFoundError crashes — observed
    2026-05-27 09:00 → 10:33 on slice_Blitz_elo1900 tier-3 boundary.
    """
    ps_cmd = [
        "powershell.exe",
        "-NoProfile",
        "-Command",
        "Get-CimInstance Win32_Process "
        "-Filter \"(Name='python.exe' OR Name='pythonw.exe')\" | "
        "Where-Object { $_.CommandLine -match 'run_2025_combined' } | "
        "Select-Object -First 1 -ExpandProperty ProcessId"
    ]
    try:
        result = subprocess.run(ps_cmd, capture_output=True, text=True, timeout=30)
        pid_str = result.stdout.strip()
        if pid_str and pid_str.isdigit():
            return int(pid_str)
    except Exception as e:
        log(f"WARN: failed to query process list: {e!r}")
    return None


def pipeline_complete() -> tuple[bool, str]:
    """
    Check whether the pipeline has reached the final completion gate.
    Returns (complete, reason).
    """
    if not FINAL_STATS_2024.exists():
        return False, f"missing {FINAL_STATS_2024.name}"
    rep_files = list(REP_DIR.glob(REP_GLOB_2024))
    if len(rep_files) < EXPECTED_REP_CNT:
        return False, (f"only {len(rep_files)}/{EXPECTED_REP_CNT} 2024 "
                       f"repertoire files written")
    return True, (f"position_stats_2024.parquet exists and all "
                  f"{EXPECTED_REP_CNT} repertoire files present")


def parse_master_log_tail() -> str:
    """Return the last non-blank line of master.log, or a notice if missing."""
    master = LOG_DIR / "master.log"
    if not master.exists():
        return "(master.log missing)"
    try:
        with open(master, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            chunk = min(size, 8192)
            f.seek(size - chunk)
            tail = f.read().decode("utf-8", errors="replace")
        lines = [ln.strip() for ln in tail.splitlines() if ln.strip()]
        return lines[-1] if lines else "(empty master.log)"
    except Exception as e:
        return f"(error reading master.log: {e!r})"


def count_2024_outputs() -> dict[str, int]:
    """Return a dict of {month_abbr: n_files} for 2024 stage 1 outputs."""
    out: dict[str, int] = {}
    for abbr in ("jan", "feb", "mar", "apr", "may", "jun",
                 "jul", "aug", "sep", "nov", "dec"):
        d = Path(f"E:/chess/position-moves-2024-{abbr}")
        if d.exists():
            out[abbr] = sum(1 for _ in d.glob("*.parquet"))
        else:
            out[abbr] = 0
    return out


def relaunch(dry_run: bool) -> bool:
    """Spawn a fresh detached orchestrator process. Returns True on success."""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    stderr_path = LOG_DIR / f"launch_stderr_watchdog_{ts}.log"

    if dry_run:
        log(f"DRY RUN: would launch '{PYTHON_EXE}' '{ORCH_SCRIPT}' "
            f"(stderr → {stderr_path})")
        return True

    # CREATE_NEW_PROCESS_GROUP + DETACHED_PROCESS so the orchestrator survives
    # this watchdog exiting (or the Task Scheduler session ending).
    # CREATE_NO_WINDOW additionally suppresses a console window for the WHOLE
    # process tree. Without it, the venv's python.exe launcher gets the
    # DETACHED_PROCESS flag but when it re-execs the uv-managed real
    # interpreter, that grandchild gets a fresh Windows Terminal / PowerShell
    # window. Closing that stray window sends CTRL_CLOSE_EVENT to the
    # process and kills the entire pipeline silently (no traceback, no
    # stderr — observed multiple times on 2026-05-25 when manually
    # relaunching during an interactive session).
    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    CREATE_NO_WINDOW = 0x08000000
    creationflags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(stderr_path, "w", encoding="utf-8") as ferr:
            proc = subprocess.Popen(
                [str(PYTHON_EXE), str(ORCH_SCRIPT)],
                cwd=str(PROJECT),
                stdout=subprocess.DEVNULL,
                stderr=ferr,
                close_fds=True,
                creationflags=creationflags,
            )
        log(f"Relaunched orchestrator: PID {proc.pid}, "
            f"stderr → {stderr_path.name}")
        return True
    except Exception as e:
        log(f"FAIL: could not spawn orchestrator: {e!r}")
        return False


def send_email_safely(subject: str, body: str) -> None:
    """Wrapper around notify.send that never raises."""
    try:
        import notify  # type: ignore[import-not-found]
        notify.send(subject, body)
    except Exception as e:
        log(f"WARN: notify.send failed: {e!r}")


def maybe_self_disable_task() -> None:
    """
    Remove our scheduled task so we don't keep running after completion.
    Only triggers when the pipeline is fully done.
    """
    task_name = "ChessPipelineWatchdog"
    try:
        result = subprocess.run(
            ["schtasks.exe", "/Delete", "/TN", task_name, "/F"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            log(f"Removed scheduled task '{task_name}' — watchdog is done.")
        else:
            # Probably wasn't registered, or already gone — fine
            log(f"(schtasks /Delete reported: {result.stdout.strip() or result.stderr.strip()})")
    except Exception as e:
        log(f"WARN: failed to remove scheduled task: {e!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
        help="Check state and report, but don't actually relaunch")
    parser.add_argument("--no-email", action="store_true",
        help="Skip the status email (still logs to file)")
    args = parser.parse_args()

    log("─" * 60)
    log("Watchdog tick")

    # 1) Check completion first
    done, reason = pipeline_complete()
    if done:
        log(f"PIPELINE COMPLETE: {reason}")
        if not args.no_email:
            send_email_safely(
                "[chess pipeline] Watchdog: pipeline complete",
                f"Watchdog detected the 2024 pipeline is fully done at "
                f"{datetime.now()}.\n\n{reason}\n\n"
                f"This watchdog is now self-disabling its scheduled task."
            )
        if not args.dry_run:
            maybe_self_disable_task()
        return 0

    # 2) Check whether orchestrator is alive
    pid = find_orchestrator_pid()
    last_log_line = parse_master_log_tail()
    month_counts = count_2024_outputs()
    months_with_data = sum(1 for c in month_counts.values() if c > 0)
    total_files = sum(month_counts.values())

    if pid is not None:
        msg = (f"healthy: PID {pid} alive | 2024 stage 1: "
               f"{months_with_data}/11 months started, {total_files} files | "
               f"master.log tail: {last_log_line[:100]}")
        log(msg)
        if not args.no_email:
            send_email_safely(
                "[chess pipeline] Watchdog OK — pipeline healthy",
                f"Watchdog check at {datetime.now()}:\n\n"
                f"  Orchestrator PID:   {pid} (alive)\n"
                f"  2024 stage 1 files: {total_files} across "
                f"{months_with_data}/11 months\n"
                f"  Per-month: {month_counts}\n"
                f"  master.log tail:    {last_log_line}\n"
            )
        return 0

    # 3) No orchestrator running and pipeline isn't done — relaunch
    log("Orchestrator NOT alive — relaunching")
    ok = relaunch(args.dry_run)
    if not args.no_email:
        if ok:
            send_email_safely(
                "[chess pipeline] Watchdog RESTARTED orchestrator",
                f"Watchdog at {datetime.now()} detected the orchestrator was "
                f"not running and relaunched it.\n\n"
                f"  2024 stage 1 files: {total_files} across "
                f"{months_with_data}/11 months\n"
                f"  Per-month: {month_counts}\n"
                f"  master.log tail:    {last_log_line}\n\n"
                f"Pipeline is resumable via internal skip gates — already-"
                f"completed work won't be redone."
            )
        else:
            send_email_safely(
                "[chess pipeline] Watchdog FAILED to relaunch",
                f"Watchdog at {datetime.now()} found no orchestrator AND "
                f"failed to relaunch it.\n\nCheck the watchdog log:\n"
                f"  {WATCHDOG_LOG}\n"
            )
    return 2 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
