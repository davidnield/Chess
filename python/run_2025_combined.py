"""
Vacation Crunch Pipeline — 2024 full-year + 2025 combined repertoires.

Processes:
  • All 12 months of 2024 into a single 2024 repertoire set
  • All 4 months of 2025 (Jan, Feb, Mar, Apr) into a single 2025 repertoire set

Designed to run unattended for 4-6 days.

Stages:
  1. Stage 1 for all months that don't already have output
     (2025 Jan/Apr already complete; 2025 Feb/Mar and all 12 of 2024 are new)
     Uses --file-level parallelism for better worker utilisation
  2. Create combined directories with NTFS hardlinks
  3. Stage 2 aggregation on each combined directory
  4. Stage 3 weight grid (fw=[0.00, 0.30] × ew=[0.00, 0.50, 1.00] × 2 perspectives)
  5. Quick sanity comparison vs individual months

Resumable: every stage has a skip gate; re-running picks up where it left off.

Usage:
    .venv\\Scripts\\python.exe python\\run_2025_combined.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

# Best-effort email notifications. notify.send() silently no-ops if no
# config is found, so the pipeline runs identically with or without setup.
sys.path.insert(0, str(Path(__file__).parent))
import notify  # noqa: E402

# ── stdout setup ────────────────────────────────────────────────────────────

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


class TeeWriter:
    """Write to both a file and the original stdout."""

    def __init__(self, logfile: Path):
        self.file = open(logfile, "a", encoding="utf-8")
        self.stdout = sys.stdout

    def write(self, text: str) -> int:
        self.stdout.write(text)
        self.file.write(text)
        self.file.flush()
        return len(text)

    def flush(self):
        self.stdout.flush()
        self.file.flush()


# ── constants ───────────────────────────────────────────────────────────────

PROJECT  = Path("C:/Users/David/Documents/Chess")
# pythonw.exe (windowless Windows-subsystem) so stage1/2/3 children
# don't pop up PowerShell windows. python.exe is console-subsystem and
# spawns a fresh console host when launched without one (which kills the
# pipeline if the user closes the window).  stdout/stderr are always
# redirected to log files in run(), so losing the console is fine.
PY       = str(PROJECT / ".venv/Scripts/pythonw.exe")
LOG_DIR  = PROJECT / "logs" / "2025_combined"
EVAL_DB  = Path("E:/chess/lichess_eval_db.parquet")
REP_DIR  = Path("E:/chess/repertoire")
STATS_DIR = Path("E:/chess/position-stats")

# ── 2025 (4 months) ────────────────────────────────────────────────────────

MONTHS_2025 = {
    1: Path("E:/chess/position-moves-2025-jan"),
    2: Path("E:/chess/position-moves-2025-feb"),
    3: Path("E:/chess/position-moves-2025-mar"),
    4: Path("E:/chess/position-moves-2025-apr"),
}
MONTHS_2025_NAMES = {1: "January", 2: "February", 3: "March", 4: "April"}

COMBINED_DIR_2025   = Path("E:/chess/position-moves-2025-all")
COMBINED_STATS_2025 = STATS_DIR / "position_stats_2025.parquet"

# Months that already have Stage 1 output (skip Stage 1 for these)
MONTHS_2025_EXISTING = {1, 4}
MONTHS_2025_NEW      = {2, 3}

# ── 2024 (12 months) ───────────────────────────────────────────────────────

MONTH_ABBREVS = {
    1: "jan", 2: "feb", 3: "mar", 4: "apr", 5: "may", 6: "jun",
    7: "jul", 8: "aug", 9: "sep", 10: "oct", 11: "nov", 12: "dec",
}

# October 2024 excluded: upstream PGN data has broken move numbering
# (starts with ...0. {white} 1. {black}). Will be re-added after upstream fix.
MONTHS_2024_SKIP = {10}

MONTHS_2024 = {
    m: Path(f"E:/chess/position-moves-2024-{MONTH_ABBREVS[m]}")
    for m in range(1, 13)
    if m not in MONTHS_2024_SKIP
}

COMBINED_DIR_2024   = Path("E:/chess/position-moves-2024-all")
COMBINED_STATS_2024 = STATS_DIR / "position_stats_2024.parquet"

# ── Stage 3 weight grid ────────────────────────────────────────────────────

FORCING_WEIGHTS = [0.00, 0.30]
EVAL_WEIGHTS    = [0.00, 0.50, 1.00]


# ── helpers ─────────────────────────────────────────────────────────────────

def notify_month_complete(year: int, month: int, month_name: str,
                          ok: bool, output_dir: Path,
                          duration_min: float) -> None:
    """Send an email summarizing one month's Stage 1 result. Best-effort."""
    if ok:
        files = list(output_dir.glob("*.parquet"))
        n = len(files)
        size_gb = sum(f.stat().st_size for f in files) / 1e9
        subject = f"[chess pipeline] OK: Stage 1 {year} {month_name} ({duration_min:.0f} min)"
        body = (
            f"Stage 1 complete for {year} {month_name}.\n\n"
            f"  Output dir:  {output_dir}\n"
            f"  Files:       {n}\n"
            f"  Size:        {size_gb:.1f} GB\n"
            f"  Duration:    {duration_min:.1f} min\n"
            f"  Finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
    else:
        subject = f"[chess pipeline] FAIL: Stage 1 {year} {month_name}"
        body = (
            f"Stage 1 FAILED for {year} {month_name}.\n\n"
            f"  Expected output dir: {output_dir}\n"
            f"  Duration before failure: {duration_min:.1f} min\n"
            f"  See logs in {LOG_DIR} for details.\n"
            f"  Pipeline has exited.\n"
        )
    notify.send(subject, body)


def notify_milestone(subject: str, body: str) -> None:
    """Send a major-milestone email (Stage 2/3 done, full pipeline done, fatal)."""
    notify.send(f"[chess pipeline] {subject}", body)


def run(name: str, cmd: list[str]) -> bool:
    """Run a command, logging stdout+stderr to LOG_DIR/{name}.log."""
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


SOURCE_ROOT = Path("D:/data/chess/standard-chess-games-compressed")


def count_source_files(year: int, month: int) -> int:
    """Count source parquet files for one (year, month) partition.

    Source layout: D:/data/chess/standard-chess-games-compressed/year=YYYY/month=M/event=*/*.parquet
    """
    src_dir = SOURCE_ROOT / f"year={year}" / f"month={month}"
    if not src_dir.exists():
        return 0
    return sum(1 for _ in src_dir.rglob("*.parquet"))


def check_stage1_complete(year: int, month: int, month_dir: Path) -> bool:
    """Check whether Stage 1 output for a month is complete.

    Handles both Stage 1 output modes used historically in this pipeline:
      • partition-level dispatch: one parquet per event family (≤6 files)
      • file-level dispatch (--file-level): one parquet per source file
        (40-50 files per recent-month source)

    Mode is detected from filename patterns — file-level outputs include a
    `_part-` or `_batch` suffix derived from the source filename, while
    partition-level outputs are just `year=Y_month=M_event=E.parquet`.

    This replaces the original "≥6 files" heuristic which mistakenly
    classified Jan 2024 as complete with only 23/49 files written.
    """
    if not month_dir.exists():
        return False
    files = list(month_dir.glob("*.parquet"))
    if not files:
        return False
    file_level = any(("_part-" in f.name or "_batch" in f.name) for f in files)
    if file_level:
        n_source = count_source_files(year, month)
        if n_source == 0:
            # Source no longer available — fall back to the old loose check
            return len(files) >= 6
        return len(files) >= n_source
    # Partition-level: one parquet per event family (typically 6 for 2020+)
    return len(files) >= 6


def run_stage1(year: int, month: int, output_dir: Path) -> bool:
    """Run Stage 1 for one month with file-level parallelism."""
    cmd = [
        PY, str(PROJECT / "python/stage1_run_all.py"),
        "--output", str(output_dir),
        "--year-range", str(year), str(year),
        "--months", str(month),
        "--max-ply", "30",
        "--file-level",
    ]
    return run(f"stage1_{year}_m{month:02d}", cmd)


def create_combined_dir(combined_dir: Path, month_dirs: dict[int, Path],
                        label: str) -> None:
    """Create a combined directory with hardlinks to all months' parquet files."""
    combined_dir.mkdir(parents=True, exist_ok=True)
    total_linked = 0
    total_skipped = 0

    for month in sorted(month_dirs):
        src_dir = month_dirs[month]
        if not src_dir.exists():
            print(f"  WARNING: {src_dir} does not exist — skipping")
            continue
        files = sorted(src_dir.glob("*.parquet"))
        for f in files:
            target = combined_dir / f.name
            if target.exists():
                total_skipped += 1
                continue
            try:
                os.link(str(f), str(target))
                total_linked += 1
            except OSError as e:
                print(f"  WARNING: Could not hardlink {f.name}: {e}")
                # Fallback: copy the file
                import shutil
                shutil.copy2(str(f), str(target))
                total_linked += 1

    all_files = list(combined_dir.glob("*.parquet"))
    print(f"  {label} combined dir: {total_linked} hardlinked, "
          f"{total_skipped} already existed")
    print(f"  Total files in {label} combined dir: {len(all_files)}")


def run_stage2(label: str, input_dir: Path, output_path: Path,
               min_games: int = 50) -> bool:
    """Run Stage 2 aggregation on a combined directory.

    min_games default raised from 10 to 50: with the larger 2024 dataset the
    long tail of <50-game positions is enormous (kills the final-pass hash
    table, slows child_hash computation by ~10x). Higher threshold means
    fewer marginal positions in the final repertoire but vastly better
    runtime for Stage 2 finalization and Stage 3 backwards induction.
    """
    cmd = [
        PY, str(PROJECT / "python/stage2_aggregate.py"),
        "--input", str(input_dir),
        "--output", str(output_path),
        "--keep-intermediates",
        "--min-games", str(min_games),
    ]
    return run(f"stage2_{label}", cmd)


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
        for fw in FORCING_WEIGHTS:
            for ew in EVAL_WEIGHTS:
                ok = run_stage3(dataset_tag, stats_path, perspective, fw, ew)
                if not ok:
                    all_ok = False
                    print(f"  WARNING: Stage 3 {dataset_tag} {perspective} "
                          f"fw={fw} ew={ew} failed — continuing with rest")
    return all_ok


def run_quick_comparison(dataset_tag: str, stats_path: Path,
                         comparison_files: list[tuple[str, Path]]) -> None:
    """Quick sanity comparison: combined repertoire vs individual months."""
    try:
        import polars as pl
    except ImportError:
        print("  polars not available — skipping comparison")
        return

    fw, ew = 0.00, 0.50
    vtag = f"f{int(fw*100):03d}_e{int(ew*100):03d}"

    combined_file = REP_DIR / f"repertoire_{dataset_tag}_white_{vtag}.parquet"
    if not combined_file.exists():
        print(f"  Skipping comparison — combined file missing: {combined_file.name}")
        return

    print(f"  Loading {dataset_tag} combined repertoire for comparison (white, {vtag})...")
    df_combined = pl.read_parquet(str(combined_file))

    for label, path in comparison_files:
        if not path.exists():
            print(f"  Skipping {label} — file missing: {path.name}")
            continue

        df_other = pl.read_parquet(str(path))

        # Join on position_hash
        joined = df_combined.join(df_other, on="position_hash", suffix="_b")

        # Filter to positions where both have a recommendation (our-turn)
        our_turn = joined.filter(
            pl.col("best_move").is_not_null() & pl.col("best_move_b").is_not_null()
        )
        if our_turn.height == 0:
            print(f"  Combined vs {label}: no shared our-turn positions")
            continue

        agree = our_turn.filter(
            pl.col("best_move") == pl.col("best_move_b")
        ).height
        pct = agree / our_turn.height * 100

        # Value correlation
        vals_a = our_turn["value"].to_list()
        vals_b = our_turn["value_b"].to_list()
        n = len(vals_a)
        ma = sum(vals_a) / n
        mb = sum(vals_b) / n
        cov = sum((a - ma) * (b - mb) for a, b in zip(vals_a, vals_b)) / n
        std_a = (sum((a - ma) ** 2 for a in vals_a) / n) ** 0.5
        std_b = (sum((b - mb) ** 2 for b in vals_b) / n) ** 0.5
        pearson = cov / (std_a * std_b) if std_a > 0 and std_b > 0 else 0.0

        print(f"  {dataset_tag} combined vs {label}: {agree:,}/{our_turn.height:,} "
              f"({pct:.1f}%) agreement, Pearson r={pearson:.4f} "
              f"on {joined.height:,} shared positions")


# ── main ────────────────────────────────────────────────────────────────────

def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REP_DIR.mkdir(parents=True, exist_ok=True)
    STATS_DIR.mkdir(parents=True, exist_ok=True)

    # Tee stdout to master log for post-vacation review
    sys.stdout = TeeWriter(LOG_DIR / "master.log")

    t_start = time.time()
    print(f"Vacation Crunch Pipeline starting at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Weight grid: forcing={FORCING_WEIGHTS} × eval={EVAL_WEIGHTS}")
    print(f"Perspectives: white, black")
    print(notify.status_string())
    print()
    notify_milestone(
        "Pipeline started",
        f"Vacation Crunch Pipeline started at {time.strftime('%Y-%m-%d %H:%M:%S')}.\n\n"
        f"Plan: 2025 combined (Feb+Mar new) → 2024 full year (11 months, Oct excluded).\n"
        f"You'll get an email after each Stage 1 month + at major milestones.\n"
    )

    # ════════════════════════════════════════════════════════════════════════
    #  PART A: 2025 COMBINED (4 months)
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n{'#'*70}")
    print(f"# PART A: 2025 COMBINED (Jan + Feb + Mar + Apr)")
    print(f"{'#'*70}")

    # ── Verify Jan and Apr Stage 1 are ready ────────────────────────────
    for month in sorted(MONTHS_2025_EXISTING):
        d = MONTHS_2025[month]
        if not check_stage1_complete(2025, month, d):
            print(f"FATAL: Stage 1 for 2025 {MONTHS_2025_NAMES[month]} is not complete!")
            print(f"  Output dir: {d}")
            sys.exit(1)
        n = len(list(d.glob("*.parquet")))
        print(f"  2025 {MONTHS_2025_NAMES[month]} Stage 1: {n} files OK")

    # ── Stage 1: Process Feb and Mar 2025 ───────────────────────────────
    for month in sorted(MONTHS_2025_NEW):
        name = MONTHS_2025_NAMES[month]
        d = MONTHS_2025[month]
        print(f"\n{'='*70}")
        print(f"  STAGE 1: 2025 {name} (file-level parallelism)")
        print(f"{'='*70}")

        if check_stage1_complete(2025, month, d):
            n = len(list(d.glob("*.parquet")))
            print(f"  Already complete ({n} files) — skipping")
            continue

        d.mkdir(parents=True, exist_ok=True)
        t_month = time.time()
        ok = run_stage1(2025, month, d)
        notify_month_complete(2025, month, name, ok, d,
                              (time.time() - t_month) / 60)
        if not ok:
            print(f"FATAL: Stage 1 (2025 {name}) failed")
            sys.exit(1)

    # ── Verify all 4 months of 2025 ────────────────────────────────────
    print(f"\n  Verifying all 2025 months...")
    total_files = 0
    for month in sorted(MONTHS_2025):
        d = MONTHS_2025[month]
        if not check_stage1_complete(2025, month, d):
            n_out = sum(1 for _ in d.glob("*.parquet"))
            n_src = count_source_files(2025, month)
            print(f"FATAL: Stage 1 for 2025 month {month} is incomplete! "
                  f"({n_out} output vs {n_src} source files)")
            sys.exit(1)
        n = len(list(d.glob("*.parquet")))
        total_files += n
        size_gb = sum(f.stat().st_size for f in d.glob("*.parquet")) / 1e9
        print(f"  2025 m{month:02d}: {n:>3} files, {size_gb:.1f} GB")
    print(f"  2025 total: {total_files} files")

    # ── Create combined directory (2025) ────────────────────────────────
    print(f"\n  Creating 2025 combined directory...")
    create_combined_dir(COMBINED_DIR_2025, MONTHS_2025, "2025")

    # ── Stage 2: 2025 combined ──────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  STAGE 2: 2025 combined aggregation")
    print(f"{'='*70}")

    if COMBINED_STATS_2025.exists():
        size_mb = COMBINED_STATS_2025.stat().st_size / 1e6
        print(f"  Stats file exists ({size_mb:.1f} MB) — skipping")
    else:
        t_s2 = time.time()
        if not run_stage2("2025_combined", COMBINED_DIR_2025, COMBINED_STATS_2025):
            notify_milestone("FAIL: Stage 2 (2025 combined)",
                             f"Stage 2 for the 2025 combined dataset failed after "
                             f"{(time.time()-t_s2)/60:.1f} min. Pipeline exiting.")
            print("FATAL: Stage 2 (2025 combined) failed")
            sys.exit(1)
        notify_milestone(
            "OK: Stage 2 done (2025 combined)",
            f"Stage 2 finished for 2025 combined in {(time.time()-t_s2)/60:.1f} min.\n"
            f"Stats file: {COMBINED_STATS_2025} "
            f"({COMBINED_STATS_2025.stat().st_size/1e6:.1f} MB)\n"
        )

    # ── Stage 3: 2025 combined ──────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  STAGE 3: 2025 combined repertoire grid")
    print(f"{'='*70}")
    t_s3 = time.time()
    run_stage3_grid("2025", str(COMBINED_STATS_2025))
    notify_milestone(
        "OK: Stage 3 grid done (2025 combined)",
        f"Stage 3 weight-grid finished for 2025 combined in "
        f"{(time.time()-t_s3)/60:.1f} min.\n"
        f"Repertoire files in: {REP_DIR}\n"
    )

    # ════════════════════════════════════════════════════════════════════════
    #  PART B: 2024 FULL YEAR (12 months)
    # ════════════════════════════════════════════════════════════════════════
    months_2024_sorted = sorted(MONTHS_2024.keys())
    print(f"\n{'#'*70}")
    print(f"# PART B: 2024 ({len(months_2024_sorted)} months, excluding Oct — broken PGN data)")
    print(f"{'#'*70}")

    # ── Stage 1: 2024 months ───────────────────────────────────────────
    for month in months_2024_sorted:
        d = MONTHS_2024[month]
        abbrev = MONTH_ABBREVS[month].capitalize()
        print(f"\n{'='*70}")
        print(f"  STAGE 1: 2024 {abbrev} (file-level parallelism)")
        print(f"{'='*70}")

        if check_stage1_complete(2024, month, d):
            n = len(list(d.glob("*.parquet")))
            print(f"  Already complete ({n} files) — skipping")
            continue

        d.mkdir(parents=True, exist_ok=True)
        t_month = time.time()
        ok = run_stage1(2024, month, d)
        notify_month_complete(2024, month, abbrev, ok, d,
                              (time.time() - t_month) / 60)
        if not ok:
            print(f"FATAL: Stage 1 (2024 {abbrev}) failed")
            sys.exit(1)

    # ── Verify all 2024 months ─────────────────────────────────────────
    print(f"\n  Verifying all 2024 months...")
    total_files = 0
    for month in months_2024_sorted:
        d = MONTHS_2024[month]
        if not check_stage1_complete(2024, month, d):
            n_out = sum(1 for _ in d.glob("*.parquet"))
            n_src = count_source_files(2024, month)
            print(f"FATAL: Stage 1 for 2024 month {month} is incomplete! "
                  f"({n_out} output vs {n_src} source files)")
            sys.exit(1)
        n = len(list(d.glob("*.parquet")))
        total_files += n
        size_gb = sum(f.stat().st_size for f in d.glob("*.parquet")) / 1e9
        print(f"  2024 m{month:02d}: {n:>3} files, {size_gb:.1f} GB")
    print(f"  2024 total: {total_files} files")

    # ── Create combined directory (2024) ────────────────────────────────
    print(f"\n  Creating 2024 combined directory...")
    create_combined_dir(COMBINED_DIR_2024, MONTHS_2024, "2024")

    # ── Stage 2: 2024 combined ──────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  STAGE 2: 2024 combined aggregation")
    print(f"{'='*70}")

    if COMBINED_STATS_2024.exists():
        size_mb = COMBINED_STATS_2024.stat().st_size / 1e6
        print(f"  Stats file exists ({size_mb:.1f} MB) — skipping")
    else:
        t_s2 = time.time()
        if not run_stage2("2024_combined", COMBINED_DIR_2024, COMBINED_STATS_2024):
            notify_milestone("FAIL: Stage 2 (2024 combined)",
                             f"Stage 2 for the 2024 combined dataset failed after "
                             f"{(time.time()-t_s2)/60:.1f} min. Pipeline exiting.")
            print("FATAL: Stage 2 (2024 combined) failed")
            sys.exit(1)
        notify_milestone(
            "OK: Stage 2 done (2024 combined)",
            f"Stage 2 finished for 2024 combined in {(time.time()-t_s2)/60:.1f} min.\n"
            f"Stats file: {COMBINED_STATS_2024} "
            f"({COMBINED_STATS_2024.stat().st_size/1e6:.1f} MB)\n"
        )

    # ── Stage 3: 2024 combined ──────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  STAGE 3: 2024 combined repertoire grid")
    print(f"{'='*70}")
    t_s3 = time.time()
    run_stage3_grid("2024", str(COMBINED_STATS_2024))
    notify_milestone(
        "OK: Stage 3 grid done (2024 combined)",
        f"Stage 3 weight-grid finished for 2024 combined in "
        f"{(time.time()-t_s3)/60:.1f} min.\n"
        f"Repertoire files in: {REP_DIR}\n"
    )

    # ════════════════════════════════════════════════════════════════════════
    #  COMPARISONS
    # ════════════════════════════════════════════════════════════════════════
    print(f"\n{'#'*70}")
    print(f"# COMPARISONS")
    print(f"{'#'*70}")

    vtag = "f000_e050"

    # 2025 combined vs Jan and Apr individually
    print(f"\n  2025 combined vs individual months:")
    run_quick_comparison("2025", COMBINED_STATS_2025, [
        ("Jan 2025", REP_DIR / f"repertoire_2025_jan_white_{vtag}.parquet"),
        ("Apr 2025", REP_DIR / f"repertoire_2025_apr_white_{vtag}.parquet"),
    ])

    # 2024 combined vs 2025 combined
    print(f"\n  Cross-year comparison:")
    run_quick_comparison("2024", COMBINED_STATS_2024, [
        ("2025 combined", REP_DIR / f"repertoire_2025_white_{vtag}.parquet"),
    ])

    # Also compare 2024 vs existing 2016
    run_quick_comparison("2024", COMBINED_STATS_2024, [
        ("2016", REP_DIR / f"repertoire_2016_white_{vtag}.parquet"),
    ])

    # ════════════════════════════════════════════════════════════════════════
    #  SUMMARY
    # ════════════════════════════════════════════════════════════════════════
    elapsed = time.time() - t_start
    print(f"\n{'#'*70}")
    print(f"# COMPLETE")
    print(f"# Total time: {elapsed/3600:.1f} hours")
    print(f"# Finished at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*70}")

    # List all repertoire files produced
    for tag, label in [("2025", "2025 combined"), ("2024", "2024 combined")]:
        rep_files = sorted(REP_DIR.glob(f"repertoire_{tag}_*.parquet"))
        # Exclude per-month files (they have month abbreviations like _jan_, _apr_)
        combined_files = [f for f in rep_files
                          if not any(f"_{abbr}_" in f.name
                                     for abbr in MONTH_ABBREVS.values())]
        print(f"\n{label} repertoire files ({len(combined_files)}):")
        for f in combined_files:
            print(f"  {f.name}  ({f.stat().st_size/1e6:.1f} MB)")

    all_rep = sorted(REP_DIR.glob("repertoire_*.parquet"))
    print(f"\nTotal repertoire files on disk: {len(all_rep)}")

    # Final summary email
    summary_lines = [
        f"Vacation Crunch Pipeline finished successfully.",
        f"",
        f"  Total runtime:   {elapsed/3600:.1f} hours",
        f"  Finished at:     {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"  Repertoire dir:  {REP_DIR}",
        f"  Total rep files: {len(all_rep)}",
    ]
    notify_milestone("DONE: Vacation pipeline complete", "\n".join(summary_lines))


if __name__ == "__main__":
    main()
