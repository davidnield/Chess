"""
Stage 1 parallel runner.

Discovers every year=*/month=*/event=* partition under SOURCE_ROOT and processes
them concurrently with multiprocessing. Skips partitions whose output already
exists (resume-friendly).

Usage:
    .venv/Scripts/python.exe python/stage1_run_all.py
    .venv/Scripts/python.exe python/stage1_run_all.py --workers 8 --max-ply 24
    .venv/Scripts/python.exe python/stage1_run_all.py --year-range 2020 2025
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import sys
import time
import traceback
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from stage1_extract_positions import (
    SOURCE_ROOT,
    DEFAULT_OUTPUT,
    process_partition,
    stable_output_name,
)


def discover_partitions(source_root: Path,
                        year_range: tuple[int, int] | None = None,
                        events: list[str] | None = None) -> list[Path]:
    """Find all year=*/month=*/event=*/ partition directories, sorted."""
    out = []
    for year_dir in sorted(source_root.glob("year=*")):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (ValueError, IndexError):
            continue
        if year_range and not (year_range[0] <= year <= year_range[1]):
            continue
        for month_dir in sorted(year_dir.glob("month=*")):
            for event_dir in sorted(month_dir.glob("event=*")):
                if not event_dir.is_dir():
                    continue
                if events:
                    event = event_dir.name.split("=", 1)[1]
                    if event not in events:
                        continue
                out.append(event_dir)
    return out


def _worker(task):
    """Top-level for picklability. Returns stats dict (with 'error' on failure)."""
    src, output_dir, max_ply, limit_games, overwrite = task
    try:
        return process_partition(
            src=src,
            output_dir=output_dir,
            max_ply=max_ply,
            limit_games=limit_games,
            overwrite=overwrite,
        )
    except Exception as e:
        return {
            "src": str(src),
            "output_path": "",
            "n_games": 0,
            "n_edges": 0,
            "n_failed": 0,
            "elapsed_sec": 0.0,
            "skipped": False,
            "error": f"{type(e).__name__}: {e}",
            "traceback": traceback.format_exc(),
        }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", default=str(SOURCE_ROOT),
                        help=f"Source root containing year=*/month=*/event=* partitions "
                             f"(default: {SOURCE_ROOT})")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT),
                        help=f"Output directory for edge tables (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--max-ply", type=int, default=30)
    parser.add_argument("--limit-games", type=int, default=None,
                        help="Per-partition game limit (useful for smoke tests)")
    parser.add_argument("--workers", type=int, default=max(1, mp.cpu_count() - 1),
                        help=f"Worker processes (default: cpu_count - 1 = {max(1, mp.cpu_count() - 1)})")
    parser.add_argument("--overwrite", action="store_true",
                        help="Reprocess partitions even if output exists")
    parser.add_argument("--year-range", type=int, nargs=2, metavar=("START", "END"),
                        help="Inclusive year range filter, e.g. --year-range 2020 2025")
    parser.add_argument("--events", nargs="+",
                        help="Restrict to specific events (e.g. --events Blitz Rapid)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List partitions that would be processed and exit")
    args = parser.parse_args()

    src_root = Path(args.source)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    year_range = tuple(args.year_range) if args.year_range else None
    partitions = discover_partitions(src_root, year_range=year_range, events=args.events)
    print(f"Discovered {len(partitions)} partitions matching filters.")

    if not args.overwrite:
        before = len(partitions)
        partitions = [p for p in partitions if not (out_dir / stable_output_name(p)).exists()]
        skipped = before - len(partitions)
        if skipped:
            print(f"Skipping {skipped} already-processed; {len(partitions)} remain.")

    if not partitions:
        print("Nothing to process.")
        return

    if args.dry_run:
        print("\nDry run -- would process:")
        for p in partitions:
            print(f"  {p}")
        return

    tasks = [(p, out_dir, args.max_ply, args.limit_games, args.overwrite) for p in partitions]
    print(f"Launching {args.workers} workers on {len(tasks)} partitions...\n")

    t0 = time.time()
    completed = 0
    total_games = 0
    total_edges = 0
    total_failed = 0
    errors = []

    with mp.Pool(args.workers) as pool:
        for stats in pool.imap_unordered(_worker, tasks):
            completed += 1
            elapsed_total = time.time() - t0

            if stats.get("error"):
                errors.append(stats)
                print(f"[{completed}/{len(tasks)}] ERROR in {stats['src']}: {stats['error']}")
                continue

            if stats["skipped"]:
                print(f"[{completed}/{len(tasks)}] SKIPPED (exists): {stats['src']}")
                continue

            total_games += stats["n_games"]
            total_edges += stats["n_edges"]
            total_failed += stats["n_failed"]

            agg_rate = total_games / elapsed_total if elapsed_total > 0 else 0
            avg_per_part = elapsed_total / completed if completed > 0 else 0
            eta_min = avg_per_part * (len(tasks) - completed) / 60

            partition_label = Path(stats["src"]).parts[-3:]
            label = "/".join(partition_label)
            print(f"[{completed}/{len(tasks)}] {label}: "
                  f"{stats['n_games']:,} games, {stats['n_edges']:,} edges, "
                  f"{stats['n_failed']} failed in {stats['elapsed_sec']:.1f}s "
                  f"| Aggregate {agg_rate:,.0f} games/sec | ETA {eta_min:.1f} min")

    elapsed = time.time() - t0
    print(f"\n=== Done in {elapsed/60:.1f} min ===")
    print(f"Total: {total_games:,} games -> {total_edges:,} edges")
    if total_games:
        print(f"Parse errors: {total_failed:,} ({total_failed / total_games * 100:.2f}%)")
    if errors:
        print(f"\n{len(errors)} partition(s) failed:")
        for e in errors[:5]:
            print(f"  {e['src']}: {e['error']}")
        if len(errors) > 5:
            print(f"  ... and {len(errors) - 5} more")


if __name__ == "__main__":
    main()
