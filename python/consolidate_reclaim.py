"""Consolidate the extract partials one kind at a time, reclaiming as it goes.

WHY THIS EXISTS
---------------
`--phase all` consolidates every kind and only then starts the final merge, so
at its peak the drive holds all the per-file partials AND all the monthly files
at once. Measured for the 2013-2026 rebuild:

    partials at extract end        ~2,469 GB   (ps 959, winpos200/300/500 ~500 ea)
    monthly files, all kinds       ~1,727 GB   (compaction ratio ~0.70, measured)
    free on E: at extract end      ~1,599 GB

so the consolidation runs out of disk before the final merge even begins, and
the merge then wants another ~671 GB for the largest bucket dir on top.

Interleaving fixes it. Peak extra space becomes the LARGEST SINGLE KIND's monthly
output (~671 GB for ps) instead of the sum of all of them, because each kind's
partials are released before the next kind is built. Consolidating everything
also releases 742 GB net, which is what makes the final merge fit with all three
winpos thresholds instead of forcing us to drop two of them.

WHY DELETING THE PARTIALS IS SAFE
---------------------------------
Every downstream consumer reads `_monthly`, not the per-file partials — verified
by reading the call sites, not assumed:

    merge_position_stats(monthly_dir, ...)     the pooled position_stats
    merge_aux_stats  -> mdir = partial_dir/"_monthly"/"*.term.parquet"
    merge_crush(monthly_dir, ..., kind=...)    each winpos histogram

Task #113's year-scoped <=2024 pool also reads `_monthly` — the year is in the
monthly filename, which is exactly why task #94's reclaim gate names `_monthly`
and not the partials. The per-file partials have no reader once their month is
consolidated.

WHAT MAKES IT SAFE IN PRACTICE
------------------------------
The extract is ~90 h and cannot be redone casually, so "the monthly looks fine"
is not good enough. Each monthly file must pass BOTH:

  1. Full row-group read. A truncated parquet still parses its footer — the
     2026-07-26 freeze post-mortem is the reason this is not a footer check.
  2. Exact conservation of every summed column against the partials that fed it.
     Consolidation is pure SUM over a GROUP BY, so the column totals are integers
     that must match to the unit. This is a proof of equivalence, not a smoke
     test: it would catch a dropped input file, a partially-read partial, or a
     grouping key that silently changed.

Only if EVERY month of a kind passes are that kind's partials deleted, and only
under --apply. The default is a dry run.

It also refuses to touch a kind while the extract might still be writing it:
an incomplete chunk set means a month could still gain partials after being
consolidated, and its monthly would then be silently short. --force overrides,
but do not use it while build_pooled_stats is running.

Usage:
    # dry run, report only
    .venv/Scripts/python.exe python/consolidate_reclaim.py

    # do it, one kind at a time, biggest first
    .venv/Scripts/python.exe python/consolidate_reclaim.py --apply

    # a single kind
    .venv/Scripts/python.exe python/consolidate_reclaim.py --kinds ps --apply
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import (STATS_DIR, consolidate_monthly,
                                discover_source_files)

# build_pooled_stats keeps this as an argparse default rather than a constant;
# it must match, or the completeness guard compares against the wrong file count.
# The "brc" in the partial-dir name is this list's initials.
EVENTS = ("Blitz", "Rapid", "Classical")

# ps first: it is the largest kind, so reclaiming it releases the most soonest
# (959 GB of partials for 671 GB of monthly = +288 GB net).
DEFAULT_KINDS = ("ps", "term", "winpos300", "winpos200", "winpos500")

# The columns consolidation SUMs, per kind. Anything not listed here is carried
# through by any_value() and cannot be checked by conservation.
SUM_COLS = {
    "ps": ("total", "white_wins", "draws", "black_wins"),
    "term": ("total", "white_wins", "draws", "black_wins"),
    "_hist": ("n", "white_wins", "black_wins"),
}
MONTH_RE = re.compile(r"year=(\d+)_month=(\d+)_")
GB = 1024 ** 3


def sum_cols(kind: str) -> tuple[str, ...]:
    return SUM_COLS.get(kind, SUM_COLS["_hist"])


def totals(con, files: list[Path] | Path, cols: tuple[str, ...]) -> tuple[int, ...]:
    """SUM every conserved column over one or many parquet files."""
    if isinstance(files, Path):
        src = f"'{files.as_posix()}'"
    else:
        src = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"
    sel = ", ".join(f"SUM({c})::HUGEINT" for c in cols)
    return con.execute(f"SELECT {sel} FROM read_parquet({src})").fetchone()


def read_fully(p: Path) -> tuple[bool, str]:
    """Read EVERY row group. The footer alone parses on a truncated file."""
    try:
        f = pq.ParquetFile(p)
        n = 0
        for i in range(f.num_row_groups):
            n += f.read_row_group(i).num_rows
        if n != f.metadata.num_rows:
            return False, f"row count {n} != footer {f.metadata.num_rows}"
        return True, ""
    except Exception as e:                                        # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def months_of(partial_dir: Path, kind: str) -> dict[tuple[int, int], list[Path]]:
    out: dict[tuple[int, int], list[Path]] = defaultdict(list)
    for f in partial_dir.glob(f"*.{kind}.parquet"):
        m = MONTH_RE.search(f.name)
        if m:
            out[(int(m.group(1)), int(m.group(2)))].append(f)
    return out


def extract_complete(partial_dir: Path, start: int, end: int) -> tuple[bool, int, int]:
    """Have all source chunks produced a ps partial? Guards against consolidating
    a month the extract has not finished writing."""
    expected = len(discover_source_files(start, end, None, list(EVENTS)))
    have = len(list(partial_dir.glob("*.ps.parquet")))
    return have >= expected, have, expected


def verify_kind(partial_dir: Path, kind: str, threads: int, mem: str) -> bool:
    """Full read + exact conservation for every month of `kind`. All or nothing."""
    mdir = partial_dir / "_monthly"
    cols = sum_cols(kind)
    parts = months_of(partial_dir, kind)
    if not parts:
        print(f"  {kind}: no partials left to verify")
        return False
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET threads={threads}")
    ok = True
    for (y, mo), files in sorted(parts.items()):
        mf = mdir / f"year={y}_month={mo}.{kind}.parquet"
        if not mf.exists():
            print(f"  FAIL {y}-{mo:02d}: monthly missing ({mf.name})")
            ok = False
            continue
        good, why = read_fully(mf)
        if not good:
            print(f"  FAIL {y}-{mo:02d}: unreadable — {why}")
            ok = False
            continue
        a = totals(con, files, cols)
        b = totals(con, mf, cols)
        if a != b:
            print(f"  FAIL {y}-{mo:02d}: conservation broken")
            for c, x, z in zip(cols, a, b):
                if x != z:
                    print(f"        {c}: partials {x:,} != monthly {z:,}")
            ok = False
    con.close()
    if ok:
        print(f"  {kind}: all {len(parts)} months verified "
              f"(full read + exact SUM conservation on {', '.join(cols)})")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--partial-dir", default=None)
    ap.add_argument("--kinds", nargs="+", default=list(DEFAULT_KINDS))
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--mem", default="48GB")
    ap.add_argument("--start-year", type=int, default=2013)
    ap.add_argument("--end-year", type=int, default=2026)
    ap.add_argument("--apply", action="store_true",
                    help="Actually delete verified partials. Default is a dry run.")
    ap.add_argument("--force", action="store_true",
                    help="Proceed even if the extract looks unfinished. Do NOT "
                         "use while build_pooled_stats is running.")
    a = ap.parse_args()

    partial_dir = (Path(a.partial_dir) if a.partial_dir else
                   STATS_DIR / "_pooled_partials_ge1800_2013_2026_brc")
    if not partial_dir.is_dir():
        print(f"FATAL: no partial dir at {partial_dir}")
        return 1

    done, have, expected = extract_complete(partial_dir, a.start_year, a.end_year)
    free0 = shutil.disk_usage(partial_dir).free
    print(f"partials  : {partial_dir}")
    print(f"chunks    : {have:,} ps partials / {expected:,} source files "
          f"— {'COMPLETE' if done else 'INCOMPLETE'}")
    print(f"free      : {free0/GB:,.1f} GB")
    print(f"mode      : {'APPLY (will delete verified partials)' if a.apply else 'DRY RUN'}\n")
    if not done and not a.force:
        print("REFUSING: the extract has not produced a partial for every source "
              "file. Consolidating now would build monthlies that a later chunk "
              "silently invalidates. Wait for the extract, or pass --force if you "
              "know it is stopped.")
        return 1

    for kind in a.kinds:
        parts = months_of(partial_dir, kind)
        if not parts:
            print(f"[{kind}] nothing to do (no partials)\n")
            continue
        nbytes = sum(f.stat().st_size for fs in parts.values() for f in fs)
        nfiles = sum(len(fs) for fs in parts.values())
        print(f"[{kind}] {nfiles:,} partials, {nbytes/GB:,.1f} GB, "
              f"{len(parts)} months")

        t0 = time.time()
        consolidate_monthly(partial_dir, a.threads, a.mem, None, (kind,))
        print(f"  consolidated in {time.time()-t0:,.0f}s", flush=True)

        if not verify_kind(partial_dir, kind, a.threads, a.mem):
            print(f"  ABORT: {kind} did not verify — partials KEPT. "
                  f"Fix before continuing; later kinds not attempted.")
            return 1

        if a.apply:
            freed = 0
            for fs in parts.values():
                for f in fs:
                    try:
                        freed += f.stat().st_size
                        f.unlink()
                    except OSError as e:
                        print(f"  unlink {f.name}: {e}")
            print(f"  reclaimed {freed/GB:,.1f} GB — free now "
                  f"{shutil.disk_usage(partial_dir).free/GB:,.1f} GB\n")
        else:
            print(f"  would reclaim {nbytes/GB:,.1f} GB (re-run with --apply)\n")

    free1 = shutil.disk_usage(partial_dir).free
    print(f"free: {free0/GB:,.1f} GB -> {free1/GB:,.1f} GB "
          f"({(free1-free0)/GB:+,.1f} GB)")
    print("\nNext: run the final merge with --phase merge. consolidate_monthly is "
          "skip-gated per month, so it will no-op and go straight to the merge.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
