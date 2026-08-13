"""One command for "how's it going" on a long pipeline run.

Answering that question by hand takes four tool calls (log tail, process list,
disk, partial count) plus arithmetic, and the arithmetic is where it goes wrong:
this project's extract ETA must be computed against KEPT games (mean_elo >= 1800),
never raw games/s, because the kept fraction rises with era so raw throughput
falls steadily while the real rate is flat. Projecting from raw games/s
under-projected by ~25% once already.

Structure: a generic frame that works for anything (processes, commit headroom,
disk, log freshness) plus a parser per stage, because each stage logs
differently. Unrecognised stages still get the frame and a log tail rather than
nothing.

    extract   [N/M] lines -> kept-games/s per worker, remaining kept, ETA
    merge     consolidate/Phase-B lines -> kinds done, months to build

THE CEILING IS COMMIT, NOT RAM. Reported first, because a second process sized
against physical RAM instead of remaining commit is how this machine gets into
trouble.

The kept-games denominators are expensive to derive (footer read of every source
file plus a mean_elo sample per year+event, ~2 min) and never change unless a
backfill lands, so they are cached. `--refresh` rebuilds; a source-file count
that disagrees with the cache warns rather than silently misprojecting.

Usage:
    .venv/Scripts/python.exe python/status.py
    .venv/Scripts/python.exe python/status.py --refresh      # rebuild the cache
"""
from __future__ import annotations

import argparse
import ctypes
import json
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

REPO = Path(__file__).resolve().parent.parent
LOGS = REPO / "logs"
CACHE = LOGS / "kept_basis_cache.json"
DEFAULT_PARTIALS = Path("E:/chess/position-stats/_pooled_partials_ge1800_2013_2026_brc")
GB = 1024 ** 3
MIN_ELO = 1800
EVENTS = ("Blitz", "Rapid", "Classical")

PIPELINE = ("build_pooled_stats", "build_sharp_reps", "stage3_backwards_induction",
            "process_pgn_parquets", "consolidate_reclaim", "replay_holdout",
            "audit_hash_collisions", "validate_partials")

# "  [50/593] part-15.parquet: 642,113/2,000,000 kept, ... (1220s) | 5,649 games/s agg | 277.6 min"
EXTRACT_RE = re.compile(
    r"\[(\d+)/(\d+)\].*?([\d,]+)/([\d,]+) kept.*?\((\d+)s\).*?([\d,]+) games/s agg"
    r"(?:.*?([\d.]+) min)?")
HEADER_RE = re.compile(r"Extract: (\d+) source files, (\d+) to process, (\d+) workers")
CONSOL_RE = re.compile(r"consolidate (\w+): (\d+) months, (\d+) to build")
PHASEB_RE = re.compile(r"Phase B \((\w+)\)")


class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]


def memory() -> tuple[float, float, float]:
    """(commit used, commit limit, free physical) in GB. Commit is the ceiling."""
    m = MEMORYSTATUSEX()
    m.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
    return ((m.ullTotalPageFile - m.ullAvailPageFile) / GB,
            m.ullTotalPageFile / GB, m.ullAvailPhys / GB)


def processes() -> list[dict]:
    """Pipeline processes with their command lines. tasklist cannot show command
    lines, so this asks CIM once rather than per process."""
    ps = ("Get-CimInstance Win32_Process -Filter \"Name like '%python%'\" | "
          "Select-Object ProcessId,ParentProcessId,WorkingSetSize,CommandLine,"
          "CreationDate | ConvertTo-Json -Compress")
    try:
        out = subprocess.run(["powershell.exe", "-NoProfile", "-Command", ps],
                             capture_output=True, text=True, timeout=60).stdout
        data = json.loads(out) if out.strip() else []
    except Exception:                                              # noqa: BLE001
        return []
    if isinstance(data, dict):
        data = [data]
    rows = []
    for p in data:
        cmd = p.get("CommandLine") or ""
        stage = next((s for s in PIPELINE if s in cmd), None)
        # multiprocessing workers carry a spawn_main command line with no script
        # name, so they are attributed by parent rather than dropped.
        rows.append({"pid": p.get("ProcessId"), "ppid": p.get("ParentProcessId"),
                     "ws": (p.get("WorkingSetSize") or 0) / GB,
                     "stage": stage, "spawn": "spawn_main" in cmd, "cmd": cmd})
    return rows


def newest_log() -> Path | None:
    ls = [p for p in LOGS.glob("*.log") if p.is_file()]
    return max(ls, key=lambda p: p.stat().st_mtime) if ls else None


def build_cache(partial_dir: Path) -> dict:
    """Per-source-file row counts and per-(year,event) kept fractions."""
    import pyarrow.parquet as pq
    from build_pooled_stats import SOURCE_ROOT, discover_source_files

    files = discover_source_files(2013, 2026, None, list(EVENTS))
    print(f"  building cache over {len(files):,} source files ...", flush=True)

    def rows(t):
        try:
            return pq.read_metadata(t[0]).num_rows
        except Exception:                                          # noqa: BLE001
            return 0

    with ThreadPoolExecutor(max_workers=16) as pool:
        counts = list(pool.map(rows, files))

    cells: dict[str, list] = {}
    for (f, y, m, ev), n in zip(files, counts):
        cells.setdefault(f"{y}|{ev}", []).append((str(f), n))

    def frac(sample: str) -> float:
        import pyarrow.compute as pc
        t = pq.read_table(sample, columns=["mean_elo"])
        if not t.num_rows:
            return 0.0
        ge = pc.sum(pc.greater_equal(t.column("mean_elo"), MIN_ELO)).as_py() or 0
        return ge / t.num_rows

    keys = sorted(cells)
    samples = [cells[k][len(cells[k]) // 2][0] for k in keys]
    with ThreadPoolExecutor(max_workers=6) as pool:
        fracs = dict(zip(keys, pool.map(frac, samples)))

    cache = {"built": time.strftime("%Y-%m-%dT%H:%M:%S"),
             "source_root": str(SOURCE_ROOT),
             "files": {str(f): {"rows": n, "cell": f"{y}|{ev}"}
                       for (f, y, m, ev), n in zip(files, counts)},
             "kept_frac": fracs}
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache), encoding="utf-8")
    return cache


def remaining_kept(cache: dict, partial_dir: Path) -> tuple[int, int, int, int]:
    """(remaining kept, total kept, chunks done, chunks total) from the partials
    actually on disk — the same skip-gate arithmetic the extract uses."""
    done = set()
    for p in partial_dir.glob("*.ps.parquet"):
        done.add(p.name[:-len(".ps.parquet")])
    tot = rem = 0
    n_done = 0
    for f, info in cache["files"].items():
        fr = cache["kept_frac"].get(info["cell"], 0.0)
        kept = int(info["rows"] * fr)
        tot += kept
        pth = Path(f)
        key = (f"year={pth.parent.parent.parent.name.split('=')[1]}"
               f"_month={int(pth.parent.parent.name.split('=')[1])}"
               f"_event={pth.parent.name.split('=')[1]}_{pth.stem}")
        if key in done:
            n_done += 1
        else:
            rem += kept
    return rem, tot, n_done, len(cache["files"])


def parse_extract(lines: list[str]) -> dict | None:
    prog = [EXTRACT_RE.search(x) for x in lines]
    prog = [m for m in prog if m]
    if not prog:
        return None
    last = prog[-1]
    recent = prog[-6:]
    rates = [int(m.group(3).replace(",", "")) / max(int(m.group(5)), 1)
             for m in recent]
    hdr = None
    for x in reversed(lines):
        h = HEADER_RE.search(x)
        if h:
            hdr = h
            break
    return {"done": int(last.group(1)), "total": int(last.group(2)),
            "raw_agg": int(last.group(6).replace(",", "")),
            "elapsed_min": float(last.group(7)) if last.group(7) else None,
            "per_worker_kept_s": sum(rates) / len(rates),
            "workers": int(hdr.group(3)) if hdr else None}


def parse_merge(lines: list[str]) -> dict | None:
    cons = {}
    for x in lines:
        m = CONSOL_RE.search(x)
        if m:
            cons[m.group(1)] = (int(m.group(2)), int(m.group(3)))
    phase_b = [PHASEB_RE.search(x).group(1) for x in lines if PHASEB_RE.search(x)]
    if not cons and not phase_b:
        return None
    return {"consolidate": cons, "phase_b": phase_b}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", default=None)
    ap.add_argument("--partial-dir", default=str(DEFAULT_PARTIALS))
    ap.add_argument("--refresh", action="store_true",
                    help="Rebuild the kept-games cache (~2 min).")
    ap.add_argument("--tail", type=int, default=400)
    a = ap.parse_args()
    partial_dir = Path(a.partial_dir)

    used, limit, free_phys = memory()
    print("=" * 72)
    print(f"MACHINE   commit {used:,.0f}/{limit:,.0f} GB "
          f"({limit-used:,.0f} GB headroom)   free RAM {free_phys:,.0f} GB")
    for drv in ("D:", "E:", "F:"):
        if Path(drv + "/").exists():
            du = shutil.disk_usage(drv + "/")
            print(f"          {drv} {du.free/GB:>7,.0f} GB free of {du.total/GB:,.0f}")

    procs = processes()
    live = [p for p in procs if p["stage"]]
    workers = [p for p in procs if p["spawn"]]
    print(f"\nPROCESSES {len(procs)} python, {len(live)} pipeline, "
          f"{len(workers)} worker(s)")
    for p in live:
        print(f"          pid {p['pid']:<7} {p['stage']:<28} {p['ws']:>5.1f} GB")
    if workers:
        pids = ", ".join(str(w["pid"]) for w in workers)
        ws_total = sum(w["ws"] for w in workers)
        print(f"          workers: {pids}   {ws_total:.1f} GB total")

    log = Path(a.log) if a.log else newest_log()
    if not log or not log.exists():
        print("\nno log found")
        return 0
    age = (time.time() - log.stat().st_mtime) / 60
    lines = log.read_text(encoding="utf-8", errors="replace").splitlines()[-a.tail:]
    # Log age alone is a BAD liveness signal here: the extract prints one line per
    # 10 chunks, which at ~18 min/chunk is an hour of legitimate silence. Partials
    # land per chunk per worker, so their mtime is the real heartbeat. Warning on
    # log age alone produced a false STALE on the first run of this script.
    part_age = None
    if partial_dir.is_dir():
        newest = max((f.stat().st_mtime for f in partial_dir.glob("*.parquet")),
                     default=None)
        if newest:
            part_age = (time.time() - newest) / 60
    heartbeat = min(x for x in (age, part_age) if x is not None)
    print(f"\nLOG       {log.name}   last written {age:,.1f} min ago"
          + (f"   (newest partial {part_age:,.1f} min)" if part_age is not None else "")
          + ("   <- STALE" if heartbeat > 30 else ""))

    ex = parse_extract(lines)
    mg = parse_merge(lines)

    if ex:
        elapsed = (f", {ex['elapsed_min']:,.0f} min elapsed"
                   if ex["elapsed_min"] else "")
        print(f"\nSTAGE     extract  [{ex['done']}/{ex['total']}] of this run{elapsed}")
        nw = ex["workers"] or max(len(workers), 1)
        agg = ex["per_worker_kept_s"] * nw
        print(f"          {ex['per_worker_kept_s']:,.0f} kept-games/s/worker "
              f"x {nw} = {agg:,.0f} aggregate   "
              f"(log's raw rate {ex['raw_agg']:,}/s falls with era — not a slowdown)")
        if a.refresh or not CACHE.exists():
            print("\n          kept-games cache:")
            cache = build_cache(partial_dir)
        else:
            cache = json.loads(CACHE.read_text(encoding="utf-8"))
        if partial_dir.is_dir():
            rem, tot, nd, nt = remaining_kept(cache, partial_dir)
            eta_h = rem / max(agg, 1) / 3600
            print(f"          chunks {nd:,}/{nt:,} ({100*nd/max(nt,1):.1f}%)   "
                  f"kept {tot-rem:,} of {tot:,} ({100*(tot-rem)/max(tot,1):.1f}%)")
            print(f"          remaining {rem:,} kept  ->  ETA {eta_h:,.1f} h "
                  f"({eta_h/24:,.1f} d), ~{time.strftime('%a %H:%M', time.localtime(time.time()+eta_h*3600))}")
            psz = sum(f.stat().st_size for f in partial_dir.glob("*.parquet"))
            print(f"          partials {psz/GB:,.1f} GB")
    elif mg:
        print("\nSTAGE     merge")
        for kind, (months, todo) in mg["consolidate"].items():
            print(f"          consolidate {kind:<10} {months-todo}/{months} months done")
        if mg["phase_b"]:
            print(f"          final merge reached: {', '.join(mg['phase_b'])}")
    else:
        print("\nSTAGE     unrecognised — frame only")

    print(f"\n{lines[-1][:110] if lines else '(empty log)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
