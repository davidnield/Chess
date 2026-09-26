"""The explorer-extract A/B benchmark: three ways to build one finished month.

    A    d79a0c7, from its own read-only worktree (Chess-explorer-A):
         build_pooled_stats --phase extract (explorer flags, epd 16), then
         consolidate_reclaim --kinds ps term --sub-buckets auto --force
         (no --apply), then backfill_epd.
    B1   this branch: the same extract at --epd-max-ply 30, the same
         consolidation, then bucket_month (no replay).
    R    the Rust explorer-extract `month`: games -> the finished month.

Every arm ends in the same layout (<out>/month=Y_M/bkt=i, _manifest, the
sentinel) plus a term monthly, so `--verify` can hold them equal with
compare_explorer_outputs.py before any timing is trusted.

MEASUREMENT. Each stage runs in its own Windows Job Object (created suspended,
assigned, then resumed, so every worker it spawns is inside): wall time,
user + kernel CPU, PeakJobMemoryUsed, and I/O bytes. A 1 s sampler records
system commit and the free space of every drive a stage writes to. CPU seconds
are reported next to wall time because a small month caps the Python pool at
one worker per source file.

IDLE GUARD. Refuses to start while a python process runs backfill_epd,
build_pooled_stats or consolidate_reclaim, or while the machine is more than 10%
busy over 60 s. --allow-contention skips it, for light correctness runs only:
numbers measured that way are not benchmarks, and the report says so.

Usage:
    python ab_extract.py --month 2015_6 --arms A B1 R --root D:\\chess\\ab_bench [--verify]
    python ab_extract.py --month 2015_6 --dry-run          # print every command
    python ab_extract.py --selfcheck                         # job accounting on a trivial child

Output: <root>/<Y_M>/report.json and a printed table.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from ctypes import wintypes
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
ARM_A_TREE = Path("C:/Users/David/Documents/Chess-explorer-A")
PY = Path(sys.executable)
RUST_EXE = Path("D:/rust-target/explorer_extract/release/explorer-extract.exe")
SOURCE = Path("D:/data/chess/standard-chess-games-compressed")
EVENTS = ["Blitz", "Bullet", "Classical", "Correspondence", "Rapid", "UltraBullet"]
EXPLORER = ["--events", *EVENTS, "--min-elo", "0", "--no-prune", "--max-ply", "30",
            "--no-fuse-winpos", "--no-child-eval", "--exclude-bots",
            "--exclude-terminations", "Rules infraction", "Abandoned",
            "--chunk-games", "250000"]
BUSY_PATTERN = re.compile(r"backfill_epd|build_pooled_stats|consolidate_reclaim")

# ── Windows job objects ───────────────────────────────────────────────────────

k32 = ctypes.WinDLL("kernel32", use_last_error=True) if sys.platform == "win32" else None
ntdll = ctypes.WinDLL("ntdll") if sys.platform == "win32" else None
CREATE_SUSPENDED = 0x4
JobObjectBasicAccountingInformation = 1
JobObjectBasicAndIoAccountingInformation = 8
JobObjectExtendedLimitInformation = 9


class IO_COUNTERS(ctypes.Structure):
    _fields_ = [(n, ctypes.c_ulonglong) for n in
                ("ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
                 "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]


class BASIC_ACCOUNTING(ctypes.Structure):
    _fields_ = [("TotalUserTime", ctypes.c_longlong), ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong),
                ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", wintypes.DWORD), ("TotalProcesses", wintypes.DWORD),
                ("ActiveProcesses", wintypes.DWORD), ("TotalTerminatedProcesses", wintypes.DWORD)]


class BASIC_AND_IO(ctypes.Structure):
    _fields_ = [("BasicInfo", BASIC_ACCOUNTING), ("IoInfo", IO_COUNTERS)]


class BASIC_LIMIT(ctypes.Structure):
    _fields_ = [("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong), ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t), ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD)]


class EXTENDED_LIMIT(ctypes.Structure):
    _fields_ = [("BasicLimitInformation", BASIC_LIMIT), ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t), ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t), ("PeakJobMemoryUsed", ctypes.c_size_t)]


class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [("dwLength", wintypes.DWORD), ("dwMemoryLoad", wintypes.DWORD),
                ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]


if k32 is not None:
    k32.CreateJobObjectW.restype = wintypes.HANDLE
    k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    k32.QueryInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                              wintypes.DWORD, ctypes.c_void_p]
    k32.CloseHandle.argtypes = [wintypes.HANDLE]
    k32.GlobalMemoryStatusEx.argtypes = [ctypes.POINTER(MEMORYSTATUSEX)]
    k32.GetSystemTimes.argtypes = [ctypes.POINTER(ctypes.c_ulonglong)] * 3
    ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]


def commit_used() -> int:
    m = MEMORYSTATUSEX()
    m.dwLength = ctypes.sizeof(m)
    k32.GlobalMemoryStatusEx(ctypes.byref(m))
    return m.ullTotalPageFile - m.ullAvailPageFile


def _query(job, cls, struct):
    s = struct()
    if not k32.QueryInformationJobObject(job, cls, ctypes.byref(s), ctypes.sizeof(s), None):
        raise ctypes.WinError(ctypes.get_last_error())
    return s


class Sampler(threading.Thread):
    """System commit and drive free space, once a second."""

    def __init__(self, drives: list[str]):
        super().__init__(daemon=True)
        self.drives = sorted(set(drives))
        self.peak_commit = 0
        self.min_free = {d: shutil.disk_usage(d + "/").free for d in self.drives}
        self.stop = threading.Event()

    def run(self):
        while not self.stop.wait(1.0):
            self.peak_commit = max(self.peak_commit, commit_used())
            for d in self.drives:
                self.min_free[d] = min(self.min_free[d], shutil.disk_usage(d + "/").free)


def run_stage(name: str, cmd: list[str], cwd: Path, log: Path, env: dict | None,
              drives: list[str]) -> dict:
    """One stage in its own job object; returns its measurements."""
    log.parent.mkdir(parents=True, exist_ok=True)
    job = k32.CreateJobObjectW(None, None)
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    sampler = Sampler(drives)
    sampler.peak_commit = commit_used()
    t0 = time.perf_counter()
    with open(log, "w", encoding="utf-8") as fh:
        fh.write(f"# {name}\n# cwd {cwd}\n# {subprocess.list2cmdline(cmd)}\n")
        fh.flush()
        p = subprocess.Popen(cmd, cwd=cwd, stdout=fh, stderr=subprocess.STDOUT,
                             env=env, creationflags=CREATE_SUSPENDED)
        try:
            if not k32.AssignProcessToJobObject(job, int(p._handle)):
                raise ctypes.WinError(ctypes.get_last_error())
            sampler.start()
            ntdll.NtResumeProcess(int(p._handle))
            rc = p.wait()
        finally:
            sampler.stop.set()
    wall = time.perf_counter() - t0
    acct = _query(job, JobObjectBasicAndIoAccountingInformation, BASIC_AND_IO)
    ext = _query(job, JobObjectExtendedLimitInformation, EXTENDED_LIMIT)
    k32.CloseHandle(job)
    return {"stage": name, "rc": rc, "wall_s": wall,
            "cpu_user_s": acct.BasicInfo.TotalUserTime / 1e7,
            "cpu_kernel_s": acct.BasicInfo.TotalKernelTime / 1e7,
            "processes": acct.BasicInfo.TotalProcesses,
            "peak_job_memory_bytes": ext.PeakJobMemoryUsed,
            "io_read_bytes": acct.IoInfo.ReadTransferCount,
            "io_write_bytes": acct.IoInfo.WriteTransferCount,
            "peak_system_commit_bytes": sampler.peak_commit,
            "min_free_bytes": sampler.min_free, "log": str(log)}


# ── the idle guard ────────────────────────────────────────────────────────────

def busy_pipeline() -> list[str]:
    out = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
         "ForEach-Object { $_.CommandLine }"],
        capture_output=True, text=True).stdout
    return [ln for ln in out.splitlines() if BUSY_PATTERN.search(ln)]


def cpu_busy(seconds: float = 60.0) -> float:
    def times():
        i, k, u = (ctypes.c_ulonglong() for _ in range(3))
        k32.GetSystemTimes(ctypes.byref(i), ctypes.byref(k), ctypes.byref(u))
        return i.value, k.value, u.value
    i0, k0, u0 = times()
    time.sleep(seconds)
    i1, k1, u1 = times()
    total = (k1 - k0) + (u1 - u0)          # kernel time includes idle time
    return 0.0 if total == 0 else 1.0 - (i1 - i0) / total


# ── the arms ──────────────────────────────────────────────────────────────────

def stages(arm: str, y: int, m: int, root: Path, a) -> list[tuple[str, list[str], Path, dict | None]]:
    tag = f"{y}_{m}"
    d = root / arm
    env_a = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    common_x = ["--start-year", str(y), "--end-year", str(y), "--months", str(m),
                "--phase", "extract", *EXPLORER, "--workers", str(a.workers),
                "--source", str(a.source)]
    cons = ["--kinds", "ps", "term", "--sub-buckets", "auto", "--force",
            "--start-year", str(y), "--end-year", str(y), "--threads", str(a.duck_threads),
            "--mem", a.duck_mem]
    if arm == "A":
        return [
            ("extract", [str(PY), "python/build_pooled_stats.py", *common_x,
                         "--partial-dir", str(d / "partials"), "--tag", "ab_A"], a.arm_a_tree, env_a),
            ("consolidate", [str(PY), "python/consolidate_reclaim.py", "--partial-dir",
                             str(d / "partials"), *cons], a.arm_a_tree, env_a),
            ("backfill", [str(PY), "-u", "python/backfill_epd.py", "--monthly-dir",
                          str(d / "partials" / "_monthly"), "--out", str(d / "out"),
                          "--work-dir", str(d / "work"), "--buckets", "512", "--months", tag,
                          "--workers", str(a.workers), "--threads", str(a.duck_threads),
                          "--mem", a.backfill_mem, "--tmp-dir", str(d / "duck")],
             a.arm_a_tree, env_a),
        ]
    if arm == "B1":
        return [
            ("extract", [str(PY), "python/build_pooled_stats.py", *common_x,
                         "--partial-dir", str(d / "partials"), "--tag", "ab_B1",
                         "--epd-max-ply", "30"], REPO, None),
            ("consolidate", [str(PY), "python/consolidate_reclaim.py", "--partial-dir",
                             str(d / "partials"), *cons], REPO, None),
            ("bucket", [str(PY), "python/bucket_month.py", "--monthly-dir",
                        str(d / "partials" / "_monthly"), "--out", str(d / "out"),
                        "--work-dir", str(d / "work"), "--buckets", "512", "--months", tag,
                        "--workers", str(a.workers), "--threads", str(a.duck_threads),
                        "--mem", a.backfill_mem, "--tmp-dir", str(d / "duck")], REPO, None),
        ]
    return [
        (f"month_rep{r + 1}", [str(a.rust_exe), "month", "--source", str(a.source),
                               "--months", tag, "--events", *EVENTS, "--out",
                               str(d / f"out{r + 1}"), "--term-dir", str(d / f"out{r + 1}" / "_term"),
                               "--threads", str(a.rust_threads), "--mem-gb", str(a.rust_mem_gb)],
         REPO, None)
        for r in range(a.rust_reps)
    ]


def verify(y: int, m: int, root: Path, arms: list[str]) -> list[dict]:
    """compare_explorer_outputs between arms: timings count only after this."""
    tag = f"{y}_{m}"
    out = {"A": root / "A" / "out", "B1": root / "B1" / "out", "R": root / "R" / "out1"}
    term = {"A": root / "A" / "partials" / "_monthly", "B1": root / "B1" / "partials" / "_monthly",
            "R": root / "R" / "out1" / "_term"}
    res = []
    for x, z in (("A", "B1"), ("A", "R"), ("B1", "R")):
        if x in arms and z in arms:
            for mode, extra in (("month", ["--ply-le"]), ("term", [])):
                a_, b_ = (out[x], out[z]) if mode == "month" else (term[x], term[z])
                p = subprocess.run([str(PY), str(HERE / "compare_explorer_outputs.py"), mode,
                                    str(a_), str(b_), "--month", tag, "--threads", "8",
                                    "--mem", "16GB", *extra], capture_output=True, text=True)
                res.append({"pair": f"{x} vs {z}", "mode": mode, "rc": p.returncode,
                            "tail": p.stdout.strip().splitlines()[-3:]})
    return res


def selfcheck() -> int:
    r = run_stage("selfcheck", [str(PY), "-c",
                                "import subprocess,sys; subprocess.run([sys.executable,'-c',"
                                "'x=bytearray(200_000_000); s=sum(range(10**7))'])"],
                  REPO, Path(os.environ.get("TEMP", ".")) / "ab_selfcheck.log", None, ["C:"])
    print(json.dumps(r, indent=2))
    ok = (r["rc"] == 0 and r["processes"] >= 2 and r["peak_job_memory_bytes"] > 150_000_000
          and r["cpu_user_s"] > 0)
    print("SELFCHECK", "PASS" if ok else "FAIL",
          "(the grandchild's 200 MB and CPU were charged to the job)")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--month", metavar="Y_M")
    ap.add_argument("--arms", nargs="+", default=["A", "B1", "R"], choices=["A", "B1", "R"])
    ap.add_argument("--root", type=Path, default=Path("D:/chess/ab_bench"))
    ap.add_argument("--source", type=Path, default=SOURCE)
    ap.add_argument("--arm-a-tree", type=Path, default=ARM_A_TREE)
    ap.add_argument("--rust-exe", type=Path, default=RUST_EXE)
    ap.add_argument("--workers", type=int, default=11, help="Python pools (A and B1 alike).")
    ap.add_argument("--duck-threads", type=int, default=8)
    ap.add_argument("--duck-mem", default="48GB", help="consolidation --mem")
    ap.add_argument("--backfill-mem", default="32GB", help="backfill / bucket_month --mem")
    ap.add_argument("--rust-threads", type=int, default=12)
    ap.add_argument("--rust-mem-gb", type=float, default=40.0)
    ap.add_argument("--rust-reps", type=int, default=2)
    ap.add_argument("--verify", action="store_true",
                    help="Compare the arms' outputs after they run.")
    ap.add_argument("--allow-contention", action="store_true",
                    help="Skip the idle guard. Correctness runs only: not a benchmark.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--selfcheck", action="store_true")
    a = ap.parse_args()
    if a.selfcheck:
        return selfcheck()
    if not a.month or not re.fullmatch(r"\d{4}_\d{1,2}", a.month):
        print("FATAL: --month Y_M is required")
        return 1
    y, m = (int(x) for x in a.month.split("_"))
    root = a.root / f"{y}_{m}"
    plan = [(arm, st) for arm in a.arms for st in stages(arm, y, m, root, a)]
    if a.dry_run:
        for arm, (name, cmd, cwd, env) in plan:
            print(f"[{arm}:{name}] (cwd {cwd}{', no .pyc' if env else ''})\n  "
                  f"{subprocess.list2cmdline(cmd)}")
        return 0
    if not a.allow_contention:
        busy = busy_pipeline()
        if busy:
            print("REFUSING: pipeline processes are running:\n  " + "\n  ".join(busy))
            return 1
        frac = cpu_busy()
        if frac > 0.10:
            print(f"REFUSING: the machine was {frac:.0%} busy over 60 s (limit 10%)")
            return 1
    for arm in a.arms:
        if (root / arm).exists():
            print(f"FATAL: {root / arm} exists; every arm starts from an empty dir")
            return 1
    drives = sorted({str(a.root)[:2], "C:", "D:", "E:"})
    report = {"month": a.month, "arms": a.arms, "contended": a.allow_contention,
              "started": time.strftime("%Y-%m-%d %H:%M:%S"), "stages": []}
    for arm, (name, cmd, cwd, env) in plan:
        print(f"[{arm}:{name}] ...", flush=True)
        r = run_stage(f"{arm}:{name}", cmd, cwd, root / "logs" / f"{arm}_{name}.log", env, drives)
        r["arm"] = arm
        report["stages"].append(r)
        print(f"  rc {r['rc']}  wall {r['wall_s']:,.0f}s  cpu {r['cpu_user_s'] + r['cpu_kernel_s']:,.0f}s  "
              f"peak job mem {r['peak_job_memory_bytes'] / 1e9:,.1f} GB", flush=True)
        if r["rc"] != 0:
            print(f"  STOPPED: see {r['log']}")
            break
    if a.verify:
        report["verify"] = verify(y, m, root, a.arms)
    root.mkdir(parents=True, exist_ok=True)
    (root / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n{'arm:stage':<22}{'wall s':>10}{'cpu s':>10}{'peak GB':>9}{'read GB':>9}{'write GB':>9}")
    for r in report["stages"]:
        print(f"{r['stage']:<22}{r['wall_s']:>10,.0f}{r['cpu_user_s'] + r['cpu_kernel_s']:>10,.0f}"
              f"{r['peak_job_memory_bytes'] / 1e9:>9.1f}{r['io_read_bytes'] / 1e9:>9.1f}"
              f"{r['io_write_bytes'] / 1e9:>9.1f}")
    if a.allow_contention:
        print("\n(--allow-contention: measured under contention, NOT a benchmark)")
    for v in report.get("verify", []):
        print(f"verify {v['pair']} {v['mode']}: rc {v['rc']} {v['tail'][-1] if v['tail'] else ''}")
    print(f"\nreport: {root / 'report.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
