"""Extract ply-keyed months on NVMe and publish verified copies to an archive.
Each root is bound to the request and extractor binary before a month is skipped.
Use --source-provenance for historical months missing a requested speed. Supply
the independent per-month provenance directory of the earlier six-speed extraction.
The default --verify hash re-reads each archived file before releasing its stage.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import datetime
import re

import month_archive as archive
import hashlib
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

MERGE_EVENTS = ["Blitz", "Bullet", "Classical", "Correspondence", "Rapid", "UltraBullet"]
EXIT_FAIL, EXIT_SPACE = 1, 4
COPY_BUFFER = 16 * 1024 * 1024


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


def parse_months(specs: list[str]) -> list[tuple[int, int]]:
    """Y_M values and Y_M..Y_M ranges, in the order given; a repeated month is an error."""
    def one(s):
        y, m = s.split("_")
        y, m = int(y), int(m)
        if not 1 <= m <= 12:
            raise ValueError(f"{s}: month {m}")
        return y, m
    out = []
    for spec in specs:
        if ".." in spec:
            a, b = (one(x) for x in spec.split(".."))
            if a > b:
                raise ValueError(f"{spec}: the range runs backwards")
            y, m = a
            while (y, m) <= b:
                out.append((y, m))
                y, m = (y, m + 1) if m < 12 else (y + 1, 1)
        else:
            out.append(one(spec))
    if len(set(out)) != len(out):
        raise ValueError("a month is listed twice")
    return out


def mergeable_events(events: list[str]) -> bool:
    """merge::months_events: a non-empty subset of the six speeds, in their order."""
    idx = [MERGE_EVENTS.index(e) if e in MERGE_EVENTS else -1 for e in events]
    return bool(idx) and -1 not in idx and all(a < b for a, b in zip(idx, idx[1:]))


def tag(ym) -> str:
    return f"{ym[0]}_{ym[1]}"


def month_parts(root: Path, ym) -> dict[str, Path]:
    """A finished month's pieces under a months root (`month`'s layout)."""
    t = tag(ym)
    return {"dir": root / f"month={t}",
            "manifest": root / "_manifest" / f"month={t}.parquet",
            "provenance": root / "_provenance" / f"month={t}.json",
            "term": root / "_term" / f"year={ym[0]}_month={ym[1]}.term.parquet",
            "conflicts": root / "_conflicts" / f"month={t}",
            "sentinel": root / f"_month={t}.DONE"}


def free_gb(path: Path) -> float:
    return shutil.disk_usage(path).free / 1e9


def sha256(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def copy_file(src: Path, dst: Path, verify: str, hashes=None, timing=None) -> int:
    """src -> dst through dst's .tmp name; returns the bytes. Raises if the copy differs."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    h = hashlib.sha256() if verify == "hash" else None
    n = 0
    started = time.monotonic()
    with src.open("rb") as fi, tmp.open("wb") as fo:
        while chunk := fi.read(COPY_BUFFER):
            fo.write(chunk)
            n += len(chunk)
            if h:
                h.update(chunk)
        fo.flush()
        os.fsync(fo.fileno())
    if tmp.stat().st_size != n or n != src.stat().st_size:
        raise OSError(f"{dst}: copied {tmp.stat().st_size} of {src.stat().st_size} bytes")
    written = time.monotonic()
    if h and sha256(tmp) != h.hexdigest():
        raise OSError(f"{dst}: the copy's SHA-256 differs from the source's")
    if timing is not None:
        timing[0] += written - started
        timing[1] += time.monotonic() - written
    tmp.replace(dst)
    if hashes is not None:
        hashes[dst] = h.hexdigest() if h else n
    return n


def tree_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file())


def receipt(root, ym):
    return root / "_archive_receipts" / (tag(ym) + ".json")


def validate_archive(a, ym, source):
    parts = month_parts(a.dest, ym)
    files = archive.check_month(parts, ym, a, source)
    rec = archive.read_json(receipt(a.dest, ym))
    if rec["source"] != source or rec["verify"] != a.verify:
        raise ValueError(f"{tag(ym)}: archive source or verification policy changed")
    files.append(a.dest / "_extract_params.json")
    actual = {p.relative_to(a.dest).as_posix(): sha256(p) if a.verify == "hash" else p.stat().st_size
              for p in files}
    if actual != rec["files"]:
        raise ValueError(f"{tag(ym)}: archive contents differ from publication receipt")


def move_month(a, ym, source):
    stage, dest, verify = a.stage, a.dest, a.verify
    s, d = month_parts(stage, ym), month_parts(dest, ym)
    files = archive.check_month(s, ym, a, source)
    t0 = time.monotonic()
    lock_s, lock_d = stage / "_extract_params.json", dest / "_extract_params.json"
    hashes = {}
    timing = [0., 0.]
    if lock_d.exists() and archive.read_json(lock_d) != archive.read_json(lock_s):
        raise ValueError(f"{lock_d}: extractor settings differ from staging")
    if lock_d.exists():
        hashes[lock_d] = sha256(lock_d) if verify == "hash" else lock_d.stat().st_size
    else:
        copy_file(lock_s, lock_d, verify, hashes)
    # The stage remains complete through every intermediate publication state.
    # The final marker hides an interrupted copy from the merge.
    if d["sentinel"].exists():
        raise ValueError(f"{tag(ym)}: already published")
    for k in ("dir", "conflicts"):
        archive.safe_child(dest, d[k])
        if d[k].exists():
            shutil.rmtree(d[k])
    nbytes = 0
    for p in files:
        if p == s["sentinel"]:
            continue
        dst = dest / p.relative_to(stage)
        archive.safe_child(dest, dst)
        nbytes += copy_file(p, dst, verify, hashes, timing)
    marker = d["sentinel"]
    hashes[marker] = sha256(s["sentinel"]) if verify == "hash" else s["sentinel"].stat().st_size
    rec = {"source": source, "verify": verify,
           "bytes": nbytes, "stream_write_seconds": timing[0], "readback_seconds": timing[1],
           "extract_seconds": archive.read_json(s["provenance"])["seconds"],
           "files": {p.relative_to(dest).as_posix(): h for p, h in hashes.items()}}
    archive.write_json(receipt(dest, ym), rec)
    copy_file(s["sentinel"], marker, verify)
    secs = time.monotonic() - t0
    archive.release(stage, s)
    return {"files": len(files), "bytes": nbytes, "copy_seconds": round(secs, 2),
            "copy_mb_per_s": round(nbytes / 1e6 / max(secs, 1e-9), 2),
            "stream_write_seconds": timing[0], "readback_seconds": timing[1]}


def month_command(a, ym) -> list[str]:
    cmd = [*a.exe, "month", "--source", str(a.source), "--months", tag(ym), "--events", *a.events,
           "--max-ply", str(a.max_ply), "--ply-key", "--out", str(a.stage),
           "--mem-gb", str(a.mem_gb), "--threads", str(a.threads)]
    if a.key128:
        cmd.append("--key128")
    if a.term_slices:
        cmd.append("--term-slices")
    for speed in a.inventories[ym]["absent_events"]:
        cmd.extend(["--absent-event", speed])
    return cmd + list(a.month_arg)


def drive(a) -> int:
    months = parse_months(a.months)
    if not mergeable_events(a.events):
        log(f"REFUSED: --events {a.events} is not a subset of {MERGE_EVENTS} in that order; "
            f"`merge` would refuse these months after the extraction")
        return EXIT_FAIL
    archive.disjoint(a.source, a.stage, a.dest)
    a.source, a.stage, a.dest = (p.resolve() for p in (a.source, a.stage, a.dest))
    if a.term_slices and not a.key128:
        raise ValueError("--term-slices requires --key128")
    for arg in a.month_arg:
        if not re.fullmatch(r"--(?:keys-per-game|bytes-per-key|passes)=[0-9]+(?:[.][0-9]+)?", arg):
            raise ValueError(f"unsupported extra extractor argument {arg}")
    exe = shutil.which(a.exe[0]) or a.exe[0]
    a.exe = [str(Path(exe).resolve())] + [str(Path(x).resolve(strict=True)) for x in a.exe[1:]]
    settings = {"version": 1, "exe": {x: sha256(Path(x)) for x in a.exe}, "source": str(a.source),
                "stage": str(a.stage), "archive": str(a.dest), "events": a.events, "key128": a.key128,
                "term_slices": a.term_slices, "max_ply": a.max_ply, "extra": a.month_arg, "verify": a.verify}
    a.inventories = {ym: archive.inventory(a.source, ym, a.events, a.source_provenance) for ym in months}
    if a.not_before and not a.dry_run:
        start = datetime.datetime.fromisoformat(a.not_before)
        while (remaining := (start - datetime.datetime.now()).total_seconds()) > 0:
            time.sleep(min(60, remaining))
    with ExitStack() as locks:
        if not a.dry_run:
            for root in sorted((a.stage, a.dest)):
                locks.enter_context(archive.root_lock(root))
        for root in (a.stage, a.dest):
            archive.root_binding(root, settings, a.dry_run)
            if (root / "_extract_params.json").exists():
                archive.check_root_params(root, a)
        todo = []
        for ym, source in a.inventories.items():
            s = month_parts(a.stage, ym)
            inventory_path = a.stage / "_source_inventory" / (tag(ym) + ".json")
            if not inventory_path.exists() and any(p.exists() for p in s.values()):
                raise ValueError(f"{tag(ym)}: staged output lacks its input inventory")
            archive.bind(inventory_path, source, a.dry_run)
            if month_parts(a.dest, ym)["sentinel"].exists():
                validate_archive(a, ym, source)
                if not a.dry_run:
                    archive.release(a.stage, s)
            else:
                if s["sentinel"].exists():
                    archive.check_month(s, ym, a, source)
                todo.append(ym)
        log(f"{len(months)} months, {len(months)-len(todo)} verified in archive, {len(todo)} to do")
        if a.dry_run:
            for ym in todo:
                log("would run: " + subprocess.list2cmdline(month_command(a, ym)))
            return 0
        return run_pending(a, months, todo)


def run_pending(a, months, todo):
    jobs: queue.Queue = queue.Queue()
    state = {"error": None, "staged_bytes": 0, "space": False}
    lock = threading.Lock()

    def staged_bytes(ym) -> int:
        return sum(f.stat().st_size for f in archive.check_month(month_parts(a.stage, ym), ym, a, a.inventories[ym]))

    def mover():
        while (item := jobs.get()) is not None:
            ym, rec, size = item
            try:
                if state["error"] is None:
                    if free_gb(a.dest) - size / 1e9 < a.min_dest_free_gb:
                        state["space"] = True
                        raise RuntimeError(f"{a.dest}: under {a.min_dest_free_gb} GB would be left")
                    rec.update(move_month(a, ym, a.inventories[ym]))
                    rec["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    with (a.dest / "_extraction_log.jsonl").open("a", encoding="utf-8") as f:
                        f.write(json.dumps(rec) + "\n")
                    log(f"{tag(ym)}: archived, {rec['bytes'] / 1e9:,.1f} GB at {rec['copy_mb_per_s']:,.0f} MB/s")
            except Exception as e:                                  # noqa: BLE001
                state["error"] = f"{tag(ym)}: copy failed: {e}"
                log("ERROR " + state["error"])
            finally:
                with lock:
                    state["staged_bytes"] -= size
                jobs.task_done()

    worker = threading.Thread(target=mover, daemon=True)
    worker.start()

    def enqueue(ym, rec):
        size = staged_bytes(ym)
        with lock:
            state["staged_bytes"] += size
        jobs.put((ym, rec, size))

    try:
        for ym in todo:
            if state["error"]:
                break
            s = month_parts(a.stage, ym)
            if s["sentinel"].exists():
                log(f"{tag(ym)}: already staged, moving")
                enqueue(ym, {"month": tag(ym), "resumed": True})
                continue
            while state["error"] is None and state["staged_bytes"] / 1e9 > a.max_staged_gb:
                time.sleep(5)                                           # back-pressure on the archive
            while state["error"] is None and free_gb(a.stage) < a.min_stage_free_gb and state["staged_bytes"]:
                time.sleep(5)                                           # the mover will free space
            if state["error"]:
                break
            if free_gb(a.stage) < a.min_stage_free_gb:
                state["error"], state["space"] = f"{a.stage}: under {a.min_stage_free_gb} GB free", True
                break
            cmd = month_command(a, ym)
            log(f"{tag(ym)}: " + subprocess.list2cmdline(cmd))
            t0 = time.monotonic()
            rc = subprocess.run(cmd).returncode
            secs = time.monotonic() - t0
            if rc or not s["sentinel"].exists():
                state["error"] = f"{tag(ym)}: `month` exited {rc}" + ("" if rc else " without its sentinel")
                break
            if archive.inventory(a.source, ym, a.events, a.source_provenance) != a.inventories[ym]:
                raise ValueError(f"{tag(ym)}: source inventory changed during extraction")
            prov = json.loads(s["provenance"].read_text(encoding="utf-8"))
            rec = {"month": tag(ym), "extract_seconds": round(secs, 1), "rows": prov.get("rows"),
                   "games_kept": (prov.get("counters") or {}).get("kept"), "passes": prov.get("passes"),
                   "peak_commit_gb": round((prov.get("peak_commit_bytes") or 0) / 1e9, 1),
                   "tool_commit": prov.get("commit")}
            log(f"{tag(ym)}: extracted in {secs:,.0f}s ({rec['rows']:,} rows, {rec['passes']} passes, "
                f"peak commit {rec['peak_commit_gb']} GB)")
            enqueue(ym, rec)
    finally:
        jobs.put(None)
        worker.join()
    if state["error"]:
        log("STOPPED: " + state["error"] + " (relaunch the same command to resume)")
        return EXIT_SPACE if state["space"] else EXIT_FAIL
    left = [tag(ym) for ym in months if not month_parts(a.dest, ym)["sentinel"].exists()]
    log(f"done: {len(months) - len(left)} of {len(months)} months in {a.dest}"
        + (f"; missing {left}" if left else ""))
    return EXIT_FAIL if left else 0


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--exe", nargs="+", required=True, help="explorer-extract.exe (or a command that stands in for it)")
    ap.add_argument("--months", nargs="+", required=True, metavar="Y_M[..Y_M]")
    ap.add_argument("--stage", type=Path, required=True, help="NVMe months root the tool writes")
    ap.add_argument("--dest", type=Path, required=True, help="the archive months root")
    ap.add_argument("--source", type=Path, default=Path("D:/data/chess/standard-chess-games-compressed"))
    ap.add_argument("--events", nargs="+", required=True)
    ap.add_argument("--max-ply", type=int, required=True)
    ap.add_argument("--key128", action="store_true")
    ap.add_argument("--term-slices", action="store_true")
    ap.add_argument("--source-provenance", type=Path, help="independent six-speed monthly provenance directory")
    ap.add_argument("--mem-gb", type=float, default=55.0)
    ap.add_argument("--threads", type=int, default=12)
    ap.add_argument("--month-arg", action="append", default=[], metavar="ARG",
                    help="an extra `month` argument (repeatable), e.g. --month-arg=--keys-per-game=40")
    ap.add_argument("--verify", choices=["size", "hash"], default="hash")
    ap.add_argument("--max-staged-gb", type=float, default=400.0,
                    help="extraction waits while more than this is staged and unmoved")
    ap.add_argument("--min-stage-free-gb", type=float, default=200.0)
    ap.add_argument("--min-dest-free-gb", type=float, default=100.0)
    ap.add_argument("--not-before", metavar="'YYYY-MM-DD HH:MM'", help="wait until this local time to start")
    ap.add_argument("--dry-run", action="store_true")
    return ap


if __name__ == "__main__":
    try:
        sys.exit(drive(parser().parse_args()))
    except (ValueError, KeyError, OSError, RuntimeError) as exc:
        log(f"STOPPED: {exc}")
        sys.exit(EXIT_FAIL)
