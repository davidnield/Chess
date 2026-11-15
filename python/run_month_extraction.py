"""Drive `explorer-extract month` over many months: extract each month into a fast staging root,
move it to its archive root in the background, and write the archive's sentinel last.

STATUS (2026-11-14): DRAFT, NEVER RUN on real months, and NOT SAFE for the full extraction until
the migration review's finding 2 (docs/repertoire-migration-review-2026-11-14.md) is fixed:
  - a month with an archive sentinel is skipped before the requested settings are compared with
    either root (reproduced: an empty dest holding only `_month=2026_1.DONE`, a request for
    --max-ply 51, exit 0);
  - a staged month with its sentinel is published without running the extractor, so the
    extractor's own settings check is bypassed;
  - --stage and --dest may resolve to the same directory, and move_month then deletes the just
    extracted month before copying it;
  - no test covers a crash between the destination rename, the sentinel and the stage cleanup.
The --events order check below does work (refuses Blitz Rapid Classical, exit 1).

    python run_month_extraction.py --exe <explorer-extract.exe> --months 2013_1..2026_9
        --stage D:/chess/explorer_p50_stage --dest F:/chess/explorer_brc_p50
        --events Blitz Classical Rapid --max-ply 50 --key128
        [--mem-gb 55] [--threads 12] [--not-before "2026-11-16 00:00"] [--dry-run]

Why two roots. The extraction writes about 500 files of a month at once and re-reads each one;
the archive is a USB hard disk. Staging on NVMe keeps the extraction at its own speed and turns
the archive's work into one sequential copy per month.

One month at a time, one `month` process each (a failed month stops the run; the months before it
are already safe). A month is DONE when `<dest>/_month=Y_M.DONE` exists, and only then:

  extract   `<exe> month --out <stage> --months Y_M ...`  (the tool's own sentinel in <stage>)
  copy      month=Y_M/ into <dest>/_tmp_month=Y_M/, then one rename; the manifest, provenance,
            term monthly and any conflicts through `.tmp` names; every file's size is checked
            (--verify hash also re-reads each copy and compares SHA-256: about twice the
            archive I/O. The merge validates every row it reads against the month's manifest
            either way, so a bad copy cannot enter a book; it costs that month's re-extraction.)
  sentinel  `<dest>/_month=Y_M.DONE`, a copy of the tool's, written LAST
  release   the staged month is deleted

`merge` refuses a root holding a `_tmp_month=` dir or a month without its sentinel, so an
interrupted copy can never be merged. `_extract_params.json` (the tool's settings lock) is copied
to <dest> with the first month and must be identical for every later one.

Resuming: launch the same command again. Months with an archive sentinel are skipped, a month
staged but not yet moved is moved, and anything else is extracted (the tool cleans up its own
unfinished month). Back-pressure: extraction waits while more than --max-staged-gb sits unmoved.
Exit status: 0 done, 1 a month failed or a copy did not verify, 4 out of space (resumable).

The events must be a subset of the six speeds in the merge's own order (Blitz, Bullet, Classical,
Correspondence, Rapid, UltraBullet): a month sorts its rows by an event's position in the list
it was given, and `merge` refuses any other order -- after the extraction. Refused here, before.

Every month appends a line to <dest>/_extraction_log.jsonl: games, rows, bytes, extract and copy
seconds and the copy's MB/s (the archive's measured write speed).
"""
from __future__ import annotations

import argparse
import datetime
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


def copy_file(src: Path, dst: Path, verify: str) -> int:
    """src -> dst through dst's .tmp name; returns the bytes. Raises if the copy differs."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    h = hashlib.sha256() if verify == "hash" else None
    n = 0
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
    if h and sha256(tmp) != h.hexdigest():
        raise OSError(f"{dst}: the copy's SHA-256 differs from the source's")
    tmp.replace(dst)
    return n


def tree_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file())


def move_month(stage: Path, dest: Path, ym, verify: str) -> dict:
    """Copy a staged month to the archive, sentinel last, then release the staged copy."""
    s, d = month_parts(stage, ym), month_parts(dest, ym)
    t0 = time.monotonic()
    lock_s, lock_d = stage / "_extract_params.json", dest / "_extract_params.json"
    want = json.loads(lock_s.read_text(encoding="utf-8"))
    if lock_d.exists():
        if json.loads(lock_d.read_text(encoding="utf-8")) != want:
            raise RuntimeError(f"{lock_d} differs from {lock_s}: the archive holds months of other settings")
    else:
        copy_file(lock_s, lock_d, verify)
    tmp_dir = dest / f"_tmp_month={tag(ym)}"
    if d["dir"].exists():                     # a copy whose sentinel was never written: redo it
        shutil.rmtree(d["dir"])
    shutil.rmtree(tmp_dir, ignore_errors=True)
    files = tree_files(s["dir"])
    if not files:
        raise RuntimeError(f"{s['dir']}: no files")
    nbytes = sum(copy_file(f, tmp_dir / f.relative_to(s["dir"]), verify) for f in files)
    if [p.relative_to(tmp_dir) for p in tree_files(tmp_dir)] != [f.relative_to(s["dir"]) for f in files]:
        raise OSError(f"{tmp_dir}: the copied tree differs from {s['dir']}")
    tmp_dir.replace(d["dir"])
    for k in ("manifest", "provenance", "term"):
        nbytes += copy_file(s[k], d[k], verify)
    if s["conflicts"].exists():
        shutil.rmtree(d["conflicts"], ignore_errors=True)
        for f in tree_files(s["conflicts"]):
            nbytes += copy_file(f, d["conflicts"] / f.relative_to(s["conflicts"]), verify)
    copy_file(s["sentinel"], d["sentinel"], verify)                # LAST
    secs = time.monotonic() - t0
    shutil.rmtree(s["dir"])
    shutil.rmtree(s["conflicts"], ignore_errors=True)
    for k in ("manifest", "provenance", "term", "sentinel"):
        s[k].unlink()
    return {"files": len(files) + 3, "bytes": nbytes, "copy_seconds": round(secs, 1),
            "copy_mb_per_s": round(nbytes / 1e6 / max(secs, 1e-9), 1)}


def month_command(a, ym) -> list[str]:
    cmd = [*a.exe, "month", "--source", str(a.source), "--months", tag(ym), "--events", *a.events,
           "--max-ply", str(a.max_ply), "--ply-key", "--out", str(a.stage),
           "--mem-gb", str(a.mem_gb), "--threads", str(a.threads)]
    if a.key128:
        cmd.append("--key128")
    return cmd + list(a.month_arg)


def drive(a) -> int:
    months = parse_months(a.months)
    if not mergeable_events(a.events):
        log(f"REFUSED: --events {a.events} is not a subset of {MERGE_EVENTS} in that order; "
            f"`merge` would refuse these months after the extraction")
        return EXIT_FAIL
    todo = [ym for ym in months if not month_parts(a.dest, ym)["sentinel"].exists()]
    log(f"{len(months)} months, {len(months) - len(todo)} already in {a.dest}, {len(todo)} to do")
    for ym in todo[:3] if a.dry_run else ():
        log("would run: " + subprocess.list2cmdline(month_command(a, ym)))
    if a.dry_run or not todo:
        return 0
    if a.not_before:
        start = datetime.datetime.fromisoformat(a.not_before)
        while datetime.datetime.now() < start:
            log(f"waiting until {start} to start")
            time.sleep(min(600, max(1, (start - datetime.datetime.now()).total_seconds())))
    a.stage.mkdir(parents=True, exist_ok=True)
    a.dest.mkdir(parents=True, exist_ok=True)

    jobs: queue.Queue = queue.Queue()
    state = {"error": None, "staged_bytes": 0, "space": False}
    lock = threading.Lock()

    def staged_bytes(ym) -> int:
        return sum(f.stat().st_size for f in tree_files(month_parts(a.stage, ym)["dir"]))

    def mover():
        while (item := jobs.get()) is not None:
            ym, rec, size = item
            try:
                if state["error"] is None:
                    if free_gb(a.dest) - size / 1e9 < a.min_dest_free_gb:
                        state["space"] = True
                        raise RuntimeError(f"{a.dest}: under {a.min_dest_free_gb} GB would be left")
                    rec.update(move_month(a.stage, a.dest, ym, a.verify))
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
        prov = json.loads(s["provenance"].read_text(encoding="utf-8"))
        rec = {"month": tag(ym), "extract_seconds": round(secs, 1), "rows": prov.get("rows"),
               "games_kept": (prov.get("counters") or {}).get("kept"), "passes": prov.get("passes"),
               "peak_commit_gb": round((prov.get("peak_commit_bytes") or 0) / 1e9, 1),
               "tool_commit": prov.get("commit")}
        log(f"{tag(ym)}: extracted in {secs:,.0f}s ({rec['rows']:,} rows, {rec['passes']} passes, "
            f"peak commit {rec['peak_commit_gb']} GB)")
        enqueue(ym, rec)
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
    ap.add_argument("--mem-gb", type=float, default=55.0)
    ap.add_argument("--threads", type=int, default=12)
    ap.add_argument("--month-arg", action="append", default=[], metavar="ARG",
                    help="an extra `month` argument (repeatable), e.g. --month-arg=--keys-per-game=40")
    ap.add_argument("--verify", choices=["size", "hash"], default="size")
    ap.add_argument("--max-staged-gb", type=float, default=400.0,
                    help="extraction waits while more than this is staged and unmoved")
    ap.add_argument("--min-stage-free-gb", type=float, default=200.0)
    ap.add_argument("--min-dest-free-gb", type=float, default=100.0)
    ap.add_argument("--not-before", metavar="'YYYY-MM-DD HH:MM'", help="wait until this local time to start")
    ap.add_argument("--dry-run", action="store_true")
    return ap


if __name__ == "__main__":
    sys.exit(drive(parser().parse_args()))
