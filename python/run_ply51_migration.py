"""David's November 2026 migration. Run from the pinned directory with the project's venv."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import shutil
import subprocess
import sys
import time
from pathlib import Path

import psutil

from month_archive import read_json, root_lock, sha, write_json
from run_month_extraction import parse_months, tag

PILOTS = ["2026_3", "2024_6", "2020_4"]
TRAIN = [tag(ym) for ym in parse_months(["2013_1..2026_7"])]
PIN = Path(__file__).resolve().parent
SOURCE = Path("D:/data/chess/standard-chess-games-compressed")
REFERENCE = SOURCE / '_source_metadata/inventory-v1.json'
ARCHIVE = Path("F:/chess/explorer_brc_p51_pilot")
STAGE = Path("D:/chess/explorer_brc_p51_pilot_stage")
OPS = Path("D:/chess/ply51-migration-20261115")
PILOT_BOOK = Path("E:/chess/position-stats/brc-p51-pilot-20261115")
TRAIN_BOOK = Path("E:/chess/position-stats/brc-p51-train-201301-202607-20261115")
EXE = PIN / "explorer-extract.exe"
COMPACT_PIN = Path(__file__).resolve().parent
COMPACT_BOOK = OPS / "compact-pilot-book"
PILOT_ONLY = False
START = dt.datetime(2026, 11, 16)
END = dt.datetime(2026, 11, 20)
GB = 10**9


def status(stage, **details):
    record = {"time": dt.datetime.now().isoformat(timespec="seconds"), "stage": stage, **details}
    write_json(OPS / ("compact-pilot-status.json" if PILOT_ONLY else "status.json"), record)
    print(json.dumps(record), flush=True)


def run(*args):
    subprocess.run([str(x) for x in args], check=True)


def check_pins():
    for filename in ("pin.json", "migration-pin.json"):
        for name, digest in read_json(PIN / filename)["files"].items():
            if sha(PIN / name) != digest.lower():
                raise ValueError(f"{name}: content differs from {filename}")


def wait_pilot(pid):
    runner = PIN / "run_ply51_pilot.ps1"
    if pid and psutil.pid_exists(pid):
        proc = psutil.Process(pid)
        if not any(Path(arg).name.lower() == runner.name and Path(arg).resolve() == runner for arg in proc.cmdline()):
            raise ValueError(f"PID {pid} belongs to another process")
        status("waiting for three measured pilot months", coordinator_pid=pid)
        proc.wait()
    report = PIN / "pilot-measurements.json"
    results = read_json(report) if report.exists() else []
    if [r["Month"] for r in results] != PILOTS:
        if pid:
            raise ValueError("pilot exited before publishing all three measurements; see pilot.err.log")
        run("pwsh.exe", "-NoProfile", "-File", runner)
        results = read_json(report)
    if [r["Month"] for r in results] != PILOTS:
        raise ValueError("incomplete pilot measurements")
    return results


def extract_command(months, *, dry=False):
    args = [sys.executable, "-u", PIN / "run_month_extraction.py", "--exe", EXE, "--source", SOURCE,
            "--source-manifest", REFERENCE, "--months", *months, "--stage", STAGE, "--dest", ARCHIVE,
            "--events", "Blitz", "Classical", "Rapid", "--max-ply", "51", "--key128", "--term-slices",
            "--threads", "12", "--mem-gb", "55", "--verify", "hash"]
    if dry:
        args.append("--dry-run")
    return args


def extract(months, *, dry=False):
    run(*extract_command(months, dry=dry))


def receipt(month, inventory):
    if not (ARCHIVE / f"_month={month}.DONE").is_file():
        raise ValueError(f"missing archive publication for {month}")
    rec = read_json(ARCHIVE / "_archive_receipts" / f"{month}.json")
    source = {k: inventory[month][k] for k in ("files", "absent_events")}
    if rec["source"] != source or rec["verify"] != "hash" or not rec["files"]:
        raise ValueError(f"{month}: published receipt differs from the reconciled training inputs")
    return rec


def term_size(months):
    total = 0
    for month in months:
        year, number = month.split("_")
        total += (ARCHIVE / "_term" / f"year={year}_month={number}.term.parquet").stat().st_size
    return total


def size_tree(root):
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def free(path):
    return shutil.disk_usage(path.anchor).free


def merge(months, book, label, stage_gb, *, compact=False, buckets=()):
    exe = COMPACT_PIN / "explorer-extract.exe" if compact else EXE
    args = [exe, "merge", "--months-root", ARCHIVE, "--months", *months, "--out", book,
            "--stage-dir", OPS / f"{label}-merge-stage", "--stage-gb", str(stage_gb),
            "--threads", "8", "--min-free-gb", "150"]
    if compact:
        args.append("--compact")
    if buckets:
        args += ["--buckets", ",".join(map(str,buckets))]
    run(*args)
    if not buckets:
        check_book(book, months, compact=compact)


def check_book(book, months, *, compact=False):
    if not (book / "_BOOK.DONE").is_file():
        raise ValueError(f"{book}: no completed book")
    lock = read_json(book / "_merge_params.json")
    if (lock["schema"] != ("book-v4" if compact else "book-v3") or len(lock["months"]) != len(months)
            or set(lock["months"]) != set(months) or not lock["input_params"]["term_slices"]
            or lock["input_params"]["max_ply"] != 51 or
            Path(lock["months_root"]).resolve() != ARCHIVE.resolve()):
        raise ValueError(f"{book}: merged population or schema differs")


def verify_book(book, *, compact=False):
    pin = COMPACT_PIN if compact else PIN
    run(sys.executable, "-u", pin / "verify_book.py", book, ARCHIVE, "--sums", "--term-digest",
        "--digest-buckets", "0,251,511", "--dup-buckets", "0,251,511", "--collisions", "--sample", "3000",
        "--threads", "2", "--workers", "2", "--tmp-dir", OPS / "verify-tmp")


def forecast(rows, inventory, pilot_output):
    games = {m: sum(i["games"].values()) for m, i in inventory.items()}
    if any(games[r["Month"]] != r["SourceGames"] for r in rows):
        raise ValueError("pilot source totals differ from the reconciled inputs")
    g = sum(r["SourceGames"] for r in rows)
    a = sum(r["ArchiveGB"] * GB for r in rows)
    rates = [r["ArchiveGB"] * GB / r["SourceGames"] for r in rows]
    hi = max(rates) * 1.20
    seconds = [r["ExtractSeconds"]/r["SourceGames"] for r in rows]
    copy = sum(r["StreamWriteSeconds"] + r["ReadbackSeconds"] for r in rows) / a
    all_games = sum(games.values())
    return {"source_games": all_games, "pilot_games": g, "pilot_archive_bytes": round(a),
            "pilot_book_bytes": pilot_output, "archive_bytes_central": round(all_games * a / g),
            "archive_bytes_planning": round(all_games * hi),
            "archive_planning_bytes_per_game": hi, "pilot_archive_bytes_per_game_range": [min(rates), max(rates)],
            "pilot_book_to_archive_ratio": pilot_output / a,
            "production_book_bytes_planning": round(all_games * hi * pilot_output/a * 1.10),
            "extraction_hours_central": round(all_games * sum(r["ExtractSeconds"] for r in rows)/g/3600, 1),
            "archive_copy_and_readback_hours_central": round(all_games*a/g*copy/3600, 1),
            "planning_serial_secs_per_game": 1.20*max(seconds) + hi*copy,
            "model": "Measured pilot bytes and seconds per game; archive planning adds 20% over the largest observed bytes/game. The book adds 10% to the pilot compaction ratio. Early small months may differ."}


def rehearse(results, inventory):
    caps = read_json(PIN / "direct-caps.json")
    if (caps["exe_sha256"] != sha(EXE) or caps["check_sha256"] != sha(PIN / "verify_ply51_caps.py")
            or [r["ply"] for r in caps["results"]] != [30, 50]):
        raise ValueError("no direct cap comparison for these inputs")
    for m in PILOTS:
        receipt(m, inventory)
    status("restarting March against its published archive")
    # A missing publication is an error above. This must be a genuine no-op extraction.
    extract([PILOTS[0]])
    size = sum(r["ArchiveGB"] * GB for r in results)
    if free(PILOT_BOOK) + size_tree(PILOT_BOOK) < size * 1.25 + 150*GB:
        raise ValueError("insufficient E: space for the pilot merge")
    stage_gb = max(25, math.ceil(term_size(PILOTS)/GB) + 10)
    if free(OPS) < (stage_gb + 150)*GB:
        raise ValueError(f"pilot needs {stage_gb} GB staging plus 150 GB free")
    status("merging and checking three-month pilot", stage_gb=stage_gb)
    started = time.monotonic()
    merge(PILOTS, PILOT_BOOK, "pilot", stage_gb)
    merge_seconds = time.monotonic() - started
    verify_book(PILOT_BOOK)
    r = forecast(results, inventory, size_tree(PILOT_BOOK))
    r["pilot_merge_wall_seconds_this_invocation"] = merge_seconds
    write_json(OPS / "forecast.json", r)
    write_json(OPS / "pilot-verified.json", {"pin": sha(PIN/"migration-pin.json"),
               "measurements": sha(PIN/"pilot-measurements.json"), "meta": sha(PILOT_BOOK/"_book.meta.json"),
               "forecast": sha(OPS/"forecast.json"), "caps": sha(PIN/"direct-caps.json")})
    return r



def compact_identity():
    return {"pin": sha(COMPACT_PIN/"pin.json"), "original_certificate": sha(OPS/"pilot-verified.json"),
            "pilot_meta": sha(COMPACT_BOOK/"_book.meta.json"), "comparison": sha(OPS/"compact-comparison.json"),
            "forecast": sha(OPS/"compact-forecast.json"), "eval_benchmark": sha(OPS/"compact-eval-benchmark.json")}


def certified_compact_forecast():
    certificate = OPS/"compact-verified.json"
    if not certificate.is_file():
        raise ValueError("all monthly archives retained; compact merge requires the pilot verification receipt")
    check_book(COMPACT_BOOK, PILOTS, compact=True)
    if read_json(certificate) != compact_identity():
        raise ValueError("compact pilot validation inputs changed")
    return read_json(OPS/"compact-forecast.json")


def compact_prediction(results, inventory):
    report = OPS / "compact-comparison.json"
    prediction = OPS / "compact-forecast.json"
    certificate = OPS / "compact-verified.json"
    if certificate.exists():
        return certified_compact_forecast()
    stage_gb = max(25, math.ceil(term_size(PILOTS)/GB) + 10)
    # Stage and the second pilot share D:. Keep the production destination on E: free.
    if free(COMPACT_BOOK) + size_tree(COMPACT_BOOK) < size_tree(PILOT_BOOK) + (stage_gb+150)*GB:
        raise ValueError("insufficient space for the compact pilot, staging and reserve on D:")
    status("rehearsing compact three-month explorer", book=str(COMPACT_BOOK), stage_gb=stage_gb)
    benchmark = OPS/"compact-eval-benchmark.json"
    if not benchmark.is_file():
        merge(PILOTS, COMPACT_BOOK, "compact-pilot", stage_gb, compact=True, buckets=(7,201,421))
    run(sys.executable, "-u", COMPACT_PIN/"benchmark_compact_evals.py", COMPACT_PIN/"explorer-extract.exe",
        PILOT_BOOK, COMPACT_BOOK, Path("H:/chess/evals/cloud_2026-10-07/data_0000.parquet"),
        Path("H:/chess/evals/fishnet/standard_rated_2025_03.parquet"), benchmark)
    merge(PILOTS, COMPACT_BOOK, "compact-pilot", stage_gb, compact=True)
    status("verifying compact pilot against the archives and original explorer", book=str(COMPACT_BOOK))
    verify_book(COMPACT_BOOK, compact=True)
    run(sys.executable, "-u", COMPACT_PIN/"verify_compact_book.py", PILOT_BOOK, COMPACT_BOOK, report)
    r = forecast(results, inventory, size_tree(COMPACT_BOOK))
    r["production_book_bytes_central"] = round(r["archive_bytes_central"] * r["pilot_book_to_archive_ratio"])
    r["original_pilot_bytes"] = size_tree(PILOT_BOOK)
    r["measured_whole_book_saving"] = 1 - r["pilot_book_bytes"]/r["original_pilot_bytes"]
    write_json(prediction, r)
    write_json(certificate, compact_identity())
    return r

def production(inventory, prediction, *, compact=False):
    todo = []
    for m in TRAIN:
        if (ARCHIVE / f"_month={m}.DONE").exists():
            receipt(m, inventory)
        else:
            todo.append(m)
    previously_published = [m for m in TRAIN if m not in todo and m not in PILOTS]
    if previously_published:
        status("revalidating previously published training months", months=len(previously_published))
        extract(previously_published, dry=True)
    remaining_games = sum(sum(inventory[m]["games"].values()) for m in todo)
    estimate = remaining_games * prediction["archive_planning_bytes_per_game"]
    if free(ARCHIVE) < estimate + 100*GB:
        raise ValueError(f"F: free {free(ARCHIVE)/GB:.0f} GB, estimated remaining archive {estimate/GB:.0f} GB plus 100 GB reserve")
    status("ready for training extraction", scheduled=START.isoformat(),
           remaining_months=len(todo), forecast=str(OPS/"forecast.json"),
           production_layout="book-v4" if compact else "book-v3",
           merge_forecast=str(OPS/("compact-forecast.json" if compact else "forecast.json")))
    while dt.datetime.now() < START:
        time.sleep(min(60, max(0, (START-dt.datetime.now()).total_seconds())))
    # Batches allow the existing launcher to overlap D: extraction and F: archival.
    # Keep the operational window bounded without interrupting a month being published.
    while todo:
        available = (END - dt.datetime.now()).total_seconds()
        batch = []
        budget = 0.
        for m in todo[:12]:
            cost = sum(inventory[m]["games"].values()) * prediction["planning_serial_secs_per_game"] + 60.
            if budget + cost > available or (batch and budget + cost > 4*3600):
                break
            batch.append(m)
            budget += cost
        if not batch:
            status("training extraction checkpointed at end of reserved window", remaining=len(todo))
            return
        status("extracting training months", batch=batch, remaining=len(todo))
        extract(batch)
        for m in batch:
            receipt(m, inventory)
        todo = todo[len(batch):]
    status("all training months archived; checking merge capacity")
    if compact:
        prediction = certified_compact_forecast()
    actual = sum(receipt(m, inventory)["bytes"] for m in TRAIN)
    needed = actual * prediction["pilot_book_to_archive_ratio"] * 1.10
    if free(TRAIN_BOOK)+size_tree(TRAIN_BOOK) < needed + 150*GB:
        raise ValueError(f"E: projected training book {needed/GB:.0f} GB plus 150 GB reserve exceeds available space")
    stage_gb = max(100, math.ceil(term_size(TRAIN)/GB) + 20)
    if free(OPS) < (stage_gb+150)*GB:
        raise ValueError(f"D: training merge needs {stage_gb} GB staging plus 150 GB reserve")
    if dt.datetime.now() >= END:
        status("training archive complete; merge outside reserved window")
        return
    status("merging training explorer", months=len(TRAIN), stage_gb=stage_gb)
    merge(TRAIN, TRAIN_BOOK, "training", stage_gb, compact=compact)
    verify_book(TRAIN_BOOK, compact=compact)
    status("training explorer verified", book=str(TRAIN_BOOK), meta_sha256=sha(TRAIN_BOOK/"_book.meta.json"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    global PIN, EXE, PILOT_ONLY, COMPACT_PIN
    parser.add_argument("--pilot-pid", type=int)
    parser.add_argument("--compact", action="store_true", help="use the separately pinned compact merger and retain the original extraction producer")
    parser.add_argument("--compact-pilot-only", action="store_true", help="rehearse separately while the original scheduler waits")
    parser.add_argument("--compact-release", type=Path, help="immutable compact merge/eval release used by the pilot")
    args = parser.parse_args()
    if args.compact_release:
        COMPACT_PIN = args.compact_release.resolve()
        args.compact = True
    PILOT_ONLY = args.compact_pilot_only
    if PILOT_ONLY:
        args.compact = True
    if args.compact:
        PIN = Path("D:/chess/bin/ply51-pilot-20261115")
        EXE = PIN/"explorer-extract.exe"
    with root_lock(OPS/"compact-rehearsal" if PILOT_ONLY else OPS):
        try:
            check_pins()
            if args.compact:
                for release in {Path(__file__).resolve().parent, COMPACT_PIN}:
                    for name, digest in read_json(release/"pin.json")["files"].items():
                        if sha(release/name) != digest:
                            raise ValueError(f"compact pin mismatch: {release/name}")
            data = read_json(PIN / "training-inputs.json")
            inventory = data["months"]
            if set(inventory) != set(TRAIN) or Path(data["source"]).resolve() != SOURCE.resolve():
                raise ValueError("training inventory mismatch")
            results = wait_pilot(args.pilot_pid)
            cert = OPS/"pilot-verified.json"
            if cert.exists():
                recorded = read_json(cert)
                expected = {"pin": sha(PIN/"migration-pin.json"), "measurements": sha(PIN/"pilot-measurements.json"),
                            "meta": sha(PILOT_BOOK/"_book.meta.json"), "forecast": sha(OPS/"forecast.json"),
                            "caps": sha(PIN/"direct-caps.json")}
                if recorded != expected:
                    raise ValueError("pilot validation inputs changed")
                check_book(PILOT_BOOK, PILOTS)
                prediction = read_json(OPS/"forecast.json")
            else:
                prediction = rehearse(results, inventory)
            if PILOT_ONLY:
                compact_prediction(results, inventory)
                status("compact pilot verified", book=str(COMPACT_BOOK), forecast=str(OPS/"compact-forecast.json"))
                return
            production(inventory, prediction, compact=args.compact)
        except Exception as exc:
            status("stopped", error=str(exc))
            raise


if __name__ == "__main__":
    main()
