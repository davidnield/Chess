"""Run the separate Cloud/Fishnet generation after the verified training explorer is published."""
from __future__ import annotations

import datetime as dt
import math
import shutil
import subprocess
import sys
import time
from pathlib import Path

import chess
import numpy as np
import pyarrow.parquet as pq

from eval_lookup128 import CLOUD, FISHNET, TERMINAL, ROW, WideEvalLookup
from month_archive import read_json, root_lock, sha, write_json

PIN = Path(__file__).resolve().parent
OPS = Path("D:/chess/ply51-migration-20261115")
STATE = OPS / "evals"
BOOK = Path("E:/chess/position-stats/brc-p51-train-201301-202607-20261115")
CLOUD_DIR = Path("H:/chess/evals/cloud_2026-10-07")
FISH_DIR = Path("H:/chess/evals/fishnet")
WORK = Path("H:/chess/eval-work-p51-20261115")
DB = Path("D:/chess/eval-db-p51-20261115")
LOOKUP = Path("D:/chess/eval-lookup-p51-20261115")
PILOT_WORK = WORK.with_name(WORK.name + "-sample")
PILOT_DB = DB.with_name(DB.name + "-sample")
BUCKETS = list(range(7, 512, 64))
GB = 10**9


def status(stage, **details):
    info = {"stage": stage, "time": dt.datetime.now().isoformat(timespec="seconds"), **details}
    write_json(STATE / "status.json", info)
    print(info, flush=True)


def space(path):
    return shutil.disk_usage(path.anchor).free


def guarded(args, drives):
    command = [str(arg) for arg in args]
    with subprocess.Popen(command, cwd=STATE) as child:
        while child.poll() is None:
            for path in drives:
                if space(path) < 80 * GB:
                    child.terminate()
                    child.wait()
                    raise RuntimeError(f"{path.anchor} below 80 GB free while running {command[0]}; completed units retained")
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                pass
        if child.returncode:
            raise subprocess.CalledProcessError(child.returncode, command)


def wait_book():
    status("waiting for the verified production explorer")
    while True:
        marker = OPS / "status.json"
        value = read_json(marker) if marker.exists() else {}
        if value.get("stage") == "training explorer verified":
            if (Path(value["book"]).resolve() != BOOK.resolve()
                    or value["meta_sha256"] != sha(BOOK / "_book.meta.json")
                    or not (BOOK / "_BOOK.DONE").is_file()):
                raise ValueError("production explorer verification receipt differs")
            return
        if value.get("stage") in {"stopped", "training extraction checkpointed at end of reserved window",
                                  "training archive complete; merge outside reserved window"}:
            raise RuntimeError(f"upstream coordinator stopped: {value}")
        time.sleep(60)


def native(work, out, sample=False, drives=()):
    args = [PIN/"explorer-extract.exe", "evals", "--book", BOOK, "--cloud", CLOUD_DIR,
            "--fishnet", FISH_DIR, "--work", work, "--out", out,
            "--threads", 12, "--mem-gb", 55, "--below-normal"]
    if sample:
        args.extend(["--buckets", ",".join(map(str, BUCKETS))])
    guarded(args, [WORK, DB, *drives])
    meta = read_json(out / "_build.meta.json")
    if (not (out/"_DONE").is_file() or meta["schema"] != "eval-db-v2"
            or meta["inputs"]["book_meta_sha256"] != sha(BOOK/"_book.meta.json")
            or meta["params"]["child_sources"] != "all"
            or meta["params"]["buckets"] != (BUCKETS if sample else "all")):
        raise ValueError(f"incomplete or wrong eval generation at {out}")


def choose_drive(available, e, c, final):
    # Phase E stays by the raw sources. Phase C can use the same drive or one other drive.
    remaining = dict(available)
    remaining["H:"] -= e
    remaining["D:"] -= final
    if min(remaining["D:"], remaining["H:"]) < 0:
        raise ValueError(f"capacity shortfall after source shards and output: {remaining}")
    choices = [drive for drive in ("H:", "D:", "E:") if remaining[drive] >= c]
    if not choices:
        raise ValueError(f"child shards need {c/GB:.0f} GB; remaining bytes after reserves: {remaining}")
    return choices[0]


def size(path):
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def forecast():
    counts = pq.ParquetFile(PILOT_DB/"_manifest.parquet").read(columns=["rows", "bytes"]).to_pylist()
    scale = 512 / len(BUCKETS) * 1.20
    e = math.ceil(size(PILOT_WORK/"e") * scale)
    c = math.ceil(size(PILOT_WORK/"c") * scale)
    # DB plus full-key lookup (19 bytes/row), mate-only rows and temporary sorting/spill.
    final = math.ceil(sum(r["bytes"] + ROW.itemsize*r["rows"] for r in counts) * scale + 90*GB)
    amounts = {drive: space(Path(drive+"/")) - 150*GB for drive in ("D:", "E:", "H:")}
    report = {"pilot_buckets": BUCKETS, "all_child_sources": True, "proportional_margin": 0.20,
              "phase_e_bytes": e, "phase_c_bytes": c, "db_lookup_and_spill_bytes": final,
              "available_after_150GB_reserve": amounts, "book_meta_sha256": sha(BOOK/"_book.meta.json")}
    write_json(STATE/"capacity.json", report)
    choice = choose_drive(amounts, e, c, final)
    report["phase_c_drive"] = choice
    write_json(STATE/"layout.json", report)
    return report


def attach_child_folder(drive):
    WORK.mkdir(exist_ok=True, parents=True)
    logical = WORK / "c"
    if drive != "H:":
        physical = Path(drive+"/chess/eval-work-p51-20261115-child")
        physical.mkdir(parents=True, exist_ok=True)
        if not logical.exists():
            if any(physical.iterdir()):
                raise ValueError(f"unbound child shard folder {physical}")
            # Paths are fixed above. Use the PowerShell junction cmdlet with literal path arguments.
            subprocess.run(["powershell.exe", "-NoProfile", "-Command",
                f"New-Item -ItemType Junction -Path '{logical}' -Target '{physical}' -ErrorAction Stop | Out-Null"], check=True)
        if logical.resolve() != physical.resolve():
            raise ValueError(f"child shard path {logical} points elsewhere")
    elif logical.exists() and logical.resolve() != logical.absolute():
        raise ValueError(f"expected a local child shard folder at {logical}")


def sample_lookup():
    meta = read_json(LOOKUP / "lookup.json")
    if len(meta["buckets"]) != 512:
        raise ValueError("production lookup must publish all 512 buckets")
    lookup = WideEvalLookup(LOOKUP)
    rng = np.random.default_rng(20261115)
    checked = mate_checked = 0
    for b in range(512):
        pf = pq.ParquetFile(DB/f"bkt{b:03d}.parquet")
        if not pf.metadata.num_rows:
            continue
        group = int(rng.integers(pf.num_row_groups))
        rows = next(pf.iter_batches(batch_size=16, row_groups=[group],
                    columns=["position_hash","position_hash_hi","source","epd","eval_cp"])).to_pylist()
        for r in rows:
            cp, source = lookup.lookup(r["position_hash"],r["position_hash_hi"])
            if int(source) == TERMINAL:
                board = chess.Board(r["epd"])
                if not board.is_checkmate() or int(cp) != (-2000 if board.turn else 2000):
                    raise ValueError(f"bad terminal override {r}")
            elif (int(cp),int(source)) != (r["eval_cp"], {"cloud":CLOUD,"fishnet":FISHNET}[r["source"]]):
                raise ValueError(f"lookup disagrees with DB at {r}")
            checked += 1
        mate = pq.ParquetFile(LOOKUP/"mate-shards"/f"bkt{b:03d}.parquet")
        if mate.metadata.num_rows:
            for r in next(mate.iter_batches(batch_size=4)).to_pylist():
                cp, source = lookup.lookup(r["lo"],r["hi"])
                if (int(cp), int(source)) != (2000 if r["pmin"] else -2000, TERMINAL):
                    raise ValueError(f"lookup missed mate {r}")
                mate_checked += 1
    if not checked or not mate_checked:
        raise ValueError("empty production lookup verification sample")
    return {"sampled_eval_rows": checked, "sampled_known_mates": mate_checked}


def main():
    with root_lock(STATE):
        try:
            for name, digest in read_json(PIN/"pin.json")["files"].items():
                if sha(PIN/name) != digest:
                    raise ValueError(f"pin mismatch: {name}")
            wait_book()
            layout = STATE/"layout.json"
            if not layout.exists():
                status("measuring eval disk use on eight distributed buckets with all child sources")
                native(PILOT_WORK, PILOT_DB, sample=True)
                plan = forecast()
            else:
                plan = read_json(layout)
            if plan["book_meta_sha256"] != sha(BOOK/"_book.meta.json"):
                raise ValueError("eval capacity plan belongs to another explorer")
            drive = plan["phase_c_drive"]
            attach_child_folder(drive)
            status("building full Cloud/Fishnet eval DB", child_shards=drive)
            native(WORK, DB, drives=[Path(drive+"/")])
            status("independently verifying full-key DB and source selection")
            temp = STATE/"verify-tmp"
            temp.mkdir(exist_ok=True)
            guarded([sys.executable,"-u",PIN/"verify_evals.py",DB,"--book",BOOK,"--cloud",CLOUD_DIR,
                     "--fishnet",FISH_DIR,"--threads",8,"--mem","24GB","--tmp-dir",temp], [DB])
            status("publishing 128-bit lookup with terminal corrections")
            guarded([sys.executable,"-u",PIN/"eval_lookup128.py","--eval-db",DB,"--book",BOOK,"--out",LOOKUP], [DB])
            status("eval DB and lookup verified", db=str(DB), lookup=str(LOOKUP),
                   db_meta_sha256=sha(DB/"_build.meta.json"), lookup_sha256=sha(LOOKUP/"lookup.json"),
                   **sample_lookup())
        except Exception as exc:
            status("stopped", error=str(exc))
            raise


if __name__ == "__main__":
    main()
