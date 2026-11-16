"""Start missing workers after signing in on Monday. Run with the project venv."""
from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
import time

import psutil

import run_ply51_migration as m
from run_ply51_sunday import COMPACT, RELEASE, check_release
from month_archive import read_json, root_lock, sha

QUEUE = Path("D:/chess/bin/ply51-queue-20261115")
EVAL_RELEASE = Path("D:/chess/bin/ply51-evals-fallback-20261115")
HOLDOUT = m.OPS/"holdout-inputs"
CACHE = Path(os.environ["LOCALAPPDATA"])/"Temp/chess-holdout-pgn-20261115"
RECIPE = Path("C:/Users/David/Documents/Chess/python/process_pgn_parquets.py")


def active(script, *, marker=None):
    for proc in psutil.process_iter(["name", "cmdline"]):
        if (proc.info["name"] or "").lower() not in {"python.exe","pythonw.exe"}:
            continue
        args = proc.info["cmdline"] or []
        normalized = [str(arg).replace("\\","/").casefold() for arg in args]
        target = script.as_posix().casefold()
        # The original holdout launcher used a relative path; identify its output too.
        matches = target in normalized
        if marker is not None:
            matches = any(Path(a).name.lower()==script.name.lower() for a in args) and marker.as_posix().casefold() in normalized
        if matches:
            return True
    return False


def launch(script, args, log, *, marker=None):
    if active(script, marker=marker):
        print(f"Already running: {script.name}")
        return
    with (m.OPS/(log+".log")).open("ab") as out, (m.OPS/(log+".err")).open("ab") as err:
        process = subprocess.Popen([sys.executable,"-X","utf8","-u",str(script),*map(str,args)],
                                   cwd=script.parent, stdout=out, stderr=err, stdin=subprocess.DEVNULL,
                                   creationflags=subprocess.CREATE_NO_WINDOW)
    print(f"Started {script.name}, PID {process.pid}. Logs: {m.OPS/log}", flush=True)


def main(check_only):
    check_release()
    manifest = read_json(QUEUE/"pin.json")
    if manifest["compact_pin"] != sha(COMPACT/"pin.json"):
        raise ValueError("training queue points to another compact release")
    if sha(EVAL_RELEASE/"pin.json") != read_json(RELEASE/"pin.json")["eval_pin"]:
        raise ValueError("eval worker release differs from the launcher pin")
    for release, pin in ((QUEUE, manifest), (EVAL_RELEASE, read_json(EVAL_RELEASE/"pin.json"))):
        for name, digest in pin["files"].items():
            if sha(release/name) != digest:
                raise ValueError(f"pin mismatch: {release/name}")
    for path in (m.ARCHIVE/"_extract_params.json", m.SOURCE, m.TRAIN_BOOK.parent,
                 Path("H:/chess/evals/cloud_2026-10-07"), Path("H:/chess/evals/fishnet"), RECIPE):
        if not path.exists():
            raise ValueError(f"unavailable input or destination: {path}")
    cert = m.OPS/"compact-verified.json"
    if cert.is_file():
        m.certified_compact_forecast()
    if check_only:
        print(f"Release pins and drive paths verified. Compact certificate present: {cert.is_file()}.")
        return
    if not m.START <= datetime.now() < m.END:
        raise SystemExit("This launcher is for the November 16–19 training window, Pacific time.")
    with root_lock(m.OPS/"trip-resume"):
        if not all((HOLDOUT/f"month=2026_{i}.json").is_file() for i in (8,9)):
            launch(RELEASE/"verify_holdout_headers.py",
                   ["--source",m.SOURCE,"--recipe",RECIPE,"--work",CACHE,"--out",HOLDOUT],
                   "holdout-resume", marker=HOLDOUT)
        if not cert.is_file():
            pilot = COMPACT/"run_ply51_migration.py"
            launch(pilot, ["--compact-pilot-only"], "compact-pilot-resume")
            while not cert.is_file():
                if not active(pilot):
                    raise RuntimeError("compact pilot exited before certification; inspect compact-pilot-status.json")
                time.sleep(15)
        m.certified_compact_forecast()
        status = read_json(m.OPS/"status.json")
        published = status.get("stage") == "training explorer verified"
        if published and (Path(status["book"]).resolve() != m.TRAIN_BOOK.resolve()
                          or not (m.TRAIN_BOOK/"_BOOK.DONE").is_file()
                          or status["meta_sha256"] != sha(m.TRAIN_BOOK/"_book.meta.json")):
            raise ValueError("training publication differs from status")
        if not published:
            queue_script = QUEUE/"run_ply51_migration.py"
            launch(queue_script, ["--compact-release",COMPACT], "compact-training")
            # On recovery the eval waiter would treat yesterday's stop as current.
            while read_json(m.OPS/"status.json").get("stage") in {
                "stopped", "training archive complete; merge outside reserved window",
                "training extraction checkpointed at end of reserved window"
            }:
                if not active(queue_script):
                    raise RuntimeError("training coordinator stopped; inspect compact-training.err")
                time.sleep(3)
        if read_json(m.OPS/"evals/status.json").get("stage") != "eval DB and lookup verified":
            launch(EVAL_RELEASE/"run_ply51_evals.py", [], "compact-eval-worker")
        print("The workers run in the background. The training queue revalidates published Sunday months.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="validate paths and releases without starting work")
    main(ap.parse_args().check)
