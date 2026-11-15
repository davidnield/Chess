"""Bounded Sunday training pre-extraction using the published 51-ply monthly producer."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import subprocess
import time

import run_ply51_migration as m
from month_archive import read_json, root_lock, sha, write_json

RELEASE = Path(__file__).resolve().parent
ORIGINAL = Path("D:/chess/bin/ply51-pilot-20261115")
COMPACT = Path("D:/chess/bin/ply51-compact-20261115")
STATE = m.OPS / "sunday-head-start"
MONTHS = tuple(f"{year}_7" for year in range(2013, 2027))
LAST_START = datetime(2026, 11, 15, 20, 0)
HARD_STOP = datetime(2026, 11, 15, 21, 0)


def status(stage, done, **other):
    info = {"stage": stage, "time": datetime.now().isoformat(timespec="seconds"),
            "verified_months": list(done), "verified_source_games": sum(done.values()), **other}
    write_json(STATE/"status.json", info)
    print(info, flush=True)


def check_release():
    m.PIN, m.EXE, m.COMPACT_PIN = ORIGINAL, ORIGINAL/"explorer-extract.exe", COMPACT
    own = read_json(RELEASE/"pin.json")
    if sha(COMPACT/"pin.json") != own["compact_pin"] or sha(ORIGINAL/"migration-pin.json") != own["monthly_pin"]:
        raise ValueError("the extraction or compact pilot release changed")
    for release in (RELEASE, COMPACT):
        for name, digest in read_json(release/"pin.json")["files"].items():
            if sha(release/name) != digest:
                raise ValueError(f"pin differs: {release/name}")
    m.check_pins()


def run_month(month):
    # The deadline applies to this one launch and its descendants. Only a
    # complete F: marker and verified receipt count toward tomorrow's queue.
    command = list(map(str, m.extract_command([month])))
    with subprocess.Popen(command) as worker:
        try:
            returncode = worker.wait(timeout=max(0, (HARD_STOP-datetime.now()).total_seconds()))
        except subprocess.TimeoutExpired:
            subprocess.run(["taskkill.exe", "/PID", str(worker.pid), "/T", "/F"], check=worker.poll() is None)
            worker.wait(timeout=30)
            return False
        if returncode:
            raise subprocess.CalledProcessError(returncode, command)
    return True


def admit(now, estimate):
    return now < LAST_START and (HARD_STOP-now).total_seconds() >= estimate + 600


def run():
    done = {}
    with root_lock(STATE):
        try:
            check_release()
            early_forecast = read_json(m.OPS/"forecast.json")
            inventory = read_json(ORIGINAL/"training-inputs.json")["months"]
            games = {t: sum(inventory[t]["games"].values()) for t in MONTHS}
            if not set(MONTHS).issubset(m.TRAIN):
                raise ValueError("requested months fall outside training")
            for t in MONTHS:
                if (m.ARCHIVE/f"_month={t}.DONE").is_file():
                    m.receipt(t, inventory)
                    done[t] = games[t]
            capacity = sum(games[t] for t in MONTHS if t not in done)*early_forecast["archive_planning_bytes_per_game"]
            if m.free(m.ARCHIVE) < capacity + 100*m.GB:
                raise ValueError("insufficient archive space for the Sunday batch and reserve")
            status("waiting for verified compact pilot", done, months=MONTHS, source_games=sum(games.values()),
                   last_start=LAST_START.isoformat(), hard_stop=HARD_STOP.isoformat())
            while not (m.OPS/"compact-verified.json").is_file():
                if datetime.now() >= LAST_START or (STATE/"stop-after-month").exists():
                    status("checkpointed before starting extraction", done)
                    return
                ps = read_json(m.OPS/"compact-pilot-status.json")
                if ps.get("stage") == "stopped":
                    raise RuntimeError(f"compact pilot stopped: {ps}")
                time.sleep(20)
            forecast = m.certified_compact_forecast()
            rate = forecast["planning_serial_secs_per_game"]
            for t in MONTHS:
                if t in done:
                    continue
                estimate = 60 + games[t]*rate
                if (STATE/"stop-after-month").exists() or not admit(datetime.now(), estimate):
                    status("checkpointed before evening cutoff", done, next_month=t)
                    return
                status("extracting and publishing month", done, month=t, estimated_minutes=round(estimate/60,1))
                started = time.monotonic()
                finished = run_month(t)
                if finished or (m.ARCHIVE/f"_month={t}.DONE").is_file():
                    m.receipt(t, inventory)
                    done[t] = games[t]
                if not finished:
                    status("checkpointed at hard cutoff", done, last_attempt=t)
                    return
                # Ignore small early months when calibrating the per-game rate.
                if games[t] >= 10_000_000:
                    rate = max(rate, (time.monotonic()-started)/games[t]*1.15)
                status("month verified", done, month=t)
            status("Sunday batch complete", done)
        except Exception as exc:
            status("stopped", done, error=str(exc))
            raise


if __name__ == "__main__":
    run()
