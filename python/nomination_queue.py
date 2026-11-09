"""User controls around the immutable nomination-method snapshot."""
import argparse
import json
from pathlib import Path
import sys
import time

import psutil


class WhiteHeld(Exception):
    pass


def run_controlled(driver):
    trial = driver.nt.TRIAL
    hold = trial/"hold-before-white.json"
    queue = trial/"queue.json"
    publish = driver.r.publish
    def check(path,value):
        if Path(path)==queue and value.get("state")=="building-white" and hold.is_file():
            publish(queue,dict(state="held-before-white",time=driver.r.now(),
                               reason=json.loads(hold.read_text())["reason"]))
            raise WhiteHeld
        publish(path,value)
    driver.r.publish = check
    try:
        driver.workflow()
    except WhiteHeld:
        print("Black comparison complete. Holding before White.",flush=True)
    finally:
        driver.r.publish = publish


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("pin",type=Path)
    a = ap.parse_args()
    sys.path.insert(0,str(a.pin.resolve()))
    import run_nomination_comparison as driver
    if driver.nt.HERE.resolve()!=a.pin.resolve():
        raise RuntimeError("Expected the pinned Black method")
    trial = driver.nt.TRIAL
    # A coordinator handover may leave its Black subprocess finishing a stage.
    # The identical pinned command then verifies or reuses its completed chunks.
    record = trial/"adopted-black-stage.json"
    if record.exists():
        worker = json.loads(record.read_text())
        driver.r.publish(trial/"queue-control.json",dict(state="waiting-for-black-worker",pid=worker["pid"],
                                                          controller=psutil.Process().pid,time=driver.r.now()))
        while True:
            try:
                p = psutil.Process(worker["pid"])
                if p.create_time()!=worker["created"] or p.cmdline()!=worker["command"]:
                    break
            except psutil.NoSuchProcess:
                break
            time.sleep(15)
    driver.r.publish(trial/"queue-control.json",dict(state="running-controlled-workflow",
                                                     controller=psutil.Process().pid,time=driver.r.now()))
    run_controlled(driver)


if __name__=="__main__":
    main()
