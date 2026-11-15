"""Compare the read-only July 2026 ChessDB snapshot with the fixed 1,000-parent live/Stockfish panel."""
from __future__ import annotations

import argparse
import csv
import re
import subprocess
import time
from collections import Counter
from pathlib import Path

import chess
import psutil
import numpy as np

from month_archive import sha, write_json

REVISION = "9de4573da712dbf0aaecadf62d164f7ba07e8145"
READER = "/opt/chessdb-pilot/cdbdirect"
TERARK = "/opt/chessdb-pilot/terarkdb"
MOVE = re.compile(r"^\s*([a-h][1-8][a-h][1-8][qrbn]?)\s*:\s*(-?\d+)\s*$")


def classify(raw):
    if raw is None:
        return "unknown",None
    magnitude = abs(raw)
    if magnitude < 15000:
        return "cp",None
    if magnitude > 30000:
        raise ValueError(f"unrecognized move score {raw}")
    if magnitude > 25000:
        return "mate",30000-magnitude
    if magnitude > 20000:
        return "tablebase_win_loss",25000-magnitude
    return "tablebase_cursed_blessed",20000-magnitude


def parse(log):
    found = {}
    item = None
    for line in log.splitlines():
        if line.startswith("Probing: "):
            epd = line.removeprefix("Probing: ").strip()
            if epd in found:
                raise ValueError(f"duplicate query: {epd}")
            item = found[epd] = {"moves":{}, "min_ply":None, "miss":False}
        elif item is not None:
            match = MOVE.fullmatch(line)
            if match:
                move,score=match.groups()
                if move in item["moves"]:
                    raise ValueError(f"duplicate scored move {move}")
                item["moves"][move]=int(score)
            elif line.startswith("Fen not found"):
                item["miss"]=True
            elif "Distance to startpos equal or less than" in line:
                item["min_ply"]=int(line.split()[-1])
            elif line.startswith("Required time:"):
                item["micros"]=float(line.split()[2])
    return found


def run_wsl(*args):
    return subprocess.check_output(["wsl.exe","-d","Ubuntu","-u","root","--exec",*args],
                                   text=True,encoding="utf-8").strip()


def analyze(panel, found):
    parsed=[]
    for r in panel:
        board=chess.Board(r["fen"])
        item=found[board.epd()]
        moves=item["moves"]
        if "micros" not in item or (item["miss"] and moves):
            raise ValueError(f"incomplete or inconsistent probe for {board.epd()}")
        for move in moves:
            board.parse_uci(move)
        best=max(moves.values()) if moves else None
        kind,distance=classify(best)
        sign=1 if board.turn else -1
        score=sign*best if kind=="cp" else None
        parsed.append({"sample_id":int(r["sample_id"]),"epd":board.epd(),"ply":int(r["earliest_parent_ply"]),
            "moves":moves,"raw_best_stm":best,"status":"absent" if item["miss"] else ("scored" if moves else "no_scored_moves"),
            "kind":kind,"white_cp":score,"white_favors":None if best is None or best==0 else (best*sign>0),
            "special_distance_plies":distance,"min_book_ply":item["min_ply"],"lookup_us":item["micros"],
            "live_queryscore":int(r["cdb_raw_side_to_move"]) if r["cdb_raw_side_to_move"] else None,
            "sf20_white_cp":int(r["sf20_white_cp"]) if r["sf20_white_cp"] else None,
            "sf20_white_mate_moves":int(r["sf20_white_mate"]) if r["sf20_white_mate"] else None})
    return parsed


def wait_ready(snapshot):
    marker = snapshot / "_DOWNLOAD.DONE"
    while not marker.is_file():
        active = any("chessdb/snapshots" in (p.info["cmdline"] or [])
                     and "--verify-local" in p.info["cmdline"]
                     for p in psutil.process_iter(["cmdline"], ad_value=None))
        if not active:
            raise RuntimeError("ChessDB checksum worker stopped before publishing its receipt")
        time.sleep(45)
    while True:
        state = run_wsl("bash", "-c",
            'if test -x /opt/chessdb-pilot/cdbdirect/cdbdirect; then echo ready; '
            'elif pgrep -f "^bash /opt/chessdb-pilot/build-reader.sh$" >/dev/null; then echo building; '
            'else echo failed; fi')
        if state == "ready":
            return
        if state != "building":
            raise RuntimeError("offline reader build stopped before producing an executable")
        time.sleep(45)


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample",required=True,type=Path)
    ap.add_argument("--snapshot",required=True,type=Path)
    ap.add_argument("--out",required=True,type=Path)
    ap.add_argument("--wait-ready",action="store_true")
    a=ap.parse_args()
    if a.wait_ready:
        wait_ready(a.snapshot)
    marker=(a.snapshot/"_DOWNLOAD.DONE").read_text(encoding="utf-8")
    if f"(chessdb/snapshots@{REVISION})" not in marker:
        raise ValueError("snapshot revision mismatch")
    hashes=run_wsl("sha256sum",READER+"/cdbdirect.cpp",READER+"/cdbdirect").splitlines()
    if hashes[0].split()[0]!="e09236cfbb16b3ce688b19076731c8d2cb954df2db2ab44134669a83356cd2ab":
        raise ValueError("unexpected reader source, including snapshot I/O error handling")
    panel=list(csv.DictReader(a.sample.open(encoding="utf-8",newline="")))
    epds=[chess.Board(r["fen"]).epd() for r in panel]
    if len(panel)!=1000 or len(set(epds))!=1000 or sorted(int(r["sample_id"]) for r in panel)!=list(range(1,1001)):
        raise ValueError("expected the original 1,000 distinct parents")
    a.out.mkdir(parents=True,exist_ok=True)
    fens=a.out/"panel.epd"
    fens.write_text("\n".join(epds)+"\n",encoding="ascii")
    source=run_wsl("wslpath","-a",str((a.snapshot/"chess-20260702/data").resolve()))
    input_file=run_wsl("wslpath","-a",str(fens.resolve()))
    # The private bind mount also protects the archived metadata files if the library creates a log.
    run_probe='''set -eu
mkdir -p /mnt/chessdb-ro
mount --bind "$1" /mnt/chessdb-ro
mount -o remount,bind,ro /mnt/chessdb-ro
findmnt -no OPTIONS --mountpoint /mnt/chessdb-ro | tr ',' '\\n' | grep -qx ro
export LD_LIBRARY_PATH=/opt/chessdb-pilot/terarkdb/output/lib
exec /opt/chessdb-pilot/cdbdirect/cdbdirect "$2"'''
    started=time.monotonic()
    raw=run_wsl("unshare","-m","--propagation","private","bash","-c",run_probe,"probe",source,input_file)
    wall=time.monotonic()-started
    (a.out/"reader.log").write_text(raw,encoding="utf-8")
    found=parse(raw)
    if set(found)!=set(epds):
        raise ValueError(f"reader returned {len(found)} queries for the 1,000-parent panel")
    rows=analyze(panel,found)
    write_json(a.out/"positions.json",rows)
    hits=[r for r in rows if r["status"]=="scored"]
    errors=[abs(r["white_cp"]-r["sf20_white_cp"]) for r in hits if r["white_cp"] is not None and r["sf20_white_cp"] is not None]
    common=[r for r in hits if r["live_queryscore"] is not None]
    summary={"snapshot_revision":REVISION,"snapshot_manifest_sha256":sha(a.snapshot/"manifest.tsv"),
        "reader_sha256":hashes[1].split()[0],"reader_revision":run_wsl("git","-C",READER,"rev-parse","HEAD"),
        "storage_revision":run_wsl("git","-C",TERARK,"rev-parse","HEAD"),
        "storage_local_diff":run_wsl("git","-C",TERARK,"diff","--","include/rocksdb/filter_policy.h","table/terark_zip_table.h"),"sample_sha256":sha(a.sample),
        "positions":len(rows),"scored":len(hits),"zero":sum(r["raw_best_stm"]==0 for r in rows),
        "status":Counter(r["status"] for r in rows),"kinds":Counter(r["kind"] for r in rows),
        "depth":[{"range":[lo,hi],"n":sum(lo<=r["ply"]<=hi for r in rows),
                  "hits":sum(lo<=r["ply"]<=hi for r in hits)} for lo,hi in [(0,15),(16,19),(20,24),(25,29)]],
        "matched_cp_sf20":len(errors),"median_absolute_cp":float(np.median(errors)) if errors else None,
        "p95_absolute_cp":float(np.quantile(errors,.95)) if errors else None,
        "live_and_offline_scored":len(common),"live_vs_offline_exact":sum(r["raw_best_stm"]==r["live_queryscore"] for r in common),
        "seconds_including_open":wall,"summed_probe_seconds":sum(r["lookup_us"] for r in rows)/1e6,
        "scope":"Best stored move vs live queryscore at a later date. Ordinary zero can include a normalized draw. Tablebase bands are stored source claims; the position keys omit the halfmove clock."}
    write_json(a.out/"summary.json",summary)
    print(summary)


if __name__=="__main__":
    main()
