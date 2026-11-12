"""Replay the native 2,000-decision gate recipes with complete own-move counts."""
import argparse
import json
import msvcrt
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import chess
import polars as pl
import psutil

from uncut_own_moves import atomic_json, digest, load, read_json
from zobrist import zobrist_int64

HERE = Path(__file__).resolve().parent
BOOK = "book_b2000.parquet"
PETROFF = -474690907812126126


def now():
    return datetime.now(timezone.utc).isoformat()


def native_args(baseline, colour, cap):
    template = 60 if (colour, cap) == ("white", 100) else cap
    receipt = baseline / "stages" / f"{colour}-cp{template}.json"
    cmd = read_json(receipt)["command"]
    args = cmd[cmd.index("--rep"):]
    args[args.index("--max-eval-loss") + 1] = str(cap)
    return receipt, args


def active_preparation(folder):
    for proc in psutil.process_iter(["cmdline", "name"]):
        args = proc.info["cmdline"] or []
        if not (proc.info["name"] or "").lower().startswith("python"):
            continue
        if "--out" in args and any(Path(v).name == "uncut_own_moves.py" for v in args):
            if Path(args[args.index("--out") + 1]).resolve() == folder:
                return True
    return False


def ensure_cache(folder, colour, args):
    stats = args[args.index("--stats") + 1]
    aux = args[args.index("--aux-stats") + 1]
    while not (folder / "ready.json").exists() and active_preparation(folder):
        time.sleep(20)
    if not (folder / "ready.json").exists():
        with (folder.parent / "prepare-resume.log").open("a", encoding="utf8") as log:
            subprocess.run([sys.executable, "-u", str(HERE / "uncut_own_moves.py"),
                            "--stats", stats, "--aux-stats", aux, "--perspective", colour,
                            "--out", str(folder)], stdout=log, stderr=subprocess.STDOUT, check=True)
    return load(folder, stats, aux, colour, None)


def prescriptions(path, colour, verify=False):
    data = (pl.scan_parquet(path)
            .filter((pl.col("side_to_move") == colour) & pl.col("best_move").is_not_null())
            .select("position_hash", "position_epd", "best_move").collect())
    if data.height != 2000 or data["position_hash"].n_unique() != 2000:
        raise ValueError(f"Expected 2,000 distinct decisions in {path}")
    moves = {}
    for h, epd, move in data.iter_rows():
        if verify:
            board = chess.Board(epd + " 0 1")
            if board.turn != (colour == "white") or zobrist_int64(board) != h:
                raise ValueError(f"Side or position hash mismatch in {path}: {epd}")
            board.parse_san(move)
        moves[h] = move.rstrip("+#")
    return moves


def petroff(cache, meta):
    from build_budget_books import GateAdmit
    rows = (pl.scan_parquet(cache / "graph.parquet").filter(pl.col("parent_hash") == PETROFF)
            .select("move_san", "child_hash", "total").collect().to_dicts())
    counts = {r["move_san"]: r["total"] for r in rows}
    src = (pl.scan_parquet(meta["source_rep"]).filter(pl.col("position_hash") == PETROFF)
           .select("best_move").collect()["best_move"].drop_nulls().unique().to_list())
    if len(src) > 1 or counts != {"f5": 28, "O-O": 12, "Bf5": 1}:
        raise ValueError(f"Petroff sentinel counts or source mismatch: {counts}, {src}")
    gate = GateAdmit({PETROFF: src[0]} if src else {}, False, meta["slice_prior"],
                     max_eval_loss=meta["max_eval_loss"], robustness_floor=meta["robustness_floor"],
                     gate_rel_floor=meta["gate_rel_floor"], gate_rel_own_margin=meta["gate_rel_own_margin"])
    gate.prefetch([PETROFF, *(r["child_hash"] for r in rows)])
    allowed = gate(PETROFF, rows)
    menu = sorted(r["move_san"] for r in rows if r["child_hash"] in allowed)
    if meta["max_eval_loss"] == 30 and menu != ["O-O"]:
        raise ValueError(f"Unexpected Petroff 30 cp menu: {menu}")
    return {"observations": counts, "eligible": menu}


def build(root, baseline, colour, cap, code):
    parent = root / colour
    parent.mkdir(exist_ok=True)
    cache = parent / "cache"
    receipt, args = native_args(baseline, colour, cap)
    atomic_json(root / "status.json", dict(stage="prepare", colour=colour, cap=cap, utc=now()))
    ready = ensure_cache(cache, colour, args)
    dest = parent / f"cp{cap}"
    stage = parent / f"cp{cap}.building"
    args[args.index("--out-prefix") + 1] = str(stage / "book")
    args[args.index("--tmp-dir") + 1] = str(parent / f"cp{cap}.tmp")
    args += ["--uncut-own-moves", str(cache)]
    cmd = [sys.executable, "-X", "utf8", "-u", str(HERE / "build_budget_books.py"), *args]
    recipe = dict(command=cmd, code=code, native_receipt=digest(receipt),
                  cache_recipe=ready["recipe_sha256"])
    if dest.exists():
        saved = read_json(dest / "receipt.json")
        if saved["recipe"] != recipe or any(digest(dest / k) != v for k, v in saved["files"].items()):
            raise ValueError(f"Published book differs from recipe or receipt: {dest}")
        return
    stage.mkdir(exist_ok=True)
    started = now()
    atomic_json(root / "status.json", dict(stage="solve", colour=colour, cap=cap, utc=started))
    t = time.monotonic()
    with (stage / "build.log").open("a", encoding="utf8") as log:
        subprocess.run(cmd, cwd=HERE.parent, stdout=log, stderr=subprocess.STDOUT, check=True)
    duration = time.monotonic() - t
    meta = read_json(stage / (BOOK + ".meta.json"))
    expected = dict(spent_distinct=2000, eval_weight=.75, max_eval_loss=cap,
                    robustness_floor=.10, gate_rel_floor=.10, gate_rel_own_margin=.02,
                    gates=True, stop_model="weak", uncut_own_recipe=ready["recipe_sha256"])
    if any(meta[k] != v for k, v in expected.items()):
        raise ValueError(f"Unexpected construction settings in {stage}")
    moves = prescriptions(stage / BOOK, colour, verify=True)
    summary = {k: meta[k] for k in ("subgraph_nodes", "subgraph_our_nodes", "peak_commit_gb",
                                     "root_value_realized", "budget_charged", "spent_distinct")}
    if not (colour == "white" and cap == 100):
        old = baseline / colour / f"cp{cap}" / BOOK
        if digest(old) != read_json(receipt)["outputs"][str(old)]:
            raise ValueError(f"Cutoff baseline changed: {old}")
        prior = prescriptions(old, colour)
        common = moves.keys() & prior.keys()
        summary["cutoff_comparison"] = dict(common_positions=len(common),
                                             same_move=sum(moves[h] == prior[h] for h in common))
    if colour == "black":
        sentinel = petroff(cache, meta)
        prescribed = moves.get(PETROFF)
        if prescribed is not None and prescribed not in sentinel["eligible"]:
            raise ValueError(f"Book prescribes an ineligible Petroff move: {prescribed}")
        summary["petroff"] = {**sentinel, "prescribed": prescribed}
    files = {n: digest(stage / n) for n in (BOOK, BOOK + ".meta.json")}
    atomic_json(stage / "receipt.json", dict(recipe=recipe, files=files, summary=summary,
                                             started=started, finished=now(), solve_seconds=duration))
    stage.rename(dest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=True)
    with (root / "sweep.lock").open("a+b") as lock:
        lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        code = {n: digest(HERE / n) for n in ("build_budget_books.py", "budget_core.py",
                "uncut_own_moves.py", "budget_compiled.py", "stage3_backwards_induction.py")}
        try:
            for cap in (30, 60, 100):
                for colour in ("black", "white"):
                    build(root, args.baseline.resolve(), colour, cap, code)
            atomic_json(root / "status.json", dict(stage="complete", utc=now()))
        except Exception as error:
            prev = read_json(root / "status.json") if (root / "status.json").exists() else {}
            atomic_json(root / "status.json", {**prev, "stage": "failed", "error": str(error),
                                                "stopped": now()})
            raise


if __name__ == "__main__":
    main()
