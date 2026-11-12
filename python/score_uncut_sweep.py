"""Score the corrected books and frozen controls under the existing ply-30 referees."""
import argparse
import msvcrt
import shutil
import subprocess
import sys
import time
from pathlib import Path

import polars as pl

from run_uncut_sweep import BOOK, atomic_json, digest, native_args, now, prescriptions, read_json


def wait_for_book(root, side, cap):
    folder = root / side / f"cp{cap}"
    while not (folder / "receipt.json").exists():
        status = read_json(root / "status.json")
        if status["stage"] == "failed":
            raise RuntimeError(f"Upstream build failed: {status}")
        time.sleep(30)
    return folder / BOOK, read_json(folder / "receipt.json")["files"][BOOK]


def freeze(folder, label, src, expected, side):
    dest = folder / (label + ".parquet")
    if not dest.exists():
        tmp = dest.with_suffix(".copying")
        shutil.copy2(src, tmp)
        if digest(tmp) != expected:
            raise ValueError(f"Book changed before freezing: {src}")
        tmp.replace(dest)
    if digest(dest) != expected:
        raise ValueError(f"Frozen book changed: {dest}")
    prescriptions(dest, side)
    return str(dest)


def panel(root, baseline, side, caps):
    frozen = root / "referee" / "frozen" / side
    frozen.mkdir(parents=True, exist_ok=True)
    books = {}
    for cap in caps:
        corrected, expected = wait_for_book(root, side, cap)
        books[f"uncut{cap}"] = freeze(frozen, f"uncut{cap}", corrected, expected, side)
        rec, args = native_args(baseline, side, cap)
        if "CANON" not in books:
            canon = Path(args[args.index("--incumbent") + 1])
            books["CANON"] = freeze(frozen, "CANON", canon, digest(canon), side)
        if (side, cap) != ("white", 100):
            old = baseline / side / f"cp{cap}" / BOOK
            books[f"cutoff{cap}"] = freeze(frozen, f"cutoff{cap}", old,
                                           read_json(rec)["outputs"][str(old)], side)
    return books


def score(work, dest, side, books, evaluator, options):
    dest.mkdir(exist_ok=True, parents=True)
    result = dest / "scores.parquet"
    cmd = [sys.executable, "-X", "utf8", "-u", str(evaluator), *options,
           "--max-ply", "30", "--seed", "1,2", "--compare-to", "CANON",
           "--sf-cache", str(work / "sf.parquet"), "--out", str(result),
           "--exits-out", str(dest / "exits")]
    for label, path in books.items():
        cmd.extend(["--book", f"{label}={side}={path}"])
    recipe = dict(command=cmd, books={k: digest(v) for k, v in books.items()},
                  evaluator=digest(evaluator))
    config = dest / "recipe.json"
    if config.exists() and read_json(config) != recipe:
        raise ValueError(f"Scoring inputs changed for {dest}")
    atomic_json(config, recipe)
    atomic_json(work / "status.json", dict(stage="score", scenario=str(dest), utc=now()))
    if not result.exists():
        with (dest / "score.log").open("a", encoding="utf8") as log:
            subprocess.run(cmd, cwd=evaluator.parent.parent, stdout=log,
                           stderr=subprocess.STDOUT, check=True)
    f = pl.read_parquet(result)
    if (f.height != 2 * len(books) or set(f.select("label", "seed").iter_rows())
            != {(label, seed) for label in books for seed in (1, 2)}
            or not (f["book_moves"] == 2000).all() or not f["uniform"].is_finite().all()):
        raise ValueError(f"Incomplete paired results: {result}")
    means = dict(f.group_by("label").agg(pl.col("uniform").mean()).iter_rows())
    atomic_json(dest / "summary.json", dict(mean_score=means, vs_canonical={
        label: value - means["CANON"] for label, value in means.items()}))


def run(root, baseline):
    work = root / "referee"
    data = baseline.parent / "population-transfer-v1"
    design = read_json(data / "design.json")
    evaluator = baseline.parents[2] / "python" / "convertibility_eval.py"
    if digest(evaluator) != design["code"]["convertibility_eval.py"]:
        raise ValueError(f"Population evaluator differs from baseline design: {evaluator}")
    for filename in ("sf.parquet", "sf.parquet.meta.json"):
        dest = work / filename
        if not dest.exists():
            src = Path("E:/chess/calibration") / filename.replace("sf.parquet", "convertibility_sf_cache_sampled.parquet")
            tmp = dest.with_suffix(".copying")
            shutil.copy2(src, tmp)
            tmp.replace(dest)

    def population(dest, side, books, target, model, own):
        models = data / "models" / target
        opts = ["--our-moves", str(models / own), "--opp-moves", str(models / model),
                "--our-elo", "1450" if own == "current" else "1900",
                "--opp-elo", str(design["targets"][target][2]), "--k", "30",
                "--tau", ".001", "--rollouts", "2", "--sf-procs", "1"]
        score(work, dest, side, books, evaluator, opts)

    atomic_json(work / "status.json", dict(stage="waiting for Black 30 cp", utc=now()))
    first = panel(root, baseline, "black", [30])
    population(work / "first-black30-rapid", "black", first, "rapid-now", "borrowed", "current")
    # Run both gate screens before the much larger sensitivity matrix.
    panels = {}
    for side in ("black", "white"):
        atomic_json(work / "status.json", dict(stage=f"waiting for {side} gate books", utc=now()))
        books = panels[side] = panel(root, baseline, side, (30, 60, 100))
        other = "white" if side == "black" else "black"
        hx = Path("E:/chess/holdout_explorer")
        opts = ["--our-moves", str(hx / f"movers1000_{side}_f64_2026_08_09"),
                "--opp-moves", str(hx / f"movers1800_{other}_f64_2026_08_09"),
                "--rollouts", "1", "--tau", ".0001", "--k", "100", "--sf-procs", "2"]
        score(work, work / side / "gate-screen", side, books,
              baseline.parent / "depth-code" / "convertibility_eval.py", opts)
    for model in ("borrowed", "pooled", "strong_rapid"):
        for own in ("current", "future"):
            for target in design["targets"]:
                for side, books in panels.items():
                    population(work / side / target / model / own, side, books, target, model, own)
    atomic_json(work / "status.json", dict(stage="complete", utc=now()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    folder = args.out.resolve() / "referee"
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / "score.lock").open("a+b") as lock:
        lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        try:
            run(args.out.resolve(), args.baseline.resolve())
        except Exception as error:
            atomic_json(folder / "status.json", dict(stage="failed", error=str(error), utc=now()))
            raise
