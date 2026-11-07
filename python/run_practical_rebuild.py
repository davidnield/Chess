"""Resumable local experiment. Never writes the frozen canonical books.

Run with the project's venv Python. The first run snapshots python/ after the full
suite passes. All later commands use that immutable source snapshot. Each stage
has a command fingerprint, output checksums, a log and elapsed wall time.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import msvcrt
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs/rebuild-2026-11-06"
CODE = OUT / "code"
DEPTH = OUT / "depth-code"
STATUS_FILE = OUT / "status.json"
HX = Path("E:/chess/holdout_explorer")
FROZEN = Path("E:/chess/repertoire/final")
PY = sys.executable
STAMP = {"white": "2026-10-28", "black": "2026-10-24"}
POOL = {
    "white": "E:/chess/position-stats/position_stats_pooled_ge1800_2013_2026_brc.parquet",
    "black": "E:/chess/position-stats/position_stats_pooled_ge2000_2013_2026_brc_p30_mg20.parquet",
}
SOURCE = {
    "white": "E:/chess/repertoire/repertoire_pooled_white_sharp.parquet",
    "black": "E:/chess/repertoire/_pool2000_p30_mg20/repertoire_pooled_black_sharp.parquet",
}
SEEDS = [1, 2, 3, 4]


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def publish(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def status(**kwargs):
    publish(STATUS_FILE, {"updated_utc": now(), "pid": os.getpid(), **kwargs})
    print(json.dumps(kwargs, default=str), flush=True)


def cmd(script, *args):
    return [PY, "-X", "utf8", str(DEPTH / script), *map(str, args)]


def stage(name, command, outputs):
    """Skip only a successful identical command with unchanged complete outputs."""
    command = list(map(str, command))
    outputs = list(map(Path, outputs))
    done = OUT / "stages" / (name + ".json")
    if done.exists():
        previous = json.loads(done.read_text(encoding="utf-8"))
        if previous["command"] != command:
            raise RuntimeError(f"Changed command for completed stage {name}")
        if all(p.is_file() and previous["outputs"].get(str(p)) == digest(p) for p in outputs):
            return
        raise RuntimeError(f"Completed stage outputs changed: {name}")
    for p in outputs:
        p.parent.mkdir(parents=True, exist_ok=True)
    log = OUT / "logs" / (name + ".log")
    started = now()
    status(state="running", stage=name, started_utc=started, log=log, command=command)
    start = time.monotonic()
    env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8", PYTHONUTF8="1",
               PYTHONPATH=str(DEPTH))
    with log.open("a", encoding="utf-8") as stream:
        stream.write(f"\nSTART {started}\n{subprocess.list2cmdline(command)}\n")
        stream.flush()
        proc = subprocess.run(command, cwd=ROOT, env=env, stdout=stream,
                              stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
    if proc.returncode:
        raise RuntimeError(f"{name} exited {proc.returncode}; see {log}")
    if any(not p.is_file() for p in outputs):
        raise RuntimeError(f"{name} omitted a required output")
    publish(done, {"command": command, "started_utc": started, "finished_utc": now(),
                   "seconds": time.monotonic() - start,
                   "outputs": {str(p): digest(p) for p in outputs}})


def incumbent(colour, extended=False):
    suffix = "_ext_c10" if extended else ""
    return FROZEN / f"lifetime_{colour}_{STAMP[colour]}{suffix}.parquet"


def recipe(colour):
    args = ["--rep", SOURCE[colour], "--stats", POOL[colour],
            "--aux-stats", POOL[colour].replace("position_stats_pooled", "position_stats_aux_pooled"),
            "--perspective", colour, "--budget", "2000", "--epsilon", ".0005",
            "--max-ply", "29", "--max-cands", "0", "--mem", "16GB", "--threads", "4",
            "--tmp-dir", str(OUT / "duckdb-temp"), "--gated-reach", "--stop-model", "weak",
            "--weak-pool", "E:/chess/position-stats/weak_edges_b1000_1800_on_ge1800.parquet",
            "--incumbent", str(incumbent(colour)), "--stick-margin", "0"]
    if colour == "white":
        args += ["--method", "price", "--price-seed", "source", "--price-reprice", "max",
                 "--price-rounds", "1", "--price-select", "best", "--price-dag-mode", "polish",
                 "--max-eval-loss", "60"]
    else:
        args += ["--method", "dp", "--match-distinct", "--match-inflate", "1.5", "--max-eval-loss", "30"]
    return args


def setup():
    test_log = OUT / "logs/tests.log"
    tests = test_log.read_text(encoding="utf-8", errors="replace")
    match = re.search(r"(\d+)/(\d+) test files passed", tests)
    if not match or match[1] != match[2]:
        raise RuntimeError("A complete passing full-suite log is required before this run")
    expected = {p.name: digest(p) for p in (ROOT / "python").glob("*.py")}
    manifest = OUT / "source.json"
    if manifest.exists():
        pinned = json.loads(manifest.read_text())
        if {p.name: digest(p) for p in CODE.glob("*.py")} != pinned["files"]:
            raise RuntimeError("Pinned source changed")
    else:
        CODE.mkdir(parents=True, exist_ok=True)
        for p in (ROOT / "python").glob("*.py"):
            shutil.copy2(p, CODE / p.name)
        rev = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        publish(manifest, {"commit": rev, "files": expected, "tests_sha256": digest(test_log)})
    sys.path.insert(0, str(CODE))
    hybrid_pin = OUT / "hybrid-code"
    hybrid_manifest = hybrid_pin / "source.json"
    files = ("hybrid_candidates.py", "validate_book.py")
    if not hybrid_manifest.exists():
        hybrid_pin.mkdir(exist_ok=True)
        for filename in files:
            shutil.copy2(ROOT / "python" / filename, hybrid_pin / filename)
        publish(hybrid_manifest, {f: digest(hybrid_pin / f) for f in files})
    if json.loads(hybrid_manifest.read_text()) != {f: digest(hybrid_pin / f) for f in files}:
        raise RuntimeError("Hybrid source snapshot changed")
    changed = ("hybrid_candidates.py", "validate_book.py", "budget_core.py", "build_budget_books.py",
               "weak_stop.py", "cloud_feedback.py", "cloud_cutoff_check.py")
    manifest = DEPTH / "source.json"
    if not manifest.exists():
        DEPTH.mkdir(exist_ok=True)
        for p in CODE.glob("*.py"):
            shutil.copy2(p, DEPTH / p.name)
        for name in changed:
            shutil.copy2(ROOT / "python" / name, DEPTH / name)
        publish(manifest, {p.name:digest(p) for p in DEPTH.glob("*.py")})
    if json.loads(manifest.read_text()) != {p.name:digest(p) for p in DEPTH.glob("*.py")}:
        raise RuntimeError("Depth-policy source snapshot changed")
    sys.path.insert(0, str(DEPTH))
    cache = OUT / "training-sf.parquet"
    if not cache.exists():
        old = Path("E:/chess/calibration/convertibility_sf_cache_sampled.parquet")
        shutil.copy2(old, cache)
        shutil.copy2(Path(str(old) + ".meta.json"), Path(str(cache) + ".meta.json"))
    frozen = {str(incumbent(c, ext)): digest(incumbent(c, ext))
              for c in STAMP for ext in (False, True)}
    frozen_record = OUT / "canonical-sha256.json"
    if frozen_record.exists():
        if json.loads(frozen_record.read_text()) != frozen:
            raise RuntimeError("A canonical book changed during this experiment")
    else:
        publish(frozen_record, frozen)


def nominate(colour, work, label, base, proposals, searched, epoch):
    from cloud_feedback import normalize
    round_id = 0
    while True:
        round_id += 1
        first_black = colour == "black" and epoch == 0 and round_id == 1
        graph_work = OUT / "black" if first_black else work
        targets = graph_work / f"targets-{round_id:02d}.parquet"
        overlay = ["--engine-candidates", proposals] if proposals else []
        stage("black-targets-01" if first_black else f"{label}-targets-{round_id:02d}",
              cmd(CODE / "build_budget_books.py" if first_black else "build_budget_books.py",
                  *base, *overlay, "--out-prefix", graph_work / "graph", "--dump-engine-targets", targets), [targets])
        new = pl.read_parquet(targets).filter(~pl.col("position_hash").is_in(list(searched)))
        if not new.height:
            publish(work / "nomination-complete.json", {"rounds":round_id-1, "searched_positions":len(searched)})
            return proposals
        todo = graph_work / f"unsearched-{round_id:02d}.parquet"
        if todo.exists():
            if not pl.read_parquet(todo).equals(new):
                raise RuntimeError("Nomination round input changed")
        else:
            tmp = todo.with_suffix(".tmp")
            new.write_parquet(tmp)
            tmp.replace(todo)
        keep = ["--keep-local", OUT / "black/depth-preserved-local.parquet"] if first_black else []
        study = ["--cutoff-study", OUT / "white/cutoff-study"] if colour == "white" and epoch == 0 and round_id == 1 else []
        fresh = work / f"hybrid-proposals-{round_id:02d}.parquet"
        stage(f"{label}-hybrid-{round_id:02d}", cmd("hybrid_candidates.py", "--targets", todo,
              "--colour", colour, *keep, *study, "--min-knodes", 0, "--nodes", 10000000, "--threads", 6, "--out", fresh),
              [fresh, Path(str(fresh) + ".meta.json"), *([OUT / "white/cutoff-study/summary.json"] if study else [])])
        combined = work / f"all-proposals-{round_id:02d}.parquet"
        frames = ([normalize(pl.read_parquet(proposals))] if proposals else []) + [normalize(pl.read_parquet(fresh))]
        merged = pl.concat(frames)
        if merged.n_unique(subset=["position_hash","child_hash"]) != merged.height:
            raise RuntimeError("Duplicate nomination edge")
        if combined.exists():
            if not pl.read_parquet(combined).equals(merged):
                raise RuntimeError("Cumulative nominations changed")
        else:
            tmp = combined.with_suffix(".tmp")
            merged.write_parquet(tmp)
            tmp.replace(combined)
        proposals = combined
        searched.update(new["position_hash"].to_list())


def provisional(colour, work, label, base, proposals):
    nomination = ["--engine-candidates", proposals] if proposals else []
    dumps = {}
    for kind in ("other", "stop"):
        dump = work / f"{kind}-positions.parquet"
        stage(f"{label}-dump-{kind}",
              cmd("build_budget_books.py", *base, *nomination, "--out-prefix", work / "dump",
                  f"--dump-{kind}-positions", dump), [dump, Path(str(dump) + ".meta.json")])
        dumps[kind] = dump
    tables = []
    for kind in ("other", "stop"):
        table = work / f"{kind}-table.parquet"
        args = ["--nodes", "2000"] if kind == "other" else [
            "--pool", POOL[colour], "--top", "12000", "--chunk", "2000"]
        outputs = [table, Path(str(table) + ".meta.json")]
        if kind == "stop":
            outputs.append(Path(str(table) + ".boundary.parquet"))
        stage(f"{label}-sample-{kind}",
              cmd(f"sampled_{kind}.py", "--dump", dumps[kind], "--colour", colour,
                  *args, "--games", 32, "--max-ply", 30, "--sf-procs", 3,
                  "--our-moves", HX / "movers1000_{colour}_f64_2026_08_09",
                  "--opp-moves", HX / "movers1800_{colour}_f64_2026_08_09",
                  "--sf-cache", OUT / "training-sf.parquet", "--out", table), outputs)
        tables += [f"--sampled-{kind}", table]
    book = work / f"practical_{colour}_b2000.parquet"
    stage(f"{label}-build",
          cmd("build_budget_books.py", *base, *nomination, *tables, "--force",
              "--out-prefix", work / f"practical_{colour}"),
          [book, Path(str(book) + ".meta.json")])
    actual = pl.read_parquet(book).filter((pl.col("side_to_move") == colour) & pl.col("best_move").is_not_null())
    if actual.height != 2000 or actual["position_hash"].n_unique() != 2000:
        raise RuntimeError(f"Expected exactly 2000 legal decisions for {colour}")
    return book


def prepare_white():
    """Use the actual first-round inputs and checkpoints while Black is searching."""
    import errno
    work = OUT / "white/depth-policy/round-00"
    work.mkdir(parents=True, exist_ok=True)
    with (work / "prepare.lock").open("a+b") as lock:
        lock.seek(0)
        while True:
            try:
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                break
            except OSError as exc:
                if exc.errno != errno.EACCES:
                    raise
                time.sleep(10)
        targets = work / "targets-01.parquet"
        stage("white-depth-00-targets-01", cmd("build_budget_books.py", *recipe("white"), "--out-prefix",
              work / "graph", "--dump-engine-targets", targets), [targets])
        todo = work / "unsearched-01.parquet"
        if not todo.exists():
            tmp = todo.with_suffix(".tmp")
            shutil.copyfile(targets, tmp)
            tmp.replace(todo)
        if not pl.read_parquet(todo).equals(pl.read_parquet(targets)):
            raise RuntimeError("White spot-check and production targets differ")
        study = OUT / "white/cutoff-study"
        fresh = work / "hybrid-proposals-01.parquet"
        stage("white-depth-00-prepare", cmd("hybrid_candidates.py", "--targets", todo, "--colour", "white",
              "--cutoff-study", study, "--min-knodes", 0, "--nodes", 10000000, "--threads", 6,
              "--out", fresh, "--prepare-only"),
              [Path(str(fresh)+".parts") / "misses.parquet", study / "summary.json"])


def build(colour):
    if colour == "white":
        prepare_white()
    base = recipe(colour)
    root = OUT / colour / "depth-policy"
    proposals, searched, epoch = None, set(), 0
    while True:
        label = f"{colour}-depth-{epoch:02d}"
        work = root / f"round-{epoch:02d}"
        work.mkdir(parents=True, exist_ok=True)
        proposals = nominate(colour, work, label, base, proposals, searched, epoch)
        if proposals is None:
            from engine_candidates import SCHEMA
            proposals = work / "empty.parquet"
            if not proposals.exists():
                pl.DataFrame(schema=SCHEMA).write_parquet(proposals)
        book = provisional(colour, work, label, base, proposals)
        corrected = work / "verified-proposals.parquet"
        record = Path(str(corrected) + ".meta.json")
        stage(f"{label}-verify", cmd("cloud_feedback.py", "--book", book, "--colour", colour,
              "--proposals", proposals, "--cache", root / "verification-cache", "--out", corrected),
              [corrected, record])
        info = json.loads(record.read_text())
        if not info["searched_positions"]:
            if digest(proposals) != digest(corrected):
                raise RuntimeError("Stable feedback round changed proposals")
            publish(root / "selected.json", {"round":epoch, "book":book, "book_sha256":digest(book),
                    "verified_proposals":proposals, "verified_proposals_sha256":digest(proposals)})
            return {"CANON":incumbent(colour), "NEW":book, "LEARNED":incumbent(colour, True)}
        proposals = corrected
        searched.update(pl.read_parquet(corrected, columns=["position_hash"])["position_hash"].to_list())
        epoch += 1



def audit(colour, books):
    audits = {}
    for label, book in books.items():
        prefix = OUT / colour / f"audit-{label}"
        stage(f"{colour}-audit-{label}",
              cmd("validate_book.py", "--book", book, "--colour", colour,
                  "--nodes", 15000000, "--best-defence", "--defence-nodes", 1000000,
                  "--incumbent", incumbent(colour), "--out", prefix),
              [Path(str(prefix) + ".json"), Path(str(prefix) + ".rows.parquet")])
        audits[label] = json.loads(Path(str(prefix) + ".json").read_text())
        if label == "NEW":
            result = audits[label]["cloud_verification"]
            if result["selected"] != result["checked"] or result["over_limit"] or audits[label]["status"].get("outside_set"):
                raise RuntimeError(f"Final selection failed its safety audit: {colour} {result}")
        if audits[label]["status"].get("illegal", 0):
            raise RuntimeError(f"Illegal decision in {label} {colour}")
    return audits


def compare(colour, books, audits, name, scenario):
    import practical_scenarios as ps
    from exit_difficulty import read_exits
    paths = []
    for player, side in [(scenario.ours, colour), (scenario.theirs, "black" if colour == "white" else "white")]:
        path, command = ps.cohort(HX, player[0], scenario.events, side)
        if not ps.ready(path, player[0], side, scenario.events, 30):
            # One extractor at a time. Same partition contract as existing baseline tables.
            command += ["--workers", "4", "--mem", "16GB", "--tmp-dir", str(OUT / "duckdb-temp")]
            stage("table-" + path.name, command, [path / f"_month={m}.DONE" for m in ps.MONTHS.split(",")])
        if not ps.ready(path, player[0], side, scenario.events, 30):
            raise RuntimeError(f"Incomplete mover table {path}")
        paths.append(path)
    dest = OUT / colour / "comparison" / name
    dest.mkdir(parents=True, exist_ok=True)
    sf = dest / "sf.parquet"
    if not sf.exists():
        shutil.copy2(OUT / "training-sf.parquet", sf)
        shutil.copy2(Path(str(OUT / "training-sf.parquet") + ".meta.json"), Path(str(sf) + ".meta.json"))
    args = ["--compare-to", "CANON", "--our-moves", paths[0], "--opp-moves", paths[1],
            "--our-elo", scenario.ours[1], "--opp-elo", scenario.theirs[1],
            "--recall", scenario.recall, "--max-ply", 30, "--tau", .0001,
            "--rollouts", 2, "--seed", ",".join(map(str, SEEDS)), "--sf-procs", 2,
            "--sf-cache", sf, "--out", dest / "scores.parquet", "--exits-out", dest / "exits"]
    for label, book in books.items():
        args += ["--book", f"{label}={colour}={book}"]
    exits = [dest / "exits" / f"b{i:03d}_s{s}.parquet" for i in range(len(books)) for s in SEEDS]
    stage(f"{colour}-score-{name}", cmd("convertibility_eval.py", *args), [dest / "scores.parquet", *exits])
    frame = pl.read_parquet(dest / "scores.parquet")
    if frame.height != len(books) * len(SEEDS):
        raise RuntimeError("Incomplete comparison")
    summary = ps.paired_summary(frame, "CANON", {k: ps.decisions(p, colour) for k, p in books.items()}, audits)
    for i, row in enumerate(summary):
        index = list(books).index(row["label"])
        partitions = [dest / "exits" / f"b{index:03d}_s{s}.parquet" for s in SEEDS]
        available = read_exits(list(map(str, partitions)))
        row.update(colour=colour, scenario=name, events=scenario.events, recall=scenario.recall,
                   exit_mass=available["mass"].sum(), risk_covered_mass=0., conditional_loss_risk=None)
        if available.filter(pl.col("ply") < 30).height:
            risk_path = dest / f"risk-{row['label']}.parquet"
            stage(f"{colour}-risk-{name}-{row['label']}",
                  cmd("exit_difficulty.py", "--exits", *partitions, "--colour", colour,
                      "--our-moves", paths[0], "--opp-moves", paths[1],
                      "--our-elo", scenario.ours[1], "--opp-elo", scenario.theirs[1],
                      "--max-ply", 30, "--top", 100, "--games", 64,
                      "--sf-cache", sf, "--out", risk_path), [risk_path])
            risk = pl.read_parquet(risk_path)
            mass = risk["mass"].sum()
            row.update(risk_covered_mass=mass, conditional_loss_risk=risk["risk"].dot(risk["mass"]) / mass)
    publish(dest / "summary.json", summary)
    all_rows = [r for p in OUT.glob("*/comparison/*/summary.json") for r in json.loads(p.read_text())]
    tmp = OUT / "comparison.csv.tmp"
    pl.DataFrame(all_rows, infer_schema_length=None).write_csv(tmp)
    tmp.replace(OUT / "comparison.csv")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "logs").mkdir(exist_ok=True)
    with (OUT / "run.lock").open("a+b") as lock:
        lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        try:
            if "--wait-for-tests" in sys.argv:
                status(state="waiting-for-tests", log=OUT / "logs/tests.log")
                start = time.monotonic()
                while True:
                    log = (OUT / "logs/tests.log").read_text(encoding="utf-8", errors="replace")
                    if re.search(r"\d+/\d+ test files passed", log):
                        break
                    if time.monotonic() - start > 7200:
                        raise RuntimeError("Full suite has not finished after two hours")
                    time.sleep(5)
            setup()
            import practical_scenarios as ps
            books, audits = {}, {}
            # Publish the first baseline comparison before spending time on other cohorts.
            for colour in ("black", "white"):
                books[colour] = build(colour)
                audits[colour] = audit(colour, books[colour])
                compare(colour, books[colour], audits[colour], "baseline", ps.SCENARIOS["baseline"])
            for name in ("strong-opponent", "rapid", "blitz", "classical", "future", "recall85"):
                for colour in ("black", "white"):
                    compare(colour, books[colour], audits[colour], name, ps.SCENARIOS[name])
            # Blind practice for the new versus frozen 2,000-decision books.
            for colour in books:
                args = ["deck", "--colour", colour, "--out", OUT / colour / "practice"]
                for index, label in enumerate(books[colour]):
                    if label == "LEARNED":
                        continue
                    args += ["--book", f"{label}={books[colour][label]}"]
                    for seed in SEEDS:
                        partition = OUT / colour / "comparison/baseline/exits" / f"b{index:03d}_s{seed}.parquet"
                        args += ["--exits", f"{label}={partition}"]
                stage(f"{colour}-practice", cmd("exit_practice.py", *args),
                      [OUT / colour / "practice/session-1.pgn", OUT / colour / "practice/session-2.pgn"])
            setup()  # Recheck frozen canonical hashes.
            status(state="complete", comparison=OUT / "comparison.csv")
        except Exception as error:
            status(state="failed", error=str(error))
            raise


if __name__ == "__main__":
    if "--prepare-white" in sys.argv:
        STATUS_FILE = OUT / "white-preparation-status.json"
        setup()
        try:
            prepare_white()
            status(state="complete", result=OUT / "white/cutoff-study/summary.json")
        except Exception as exc:
            status(state="failed", error=str(exc))
            raise
    else:
        main()
