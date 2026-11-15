"""One corrected book: build on the uncut own-move lookup with --rare-verification, verify what the
book rests on (rare_verify.py), rebuild with the verified scores, repeat until a verification
pass produces no new evidence.

    python run_rare_book.py --colour black --baseline <cutoff screen dir>
                            --lookup <uncut cache> --out <work dir> --searches <search cache>

New runs use the lifetime expected-score gates. An explicit --cap replays a historical cp arm.

Round r lives in <out>/round-NN: the build (book_b<B>.parquet, its .meta.json and .rare.json) and
the verifier's overlay. Round 0 is the expanded menu before any fresh score; keep it.

Stability. Any pass that searched anything -- at a booked decision or on the unbooked frontier --
is followed by a rebuild that consumes its overlay: a corrected value off the book can make that
line worth a place in it. The run ends when a pass asks for nothing, and only if the final book's
meta names the last overlay that carried new evidence. The frontier is searched in round 0 only
(reach threshold and batch limit are flags); what is left of it is reported in final.json.

Starting from earlier evidence (--seed-overlay). A verified score is a fact about a position, not
about a book, and the position searches are cached. Without a seed a new run still rebuilds once
per round to rediscover which of those scores its book rests on. With the last overlay of another
run of the same colour as the seed, round 0 is built with that evidence already applied
(so it is no longer the bare expanded menu), and the run needs only the rounds its own new
decisions call for. The seed is part of round 0's receipt; final.json names it.

The code a stage runs. The build and the verifier are run from a SNAPSHOT of python/*.py (and the
Rust extension, if built) taken at launch (<out>/code-<hash>/), never from the working tree, so an edit made while a run is live
cannot execute under an old identity. The snapshot is re-hashed before and after every stage.

Resuming. A stage is done only when its RECEIPT exists, and the receipt is written last. It binds
the stage to what made it: the code hash, the command, the lookup's manifest, the overlay that
went in, the search settings, and the hash of every output. On a rerun a receipt is checked:
  - identity and outputs match          -> the stage is reused;
  - no receipt (interrupted)            -> its partial outputs are deleted and it runs again;
  - anything differs (code, budget, a swapped overlay, a missing or truncated output)
                                        -> StaleRound, naming what changed. Nothing is overwritten:
                                           start a fresh --out (the position searches are reusable).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


class StaleRound(RuntimeError):
    pass


def sha(path) -> str:
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def code_files(folder: Path = HERE) -> list[Path]:
    """The maintained code a stage can execute: python/*.py and the Rust extension, if built
    (build_budget_books --rs-core imports it, so it is part of the code's identity)."""
    return sorted([p for p in folder.glob("*.py") if not p.name.startswith("_test_")]
                  + list(folder.glob("book_builder_rs.pyd")))


# build_budget_books options retired on 2026-11-12 (the simplify step): the value each one may
# carry in an older receipt is the behaviour that is now the only one, so it is dropped
RETIRED = {"--max-cands": "0", "--stop-model": "weak", "--price-seed": "source",
           "--price-reprice": "max", "--price-rounds": "1", "--price-select": "best",
           "--price-dag-mode": "polish"}


def modern(cmd: list[str]) -> list[str]:
    """An older receipt's command for today's build_budget_books: retired options dropped,
    refused when one asks for a behaviour that no longer exists."""
    out, i = [], 0
    while i < len(cmd):
        f = cmd[i]
        if f in RETIRED:
            if i + 1 >= len(cmd) or cmd[i + 1] != RETIRED[f]:
                raise ValueError(f"{f} {cmd[i + 1] if i + 1 < len(cmd) else ''}: retired, and "
                                 f"only {RETIRED[f]} (today's behaviour) can be dropped")
            i += 2
            continue
        out.append(f)
        i += 1
    return out


def code_sha(files=None) -> str:
    """One hash over the maintained code a build or a verification can execute."""
    h = hashlib.sha256()
    for p in (code_files() if files is None else files):
        h.update(p.name.encode())
        data = p.read_bytes()
        h.update(data.replace(b"\r\n", b"\n") if p.suffix == ".py" else data)
    return h.hexdigest()


def write_json(path: Path, value) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=1), encoding="utf8")
    tmp.replace(path)


def stage(receipt: Path, identity: dict, outputs: list[Path], work, guard=None) -> tuple[dict, bool]:
    """Run `work()` unless a receipt proves the stage is complete AND is this stage.
    guard(): raises if the code about to run, or that just ran, is not the identity's.
    Returns (receipt, reused)."""
    if receipt.exists():
        old = json.loads(receipt.read_text(encoding="utf8"))
        changed = sorted(k for k in set(old["identity"]) | set(identity)
                         if old["identity"].get(k) != identity.get(k))
        if changed:
            raise StaleRound(f"{receipt}: made with other {', '.join(changed)}")
        for p in outputs:
            want = old["outputs"].get(p.name)
            if want is None or not p.exists() or sha(p) != want:
                raise StaleRound(f"{receipt}: output {p.name} is missing or has changed")
        return old, True
    for p in outputs:                       # an interrupted stage left these; they prove nothing
        p.unlink(missing_ok=True)
    if guard:
        guard()
    t = time.monotonic()
    work()
    if guard:
        guard()                             # the receipt names the bytes that ran, or none is written
    missing = [p.name for p in outputs if not p.exists()]
    if missing:
        raise RuntimeError(f"{receipt.parent}: the stage finished without {missing}")
    done = {"identity": identity, "outputs": {p.name: sha(p) for p in outputs},
            "seconds": time.monotonic() - t, "finished": time.strftime("%Y-%m-%d %H:%M:%S")}
    write_json(receipt, done)
    return done, False


def run(cmd, log: Path, cwd: Path) -> None:
    with open(log, "a", encoding="utf8") as f:
        f.write("\n$ " + subprocess.list2cmdline(map(str, cmd)) + "\n")
        f.flush()
        subprocess.run(list(map(str, cmd)), cwd=cwd, stdout=f, stderr=subprocess.STDOUT, check=True)


def snapshot(work: Path, code: str, source: Path = HERE) -> Path:
    """A runnable copy of the code this run executes, beside its outputs."""
    dest = work / f"code-{code[:12]}"
    if not dest.exists():
        tmp = work / f"code-{code[:12]}.tmp"
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        for p in code_files(source):
            shutil.copy2(p, tmp / p.name)
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=source, capture_output=True,
                              text=True).stdout.strip()
        (tmp / "HEAD.txt").write_text(head + "\n")
        tmp.replace(dest)
    return dest


def lookahead_depth(args):
    # argparse accepts both spellings, and its last occurrence wins.
    ap = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    ap.add_argument("--lookahead-floor", type=int, default=0)
    return ap.parse_known_args(args)[0].lookahead_floor


def drive(a, build=None, verify=None, source: Path = HERE) -> dict:
    """The round loop. build(args, log) and verify(book, overlay_out, frontier_args, log) default
    to the snapshot's build_budget_books.py and rare_verify.py; tests pass their own."""
    import polars as pl

    template = a.template_cap if a.template_cap is not None else (60 if a.cap is None else a.cap)
    receipt = a.baseline / "stages" / f"{a.colour}-cp{template}.json"
    cmd = json.loads(receipt.read_text())["command"]
    base = modern(cmd[cmd.index("--rep"):])
    if a.cap is not None:
        base[base.index("--max-eval-loss") + 1] = str(a.cap)
    else:
        from budget_core import MAX_SCORE_LOSS, SCORE_ABSOLUTE_TOLERANCE
        for flag in ("--max-eval-loss", "--robustness-floor", "--gate-rel-floor", "--gate-rel-own-margin"):
            if flag in base:
                i = base.index(flag)
                del base[i:i+2]
        limit = a.max_score_loss if a.max_score_loss is not None else MAX_SCORE_LOSS
        base.extend(["--max-score-loss", str(limit), "--robustness-floor", str(SCORE_ABSOLUTE_TOLERANCE[a.colour])])
    base += list(getattr(a, "build_arg", None) or [])
    budget = base[base.index("--budget") + 1]
    while a.wait and not (a.lookup / "ready.json").exists():
        time.sleep(30)
    lookup = json.loads((a.lookup / "ready.json").read_text())["recipe_sha256"]
    a.out.mkdir(parents=True, exist_ok=True)
    code = code_sha(code_files(source))
    kept = snapshot(a.out, code, source)

    def guard():
        if code_sha(code_files(kept)) != code:
            raise StaleRound(f"{kept}: the code snapshot no longer matches its hash")

    py = sys.executable
    build = build or (lambda args, log: run(
        [py, "-X", "utf8", "-u", kept / "build_budget_books.py", *args], log, kept))
    verify = verify or (lambda book, out, front, log: run(
        [py, "-X", "utf8", "-u", kept / "rare_verify.py", "--book", book, "--colour", a.colour,
         "--cache", a.searches, "--out", out, "--nodes", a.nodes, "--deep-nodes", a.deep_nodes,
         "--band", a.band, "--threads", a.threads, *front], log, kept))
    seed = getattr(a, "seed_overlay", None)
    rounds, overlay = [], seed
    for r in range(a.max_rounds):
        rd = a.out / f"round-{r:02d}"
        rd.mkdir(exist_ok=True)
        book = rd / f"book_b{budget}.parquet"
        args = list(base)
        args[args.index("--out-prefix") + 1] = str(rd / "book")
        args[args.index("--tmp-dir") + 1] = str(rd / "tmp")
        args += ["--uncut-own-moves", str(a.lookup), "--rare-verification"]
        if overlay:
            args += ["--engine-candidates", str(overlay)]
        outputs = [book, Path(str(book)+".meta.json"), Path(str(book)+".rare.json")]
        depth = lookahead_depth(args)
        audit_path = Path(str(book)+".lookahead-audit.json")
        if depth:
            outputs.extend([Path(str(book)+".lookahead-inputs.parquet"), Path(str(book)+".lookahead.json"), audit_path])

        def build_and_check():
            build(args, rd / "build.log")
            if depth:
                # Use the captured implementation, even if the live worktree changes.
                run([py, "-X", "utf8", kept / "lookahead_audit.py", book, audit_path,
                     "--depth", str(depth)], rd / "lookahead-audit.log", kept)

        built, reused = stage(
            rd / "build.receipt.json",
            {"code": code, "command": args, "lookup": lookup, "baseline_receipt": sha(receipt),
             "overlay_in": sha(overlay) if overlay else None},
            outputs, build_and_check, guard)
        ours = pl.read_parquet(book).filter((pl.col("side_to_move") == a.colour)
                                            & pl.col("best_move").is_not_null())
        if ours.height != int(budget) or ours["position_hash"].n_unique() != int(budget):
            raise RuntimeError(f"{book}: {ours.height} decisions, expected {budget}")
        meta = json.loads(Path(str(book) + ".meta.json").read_text())
        new = rd / "overlay.parquet"
        front = (["--frontier-reach", a.frontier_reach, "--frontier", a.frontier] if r == 0 else [])
        checked, reused_v = stage(
            rd / "verify.receipt.json",
            {"code": code, "book": built["outputs"][book.name],
             "overlay_in": sha(overlay) if overlay else None, "searches": str(a.searches),
             "nodes": a.nodes, "deep_nodes": a.deep_nodes, "band": a.band, "threads": a.threads,
             "frontier": front and [a.frontier_reach, a.frontier]},
            [new, Path(str(new) + ".meta.json")],
            lambda: verify(book, new, front, rd / "verify.log"), guard)
        info = json.loads(Path(str(new) + ".meta.json").read_text())
        rec = {"round": r, "book": str(book), "root_value": meta.get("root_value_realized"),
               "subgraph_nodes": meta.get("subgraph_nodes"), "our_nodes": meta.get("subgraph_our_nodes"),
               "peak_commit_gb": meta.get("peak_commit_gb"), "disputes": meta.get("rare_disputes"),
               "disputes_booked": meta.get("rare_disputes_booked"),
               "build_seconds": built["seconds"], "build_reused": reused,
               "verify_seconds": checked["seconds"], "verify_reused": reused_v,
               "requested": info["requested_parents"], "by_reason": info["by_reason"],
               "searches": info.get("searches"), "deep_searches": info.get("deep_searches"),
               "retried_parents": len(info.get("retried_parents", [])),
               "deep_carried_forward": sum(x.get("why") == "carried forward"
                                           for x in info.get("retried_parents", [])),
               "engine_choice_unplayed": len(info.get("engine_choice_unplayed", [])),
               "receipts": [str(rd / "build.receipt.json"), str(rd / "verify.receipt.json")]}
        rounds.append(rec)
        write_json(a.out / "rounds.json", rounds)
        print(json.dumps(rec), flush=True)
        if not info["requested_parents"]:
            # Nothing left to search. Every earlier pass's evidence, frontier results included,
            # must already be in this book: its meta names the overlay it was built with.
            applied = meta.get("engine_candidates_sha256")
            if applied != (sha(overlay) if overlay else None):
                raise StaleRound(f"{book}: not built with the last verification's overlay")
            left = [d for d in json.loads(Path(str(book) + ".rare.json").read_text())
                    if not d["booked"] and d["at_risk"]]
            final = {
                "book": str(book), "round": r, "overlay": str(overlay) if overlay else None,
                "applied_overlay_sha256": applied, "code_sha256": code,
                "code_snapshot": str(kept), "lookup": lookup,
                "seed_overlay": str(seed) if seed else None,
                "receipts": [x for rec in rounds for x in rec["receipts"]],
                "frontier": {"searched_in_round_0_only": True, "reach_threshold": a.frontier_reach,
                             "batch_limit": a.frontier, "unbooked_disputes_left": len(left),
                             "left_at_or_above_threshold":
                                 sum(d["reach"] >= a.frontier_reach for d in left),
                             "largest_reach_left": max((d["reach"] for d in left), default=0.0)}}
            write_json(a.out / "final.json", final)
            print("stable", flush=True)
            return final
        overlay = new
    raise RuntimeError("verification did not stabilise")


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--colour", required=True, choices=["black", "white"])
    gate = ap.add_mutually_exclusive_group()
    gate.add_argument("--cap", type=int, help="Replay a historical centipawn cap")
    gate.add_argument("--max-score-loss", type=float, help="Relative score fraction (default 0.095)")
    ap.add_argument("--baseline", type=Path, required=True,
                    help="cutoff screen dir holding stages/<colour>-cp<cap>.json")
    ap.add_argument("--template-cap", type=int, help="take other settings from this cp receipt (score mode default 60)")
    ap.add_argument("--lookup", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--searches", type=Path, required=True,
                    help="position search cache, shared by every gate of this colour")
    ap.add_argument("--nodes", type=int, default=5_000_000)
    ap.add_argument("--deep-nodes", type=int, default=20_000_000)
    ap.add_argument("--band", type=int, default=10)
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--frontier-reach", type=float, default=0.002)
    ap.add_argument("--frontier", type=int, default=300)
    ap.add_argument("--max-rounds", type=int, default=8)
    ap.add_argument("--wait", action="store_true", help="wait for the lookup's ready.json")
    ap.add_argument("--seed-overlay", type=Path, metavar="PARQUET",
                    help="the last overlay of another run of this colour: round 0 starts "
                         "from its verified scores (see the header)")
    ap.add_argument("--build-arg", action="append", default=[], metavar="ARG",
                    help="an extra build_budget_books argument (repeatable), e.g. "
                         "--build-arg=--rs-core")
    return ap


if __name__ == "__main__":
    drive(parser().parse_args())
