"""
Stage B CLI: targeted Stockfish pass for annotation threat/plan lines.

The eval DB gives candidate *evals* but no *lines*. This pass produces the
concrete variations annotations quote, for exactly the annotation targets:

  mode "here"   — MultiPV-3 depth-D at the card position (our plans/best tries)
  mode "after"  — MultiPV-3 depth-D after our best_move (opponent's tries + our
                  follow-ups; the reply refutations)
  mode "threat" — after our best_move, push a NULL move and analyse depth-Dt:
                  the concrete threat if the opponent does nothing. Skipped when
                  the after-position is in check (null move illegal).

Targets are the union over both colors' fact sheets (deduped by EPD). Resumable:
cache is keyed (epd, mode); skip anything already present at >= requested depth.

Cache E:/chess/engine-cache/annotation_evals.parquet, one row per PV:
  epd | mode | rank | cp | pv_uci | depth | multipv | evaluated_at

DO NOT run while the recompression job is using the CPU — this wants all cores.

Usage:
    set PYTHONPATH=<repo>\\python
    .venv\\Scripts\\python.exe -m annotate.engine_pass
        [--data DIR] [--workers 5] [--threads 2] [--depth 18] [--threat-depth 14]
        [--multipv 3] [--stockfish stockfish] [--limit N]  (--limit = smoke test)
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
import time
from multiprocessing import Process, Queue
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import chess
import chess.engine
import polars as pl

from trainer_app.config import resolve_data_dir

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

CACHE = Path("E:/chess/engine-cache/annotation_evals.parquet")
MATE = 10000


# ── targets ──────────────────────────────────────────────────────────────────

def collect_targets(data_dir: Path) -> list[tuple[str, str]]:
    """Union of (epd, mode) analysis targets over both colors' fact sheets.
    'here' at the card EPD; 'after'+'threat' at the post-best_move EPD."""
    seen: set[tuple[str, str]] = set()
    targets: list[tuple[str, str]] = []
    for color in ("white", "black"):
        fp = data_dir / "annotations" / f"facts_{color}.parquet"
        if not fp.exists():
            continue
        df = pl.read_parquet(fp, columns=["facts_json"])
        for (fj,) in df.iter_rows():
            f = json.loads(fj)
            board = _board_from_line(f["line"])
            if board is None:
                continue
            here = board.epd()
            board.push_san(f["our_move"]["san"])
            after = board.epd()
            for epd, mode in ((here, "here"), (after, "after")):
                if (epd, mode) not in seen:
                    seen.add((epd, mode)); targets.append((epd, mode))
            if not board.is_check():                    # null move legal
                if (after, "threat") not in seen:
                    seen.add((after, "threat")); targets.append((after, "threat"))
    return targets


def _board_from_line(line: str) -> chess.Board | None:
    import re
    b = chess.Board()
    try:
        for tok in line.replace(".", ". ").split():
            tok = re.sub(r"^\d+\.*$", "", tok)
            tok = re.sub(r"^\d+\.+", "", tok)
            if tok:
                b.push_san(tok)
    except ValueError:
        return None
    return b


# ── cache ────────────────────────────────────────────────────────────────────

def load_cache(path: Path) -> dict[tuple[str, str], list[dict]]:
    if not path.exists():
        return {}
    out: dict[tuple[str, str], list[dict]] = {}
    for r in pl.read_parquet(path).iter_rows(named=True):
        out.setdefault((r["epd"], r["mode"]), []).append(r)
    return out


def save_cache(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    pl.from_dicts(rows).write_parquet(tmp, compression="zstd")
    tmp.replace(path)


# ── worker ───────────────────────────────────────────────────────────────────

def _analyse_one(engine, epd, mode, depth, threat_depth, multipv) -> list[dict]:
    board = chess.Board(epd)
    now = datetime.datetime.now().isoformat(timespec="seconds")
    if mode == "threat":
        board.push(chess.Move.null())
        info = engine.analyse(board, chess.engine.Limit(depth=threat_depth))
        pv = info.get("pv", [])
        cp = info["score"].white().score(mate_score=MATE)
        return [{"epd": epd, "mode": mode, "rank": 1,
                 "cp": int(cp) if cp is not None else None,
                 "pv_uci": " ".join(m.uci() for m in pv),
                 "depth": threat_depth, "multipv": 1, "evaluated_at": now}]
    infos = engine.analyse(board, chess.engine.Limit(depth=depth), multipv=multipv)
    rows = []
    for rank, info in enumerate(infos, 1):
        pv = info.get("pv", [])
        cp = info["score"].white().score(mate_score=MATE)
        rows.append({"epd": epd, "mode": mode, "rank": rank,
                     "cp": int(cp) if cp is not None else None,
                     "pv_uci": " ".join(m.uci() for m in pv),
                     "depth": depth, "multipv": multipv, "evaluated_at": now})
    return rows


def worker(wid, jobs, out_q, sf, threads, depth, threat_depth, multipv):
    engine = chess.engine.SimpleEngine.popen_uci(sf)
    try:
        try:
            engine.configure({"Threads": threads, "Hash": 192})
        except Exception:
            pass
        for epd, mode in jobs:
            try:
                rows = _analyse_one(engine, epd, mode, depth, threat_depth, multipv)
                out_q.put(rows)
            except Exception as e:
                out_q.put([{"_error": f"{mode} {epd[:50]}: {e}"}])
    finally:
        engine.quit()
        out_q.put(None)   # sentinel: this worker is done


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=None)
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--depth", type=int, default=18)
    ap.add_argument("--threat-depth", type=int, default=14)
    ap.add_argument("--multipv", type=int, default=3)
    ap.add_argument("--stockfish", default="stockfish")
    ap.add_argument("--cache", default=str(CACHE))
    ap.add_argument("--limit", type=int, default=None, help="smoke test: first N targets")
    ap.add_argument("--save-every", type=int, default=200)
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    cache_path = Path(args.cache)

    targets = collect_targets(data_dir)
    cache = load_cache(cache_path)
    todo = [(e, m) for (e, m) in targets
            if not any(r.get("depth", 0) >= (args.threat_depth if m == "threat" else args.depth)
                       for r in cache.get((e, m), []))]
    if args.limit:
        todo = todo[:args.limit]

    n_by_mode = {m: sum(1 for _, mm in targets if mm == m) for m in ("here", "after", "threat")}
    print(f"Targets: {len(targets):,} ({n_by_mode}); cached: {len(targets)-len([t for t in targets if t in {(e,m) for e,m in todo}]):,}; "
          f"to analyse: {len(todo):,}", flush=True)
    if not todo:
        print("Nothing to do.")
        return

    # flat list of all cache rows (existing + new) for atomic rewrites
    all_rows = [r for rows in cache.values() for r in rows]

    # round-robin partition
    parts: list[list] = [[] for _ in range(args.workers)]
    for i, t in enumerate(todo):
        parts[i % args.workers].append(t)

    out_q: Queue = Queue(maxsize=args.workers * 4)
    procs = [Process(target=worker,
                     args=(w, parts[w], out_q, args.stockfish, args.threads,
                           args.depth, args.threat_depth, args.multipv))
             for w in range(args.workers) if parts[w]]
    for p in procs:
        p.start()

    t0 = time.time()
    done = 0
    finished = 0
    n_err = 0
    while finished < len(procs):
        item = out_q.get()
        if item is None:
            finished += 1
            continue
        if item and "_error" in item[0]:
            n_err += 1
            if n_err <= 10:
                print("  WARN:", item[0]["_error"], flush=True)
            continue
        all_rows.extend(item)
        done += 1
        if done % args.save_every == 0:
            save_cache(all_rows, cache_path)
            el = time.time() - t0
            rate = done / el if el else 0
            eta = (len(todo) - done) / rate / 60 if rate else 0
            print(f"  [{done:>6,}/{len(todo):,}]  {rate:4.1f} analyses/s  "
                  f"ETA {eta:.1f} min", flush=True)
    for p in procs:
        p.join()
    save_cache(all_rows, cache_path)
    print(f"\nDone: {done:,} analyses, {n_err} errors, "
          f"{(time.time()-t0)/60:.1f} min -> {cache_path}")


if __name__ == "__main__":
    main()
