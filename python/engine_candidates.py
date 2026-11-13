"""Engine-proposed candidate moves for the budget builder (follow-up review R5, 2026-10-31).

The builder only considers moves somebody played (at the pool's floor) plus the source book's
move, so where every recorded move fails --max-eval-loss it books the least-losing one even
when a sound move exists (Black's 10...g5, where Stockfish wants the unplayed Qb4). This
searches such positions with a single principal variation -- no MultiPV: one move per
position is all a candidate needs -- and writes the move, its child and the child's eval,
for build_budget_books --engine-candidates, which judges it in the complete candidate set
under the same gates and loss limit as a recorded move.

Positions come from a pre-selection graph (--targets, from build_budget_books
--dump-engine-targets), or validate_book's rows (--rows PREFIX.rows.parquet): our decisions whose
status is fallback or unknown, or whose cached loss exceeds the limit. A result per position
is cached in --out (rerun-safe: done positions are skipped); every row records the engine,
its version, the node budget and the date.

    PYTHONPATH=python .venv/Scripts/python.exe python/engine_candidates.py \\
        --rows PREFIX.rows.parquet --colour white --out E:/chess/repertoire/_engine/white.parquet \\
        [--nodes 10000000]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import chess
import chess.engine
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from zobrist import zobrist_int64  # noqa: E402

SF = "C:/Users/David/AppData/Local/Microsoft/WinGet/Links/stockfish.exe"
SCHEMA = {"position_hash": pl.Int64, "position_epd": pl.Utf8, "move_san": pl.Utf8,
          "child_hash": pl.Int64, "child_cp_white": pl.Int32, "nodes": pl.Int64,
          "depth": pl.Int32, "engine": pl.Utf8, "searched": pl.Utf8, "reason": pl.Utf8}


def target_rows(graph, colour):
    """Reachable our-turn decisions, including positions an initial book would omit."""
    exceptions = graph.meta.get("loss_exceptions", {})
    rows = []
    for h, node in graph.our.items():
        # a Graph's OurNode, or a budget_rs.GraphView's candidate count
        has_cands = node.cands if hasattr(node, "cands") else node
        reason = exceptions.get(h) if has_cands else "empty"
        if reason and graph.epd.get(h):
            rows.append((h, graph.epd[h], colour, reason, graph.reach[h], graph.ply[h]))
    schema = {"position_hash": pl.Int64, "epd": pl.String, "colour": pl.String,
              "status": pl.String, "reach": pl.Float64, "ply": pl.Int32}
    return pl.DataFrame(rows, schema=schema, orient="row").sort(
        ["reach", "position_hash"], descending=[True, False])


def main():
    ap = argparse.ArgumentParser()
    inputs = ap.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--rows", help="validate_book's PREFIX.rows.parquet")
    inputs.add_argument("--targets", help="build_budget_books --dump-engine-targets output")
    ap.add_argument("--limit", type=int, help="maximum targets, ranked by reach for --targets")
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--nodes", type=int, default=10_000_000)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    if a.nodes < 1 or a.threads < 1 or (a.limit is not None and a.limit < 1):
        ap.error("nodes, threads and limit must be positive")
    if a.targets:
        rows = pl.read_parquet(a.targets)
        if rows.filter(pl.col("colour") != a.colour).height:
            ap.error("target colour differs from --colour")
        rows = rows.sort(["reach", "position_hash"], descending=[True, False])
    else:
        rows = pl.read_parquet(a.rows).filter(
            pl.col("legal") & ~pl.col("extension")
            & (pl.col("status").is_in(["fallback", "unknown"]) | pl.col("flagged")))
    if a.limit is not None:
        rows = rows.head(a.limit)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    meta = Path(str(out) + ".meta.json")
    contract = {"input_sha256": hashlib.sha256(Path(a.targets or a.rows).read_bytes()).hexdigest(),
                "nodes": a.nodes, "colour": a.colour, "threads": a.threads, "limit": a.limit,
                "engine_sha256": hashlib.sha256(Path(SF).read_bytes()).hexdigest()}
    if ((meta.exists() and json.loads(meta.read_text()) != contract)
            or (out.exists() and not meta.exists())):
        raise SystemExit(f"Use a fresh output for this search configuration: {out}")
    temp_meta = Path(str(meta) + ".tmp")
    temp_meta.write_text(json.dumps(contract))
    temp_meta.replace(meta)
    have = pl.read_parquet(out) if out.exists() else pl.DataFrame(schema=SCHEMA)
    done = set(have["position_hash"].to_list())
    todo = rows.filter(~pl.col("position_hash").is_in(list(done)))
    print(f"{rows.height:,} positions to propose for, {len(done):,} cached, {todo.height:,} to search",
          flush=True)
    if not todo.height:
        if not out.exists():
            tmp = out.with_suffix(".parquet.tmp")
            have.write_parquet(tmp)
            tmp.replace(out)
        return
    eng = chess.engine.SimpleEngine.popen_uci(SF)
    eng.configure({"Threads": a.threads, "Hash": 2048})
    name = eng.id.get("name", "stockfish")
    new = []
    try:
        for i, r in enumerate(todo.iter_rows(named=True)):
            b = chess.Board(r["epd"] + " 0 1")
            if b.turn != (a.colour == "white") or zobrist_int64(b) != r["position_hash"]:
                raise ValueError("target EPD, hash or side to move disagree")
            eng.configure({"Clear Hash": None})
            info = eng.analyse(b, chess.engine.Limit(nodes=a.nodes))
            mv = info["pv"][0]
            san = b.san(mv)
            b.push(mv)
            reason = r["status"] if r["status"] in ("fallback", "unknown", "empty") else "flagged"
            new.append({"position_hash": r["position_hash"], "position_epd": r["epd"],
                        "move_san": san, "child_hash": zobrist_int64(b),
                        "child_cp_white": info["score"].white().score(mate_score=2000),
                        "nodes": info.get("nodes", a.nodes), "depth": info.get("depth"),
                        "engine": name, "searched": dt.date.today().isoformat(),
                        "reason": reason})
            if len(new) % 25 == 0 or i == todo.height - 1:
                df = pl.concat([have, pl.DataFrame(new, schema=SCHEMA)])
                tmp = out.with_suffix(".parquet.tmp")
                out.parent.mkdir(parents=True, exist_ok=True)
                df.write_parquet(tmp)
                tmp.replace(out)
                print(f"  {i + 1:,}/{todo.height:,}", flush=True)
    finally:
        eng.quit()
    # a proposal equal to the book's own move adds nothing; keep it anyway (cache), but say so
    df = pl.read_parquet(out) if out.exists() else have
    print(f"{df.height:,} proposals -> {out}")


if __name__ == "__main__":
    main()
