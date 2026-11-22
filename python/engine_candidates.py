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
from consumer_keys import PAIR, checked_board, full_column, stored_key
from month_archive import file_lock, sha

from engine_path import STOCKFISH as SF  # noqa: E402  (Stockfish 19 since 2026-11-21)
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


def check_edges(frame, wide):
    """Verify the persisted parent and child against the engine's board and move."""
    if wide:
        frame = frame.with_columns(full_column(frame, key) for key in ("position_hash", "child_hash"))
    for parent, epd, san, child in frame.select(
            "position_hash", "position_epd", "move_san", "child_hash").iter_rows():
        if parent is None:
            raise ValueError("proposal lacks its parent position")
        board = checked_board(epd, parent)
        board.push_san(san)
        if stored_key(board, wide) != child:
            raise ValueError(f"proposal child hash disagrees with {epd} {san}")


def publish(frame, dest):
    tmp = Path(str(dest) + ".tmp")
    frame.write_parquet(tmp)
    tmp.replace(dest)


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
    out = Path(a.out)
    with file_lock(Path(str(out) + ".lock"), wait=True):
        generate(a, out)


def generate(a, out):
    snapshot = Path(a.targets or a.rows).read_bytes()
    rows = pl.read_parquet(snapshot)
    wide = rows.schema["position_hash"] == PAIR or "position_hash_hi" in rows.columns
    if wide:
        rows = rows.with_columns(full_column(rows, "position_hash")).drop("position_hash_hi", strict=False)
    elif rows.schema["position_hash"] != pl.Int64:
        raise ValueError("invalid target position-key type")
    if a.targets:
        if rows.filter(pl.col("colour") != a.colour).height:
            raise ValueError("target colour differs from --colour")
        rows = rows.sort(["reach", "position_hash"], descending=[True, False])
    else:
        rows = rows.filter(
            pl.col("legal") & ~pl.col("extension")
            & (pl.col("status").is_in(["fallback", "unknown"]) | pl.col("flagged")))
    if a.limit is not None:
        rows = rows.head(a.limit)
    if rows["position_hash"].null_count():
        raise ValueError("target has no position key")
    if rows.select("position_hash", "epd").unique()["position_hash"].is_duplicated().any():
        raise ValueError("one target key names different boards")
    out.parent.mkdir(parents=True, exist_ok=True)
    meta = Path(str(out) + ".meta.json")
    pending = Path(str(out) + ".partial.parquet")
    contract = {"input_sha256": hashlib.sha256(snapshot).hexdigest(),
                "nodes": a.nodes, "colour": a.colour, "threads": a.threads, "limit": a.limit,
                "engine_sha256": sha(Path(SF))}
    if wide:
        contract["key_bits"] = 128
    if ((meta.exists() and json.loads(meta.read_text()) != contract)
            or ((out.exists() or pending.exists()) and not meta.exists())):
        raise SystemExit(f"Use a fresh output for this search configuration: {out}")
    temp_meta = Path(str(meta) + ".tmp")
    temp_meta.write_text(json.dumps(contract))
    temp_meta.replace(meta)
    schema = SCHEMA | ({"position_hash": PAIR, "child_hash": PAIR} if wide else {})
    have = pl.read_parquet(out) if out.exists() else pl.DataFrame(schema=schema)
    if have.schema != schema:
        raise ValueError(f"{out}: proposal cache has the wrong schema")
    if pending.exists():
        # A crash after publishing the batch may leave identical rows in both files.
        tail = pl.read_parquet(pending)
        if tail.schema != schema:
            raise ValueError(f"{pending}: proposal checkpoint has the wrong schema")
        have = pl.concat([have, tail]).unique(maintain_order=True)
    if have.height:
        check_edges(have, wide)
        targets = rows.select("position_hash", pl.col("epd").alias("position_epd"))
        if (have["position_hash"].n_unique() != have.height or
                have.join(targets, on=["position_hash", "position_epd"], how="anti").height):
            raise ValueError(f"{out}: duplicate or unrequested cached position")
    if pending.exists():
        publish(have, out)
        pending.unlink()
    todo = rows.unique("position_hash", maintain_order=True).join(have, on="position_hash", how="anti",
                                                                 maintain_order="left")
    print(f"{rows.height:,} positions to propose for, {have.height:,} cached, {todo.height:,} to search",
          flush=True)
    if not todo.height:
        if not out.exists():
            publish(have, out)
        return
    eng = chess.engine.SimpleEngine.popen_uci(SF)
    new = []
    try:
        eng.configure({"Threads": a.threads, "Hash": 2048})
        name = eng.id.get("name", "stockfish")
        for i, r in enumerate(todo.iter_rows(named=True)):
            b = checked_board(r["epd"], r["position_hash"])
            if b.turn != (a.colour == "white"):
                raise ValueError("target side to move disagrees")
            eng.configure({"Clear Hash": None})
            info = eng.analyse(b, chess.engine.Limit(nodes=a.nodes))
            mv = info["pv"][0]
            san = b.san(mv)
            b.push(mv)
            reason = r["status"] if r["status"] in ("fallback", "unknown", "empty") else "flagged"
            new.append({"position_hash": r["position_hash"], "position_epd": r["epd"],
                        "move_san": san, "child_hash": stored_key(b, wide),
                        "child_cp_white": info["score"].white().score(mate_score=2000),
                        "nodes": info.get("nodes", a.nodes), "depth": info.get("depth"),
                        "engine": name, "searched": dt.date.today().isoformat(),
                        "reason": reason})
            batch = pl.DataFrame(new, schema=schema)
            if len(new) == 25 or i == todo.height - 1:
                have = pl.concat([have, batch])
                publish(have, out)
                pending.unlink(missing_ok=True)
                new.clear()
                print(f"  {i + 1:,}/{todo.height:,}", flush=True)
            else:
                publish(batch, pending)
    finally:
        eng.quit()
    # a proposal equal to the book's own move adds nothing; keep it anyway (cache), but say so
    df = pl.read_parquet(out) if out.exists() else have
    print(f"{df.height:,} proposals -> {out}")


if __name__ == "__main__":
    main()
