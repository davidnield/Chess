"""Engine-proposed candidate moves for the budget builder (follow-up review R5, 2026-10-31).

The builder only considers moves somebody played (at the pool's floor) plus the source book's
move, so where every recorded move fails --max-eval-loss it books the least-losing one even
when a sound move exists (Black's 10...g5, where Stockfish wants the unplayed Qb4). This
searches such positions with a single principal variation -- no MultiPV: one move per
position is all a candidate needs -- and writes the move, its child and the child's eval,
for build_budget_books --engine-candidates, which judges it in the complete candidate set
under the same gates and loss limit as a recorded move.

Positions come from validate_book's rows (--rows PREFIX.rows.parquet): our decisions whose
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
import sys
from pathlib import Path

import chess
import chess.engine
import polars as pl

sys.path.insert(0, "python")
from zobrist import zobrist_int64  # noqa: E402

SF = "C:/Users/David/AppData/Local/Microsoft/WinGet/Links/stockfish.exe"
SCHEMA = {"position_hash": pl.Int64, "position_epd": pl.Utf8, "move_san": pl.Utf8,
          "child_hash": pl.Int64, "child_cp_white": pl.Int32, "nodes": pl.Int64,
          "depth": pl.Int32, "engine": pl.Utf8, "searched": pl.Utf8, "reason": pl.Utf8}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", required=True, help="validate_book's PREFIX.rows.parquet")
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--nodes", type=int, default=10_000_000)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    rows = pl.read_parquet(a.rows).filter(
        pl.col("legal") & ~pl.col("extension")
        & (pl.col("status").is_in(["fallback", "unknown"]) | pl.col("flagged")))
    out = Path(a.out)
    have = pl.read_parquet(out) if out.exists() else pl.DataFrame(schema=SCHEMA)
    done = set(have["position_hash"].to_list())
    todo = rows.filter(~pl.col("position_hash").is_in(list(done)))
    print(f"{rows.height:,} positions to propose for, {len(done):,} cached, {todo.height:,} to search",
          flush=True)
    eng = chess.engine.SimpleEngine.popen_uci(SF)
    eng.configure({"Threads": a.threads, "Hash": 2048})
    name = eng.id.get("name", "stockfish")
    new = []
    try:
        for i, r in enumerate(todo.iter_rows(named=True)):
            b = chess.Board(r["epd"] + " 0 1")
            eng.configure({"Clear Hash": None})
            info = eng.analyse(b, chess.engine.Limit(nodes=a.nodes))
            mv = info["pv"][0]
            san = b.san(mv)
            b.push(mv)
            reason = r["status"] if r["status"] in ("fallback", "unknown") else "flagged"
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
    same = df.join(rows.select("position_hash", pl.col("move").alias("book")), on="position_hash") \
        .filter(pl.col("move_san").str.strip_chars("+#") == pl.col("book").str.strip_chars("+#")).height
    print(f"{df.height:,} proposals; {same:,} equal the book's own move")


if __name__ == "__main__":
    main()
