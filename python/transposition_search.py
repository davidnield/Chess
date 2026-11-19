"""Re-search materially conflicting incoming PVs from their shared child position.

Add a position-level score to the proposal overlay without altering move-specific
searches, losses or provenance. Cache the independent child searches across rounds.
"""
import argparse
import json
from pathlib import Path

import chess
import chess.engine
import polars as pl

from engine_candidates import SF
from hybrid_candidates import sha, write_frame, write_json
from consumer_keys import PAIR, checked_board, stored_key


def material_children(frame, spread_cp):
    groups = frame.group_by("child_hash").agg(
        pl.col("position_hash").n_unique().alias("parents"),
        pl.col("child_cp_white").min().alias("low"),
        pl.col("child_cp_white").max().alias("high"))
    return groups.filter((pl.col("parents") > 1) & (pl.col("high") - pl.col("low") > spread_cp))


def child_epd(group, child_hash):
    positions = set()
    for parent, epd, san in group.select("position_hash", "position_epd", "move_san").iter_rows():
        board = checked_board(epd, parent)
        board.push_san(san)
        if stored_key(board,isinstance(child_hash,dict)) != child_hash:
            raise ValueError(f"Transposition child mismatch at {parent} {san}")
        positions.add(board.epd(en_passant="xfen"))
    if len(positions) != 1:
        raise ValueError(f"Different child positions for hash {child_hash}")
    return positions.pop()


def resolve(frame, cache, spread_cp=30, nodes=30_000_000):
    wide = frame.schema["child_hash"] == PAIR
    cache = Path(cache) / "key128" if wide else Path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    contract = {"engine_sha256": sha(SF), "script_sha256": sha(__file__), "nodes": nodes,
                "threads": 1, "hash_mb": 128, "mate_cp": 2000}
    if wide: contract["key_bits"] = 128
    config = cache / "config.json"
    if config.exists() and json.loads(config.read_text()) != contract:
        raise ValueError("Transposition search cache settings changed")
    write_json(contract, config)
    conflicts = material_children(frame, spread_cp)
    edges = frame.join(conflicts, on="child_hash", how="inner").sort("child_hash", "position_hash")
    records = []
    engine = None
    try:
        for (child,), group in edges.group_by("child_hash", maintain_order=True):
            epd = child_epd(group, child)
            name = f"{child['lo']}_{child['hi']}" if wide else str(child)
            file = cache / f"{name}.json"
            if file.exists():
                saved = json.loads(file.read_text())
                if saved["epd"] != epd or saved["child_hash"] != child:
                    raise ValueError(f"Transposition cache identity mismatch at {child}")
            else:
                board = chess.Board(epd + " 0 1")
                outcome = board.outcome()
                if outcome:
                    cp = 0 if outcome.winner is None else (2000 if outcome.winner else -2000)
                    searched_nodes, depth = 0, 0
                else:
                    if engine is None:
                        engine = chess.engine.SimpleEngine.popen_uci(SF)
                        engine.configure({"Threads": 1, "Hash": contract["hash_mb"]})
                    engine.configure({"Clear Hash": None})
                    info = engine.analyse(board, chess.engine.Limit(nodes=nodes), game=child)
                    cp = info["score"].white().score(mate_score=contract["mate_cp"])
                    searched_nodes, depth = info["nodes"], info["depth"]
                saved = {"child_hash": child, "epd": epd, "cp_white": cp,
                         "nodes": searched_nodes, "depth": depth}
                write_json(saved, file)
            records.append({**saved, "low": group["low"][0], "high": group["high"][0],
                            "parents": group["parents"][0]})
    finally:
        if engine is not None:
            engine.quit()
    direct = pl.DataFrame([(r["child_hash"], r["cp_white"]) for r in records],
                          schema={"child_hash": PAIR if wide else pl.Int64, "transposition_cp_white": pl.Int32}, orient="row")
    # A resolved child remains evidence even when later incoming scores happen to agree.
    if "transposition_cp_white" in frame.columns:
        old=frame.select("child_hash","transposition_cp_white").drop_nulls().unique()
        if old["child_hash"].n_unique()!=old.height:
            raise ValueError("conflicting previous direct transposition scores")
        direct=pl.concat([old.join(direct.select("child_hash"),on="child_hash",how="anti"),direct])
    result = frame.drop("transposition_cp_white", strict=False).join(direct, on="child_hash", how="left")
    return result.sort("position_hash", "child_hash"), {"spread_cp": spread_cp, "contract": contract,
                                                        "positions": records}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("proposals", "cache", "out"):
        parser.add_argument("--" + arg, required=True, type=Path)
    args = parser.parse_args()
    result, report = resolve(pl.read_parquet(args.proposals), args.cache)
    write_frame(result, args.out)
    write_json({**report, "input_sha256": sha(args.proposals), "output_sha256": sha(args.out)},
               Path(str(args.out) + ".meta.json"))
    print(f"{len(report['positions'])} material transpositions among {result.height} proposals", flush=True)


if __name__ == "__main__":
    main()
