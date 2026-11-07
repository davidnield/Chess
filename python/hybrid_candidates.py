"""Fill preselection targets from the verified Lichess DB and local Stockfish.
One immutable checkpoint per cloud bucket, then reuse the standard engine runner
for remaining targets. A legacy local checkpoint takes precedence over all cloud hits.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import chess
import duckdb
import polars as pl

import engine_candidates as ec
from zobrist import zobrist_int64

CLOUD = "Lichess cloud PV"
MIN_DEPTH = 22
MIN_KNODES = 10000


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_frame(frame, path):
    tmp = path.with_suffix(".tmp")
    frame.write_parquet(tmp)
    tmp.replace(path)


def write_json(value, path):
    tmp = path.with_suffix(".tmp.json")
    tmp.write_text(json.dumps(value, indent=2), encoding="utf-8")
    tmp.replace(path)


def cloud_bucket(con, targets, db, bucket, colour, date):
    con.register("wanted", targets.to_arrow())
    rows = con.execute("""SELECT t.position_hash, t.epd, t.status, e.cloud_line,
                        e.cloud_cp, e.cloud_mate, e.cloud_depth, e.cloud_knodes
        FROM read_parquet(?) e JOIN wanted t USING(position_hash,epd)
        WHERE e.source='cloud' AND NOT e.fishnet_disagrees AND NOT e.hash_ambiguous
        AND e.cloud_depth >= ? AND e.cloud_knodes >= ? AND e.cloud_line <> ''
        AND (e.cloud_cp IS NULL) <> (e.cloud_mate IS NULL)
        ORDER BY t.position_hash""", [str(db / f"bkt{bucket:03d}.parquet"), MIN_DEPTH, MIN_KNODES]).fetchall()
    proposals = []
    for h, epd, status, line, cp, mate, depth, knodes in rows:
        board = chess.Board(epd + " 0 1")
        if board.turn != (colour == "white") or zobrist_int64(board) != h or not board.is_valid():
            raise ValueError(f"Invalid target identity: {h}")
        try:
            move = board.parse_uci(line.split()[0])
        except (ValueError, IndexError):
            continue
        if move not in board.legal_moves or mate == 0:
            continue
        san = board.san(move)
        board.push(move)
        score = cp if cp is not None else (2000 if mate > 0 else -2000)
        proposals.append((h, epd, san, zobrist_int64(board), score, knodes * 1000,
                          depth, CLOUD, date, status))
    return pl.DataFrame(proposals, schema=ec.SCHEMA, orient="row")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", required=True, type=Path)
    parser.add_argument("--colour", required=True, choices=["black", "white"])
    parser.add_argument("--db", type=Path, default=Path("D:/chess/eval_full"))
    parser.add_argument("--keep-local", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--nodes", type=int, default=10_000_000)
    parser.add_argument("--threads", type=int, default=6)
    args = parser.parse_args()
    if not (args.db / "_DONE").is_file():
        raise ValueError("Incomplete evaluation database")
    parts = Path(str(args.out) + ".parts")
    parts.mkdir(parents=True, exist_ok=True)
    contract = {"targets": sha(args.targets), "db": str(args.db.resolve()),
                "db_build": sha(args.db / "_build.meta.json"), "db_manifest": sha(args.db / "_manifest.parquet"),
                "depth": MIN_DEPTH, "knodes": MIN_KNODES, "colour": args.colour,
                "local_nodes": args.nodes, "threads": args.threads, "script": sha(__file__),
                "legacy": sha(args.keep_local) if args.keep_local else None, "engine": sha(ec.SF)}
    lock = parts / "inputs.json"
    if lock.exists() and json.loads(lock.read_text()) != contract:
        raise ValueError("Hybrid checkpoint has different inputs")
    write_json(contract, lock)
    targets = pl.read_parquet(args.targets)
    if targets["position_hash"].n_unique() != targets.height or targets.filter(pl.col("colour") != args.colour).height:
        raise ValueError("Duplicate or wrong-colour targets")
    keys = ["position_hash", "epd"]
    local = pl.DataFrame(schema=ec.SCHEMA)
    if args.keep_local:
        old = json.loads(Path(str(args.keep_local) + ".meta.json").read_text())
        if old != {"input_sha256": contract["targets"], "nodes": args.nodes, "colour": args.colour,
                   "threads": args.threads, "limit": None, "engine_sha256": contract["engine"]}:
            raise ValueError("Legacy Stockfish cache has different inputs")
        local = pl.read_parquet(args.keep_local)
    known = local.rename({"position_epd": "epd"}).select(keys)
    if known.join(targets, on=keys, how="anti").height or known.n_unique() != known.height:
        raise ValueError("Invalid preserved checkpoint")
    todo = targets.join(known, on=keys, how="anti")
    print(f"Retaining {local.height} Stockfish results; querying cloud for {todo.height}.", flush=True)
    hits = []
    with duckdb.connect() as con:
        con.execute("SET threads=3")
        groups = todo.with_columns((pl.col("position_hash") % 512).alias("bucket")).partition_by("bucket", as_dict=True)
        for i, ((bucket,), group) in enumerate(sorted(groups.items()), 1):
            path = parts / f"cloud-{bucket:03d}.parquet"
            if not path.exists():
                write_frame(cloud_bucket(con, group, args.db, bucket, args.colour, datetime.date.today().isoformat()), path)
            hits.append(pl.read_parquet(path))
            if i % 16 == 0:
                print(f"Cloud buckets {i}/{len(groups)}", flush=True)
    cloud = pl.concat(hits) if hits else local.clear()
    missing = todo.join(cloud.rename({"position_epd":"epd"}).select(keys), on=keys, how="anti")
    misses = parts / "misses.parquet"
    if not misses.exists():
        write_frame(missing, misses)
    elif not pl.read_parquet(misses).equals(missing):
        raise ValueError("Remaining target set changed")
    print(f"{cloud.height} cloud hits, {missing.height} fresh Stockfish searches.", flush=True)
    result = parts / "local.parquet"
    subprocess.run([sys.executable, ec.__file__, "--colour", args.colour, "--targets", str(misses),
                    "--out", str(result), "--threads", str(args.threads), "--nodes", str(args.nodes)], check=True)
    final = pl.concat([local, cloud, pl.read_parquet(result)])
    identities = final.rename({"position_epd":"epd"}).select(keys)
    if final.height != targets.height or identities.n_unique() != final.height or identities.join(targets,on=keys,how="anti").height:
        raise ValueError("Hybrid proposals do not cover the exact target set")
    write_frame(final, args.out)
    write_json({**contract, "preserved_local":local.height, "cloud":cloud.height, "fresh_local":missing.height},
               Path(str(args.out) + ".meta.json"))


if __name__ == "__main__":
    main()
