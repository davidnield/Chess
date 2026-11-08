"""Request cloud nominations on a graph, or replay only explicitly requested local nominations."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

import chess
import duckdb
import polars as pl

import engine_candidates as ec
from hybrid_candidates import CLOUD, cloud_bucket, sha, write_frame, write_json
from zobrist import zobrist_int64

KEYS = ["position_hash", "epd"]


def replay(wanted, sources):
    """The request list is fixed before examining local results. No child or verification overlay enters."""
    frames = []
    for src in sources:
        raw = pl.read_parquet(src)
        if set(raw.columns) != set(ec.SCHEMA):
            raise ValueError(f"Expected raw nominations: {src}")
        eligible = raw.filter(pl.col("engine") != CLOUD).rename({"position_epd":"epd"})
        frames.append(eligible.join(wanted.select(KEYS), on=KEYS, how="semi"))
    if not frames:
        return pl.DataFrame(schema=ec.SCHEMA)
    found = pl.concat(frames).rename({"epd":"position_epd"}).select(ec.SCHEMA.keys()).unique()
    if found["position_hash"].n_unique() != found.height:
        raise ValueError("Ambiguous replay evidence")
    for h, epd, san, child in found.select("position_hash","position_epd","move_san","child_hash").iter_rows():
        b = chess.Board(epd + " 0 1")
        if zobrist_int64(b) != h or not b.is_valid():
            raise ValueError("Replay root mismatch")
        b.push_san(san)
        if zobrist_int64(b) != child:
            raise ValueError("Replay child mismatch")
    return found


def run(args):
    wanted = pl.read_parquet(args.targets)
    if wanted["position_hash"].n_unique() != wanted.height or set(wanted["colour"]) - {args.colour}:
        raise ValueError("Invalid request identities")
    meta = {"targets":sha(args.targets), "kind":args.kind, "colour":args.colour,
            "script":sha(__file__), "engine":sha(ec.SF), "sources":{}}
    if args.kind == "cloud":
        meta.update(db=sha(args.db/"_manifest.parquet"), build=sha(args.db/"_build.meta.json"))
        if not (args.db/"_DONE").is_file():
            raise ValueError("Incomplete cloud database")
    else:
        for src in args.replay:
            contract = json.loads(Path(str(src)+".parts/inputs.json").read_text())
            if (contract["local_nodes"],contract["threads"],contract["engine"],contract["colour"]) != (
                    10_000_000,6,meta["engine"],args.colour):
                raise ValueError(f"Local nomination contract differs: {src}")
            meta["sources"][str(src)] = sha(src)
    parts = Path(str(args.out)+".parts")
    parts.mkdir(parents=True, exist_ok=True)
    contract_file = parts/"inputs.json"
    if contract_file.exists() and json.loads(contract_file.read_text()) != meta:
        raise ValueError("Request or search inputs changed")
    write_json(meta, contract_file)
    if args.kind == "cloud":
        frames = []
        with duckdb.connect() as con:
            con.execute("SET threads=2")
            groups = wanted.with_columns((pl.col("position_hash") % 512).alias("bucket")).partition_by("bucket",as_dict=True)
            for (bucket,), group in sorted(groups.items()):
                path = parts/f"{bucket}.parquet"
                if not path.exists():
                    write_frame(cloud_bucket(con,group,args.db,bucket,args.colour,"cloud index"),path)
                frames.append(pl.read_parquet(path))
        result = pl.concat(frames) if frames else pl.DataFrame(schema=ec.SCHEMA)
    else:
        known = replay(wanted,args.replay)
        missing = wanted.join(known.rename({"position_epd":"epd"}).select(KEYS),on=KEYS,how="anti")
        misses = parts/"missing.parquet"
        if not misses.exists():
            write_frame(missing,misses)
        elif not pl.read_parquet(misses).equals(missing):
            raise ValueError("Missing search list changed")
        fresh = parts/"fresh.parquet"
        subprocess.run([sys.executable,ec.__file__,"--targets",str(misses),"--out",str(fresh),
                        "--colour",args.colour,"--nodes","10000000","--threads","6"],check=True)
        result = pl.concat([known,pl.read_parquet(fresh)])
        if result.height != wanted.height:
            raise ValueError("Local coverage mismatch")
        meta.update(replayed=known.height, fresh=missing.height, nominal_nodes=10_000_000*wanted.height)
    write_frame(result.sort("position_hash"),args.out)
    write_json({**meta,"requested":wanted.height,"returned":result.height,"output":sha(args.out)},
               Path(str(args.out)+".meta.json"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind",choices=["cloud","local"])
    for key in ["targets","out"]:
        parser.add_argument("--"+key,type=Path,required=True)
    parser.add_argument("--colour",choices=["black","white"],required=True)
    parser.add_argument("--db",type=Path,default=Path("D:/chess/eval_full"))
    parser.add_argument("--replay",type=Path,nargs="*",default=[])
    run(parser.parse_args())
