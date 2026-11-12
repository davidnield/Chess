"""Recover individual reply outcomes and missing parent flows for the paired Black builds."""
import argparse
from collections import Counter
import json
import sys
from pathlib import Path

import chess
import duckdb
import numpy as np
import polars as pl
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pool_from_book import SUMS, slice_files
from run_practical_rebuild import digest, publish
from zobrist import zobrist_int64


def write(frame, path):
    tmp = path.with_name(path.name + ".tmp")
    frame.write_parquet(tmp)
    tmp.replace(path)


def read_json(path):
    return json.loads(path.read_text(encoding="utf8"))


def recover(broad, dest):
    pilot = broad / "opponent-prior-pilot"
    inv = pilot / "positions.parquet"
    policy = pilot / "policy.parquet"
    previous = read_json(pilot / "challenger-replies.parquet.json")
    pool, aux = Path(previous["source_pool"]), Path(previous["source_aux"])
    source = read_json(Path(str(pool) + ".meta.json"))
    archive = Path(source["book"])
    manifest = dest / "inputs.json"
    buckets = dest / "buckets"
    buckets.mkdir(parents=True, exist_ok=True)
    identity = {"inventory": digest(inv), "policy": digest(policy), "pool_meta": digest(Path(str(pool) + ".meta.json")),
                "archive": digest(archive / "_book.meta.json"), "code": digest(__file__)}
    policy_cache = read_json(pilot / "policy-cache.json")
    if (identity["archive"] != source["book_meta_sha256"] or identity["policy"] != policy_cache["sha256"]
            or identity["inventory"] != policy_cache["identity"]["inventory"] or source["max_ply"] != 30
            or source["population"]["book_filters"]["max_ply"] != 30 or source["min_games"] != 20):
        raise ValueError("The archived pool, policy and inventory must describe the frozen pilot")
    if manifest.exists() and read_json(manifest) != identity:
        raise ValueError("Refusing to combine shards from different inputs")
    publish(manifest, identity)
    db = duckdb.connect(config={"threads": 2, "memory_limit": "2GB"})
    inventory = pl.read_parquet(inv).filter(pl.col("population"))
    db.register("positions", inventory.to_arrow())
    cols = ", ".join(SUMS)
    retained = db.execute(f"""SELECT parent_hash, child_hash, {cols} FROM read_parquet(?)
        SEMI JOIN positions ON parent_hash=position_hash""", [str(pool)]).pl()
    original_aux = db.execute("""SELECT a.* FROM read_parquet(?) a SEMI JOIN positions USING(position_hash)""",
                              [str(aux)]).pl()
    count_files = sorted(p for folder in ("counts", "counts-extra") for p in (pilot/folder).glob("b*.parquet"))
    if len(count_files) != 1024:
        raise ValueError("Incomplete cached training counts")
    history = db.execute("""SELECT h,san,n FROM read_parquet(?) SEMI JOIN positions ON h=position_hash""",
                         [[str(p) for p in count_files]]).pl()
    supported = set(history["h"])
    missing = supported - set(original_aux["position_hash"])
    # Independent arrival checks for a few existing pool parents help detect a wrong
    # rating slice or off-by-one in the reconstructed nonmove outcomes.
    root = zobrist_int64(chess.Board())
    controls = sorted((supported - missing) - {root})[::1000]
    incoming = sorted(missing.union(controls))
    db.register("incoming", pl.DataFrame({"h": incoming}, schema={"h":pl.Int64}).to_arrow())
    collisions = set(pl.read_parquet(archive / "_collisions.parquet", columns=["parent_hash"])["parent_hash"])
    if supported.intersection(collisions):
        raise ValueError("Historical support contains a collision hash")
    target_epds = dict(inventory.select("position_hash", "epd").iter_rows())
    groups = {i: [] for i in range(512)}
    for h in sorted(supported):
        groups[h % 512].append(h)
    events = source["params"]["events"]
    bands = source["params"]["elo_bands"]
    print(f"{len(supported)} parents with historical replies, {len(missing)} absent from floor 20", flush=True)
    sumcols = ", ".join(f"sum({c})::BIGINT AS {c}" for c in SUMS)
    for i, hashes in groups.items():
        moves_file, arrivals_file = (buckets / f"b{i:03}.{kind}.parquet" for kind in ("moves","arrivals"))
        files = slice_files(archive, events, bands, i)
        if len(files) != len(events)*len(bands):
            raise ValueError(f"Missing training slice in bucket {i}")
        if not moves_file.exists():
            # Use the sorted parent column to skip row groups before decoding their payload.
            # A position often has many rows for the same move across plies and slices.
            result = Counter()
            targets = np.asarray(hashes, dtype="int64")
            for path in files:
                f = pq.ParquetFile(path)
                hi = f.schema_arrow.names.index("parent_hash")
                for j in range(f.num_row_groups):
                    limits = f.metadata.row_group(j).column(hi).statistics
                    subset = targets[targets.searchsorted(limits.min):targets.searchsorted(limits.max, "right")]
                    if not len(subset):
                        continue
                    parents = f.read_row_group(j, columns=["parent_hash"]).column(0).to_numpy()
                    low, high = parents.searchsorted(subset), parents.searchsorted(subset, "right")
                    if not (high > low).any():
                        continue
                    names = ["parent_epd","move_san","child_hash","ply",*SUMS]
                    payload = f.read_row_group(j, columns=names)
                    for h,l,r in zip(subset,low,high):
                        if r == l:
                            continue
                        for epd, san, ch, ply, w,d,b,n in zip(*(c.to_pylist() for c in payload.slice(l,r-l).columns)):
                            if epd != target_epds[h] or ply > 30 or w+d+b != n:
                                raise ValueError(f"Unexpected source row at {h}")
                            key = (int(h), ch, san.rstrip("+#"))
                            result[key] += np.array([w,d,b,n], dtype="int64")
            schema = {"h":pl.Int64,"ch":pl.Int64,"san":pl.String, **dict.fromkeys(SUMS,pl.Int64)}
            write(pl.DataFrame([(*k,*v) for k,v in sorted(result.items())], schema=schema, orient="row"),moves_file)
        if not arrivals_file.exists():
            # Parent buckets partition the whole source. A target can be reached
            # from any of them. Read only incoming child hashes and outcome counts.
            arrivals = db.execute(f"""SELECT child_hash AS h, {sumcols} FROM read_parquet(?, hive_partitioning=false)
                SEMI JOIN incoming ON child_hash=incoming.h WHERE ply BETWEEN 1 AND 30
                GROUP BY child_hash ORDER BY child_hash""", [files]).pl()
            write(arrivals, arrivals_file)
        if i % 16 == 0 or i == 511:
            print(f"{i+1}/512 archived buckets", flush=True)

    moves = pl.read_parquet(buckets/"*.moves.parquet")
    arrivals = pl.read_parquet(buckets/"*.arrivals.parquet").group_by("h").agg(pl.col(*SUMS).sum())
    expected = {(h,s):n for h,s,n in history.iter_rows()}
    actual = {(h,s):n for h,s,n in moves.group_by("h","san").agg(pl.col("total").sum()).iter_rows()}
    if actual != expected:
        raise ValueError("Per-move counts differ from the independent Brier reconstruction")
    outcomes = moves.group_by("h","ch").agg(pl.col(*SUMS).sum())
    named = {(h,ch):np.array(v) for h,ch,*v in outcomes.select("h","ch",*SUMS).iter_rows()}
    rare = {h:np.zeros(4, dtype="int64") for h in supported}
    retained_counts = {(h,ch):np.array(v) for h,ch,*v in retained.group_by("parent_hash","child_hash")
                       .agg(pl.col(*SUMS).sum()).select("parent_hash","child_hash",*SUMS).iter_rows()}
    if retained_counts.keys() - named.keys():
        raise ValueError("A retained child is absent from the recovered history")
    for (h,ch),wdl in named.items():
        remain = wdl - retained_counts.get((h,ch),0)
        if (remain < 0).any():
            raise ValueError("Retained outcomes exceed archive outcomes")
        rare[h] += remain
    aux_rows = {r["position_hash"]:r for r in original_aux.to_dicts()}
    for h,row in aux_rows.items():
        if not np.array_equal(rare[h], [row["other_"+c] for c in SUMS]):
            raise ValueError(f"Recovered rare move outcomes differ from OTHER at {h}")
    arrivals = {h:np.array(v) for h,*v in arrivals.select("h",*SUMS).iter_rows()}
    departures = {h:np.array(v) for h,*v in outcomes.group_by("h").agg(pl.col(*SUMS).sum()).select("h",*SUMS).iter_rows()}
    extra = []
    for h in incoming:
        # The production p30 source stops at ply 30. It stores arrivals at
        # that cap with no further observed move in term_other (horizon_total=0).
        ended = arrivals[h] - departures[h]
        if (ended < 0).any() or ended[:3].sum() != ended[3]:
            raise ValueError(f"Invalid historical flow at {h}")
        if h in missing:
            row = {"position_hash":h, **{"term_other_"+c:int(v) for c,v in zip(SUMS,ended)}}
            extra.append(row)
            aux_rows[h] = row
        elif not np.array_equal(ended,[aux_rows[h]["term_other_"+c] for c in SUMS]):
            raise ValueError(f"Incoming-flow control disagrees with original aux at {h}")
    dest_file = dest/"replies.parquet"
    extra_file = Path(str(dest_file)+".aux.parquet")
    write(pl.DataFrame(extra,schema={"position_hash":pl.Int64,**{"term_other_"+c:pl.Int64 for c in SUMS}}),
          extra_file)
    probs = {(h,s):p for h,s,p in pl.read_parquet(policy).select("h","san","p").iter_rows()}
    labels = {(h,s):ch for h,s,ch in moves.select("h","san","ch").unique().iter_rows()}
    for (h,s),ch in labels.items():
        board = chess.Board(target_epds[h]+" 0 1")
        board.push_san(s)
        if zobrist_int64(board) != ch:
            raise ValueError(f"Incorrect successor for {h} {s}")
    rows = []
    for h in sorted(supported):
        epd = target_epds[h]
        board = chess.Board(epd+" 0 1")
        legal = [(board.san(m),m) for m in board.legal_moves]
        norm = sum(probs[h,s.rstrip("+#")] for s,_ in legal)
        if not np.isclose(norm,1) or not board.turn:
            raise ValueError(f"Invalid opponent policy at {h}")
        nonmove = sum(aux_rows[h].get(g+"_total",0) for g in ("term_other","term_normal","term_flag","horizon"))
        for san,m in legal:
            board.push(m)
            ch = zobrist_int64(board)
            board.pop()
            w,d,b,n = named.get((h,ch),(0,0,0,0))
            rows.append((h,epd,san,ch,n,w,d,b,(w+.5*d)/n if n else .5,n,probs[h,san.rstrip("+#")]/norm,nonmove))
    columns = ["parent_hash","parent_epd","move_san","child_hash","total","white_wins","draws","black_wins",
               "white_score_avg","reply_observed","maia_p","nonmove"]
    write(pl.DataFrame(rows,schema=columns,orient="row"),dest_file)
    meta = {**previous,"schema":2,"sha256":digest(dest_file),"extra_aux_sha256":digest(extra_file),
            "scope":"Both arms share the canonical/provisional Black decision inventory with at least one observed opponent reply.",
            "positions":len(supported),"inventory_positions":inventory.height,"all_subfloor_parents":len(missing),
            "zero_history_excluded":inventory.height-len(supported),"arrival_control_positions":len(controls),
            "rare_named_edges":sum(v[3]>0 and (key not in retained_counts) for key,v in named.items()),
            "source_inputs":identity}
    meta["rare_named_edges"] = int(meta["rare_named_edges"])
    publish(Path(str(dest_file)+".json"),meta)
    print(json.dumps(meta),flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--broad",type=Path,required=True)
    ap.add_argument("--dest",type=Path,required=True)
    args = ap.parse_args()
    from run_opponent_prior_challenger import lock
    args.dest.mkdir(parents=True,exist_ok=True)
    with lock(args.dest/"extract.lock"):
        recover(args.broad.resolve(),args.dest.resolve())
