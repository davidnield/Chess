"""Paired White first-pass replication of the Black 10M-node cutoff study."""
import hashlib
import json
from pathlib import Path

import chess
import chess.engine
import polars as pl

from cloud_feedback import search
from engine_candidates import SF
from hybrid_candidates import sha, write_frame, write_json
from validate_book import fresh_verdict
from zobrist import zobrist_int64


def run(cloud, out, colour, limit, size=64):
    out.mkdir(parents=True, exist_ok=True)
    seed = f"first-pass-cutoff-{colour}-20261107"
    pool = {"below":[], "above":[]}
    for r in cloud.iter_rows(named=True):
        arm = "below" if r["nodes"] < 10_000_000 else "above"
        key = hashlib.sha256(f"{seed}|{r['position_hash']}".encode()).hexdigest()
        pool[arm].append((key, r))
    drawn = [(r, arm) for arm in pool for _, r in sorted(pool[arm], key=lambda x:x[0])[:size]]
    sample = pl.DataFrame([{**r, "arm":arm} for r,arm in drawn]) if drawn else cloud.with_columns(pl.lit(None).alias("arm"))
    path = out / "sample.parquet"
    if path.exists() and not pl.read_parquet(path).equals(sample):
        raise ValueError("White cutoff sample changed")
    if not path.exists():
        write_frame(sample, path)
    config = {"seed":seed, "sample":sha(path), "engine":sha(Path(SF)), "script":sha(__file__),
              "population":{arm:len(rows) for arm,rows in pool.items()}, "limit_cp":limit,
              "size_per_arm":size, "reference_nodes":15_000_000, "replacement_nodes":10_000_000}
    meta = out / "design.json"
    if meta.exists() and json.loads(meta.read_text()) != config:
        raise ValueError("Changed cutoff study settings")
    write_json(config, meta)
    path = out / "results.json"
    done = json.loads(path.read_text()) if path.exists() else []
    seen = {r["position_hash"] for r in done}
    if len(seen) != len(done) or not seen <= {r["position_hash"] for r,_ in drawn}:
        raise ValueError("Invalid cutoff study checkpoint")
    eng = None
    try:
        for r, arm in drawn:
            h = r["position_hash"]
            if h in seen:
                continue
            if eng is None:
                eng = chess.engine.SimpleEngine.popen_uci(SF)
                eng.configure({"Threads":2,"Hash":1024})
            board = chess.Board(r["position_epd"] + " 0 1")
            if zobrist_int64(board) != h or board.turn != (colour == "white"):
                raise ValueError(f"Study target mismatch at {h}")
            cloud_move = r["move_san"]
            v = fresh_verdict(eng, board, cloud_move, 15_000_000, limit, board.turn)
            row = {"position_hash":h, "arm":arm, "cloud_move":cloud_move, **v}
            if arm == "below":
                eng.configure({"Threads":6,"Hash":2048})
                local = search(eng, board, 10_000_000)
                eng.configure({"Threads":2,"Hash":1024})
                row["replacement"] = local
                if local["san"] == cloud_move:
                    loss = v["sf_loss"]
                elif local["san"] == v["sf_best"]:
                    loss = 0
                else:
                    sign = 1 if board.turn else -1
                    best = v.get("sf_best_restricted_cp")
                    if best is None:
                        best = sign * search(eng, board, 15_000_000, v["sf_best"])["cp"]
                    loss = best - sign * search(eng, board, 15_000_000, local["san"])["cp"]
                row["replacement_loss"] = loss
            done.append(row)
            write_json(done, path)
            if len(done) % 8 == 0:
                print(f"White cutoff check {len(done)}/{len(drawn)}", flush=True)
    finally:
        if eng:
            eng.quit()
    summary = {}
    for arm in pool:
        group = [r for r in done if r["arm"] == arm]
        summary[arm] = {"n":len(group), "same_move":sum(r["fresh"] == "same_move" for r in group),
                        "over_limit":sum(r["sf_loss"] > limit for r in group),
                        "over_30":sum(r["sf_loss"] > 30 for r in group)}
        if arm == "below":
            summary[arm].update(prevented=sum(r["sf_loss"] > limit >= r["replacement_loss"] for r in group),
                                introduced=sum(r["replacement_loss"] > limit >= r["sf_loss"] for r in group))
    write_json(summary, out / "summary.json")
    print(json.dumps(summary), flush=True)
