"""Freeze the November 6 nomination checkpoints after the old writer has exited."""
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import chess
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "outputs/rebuild-2026-11-06"
sys.path.insert(0, str(RUN / "code"))
from engine_candidates import SCHEMA, SF
from zobrist import zobrist_int64


def digest(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    black = RUN / "black"
    target = black / "unsearched-01.parquet"
    original = black / "proposals-01.parquet"
    parts = black / "hybrid-proposals-01.parquet.parts"
    old = parts / "local.parquet"
    miss = parts / "misses.parquet"
    study = black / "cutoff-study"
    sample = study / "sample.parquet"
    paired = study / "paired-nominations.json"
    output = black / "depth-preserved-local.parquet"
    inputs = [original, old, miss, parts / "inputs.json", sample, study / "design.json", paired,
              Path(str(original) + ".meta.json"), Path(str(old) + ".meta.json")]
    sources = {str(p): digest(p) for p in inputs}
    contract = {"input_sha256": digest(target), "nodes": 10_000_000, "colour":"black",
                "threads":6, "limit":None, "engine_sha256":digest(Path(SF))}
    assert json.loads(inputs[-2].read_text()) == contract
    assert json.loads(inputs[-1].read_text()) == {**contract, "input_sha256":digest(miss)}
    parent = json.loads((parts / "inputs.json").read_text())
    assert parent["targets"] == contract["input_sha256"] and parent["legacy"] == digest(original)
    assert parent["engine"] == contract["engine_sha256"] and parent["knodes"] == 10000
    design = json.loads((study / "design.json").read_text())
    assert design["engine_sha256"] == contract["engine_sha256"] and design["sample_sha256"] == digest(sample)
    rows = {r["position_hash"]:r for r in pl.read_parquet(target).iter_rows(named=True)}
    keep = {}
    counts = Counter()
    def add(record, source):
        h = record["position_hash"]
        r = rows[h]
        board = chess.Board(r["epd"] + " 0 1")
        assert r["epd"] == record["position_epd"] and board.turn == chess.BLACK and zobrist_int64(board) == h
        board.push_san(record["move_san"])
        assert zobrist_int64(board) == record["child_hash"] and record["nodes"] >= contract["nodes"]
        if h not in keep:
            keep[h] = record
            counts[source] += 1
    for path in [original, old]:
        records = pl.read_parquet(path)
        if path == old:
            assert set(records["position_hash"]) <= set(pl.read_parquet(miss)["position_hash"])
        for record in records.iter_rows(named=True):
            assert record["position_hash"] not in keep
            add(record, path.name)
    eligible = set(pl.read_parquet(sample).filter(pl.col("arm") == "below")["position_hash"])
    comparisons = json.loads(paired.read_text())
    assert len(comparisons) == len(eligible) == 64 and {r["position_hash"] for r in comparisons} == eligible
    for rec in comparisons:
        h = rec["position_hash"]
        r = rows[h]
        b = chess.Board(r["epd"] + " 0 1")
        b.push_san(rec["move"])
        add(dict(position_hash=h, position_epd=r["epd"], move_san=rec["move"], child_hash=zobrist_int64(b),
                 child_cp_white=rec["child_cp_white"], nodes=10_000_000, depth=None,
                 engine="Stockfish 18 (cutoff study)", searched="2026-11-07", reason=r["status"]), "cutoff-study")
    tmp = output.with_suffix(".tmp")
    pl.DataFrame(list(keep.values()), schema=SCHEMA).write_parquet(tmp)
    if output.exists() and digest(tmp) != digest(output):
        raise RuntimeError("Previous migration differs")
    tmp.replace(output)
    meta = {"kind":"migrated-local", "output_sha256":digest(output), "sources":sources,
            "retained":dict(counts), "search_contract":contract}
    path = Path(str(output) + ".meta.json")
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=2))
    tmp.replace(path)
    print(len(keep), dict(counts), flush=True)

if __name__ == "__main__":
    main()
