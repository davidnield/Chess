"""Bounded, documented raw-game extraction for milestone 2.

python -B -m research.human_repertoire.cohort
Reads only the configured row-group prefix, with projection/batch/frame caps.
Uses python-chess's PGN parser and the local Zobrist/readonly eval-array helpers.
Does not invoke any production CLI. Outputs are confined to _artifacts/m2/.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import heapq
import io
import json
from pathlib import Path
import re

import chess
import chess.pgn
import numpy as np
import pyarrow.parquet as pq

from python.zobrist import zobrist_int64
from python.eval_arrays import MISSING, lookup_evals, open_eval_arrays, verify_eval_arrays
from .io import ROOT, fingerprint, output_path, sha256, write_json

CONFIG = ROOT / "docs/human_repertoire/cohort_config.json"
COLUMNS = ["game_id", "white", "black", "white_elo", "black_elo", "utc_date",
           "time_control", "termination", "result", "movetext"]
RESULTS = {"1-0": (1, 0, 0), "1/2-1/2": (0, 1, 0), "0-1": (0, 0, 1)}


def san_prefix(text, n):
    # Fast selection only. The selected movetext is subsequently independently
    # parsed and legally replayed with python-chess. No PGN variation assumptions
    # are silently applied to accepted records.
    clean = re.sub(r"\{[^}]*\}|\$\d+", " ", text or "")
    clean = re.sub(r"\d+\.(?:\.\.)?", " ", clean)
    tokens = [t.rstrip("?!") for t in clean.split() if t not in RESULTS and t != "*"]
    return tokens[:n]


def select_reason(row, c):
    if not row.get("game_id") or not row.get("white") or not row.get("black"):
        return "missing_identity"
    a, b = row.get("white_elo"), row.get("black_elo")
    if a is None or b is None or min(a, b) < c["min_rating"]:
        return "rating"
    if abs(a-b) > c["max_rating_gap"]:
        return "rating_gap"
    if row.get("result") not in RESULTS:
        return "unfinished_result"
    if san_prefix(row.get("movetext"), len(c["root_san"])) != c["root_san"]:
        return "root_prefix"
    return None


def rank_id(game_id, seed):
    return int.from_bytes(hashlib.sha256(f"{seed}:{game_id}".encode()).digest(), "big")


def replay(row, c):
    # Read only prefix+H+one further move, so malformed deep tails cannot censor
    # a valid opening. PGN library parsing handles numbering/SAN annotations.
    tokens = san_prefix(row["movetext"], len(c["root_san"]) + c["horizon"] + 1)
    text = " ".join(tokens[:len(c["root_san"]) + c["horizon"]])
    parsed = chess.pgn.read_game(io.StringIO(text))
    if parsed is None or parsed.errors:
        raise ValueError("invalid opening movetext")
    board = parsed.board()
    moves = list(parsed.mainline_moves())
    prefix = []
    for move in moves[:len(c["root_san"])]:
        prefix.append(board.san(move))
        board.push(move)
    if prefix != c["root_san"]:
        raise ValueError("legal root differs from selected prefix")
    positions = [{"key": str(zobrist_int64(board)), "fen": board.fen()}]
    sans = []
    for move in moves[len(prefix):len(prefix)+c["horizon"]]:
        sans.append(board.san(move))
        board.push(move)
        positions.append({"key": str(zobrist_int64(board)), "fen": board.fen()})
    reached_horizon = len(sans) == c["horizon"]
    # Reaching H while the real game also ends there counts as an observed end.
    actual_end = len(tokens) <= len(prefix) + c["horizon"]
    result = board.outcome(claim_draw=False)
    if actual_end and result and RESULTS[row["result"]] != ((0, 1, 0) if result.winner is None else (1, 0, 0) if result.winner else (0, 0, 1)):
        raise ValueError("board terminal disagrees with game result")
    return {**{k: (str(v) if k == "utc_date" else v) for k, v in row.items() if k != "movetext"},
            "moves": sans, "positions": positions, "wdl": RESULTS[row["result"]],
            "observed_end": actual_end, "reached_horizon": reached_horizon,
            "rule_terminal": bool(result), "opening_movetext": text}


def projected_limit(pf, c):
    rg = pf.metadata.row_group(c["row_group"])
    names = pf.schema_arrow.names
    size = sum(rg.column(names.index(name)).total_uncompressed_size for name in COLUMNS)
    if rg.num_rows > c["max_row_group_rows"] or size > c["max_projected_row_group_bytes"]:
        raise ValueError("projected row group exceeds declared resource cap")
    if c["frame_rows"] > rg.num_rows or c["frame_rows"] <= 0 or c["batch_rows"] <= 0:
        raise ValueError("invalid bounded frame")
    return {"rows": rg.num_rows, "projected_uncompressed_bytes": size,
            "projected_compressed_bytes": sum(rg.column(names.index(name)).total_compressed_size for name in COLUMNS)}


def extract_one(path, c):
    pf = pq.ParquetFile(path)
    limits = projected_limit(pf, c)
    counts = Counter()
    heap = []
    seen_ids = set()
    remaining = c["frame_rows"]
    for batch in pf.iter_batches(batch_size=c["batch_rows"], row_groups=[c["row_group"]], columns=COLUMNS, use_threads=False):
        rows = batch.slice(0, min(remaining, len(batch))).to_pylist()
        for row in rows:
            counts["scanned"] += 1
            reason = select_reason(row, c)
            if reason:
                counts[reason] += 1
                continue
            if row["game_id"] in seen_ids:
                counts["duplicate_id"] += 1
                continue
            seen_ids.add(row["game_id"])
            counts["eligible"] += 1
            rank = rank_id(row["game_id"], c["rank_seed"])
            item = (-rank, row["game_id"], row)
            if len(heap) < c["max_games_per_cohort"]:
                heapq.heappush(heap, item)
            elif item > heap[0]:
                heapq.heapreplace(heap, item)
        remaining -= len(rows)
        if remaining == 0:
            break
    if remaining:
        raise ValueError("source yielded fewer rows than declared frame")
    games = []
    for _, _, row in sorted(heap, reverse=True):
        try:
            games.append(replay(row, c))
        except (ValueError, AssertionError, IndexError):
            counts["invalid_opening"] += 1
    counts["selected_before_replay"] = len(heap)
    counts["accepted"] = len(games)
    counts["rank_cap_excluded"] = counts["eligible"] - len(heap)
    audit = {"counts": dict(counts), "limits": limits, "schema": str(pf.schema_arrow),
             "dates": dict(Counter(g["utc_date"] for g in games)),
             "time_controls": dict(Counter(g["time_control"] for g in games)),
             "terminations": dict(Counter(g["termination"] for g in games)),
             "rating_range": [min(min(g["white_elo"], g["black_elo"]) for g in games), max(max(g["white_elo"], g["black_elo"]) for g in games)] if games else None,
             "observed_ends": sum(g["observed_end"] for g in games)}
    return games, audit


def extract(c):
    sources = [p for group in c["sources"].values() for p in group.values()] + [str(Path(c["eval_arrays"]) / p) for p in ("eval_hash.npy", "eval_cp.npy", "eval_arrays.meta.json")] + [c["eval_db"]]
    before = [fingerprint(p, full=Path(p).stat().st_size < 1_000_000) for p in sources]
    identity = {"config": c, "sources": before,
                "extractor_sha256": sha256(__file__), "hash_adapter_sha256": sha256(ROOT / "python/zobrist.py"),
                "eval_adapter_sha256": sha256(ROOT / "python/eval_arrays.py")}
    manifest_path = output_path("m2/cohort_manifest.json")
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text())
        if identity != previous["identity"]:
            raise ValueError("cohort source/config/code drift; preserve the frozen version")
        for p, digest in previous["snapshots"].items():
            if sha256(output_path(p)) != digest:
                raise ValueError("snapshot drift")
        print("Verified frozen milestone-2 cohorts")
        return
    cohorts, audits = {}, {}
    for name, paths in c["sources"].items():
        candidates, audits[name] = [], {}
        for event, path in paths.items():
            games, audit = extract_one(path, c)
            for game in games:
                game["event"] = event
            candidates.extend(games)
            audits[name][event] = audit
            print(name, event, json.dumps(audit["counts"]), flush=True)
        ids = [g["game_id"] for g in candidates]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate game across event frames")
        cohorts[name] = sorted(candidates, key=lambda g: rank_id(g["game_id"], c["rank_seed"]))[:c["max_games_per_cohort"]]
        audits[name]["pooled_selected"] = dict(Counter(g["event"] for g in cohorts[name]))
    if {g["game_id"] for g in cohorts["train"]} & {g["game_id"] for g in cohorts["validation"]}:
        raise ValueError("game IDs overlap across cohorts")
    positions = {p["key"]: p["fen"] for games in cohorts.values() for g in games for p in g["positions"]}
    if len(positions) > c["max_graph_nodes"]:
        raise ValueError("position cap exceeded")
    status = verify_eval_arrays(Path(c["eval_arrays"]), Path(c["eval_db"]), adopt=False)
    hh, cc = open_eval_arrays(Path(c["eval_arrays"]))
    keys = sorted(positions)
    values = lookup_evals(np.array(keys, dtype=np.int64), hh, cc)
    evals = {key: None if value == MISSING else int(value) for key, value in zip(keys, values)}
    after = [fingerprint(p, full=Path(p).stat().st_size < 1_000_000) for p in sources]
    if before != after:
        raise ValueError("source changed during extraction")
    paths = {f"m2/{name}_games.json": games for name, games in cohorts.items()}
    paths["m2/evals.json"] = evals
    snapshots = {p: sha256(write_json(p, data)) for p, data in paths.items()}
    players = {name: {g[color].casefold() for g in games for color in ("white", "black")} for name, games in cohorts.items()}
    manifest = {"identity": identity, "created_utc": datetime.now(timezone.utc).isoformat(),
                "snapshots": snapshots, "audits": audits, "unique_positions": len(evals),
                "missing_evals": sum(v is None for v in evals.values()), "eval_verification": status,
                "shared_players": len(players["train"] & players["validation"]),
                "players_per_cohort": {k: len(v) for k, v in players.items()}}
    write_json("m2/cohort_manifest.json", manifest)
    print(json.dumps({k: manifest[k] for k in ("unique_positions", "missing_evals", "shared_players")}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    args = parser.parse_args()
    extract(json.loads(args.config.read_text()))


if __name__ == "__main__":
    main()
