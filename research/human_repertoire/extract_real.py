"""Create a bounded real opening snapshot. No full dataset scans or engine jobs.

Run from this checkout:
  research/human_repertoire/_artifacts/venv/Scripts/python.exe -m research.human_repertoire.extract_real

Reads only size/row-capped small stats/reference files, their footers, and sparse
read-only mmap engine lookups. Refuses to replace a snapshot if source identities
or the extraction config changed. Outputs always stay under _artifacts/real/.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path

import chess
import numpy as np
import pyarrow.parquet as pq

from python.zobrist import zobrist_int64
from python.eval_arrays import MISSING, lookup_evals, open_eval_arrays, verify_eval_arrays
from .io import DEFAULT_CONFIG, ROOT, fingerprint, output_path, sha256, write_json
from .model import Edge, Graph, Node, graph_dict


def transition(epd, san, expected_child=None):
    board = chess.Board(epd)
    board.push_san(san)
    child = zobrist_int64(board)
    if expected_child is not None and child != expected_child:
        raise ValueError("recorded child hash differs from legal chess replay")
    return board, str(child)


def bounded_rows(path, cfg):
    if Path(path).stat().st_size > cfg["max_source_bytes"]:
        raise ValueError("source exceeds byte cap; a full scan is not authorised by this extractor")
    pf = pq.ParquetFile(path)
    if pf.metadata.num_rows > cfg["max_source_rows"]:
        raise ValueError("source exceeds row cap")
    return pf.read(use_threads=False).to_pylist(), str(pf.schema_arrow)


def extract(cfg):
    if cfg["depth"] != 3:
        raise ValueError("this bounded adapter supports exactly three plies")
    source_paths = [cfg[k] for k in ("stats", "aux", "reference")]
    source_paths += [str(Path(cfg["eval_arrays"]) / f) for f in ("eval_hash.npy", "eval_cp.npy", "eval_arrays.meta.json")]
    source_paths += [cfg["eval_db"]]
    before = [fingerprint(p, full=Path(p).stat().st_size <= cfg["max_source_bytes"]) for p in source_paths]
    identity = {"config": cfg, "sources": before,
                "adapter_sha256": {str(p.relative_to(ROOT)): sha256(p) for p in
                                   [Path(__file__), ROOT / "python/zobrist.py", ROOT / "python/eval_arrays.py"]}}
    manifest_path = output_path("real/provenance.json")
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text())
        if previous["identity"] != identity:
            raise ValueError("source/config drift: existing snapshot is frozen; use a separately versioned experiment")
        for relative, digest in previous["snapshot_sha256"].items():
            if sha256(output_path(relative)) != digest:
                raise ValueError("snapshot fingerprint mismatch")
        print("Verified frozen real snapshot")
        return previous
    stats, stats_schema = bounded_rows(cfg["stats"], cfg)
    aux, aux_schema = bounded_rows(cfg["aux"], cfg)
    reference_rows, reference_schema = bounded_rows(cfg["reference"], cfg)
    if {(r["event"], r["elo_band"]) for r in stats} != {("Pooled", 0)}:
        raise ValueError("expected exactly one pooled slice")
    by_parent = defaultdict(list)
    for row in stats:
        if row["total"] <= 0 or sum(row[k] for k in ("white_wins", "draws", "black_wins")) != row["total"]:
            raise ValueError("bad outcome counts")
        by_parent[str(row["parent_hash"])].append(row)
    for rows in by_parent.values():
        if len({r["move_san"] for r in rows}) != len(rows):
            raise ValueError("duplicate position/move")
        rows.sort(key=lambda r: (-r["total"], r["move_san"]))
    reference = {str(r["position_hash"]): r["best_move"] for r in reference_rows if r.get("best_move") and r["side_to_move"] == "white"}
    root_board = chess.Board()
    for san in cfg["root_san"]:
        root_board.push_san(san)
    if root_board.turn != chess.WHITE:
        raise ValueError("this bounded example requires a White-to-move root")
    root = str(zobrist_int64(root_board))
    if root not in reference:
        raise ValueError("reference must contain the experiment root")
    boards = {root: root_board}
    depth = {root: 0}
    expanded = {}
    allowed = {}
    queue = [root]
    while queue:
        key = queue.pop(0)
        rows = by_parent.get(key, [])
        if not rows or depth[key] == cfg["depth"]:
            continue
        board = boards[key]
        expanded[key] = rows
        if board.turn == chess.WHITE:
            candidates = [r["move_san"] for r in rows[:cfg["candidate_moves"]]]
            if reference.get(key) in {r["move_san"] for r in rows} and reference[key] not in candidates:
                candidates.append(reference[key])
            allowed[key] = candidates
            recurse = set(candidates)
        else:
            recurse = {r["move_san"] for r in rows[:cfg["expanded_replies"]]}
        for r in rows:
            if zobrist_int64(chess.Board(r["parent_epd"])) != int(key):
                raise ValueError("parent hash mismatch")
            child_board, child = transition(board.fen(), r["move_san"], r["child_hash"])
            if child in boards and boards[child].epd() != child_board.epd():
                raise ValueError("hash collision / incompatible state")
            boards[child] = child_board
            depth[child] = min(depth.get(child, 100), depth[key] + 1)
            if depth[child] < cfg["depth"] and r["move_san"] in recurse and child not in expanded and child not in queue:
                queue.append(child)
            if len(boards) > cfg["max_nodes"]:
                raise ValueError("node cap exceeded")
    verification = verify_eval_arrays(Path(cfg["eval_arrays"]), Path(cfg["eval_db"]), adopt=False)
    hashes, cp = open_eval_arrays(Path(cfg["eval_arrays"]))
    keys = sorted(boards)
    vals = lookup_evals(np.array([int(k) for k in keys], dtype=np.int64), hashes, cp)
    evals = {k: None if v == MISSING else int(v) for k, v in zip(keys, vals)}
    # Node outcome priors pool observed arrivals from the bounded edge extract,
    # not production propagated values. Expanded nodes use outgoing counts;
    # frontier nodes use incoming counts if no outgoing stats were extracted.
    incoming = defaultdict(lambda: [0, 0, 0])
    for rows in expanded.values():
        for r in rows:
            for i, name in enumerate(("white_wins", "draws", "black_wins")):
                incoming[str(r["child_hash"])][i] += r[name]
    nodes = {}
    for key, board in boards.items():
        rows = expanded.get(key, [])
        counts = [sum(r[name] for r in rows) for name in ("white_wins", "draws", "black_wins")] if rows else incoming[key]
        strength = cfg["wdl_prior_strength"]
        wdl = tuple((n + strength * prior) / (sum(counts) + strength) for n, prior in zip(counts, cfg["wdl_prior"]))
        outcome = board.outcome(claim_draw=False)
        if outcome:
            wdl = (0, 1, 0) if outcome.winner is None else ((1, 0, 0) if outcome.winner else (0, 0, 1))
            nodes[key] = Node("terminal", cp=evals[key], wdl=wdl)
        elif rows:
            total = sum(r["total"] for r in rows)
            nodes[key] = Node("us" if board.turn else "opp", tuple(Edge(r["move_san"], str(r["child_hash"]), r["total"]/total, evals[str(r["child_hash"])]) for r in rows), evals[key], wdl)
        else:
            nodes[key] = Node("frontier", cp=evals[key], wdl=wdl)
    graph = Graph(root, nodes)
    graph.validate()
    projected = {k: m for k, m in reference.items() if k in expanded and m in allowed.get(k, [])}
    aux_rows = [r for r in aux if str(r["position_hash"]) in expanded]
    after = [fingerprint(p, full=Path(p).stat().st_size <= cfg["max_source_bytes"]) for p in source_paths]
    if before != after:
        raise ValueError("source changed during extraction")
    data = {"graph": graph_dict(graph), "allowed": allowed, "reference": projected,
            "positions": {k: {"fen": b.fen(), "depth": depth[k]} for k, b in boards.items()},
            "root_san": cfg["root_san"]}
    files = {
        "real/graph.json": data,
        "real/stats_rows.json": [r for rows in expanded.values() for r in rows],
        "real/aux_rows.json": aux_rows,
        "real/reference_rows.json": reference_rows,
        "real/eval_rows.json": evals,
    }
    digests = {relative: sha256(write_json(relative, content)) for relative, content in files.items()}
    manifest = {
        "schema_version": 1, "created_utc": datetime.now(timezone.utc).isoformat(),
        "identity": identity, "snapshot_sha256": digests,
        "source_schemas": {"stats": stats_schema, "aux": aux_schema, "reference": reference_schema},
        "eval_verification": verification,
        "counts": {"source_stats_rows": len(stats), "extracted_stats_rows": sum(map(len, expanded.values())), "nodes": len(nodes), "expanded_positions": len(expanded), "reference_cards": len(projected), "unknown_evals": sum(v is None for v in evals.values())},
        "limitations": [
            "Stats filename indicates January 2018 test slice; sampling recipe and rating threshold are not independently documented in its metadata.",
            "Condition on recorded surviving move continuations at every expanded position. Aux term/flag/other/horizon counts are retained for audit but excluded from the model population.",
            "Unexpanded branches retain all recorded move mass and end at a frontier with historical W/D/L; later first hits are unobserved.",
            "Reference is an existing 20-card truncated sharp book projection; build recipe/date sidecar absent. Its value fields are unused.",
            "Engine array provenance is verified, but individual engine versions/depths and dates are absent; no contemporaneous engine reanalysis.",
            "No training/validation/test split or human calibration; the old slice and newer reference/evals are deliberately not a holdout comparison.",
            "Large eval inputs fingerprinted by size/mtime/footer row count and small metadata hash, not full multi-GB content hashes. Frozen extracted rows have full hashes."
        ]}
    write_json("real/provenance.json", manifest)
    print(json.dumps(manifest["counts"], indent=2))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    extract(json.loads(args.config.read_text())["real"])


if __name__ == "__main__":
    main()
