"""M5 pilot, capped engine work, and independent final/control trajectories.

python -B -m research.human_repertoire.rollout_experiment --pilot
python -B -m research.human_repertoire.rollout_experiment --engine
python -B -m research.human_repertoire.rollout_experiment --final
Writes only _artifacts/m5. No game extraction, fitting, or book optimisation.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import chess
import chess.engine
import numpy as np
from python.eval_arrays import MISSING, lookup_evals, open_eval_arrays, verify_eval_arrays
from python.zobrist import zobrist_int64

from .common_graph import apply_evals
from .continuation_audit import (CONFIG as M4_CONFIG, analyse_position, frozen_inputs, verified_preparation)
from .empirical import stats
from .io import ROOT, fingerprint, output_path, sha256, write_json
from .model import Config, Edge, Graph, Node, eligible, graph_from_dict
from .trajectory import (ChessBackend, GraphBackend, arm_summary, paired_summary,
                         sample_trace, score_trace)

CONFIG = ROOT / "docs/human_repertoire/milestone5_config.json"


def identity(c):
    return {"config": c, "code_sha256": {n: sha256(ROOT/"research/human_repertoire"/n) for n in
        ("trajectory.py", "rollout_experiment.py", "continuation_audit.py", "common_graph.py", "model.py", "shared_fallback.py", "empirical.py")},
        "plan_sha256": sha256(ROOT/"docs/human_repertoire/milestone5_plan.md"),
        "m4_preparation_sha256": sha256(output_path("m4/preparation.json")),
        "m4_engine_sha256": sha256(output_path("m4/engine_audit.json")),
        "m4_results_sha256": sha256(output_path("m4/results.json")),
        "python_chess_version": chess.__version__}


def base_inputs(c):
    c4 = json.loads(M4_CONFIG.read_text())
    if c["config"] != c4["config"] or len({c[k+"_seed"] for k in ("pilot", "final", "control")}) != 3:
        raise ValueError("changed evaluator assumptions or overlapping random streams")
    prep = verified_preparation(c4)
    old_engine = json.loads(output_path("m4/engine_audit.json").read_text())
    if not old_engine["complete"] or old_engine["identity"]["preparation_sha256"] != sha256(output_path("m4/preparation.json")):
        raise ValueError("incomplete prior engine evidence")
    if fingerprint(c4["engine_path"], full=True) != prep["engine_identity"]["binary"]:
        raise ValueError("engine changed; cannot silently reuse homogeneous audited scores")
    games, fitted, specs = frozen_inputs(c4)
    train = stats(games)
    old_scores = {k: v["cp"] for k, v in {**old_engine["base"], **old_engine["deeper"]}.items()}
    cfg = Config(**c["config"])
    gates = []
    for key, move in sorted({(k, m) for b in prep["books"].values() for k, m in b.items()}):
        board = chess.Board(train["fens"][key])
        board.push_san(move)
        child = str(zobrist_int64(board))
        graph = Graph(key, {key: Node("us", (Edge(move, child, 1, old_scores.get(child)),), old_scores.get(key)), child: Node("frontier", cp=old_scores.get(child))})
        if move not in eligible(graph, cfg, key):
            raise ValueError("fixed prescription no longer certified")
        gates.append({"position": key, "move": move, "parent_cp": old_scores[key], "child_cp": old_scores[child]})
    return c4, prep, train, fitted, specs, old_scores, gates


def generate(c, base, phase):
    c4, prep, train, fitted, specs, _, _ = base
    n = c[f"{phase}_trajectories_per_arm"]
    backend = ChessBackend(train["fens"][next(iter(prep["books"]["source_one_card"]))],
        fitted, specs, train, c4, c[f"max_{phase}_positions"])
    cfg = Config(**c["config"])
    traces = {}
    started = time.monotonic()
    for model in sorted(specs):
        for name, book in sorted(prep["books"].items()):
            arm = model+"/"+name
            values = []
            for i in range(n):
                if time.monotonic()-started > c["sampling_runtime_seconds"]:
                    raise TimeoutError("sampling runtime guard; no partial result claimed")
                values.append(sample_trace(backend, model, book, cfg, c[f"{phase}_seed"], i))
            if any(t["reason"] == "frontier" for t in values):
                raise AssertionError("full legal sampler created a frontier")
            traces[arm] = values
            print(phase, arm, n, "trajectories;", len(backend.fens), "positions", flush=True)
    return traces, backend.fens, time.monotonic()-started


def source_lookup(fens, c4, expected=None):
    paths = [str(Path(c4["eval_arrays"])/n) for n in ("eval_hash.npy", "eval_cp.npy", "eval_arrays.meta.json")] + [c4["eval_db"]]
    before = [fingerprint(p, full=Path(p).stat().st_size < 1000000) for p in paths]
    if expected is not None and before != expected:
        raise ValueError("canonical source changed between pilot and final")
    status = verify_eval_arrays(Path(c4["eval_arrays"]), Path(c4["eval_db"]), adopt=False)
    hh, cc = open_eval_arrays(Path(c4["eval_arrays"]))
    keys = sorted(fens)
    values = lookup_evals(np.array(keys, dtype=np.int64), hh, cc)
    result = {k: None if v == MISSING else int(v) for k, v in zip(keys, values)}
    del hh, cc
    if before != [fingerprint(p, full=Path(p).stat().st_size < 1000000) for p in paths]:
        raise ValueError("canonical eval source changed during lookup")
    return result, before, status


def select_missing_targets(traces, source, old_scores, fens, cfg, cap):
    scores = {**source, **old_scores}
    first_by_arm, visits = {}, Counter()
    for arm, paths in traces.items():
        first = Counter()
        for trace in paths:
            visits.update(trace["keys"])
            for ply, key in enumerate(trace["keys"]):
                cp = trace["exact_cp"] if ply == len(trace["keys"])-1 and trace.get("exact_cp") is not None else scores.get(key)
                if cp is None:
                    first[key] += 1
                    break
                if abs(cp) >= cfg.threshold_cp:
                    break
        first_by_arm[arm] = first
    missing = [k for k in visits if scores.get(k) is None and chess.Board(fens[k]).outcome(claim_draw=False) is None]
    rank = lambda k: (-max(x[k] for x in first_by_arm.values()), -sum(x[k] for x in first_by_arm.values()), -visits[k], k)
    selected = sorted(missing, key=rank)[:cap]
    return {"targets": selected, "available_missing": len(missing),
        "ranking": [{"position": k, "max_arm_first_gap_count": max(x[k] for x in first_by_arm.values()),
                     "total_first_gap_count": sum(x[k] for x in first_by_arm.values()), "visits": visits[k]} for k in selected]}


def verified_pilot(c):
    p = json.loads(output_path("m5/pilot_manifest.json").read_text())
    if p["identity"] != identity(c):
        raise ValueError("pilot code/config/input identity drift")
    for name, digest in p["snapshots"].items():
        if sha256(output_path(name)) != digest:
            raise ValueError("pilot snapshot drift")
    return p


def pilot(c):
    if output_path("m5/pilot_manifest.json").exists():
        verified_pilot(c)
        print("Verified frozen pilot and engine selection", flush=True)
        return
    base = base_inputs(c)
    traces, fens, elapsed = generate(c, base, "pilot")
    source, sources, status = source_lookup(fens, base[0])
    selection = select_missing_targets(traces, source, base[5], fens, Config(**c["config"]), c["engine_positions"])
    selection["deeper"] = selection["targets"][:c["engine_deeper_positions"]]
    snapshots = {name: sha256(write_json(name, obj)) for name, obj in {
        "m5/pilot_traces.json": traces, "m5/pilot_fens.json": fens,
        "m5/pilot_source_evals.json": source, "m5/engine_selection.json": selection}.items()}
    output = {"identity": identity(c), "snapshots": snapshots, "eval_sources": sources,
        "eval_verification": status, "gates": base[6], "engine_identity": base[1]["engine_identity"],
        "visited_positions": len(fens), "missing_source_evals": sum(v is None for v in source.values()),
        "engine_targets": len(selection["targets"]), "generation_seconds": elapsed,
        "frozen_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m5/pilot_manifest.json", output)
    print(json.dumps({k: output[k] for k in ("visited_positions", "missing_source_evals", "engine_targets", "generation_seconds")}), flush=True)


def engine(c):
    p = verified_pilot(c)
    c4 = json.loads(M4_CONFIG.read_text())
    if fingerprint(c4["engine_path"], full=True) != p["engine_identity"]["binary"]:
        raise ValueError("pilot engine executable drift")
    cache_path = output_path("m5/engine_audit.json")
    ident = {"pilot_manifest_sha256": sha256(output_path("m5/pilot_manifest.json")), "engine": p["engine_identity"]}
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {"identity": ident, "base": {}, "deeper": {}, "complete": False}
    if cache["identity"] != ident:
        raise ValueError("engine cache identity drift")
    if cache["complete"]:
        print("Verified frozen M5 engine audit", flush=True)
        return
    selection = json.loads(output_path("m5/engine_selection.json").read_text())
    fens = json.loads(output_path("m5/pilot_fens.json").read_text())
    started = time.monotonic()
    with chess.engine.SimpleEngine.popen_uci(c4["engine_path"], timeout=10) as worker:
        if worker.id != p["engine_identity"]["id"]:
            raise ValueError("engine UCI identity drift")
        worker.configure({**c4["engine_options"], "SyzygyPath": ""})
        for phase, keys, nodes, seconds in (("base", selection["targets"], c["engine_nodes"], c["engine_time_seconds"]),
                ("deeper", selection["deeper"], c["deeper_nodes"], c["deeper_time_seconds"])):
            for i, key in enumerate(keys, 1):
                if key in cache[phase]:
                    continue
                if time.monotonic()-started > c["engine_runtime_seconds"]:
                    write_json("m5/engine_audit.json", cache)
                    raise TimeoutError("engine runtime guard; partial work saved")
                cache[phase][key] = analyse_position(worker, fens[key], nodes, seconds)
                if i % 16 == 0 or i == len(keys):
                    write_json("m5/engine_audit.json", cache)
                if i % 64 == 0 or i == len(keys):
                    print(phase, i, "/", len(keys), f"{time.monotonic()-started:.1f}s", flush=True)
    if fingerprint(c4["engine_path"], full=True) != p["engine_identity"]["binary"]:
        raise ValueError("engine changed during audit")
    for phase, keys in (("base", selection["targets"]), ("deeper", selection["deeper"])):
        if set(cache[phase]) != set(keys):
            raise ValueError("incomplete engine target coverage")
    cache.update(complete=True, final_session_seconds=time.monotonic()-started, frozen_utc=datetime.now(timezone.utc).isoformat())
    write_json("m5/engine_audit.json", cache)


def control(c, base):
    stored = json.loads(output_path("m4/graphs.json").read_text())["expanded"]
    template = json.loads(output_path("m4/templates.json").read_text())["expanded"]
    source = json.loads(output_path("m4/source_evals.json").read_text())
    reference = json.loads(output_path("m4/results.json").read_text())["rows"]
    cfg, out = Config(**c["config"]), {}
    started = time.monotonic()
    for model, data in stored.items():
        g = apply_evals(graph_from_dict(data), template, {**source, **base[5]})
        backend = GraphBackend(g)
        evals = {k: n.cp for k, n in g.nodes.items()}
        for name, book in base[1]["books"].items():
            rows = []
            for i in range(c["control_trajectories_per_arm"]):
                if time.monotonic()-started > c["sampling_runtime_seconds"]:
                    raise TimeoutError("control runtime guard")
                rows.append(score_trace(sample_trace(backend, model, book, cfg, c["control_seed"], i), evals, cfg))
            summary = arm_summary(rows, c["monte_carlo_alpha"])
            exact = next(r["metrics"] for r in reference if r["variant"] == "expanded" and r["evidence"] == "audited_overlay" and r["model"] == model and r["plan"] == name)
            checks = {}
            for metric, result in summary["metrics"].items():
                if "mean" not in result:
                    continue
                tolerance = 6*result["mc_se"]+2/len(rows)
                error = abs(result["mean"]-exact[metric])
                checks[metric] = {"exact": exact[metric], "error": error, "tolerance": tolerance}
                if error > tolerance:
                    raise AssertionError(f"control disagrees with exact evaluator: {model}/{name}/{metric}")
            out[model+"/"+name] = {"summary": summary, "checks": checks}
    return {"arms": out, "elapsed_seconds": time.monotonic()-started, "status": "passed"}


def final(c):
    p = verified_pilot(c)
    audit_path = output_path("m5/engine_audit.json")
    audit = json.loads(audit_path.read_text())
    if not audit["complete"] or audit["identity"]["pilot_manifest_sha256"] != sha256(output_path("m5/pilot_manifest.json")):
        raise ValueError("final sample requires a completed frozen pilot engine map")
    result_path = output_path("m5/results.json")
    ident = {"pilot_manifest_sha256": sha256(output_path("m5/pilot_manifest.json")), "engine_audit_sha256": sha256(audit_path)}
    if result_path.exists():
        saved = json.loads(result_path.read_text())
        if saved["identity"] != ident or any(sha256(output_path(k)) != v for k, v in saved["snapshots"].items()):
            raise ValueError("frozen final result drift")
        print("Verified frozen M5 final result", flush=True)
        return
    base = base_inputs(c)
    controlled = control(c, base)
    traces, fens, elapsed = generate(c, base, "final")
    source, sources, status = source_lookup(fens, base[0], p["eval_sources"])
    new = {k: v["cp"] for k, v in {**audit["base"], **audit["deeper"]}.items()}
    maps = {"source": source, "audited_overlay": {**source, **base[5], **new}, "audited_only": {**base[5], **new}}
    cfg, summaries, contrasts = Config(**c["config"]), {}, {}
    for evidence, evals in maps.items():
        scored = {arm: [score_trace(t, evals, cfg) for t in paths] for arm, paths in traces.items()}
        summaries[evidence] = {arm: arm_summary(rows, c["monte_carlo_alpha"]) for arm, rows in scored.items()}
        contrasts[evidence] = {}
        for model in base[4]:
            a, b = model+"/two_cards", model+"/source_one_card"
            contrasts[evidence][model] = paired_summary(scored[a], scored[b], traces[a], traces[b], c["monte_carlo_alpha"])
    snapshots = {name: sha256(write_json(name, obj)) for name, obj in {
        "m5/final_traces.json": traces, "m5/final_fens.json": fens,
        "m5/final_source_evals.json": source, "m5/control.json": controlled}.items()}
    primary = contrasts["audited_overlay"]["shared_position"]["discounted_first_advantage"]
    result = {"identity": ident, "snapshots": snapshots, "summaries": summaries, "contrasts": contrasts,
        "primary_contrast": primary, "primary_outer_excludes_zero": primary["mc_outer_95"][0] > 0 or primary["mc_outer_95"][1] < 0,
        "counts": {"arms": len(traces), "trajectories_per_arm": c["final_trajectories_per_arm"],
            "visited_positions": len(fens), "missing_source_evals": sum(v is None for v in source.values()),
            "missing_overlay_evals": sum(maps["audited_overlay"].get(k) is None for k in fens),
            "new_engine_targets_seen_in_final": len(set(new)&set(fens))},
        "generation_seconds": elapsed, "eval_sources": sources, "eval_verification": status,
        "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m5/results.json", result)
    print(json.dumps({"counts": result["counts"], "primary_contrast": primary, "generation_seconds": elapsed}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    phase = parser.add_mutually_exclusive_group(required=True)
    for name in ("pilot", "engine", "final"):
        phase.add_argument("--"+name, action="store_true")
    args = parser.parse_args()
    c = json.loads(CONFIG.read_text())
    (pilot if args.pilot else engine if args.engine else final)(c)


if __name__ == "__main__":
    main()
