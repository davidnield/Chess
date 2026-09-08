"""M6: freeze 64 paired paths, complete budget-matched engine maps, compare.

python -B -m research.human_repertoire.paired_panel --prepare
python -B -m research.human_repertoire.paired_panel --engine
python -B -m research.human_repertoire.paired_panel --compare
All outputs stay in _artifacts/m6; canonical inputs are read-only.
"""
import argparse
from datetime import datetime, timezone
import json
import time

import chess
import chess.engine
from python.zobrist import zobrist_int64

from .continuation_audit import analyse_position
from .io import ROOT, fingerprint, output_path, sha256, write_json
from .model import Config, Edge, Graph, Node, eligible
from .rollout_experiment import CONFIG as M5_CONFIG, base_inputs, source_lookup, verified_pilot
from .trajectory import ChessBackend, arm_summary, paired_summary, sample_trace, score_trace

CONFIG = ROOT/"docs/human_repertoire/milestone6_config.json"


def identity(c):
    names = ("paired_panel.py", "trajectory.py", "verify_rollout.py", "rollout_experiment.py",
             "continuation_audit.py", "common_graph.py", "model.py", "shared_fallback.py", "empirical.py", "io.py")
    return {"config": c, "plan_sha256": sha256(ROOT/"docs/human_repertoire/milestone6_plan.md"),
        "code_sha256": {n: sha256(ROOT/"research/human_repertoire"/n) for n in names},
        "m5_manifest_sha256": sha256(output_path("m5/pilot_manifest.json")),
        "m5_engine_sha256": sha256(output_path("m5/engine_audit.json")),
        "m5_results_sha256": sha256(output_path("m5/results.json")), "python_chess_version": chess.__version__}


def checked_records(audit, phase, nodes, seconds):
    if not audit["complete"]:
        raise ValueError("incomplete inherited engine audit")
    records = audit[phase]
    for key, r in records.items():
        if r["limits"] != {"nodes": nodes, "seconds": seconds}:
            raise ValueError("inherited search budget mismatch")
        if str(zobrist_int64(chess.Board(r["fen"]))) != key or r["cp"] is None:
            raise ValueError("invalid inherited position/score")
    return records


def inherited(c):
    c5 = json.loads(M5_CONFIG.read_text())
    p5 = verified_pilot(c5)
    if c["config"] != c5["config"] or c["panel_seed"] in {c5[k+"_seed"] for k in ("pilot", "final", "control")}:
        raise ValueError("changed semantics or reused random stream")
    base = base_inputs(c5)
    audits = [json.loads(output_path(f"m{i}/engine_audit.json").read_text()) for i in (4, 5)]
    if audits[1]["identity"]["pilot_manifest_sha256"] != sha256(output_path("m5/pilot_manifest.json")):
        raise ValueError("inherited M5 audit drift")
    if json.loads(output_path("m5/results.json").read_text())["identity"]["engine_audit_sha256"] != sha256(output_path("m5/engine_audit.json")):
        raise ValueError("inherited M5 result drift")
    records = {"base": {}, "deeper": {}}
    for audit in audits:
        if audit["identity"]["engine"] != base[1]["engine_identity"]:
            raise ValueError("inherited engine identity mismatch")
        for phase, nodes, seconds in (("base", c["engine_nodes"], c["engine_time_seconds"]),
                ("deeper", c["deeper_nodes"], c["deeper_time_seconds"])):
            records[phase].update(checked_records(audit, phase, nodes, seconds))
    return base, p5, records


def target_selection(paths, fens, gate_keys, records, c):
    arms = sorted(paths)
    n = c["panel_trajectories_per_arm"]
    if len(arms) != 2 or any([t["index"] for t in paths[a]] != list(range(n)) for a in arms):
        raise ValueError("panel must keep every paired index")
    result = {}
    for phase, size, cap in (("base", n, c["max_base_positions"]), ("deeper", c["deeper_pairs"], c["max_deeper_positions"])):
        ordered = list(sorted(gate_keys))
        for index in range(size):
            for arm in arms:
                ordered.extend(paths[arm][index]["keys"])
        ordered = list(dict.fromkeys(ordered))
        if len(ordered) > cap:
            raise ValueError("position cap exceeded; no paths dropped")
        required = [k for k in ordered if chess.Board(fens[k]).outcome(claim_draw=False) is None]
        reused = [k for k in required if k in records[phase]]
        for key in reused:
            old, new = chess.Board(records[phase][key]["fen"]), chess.Board(fens[key])
            if old.fen().split()[:4] != new.fen().split()[:4] or max(old.halfmove_clock, new.halfmove_clock)+c["config"]["horizon"] >= 100:
                raise ValueError("cached FEN/counter mismatch")
        result[phase] = {"required": required, "reused": reused, "targets": [k for k in required if k not in records[phase]],
                         "exact_terminals": [k for k in ordered if k not in required], "pairs": size}
    return result


def gate_definitions(base):
    _, prep, train, _, _, _, _ = base
    gates, fens = [], {}
    for key, move in sorted({(k, m) for b in prep["books"].values() for k, m in b.items()}):
        board = chess.Board(train["fens"][key])
        fens[key] = board.fen()
        board.push_san(move)
        child = str(zobrist_int64(board))
        fens[child] = board.fen()
        gates.append({"position": key, "move": move, "child": child})
    return gates, fens


def check_gates(gates, scores, cfg):
    out = []
    for g in gates:
        key, child, move = g["position"], g["child"], g["move"]
        parent_cp, child_cp = scores.get(key), scores.get(child)
        graph = Graph(key, {key: Node("us", (Edge(move, child, 1, child_cp),), parent_cp), child: Node("frontier", cp=child_cp)})
        out.append({**g, "parent_cp": parent_cp, "child_cp": child_cp, "passed": move in eligible(graph, cfg, key)})
    return out


def verified_panel(c):
    p = json.loads(output_path("m6/panel_manifest.json").read_text())
    if p["identity"] != identity(c):
        raise ValueError("panel code/config/input drift")
    if any(sha256(output_path(k)) != v for k, v in p["snapshots"].items()):
        raise ValueError("panel snapshot drift")
    return p


def prepare(c):
    if output_path("m6/panel_manifest.json").exists():
        verified_panel(c)
        print("Verified frozen paired panel", flush=True)
        return
    base, p5, records = inherited(c)
    c4, prep, train, fitted, specs, _, _ = base
    root = next(iter(prep["books"]["source_one_card"]))
    backend = ChessBackend(train["fens"][root], fitted, specs, train, c4, c["max_panel_positions"])
    cfg, paths, started = Config(**c["config"]), {}, time.monotonic()
    for name in c["plans"]:
        traces = []
        for i in range(c["panel_trajectories_per_arm"]):
            if time.monotonic()-started > c["sampling_runtime_seconds"]:
                raise TimeoutError("panel sampling guard")
            traces.append(sample_trace(backend, c["model"], prep["books"][name], cfg, c["panel_seed"], i))
        paths[c["model"]+"/"+name] = traces
    gates, gate_fens = gate_definitions(base)
    fens = {**gate_fens, **backend.fens}
    selection = target_selection(paths, fens, gate_fens.keys(), records, c)
    source, sources, status = source_lookup(fens, c4, p5["eval_sources"])
    # Keep bounded copies of only records needed for this panel.
    kept = {phase: {k: records[phase][k] for k in selection[phase]["reused"]} for phase in records}
    overlay = {k: r["cp"] for k, r in {**records["base"], **records["deeper"]}.items() if k in fens}
    snapshots = {name: sha256(write_json(name, obj)) for name, obj in {
        "m6/panel_traces.json": paths, "m6/panel_fens.json": backend.fens,
        "m6/engine_fens.json": fens, "m6/source_evals.json": source,
        "m6/engine_selection.json": selection, "m6/inherited_records.json": kept,
        "m6/inherited_overlay.json": overlay}.items()}
    result = {"identity": identity(c), "snapshots": snapshots, "engine_identity": prep["engine_identity"],
        "engine_path": c4["engine_path"], "engine_options": {**c4["engine_options"], "SyzygyPath": ""},
        "gates": gates, "eval_sources": sources, "eval_verification": status,
        "counts": {"pairs": c["panel_trajectories_per_arm"], "visited_positions": len(backend.fens),
            "base_targets": len(selection["base"]["targets"]), "deeper_targets": len(selection["deeper"]["targets"])},
        "elapsed_seconds": time.monotonic()-started, "frozen_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m6/panel_manifest.json", result)
    print(json.dumps(result["counts"], indent=2), flush=True)


def audit_identity(p):
    return {"panel_manifest_sha256": sha256(output_path("m6/panel_manifest.json")), "engine": p["engine_identity"]}


def engine(c):
    p = verified_panel(c)
    if fingerprint(p["engine_path"], full=True) != p["engine_identity"]["binary"]:
        raise ValueError("panel engine executable drift")
    path = output_path("m6/engine_audit.json")
    cache = json.loads(path.read_text()) if path.exists() else {"identity": audit_identity(p), "base": {}, "deeper": {}, "complete": False}
    if cache["identity"] != audit_identity(p):
        raise ValueError("panel engine cache drift")
    if cache["complete"]:
        print("Verified completed paired-panel engine work", flush=True)
        return
    selection = json.loads(output_path("m6/engine_selection.json").read_text())
    fens = json.loads(output_path("m6/engine_fens.json").read_text())
    started = time.monotonic()
    with chess.engine.SimpleEngine.popen_uci(p["engine_path"], timeout=10) as worker:
        if worker.id != p["engine_identity"]["id"]:
            raise ValueError("panel engine UCI identity drift")
        worker.configure(p["engine_options"])
        for phase, nodes, seconds in (("base", c["engine_nodes"], c["engine_time_seconds"]),
                ("deeper", c["deeper_nodes"], c["deeper_time_seconds"])):
            keys = selection[phase]["targets"]
            for i, key in enumerate(keys, 1):
                if key in cache[phase]:
                    continue
                if time.monotonic()-started > c["engine_runtime_seconds"]:
                    write_json("m6/engine_audit.json", cache)
                    raise TimeoutError("engine session guard; full panel and partial scores retained")
                cache[phase][key] = analyse_position(worker, fens[key], nodes, seconds)
                if i % 16 == 0 or i == len(keys):
                    write_json("m6/engine_audit.json", cache)
                if i % 64 == 0 or i == len(keys):
                    print(phase, i, "/", len(keys), f"{time.monotonic()-started:.1f}s", flush=True)
    if any(set(cache[phase]) != set(selection[phase]["targets"]) for phase in ("base", "deeper")):
        raise ValueError("incomplete panel target coverage")
    if fingerprint(p["engine_path"], full=True) != p["engine_identity"]["binary"]:
        raise ValueError("engine changed during panel audit")
    cache.update(complete=True, final_session_seconds=time.monotonic()-started, frozen_utc=datetime.now(timezone.utc).isoformat())
    write_json("m6/engine_audit.json", cache)


def score_panel(paths, scores, cfg, alpha, require_complete=False):
    rows = {arm: [score_trace(t, scores, cfg) for t in traces] for arm, traces in paths.items()}
    if require_complete and any(r["unknown_eval_mass"] or r["frontier_mass"] for values in rows.values() for r in values):
        raise ValueError("complete panel still has missing positions or a frontier")
    arms = sorted(paths)
    return {"arms": {arm: arm_summary(values, alpha) for arm, values in rows.items()},
        "two_minus_one": paired_summary(rows[arms[1]], rows[arms[0]], paths[arms[1]], paths[arms[0]], alpha)}


def compare(c):
    p = verified_panel(c)
    audit = json.loads(output_path("m6/engine_audit.json").read_text())
    if audit["identity"] != audit_identity(p):
        raise ValueError("panel comparison audit drift")
    ident = {"panel_manifest_sha256": sha256(output_path("m6/panel_manifest.json")), "engine_audit_sha256": sha256(output_path("m6/engine_audit.json"))}
    name = "m6/results.json" if audit["complete"] else "m6/partial_results.json"
    if output_path(name).exists() and json.loads(output_path(name).read_text())["identity"] == ident:
        print("Verified saved panel comparison", flush=True)
        return
    paths = json.loads(output_path("m6/panel_traces.json").read_text())
    source = json.loads(output_path("m6/source_evals.json").read_text())
    overlay = json.loads(output_path("m6/inherited_overlay.json").read_text())
    prior = json.loads(output_path("m6/inherited_records.json").read_text())
    selection = json.loads(output_path("m6/engine_selection.json").read_text())
    cfg, alpha = Config(**c["config"]), c["monte_carlo_alpha"]
    maps = {phase: {k: r["cp"] for k, r in {**prior[phase], **audit[phase]}.items()} for phase in prior}
    complete = audit["complete"]
    summaries = {"source_only": score_panel(paths, source, cfg, alpha),
        "inherited_mixed_overlay": score_panel(paths, {**source, **overlay}, cfg, alpha),
        "base_100k": score_panel(paths, maps["base"], cfg, alpha, complete)}
    subset = {arm: values[:c["deeper_pairs"]] for arm, values in paths.items()}
    deeper = {"base_100k": score_panel(subset, maps["base"], cfg, alpha, complete),
              "deeper_400k": score_panel(subset, maps["deeper"], cfg, alpha, complete)}
    curve = []
    available = [k for k in selection["base"]["targets"] if k in audit["base"]]
    for n in sorted({min(k, len(available)) for k in c["budget_checkpoints"]} | {len(available)}):
        scores = {k: r["cp"] for k, r in prior["base"].items()}
        scores.update({k: audit["base"][k]["cp"] for k in available[:n]})
        curve.append({"new_positions": n, **score_panel(paths, scores, cfg, alpha)})
    gates = {phase: check_gates(p["gates"], scores, cfg) for phase, scores in maps.items()}
    result = {"identity": ident, "complete": complete, "summaries": summaries, "deeper_subset": deeper,
        "budget_curve": curve, "gates": gates,
        "primary_contrast": summaries["base_100k"]["two_minus_one"]["discounted_first_advantage"],
        "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json(name, result)
    print(json.dumps({k: result[k] for k in ("complete", "primary_contrast", "gates")}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    for name in ("prepare", "engine", "compare"):
        group.add_argument("--"+name, action="store_true")
    args = parser.parse_args()
    c = json.loads(CONFIG.read_text())
    (prepare if args.prepare else engine if args.engine else compare)(c)


if __name__ == "__main__":
    main()
