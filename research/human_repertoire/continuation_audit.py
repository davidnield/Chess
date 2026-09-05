"""Frozen common-graph experiment; each phase is bounded and separately saved.

python -B -m research.human_repertoire.continuation_audit --prepare
python -B -m research.human_repertoire.continuation_audit --engine
python -B -m research.human_repertoire.continuation_audit --compare
Only --prepare reads sparse canonical evals; --engine runs the isolated UCI job.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import time

import chess
import chess.engine
import numpy as np
from python.eval_arrays import MISSING, lookup_evals, open_eval_arrays, verify_eval_arrays
from python.zobrist import zobrist_int64

from .common_graph import (Kernels, apply_evals, base_template, expand_template,
    materialize, missing_edge_ranking, position_priorities, select_engine_targets)
from .empirical import audit_metrics, build_graph, stats
from .io import ROOT, fingerprint, output_path, sha256, write_json
from .model import Config, book_cost, eligible, evaluate, feasible, graph_dict, graph_from_dict
from .oracle import path_evaluate

CONFIG = ROOT / "docs/human_repertoire/milestone4_config.json"


def code_identity(c):
    names = ("common_graph.py", "continuation_audit.py", "shared_fallback.py", "model.py", "empirical.py", "oracle.py")
    return {"config": c, "code_sha256": {n: sha256(ROOT/"research/human_repertoire"/n) for n in names},
        "adapter_sha256": {n: sha256(ROOT/"python"/n) for n in ("zobrist.py", "eval_arrays.py")},
        "plan_sha256": sha256(ROOT/"docs/human_repertoire/milestone4_plan.md"),
        "frozen_model_sha256": sha256(output_path(c["frozen_model"])), "python_chess_version": chess.__version__}


def frozen_inputs(c):
    model = json.loads(output_path(c["frozen_model"]).read_text())
    if sha256(output_path(c["fitted_counts"])) != model["fitted_counts_sha256"]:
        raise ValueError("fitted counts drift")
    manifest = json.loads(output_path("m3/cohort_manifest.json").read_text())
    if sha256(output_path("m3/cohort_manifest.json")) != model["identity"]["cohort_manifest_sha256"]:
        raise ValueError("model/cohort identity drift")
    if sha256(output_path(c["train_games"])) != manifest["snapshots"][c["train_games"]]:
        raise ValueError("training snapshot drift")
    games = json.loads(output_path(c["train_games"]).read_text())
    fitted = json.loads(output_path(c["fitted_counts"]).read_text())
    specs = {n: model["selected_specs"][n] for n in c["model_names"]}
    return games, fitted, specs


def fixed_books(c, train):
    board = chess.Board()
    for move in ("e4", "e5", "Nf3", "Nc6"):
        board.push_san(move)
    root = str(zobrist_int64(board))
    reference = json.loads(output_path(c["reference_snapshot"]).read_text())
    if not any(str(r["position_hash"]) == root and r.get("best_move") == "d4" for r in reference):
        raise ValueError("reference projection no longer supplies 3.d4")
    source = {root: "d4"}
    board.push_san("d4")
    board.push_san("exd4")
    followup = str(zobrist_int64(board))
    if followup not in train["fens"]:
        raise ValueError("fixed follow-up absent from training")
    return root, {"empty": {}, "source_one_card": source, "two_cards": {**source, followup: "Nxd4"}}


def verified_preparation(c):
    data = json.loads(output_path("m4/preparation.json").read_text())
    if data["identity"] != code_identity(c):
        raise ValueError("prepared code/config/model drift")
    for name, digest in data["snapshots"].items():
        if sha256(output_path(name)) != digest:
            raise ValueError(f"prepared snapshot drift: {name}")
    return data


def prepare(c):
    if output_path("m4/preparation.json").exists():
        verified_preparation(c)
        print("Verified frozen common graphs and engine target selection", flush=True)
        return
    started = time.monotonic()
    games, fitted, specs = frozen_inputs(c)
    train = stats(games)
    root, books = fixed_books(c, train)
    kernels = Kernels(fitted, specs, c["max_legal_cache_positions"])
    base = base_template(train, root, books)
    temporary = {name: materialize(base, kernels, train, {}, name, c) for name in specs}
    seeds = missing_edge_ranking(base, kernels, temporary, books, c)[:c["expansion_seeds"]]
    if len(seeds) != c["expansion_seeds"]:
        raise ValueError("not enough missing edges for fixed expansion")
    expanded = expand_template(base, seeds, kernels, train, c)
    templates = {"observed": base, "expanded": expanded}
    source_paths = [str(Path(c["eval_arrays"])/n) for n in ("eval_hash.npy", "eval_cp.npy", "eval_arrays.meta.json")] + [c["eval_db"]]
    before = [fingerprint(p, full=Path(p).stat().st_size < 1000000) for p in source_paths]
    verification = verify_eval_arrays(Path(c["eval_arrays"]), Path(c["eval_db"]), adopt=False)
    hh, cc = open_eval_arrays(Path(c["eval_arrays"]))
    keys = sorted(expanded["fens"])
    values = lookup_evals(np.array(keys, dtype=np.int64), hh, cc)
    source = {k: None if v == MISSING else int(v) for k, v in zip(keys, values)}
    del hh, cc
    if before != [fingerprint(p, full=Path(p).stat().st_size < 1000000) for p in source_paths]:
        raise ValueError("canonical eval source changed during sparse lookup")
    graphs = {variant: {name: materialize(template, kernels, train, source, name, c) for name in specs}
              for variant, template in templates.items()}
    # The entire uniform baseline must reproduce the old adapter's kernel.
    allowed = {k: [m] for book in books.values() for k, m in book.items()}
    old, _ = build_graph(train, train, source, allowed, root, {**c, "move_prior_strength": 1})
    new = graphs["observed"]["tuned_position"]
    if set(old.nodes) != set(new.nodes):
        raise AssertionError("baseline node set changed")
    max_probability_error = 0.0
    for key in old.nodes:
        a, b = old.nodes[key], new.nodes[key]
        if replace(a, edges=()) != replace(b, edges=()):
            raise AssertionError(f"baseline node semantics changed: {key}")
        ae, be = {e.move: e for e in a.edges}, {e.move: e for e in b.edges}
        if set(ae) != set(be):
            raise AssertionError("baseline edge set changed")
        for move in ae:
            if replace(ae[move], probability=0) != replace(be[move], probability=0):
                raise AssertionError("baseline child/gate changed")
            max_probability_error = max(max_probability_error, abs(ae[move].probability-be[move].probability))
    if max_probability_error > 1e-12:
        raise AssertionError("baseline probability mismatch")
    scores = position_priorities(graphs["expanded"], books, c)
    mandatory = {k for b in books.values() for k in b}
    mandatory |= {expanded["explicit"][k][m] for b in books.values() for k, m in b.items()}
    selection = select_engine_targets(expanded["fens"], scores, source, mandatory,
        c["engine_positions"], c["engine_missing_quota"], c["engine_deeper_positions"])
    engine_fp = fingerprint(c["engine_path"], full=True)
    # No search occurs in this handshake. The actual executable is frozen now.
    with chess.engine.SimpleEngine.popen_uci(c["engine_path"], timeout=10) as engine:
        engine_identity = {"binary": engine_fp, "id": engine.id,
            "options": {n: {"type": o.type, "default": o.default, "min": o.min, "max": o.max, "var": o.var} for n, o in engine.options.items()}}
    snapshots = {}
    outputs = {"m4/templates.json": templates, "m4/source_evals.json": source,
        "m4/engine_selection.json": selection, "m4/graphs.json": {v: {n: graph_dict(g) for n, g in group.items()} for v, group in graphs.items()}}
    for path, obj in outputs.items():
        snapshots[path] = sha256(write_json(path, obj))
    manifest = {"identity": code_identity(c), "created_utc": datetime.now(timezone.utc).isoformat(),
        "snapshots": snapshots, "books": books, "specs": specs, "engine_identity": engine_identity,
        "eval_sources": before, "eval_verification": verification,
        "reference_sha256": sha256(output_path(c["reference_snapshot"])),
        "baseline_max_probability_error": max_probability_error,
        "counts": {"real_positions": len(source), "missing_source_evals": sum(v is None for v in source.values()),
            "observed_nodes": len(new.nodes), "expanded_nodes": len(graphs["expanded"]["tuned_position"].nodes),
            "added_real_positions": len(expanded["fens"])-len(base["fens"]), "new_expanded_positions": len(expanded["new_expanded"]),
            "engine_targets": len(selection["targets"]), "missing_engine_targets": selection["missing_targets"]},
        "elapsed_seconds": time.monotonic()-started}
    write_json("m4/preparation.json", manifest)
    print(json.dumps({"counts": manifest["counts"], "baseline_max_probability_error": max_probability_error,
        "elapsed_seconds": manifest["elapsed_seconds"]}, indent=2), flush=True)


def analyse_position(engine, fen, nodes, seconds):
    board = chess.Board(fen)
    engine.configure({"Clear Hash": None})
    info = engine.analyse(board, chess.engine.Limit(nodes=nodes, time=seconds), game=object())
    if "score" not in info or info.get("lowerbound") or info.get("upperbound"):
        raise ValueError("engine returned no unbounded root score")
    score = info["score"].white()
    return {"cp": score.score(mate_score=100000), "mate": score.mate(),
        "depth": info.get("depth"), "seldepth": info.get("seldepth"), "nodes": info.get("nodes"),
        "seconds": info.get("time"), "pv": [m.uci() for m in info.get("pv", [])],
        "wdl": list(info["wdl"].white()) if "wdl" in info else None,
        "limits": {"nodes": nodes, "seconds": seconds}, "fen": fen}


def engine_audit(c):
    prep = verified_preparation(c)
    if fingerprint(c["engine_path"], full=True) != prep["engine_identity"]["binary"]:
        raise ValueError("engine executable drift")
    templates = json.loads(output_path("m4/templates.json").read_text())
    selection = json.loads(output_path("m4/engine_selection.json").read_text())
    identity = {"preparation_sha256": sha256(output_path("m4/preparation.json")), "engine": prep["engine_identity"]}
    cache_path = output_path("m4/engine_audit.json")
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {"identity": identity, "base": {}, "deeper": {}, "complete": False}
    if cache["identity"] != identity:
        raise ValueError("engine cache drift")
    if cache["complete"]:
        print("Verified completed isolated engine audit", flush=True)
        return
    started = time.monotonic()
    with chess.engine.SimpleEngine.popen_uci(c["engine_path"], timeout=10) as engine:
        if engine.id != prep["engine_identity"]["id"]:
            raise ValueError("engine UCI identity drift")
        engine.configure({**c["engine_options"], "SyzygyPath": ""})
        for phase, targets, nodes, seconds in (
            ("base", selection["targets"], c["engine_nodes"], c["engine_time_seconds"]),
            ("deeper", selection["deeper"], c["deeper_nodes"], c["deeper_time_seconds"])):
            for i, key in enumerate(targets, 1):
                if key in cache[phase]:
                    continue
                if time.monotonic()-started > c["engine_runtime_seconds"]:
                    write_json("m4/engine_audit.json", cache)
                    raise TimeoutError("engine runtime guard; partial cache preserved")
                cache[phase][key] = analyse_position(engine, templates["expanded"]["fens"][key], nodes, seconds)
                cache["updated_utc"] = datetime.now(timezone.utc).isoformat()
                if i % 16 == 0 or i == len(targets):
                    write_json("m4/engine_audit.json", cache)
                    print(phase, i, "/", len(targets), f"{time.monotonic()-started:.1f}s", flush=True)
    if any(set(cache[p]) != set(selection[key]) for p, key in (("base", "targets"), ("deeper", "deeper"))):
        raise ValueError("incomplete engine audit")
    if fingerprint(c["engine_path"], full=True) != prep["engine_identity"]["binary"]:
        raise ValueError("engine changed during audit")
    cache.update(complete=True, final_session_seconds=time.monotonic()-started)
    write_json("m4/engine_audit.json", cache)


def compare(c):
    prep = verified_preparation(c)
    engine_path = output_path("m4/engine_audit.json")
    audit = json.loads(engine_path.read_text())
    if not audit["complete"] or audit["identity"]["preparation_sha256"] != sha256(output_path("m4/preparation.json")):
        raise ValueError("incomplete or mismatched engine audit")
    result_path = output_path("m4/results.json")
    result_identity = {"preparation_sha256": sha256(output_path("m4/preparation.json")), "engine_audit_sha256": sha256(engine_path)}
    if result_path.exists():
        if json.loads(result_path.read_text())["identity"] != result_identity:
            raise ValueError("comparison identity drift")
        print("Verified frozen common-graph comparison", flush=True)
        return
    started = time.monotonic()
    templates = json.loads(output_path("m4/templates.json").read_text())
    stored = json.loads(output_path("m4/graphs.json").read_text())
    graphs = {v: {n: graph_from_dict(g) for n, g in group.items()} for v, group in stored.items()}
    source = json.loads(output_path("m4/source_evals.json").read_text())
    selected = {**audit["base"], **audit["deeper"]}
    fresh = {k: record["cp"] for k, record in selected.items()}
    maps = {"source": source, "audited_overlay": {**source, **fresh}, "audited_only": fresh}
    cfg = Config(**c["config"])
    rows = []
    oracle_errors = []
    for variant, group in graphs.items():
        for evidence, evals in maps.items():
            for name, graph in group.items():
                g = apply_evals(graph, templates[variant], evals)
                for plan, book in prep["books"].items():
                    if time.monotonic()-started > c["comparison_runtime_seconds"]:
                        raise TimeoutError("comparison runtime guard; no completed result saved")
                    rejected = [{"position": k, "move": m, "parent_cp": g.nodes[k].cp,
                        "child_cp": next(e.sound_cp for e in g.nodes[k].edges if e.move == m)}
                        for k, m in book.items() if m not in eligible(g, cfg, k)]
                    row = {"variant": variant, "evidence": evidence, "model": name, "plan": plan,
                           "book": book, "cards": book_cost(g, book), "rejected_gates": rejected}
                    if not rejected:
                        metrics = evaluate(g, book, cfg)
                        row.update(metrics=audit_metrics(metrics), feasible=feasible(metrics, cfg))
                        if variant == "expanded" and evidence == "audited_overlay" and plan == "source_one_card":
                            small = replace(cfg, horizon=6)
                            a, b = evaluate(g, book, small), path_evaluate(g, book, small)
                            error = max(abs(getattr(a, k)-getattr(b, k)) for k in asdict(a))
                            if error > 1e-9:
                                raise AssertionError("real path-oracle discrepancy")
                            oracle_errors.append({"model": name, "horizon": 6, "max_error": error})
                    rows.append(row)
                print(variant, evidence, name, f"{time.monotonic()-started:.1f}s", flush=True)
    threshold = cfg.threshold_cp
    marker = lambda cp: 1 if cp >= threshold else -1 if cp <= -threshold else 0
    known = [{"position": k, "source_cp": source[k], "audited_cp": v["cp"], "difference": v["cp"]-source[k],
              "marker_changed": marker(source[k]) != marker(v["cp"])} for k, v in selected.items() if source.get(k) is not None]
    sensitivity = [{"position": k, "base_cp": audit["base"][k]["cp"], "deeper_cp": v["cp"],
                    "marker_changed": marker(audit["base"][k]["cp"]) != marker(v["cp"])} for k, v in audit["deeper"].items()]
    result = {"identity": result_identity, "resolved_config": asdict(cfg), "rows": rows,
        "oracle_checks": oracle_errors, "engine_comparison": {"known": known, "deeper_sensitivity": sensitivity,
        "source_marker_changes": sum(r["marker_changed"] for r in known),
        "deeper_marker_changes": sum(r["marker_changed"] for r in sensitivity),
        "median_absolute_cp_difference": statistics.median(abs(r["difference"]) for r in known) if known else None},
        "elapsed_seconds": time.monotonic()-started, "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m4/results.json", result)
    print(json.dumps({"rows": len(rows), "oracle_checks": oracle_errors, "elapsed_seconds": result["elapsed_seconds"]}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    phase = parser.add_mutually_exclusive_group(required=True)
    for name in ("prepare", "engine", "compare"):
        phase.add_argument("--"+name, action="store_true")
    args = parser.parse_args()
    c = json.loads(CONFIG.read_text())
    (prepare if args.prepare else engine_audit if args.engine else compare)(c)


if __name__ == "__main__":
    main()
