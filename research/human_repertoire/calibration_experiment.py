"""M7 frozen human/model move-loss diagnostic.

python -B -m research.human_repertoire.calibration_experiment --prepare
python -B -m research.human_repertoire.calibration_experiment --engine
python -B -m research.human_repertoire.calibration_experiment --compare
Only bounded frozen game/engine inputs; outputs stay in _artifacts/m7.
"""
import argparse
from datetime import datetime, timezone
import json
import time

import chess
import chess.engine

from .continuation_audit import analyse_position
from .date_cohort import CONFIG as M3_CONFIG
from .io import ROOT, fingerprint, output_path, sha256, write_json
from .loss_calibration import loss_rows, propose, select_cases, summaries, targets
from .paired_panel import CONFIG as M6_CONFIG, checked_records, inherited, verified_panel
from .shared_fallback import inputs

CONFIG = ROOT/"docs/human_repertoire/milestone7_config.json"


def identity(c):
    return {"config": c, "plan_sha256": sha256(ROOT/"docs/human_repertoire/milestone7_plan.md"),
        "code_sha256": {n: sha256(ROOT/"research/human_repertoire"/n) for n in
            ("calibration_experiment.py", "loss_calibration.py", "paired_panel.py", "common_graph.py", "shared_fallback.py", "trajectory.py", "io.py")},
        "m6_manifest_sha256": sha256(output_path("m6/panel_manifest.json")),
        "m6_engine_sha256": sha256(output_path("m6/engine_audit.json")),
        "m6_results_sha256": sha256(output_path("m6/results.json")),
        "test_games_sha256": sha256(output_path("m3/test_games.json")),
        "cohort_manifest_sha256": sha256(output_path("m3/cohort_manifest.json")),
        "fitted_counts_sha256": sha256(output_path("m3/fitted_counts.json")), "python_chess_version": chess.__version__}


def frozen(c):
    c6 = json.loads(M6_CONFIG.read_text())
    p6 = verified_panel(c6)
    base, _, records = inherited(c6)
    audit = json.loads(output_path("m6/engine_audit.json").read_text())
    if audit["identity"] != {"panel_manifest_sha256": sha256(output_path("m6/panel_manifest.json")), "engine": p6["engine_identity"]}:
        raise ValueError("inherited M6 engine drift")
    if json.loads(output_path("m6/results.json").read_text())["identity"]["engine_audit_sha256"] != sha256(output_path("m6/engine_audit.json")):
        raise ValueError("inherited M6 result drift")
    for phase, nodes, seconds in (("base", c["engine_nodes"], c["engine_time_seconds"]),
            ("deeper", c["deeper_nodes"], c["deeper_time_seconds"])):
        records[phase].update(checked_records(audit, phase, nodes, seconds))
        for r in records[phase].values():
            if r["limits"] != {"nodes": nodes, "seconds": seconds}:
                raise ValueError("inherited budget mismatch")
    games = inputs(json.loads(M3_CONFIG.read_text()), ("test",))["test"]
    return games, base[3], base[4], records, p6


def verified_cases(c):
    p = json.loads(output_path("m7/case_manifest.json").read_text())
    if p["identity"] != identity(c):
        raise ValueError("calibration code/config/input drift")
    if any(sha256(output_path(k)) != v for k, v in p["snapshots"].items()):
        raise ValueError("calibration snapshot drift")
    return p


def prepare(c):
    if output_path("m7/case_manifest.json").exists():
        verified_cases(c)
        print("Verified frozen calibration cases", flush=True)
        return
    started = time.monotonic()
    games, fitted, specs, records, p6 = frozen(c)
    selected, frame = select_cases(games, fitted, c)
    cases, fens = propose(selected, fitted, specs, c)
    selection = targets(cases, fens, records, c)
    kept = {phase: {k: records[phase][k] for k in selection[phase]["reused"]} for phase in records}
    if time.monotonic()-started > c["prepare_runtime_seconds"]:
        raise TimeoutError("calibration preparation guard")
    snapshots = {name: sha256(write_json(name, obj)) for name, obj in {
        "m7/cases.json": cases, "m7/selection_frame.json": frame, "m7/fens.json": fens,
        "m7/engine_selection.json": selection, "m7/inherited_records.json": kept}.items()}
    manifest = {"identity": identity(c), "snapshots": snapshots,
        "engine_identity": p6["engine_identity"], "engine_path": p6["engine_path"], "engine_options": p6["engine_options"],
        "counts": {"cases": len(cases), "unique_games": len({r["game_id"] for r in cases}),
            "positions": len(fens), "base_targets": len(selection["base"]["targets"]), "deeper_targets": len(selection["deeper"]["targets"])},
        "generation_seconds": time.monotonic()-started, "frozen_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m7/case_manifest.json", manifest)
    print(json.dumps(manifest["counts"], indent=2), flush=True)


def audit_identity(p):
    return {"case_manifest_sha256": sha256(output_path("m7/case_manifest.json")), "engine": p["engine_identity"]}


def engine(c):
    p = verified_cases(c)
    if fingerprint(p["engine_path"], full=True) != p["engine_identity"]["binary"]:
        raise ValueError("calibration engine executable drift")
    path = output_path("m7/engine_audit.json")
    audit = json.loads(path.read_text()) if path.exists() else {"identity": audit_identity(p), "base": {}, "deeper": {}, "complete": False}
    if audit["identity"] != audit_identity(p):
        raise ValueError("calibration engine cache drift")
    if audit["complete"]:
        print("Verified complete calibration engine work", flush=True)
        return
    selection = json.loads(output_path("m7/engine_selection.json").read_text())
    fens = json.loads(output_path("m7/fens.json").read_text())
    started = time.monotonic()
    with chess.engine.SimpleEngine.popen_uci(p["engine_path"], timeout=10) as worker:
        if worker.id != p["engine_identity"]["id"]:
            raise ValueError("calibration UCI identity drift")
        worker.configure(p["engine_options"])
        for phase, nodes, seconds in (("base", c["engine_nodes"], c["engine_time_seconds"]),
                ("deeper", c["deeper_nodes"], c["deeper_time_seconds"])):
            keys = selection[phase]["targets"]
            for index, key in enumerate(keys, 1):
                if key in audit[phase]:
                    continue
                if time.monotonic()-started > c["engine_runtime_seconds"]:
                    write_json("m7/engine_audit.json", audit)
                    raise TimeoutError("calibration engine guard; every case and partial score retained")
                audit[phase][key] = analyse_position(worker, fens[key], nodes, seconds)
                if index % 16 == 0 or index == len(keys):
                    write_json("m7/engine_audit.json", audit)
                if index % 64 == 0 or index == len(keys):
                    print(phase, index, "/", len(keys), f"{time.monotonic()-started:.1f}s", flush=True)
    if any(set(audit[phase]) != set(selection[phase]["targets"]) for phase in ("base", "deeper")):
        raise ValueError("calibration target coverage incomplete")
    if fingerprint(p["engine_path"], full=True) != p["engine_identity"]["binary"]:
        raise ValueError("engine changed during calibration")
    audit.update(complete=True, final_session_seconds=time.monotonic()-started, frozen_utc=datetime.now(timezone.utc).isoformat())
    write_json("m7/engine_audit.json", audit)


def compare(c):
    p = verified_cases(c)
    audit = json.loads(output_path("m7/engine_audit.json").read_text())
    if not audit["complete"] or audit["identity"] != audit_identity(p):
        raise ValueError("completed calibration requires full frozen engine coverage")
    ident = {"case_manifest_sha256": sha256(output_path("m7/case_manifest.json")), "engine_audit_sha256": sha256(output_path("m7/engine_audit.json"))}
    path = output_path("m7/results.json")
    if path.exists():
        saved = json.loads(path.read_text())
        if saved["identity"] != ident or sha256(output_path("m7/scored_cases.json")) != saved["scored_cases_sha256"]:
            raise ValueError("calibration result drift")
        print("Verified frozen calibration result", flush=True)
        return
    cases = json.loads(output_path("m7/cases.json").read_text())
    selection = json.loads(output_path("m7/engine_selection.json").read_text())
    prior = json.loads(output_path("m7/inherited_records.json").read_text())
    maps = {phase: {**{k: r["cp"] for k, r in {**prior[phase], **audit[phase]}.items()}, **selection[phase]["exact"]} for phase in prior}
    rows = loss_rows(cases, maps["base"], c)
    subset = [r for r in cases if r["deeper"]]
    subrows = {phase: loss_rows(subset, scores, c) for phase, scores in maps.items()}
    summary = summaries(rows, c)
    sensitivity = {phase: summaries(values, c, bootstrap=False) for phase, values in subrows.items()}
    snapshots = {"base": rows, "deeper_subset": subrows}
    result = {"identity": ident, "scored_cases_sha256": sha256(write_json("m7/scored_cases.json", snapshots)),
        "summary": summary, "deeper_subset": sensitivity,
        "primary": summary["groups"]["balanced"]["metrics"]["loss_primary"],
        "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m7/results.json", result)
    print(json.dumps({"primary": result["primary"], "bootstrap": summary["bootstrap"]}, indent=2), flush=True)


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
