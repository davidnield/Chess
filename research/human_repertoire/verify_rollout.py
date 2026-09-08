"""Replay every M5 pilot/final path and independently reconstruct its metrics.

python -B -m research.human_repertoire.verify_rollout
Only bounded frozen snapshots are read. No engine work or canonical data scan.
"""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import time

import chess
from python.zobrist import zobrist_int64

from .common_graph import Kernels
from .empirical import wdl
from .io import ROOT, fingerprint, output_path, sha256, write_json
from .model import Config
from .rollout_experiment import CONFIG, base_inputs, select_missing_targets, verified_pilot
from .trajectory import arm_summary, paired_summary, score_trace, uniform


def reconstruct(trace, evals, cfg):
    """Independent whole-path first-hit accounting, without the sampler scorer."""
    values = [evals.get(k) for k in trace["keys"]]
    if trace.get("exact_cp") is not None:
        values[-1] = trace["exact_cp"]
    hits = [i for i, v in enumerate(values) if v is not None and abs(v) >= cfg.threshold_cp]
    first = hits[0] if hits else None
    positive = first is not None and values[first] > 0
    negative = first is not None and values[first] < 0
    gap = None in (values[:first+1] if first is not None else values)
    reason = trace["reason"]
    early_cut = reason == "frontier" and len(values)-1 < cfg.horizon
    early = cfg.discount**first if positive else 0
    w, d, l = trace["wdl"]
    return {"win": w, "draw": d, "loss": l, "points": w+.5*d,
        "advantage": float(positive), "disadvantage": float(negative), "no_hit": float(first is None),
        "early_advantage": early, "win_after_advantage": w if positive else 0,
        "terminal_mass": float(reason == "terminal"), "frontier_mass": float(reason == "frontier"),
        "horizon_mass": float(reason == "horizon"), "repetition_mass": float(reason == "repetition"),
        "human_end_mass": float(reason == "human_end"), "unknown_eval_mass": float(None in values),
        "frontier_before_horizon_mass": float(early_cut), "unresolved_no_hit_mass": float(early_cut and first is None),
        "certain_advantage_mass": float(positive and not gap), "certain_early_advantage": early if not gap else 0,
        "unresolved_first_hit_mass": float(gap or (early_cut and first is None))}


def replay_phase(paths, fens, phase, base, c):
    c4, prep, train, fitted, specs, _, _ = base
    kernels = Kernels(fitted, specs, c[f"max_{phase}_positions"])
    cfg, seed = Config(**c["config"]), c[f"{phase}_seed"]
    root = next(iter(prep["books"]["source_one_card"]))
    counts, visited, arm_diagnostics = Counter(), set(), {}
    for arm, traces in paths.items():
        model, name = arm.split("/")
        book, diagnostics = prep["books"][name], Counter()
        assert [t["index"] for t in traces] == list(range(c[f"{phase}_trajectories_per_arm"]))
        for trace in traces:
            board, history, active = chess.Board(train["fens"][root]), [], True
            assert len(trace["keys"]) == len(trace["moves"])+1 <= cfg.horizon+1
            for ply, key in enumerate(trace["keys"]):
                assert str(zobrist_int64(board)) == key
                assert board.fen().split()[:4] == fens[key].split()[:4]
                visited.add(key)
                diagnostics["arrivals"] += 1
                counts["arrivals"] += 1
                outcome = board.outcome(claim_draw=False)
                end = train["endings"].get(key, (0, 0, 0))
                end_n = sum(end)
                moves_n = sum(train["moves"].get(key, {}).values())
                repeated = cfg.repetition_visits is not None and history.count(key)+1 >= cfg.repetition_visits
                history.append(key)
                if outcome is not None:
                    reason = "terminal"
                    vector = (0, 1, 0) if outcome.winner is None else (1, 0, 0) if outcome.winner else (0, 0, 1)
                    cp = (-100000 if board.turn else 100000) if board.is_checkmate() else 0
                elif repeated:
                    reason, vector, cp = "repetition", (0, 1, 0), None
                elif end_n and not moves_n:
                    reason, vector, cp = "human_end", tuple(x/end_n for x in end), None
                elif ply == cfg.horizon:
                    reason, vector, cp = "horizon", wdl(train["outcomes"].get(key, (0, 0, 0)), c4), None
                elif uniform(seed, trace["index"], ply, "end") < (end_n/(end_n+moves_n) if end_n+moves_n else 0):
                    reason, vector, cp = "human_end", tuple(x/end_n for x in end), None
                else:
                    reason = None
                if reason:
                    assert ply == len(trace["moves"])
                    assert trace["reason"] == reason and trace["wdl"] == list(vector) and trace["exact_cp"] == cp
                    counts[reason] += 1
                    diagnostics[reason] += 1
                    break
                assert ply < len(trace["moves"])
                diagnostics["decisions"] += 1
                diagnostics["decisions_without_training_moves"] += not moves_n
                legal, probabilities = kernels.get(key, board.fen())
                r = cfg.recall_by_position.get(key, cfg.recall) if board.turn and key in book and (active or cfg.reentry) else 0
                if uniform(seed, trace["index"], ply, "recall") < r:
                    chosen, active = book[key], True
                else:
                    u, total, chosen = uniform(seed, trace["index"], ply, "move"), 0, None
                    for san in sorted(legal):
                        total += probabilities[model][san]
                        if u < total:
                            chosen = san
                            break
                    assert chosen is not None
                    if board.turn:
                        active = cfg.reentry
                assert chosen == trace["moves"][ply]
                move = board.parse_san(chosen)
                assert move in board.legal_moves and move.uci() == legal[chosen]
                board.push(move)
            counts["trajectories"] += 1
        arm_diagnostics[arm] = dict(diagnostics)
    assert visited == set(fens) and len(visited) <= c[f"max_{phase}_positions"]
    return {"counts": dict(counts), "visited_positions": len(visited), "arms": arm_diagnostics}


def main():
    started = time.monotonic()
    c = json.loads(CONFIG.read_text())
    pilot = verified_pilot(c)
    base = base_inputs(c)
    result = json.loads(output_path("m5/results.json").read_text())
    assert result["identity"]["pilot_manifest_sha256"] == sha256(output_path("m5/pilot_manifest.json"))
    assert result["identity"]["engine_audit_sha256"] == sha256(output_path("m5/engine_audit.json"))
    for path, digest in result["snapshots"].items():
        assert sha256(output_path(path)) == digest
    for source in pilot["eval_sources"]:
        assert fingerprint(source["path"], full=Path(source["path"]).stat().st_size < 1000000) == source
    assert result["eval_sources"] == pilot["eval_sources"]
    audit = json.loads(output_path("m5/engine_audit.json").read_text())
    selection = json.loads(output_path("m5/engine_selection.json").read_text())
    assert audit["complete"] and audit["identity"]["pilot_manifest_sha256"] == result["identity"]["pilot_manifest_sha256"]
    assert set(audit["base"]) == set(selection["targets"])
    assert set(audit["deeper"]) == set(selection["deeper"])
    assert selection["deeper"] == selection["targets"][:c["engine_deeper_positions"]]
    assert datetime.fromisoformat(pilot["frozen_utc"]) < datetime.fromisoformat(audit["frozen_utc"]) < datetime.fromisoformat(result["completed_utc"])
    phases, saved_paths = {}, {}
    for phase in ("pilot", "final"):
        paths = json.loads(output_path(f"m5/{phase}_traces.json").read_text())
        fens = json.loads(output_path(f"m5/{phase}_fens.json").read_text())
        saved_paths[phase] = paths
        phases[phase] = replay_phase(paths, fens, phase, base, c)
        if phase == "pilot":
            source = json.loads(output_path("m5/pilot_source_evals.json").read_text())
            reconstructed = select_missing_targets(paths, source, base[5], fens, Config(**c["config"]), c["engine_positions"])
            assert all(selection[k] == v for k, v in reconstructed.items())
            assert all(source.get(k) is None and base[5].get(k) is None for k in selection["targets"])
    source = json.loads(output_path("m5/final_source_evals.json").read_text())
    new = {k: v["cp"] for k, v in {**audit["base"], **audit["deeper"]}.items()}
    maps = {"source": source, "audited_overlay": {**source, **base[5], **new}, "audited_only": {**base[5], **new}}
    cfg, metric_checks, paths = Config(**c["config"]), 0, saved_paths["final"]
    coverage = {}
    for arm, traces in paths.items():
        before = sum(reconstruct(t, {**source, **base[5]}, cfg)["unresolved_first_hit_mass"] for t in traces)/len(traces)
        after = result["summaries"]["audited_overlay"][arm]["metrics"]["unresolved_first_hit_mass"]["mean"]
        coverage[arm] = {"m4_overlay_unresolved_on_final": before, "m5_overlay_unresolved_on_final": after,
                         "additional_m5_resolved_fraction": before-after}
    for evidence, evals in maps.items():
        rows = {}
        for arm, traces in paths.items():
            rows[arm] = []
            for trace in traces:
                actual, independent = score_trace(trace, evals, cfg), reconstruct(trace, evals, cfg)
                assert all(actual[k] == v for k, v in independent.items())
                assert independent["terminal_mass"]+independent["human_end_mass"]+independent["repetition_mass"]+independent["horizon_mass"] == 1
                rows[arm].append(independent)
                metric_checks += len(independent)
            assert arm_summary(rows[arm], c["monte_carlo_alpha"]) == result["summaries"][evidence][arm]
        for model in base[4]:
            a, b = model+"/two_cards", model+"/source_one_card"
            assert paired_summary(rows[a], rows[b], paths[a], paths[b], c["monte_carlo_alpha"]) == result["contrasts"][evidence][model]
    controlled = json.loads(output_path("m5/control.json").read_text())
    assert controlled["status"] == "passed"
    control_checks = [v for arm in controlled["arms"].values() for v in arm["checks"].values()]
    assert all(v["error"] <= v["tolerance"] for v in control_checks)
    marker = lambda cp: 1 if cp >= cfg.threshold_cp else -1 if cp <= -cfg.threshold_cp else 0
    differences = [abs(v["cp"]-audit["base"][k]["cp"]) for k, v in audit["deeper"].items()]
    work = {}
    for phase in ("base", "deeper"):
        values = list(audit[phase].values())
        work[phase] = {"positions": len(values), "nodes": sum(v["nodes"] for v in values),
            "engine_seconds": sum(v["seconds"] for v in values), "min_depth": min(v["depth"] for v in values),
            "median_depth": statistics.median(v["depth"] for v in values), "max_depth": max(v["depth"] for v in values),
            "interrupted_final_bound": sum(v["final_score_was_bound"] for v in values)}
    output = {"status": "passed", "result_sha256": sha256(output_path("m5/results.json")),
        "verifier_sha256": sha256(ROOT/"research/human_repertoire/verify_rollout.py"),
        "phases": phases, "independent_metric_checks": metric_checks, "reconstructed_summaries": 18,
        "reconstructed_paired_contrasts": 6, "control_metric_checks": len(control_checks),
        "control_max_error_over_tolerance": max(v["error"]/v["tolerance"] for v in control_checks),
        "engine_work": work, "deeper_median_cp_difference": statistics.median(differences),
        "deeper_max_cp_difference": max(differences),
        "deeper_marker_changes": sum(marker(v["cp"]) != marker(audit["base"][k]["cp"]) for k, v in audit["deeper"].items()),
        "new_target_markers": dict(Counter(marker(v) for v in new.values())),
        "post_experiment_coverage_diagnostic": coverage,
        "elapsed_seconds": time.monotonic()-started, "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m5/verification.json", output)
    print(json.dumps({k: output[k] for k in ("status", "independent_metric_checks", "engine_work", "deeper_marker_changes", "elapsed_seconds")}, indent=2))


if __name__ == "__main__":
    main()
