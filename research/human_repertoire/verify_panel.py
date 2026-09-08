"""Verify the M6 panel, complete engine maps, budget curve and depth sensitivity.

python -B -m research.human_repertoire.verify_panel
Reads bounded snapshots and source metadata only; no engine work or data scan.
"""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import time

import chess
from python.zobrist import zobrist_int64

from .io import ROOT, fingerprint, output_path, sha256, write_json
from .model import Config
from .paired_panel import (CONFIG, audit_identity, check_gates, inherited,
                           target_selection, verified_panel)
from .trajectory import arm_summary, paired_summary
from .verify_rollout import reconstruct, replay_phase


def independent_summary(paths, scores, cfg, alpha):
    rows = {arm: [reconstruct(t, scores, cfg) for t in traces] for arm, traces in paths.items()}
    arms = sorted(paths)
    for values in rows.values():
        for r in values:
            assert abs(r["win"]+r["draw"]+r["loss"]-1) < 1e-12
            assert r["advantage"]+r["disadvantage"]+r["no_hit"] == 1
            assert sum(r[k] for k in ("terminal_mass", "human_end_mass", "repetition_mass", "horizon_mass")) == 1
    return {"arms": {arm: arm_summary(values, alpha) for arm, values in rows.items()},
        "two_minus_one": paired_summary(rows[arms[1]], rows[arms[0]], paths[arms[1]], paths[arms[0]], alpha)}


def signature(trace, scores, cfg):
    for ply, key in enumerate(trace["keys"]):
        cp = trace["exact_cp"] if ply == len(trace["keys"])-1 and trace.get("exact_cp") is not None else scores[key]
        if abs(cp) >= cfg.threshold_cp:
            return (1 if cp > 0 else -1, ply)
    return (0, None)


def main():
    started = time.monotonic()
    c = json.loads(CONFIG.read_text())
    p = verified_panel(c)
    base, p5, inherited_all = inherited(c)
    paths = json.loads(output_path("m6/panel_traces.json").read_text())
    fens = json.loads(output_path("m6/panel_fens.json").read_text())
    engine_fens = json.loads(output_path("m6/engine_fens.json").read_text())
    prior = json.loads(output_path("m6/inherited_records.json").read_text())
    selection = json.loads(output_path("m6/engine_selection.json").read_text())
    audit = json.loads(output_path("m6/engine_audit.json").read_text())
    result = json.loads(output_path("m6/results.json").read_text())
    assert audit["identity"] == audit_identity(p) and audit["complete"] and result["complete"]
    assert result["identity"] == {"panel_manifest_sha256": sha256(output_path("m6/panel_manifest.json")),
                                  "engine_audit_sha256": sha256(output_path("m6/engine_audit.json"))}
    assert datetime.fromisoformat(p["frozen_utc"]) < datetime.fromisoformat(audit["frozen_utc"]) < datetime.fromisoformat(result["completed_utc"])
    for source in p["eval_sources"]:
        assert fingerprint(source["path"], full=Path(source["path"]).stat().st_size < 1000000) == source
    assert p["eval_sources"] == p5["eval_sources"]
    replay = replay_phase(paths, fens, "panel", base, c)
    gates = {k for g in p["gates"] for k in (g["position"], g["child"])}
    assert target_selection(paths, engine_fens, gates, inherited_all, c) == selection
    work, complete_records = {}, {}
    for phase, n, nodes, seconds in (("base", c["panel_trajectories_per_arm"], c["engine_nodes"], c["engine_time_seconds"]),
            ("deeper", c["deeper_pairs"], c["deeper_nodes"], c["deeper_time_seconds"])):
        arrivals = gates | {k for traces in paths.values() for t in traces[:n] for k in t["keys"]}
        required = {k for k in arrivals if chess.Board(engine_fens[k]).outcome(claim_draw=False) is None}
        assert required == set(selection[phase]["required"])
        assert set(selection[phase]["targets"]) == set(audit[phase])
        assert set(prior[phase]) == set(selection[phase]["reused"])
        assert not set(prior[phase]) & set(audit[phase])
        assert all(prior[phase][k] == inherited_all[phase][k] for k in prior[phase])
        complete_records[phase] = {**prior[phase], **audit[phase]}
        assert set(complete_records[phase]) == required
        for key, r in complete_records[phase].items():
            assert r["limits"] == {"nodes": nodes, "seconds": seconds}
            assert str(zobrist_int64(chess.Board(r["fen"]))) == key
            assert r["fen"].split()[:4] == engine_fens[key].split()[:4]
            assert 0 < r["score_nodes"] <= r["nodes"] and r["depth"] > 0
            board = chess.Board(r["fen"])
            for uci in r["pv"]:
                move = chess.Move.from_uci(uci)
                assert move in board.legal_moves
                board.push(move)
        new = list(audit[phase].values())
        work[phase] = {"required_positions": len(required), "new_positions": len(new), "reused_positions": len(prior[phase]),
            "new_nodes": sum(r["nodes"] for r in new), "new_engine_seconds": sum(r["seconds"] for r in new),
            "new_interrupted_final_bound": sum(r["final_score_was_bound"] for r in new),
            "new_min_depth": min(r["depth"] for r in new), "new_median_depth": statistics.median(r["depth"] for r in new),
            "new_max_depth": max(r["depth"] for r in new),
            "new_time_at_least_limit": sum(r["seconds"] >= seconds for r in new)}
    cfg, alpha = Config(**c["config"]), c["monte_carlo_alpha"]
    maps = {phase: {k: r["cp"] for k, r in records.items()} for phase, records in complete_records.items()}
    source = json.loads(output_path("m6/source_evals.json").read_text())
    overlay = json.loads(output_path("m6/inherited_overlay.json").read_text())
    for evidence, scores in (("source_only", source), ("inherited_mixed_overlay", {**source, **overlay}), ("base_100k", maps["base"])):
        assert independent_summary(paths, scores, cfg, alpha) == result["summaries"][evidence]
    subset = {arm: traces[:c["deeper_pairs"]] for arm, traces in paths.items()}
    for evidence, phase in (("base_100k", "base"), ("deeper_400k", "deeper")):
        assert independent_summary(subset, maps[phase], cfg, alpha) == result["deeper_subset"][evidence]
        for arm in result["deeper_subset"][evidence]["arms"].values():
            assert arm["metrics"]["unknown_eval_mass"]["mean"] == arm["metrics"]["unresolved_first_hit_mass"]["mean"] == 0
    for arm in result["summaries"]["base_100k"]["arms"].values():
        assert arm["metrics"]["unknown_eval_mass"]["mean"] == arm["metrics"]["unresolved_first_hit_mass"]["mean"] == 0
    for row in result["budget_curve"]:
        scores = {k: r["cp"] for k, r in prior["base"].items()}
        scores.update({k: audit["base"][k]["cp"] for k in selection["base"]["targets"][:row["new_positions"]]})
        assert independent_summary(paths, scores, cfg, alpha) == {k: v for k, v in row.items() if k != "new_positions"}
    for phase, scores in maps.items():
        assert check_gates(p["gates"], scores, cfg) == result["gates"][phase]
    marker = lambda cp: 1 if cp >= cfg.threshold_cp else -1 if cp <= -cfg.threshold_cp else 0
    changed_positions = [{"position": k, "base_cp": maps["base"][k], "deeper_cp": v} for k, v in maps["deeper"].items()
                         if marker(maps["base"][k]) != marker(v)]
    diffs = [abs(maps["base"][k]-v) for k, v in maps["deeper"].items()]
    sensitivity = {}
    for arm, traces in subset.items():
        events = [(signature(t, maps["base"], cfg), signature(t, maps["deeper"], cfg)) for t in traces]
        sensitivity[arm] = {"pairs_in_subset": len(traces), "changed_event_or_ply": sum(a != b for a, b in events),
            "changed_event_sign": sum(a[0] != b[0] for a, b in events)}
    panel_markers = {phase: dict(Counter(marker(v) for v in scores.values())) for phase, scores in maps.items()}
    verification = {"status": "passed", "result_sha256": sha256(output_path("m6/results.json")),
        "verifier_sha256": sha256(ROOT/"research/human_repertoire/verify_panel.py"), "replay": replay,
        "reconstructed_comparisons": 5+len(result["budget_curve"]), "engine_work": work,
        "position_markers": panel_markers, "deeper_marker_changes": changed_positions,
        "deeper_median_abs_cp_change": statistics.median(diffs), "deeper_max_abs_cp_change": max(diffs),
        "deeper_path_sensitivity": sensitivity, "elapsed_seconds": time.monotonic()-started,
        "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m6/verification.json", verification)
    print(json.dumps({k: verification[k] for k in ("status", "engine_work", "deeper_marker_changes", "deeper_path_sensitivity", "elapsed_seconds")}, indent=2))


if __name__ == "__main__":
    main()
