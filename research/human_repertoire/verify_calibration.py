"""Independent M7 replay, scalar-loss reconstruction and frozen-output audit.

python -B -m research.human_repertoire.verify_calibration
No engine requests or canonical scans.
"""
from collections import Counter
from datetime import datetime, timezone
import json
import statistics
import time

import chess
from python.zobrist import zobrist_int64

from .calibration_experiment import CONFIG, audit_identity, frozen, verified_cases
from .io import ROOT, output_path, sha256, write_json
from .loss_calibration import METRICS, select_cases, summaries, targets
from .shared_fallback import distribution, move_token
from .trajectory import uniform


def main():
    started = time.monotonic()
    c = json.loads(CONFIG.read_text())
    p = verified_cases(c)
    games, fitted, specs, old, _ = frozen(c)
    games_by_id = {g["game_id"]: g for g in games}
    cases = json.loads(output_path("m7/cases.json").read_text())
    fens = json.loads(output_path("m7/fens.json").read_text())
    selection = json.loads(output_path("m7/engine_selection.json").read_text())
    prior = json.loads(output_path("m7/inherited_records.json").read_text())
    audit = json.loads(output_path("m7/engine_audit.json").read_text())
    results = json.loads(output_path("m7/results.json").read_text())
    scored = json.loads(output_path("m7/scored_cases.json").read_text())
    assert audit["complete"] and audit["identity"] == audit_identity(p)
    assert results["identity"] == {"case_manifest_sha256": sha256(output_path("m7/case_manifest.json")),
                                   "engine_audit_sha256": sha256(output_path("m7/engine_audit.json"))}
    assert results["scored_cases_sha256"] == sha256(output_path("m7/scored_cases.json"))
    assert datetime.fromisoformat(p["frozen_utc"]) < datetime.fromisoformat(audit["frozen_utc"]) < datetime.fromisoformat(results["completed_utc"])
    selected, frame = select_cases(games, fitted, c)
    assert frame == json.loads(output_path("m7/selection_frame.json").read_text())
    assert len(selected) == len(cases)
    visited = set()
    for raw, case in zip(selected, cases):
        assert all(case[k] == v for k, v in raw.items())
        game = games_by_id[case["game_id"]]
        board = chess.Board()
        for san in ["e4", "e5", "Nf3", "Nc6"]+game["moves"][:case["ply"]]:
            board.push_san(san)
        assert board.fen().split()[:4] == case["fen"].split()[:4]
        assert str(zobrist_int64(board)) == case["parent"]
        assert game["moves"][case["ply"]] == case["human_san"]
        legal = sorted(board.legal_moves, key=lambda move: board.san(move))
        sans = [board.san(m) for m in legal]
        ps = distribution([m.uci() for m in legal], [move_token(board, m) for m in legal],
            fitted["positions"].get(case["parent"], {}), fitted["tokens"], specs[c["model"]])
        u, cumulative, chosen = uniform(c["draw_seed"], case["case_id"], 0, "move"), 0, None
        for san, probability in zip(sans, ps):
            cumulative += probability
            if u < cumulative:
                chosen = san
                break
        assert chosen == case["model_san"]
        for role in ("human", "model"):
            assert abs(ps[sans.index(case[role+"_san"])]-case[role+"_move_probability"]) < 1e-12
            child = board.copy()
            child.push_san(case[role+"_san"])
            assert str(zobrist_int64(child)) == case[role+"_child"]
            assert child.fen().split()[:4] == fens[case[role+"_child"]].split()[:4]
            visited.add(case[role+"_child"])
        assert case["human_child"] == game["positions"][case["ply"]+1]["key"]
        visited.add(case["parent"])
    assert visited == set(fens)
    assert targets(cases, fens, old, c) == selection
    maps, work = {}, {}
    for phase, nodes, seconds in (("base", c["engine_nodes"], c["engine_time_seconds"]), ("deeper", c["deeper_nodes"], c["deeper_time_seconds"])):
        assert set(audit[phase]) == set(selection[phase]["targets"])
        assert set(prior[phase]) == set(selection[phase]["reused"])
        assert not set(prior[phase]) & set(audit[phase])
        assert all(prior[phase][k] == old[phase][k] for k in prior[phase])
        records = {**prior[phase], **audit[phase]}
        assert set(records) == set(selection[phase]["required"])
        for key, r in records.items():
            assert r["limits"] == {"nodes": nodes, "seconds": seconds}
            board = chess.Board(r["fen"])
            assert str(zobrist_int64(board)) == key
            assert board.fen().split()[:4] == fens[key].split()[:4]
            assert 0 < r["score_nodes"] <= r["nodes"]
            for uci in r["pv"]:
                move = chess.Move.from_uci(uci)
                assert move in board.legal_moves
                board.push(move)
        maps[phase] = {**{k: r["cp"] for k, r in records.items()}, **selection[phase]["exact"]}
        new = list(audit[phase].values())
        work[phase] = {"required_positions": len(records), "reused_positions": len(prior[phase]), "new_positions": len(new),
            "new_nodes": sum(r["nodes"] for r in new), "new_engine_seconds": sum(r["seconds"] for r in new),
            "min_depth": min(r["depth"] for r in new), "median_depth": statistics.median(r["depth"] for r in new),
            "max_depth": max(r["depth"] for r in new), "interrupted_final_bound": sum(r["final_score_was_bound"] for r in new),
            "time_at_least_limit": sum(r["seconds"] >= seconds for r in new)}
    checks = 0
    for label, subset, rows, scores in (("base", cases, scored["base"], maps["base"]),
            ("subset_base", [r for r in cases if r["deeper"]], scored["deeper_subset"]["base"], maps["base"]),
            ("subset_deeper", [r for r in cases if r["deeper"]], scored["deeper_subset"]["deeper"], maps["deeper"])):
        assert len(subset) == len(rows)
        for case, row in zip(subset, rows):
            assert row["case_id"] == case["case_id"]
            parent = scores[case["parent"]]
            for role in ("human", "model"):
                child = scores[case[role+"_child"]]
                signed = parent-child if chess.Board(case["fen"]).turn else child-parent
                assert row[role+"_signed_loss_cp"] == signed
                assert row[role+"_loss_cp"] == max(0, signed)
                assert row[role+"_loss_primary"] == int(signed >= c["primary_loss_cp"])
                assert row[role+"_loss_secondary"] == int(signed >= c["secondary_loss_cp"])
                assert row[role+"_capped_loss_cp"] == min(c["mean_loss_cap_cp"], max(0, signed))
                checks += 5
    assert summaries(scored["base"], c) == results["summary"]
    for phase in ("base", "deeper"):
        assert summaries(scored["deeper_subset"][phase], c, bootstrap=False) == results["deeper_subset"][phase]
    # Separate aggregate arithmetic check, independent of the summary builder.
    for group in results["summary"]["groups"].values():
        for metric in METRICS:
            for role in ("human", "model"):
                stratum_means = []
                for s in group["strata"]:
                    values = [r[role+"_"+metric] for r in scored["base"] if r["stratum"] == s]
                    stratum_means.append(statistics.mean(values))
                assert abs(statistics.mean(stratum_means)-group["metrics"][metric][role]) < 1e-12
    sensitivity = {role: {metric: sum(a[role+"_"+metric] != b[role+"_"+metric] for a, b in zip(scored["deeper_subset"]["base"], scored["deeper_subset"]["deeper"]))
                         for metric in ("loss_primary", "loss_secondary")} for role in ("human", "model")}
    metadata = {}
    for s in frame["strata"]:
        rows = [r for r in cases if r["stratum"] == s]
        metadata[s] = {"cases": len(rows), "median_ply_after_root": statistics.median(r["ply"] for r in rows),
            "median_training_move_count": statistics.median(r["training_move_count"] for r in rows),
            "training_move_count_range": [min(r["training_move_count"] for r in rows), max(r["training_move_count"] for r in rows)],
            "white_to_move": sum(r["white_to_move"] for r in rows)}
    differences = [abs(v-maps["base"][k]) for k, v in maps["deeper"].items()]
    output = {"status": "passed", "result_sha256": sha256(output_path("m7/results.json")),
        "verifier_sha256": sha256(ROOT/"research/human_repertoire/verify_calibration.py"),
        "cases_replayed": len(cases), "independent_scalar_checks": checks,
        "engine_work": work, "deeper_category_changes": sensitivity,
        "deeper_median_abs_cp_change": statistics.median(differences), "deeper_max_abs_cp_change": max(differences),
        "selected_metadata": metadata, "source_frame": frame,
        "bootstrap_empty_draws": {name: group["empty_stratum_draws"] for name, group in results["summary"]["groups"].items()},
        "elapsed_seconds": time.monotonic()-started, "completed_utc": datetime.now(timezone.utc).isoformat()}
    write_json("m7/verification.json", output)
    print(json.dumps({k: output[k] for k in ("status", "engine_work", "deeper_category_changes", "elapsed_seconds")}, indent=2))


if __name__ == "__main__":
    main()
