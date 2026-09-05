"""Audit frozen M3 results and parity with the unchanged M2 move diagnostic.

python -B -m research.human_repertoire.verify_shared_fallback
Reads local snapshots only; writes _artifacts/m3/verification.json.
"""
import json
from math import isclose

from .date_cohort import CONFIG
from .empirical import move_diagnostics, stats
from .io import output_path, sha256, write_json
from .shared_fallback import identity, inputs


def main():
    c = json.loads(CONFIG.read_text())
    frozen_path = output_path("m3/frozen_model.json")
    frozen = json.loads(frozen_path.read_text())
    result_path = output_path("m3/test_results.json")
    results = json.loads(result_path.read_text())
    assert frozen["identity"] == identity(c)
    assert results["frozen_model_sha256"] == sha256(frozen_path)
    for name, digest in (("fitted_counts.json", frozen["fitted_counts_sha256"]),
                         ("tuning_scores.json", frozen["tuning_scores_sha256"]),
                         ("test_game_scores.json", results["test_game_scores_sha256"])):
        assert sha256(output_path(f"m3/{name}")) == digest, name
    games = inputs(c, ("train", "tune", "test"))
    ids = [g["game_id"] for cohort in games.values() for g in cohort]
    assert len(ids) == len(set(ids))
    for cohort in games.values():
        assert all(min(g["white_elo"], g["black_elo"]) >= c["min_rating"] and
                   abs(g["white_elo"]-g["black_elo"]) <= c["max_rating_gap"] for g in cohort)
    reference, _ = move_diagnostics(stats(games["train"]), games["test"], {"move_prior_strength": 1})
    actual = results["summary"]["milestone2_position"]
    errors = {}
    for group, old in [("pooled", reference["pooled"])] + [(f"event:{e}", x) for e, x in reference["by_event"].items()]:
        errors[group] = {metric: actual[group][metric]-old[metric] for metric in
            ("log_loss", "brier", "decisions", "unseen_position", "unseen_move", "thin_position")}
        assert all(abs(x) < 1e-10 for x in errors[group].values()), errors
    rows = json.loads(output_path("m3/test_game_scores.json").read_text())
    assert {r["game_id"] for r in rows} == {g["game_id"] for g in games["test"]}
    for name, groups in results["summary"].items():
        for group, selected in [("pooled", rows)] + [(f"event:{e}", [r for r in rows if r["event"] == e]) for e in c["events"]]:
            n = sum(r["models"][name].get("decisions", 0) for r in selected)
            assert n == groups[group]["decisions"]
            for metric in ("log_loss", "brier", "predicted_unseen_move_mass"):
                expected = sum(r["models"][name].get(metric, 0) for r in selected)/n
                assert isclose(expected, groups[group][metric], abs_tol=1e-10)
    audit = {"status": "passed", "test_results_sha256": sha256(result_path),
        "previous_diagnostic_differences": errors,
        "game_counts": {k: len(v) for k, v in games.items()},
        "decisions": {k: sum(len(g["moves"]) for g in v) for k, v in games.items()},
        "zero_decision_games": {k: sum(not g["moves"] for g in v) for k, v in games.items()},
        "observed_endings": {k: sum(g["observed_end"] for g in v) for k, v in games.items()},
        "rating_range": {k: [min(min(g["white_elo"], g["black_elo"]) for g in v), max(max(g["white_elo"], g["black_elo"]) for g in v)] for k, v in games.items()},
        "verification_code_sha256": sha256(__file__)}
    write_json("m3/verification.json", audit)
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
