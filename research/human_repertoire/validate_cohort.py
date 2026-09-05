"""Run milestone-2 calibration and tiny fixed-book comparisons on frozen cohorts.

python -B -m research.human_repertoire.validate_cohort
No external data reads, engine jobs or large models. January chooses books;
February supplies separate predictive checks and a common re-estimated kernel.
The latter is a validation-model prediction, not a causal holdout book score.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, replace
from datetime import datetime, timezone
from importlib.metadata import version
import json
from pathlib import Path
import platform
import subprocess
import time

from .cohort import CONFIG
from .empirical import (audit_metrics, build_graph, candidates, combine, move_diagnostics,
                        observed_hits, outcome_diagnostics, player_component_bootstrap, stats)
from .io import ROOT, output_path, sha256, write_json
from .model import Config, book_cost, eligible, evaluate, feasible, graph_dict, objective
from .oracle import enumerate_books, path_evaluate


def choose(books, values, g, c, budget, allowed=None):
    best = None
    for book in books:
        if book_cost(g, book) > budget or (allowed is not None and any(move != allowed.get(k) for k, move in book.items())):
            continue
        m = values[tuple(sorted(book.items()))]
        if feasible(m, c) and (best is None or objective(m, book_cost(g, book)) > objective(best[1], book_cost(g, best[0]))):
            best = book, m
    return best


def run(c):
    started = time.perf_counter()
    manifest = json.loads(output_path("m2/cohort_manifest.json").read_text())
    if manifest["identity"]["config"] != c:
        raise ValueError("config differs from frozen cohort")
    for p, digest in manifest["snapshots"].items():
        if sha256(output_path(p)) != digest:
            raise ValueError("cohort snapshot changed")
    games = {name: json.loads(output_path(f"m2/{name}_games.json").read_text()) for name in ("train", "validation")}
    evals = json.loads(output_path("m2/evals.json").read_text())
    root = games["train"][0]["positions"][0]["key"]
    ss = {k: stats(v) for k, v in games.items()}
    reference_path = output_path(c["reference_snapshot"])
    reference = {str(r["position_hash"]): r["best_move"] for r in json.loads(reference_path.read_text())
                 if str(r["position_hash"]) == root and r.get("best_move")}
    allowed = candidates(ss["train"], root, reference, c)
    graphs, audits = {}, {}
    for name in games:
        graphs[name], audits[name] = build_graph(ss[name], ss["train"], evals, allowed, root, c)
    base = Config(**c["config"])
    # Every study position must be observed in the validation frame for this
    # small comparison. Unsupported prescriptions get explicit frontiers.
    if audits["validation"]["unobserved_study_positions"]:
        raise ValueError("validation lacks a training-selected study position; report adequacy before choosing a different universe")
    allowed = {k: [m for m in moves if m in eligible(graphs["train"], base, k)] for k, moves in allowed.items()}
    ref = {k: m for k, m in reference.items() if m in allowed.get(k, [])}
    books = list(enumerate_books(graphs["train"], base, max(c["budgets"]), allowed))
    if len(books) > 1024:
        raise ValueError("book comparison cap exceeded")
    graph_paths = {f"m2/{name}_graph.json": {"graph": graph_dict(g), "allowed": allowed, "audit": audits[name]} for name, g in graphs.items()}
    graph_hashes = {p: sha256(write_json(p, data)) for p, data in graph_paths.items()}
    print(f"Frozen {len(books)} feasible candidate books; graphs " + str({k: len(g.nodes) for k, g in graphs.items()}), flush=True)
    cache = {}
    def score(name, book, recall):
        if recall == 0 and not base.recall_by_position:
            book = {}  # all cards have exactly zero effect under this scenario
        key = name, tuple(sorted(book.items())), recall
        if key not in cache:
            if time.perf_counter()-started > 240:
                raise RuntimeError("bounded run exceeded 240 seconds; no completed result claimed")
            cache[key] = evaluate(graphs[name], book, replace(base, recall=recall))
            audit_metrics(cache[key])
        return cache[key]
    train_values = {}
    for recall in sorted(set(c["recalls"] + [0, 1])):
        train_values[recall] = {tuple(sorted(b.items())): score("train", b, recall) for b in books}
        print(f"Training evaluations complete: recall={recall}; {time.perf_counter()-started:.1f}s", flush=True)
    def scenario_values(recall, mode):
        if mode == "independent":
            return train_values[recall]
        return {key: combine([(recall, train_values[1][key]), (1-recall, train_values[0][key])]) for key in train_values[0]}
    def scenario_score(name, book, recall, mode):
        return score(name, book, recall) if mode == "independent" else combine([(recall, score(name, book, 1)), (1-recall, score(name, book, 0))])
    def arm(book, recall, mode):
        if book is None:
            return None
        values = {name: scenario_score(name, book, recall, mode) for name in games}
        return {"book": book, "cost": book_cost(graphs["train"], book),
                "train": audit_metrics(values["train"]), "validation_kernel": audit_metrics(values["validation"]),
                "train_feasible": feasible(values["train"], base), "validation_feasible": feasible(values["validation"], base)}
    rows = []
    selected = set()
    for mode in ("independent", "session_correlated"):
        for recall in c["recalls"]:
            values = scenario_values(recall, mode)
            for budget in c["budgets"]:
                best = choose(books, values, graphs["train"], base, budget)
                source = choose(books, values, graphs["train"], base, budget, ref)
                perfect = choose(books, train_values[1], graphs["train"], base, budget)
                plans = {"train_optimal": best[0] if best else None,
                         "source_moves_only": source[0] if source else None,
                         "reference_projection": ref if len(ref) <= budget else {},
                         "perfect_plan_replayed": perfect[0] if perfect else None, "empty": {}}
                for b in plans.values():
                    if b is not None:
                        selected.add(tuple(sorted(b.items())))
                rows.append({"mode": mode, "recall": recall, "budget": budget,
                             "resolved_config": asdict(replace(base, recall=recall)),
                             "arms": {name: arm(b, recall, mode) for name, b in plans.items()}})
    print(f"Evaluated frozen selected books on validation; {time.perf_counter()-started:.1f}s", flush=True)
    # Independently sum complete paths for every selected book under a nontrivial
    # recall setting in both cohorts. Other recall/ending cases have tiny oracles.
    oracle_checks = 0
    for name in games:
        for book_tuple in sorted(selected):
            b = dict(book_tuple)
            m = score(name, b, .85)
            oracle = path_evaluate(graphs[name], b, replace(base, recall=.85))
            if any(abs(getattr(m, k)-getattr(oracle, k)) > 1e-9 for k in asdict(m)):
                raise AssertionError("real complete-path oracle mismatch")
            oracle_checks += 1
    print(f"Independent path checks passed: {oracle_checks}", flush=True)
    diagnostic, decision_rows = move_diagnostics(ss["train"], games["validation"], c)
    outcome, outcome_rows = outcome_diagnostics(games["train"], games["validation"], c)
    outcome["points_error_uncertainty"] = player_component_bootstrap(outcome_rows, "points_error")
    # Fit one same-recipe move model per event on training only; compare its
    # predictive diagnostics on the corresponding validation games.
    event_diagnostics = {}
    for event in ("Blitz", "Rapid", "Classical"):
        training = [g for g in games["train"] if g["event"] == event]
        validation = [g for g in games["validation"] if g["event"] == event]
        event_diagnostics[event] = move_diagnostics(stats(training), validation, c)[0]["pooled"]
    # Evaluate the already-frozen pooled .85 independent plans on each validation
    # stratum. No per-stratum re-optimisation or changed candidate universe.
    stratum_predictions = {}
    frozen = next(r for r in rows if r["mode"] == "independent" and r["recall"] == .85 and r["budget"] == 2)
    for event in ("Blitz", "Rapid", "Classical"):
        g, audit = build_graph(stats([g for g in games["validation"] if g["event"] == event]), ss["train"], evals, allowed, root, c)
        predictions = {}
        for name in ("train_optimal", "reference_projection", "empty"):
            plan = frozen["arms"][name]
            if plan is not None and not audit["unobserved_study_positions"]:
                predictions[name] = audit_metrics(evaluate(g, plan["book"], replace(base, recall=.85)))
        stratum_predictions[event] = {"audit": audit, "predictions": predictions}
    selected_positions = {k: {"fen": ss["train"]["fens"][k], "training_move_counts": dict(ss["train"]["moves"][k]), "eligible_candidates": moves} for k, moves in allowed.items()}
    result = {"label": "MODEL PREDICTIONS AND DESCRIPTIVE VALIDATION; NOT HUMAN UPLIFT",
              "created_utc": datetime.now(timezone.utc).isoformat(),
              "code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "code_sha256": {p.name: sha256(p) for p in Path(__file__).parent.glob("*.py")},
              "python": platform.python_version(), "dependencies": {p: version(p) for p in ("chess", "numpy", "pyarrow")},
              "config": c, "cohort_manifest_sha256": sha256(output_path("m2/cohort_manifest.json")),
              "reference_sha256": sha256(reference_path), "graph_sha256": graph_hashes,
              "candidate_books": len(books), "selected_positions": selected_positions,
              "oracle_checks": oracle_checks, "rows": rows,
              "observed_hits": {name: {"pooled": observed_hits(v, evals), "by_event": {e: observed_hits([g for g in v if g["event"] == e], evals) for e in ("Blitz", "Rapid", "Classical")}} for name, v in games.items()},
              "move_diagnostics_pooled_model": diagnostic, "move_diagnostics_event_models": event_diagnostics,
              "outcome_diagnostics": outcome, "stratum_predictions": stratum_predictions,
              "validation_model_warning": "February outcomes/probabilities fit the February kernel. Books were chosen on January only. Kernel comparisons are model-based, not observed outcomes of following those books.",
              "elapsed_seconds": time.perf_counter()-started}
    write_json("m2/decision_diagnostics.json", decision_rows)
    write_json("m2/outcome_diagnostics.json", outcome_rows)
    write_json("m2/results.json", result)
    print(f"Completed {len(rows)} scenarios in {result['elapsed_seconds']:.1f}s", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    args = parser.parse_args()
    run(json.loads(args.config.read_text()))


if __name__ == "__main__":
    main()
