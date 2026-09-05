"""Run the resolved synthetic grid and optional frozen real example.

  python -m research.human_repertoire.experiments --synthetic-only
  python -m research.human_repertoire.experiments

Use the worktree-local venv interpreter. Real mode requires extract_real first;
this command never reads external chess data. All results go to _artifacts/runs.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, replace
from datetime import datetime, timezone
from importlib.metadata import version
from itertools import product
import json
from math import exp, isclose
from pathlib import Path
import platform
import subprocess
import time

from .fixtures import demanding, fallback_skill, shared_reentry
from .io import DEFAULT_CONFIG, ROOT, output_path, sha256, write_json
from .model import Config, Graph, book_cost, eligible, evaluate, feasible, graph_dict, graph_from_dict, optimise
from .oracle import oracle_best, path_evaluate


def describe(g, c, book):
    if book is None:
        return None
    m = evaluate(g, book, c)
    oracle = path_evaluate(g, book, c)
    for k in asdict(m):
        if not isclose(getattr(m, k), getattr(oracle, k), abs_tol=1e-10):
            raise AssertionError(f"path evaluator mismatch: {k}")
    assert isclose(m.win + m.draw + m.loss, 1, abs_tol=1e-10)
    assert isclose(m.advantage + m.disadvantage + m.no_hit, 1, abs_tol=1e-10)
    assert isclose(m.terminal_mass + m.frontier_mass + m.horizon_mass + m.repetition_mass, 1, abs_tol=1e-10)
    return {"book": book, "cost": book_cost(g, book), "metrics": m.report(), "feasible": feasible(m, c),
            "selected_gate_cp": {k: next(e.sound_cp for e in g.nodes[k].edges if e.move == move) for k, move in book.items()}}


def verified_optimum(g, c, budget, allowed=None):
    s = optimise(g, c, budget, allowed)
    oracle = oracle_best(g, c, budget, allowed)
    if s is None:
        assert oracle is None
        return None
    assert oracle is not None and s.cost <= budget
    assert abs(s.metrics.early_advantage - oracle[2].early_advantage) < 1e-10
    assert abs(s.metrics.points - oracle[2].points) < 1e-10
    assert s.cost == -oracle[0][2]
    return s


def synthetic(cfg):
    g = demanding()
    source = {"root": "demanding", "d1": "remember1", "d2": "remember2"}
    source_allowed = {k: (m,) for k, m in source.items()}
    rows = []
    for skill, opponent_error, recall, budget in product(cfg["fallback_success"], cfg["opponent_payoff_error"], cfg["recalls"], cfg["budgets"]):
        c = Config(**cfg["base"], recall=recall, fallback=fallback_skill(g, skill),
                   opponent={"payoff": {"error": opponent_error, "defend": 1-opponent_error}})
        opt = verified_optimum(g, c, budget)
        source_opt = verified_optimum(g, c, budget, source_allowed)
        perfect = verified_optimum(g, replace(c, recall=1), budget)
        prefix = dict(list(source.items())[:budget])
        arms = {"exact": opt.book if opt else None, "source_only_exact": source_opt.book if source_opt else None,
                "source_prefix": prefix, "perfect_plan_replayed": perfect.book if perfect else None, "empty": {}}
        rows.append({"fallback_success": skill, "opponent_error": opponent_error, "recall": recall, "budget": budget,
                     "resolved_config": asdict(c), "evaluated_books": opt.evaluated_books if opt else None,
                     "arms": {name: describe(g, c, book) for name, book in arms.items()}})
    shared = shared_reentry()
    reentry = [{"reentry": enabled, **describe(shared, Config(reentry=enabled, discount=1), {"shared": "find"})} for enabled in (True, False)]
    return {"graph": graph_dict(g), "rows": rows, "shared_graph": graph_dict(shared), "reentry": reentry}


def tilted_policy(g, side, amount):
    """Uncalibrated skill/familiarity sensitivity: tilt observed frequencies.

    Favor smaller engine loss by exp(amount * clipped cp / 100), sign-adjusted
    for Black. Preserve support; unknown evals get neutral tilt.
    """
    result = {}
    for key, n in g.nodes.items():
        if n.side != side:
            continue
        sign = 1 if side == "us" else -1
        raw = {e.move: e.probability * exp(amount * max(-3, min(3, sign*(e.sound_cp or 0)/100))) for e in n.edges}
        total = sum(raw.values())
        result[key] = {m: x / total for m, x in raw.items()}
    return result


def real(cfg):
    provenance = json.loads(output_path("real/provenance.json").read_text())
    for relative, digest in provenance["snapshot_sha256"].items():
        if sha256(output_path(relative)) != digest:
            raise ValueError("frozen real snapshot has changed")
    if provenance["identity"]["config"] != cfg:
        raise ValueError("real config differs from frozen snapshot")
    data = json.loads(output_path("real/graph.json").read_text())
    g = graph_from_dict(data["graph"])
    allowed = data["allowed"]
    reference = data["reference"]
    rows = []
    for skill, opponent, recall, budget in product(cfg["fallback_engine_tilt"], cfg["opponent_engine_tilt"], cfg["recalls"], cfg["budgets"]):
        c = Config(**cfg["base"], recall=recall, fallback=tilted_policy(g, "us", skill), opponent=tilted_policy(g, "opp", opponent))
        opt = verified_optimum(g, c, budget, allowed)
        perfect = verified_optimum(g, replace(c, recall=1), budget, allowed)
        certified_reference = {k: m for k, m in reference.items() if m in eligible(g, c, k)}
        source = verified_optimum(g, c, budget, {k: (m,) for k, m in certified_reference.items()})
        # Projection has one root card in this snapshot; size remains explicit.
        fixed_reference = certified_reference if book_cost(g, certified_reference) <= budget else {}
        arms = {"exact": opt.book if opt else None, "source_only_exact": source.book if source else None,
                "reference_projection": fixed_reference, "perfect_plan_replayed": perfect.book if perfect else None, "empty": {}}
        rows.append({"fallback_engine_tilt": skill, "opponent_engine_tilt": opponent, "recall": recall, "budget": budget,
                     "resolved_config": asdict(c), "evaluated_books": opt.evaluated_books if opt else None,
                     "uncertified_reference": {k: m for k, m in reference.items() if k not in certified_reference},
                     "arms": {name: describe(g, c, book) for name, book in arms.items()}})
    # Horizon sensitivity changes only the evaluator over the same frozen graph.
    horizons = []
    for horizon in (1, 2, 3):
        c = Config(**{**cfg["base"], "horizon": horizon}, recall=.85)
        s = verified_optimum(g, c, 2, allowed)
        horizons.append({"horizon": horizon, "resolved_config": asdict(c), "exact": describe(g, c, s.book if s else None)})
    # Leaf W/D/L stress shifts points by at most .02 while cp/hit inputs are fixed.
    leaf_stress = []
    for delta in (-.02, 0, .02):
        nodes = {}
        for k, n in g.nodes.items():
            w, d, l = n.wdl
            shift = max(-w, min(l, delta)) if n.side == "frontier" else 0
            nodes[k] = replace(n, wdl=(w+shift, d, l-shift))
        stressed = Graph(g.root, nodes)
        c = Config(**cfg["base"], recall=.85)
        s = verified_optimum(stressed, c, 2, allowed)
        leaf_stress.append({"frontier_points_shift": delta, "exact": describe(stressed, c, s.book if s else None)})
    # A uniform error cannot test relative ranking. Also move the two main
    # alternatives' frontier points in opposite directions, excluding shared
    # descendants. This is a stated sensitivity scenario, not a confidence band.
    descendants = {}
    for edge in g.nodes[g.root].edges:
        pending, seen = [edge.child], set()
        while pending:
            key = pending.pop()
            if key in seen:
                continue
            seen.add(key)
            pending.extend(e.child for e in g.nodes[key].edges)
        descendants[edge.move] = seen
    relative_stress = []
    for delta in (-.01, 0, .01):
        nodes = {}
        for k, n in g.nodes.items():
            w, d, l = n.wdl
            direction = int(k in descendants.get("d4", set())) - int(k in descendants.get("Bb5", set()))
            shift = max(-w, min(l, delta * direction)) if n.side == "frontier" else 0
            nodes[k] = replace(n, wdl=(w+shift, d, l-shift))
        stressed = Graph(g.root, nodes)
        c = Config(**cfg["base"], recall=.85)
        s = verified_optimum(stressed, c, 2, allowed)
        relative_stress.append({"d4_points_shift_and_negative_Bb5_shift": delta, "exact": describe(stressed, c, s.book if s else None)})
    return {"rows": rows, "horizon_sensitivity": horizons, "leaf_stress": leaf_stress,
            "relative_leaf_stress": relative_stress, "provenance": provenance}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--synthetic-only", action="store_true")
    args = parser.parse_args()
    started = time.perf_counter()
    cfg = json.loads(args.config.read_text())
    result = {"schema_version": 1, "label": "MODEL PREDICTIONS; NOT HUMAN UPLIFT",
              "created_utc": datetime.now(timezone.utc).isoformat(), "experiment_config": cfg,
              "code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "code_sha256": {p.name: sha256(p) for p in sorted(Path(__file__).parent.glob("*.py"))},
              "python": platform.python_version(), "dependencies": {p: version(p) for p in ("chess", "numpy", "pyarrow")} if not args.synthetic_only else {},
              "synthetic": synthetic(cfg["synthetic"])}
    if not args.synthetic_only:
        result["real"] = real(cfg["real"])
    result["elapsed_seconds"] = time.perf_counter() - started
    path = write_json("runs/synthetic.json" if args.synthetic_only else "runs/milestone1.json", result)
    print(f"Verified {len(result['synthetic']['rows'])} synthetic configurations and {len(result.get('real', {}).get('rows', []))} real configurations")
    print(f"Saved {path}; elapsed {result['elapsed_seconds']:.2f}s")


if __name__ == "__main__":
    main()
