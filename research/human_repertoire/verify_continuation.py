"""Audit frozen M4 outputs and partition the first source of uncertainty.

python -B -m research.human_repertoire.verify_continuation
Reads research snapshots only. No engine job or source scan.
"""
from collections import Counter
import json

from .common_graph import apply_evals
from .continuation_audit import CONFIG, verified_preparation
from .io import output_path, sha256, write_json
from .model import Config, eligible, graph_from_dict


def first_gap_partition(graph, book, cfg):
    """Absorb mass at a known first hit or the first observation gap.

    Only independent recall with re-entry is used by this frozen experiment.
    Sum of the three gap buckets must equal the evaluator's first-hit width.
    This classifies the first gap, not how much a particular repair will help.
    """
    if not cfg.reentry or cfg.fallback or cfg.opponent:
        raise ValueError("gap diagnostic expects the frozen re-entry kernels")
    totals = Counter()
    stack = [(graph.root, 1.0, ())]
    states = 0
    while stack:
        key, mass, path = stack.pop()
        states += 1
        if states > cfg.max_states:
            raise ValueError("gap diagnostic state cap")
        node = graph.nodes[key]
        if node.cp is None:
            totals["omitted_legal_move" if key.startswith("unknown:") else "missing_real_eval"] += mass
            continue
        if abs(node.cp) >= cfg.threshold_cp:
            totals["known_first_hit"] += mass
            continue
        repeated = cfg.repetition_visits is not None and path.count(key)+1 >= cfg.repetition_visits
        if node.side in {"terminal", "human_terminal"} or repeated or len(path) == cfg.horizon:
            totals["fully_observed_no_hit"] += mass
            continue
        if node.side == "frontier":
            totals["unexpanded_real_position"] += mass
            continue
        totals["fully_observed_no_hit"] += mass*node.end_probability
        mass *= 1-node.end_probability
        recall = cfg.recall_by_position.get(key, cfg.recall) if node.side == "us" and key in book else 0
        for edge in node.edges:
            p = (1-recall)*edge.probability+recall*(edge.move == book.get(key))
            if p and mass:
                stack.append((edge.child, mass*p, path+(key,)))
    if abs(sum(totals.values())-1) > 1e-9:
        raise AssertionError("gap partition mass not conserved")
    return {k: totals[k] for k in ("missing_real_eval", "omitted_legal_move", "unexpanded_real_position",
                                   "known_first_hit", "fully_observed_no_hit")}


def main():
    c = json.loads(CONFIG.read_text())
    prep = verified_preparation(c)
    result_path = output_path("m4/results.json")
    results = json.loads(result_path.read_text())
    engine = json.loads(output_path("m4/engine_audit.json").read_text())
    targets = json.loads(output_path("m4/engine_selection.json").read_text())
    assert results["identity"]["preparation_sha256"] == sha256(output_path("m4/preparation.json"))
    assert results["identity"]["engine_audit_sha256"] == sha256(output_path("m4/engine_audit.json"))
    assert engine["complete"]
    assert set(engine["base"]) == set(targets["targets"])
    assert set(engine["deeper"]) == set(targets["deeper"])
    assert set(targets["mandatory"]) <= set(engine["deeper"])
    templates = json.loads(output_path("m4/templates.json").read_text())
    source = json.loads(output_path("m4/source_evals.json").read_text())
    stored = json.loads(output_path("m4/graphs.json").read_text())
    graphs = {v: {n: graph_from_dict(g) for n, g in group.items()} for v, group in stored.items()}
    for group in graphs.values():
        a, b = group.values()
        assert a.nodes.keys() == b.nodes.keys()
        for k in a.nodes:
            assert [(e.move, e.child) for e in a.nodes[k].edges] == [(e.move, e.child) for e in b.nodes[k].edges]
    fresh_base = {k: v["cp"] for k, v in engine["base"].items()}
    fresh = {**fresh_base, **{k: v["cp"] for k, v in engine["deeper"].items()}}
    maps = {"source": source, "audited_overlay": {**source, **fresh}, "audited_only": fresh}
    cfg = Config(**c["config"])
    partitions = []
    for row in results["rows"]:
        if row["rejected_gates"]:
            continue
        variant, name = row["variant"], row["model"]
        g = apply_evals(graphs[variant][name], templates[variant], maps[row["evidence"]])
        p = first_gap_partition(g, row["book"], cfg)
        unresolved = sum(p[k] for k in ("missing_real_eval", "omitted_legal_move", "unexpanded_real_position"))
        assert abs(unresolved-row["metrics"]["unresolved_first_hit_mass"]) < 1e-9
        partitions.append({k: row[k] for k in ("variant", "model", "evidence", "plan")} | p)
    gates = []
    template, graph = templates["expanded"], graphs["expanded"]["tuned_position"]
    gate_maps = {"source": source, "base_budget": fresh_base, "deeper_budget": fresh}
    for key, move in sorted({(k, m) for b in prep["books"].values() for k, m in b.items()}):
        record = {"position": key, "move": move, "fen": template["fens"][key], "maps": {}}
        for name, evidence in gate_maps.items():
            g = apply_evals(graph, template, evidence)
            record["maps"][name] = {"eligible": move in eligible(g, cfg, key), "parent_cp": g.nodes[key].cp,
                "child_cp": next(e.sound_cp for e in g.nodes[key].edges if e.move == move)}
        gates.append(record)
    old = json.loads(output_path("m4_before_score_stream_fix/preparation.json").read_text())
    assert old["snapshots"] == prep["snapshots"]
    work = {}
    for phase in ("base", "deeper"):
        records = list(engine[phase].values())
        work[phase] = {"positions": len(records), "total_nodes": sum(v["nodes"] for v in records),
            "total_engine_seconds": sum(v["seconds"] for v in records),
            "depth_range": [min(v["depth"] for v in records), max(v["depth"] for v in records)],
            "interrupted_final_iterations": sum(v["final_score_was_bound"] for v in records),
            "max_recorded_seconds": max(v["seconds"] for v in records)}
    output = {"status": "passed", "results_sha256": sha256(result_path), "first_gap_partitions": partitions,
        "gates": gates, "engine_work": work, "initial_snapshot_hashes_unchanged": True,
        "verification_code_sha256": sha256(__file__)}
    write_json("m4/verification.json", output)
    print(json.dumps({"status": "passed", "partitions": len(partitions), "gates": gates, "engine_work": work}, indent=2))


if __name__ == "__main__":
    main()
