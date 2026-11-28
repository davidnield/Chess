"""Screen-then-confirm scoring for the extension comparison (David, 27 Nov 2026; idea from the SiegeChess race).

Off by default. Only a manifest written with ``sweep.spec --screening`` (v6) carries the rule, in
its ``extension_comparison.evaluation``; the v5 manifest and its four-seeds-for-every-variant
evaluation are unchanged.

The rule, fixed before any screening score is read:

- **Screen.** Score every variant on the screening seeds (1, 2) only. Compare it with the best
  variant of its family (the runs on the same base arm): the leader is the highest mean paired score
  on those seeds, ties to the smaller run id. Per trajectory, the variant's score minus the leader's
  on the same paired random stream, pooled over both seeds; SE = sd / sqrt(n).
- **Drop** a variant whose mean trails the leader by more than ``z`` x SE + ``tie_margin``.
  The leader and the family's control (default metric at the default threshold, the current
  extension) are never dropped.
- **Confirm.** The survivors get the confirmation seeds (3-6), disjoint from the screening seeds.
  Reported paired scores use those seeds only, so nothing selected on seeds 1-2 is scored on them.
- A dropped variant keeps everything its build produced; its score is labelled a screening result.

This only decides which variants the referee scores again. It cuts referee time, never a build.
"""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path

SCREENING = {"name": "screen-then-confirm", "screen_seeds": [1, 2], "confirm_seeds": [3, 4, 5, 6],
             "z": 2.33, "tie_margin": 0.001, "family": "base_arm",
             "leader": "highest-mean-on-screen-seeds-ties-to-smaller-run-id",
             "se": "per-trajectory-paired-differences-pooled-over-screen-seeds",
             "never_dropped": ["family-leader", "default-metric-at-default-threshold"],
             "reported_scores": "confirm-seeds-only",
             "dropped_variants": "build-outputs-kept-score-labelled-screening"}


def control_run(comparison, base_arm, default_metric, default_threshold):
    """The family's control: the default metric at the default threshold, with default severity."""
    found = [r["id"] for r in comparison["runs"] if r["recipe"]["base_arm"] == base_arm
             and r["role"] == "default-calibration"
             and r["recipe"]["extensions"]["metric"] == default_metric
             and r["recipe"]["extensions"]["threshold"] == default_threshold]
    if len(found) != 1:
        raise ValueError(f"family {base_arm} has {len(found)} default-calibration controls, want 1")
    return found[0]


def _mean_se(values):
    n = len(values)
    if n < 2:
        raise ValueError("need at least two paired trajectories")
    mean = math.fsum(values) / n
    var = math.fsum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(var / n)


def screen(comparison, scores, *, default_metric, default_threshold, rule=None):
    """Decide which variants are confirmed.

    ``scores[run_id][seed]`` is the run's per-trajectory score vector on that seed. Within a family
    and seed every vector has the same length: trajectory i is the same paired random stream.
    """
    rule = rule or comparison.get("evaluation")
    if rule != SCREENING:
        raise ValueError("this comparison was not queued with the screening rule (manifest v6)")
    seeds = rule["screen_seeds"]
    if set(seeds) & set(rule["confirm_seeds"]):
        raise ValueError("confirmation seeds must be disjoint from the screening seeds")
    runs = {r["id"]: r for r in comparison["runs"]}
    if set(scores) != set(runs):
        missing, extra = sorted(set(runs) - set(scores)), sorted(set(scores) - set(runs))
        raise ValueError(f"screening needs every queued run and no other (missing {missing}, extra {extra})")
    for rid, by_seed in scores.items():
        if sorted(int(s) for s in by_seed) != sorted(seeds):
            raise ValueError(f"run {rid}: scores must be on the screening seeds {seeds} only")
    families = {}
    for rid, r in sorted(runs.items()):
        families.setdefault(r["recipe"]["base_arm"], []).append(rid)
    out = {}
    confirm, dropped = [], []
    for base, members in sorted(families.items()):
        vec = {rid: {int(s): [float(x) for x in v] for s, v in scores[rid].items()} for rid in members}
        for s in seeds:
            if len({len(vec[rid][s]) for rid in members}) != 1:
                raise ValueError(f"family {base}, seed {s}: unpaired trajectory counts")
        for rid in members:
            if any(not math.isfinite(x) for s in seeds for x in vec[rid][s]):
                raise ValueError(f"run {rid}: non-finite score")
        pooled = {rid: [x for s in seeds for x in vec[rid][s]] for rid in members}
        means = {rid: math.fsum(v) / len(v) for rid, v in pooled.items()}
        leader = min(members, key=lambda rid: (-means[rid], rid))
        control = control_run(comparison, base, default_metric, default_threshold)
        decisions = {}
        for rid in members:
            if rid == leader:
                d = dict(mean=means[rid], diff=0.0, se=0.0, decision="leader")
            else:
                diff, se = _mean_se([a - b for a, b in zip(pooled[rid], pooled[leader])])
                trails = -diff > rule["z"] * se + rule["tie_margin"]
                decision = "control" if rid == control else ("drop" if trails else "confirm")
                d = dict(mean=means[rid], diff=diff, se=se, decision=decision,
                         threshold=rule["z"] * se + rule["tie_margin"])
            decisions[rid] = d
            (dropped if d["decision"] == "drop" else confirm).append(rid)
        out[base] = dict(leader=leader, control=control, runs=decisions)
    return dict(schema="extension-screening-v1", rule=rule, families=out,
                confirm=sorted(confirm), dropped=sorted(dropped),
                confirm_seeds=rule["confirm_seeds"],
                referee_runs=dict(screen=len(runs) * len(seeds),
                                  confirm=len(confirm) * len(rule["confirm_seeds"]),
                                  unscreened=len(runs) * (len(seeds) + len(rule["confirm_seeds"]))))


def read_scores(path):
    """A parquet of run, seed, trajectory, score -> {run: {seed: [score by trajectory]}}."""
    import polars as pl
    frame = pl.read_parquet(path).sort(["run", "seed", "trajectory"])
    if frame.select(pl.struct("run", "seed", "trajectory").is_duplicated().any()).item():
        raise ValueError("duplicate (run, seed, trajectory) rows")
    out = {}
    for (run, seed), part in frame.group_by(["run", "seed"], maintain_order=True):
        if part["trajectory"].to_list() != list(range(part.height)):
            raise ValueError(f"run {run}, seed {seed}: trajectories must be 0..n-1")
        out.setdefault(run, {})[int(seed)] = part["score"].to_list()
    return out


def main():
    from .spec import DEFAULT_EXTENSION_METRIC, EXTENSIONS, validate
    from .stages import atomic_json, file_sha
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--manifest", type=Path, required=True, help="a v6 manifest (sweep.spec --screening)")
    p.add_argument("--scores", type=Path, required=True, help="parquet: run, seed, trajectory, score")
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    m = json.loads(a.manifest.read_text(encoding="utf8"))
    validate(m)
    result = screen(m["extension_comparison"], read_scores(a.scores),
                    default_metric=DEFAULT_EXTENSION_METRIC, default_threshold=EXTENSIONS["threshold"])
    result.update(manifest_sha256=file_sha(a.manifest), scores_sha256=file_sha(a.scores))
    if a.out.exists() and json.loads(a.out.read_text(encoding="utf8")) != result:
        raise SystemExit(f"{a.out} exists with a different decision; screening is decided once")
    atomic_json(a.out, result)
    print(json.dumps(dict(confirm=len(result["confirm"]), dropped=len(result["dropped"]),
                          referee_runs=result["referee_runs"]), indent=2))


if __name__ == "__main__":
    main()
