"""Approved recipes. Creating a manifest never starts a production job."""
from __future__ import annotations
import argparse
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path

BASELINES = {"white": 0.5183137990703003, "black": 0.5195669627673518}
GATES = {"stricter": {"white": .050, "black": .050, "relative": .060},
         "permissive": {"white": .110, "black": .075, "relative": .095}}
POLICY = {"name": "otb-ensemble-v1", "model": "maia3-79m-position",
          "ratings": [2600, 2600], "temperature": 1.35,
          "empirical_strength": 30., "maia_strength": 3., "otb_strength": 10.,
          "backup_discount": .10, "broad_discount": .01}
FIXED = {"key_bits": 128, "max_ply": 30, "source_plies": 51, "min_games": 1,
         "eval_weight": .75, "prior_strength": 500., "own_policy": "historical-1000-1799",
         "candidate_pool": "brc1800-p30", "engine_nominations": "shared-verified",
         "epsilon": .0005, "share_floor": .002, "incumbent_stickiness": 0.,
         "lookahead_floor": 0, "weak_min_games": 20., "grid_cap": 256, "match_inflate": 1.6,
         "verification": {"mode": "quality", "nodes": 5_000_000,
                          "deep_nodes": 20_000_000, "score_band": .01,
                          "independent_final_cloud": True, "no_quality_downgrade": True}}
REQUIRED_BINDINGS = ["explorer_receipt", "eval_lookup_receipt", "code_receipt",
                     "maia_checkpoint_receipt", "engine_receipt", "otb_split_receipt",
                     "weak_policy_receipt", "candidate_source_receipt"]
DEFAULT_EXTENSION_METRIC = "remaining-maximum"
EXTENSION_EXPOSURE = {
    "remaining-maximum": "reduction-in-expected-maximum-remaining-unbooked-risk",
    "covered-maximum": "expected-trajectory-maximum"}
EXTENSIONS = {"metric": DEFAULT_EXTENSION_METRIC, "threshold": .00003, "calibration": [.00001, .00003, .00010],
              "severity": "max(gap2,0.60*gap3)", "exposure": EXTENSION_EXPOSURE[DEFAULT_EXTENSION_METRIC],
              "cost": "new-distinct-prescriptions-including-connectors",
              "depth_cap": None, "reply_cutoff": None, "recall_discount": False,
              "unresolved_frontier": "must-bound-or-expand"}
EVALUATION = {"backend": "pending-Allie-validation", "primary_trajectories": 4096,
              "primary_seeds": [1,2,3,4], "finalist_trajectories": 32768,
              "finalist_seeds": list(range(101,109)), "minimum_horizon": 50,
              "extension_tail": 10, "finalist_extra_horizon": 20,
              "recall": {"half_life_days": [2,5,10], "multipliers": [1.6,2.,2.4],
                         "max_interval_days": 365, "cards_per_color_day": 30,
                         "finalist_cards": [15,60], "schedules": 8, "days": [30,90,240]}}

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)

def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()

def recipe(colour, gate, budget, mode="independent", **overrides):
    result = deepcopy(FIXED)
    result.update(colour=colour, gate=gate, budget=budget, mode=mode,
                  slice_prior=BASELINES[colour], robustness_floor=GATES[gate][colour],
                  max_score_loss=GATES[gate]["relative"], opponent_policy=deepcopy(POLICY),
                  solver="price+polish" if colour=="white" else "dp+match-distinct")
    result.update(overrides)
    return result

def arm(r, family="primary", depends=()):
    return {"id": digest(r)[:20], "family": family, "recipe": r,
            "depends_on": list(depends), "state": "not-started"}

def pool_recipes():
    bands = [0,1000,1200,1400,1600,1800,2000,2200,2500]
    definitions = [
        ("brc-all", ["Blitz","Rapid","Classical"], bands),
        ("brc1800", ["Blitz","Rapid","Classical"], [1800,2000,2200,2500]),
        ("brc2200", ["Blitz","Rapid","Classical"], [2200,2500]),
        ("rapid2200", ["Rapid"], [2200,2500]),
        ("classical1800", ["Classical"], [1800,2000,2200,2500]),
        ("weak1000-1799", ["Blitz","Rapid","Classical"], [1000,1200,1400,1600])]
    # These are game-mean construction bands, never mover-rating evaluation panels.
    return [dict(id=name+"-p"+str(cap), events=speeds, elo_bands=bands, max_ply=cap,
                 min_games=1, key_bits=128, rating_semantics="game-mean",
                 training_months=["2013-01","2026-07"])
            for cap in (30,50) for name,speeds,bands in definitions]

def extension_comparison(main):
    """Deferred calibration queue: four existing masters, shared evidence, both metrics."""
    anchors=[a for a in main if a["recipe"]["mode"]=="independent" and a["recipe"]["budget"]==2000]
    runs=[]
    for anchor in anchors:
        for threshold in EXTENSIONS["calibration"]:
            for metric in (DEFAULT_EXTENSION_METRIC,"covered-maximum"):
                settings={k:deepcopy(v) for k,v in EXTENSIONS.items() if k!="calibration"}
                settings.update(metric=metric,threshold=threshold,exposure=EXTENSION_EXPOSURE[metric])
                recipe=dict(base_arm=anchor["id"],extensions=settings)
                runs.append(dict(id=digest(recipe)[:20],recipe=recipe,depends_on=[anchor["id"]],
                                 role="default-calibration" if metric==DEFAULT_EXTENSION_METRIC else "legacy-control",
                                 state="queued-waiting-for-prerequisites",
                                 cli_args=["--metric",metric,"--threshold",str(threshold)]))
    return dict(schema=1,default_metric=DEFAULT_EXTENSION_METRIC,
                prerequisites=["inputs-verified","black-pilot-closed","four-anchors-quality-closed",
                               "extension-cost-pilot-passed"],
                shared_inputs=["base-book","engine-cache","opponent-policy-cache"],
                comparison_evidence="common-frozen-union-of-explored-continuations",
                separate_checkpoints=True,
                evaluation="paired-seeds-and-common-horizon",
                report=["additional-distinct-prescriptions","selected-move-disagreement",
                        "later-unbooked-risk","reach-weighted-coverage","paired-score",
                        "runtime-and-cache-reuse","unresolved-exposure-bounds"],runs=runs)


def manifest():
    main = []
    for colour in ("white","black"):
        for gate in GATES:
            independent = {n: arm(recipe(colour,gate,n)) for n in (500,1000,2000)}
            main.extend(independent.values())
            master = independent[2000]["id"]
            # Build reduced 1000 from 2000, then 500 from that 1000.
            # Publication/extension order is reversed: carry 500 into 1000 into 2000.
            r1000 = arm(recipe(colour,gate,1000,"reduced", master=master),
                        depends=[master])
            r500 = arm(recipe(colour,gate,500,"reduced", master=master,
                              allowed_parent=r1000["id"]), depends=[r1000["id"]])
            main += [r1000,r500]
    sensitivity = []
    for colour in ("white","black"):
        for gate in GATES:
            anchor = next(a for a in main if a["recipe"]["colour"]==colour
                          and a["recipe"]["gate"]==gate and a["recipe"]["budget"]==1000
                          and a["recipe"]["mode"]=="independent")
            for weight in (.50,1.):
                sensitivity.append(arm(recipe(colour,gate,1000,eval_weight=weight),
                                       "weight", [anchor["id"]]))
            for rating in (1800,2200):
                policy = dict(name=f"brc{rating}-maia30", model="maia3-79m-position",
                              ratings=[2200,2200],temperature=1.,pseudogames=30.)
                sensitivity.append(arm(recipe(colour,gate,1000,opponent_policy=policy),
                                       "policy",[anchor["id"]]))
            sensitivity.append(arm(recipe(colour,gate,1000,max_ply=50,
                                          candidate_pool="brc1800-p50"),"horizon",[anchor["id"]]))
    result = dict(schema=1, primary=main, sensitivities=sensitivity, inputs=pool_recipes(),
                  required_bindings=REQUIRED_BINDINGS, extensions=deepcopy(EXTENSIONS),
                  extension_comparison=extension_comparison(main),
                  evaluation=EVALUATION, pilot=next(a["id"] for a in main if
                  a["recipe"]["colour"]=="black" and a["recipe"]["gate"]=="stricter"
                  and a["recipe"]["budget"]==2000 and a["recipe"]["mode"]=="independent"),
                  release_gates=["inputs-verified", "policy-reproduced", "black-pilot-closed",
                                 "extension-calibrated", "evaluation-backend-frozen"])
    validate(result)
    return result

def validate(m):
    if m.get("schema") != 1 or len(m["primary"]) != 20:
        raise ValueError("expected schema 1 and exactly 20 primary arms")
    all_arms = m["primary"]+m["sensitivities"]
    ids = {a["id"] for a in all_arms}
    if len(ids)!=len(all_arms):
        raise ValueError("duplicate experiment recipes")
    for a in all_arms:
        r=a["recipe"]
        if a["id"] != digest(r)[:20] or set(a["depends_on"])-ids:
            raise ValueError("changed recipe or unresolved arm dependency")
        if r["min_games"]!=1 or r["key_bits"]!=128 or r["source_plies"]<=r["max_ply"]:
            raise ValueError("cutoff, key width, or source horizon violates the recipe")
        if r["slice_prior"]!=BASELINES[r["colour"]]:
            raise ValueError("absolute baseline drifted")
        if r["max_score_loss"]!=GATES[r["gate"]]["relative"] or r["robustness_floor"]!=GATES[r["gate"]][r["colour"]]:
            raise ValueError("gate settings drifted")
        if r["mode"]=="reduced" and r["budget"]==2000:
            raise ValueError("do not count the common master twice")
    # Historical manifests without this stage remain readable for reproducibility.
    if "extension_comparison" in m:
        if m["extensions"]!=EXTENSIONS or m["extension_comparison"]!=extension_comparison(m["primary"]):
            raise ValueError("extension default or comparison queue drifted")
    # Fail closed on cyclic dependencies.
    pending={a["id"]:set(a["depends_on"]) for a in all_arms}
    while pending:
        done={k for k,v in pending.items() if not v}
        if not done: raise ValueError("dependency cycle")
        pending={k:v-done for k,v in pending.items() if k not in done}
    canonical(m)

def evaluation_horizon(deepest_ply):
    if deepest_ply<0: raise ValueError("negative ply")
    return 2*math.ceil(max(50,deepest_ply+10)/2)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out",type=Path,required=True)
    a=p.parse_args()
    m=manifest()
    a.out.parent.mkdir(parents=True,exist_ok=True)
    if a.out.exists() and json.loads(a.out.read_text(encoding="utf8"))!=m:
        raise ValueError("manifest exists with a different recipe; use a new path")
    a.out.write_text(json.dumps(m,indent=2)+"\n",encoding="utf8")
    print(f"20 primary arms, {len(m['sensitivities'])} follow-up arms, {len(m['extension_comparison']['runs'])} queued extension variants; no jobs launched")

if __name__=="__main__": main()
