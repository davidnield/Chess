"""Hold an arm's bound data to what its recipe says it is (review of 19 November 2026, finding 4).

A recipe names its inputs by label: ``candidate_pool`` ("brc1800-p30"), ``own_policy``
("historical-1000-1799") and the opponent policy (the OTB ensemble, or a "brc{R}-maia30"
control). The coordinator binds files to an arm from its run config, so without this check a
horizon-50 arm could be built on the ply-30 pool, or both policy controls on one overlay, and
every receipt would still verify. Each bound pool must carry the ``pool_from_book`` metadata
(population, cap, floor, key width) of the pool recipe its label names, at the arm's depth.
"""
from __future__ import annotations
import json
from pathlib import Path
from .spec import pool_recipes

OWN_POLICY_POOL = {"historical-1000-1799": "weak1000-1799"}
ENSEMBLE_SOURCES = {"broad": "brc-all", "adult": "brc1800", "rapid": "rapid2200", "classical": "classical1800"}


def _recipes():
    return {r["id"]: r for r in pool_recipes()}


def pool_meta(path):
    p = Path(str(path) + ".meta.json")
    if not p.exists():
        raise ValueError(f"{path}: no pool metadata ({p.name}); cannot check what population it holds")
    return json.loads(p.read_text(encoding="utf8"))


def check_pool(path, pool_id, what):
    """``path`` must be the pool recipe ``pool_id``: same speeds, rating bands, cap, floor, keys."""
    want = _recipes().get(pool_id)
    if want is None:
        raise ValueError(f"{what}: no pool recipe {pool_id!r}")
    meta = pool_meta(path)
    pop = meta.get("population") or {}
    got = dict(events=sorted(pop.get("events") or []), elo_bands=sorted(pop.get("elo_bands") or []),
               max_ply=meta.get("max_ply"), min_games=meta.get("min_games"), key_bits=meta.get("key_bits"))
    expected = dict(events=sorted(want["events"]), elo_bands=sorted(want["elo_bands"]),
                    max_ply=want["max_ply"], min_games=want["min_games"], key_bits=want["key_bits"])
    bad = {k: (got[k], expected[k]) for k in expected if got[k] != expected[k]}
    if bad:
        raise ValueError(f"{what} is not {pool_id}: " + "; ".join(f"{k} {g!r}, want {w!r}" for k, (g, w) in bad.items()))


def check_arm(recipe, inputs):
    """Raise unless the arm's bound stats, weak pool and opponent policy match its recipe."""
    depth = recipe["max_ply"]
    if not recipe["candidate_pool"].endswith(f"-p{depth}"):
        raise ValueError(f"candidate pool {recipe['candidate_pool']} does not match construction depth {depth}")
    check_pool(inputs["stats"], recipe["candidate_pool"], "candidate pool (--stats)")
    own = OWN_POLICY_POOL.get(recipe["own_policy"])
    if own is None:
        raise ValueError(f"unknown own post-book policy {recipe['own_policy']!r}")
    check_pool(inputs["weak"], f"{own}-p{depth}", "own post-book policy (--weak-pool)")
    policy = recipe["opponent_policy"]
    if policy["name"] == "otb-ensemble-v1":
        config, overlay = inputs.get("opponent_policy_config"), inputs.get("opponent_overlay")
        if config:
            sources = json.loads(Path(config).read_text(encoding="utf8"))["sources"]
            for role, base in ENSEMBLE_SOURCES.items():
                check_pool(sources[role], f"{base}-p{depth}", f"ensemble source {role}")
            name = Path(sources["otb"]).name
            if name != f"training-p{depth}.parquet":
                raise ValueError(f"ensemble OTB source {name} is not the training-role p{depth} export")
        elif overlay:
            # a frozen (schema 3) ensemble overlay: its recipe and outcome pool must be this arm's
            meta = json.loads(Path(str(overlay) + ".json").read_text(encoding="utf8"))
            if meta.get("schema") != 3 or meta.get("policy_recipe") != policy:
                raise ValueError("the bound opponent overlay is not a frozen ensemble of this recipe")
            check_pool(meta["source_pool"], f"{ENSEMBLE_SOURCES['adult']}-p{depth}", "ensemble outcome pool")
        else:
            raise ValueError("the OTB ensemble arm needs a selective policy config or a frozen ensemble overlay")
    else:
        overlay = inputs.get("opponent_overlay")
        if not overlay:
            raise ValueError(f"control policy {policy['name']} needs its opponent overlay")
        meta = json.loads(Path(str(overlay) + ".json").read_text(encoding="utf8"))
        if meta.get("schema") == 3:
            raise ValueError(f"control policy {policy['name']} is bound to an ensemble overlay")
        # The overlay's source_pool is the OUTCOME pool (it must equal --stats); the reply
        # population it was built from must be declared separately, as reply_pool.
        check_pool(meta["source_pool"], f"{ENSEMBLE_SOURCES['adult']}-p{depth}",
                   f"control overlay {policy['name']} outcome pool")
        if not meta.get("reply_pool"):
            raise ValueError(f"control overlay for {policy['name']} does not declare its reply_pool")
        base = policy["name"].split("-")[0]
        check_pool(meta["reply_pool"], f"{base}-p{depth}", f"control overlay {policy['name']} reply pool")
