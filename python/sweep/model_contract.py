"""Contracts for the corrected pipeline; legacy sweep recipes remain reproducible.

Engineering recipes are runnable diagnostics, not accepted production recipes.
Calibration and real-data cost/continuation evidence are required before promotion.
"""
import math
from .spec import digest

SCHEMA="human-continuation-v1"
UTILITY="lichess-source-clip2000-v1"
NONMOVE="conditional-board-v1"


def validate(recipe):
    if recipe.get("schema")!=SCHEMA or recipe.get("utility")!=UTILITY or recipe.get("nonmove")!=NONMOVE:
        raise ValueError("unknown continuation model contract")
    for k in ("candidate_cap","count_cap","source_cap","evaluation_horizon","max_states","batch_roots"):
        if type(recipe.get(k)) is not int or recipe[k]<1: raise ValueError(f"invalid {k}")
    if not recipe["candidate_cap"]<=recipe["count_cap"]<=recipe["source_cap"]:
        raise ValueError("candidate/count/source horizons disagree")
    if recipe["evaluation_horizon"]<recipe["candidate_cap"]+3:
        raise ValueError("evaluation horizon omits the short continuation")
    if recipe.get("beyond_source")!="model-only-explicit":
        raise ValueError("beyond-source behavior must be explicit")
    own=recipe.get("own",{})
    if own.get("name")!="weak-maia-v1" or own.get("model")!="maia3-79m-position":
        raise ValueError("unknown own policy")
    if own.get("ratings") not in [[a,b] for a in (1200,1450,1700) for b in (1800,2200)]:
        raise ValueError("own ratings outside the planned calibration grid")
    if own.get("strength") not in (10.,30.,100.) or own.get("temperature") not in (1.,1.35):
        raise ValueError("own parameters outside the planned calibration grid")
    threshold=recipe.get("min_expand")
    if not isinstance(threshold,(int,float)) or not math.isfinite(threshold) or not 0<=threshold<=1:
        raise ValueError("invalid expansion threshold")
    if recipe.get("status")!="engineering":
        raise ValueError("production promotion requires the pending calibration and cost acceptance")
    return digest(recipe)


def calibration_grid():
    """36 transforms over six raw rating-pair predictions; no final-test tuning."""
    return [dict(name="weak-maia-v1",model="maia3-79m-position",ratings=[a,b],temperature=t,strength=k)
            for a in (1200,1450,1700) for b in (1800,2200) for t in (1.,1.35) for k in (10.,30.,100.)]
