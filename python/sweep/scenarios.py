"""Two source-policy versions of the approved first pilot, with common counts/referee."""
from copy import deepcopy
from pathlib import Path
from eval_binding import read_binding
from .spec import digest


def source_arm(base, binding_path):
    arm = deepcopy(base)
    arm["recipe"]["evaluation_binding_sha256"] = read_binding(binding_path)["binding_sha256"]
    arm["id"] = digest(arm["recipe"])[:20]
    return arm


def check_pair(config, pilot):
    entries = config["arms"]
    if len(entries) != 2 or pilot["recipe"]["mode"] != "independent":
        raise ValueError("paired source pilot needs exactly two independent pilot arms")
    bindings = []
    for entry in entries:
        binding = entry["inputs"].get("eval_binding")
        if not binding or entry["arm"] != source_arm(pilot, binding):
            raise ValueError("source scenario changes the approved pilot or its binding")
        bindings.append(read_binding(binding))
    if bindings[0]["lookup_sha256"] == bindings[1]["lookup_sha256"]:
        raise ValueError("source scenarios must bind distinct evaluation generations")
    a, b = [e["inputs"] for e in entries]
    if {k:v for k,v in a.items() if k!="eval_binding"} != {k:v for k,v in b.items() if k!="eval_binding"}:
        raise ValueError("source scenarios must share all count, policy and other inputs")
    referee = config.get("referee_eval_binding")
    if not referee:
        raise ValueError("source scenarios require one frozen independent referee binding")
    return read_binding(referee)
