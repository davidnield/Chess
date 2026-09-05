"""Workspace-confined outputs and bounded, fingerprinted inputs."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = Path(__file__).resolve().parent / "_artifacts"
DEFAULT_CONFIG = ROOT / "docs/human_repertoire/experiment.json"


def output_path(relative):
    path = (ARTIFACTS / relative).resolve()
    if not path.is_relative_to(ARTIFACTS.resolve()) or path == ARTIFACTS.resolve():
        raise ValueError("outputs must be inside the research artifact directory")
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_json(relative, obj):
    path = output_path(relative)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fingerprint(path, full=False):
    path = Path(path)
    stat = path.stat()
    result = {"path": str(path.resolve()), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    if full:
        result["sha256"] = sha256(path)
    return result
